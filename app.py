import sqlite3
import oracledb
import os
from dotenv import load_dotenv
from datetime import datetime
import time
from typing import List, Tuple

load_dotenv()

WALLET_LOCATION = os.getenv('WALLET_LOCATION')
WALLET_PASSWORD = os.getenv('WALLET_PASSWORD')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DSN = os.getenv('DB_DSN')
SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH')

class DatabaseSync:
    def __init__(self):
        self.sqlite_conn = sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)
        self.sqlite_conn.row_factory = sqlite3.Row
        self.sqlite_cursor = self.sqlite_conn.cursor()
        
        self.oracle_conn = oracledb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            config_dir=WALLET_LOCATION,
            wallet_location=WALLET_LOCATION,
            wallet_password=WALLET_PASSWORD
        )
        self.oracle_cursor = self.oracle_conn.cursor()

        # [핵심] Truncate 등으로 인덱스가 삭제되었을 경우를 대비한 안전장치
        # 이 인덱스가 있어야 30만건 MERGE가 30초 안에 끝납니다.
        try:
            self.oracle_cursor.execute("CREATE UNIQUE INDEX PK_REPORT_ID_SYNC ON DATA_MAIN_DAILY_SEND(REPORT_ID)")
            self.oracle_conn.commit()
        except:
            pass

        self.sqlite_cursor.execute("CREATE INDEX IF NOT EXISTS idx_save_time ON data_main_daily_send (SAVE_TIME)")

    def close_connections(self):
        self.sqlite_conn.close()
        self.oracle_conn.close()

    def get_counts(self) -> Tuple[int, int]:
        self.sqlite_cursor.execute("SELECT COUNT(*) FROM data_main_daily_send")
        sqlite_count = self.sqlite_cursor.fetchone()[0]
        self.oracle_cursor.execute("SELECT COUNT(*) FROM DATA_MAIN_DAILY_SEND")
        oracle_count = self.oracle_cursor.fetchone()[0]
        return sqlite_count, oracle_count

    def fetch_new_sqlite_data(self, last_oracle_time: str) -> List[sqlite3.Row]:
        query = """
            SELECT report_id, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                   ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                   DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                   GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL 
            FROM data_main_daily_send 
            WHERE SAVE_TIME > ?
            ORDER BY report_id
        """
        self.sqlite_cursor.execute(query, (last_oracle_time,))
        return self.sqlite_cursor.fetchall()

    def get_latest_save_time(self, db_type: str = "oracle") -> str:
        if db_type == "oracle":
            # Oracle 환경 설정(NLS)에 상관없이 ISO 규격으로 가져오도록 TO_CHAR 사용
            query = "SELECT TO_CHAR(MAX(SAVE_TIME), 'YYYY-MM-DD HH24:MI:SS') FROM data_main_daily_send"
        else:
            query = "SELECT MAX(SAVE_TIME) FROM data_main_daily_send"

        cursor = self.oracle_cursor if db_type == "oracle" else self.sqlite_cursor
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if not result:
            return '1900-01-01'
        
        # result가 datetime 객체로 반환되는 경우 문자열로 변환
        if isinstance(result, datetime):
            return result.strftime('%Y-%m-%d %H:%M:%S')
            
        return str(result)

    def parse_dt(self, dt_str):
        if not dt_str or str(dt_str).strip() in ['', 'None']:
            return None
        dt_str = str(dt_str).replace('T', ' ').strip()
        formats = ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y%m%d%H%M%S', '%Y-%m-%d', '%Y%m%d']
        for fmt in formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        return None

    def sync_to_oracle(self, full_sync: bool = False):
        sqlite_count, oracle_count = self.get_counts()
        print(f"SQLite3 레코드 수: {sqlite_count}, Oracle 레코드 수: {oracle_count}")

        last_oracle_time = '1900-01-01' if full_sync else self.get_latest_save_time("oracle")
        print(f"{'전체' if full_sync else '마지막 Oracle'} SAVE_TIME: {last_oracle_time}")

        start_time = time.time()
        new_data = self.fetch_new_sqlite_data(last_oracle_time)
        print(f"{'전체' if full_sync else '증분'} 동기화 대상 레코드 수: {len(new_data)}")

        if not new_data:
            print("동기화할 데이터가 없습니다.")
            return

        # OracleManager의 _insert_sync_process MERGE 구조 그대로 적용
        upsert_query = """
        MERGE INTO DATA_MAIN_DAILY_SEND t
        USING (SELECT :REPORT_ID as REPORT_ID, :SEC_FIRM_ORDER as SEC_FIRM_ORDER, 
                      :ARTICLE_BOARD_ORDER as ARTICLE_BOARD_ORDER, :FIRM_NM as FIRM_NM, 
                      :SEND_USER as SEND_USER, :MAIN_CH_SEND_YN as MAIN_CH_SEND_YN, 
                      :DOWNLOAD_STATUS_YN as DOWNLOAD_STATUS_YN, :SAVE_TIME as SAVE_TIME,
                      :REG_DT as REG_DT, :WRITER as WRITER, :KEY as "KEY", :MKT_TP as MKT_TP, 
                      :ATTACH_URL as ATTACH_URL, :ARTICLE_TITLE as ARTICLE_TITLE, 
                      :TELEGRAM_URL as TELEGRAM_URL, :ARTICLE_URL as ARTICLE_URL, 
                      :DOWNLOAD_URL as DOWNLOAD_URL, :GEMINI_SUMMARY as GEMINI_SUMMARY,
                      :SUMMARY_TIME as SUMMARY_TIME, :SUMMARY_MODEL as SUMMARY_MODEL
               FROM DUAL) s
        ON (t.REPORT_ID = s.REPORT_ID)
        WHEN MATCHED THEN
            UPDATE SET 
                t.SEC_FIRM_ORDER = NVL(s.SEC_FIRM_ORDER, t.SEC_FIRM_ORDER),
                t.ARTICLE_BOARD_ORDER = NVL(s.ARTICLE_BOARD_ORDER, t.ARTICLE_BOARD_ORDER),
                t.FIRM_NM = NVL(s.FIRM_NM, t.FIRM_NM),
                t.SEND_USER = NVL(s.SEND_USER, t.SEND_USER),
                t.MAIN_CH_SEND_YN = NVL(s.MAIN_CH_SEND_YN, t.MAIN_CH_SEND_YN),
                t.DOWNLOAD_STATUS_YN = NVL(s.DOWNLOAD_STATUS_YN, t.DOWNLOAD_STATUS_YN),
                t.SAVE_TIME = NVL(s.SAVE_TIME, t.SAVE_TIME),
                t.REG_DT = NVL(s.REG_DT, t.REG_DT),
                t.WRITER = NVL(s.WRITER, t.WRITER),
                t.KEY = NVL(s.KEY, t.KEY),
                t.MKT_TP = NVL(s.MKT_TP, t.MKT_TP),
                t.TELEGRAM_URL = NVL(s.TELEGRAM_URL, t.TELEGRAM_URL),
                t.ATTACH_URL = NVL(s.ATTACH_URL, t.ATTACH_URL),
                t.ARTICLE_TITLE = NVL(s.ARTICLE_TITLE, t.ARTICLE_TITLE),
                t.ARTICLE_URL = NVL(s.ARTICLE_URL, t.ARTICLE_URL),
                t.DOWNLOAD_URL = NVL(s.DOWNLOAD_URL, t.DOWNLOAD_URL),
                t.GEMINI_SUMMARY = NVL(s.GEMINI_SUMMARY, t.GEMINI_SUMMARY),
                t.SUMMARY_TIME = NVL(s.SUMMARY_TIME, t.SUMMARY_TIME),
                t.SUMMARY_MODEL = NVL(s.SUMMARY_MODEL, t.SUMMARY_MODEL)
        WHEN NOT MATCHED THEN
            INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, SEND_USER,
                    MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, SAVE_TIME, REG_DT, WRITER, 
                    KEY, MKT_TP, ATTACH_URL, ARTICLE_TITLE, TELEGRAM_URL, 
                    ARTICLE_URL, DOWNLOAD_URL, GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL)
            VALUES (s.REPORT_ID, s.SEC_FIRM_ORDER, s.ARTICLE_BOARD_ORDER, s.FIRM_NM, s.SEND_USER,
                    s.MAIN_CH_SEND_YN, s.DOWNLOAD_STATUS_YN, s.SAVE_TIME, s.REG_DT, s.WRITER, 
                    s.KEY, s.MKT_TP, s.ATTACH_URL, s.ARTICLE_TITLE, s.TELEGRAM_URL, 
                    s.ARTICLE_URL, s.DOWNLOAD_URL, s.GEMINI_SUMMARY, s.SUMMARY_TIME, s.SUMMARY_MODEL)
        """

        params_list = []
        for row in new_data:
            # OracleManager의 get_str, get_url_val 로직 그대로 구현
            def get_str(val, max_len=None):
                if val is None or str(val).strip() == "":
                    return None
                return str(val)[:max_len] if max_len else str(val)

            def get_url_val(key, max_len=4000):
                val = get_str(row[key], max_len)
                if val: return val
                tg_url = get_str(row['TELEGRAM_URL'], max_len)
                if tg_url: return tg_url
                key_val = get_str(row['KEY'], max_len)
                return key_val if key_val else "N/A"

            params_list.append({
                "REPORT_ID": row['report_id'],
                "SEC_FIRM_ORDER": row['SEC_FIRM_ORDER'] if row['SEC_FIRM_ORDER'] not in [None, ''] else 0,
                "ARTICLE_BOARD_ORDER": row['ARTICLE_BOARD_ORDER'] if row['ARTICLE_BOARD_ORDER'] not in [None, ''] else 0,
                "FIRM_NM": get_str(row['FIRM_NM'], 300),
                "SEND_USER": get_str(row['SEND_USER'], 100),
                "MAIN_CH_SEND_YN": get_str(row['MAIN_CH_SEND_YN'], 100) or 'N',
                "DOWNLOAD_STATUS_YN": get_str(row['DOWNLOAD_STATUS_YN'], 100),
                "SAVE_TIME": self.parse_dt(row['SAVE_TIME']),
                "REG_DT": get_str(row['REG_DT'], 100), # OracleManager에선 REG_DT를 문자열로 처리
                "WRITER": get_str(row['WRITER'], 200),
                "KEY": get_str(row['KEY'], 4000),
                "MKT_TP": get_str(row['MKT_TP'], 100) or 'KR',
                "ATTACH_URL": get_url_val("ATTACH_URL", 4000),
                "ARTICLE_TITLE": get_str(row['ARTICLE_TITLE'], 4000) or "No Title",
                "TELEGRAM_URL": get_str(row['TELEGRAM_URL'], 4000),
                "ARTICLE_URL": get_url_val("ARTICLE_URL", 4000),
                "DOWNLOAD_URL": get_url_val("DOWNLOAD_URL", 4000),
                "GEMINI_SUMMARY": get_str(row['GEMINI_SUMMARY'], 4000),
                "SUMMARY_TIME": self.parse_dt(row['SUMMARY_TIME']),
                "SUMMARY_MODEL": get_str(row['SUMMARY_MODEL'], 100)
            })

        try:
            self.oracle_cursor.executemany(upsert_query, params_list, batcherrors=True)
            self.oracle_conn.commit()
            print(f"데이터 동기화 성공. (소요 시간: {time.time() - start_time:.2f}초)")
            for error in self.oracle_cursor.getbatcherrors():
                print(f"행 {error.offset}에서 오류: {error.message}")
            new_sqlite_count, new_oracle_count = self.get_counts()
            print(f"동기화 후: SQLite3 레코드 수: {new_sqlite_count}, Oracle 레코드 수: {new_oracle_count}")
        except oracledb.DatabaseError as e:
            self.oracle_conn.rollback()
            print(f"동기화 중 오류: {e}")

            print(f"데이터 동기화 성공. (소요 시간: {time.time() - start_time:.2f}초)")
            for error in self.oracle_cursor.getbatcherrors():
                print(f"행 {error.offset}에서 오류: {error.message}")
            new_sqlite_count, new_oracle_count = self.get_counts()
            print(f"동기화 후: SQLite3 레코드 수: {new_sqlite_count}, Oracle 레코드  수: {new_oracle_count}")
        except oracledb.DatabaseError as e:
            self.oracle_conn.rollback()
            print(f"동기화 중 오류: {e}")

    def remove_excess_oracle_records(self):
        self.sqlite_cursor.execute("SELECT REPORT_ID FROM data_main_daily_send")
        sqlite_ids = set(row[0] for row in self.sqlite_cursor.fetchall())
        
        self.oracle_cursor.execute("SELECT REPORT_ID FROM DATA_MAIN_DAILY_SEND")
        oracle_ids = set(row[0] for row in self.oracle_cursor.fetchall())
        
        excess_ids = oracle_ids - sqlite_ids
        
        if excess_ids:
            print(f"Oracle에만 존재하는 REPORT_ID: {len(excess_ids)}개")
            delete_query = "DELETE FROM DATA_MAIN_DAILY_SEND WHERE REPORT_ID = :1"
            try:
                self.oracle_cursor.executemany(delete_query, [(report_id,) for report_id in excess_ids])
                self.oracle_conn.commit()
                print(f"삭제 완료: {len(excess_ids)}개 레코드 삭제됨.")
                new_sqlite_count, new_oracle_count = self.get_counts()
                print(f"삭제 후: SQLite3 레코드 수: {new_sqlite_count}, Oracle 레코드 수: {new_oracle_count}")
            except oracledb.DatabaseError as e:
                self.oracle_conn.rollback()
                print(f"삭제 중 오류: {e}")
        else:
            print("Oracle에 SQLite에 없는 REPORT_ID가 없습니다.")

if __name__ == "__main__":
    sync = DatabaseSync()
    try:
        sync.sync_to_oracle(full_sync=False)
        sync.remove_excess_oracle_records()
    except Exception as e:
        print(f"오류 발생: {e}")
        exit(1)
    finally:
        sync.close_connections()
