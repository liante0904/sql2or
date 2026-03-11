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

        # NVL(src.COL, dest.COL)을 사용하여 소스 데이터가 NULL일 경우 기존 오라클 데이터를 유지함
        upsert_query = """
            MERGE INTO DATA_MAIN_DAILY_SEND dest
            USING (
                SELECT :1 AS REPORT_ID, :2 AS SEC_FIRM_ORDER, :3 AS ARTICLE_BOARD_ORDER, 
                       :4 AS FIRM_NM, :5 AS ATTACH_URL, :6 AS ARTICLE_TITLE, :7 AS ARTICLE_URL, 
                       :8 AS SEND_USER, :9 AS MAIN_CH_SEND_YN, :10 AS DOWNLOAD_STATUS_YN, 
                       :11 AS DOWNLOAD_URL, :12 AS SAVE_TIME, :13 AS REG_DT, 
                       :14 AS WRITER, :15 AS "KEY", :16 AS TELEGRAM_URL, :17 AS MKT_TP, 
                       :18 AS GEMINI_SUMMARY, :19 AS SUMMARY_TIME, :20 AS SUMMARY_MODEL 
                FROM dual
            ) src
            ON (dest.REPORT_ID = src.REPORT_ID)
            WHEN MATCHED THEN
                UPDATE SET 
                    dest.SEC_FIRM_ORDER = NVL(src.SEC_FIRM_ORDER, dest.SEC_FIRM_ORDER),
                    dest.ARTICLE_BOARD_ORDER = NVL(src.ARTICLE_BOARD_ORDER, dest.ARTICLE_BOARD_ORDER),
                    dest.FIRM_NM = NVL(src.FIRM_NM, dest.FIRM_NM),
                    dest.ATTACH_URL = NVL(src.ATTACH_URL, dest.ATTACH_URL),
                    dest.ARTICLE_TITLE = NVL(src.ARTICLE_TITLE, dest.ARTICLE_TITLE),
                    dest.ARTICLE_URL = NVL(src.ARTICLE_URL, dest.ARTICLE_URL),
                    dest.SEND_USER = NVL(src.SEND_USER, dest.SEND_USER),
                    dest.MAIN_CH_SEND_YN = NVL(src.MAIN_CH_SEND_YN, dest.MAIN_CH_SEND_YN),
                    dest.DOWNLOAD_STATUS_YN = NVL(src.DOWNLOAD_STATUS_YN, dest.DOWNLOAD_STATUS_YN),
                    dest.DOWNLOAD_URL = NVL(src.DOWNLOAD_URL, dest.DOWNLOAD_URL),
                    dest.SAVE_TIME = NVL(src.SAVE_TIME, dest.SAVE_TIME),
                    dest.REG_DT = NVL(src.REG_DT, dest.REG_DT),
                    dest.WRITER = NVL(src.WRITER, dest.WRITER),
                    dest."KEY" = NVL(src."KEY", dest."KEY"),
                    dest.TELEGRAM_URL = NVL(src.TELEGRAM_URL, dest.TELEGRAM_URL),
                    dest.MKT_TP = NVL(src.MKT_TP, dest.MKT_TP),
                    dest.GEMINI_SUMMARY = NVL(src.GEMINI_SUMMARY, dest.GEMINI_SUMMARY),
                    dest.SUMMARY_TIME = NVL(src.SUMMARY_TIME, dest.SUMMARY_TIME),
                    dest.SUMMARY_MODEL = NVL(src.SUMMARY_MODEL, dest.SUMMARY_MODEL)
            WHEN NOT MATCHED THEN
                INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                        ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                        DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                        GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL)
                VALUES (src.REPORT_ID, src.SEC_FIRM_ORDER, src.ARTICLE_BOARD_ORDER, src.FIRM_NM, 
                        src.ATTACH_URL, src.ARTICLE_TITLE, src.ARTICLE_URL, src.SEND_USER, 
                        src.MAIN_CH_SEND_YN, src.DOWNLOAD_STATUS_YN, src.DOWNLOAD_URL, 
                        src.SAVE_TIME, src.REG_DT, src.WRITER, src."KEY", src.TELEGRAM_URL, src.MKT_TP, 
                        src.GEMINI_SUMMARY, src.SUMMARY_TIME, src.SUMMARY_MODEL)
        """

        params = []
        for row in new_data:
            def get_url_val(key):
                val = row[key]
                if val and str(val).strip(): return str(val)
                tg_url = row['TELEGRAM_URL']
                if tg_url and str(tg_url).strip(): return str(tg_url)
                key_val = row['KEY']
                return str(key_val) if key_val else "N/A"

            params.append((
                row['report_id'],
                row['SEC_FIRM_ORDER'] or 0,
                row['ARTICLE_BOARD_ORDER'] or 0,
                row['FIRM_NM'] or ' ',
                get_url_val('ATTACH_URL'),
                row['ARTICLE_TITLE'] or 'No Title',
                get_url_val('ARTICLE_URL'),
                row['SEND_USER'] or ' ',
                row['MAIN_CH_SEND_YN'] or 'N',
                row['DOWNLOAD_STATUS_YN'] or ' ',
                get_url_val('DOWNLOAD_URL'),
                self.parse_dt(row['SAVE_TIME']),
                self.parse_dt(row['REG_DT']),
                row['WRITER'] or ' ',
                row['KEY'] or ' ',
                row['TELEGRAM_URL'] or ' ',
                row['MKT_TP'] or 'KR',
                row['GEMINI_SUMMARY'] or ' ',
                self.parse_dt(row['SUMMARY_TIME']),
                row['SUMMARY_MODEL'] or ' '
            ))

        try:
            self.oracle_cursor.executemany(upsert_query, params, batcherrors=True)
            self.oracle_conn.commit()
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
