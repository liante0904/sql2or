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
            ORDER BY SAVE_TIME ASC, report_id ASC
        """
        self.sqlite_cursor.execute(query, (last_oracle_time,))
        return self.sqlite_cursor.fetchall()

    def get_latest_save_time(self, db_type: str = "oracle") -> str:
        # Oracle에서 조회 시 SQLite 문자열 포맷과 완벽히 일치시킴 (T 포함 및 FF6)
        if db_type == "oracle":
            query = "SELECT TO_CHAR(MAX(SAVE_TIME), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6') FROM data_main_daily_send"
        else:
            query = "SELECT MAX(SAVE_TIME) FROM data_main_daily_send"
            
        cursor = self.oracle_cursor if db_type == "oracle" else self.sqlite_cursor
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result if result else '1900-01-01T00:00:00.000000'

    def sync_to_oracle(self, full_sync: bool = False):
        sqlite_count, oracle_count = self.get_counts()
        print(f"SQLite3 레코드 수: {sqlite_count}, Oracle 레코드 수: {oracle_count}")

        last_oracle_time = '1900-01-01T00:00:00.000000' if full_sync else self.get_latest_save_time("oracle")
        print(f"{'전체' if full_sync else '마지막 Oracle'} SAVE_TIME 기준점: {last_oracle_time}")

        # SQLite에서 데이터 가져오기 (커서를 직접 순회하여 메모리 효율성 증대)
        query = """
            SELECT report_id, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                   ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                   DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                   GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL 
            FROM data_main_daily_send 
            WHERE SAVE_TIME > ?
            ORDER BY SAVE_TIME ASC, report_id ASC
        """
        self.sqlite_cursor.execute(query, (last_oracle_time,))
        
        upsert_query = """
            MERGE INTO DATA_MAIN_DAILY_SEND dest
            USING (
                SELECT :rid AS REPORT_ID FROM dual
            ) src
            ON (dest.REPORT_ID = src.REPORT_ID)
            WHEN MATCHED THEN
                UPDATE SET 
                    dest.SEC_FIRM_ORDER = :sfo,
                    dest.ARTICLE_BOARD_ORDER = :abo,
                    dest.FIRM_NM = :fnm,
                    dest.ATTACH_URL = :aurl,
                    dest.ARTICLE_TITLE = :atit,
                    dest.ARTICLE_URL = :artu,
                    dest.SEND_USER = :susr,
                    dest.MAIN_CH_SEND_YN = :mcy,
                    dest.DOWNLOAD_STATUS_YN = :dsy,
                    dest.DOWNLOAD_URL = :durl,
                    dest.SAVE_TIME = :stime,
                    dest.REG_DT = :rdt,
                    dest.WRITER = :wtr,
                    dest."KEY" = :key,
                    dest.TELEGRAM_URL = :turl,
                    dest.MKT_TP = :mtp,
                    dest.GEMINI_SUMMARY = :gsum,
                    dest.SUMMARY_TIME = :sumt,
                    dest.SUMMARY_MODEL = :summ
            WHEN NOT MATCHED THEN
                INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                        ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                        DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                        GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL)
                VALUES (:rid, :sfo, :abo, :fnm, :aurl, :atit, :artu, :susr, :mcy, :dsy, 
                        :durl, :stime, :rdt, :wtr, :key, :turl, :mtp, :gsum, :sumt, :summ)
        """

        def parse_dt(val):
            if val is None or str(val).strip() == "":
                return None
            try:
                # SQLite의 'YYYY-MM-DDTHH:MM:SS.ffffff' 형식을 처리
                clean_val = str(val).replace('"', '').replace('T', ' ')
                # .ffffff 부분이 없을 경우를 대비해 처리
                if '.' not in clean_val:
                    return datetime.strptime(clean_val, '%Y-%m-%d %H:%M:%S')
                return datetime.strptime(clean_val, '%Y-%m-%d %H:%M:%S.%f')
            except Exception:
                return None

        def clean(val):
            if val is None or str(val).strip() == "":
                return None  # Oracle에서는 None이 NULL로 처리되며, 공백 1칸보다 효율적입니다.
            return str(val)

        batch_size = 5000
        params = []
        total_synced = 0
        
        start_time = time.time()

        while True:
            rows = self.sqlite_cursor.fetchmany(batch_size)
            if not rows:
                break
            
            batch_params = []
            for row in rows:
                batch_params.append({
                    "rid": row['report_id'],
                    "sfo": row['SEC_FIRM_ORDER'] if row['SEC_FIRM_ORDER'] is not None else 0,
                    "abo": row['ARTICLE_BOARD_ORDER'] if row['ARTICLE_BOARD_ORDER'] is not None else 0,
                    "fnm": clean(row['FIRM_NM']),
                    "aurl": clean(row['ATTACH_URL']),
                    "atit": clean(row['ARTICLE_TITLE']),
                    "artu": clean(row['ARTICLE_URL']),
                    "susr": clean(row['SEND_USER']),
                    "mcy": clean(row['MAIN_CH_SEND_YN']),
                    "dsy": clean(row['DOWNLOAD_STATUS_YN']),
                    "durl": clean(row['DOWNLOAD_URL']),
                    "stime": parse_dt(row['SAVE_TIME']),
                    "rdt": parse_dt(row['REG_DT']),
                    "wtr": clean(row['WRITER']),
                    "key": clean(row['KEY']),
                    "turl": clean(row['TELEGRAM_URL']),
                    "mtp": clean(row['MKT_TP']),
                    "gsum": clean(row['GEMINI_SUMMARY']),
                    "sumt": parse_dt(row['SUMMARY_TIME']),
                    "summ": clean(row['SUMMARY_MODEL'])
                })

            try:
                self.oracle_cursor.executemany(upsert_query, batch_params, batcherrors=True)
                self.oracle_conn.commit()
                total_synced += len(batch_params)
                print(f"동기화 중... 현재 {total_synced}개 완료 (경과 시간: {time.time() - start_time:.2f}초)")
            except oracledb.DatabaseError as e:
                self.oracle_conn.rollback()
                print(f"배치 동기화 중 오류: {e}")
                # batcherrors=True 사용 시 상세 오류 확인 가능
                for error in self.oracle_cursor.getbatcherrors():
                    print(f"Error {error.message} at row offset {error.offset}")

        if total_synced > 0:
            print(f"총 {total_synced}개 레코드 동기화 완료. (총 소요 시간: {time.time() - start_time:.2f}초)")
            new_sqlite_count, new_oracle_count = self.get_counts()
            print(f"동기화 후: SQLite3 {new_sqlite_count}, Oracle {new_oracle_count}")
        else:
            print("동기화할 데이터가 없습니다.")

if __name__ == "__main__":
    sync = DatabaseSync()
    try:
        # 처음 실행 시에는 반드시 아래를 True로 바꿔서 과거 누락분(124개)을 채우세요.
        # sync.sync_to_oracle(full_sync=True) 
        
        sync.sync_to_oracle(full_sync=False) # 그 다음부터는 False로 운영
    finally:
        sync.close_connections()