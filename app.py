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
            ORDER BY report_id
        """
        self.sqlite_cursor.execute(query, (last_oracle_time,))
        return self.sqlite_cursor.fetchall()

    def get_latest_save_time(self, db_type: str = "oracle") -> str:
        query = "SELECT MAX(SAVE_TIME) FROM data_main_daily_send"
        cursor = self.oracle_cursor if db_type == "oracle" else self.sqlite_cursor
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result if result else '1900-01-01T00:00:00.000000'

    def sync_to_oracle(self, full_sync: bool = False):
        sqlite_count, oracle_count = self.get_counts()
        print(f"SQLite3 레코드 수: {sqlite_count}, Oracle 레코드 수: {oracle_count}")

        last_oracle_time = '1900-01-01T00:00:00.000000' if full_sync else self.get_latest_save_time("oracle")
        print(f"{'전체' if full_sync else '마지막 Oracle'} SAVE_TIME: {last_oracle_time}")

        new_data = self.fetch_new_sqlite_data(last_oracle_time)
        print(f"{'전체' if full_sync else '증분'} 동기화: 동기화할 레코드 수: {len(new_data)}")

        if not new_data:
            print("동기화할 데이터가 없습니다.")
            return

        # 네임드 바인드(:변수명)를 사용하여 중복 참조 및 개수 오류 해결
        upsert_query = """
            MERGE INTO DATA_MAIN_DAILY_SEND dest
            USING (
                SELECT :rid AS REPORT_ID, :sfo AS SEC_FIRM_ORDER, :abo AS ARTICLE_BOARD_ORDER, 
                       :fnm AS FIRM_NM, :aurl AS ATTACH_URL, :atit AS ARTICLE_TITLE, :artu AS ARTICLE_URL, 
                       :susr AS SEND_USER, :mcy AS MAIN_CH_SEND_YN, :dsy AS DOWNLOAD_STATUS_YN, 
                       :durl AS DOWNLOAD_URL, 
                       CASE WHEN :stime IS NOT NULL THEN TO_TIMESTAMP(:stime, 'YYYY-MM-DD"T"HH24:MI:SS.FF') ELSE NULL END AS SAVE_TIME, 
                       CASE WHEN :rdt IS NOT NULL THEN TO_TIMESTAMP(:rdt, 'YYYY-MM-DD"T"HH24:MI:SS.FF') ELSE NULL END AS REG_DT, 
                       :wtr AS WRITER, :key AS "KEY", :turl AS TELEGRAM_URL, :mtp AS MKT_TP, 
                       :gsum AS GEMINI_SUMMARY, 
                       CASE WHEN :sumt IS NOT NULL THEN TO_TIMESTAMP(:sumt, 'YYYY-MM-DD"T"HH24:MI:SS.FF') ELSE NULL END AS SUMMARY_TIME, 
                       :summ AS SUMMARY_MODEL 
                FROM dual
            ) src
            ON (dest.REPORT_ID = src.REPORT_ID)
            WHEN MATCHED THEN
                UPDATE SET 
                    dest.SEC_FIRM_ORDER = src.SEC_FIRM_ORDER,
                    dest.ARTICLE_BOARD_ORDER = src.ARTICLE_BOARD_ORDER,
                    dest.FIRM_NM = src.FIRM_NM,
                    dest.ATTACH_URL = src.ATTACH_URL,
                    dest.ARTICLE_TITLE = src.ARTICLE_TITLE,
                    dest.ARTICLE_URL = src.ARTICLE_URL,
                    dest.SEND_USER = src.SEND_USER,
                    dest.MAIN_CH_SEND_YN = src.MAIN_CH_SEND_YN,
                    dest.DOWNLOAD_STATUS_YN = src.DOWNLOAD_STATUS_YN,
                    dest.DOWNLOAD_URL = src.DOWNLOAD_URL,
                    dest.SAVE_TIME = src.SAVE_TIME,
                    dest.REG_DT = src.REG_DT,
                    dest.WRITER = src.WRITER,
                    dest."KEY" = src."KEY",
                    dest.TELEGRAM_URL = src.TELEGRAM_URL,
                    dest.MKT_TP = src.MKT_TP,
                    dest.GEMINI_SUMMARY = src.GEMINI_SUMMARY,
                    dest.SUMMARY_TIME = src.SUMMARY_TIME,
                    dest.SUMMARY_MODEL = src.SUMMARY_MODEL
            WHEN NOT MATCHED THEN
                INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                        ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                        DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL)
                VALUES (src.REPORT_ID, src.SEC_FIRM_ORDER, src.ARTICLE_BOARD_ORDER, src.FIRM_NM, 
                        src.ATTACH_URL, src.ARTICLE_TITLE, src.ARTICLE_URL, src.SEND_USER, 
                        src.MAIN_CH_SEND_YN, src.DOWNLOAD_STATUS_YN, src.DOWNLOAD_URL, 
                        src.SAVE_TIME, src.REG_DT, src.WRITER, src."KEY", src.TELEGRAM_URL, src.MKT_TP, src.GEMINI_SUMMARY, src.SUMMARY_TIME, src.SUMMARY_MODEL)
        """

        def clean(val, is_date=False):
            if val is None or str(val).strip() == "":
                return None if is_date else " "
            return str(val)

        # 딕셔너리 리스트 형태로 파라미터 준비 (네임드 바인드 매핑)
        params = [
            {
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
                "stime": clean(row['SAVE_TIME'], True),
                "rdt": clean(row['REG_DT'], True),
                "wtr": clean(row['WRITER']),
                "key": clean(row['KEY']),
                "turl": clean(row['TELEGRAM_URL']),
                "mtp": clean(row['MKT_TP']),
                "gsum": clean(row['GEMINI_SUMMARY']),
                "sumt": clean(row['SUMMARY_TIME'], True),
                "summ": clean(row['SUMMARY_MODEL'])
            }
            for row in new_data
        ]

        try:
            # executemany에서 딕셔너리 리스트 사용
            self.oracle_cursor.executemany(upsert_query, params, batcherrors=True)
            self.oracle_conn.commit()
            print("데이터 동기화 성공.")
            for error in self.oracle_cursor.getbatcherrors():
                print(f"행 {error.offset}에서 오류: {error.message}")
            
            new_sqlite_count, new_oracle_count = self.get_counts()
            print(f"동기화 후: SQLite3 레코드 수: {new_sqlite_count}, Oracle 레코드 수: {new_oracle_count}")
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
            print(f"Oracle에만 존재하는 REPORT_ID 삭제 중... ({len(excess_ids)}개)")
            delete_query = "DELETE FROM DATA_MAIN_DAILY_SEND WHERE REPORT_ID = :rid"
            try:
                self.oracle_cursor.executemany(delete_query, [{"rid": rid} for rid in excess_ids])
                self.oracle_conn.commit()
                print("삭제 완료.")
            except oracledb.DatabaseError as e:
                self.oracle_conn.rollback()
                print(f"삭제 중 오류: {e}")
        else:
            print("Oracle에 정리할 레코드가 없습니다.")

if __name__ == "__main__":
    sync = DatabaseSync()
    try:
        sync.sync_to_oracle(full_sync=False)
        sync.remove_excess_oracle_records()
    except Exception as e:
        print(f"치명적 오류 발생: {e}")
        exit(1)
    finally:
        sync.close_connections()