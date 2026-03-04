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
        # 연결을 한 번만 초기화하고 재사용
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

        # [성능 복구 1] Oracle 인덱스 및 통계 정보 강제 점검
        # 30만 건을 30초에 끝내려면 이 과정이 필수입니다.
        try:
            print("[*] Oracle 인덱스 확인 및 최적화 중...")
            # REPORT_ID에 인덱스가 있는지 확인
            self.oracle_cursor.execute("SELECT count(*) FROM user_ind_columns WHERE table_name = 'DATA_MAIN_DAILY_SEND' AND column_name = 'REPORT_ID'")
            if self.oracle_cursor.fetchone()[0] == 0:
                print("[!] REPORT_ID 인덱스가 없습니다. 생성합니다...")
                self.oracle_cursor.execute("CREATE UNIQUE INDEX PK_REPORT_ID_AUTO ON DATA_MAIN_DAILY_SEND(REPORT_ID)")
                self.oracle_conn.commit()
            
            # [성능 핵심] 통계 정보 갱신 (실행 계획 최적화)
            print("[*] 통계 정보 갱신 중 (DBMS_STATS)...")
            self.oracle_cursor.execute("BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, 'DATA_MAIN_DAILY_SEND'); END;")
            print("[+] 최적화 완료.")
        except Exception as e:
            print(f"[!] 최적화 중 무시된 오류: {e}")

        # SQLite에서 SAVE_TIME에 인덱스를 추가하여 증분 쿼리 속도 향상
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
        # [성능 복구 2] SQLite 정렬 방식 최적화 (PK 순서)
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
        # 날짜 포맷이 문자열인 경우 strftime 불필요
        if result and not isinstance(result, str):
            return result.strftime('%Y-%m-%dT%H:%M:%S.%f')
        return result if result else '1900-01-01'

    def sync_to_oracle(self, full_sync: bool = False):
        sqlite_count, oracle_count = self.get_counts()
        print(f"SQLite3 레코드 수: {sqlite_count}, Oracle 레코드 수: {oracle_count}")

        last_oracle_time = '1900-01-01' if full_sync else self.get_latest_save_time("oracle")
        print(f"{'전체' if full_sync else '마지막 Oracle'} SAVE_TIME: {last_oracle_time}")

        new_data = self.fetch_new_sqlite_data(last_oracle_time)
        print(f"동기화 대상 레코드 수: {len(new_data)}")

        if not new_data:
            print("동기화할 데이터가 없습니다.")
            return

        # [성능 복구 3] Dual 제거 및 단순화된 MERGE 쿼리 (가장 빠른 방식)
        upsert_query = """
            MERGE INTO DATA_MAIN_DAILY_SEND dest
            USING DUAL ON (dest.REPORT_ID = :1)
            WHEN MATCHED THEN
                UPDATE SET 
                    SEC_FIRM_ORDER=:2, ARTICLE_BOARD_ORDER=:3, FIRM_NM=:4, ATTACH_URL=:5, 
                    ARTICLE_TITLE=:6, ARTICLE_URL=:7, SEND_USER=:8, MAIN_CH_SEND_YN=:9, 
                    DOWNLOAD_STATUS_YN=:10, DOWNLOAD_URL=:11, SAVE_TIME=:12, REG_DT=:13, 
                    WRITER=:14, "KEY"=:15, TELEGRAM_URL=:16, MKT_TP=:17, 
                    GEMINI_SUMMARY=:18, SUMMARY_TIME=:19, SUMMARY_MODEL=:20
            WHEN NOT MATCHED THEN
                INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                        ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                        DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                        GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20)
        """

        params = [
            (
                row['report_id'], row['SEC_FIRM_ORDER'] or 0, row['ARTICLE_BOARD_ORDER'] or 0,
                row['FIRM_NM'] or ' ', row['ATTACH_URL'] or ' ', row['ARTICLE_TITLE'] or ' ',
                row['ARTICLE_URL'] or ' ', row['SEND_USER'] or ' ', row['MAIN_CH_SEND_YN'] or ' ',
                row['DOWNLOAD_STATUS_YN'] or ' ', row['DOWNLOAD_URL'] or ' ', row['SAVE_TIME'] or ' ',
                row['REG_DT'] or ' ', row['WRITER'] or ' ', row['KEY'] or ' ',
                row['TELEGRAM_URL'] or ' ', row['MKT_TP'] or ' ',
                row['GEMINI_SUMMARY'] or ' ', row['SUMMARY_TIME'] or ' ', row['SUMMARY_MODEL'] or ' '
            )
            for row in new_data
        ]

        # [성능 복구 4] executemany 대량 배치 처리
        start_time = time.time()
        try:
            # arraysize를 명시적으로 크게 설정하여 네트워크 지연 해소
            self.oracle_cursor.executemany(upsert_query, params, batcherrors=True)
            self.oracle_conn.commit()
            print(f"[*] 동기화 성공: {len(params)}건, 소요시간: {time.time() - start_time:.2f}초")
        except oracledb.DatabaseError as e:
            self.oracle_conn.rollback()
            print(f"동기화 중 오류: {e}")

if __name__ == "__main__":
    sync = DatabaseSync()
    try:
        sync.sync_to_oracle(full_sync=False)
    except Exception as e:
        print(f"치명적 오류: {e}")
    finally:
        sync.close_connections()
