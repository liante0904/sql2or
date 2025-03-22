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
        self.sqlite_conn.row_factory = sqlite3.Row  # 딕셔너리처럼 접근 가능하도록 Row 팩토리 사용
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
        query = """
            SELECT report_id, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                   ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                   DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP 
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
        return result if result else '1900-01-01'

    def sync_to_oracle(self, full_sync: bool = False):
        sqlite_count, oracle_count = self.get_counts()
        print(f"SQLite3 레코드 수: {sqlite_count}, Oracle 레코드 수: {oracle_count}")

        last_oracle_time = '1900-01-01' if full_sync else self.get_latest_save_time("oracle")
        print(f"{'전체' if full_sync else '마지막 Oracle'} SAVE_TIME: {last_oracle_time}")

        # 데이터 가져오기 (전체 동기화는 동일 쿼리에 과거 타임스탬프 사용)
        new_data = self.fetch_new_sqlite_data(last_oracle_time)
        print(f"{'전체' if full_sync else '증분'} 동기화: 동기화할 레코드 수: {len(new_data)}")

        if not new_data:
            print("동기화할 데이터가 없습니다.")
            return

        upsert_query = """
            MERGE INTO DATA_MAIN_DAILY_SEND dest
            USING (
                SELECT :1 AS REPORT_ID, :2 AS SEC_FIRM_ORDER, :3 AS ARTICLE_BOARD_ORDER, 
                       :4 AS FIRM_NM, :5 AS ATTACH_URL, :6 AS ARTICLE_TITLE, :7 AS ARTICLE_URL, 
                       :8 AS SEND_USER, :9 AS MAIN_CH_SEND_YN, :10 AS DOWNLOAD_STATUS_YN, 
                       :11 AS DOWNLOAD_URL, :12 AS SAVE_TIME, :13 AS REG_DT, :14 AS WRITER, 
                       :15 AS "KEY", :16 AS TELEGRAM_URL, :17 AS MKT_TP 
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
                    dest.MKT_TP = src.MKT_TP
            WHEN NOT MATCHED THEN
                INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                        ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                        DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP)
                VALUES (src.REPORT_ID, src.SEC_FIRM_ORDER, src.ARTICLE_BOARD_ORDER, src.FIRM_NM, 
                        src.ATTACH_URL, src.ARTICLE_TITLE, src.ARTICLE_URL, src.SEND_USER, 
                        src.MAIN_CH_SEND_YN, src.DOWNLOAD_STATUS_YN, src.DOWNLOAD_URL, 
                        src.SAVE_TIME, src.REG_DT, src.WRITER, src."KEY", src.TELEGRAM_URL, src.MKT_TP)
        """

        # sqlite3.Row를 직접 사용해 파라미터 준비 최적화
        params = [
            (
                row['report_id'],
                row['SEC_FIRM_ORDER'] or 0,
                row['ARTICLE_BOARD_ORDER'] or 0,
                row['FIRM_NM'] or ' ',
                row['ATTACH_URL'] or ' ',
                row['ARTICLE_TITLE'] or ' ',
                row['ARTICLE_URL'] or ' ',
                row['SEND_USER'] or ' ',
                row['MAIN_CH_SEND_YN'] or ' ',
                row['DOWNLOAD_STATUS_YN'] or ' ',
                row['DOWNLOAD_URL'] or ' ',
                row['SAVE_TIME'] or ' ',
                row['REG_DT'] or ' ',
                row['WRITER'] or ' ',
                row['KEY'] or ' ',
                row['TELEGRAM_URL'] or ' ',
                row['MKT_TP'] or ' '
            )
            for row in new_data
        ]

        try:
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

    def run_periodically(self, interval_minutes: int = 60):
        print("초기 전체 동기화 수행 중...")
        self.sync_to_oracle(full_sync=False)
        while False:
            print(f"{datetime.now()}에 증분 동기화 시작")
            self.sync_to_oracle(full_sync=False)
            print(f"{interval_minutes}분 후 다음 동기화...")
            time.sleep(interval_minutes * 60)

    def remove_excess_oracle_records(self):
        # SQLite와 Oracle의 REPORT_ID 집합 가져오기
        self.sqlite_cursor.execute("SELECT REPORT_ID FROM data_main_daily_send")
        sqlite_ids = set(row[0] for row in self.sqlite_cursor.fetchall())
        
        self.oracle_cursor.execute("SELECT REPORT_ID FROM DATA_MAIN_DAILY_SEND")
        oracle_ids = set(row[0] for row in self.oracle_cursor.fetchall())
        
        # Oracle에만 존재하는 REPORT_ID 찾기
        excess_ids = oracle_ids - sqlite_ids
        
        if excess_ids:
            print(f"Oracle에만 존재하는 REPORT_ID: {excess_ids}")
            delete_query = "DELETE FROM DATA_MAIN_DAILY_SEND WHERE REPORT_ID = :1"
            try:
                self.oracle_cursor.executemany(delete_query, [(report_id,) for report_id in excess_ids])
                self.oracle_conn.commit()
                print(f"삭제 완료: {len(excess_ids)}개 레코드 삭제됨.")
                
                # 삭제 후 레코드 수 확인
                new_sqlite_count, new_oracle_count = self.get_counts()
                print(f"삭제 후: SQLite3 레코드 수: {new_sqlite_count}, Oracle 레코드 수: {new_oracle_count}")
            except oracledb.DatabaseError as e:
                self.oracle_conn.rollback()
                print(f"삭제 중 오류: {e}")
        else:
            print("Oracle에 SQLite에 없는 REPORT_ID가 없습니다.")

# 사용 예시
if __name__ == "__main__":
    sync = DatabaseSync()
    try:
        sync.sync_to_oracle(full_sync=False)  # 기존 동기화 실행
        sync.remove_excess_oracle_records()   # 초과 레코드 삭제
    except Exception as e:
            print(f"오류 발생: {e}")
            exit(1)  # 비정상 종료 시 상태 코드 1 반환
    except KeyboardInterrupt:
        print("사용자에 의해 중단됨.")
    finally:
        sync.close_connections()
            
# if __name__ == "__main__":
#     sync = DatabaseSync()
#     try:
#         sync.run_periodically(interval_minutes=60)
#     except KeyboardInterrupt:
#         print("사용자에 의해 동기화 중단.")
#     finally:
#         sync.close_connections()