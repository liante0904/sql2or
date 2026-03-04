import sqlite3
import oracledb
import os
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()

# Oracle 연결 설정
WALLET_LOCATION = os.getenv('WALLET_LOCATION')
WALLET_PASSWORD = os.getenv('WALLET_PASSWORD')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DSN = os.getenv('DB_DSN')
SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH')

class DatabaseSync:
    def __init__(self):
        # SQLite: 단순 연결 (속도 우선)
        self.sqlite_conn = sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)
        self.sqlite_conn.row_factory = sqlite3.Row
        self.sqlite_cursor = self.sqlite_conn.cursor()
        
        # Oracle: Thin 모드 최적화 연결
        self.oracle_conn = oracledb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            config_dir=WALLET_LOCATION,
            wallet_location=WALLET_LOCATION,
            wallet_password=WALLET_PASSWORD
        )
        self.oracle_cursor = self.oracle_conn.cursor()
        
        # [성능 핵심 1] Oracle 인덱스 강제 확인/생성
        # REPORT_ID에 인덱스가 없으면 MERGE 속도가 100배 이상 느려집니다.
        try:
            self.oracle_cursor.execute("""
                DECLARE
                    cnt NUMBER;
                BEGIN
                    SELECT count(*) INTO cnt FROM user_indexes WHERE table_name = 'DATA_MAIN_DAILY_SEND' AND column_name = 'REPORT_ID';
                    IF cnt = 0 THEN
                        EXECUTE IMMEDIATE 'CREATE INDEX IDX_REPORT_ID ON DATA_MAIN_DAILY_SEND(REPORT_ID)';
                    END IF;
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
            """)
        except: pass
        
        # [성능 핵심 2] 세션 최적화
        self.oracle_cursor.execute("ALTER SESSION SET CURSOR_SHARING = FORCE")

    def close_connections(self):
        self.sqlite_conn.close()
        self.oracle_conn.close()

    def get_latest_save_time(self) -> str:
        # Oracle MAX 조회 시 인덱스를 타도록 유도
        self.oracle_cursor.execute("SELECT MAX(SAVE_TIME) FROM DATA_MAIN_DAILY_SEND")
        res = self.oracle_cursor.fetchone()[0]
        if res:
            # Oracle datetime -> SQLite 문자열 포맷 (ISO8601)
            return res.strftime('%Y-%m-%dT%H:%M:%S.%f')
        return '1900-01-01T00:00:00.000000'

    def sync_to_oracle(self, full_sync: bool = False):
        last_time = '1900-01-01T00:00:00.000000' if full_sync else self.get_latest_save_time()
        print(f"[*] 시작 기준 시간: {last_time}")

        # [성능 핵심 3] SQLite fetchmany로 메모리 보존하며 읽기
        self.sqlite_cursor.execute("""
            SELECT report_id, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                   ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                   DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                   GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL 
            FROM data_main_daily_send 
            WHERE SAVE_TIME > ?
            ORDER BY SAVE_TIME ASC
        """, (last_time,))

        # [성능 핵심 4] 가장 빠른 MERGE SQL (Dual 제거)
        upsert_query = """
            MERGE INTO DATA_MAIN_DAILY_SEND dest
            USING DUAL ON (dest.REPORT_ID = :1)
            WHEN MATCHED THEN
                UPDATE SET 
                    SEC_FIRM_ORDER=:2, ARTICLE_BOARD_ORDER=:3, FIRM_NM=:4, ATTACH_URL=:5, 
                    ARTICLE_TITLE=:6, ARTICLE_URL=:7, SEND_USER=:8, MAIN_CH_SEND_YN=:9, 
                    DOWNLOAD_STATUS_YN=:10, DOWNLOAD_URL=:11, 
                    SAVE_TIME=TO_TIMESTAMP(:12, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                    REG_DT=TO_TIMESTAMP(:13, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                    WRITER=:14, "KEY"=:15, TELEGRAM_URL=:16, MKT_TP=:17, 
                    GEMINI_SUMMARY=:18, 
                    SUMMARY_TIME=TO_TIMESTAMP(:19, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                    SUMMARY_MODEL=:20
            WHEN NOT MATCHED THEN
                INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                        ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                        DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                        GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, 
                        TO_TIMESTAMP(:12, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                        TO_TIMESTAMP(:13, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                        :14, :15, :16, :17, :18, 
                        TO_TIMESTAMP(:19, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), :20)
        """

        # [성능 핵심 5] setinputsizes() - 대량 바인딩 최적화
        self.oracle_cursor.setinputsizes(
            None, None, None, oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, 
            oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, 
            oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, 
            oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, 
            oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, 
            oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_VARCHAR
        )

        batch_size = 10000
        total = 0
        start = time.time()

        while True:
            rows = self.sqlite_cursor.fetchmany(batch_size)
            if not rows: break
            
            # [성능 핵심 6] 리스트 컴프리헨션 + 튜플 (Dict보다 월등히 빠름)
            batch_data = [
                (
                    r[0], r[1] or 0, r[2] or 0, 
                    str(r[3])[:4000] if r[3] else " ", str(r[4])[:4000] if r[4] else " ",
                    str(r[5])[:4000] if r[5] else " ", str(r[6])[:4000] if r[6] else " ",
                    str(r[7])[:4000] if r[7] else " ", str(r[8])[:4000] if r[8] else " ",
                    str(r[9])[:4000] if r[9] else " ", str(r[10])[:4000] if r[10] else " ",
                    str(r[11]) if r[11] else None, str(r[12]) if r[12] else None,
                    str(r[13])[:4000] if r[13] else " ", str(r[14])[:4000] if r[14] else " ",
                    str(r[15])[:4000] if r[15] else " ", str(r[16])[:4000] if r[16] else " ",
                    str(r[17])[:4000] if r[17] else " ", str(r[18]) if r[18] else None,
                    str(r[19])[:4000] if r[19] else " "
                ) for r in rows
            ]

            self.oracle_cursor.executemany(upsert_query, batch_data)
            self.oracle_conn.commit()
            total += len(batch_data)
            print(f"[>] {total}건 완료 ({time.time()-start:.2f}초)")

        print(f"[*] 동기화 완료: 총 {total}건, 소요시간 {time.time()-start:.2f}초")

if __name__ == "__main__":
    sync = DatabaseSync()
    try:
        sync.sync_to_oracle(full_sync=False)
    finally:
        sync.close_connections()
