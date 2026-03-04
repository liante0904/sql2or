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
        self.sqlite_conn = sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)
        self.sqlite_conn.row_factory = sqlite3.Row
        self.sqlite_cursor = self.sqlite_conn.cursor()
        
        self.oracle_conn = oracledb.connect(
            user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN,
            config_dir=WALLET_LOCATION, wallet_location=WALLET_LOCATION, wallet_password=WALLET_PASSWORD
        )
        self.oracle_cursor = self.oracle_conn.cursor()
        
        # [핵심] 인덱스 확인 - 30만 건 처리에 필수
        try:
            self.oracle_cursor.execute("CREATE INDEX IDX_REPORT_ID_SYNC ON DATA_MAIN_DAILY_SEND(REPORT_ID)")
            self.oracle_conn.commit()
        except: pass # 이미 있으면 무시
        
        self.oracle_cursor.execute("ALTER SESSION SET CURSOR_SHARING = FORCE")

    def close_connections(self):
        self.sqlite_conn.close()
        self.oracle_conn.close()

    def get_latest_save_time(self) -> str:
        self.oracle_cursor.execute("SELECT MAX(SAVE_TIME) FROM DATA_MAIN_DAILY_SEND")
        res = self.oracle_cursor.fetchone()[0]
        return res.strftime('%Y-%m-%dT%H:%M:%S.%f') if res else '1900-01-01T00:00:00.000000'

    def sync_to_oracle(self, full_sync: bool = False):
        last_time = '1900-01-01T00:00:00.000000' if full_sync else self.get_latest_save_time()
        print(f"[*] 시작 기준: {last_time}")

        # SQLite에서 필요한 데이터만 빠르게 추출
        self.sqlite_cursor.execute("""
            SELECT report_id, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                   ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                   DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                   GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL 
            FROM data_main_daily_send 
            WHERE SAVE_TIME > ?
            ORDER BY SAVE_TIME ASC
        """, (last_time,))

        # [핵심] 이름 기반 바인딩으로 DPY-4009 에러 해결 및 재사용성 극대화
        upsert_query = """
            MERGE INTO DATA_MAIN_DAILY_SEND dest
            USING DUAL ON (dest.REPORT_ID = :rid)
            WHEN MATCHED THEN
                UPDATE SET 
                    SEC_FIRM_ORDER=:sfo, ARTICLE_BOARD_ORDER=:abo, FIRM_NM=:fnm, ATTACH_URL=:aurl, 
                    ARTICLE_TITLE=:atit, ARTICLE_URL=:artu, SEND_USER=:susr, MAIN_CH_SEND_YN=:mcy, 
                    DOWNLOAD_STATUS_YN=:dsy, DOWNLOAD_URL=:durl, 
                    SAVE_TIME=TO_TIMESTAMP(:stime, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                    REG_DT=TO_TIMESTAMP(:rdt, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                    WRITER=:wtr, "KEY"=:key, TELEGRAM_URL=:turl, MKT_TP=:mtp, 
                    GEMINI_SUMMARY=:gsum, 
                    SUMMARY_TIME=TO_TIMESTAMP(:sumt, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                    SUMMARY_MODEL=:summ
            WHEN NOT MATCHED THEN
                INSERT (REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
                        ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
                        DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
                        GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL)
                VALUES (:rid, :sfo, :abo, :fnm, :aurl, :atit, :artu, :susr, :mcy, :dsy, :durl, 
                        TO_TIMESTAMP(:stime, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                        TO_TIMESTAMP(:rdt, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), 
                        :wtr, :key, :turl, :mtp, :gsum, 
                        TO_TIMESTAMP(:sumt, 'YYYY-MM-DD"T"HH24:MI:SS.FF'), :summ)
        """

        # 데이터 타입 고정으로 바인딩 속도 향상
        self.oracle_cursor.setinputsizes(
            rid=oracledb.DB_TYPE_VARCHAR, sfo=oracledb.DB_TYPE_NUMBER, abo=oracledb.DB_TYPE_NUMBER,
            fnm=oracledb.DB_TYPE_VARCHAR, aurl=oracledb.DB_TYPE_VARCHAR, atit=oracledb.DB_TYPE_VARCHAR,
            artu=oracledb.DB_TYPE_VARCHAR, susr=oracledb.DB_TYPE_VARCHAR, mcy=oracledb.DB_TYPE_VARCHAR,
            dsy=oracledb.DB_TYPE_VARCHAR, durl=oracledb.DB_TYPE_VARCHAR, stime=oracledb.DB_TYPE_VARCHAR,
            rdt=oracledb.DB_TYPE_VARCHAR, wtr=oracledb.DB_TYPE_VARCHAR, key=oracledb.DB_TYPE_VARCHAR,
            turl=oracledb.DB_TYPE_VARCHAR, mtp=oracledb.DB_TYPE_VARCHAR, gsum=oracledb.DB_TYPE_VARCHAR,
            sumt=oracledb.DB_TYPE_VARCHAR, summ=oracledb.DB_TYPE_VARCHAR
        )

        batch_size = 10000
        total = 0
        start = time.time()

        while True:
            rows = self.sqlite_cursor.fetchmany(batch_size)
            if not rows: break
            
            batch_data = [
                {
                    "rid": r[0], "sfo": r[1] or 0, "abo": r[2] or 0, 
                    "fnm": str(r[3])[:4000] if r[3] else " ", "aurl": str(r[4])[:4000] if r[4] else " ",
                    "atit": str(r[5])[:4000] if r[5] else " ", "artu": str(r[6])[:4000] if r[6] else " ",
                    "susr": str(r[7])[:4000] if r[7] else " ", "mcy": str(r[8])[:4000] if r[8] else " ",
                    "dsy": str(r[9])[:4000] if r[9] else " ", "durl": str(r[10])[:4000] if r[10] else " ",
                    "stime": str(r[11]) if r[11] else None, "rdt": str(r[12]) if r[12] else None,
                    "wtr": str(r[13])[:4000] if r[13] else " ", "key": str(r[14])[:4000] if r[14] else " ",
                    "turl": str(r[15])[:4000] if r[15] else " ", "mtp": str(r[16])[:4000] if r[16] else " ",
                    "gsum": str(r[17])[:4000] if r[17] else " ", "sumt": str(r[18]) if r[18] else None,
                    "summ": str(r[19])[:4000] if r[19] else " "
                } for r in rows
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
