import sqlite3
import oracledb
import os
from dotenv import load_dotenv
import time

load_dotenv()

def sync_fast():
    # 1. Oracle 연결 (Thin 모드)
    conn = oracledb.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        dsn=os.getenv('DB_DSN'),
        config_dir=os.getenv('WALLET_LOCATION'),
        wallet_location=os.getenv('WALLET_LOCATION'),
        wallet_password=os.getenv('WALLET_PASSWORD')
    )
    cursor = conn.cursor()
    
    # [30초 복구의 핵심] REPORT_ID에 인덱스가 없으면 30만 건은 절대 30초 안에 못 들어갑니다.
    print("[1/3] 인덱스 확인 및 생성 중...")
    try:
        cursor.execute("CREATE UNIQUE INDEX PK_REPORT_ID_FAST ON DATA_MAIN_DAILY_SEND(REPORT_ID)")
        conn.commit()
        print(" -> 인덱스 생성 완료.")
    except:
        print(" -> 인덱스 이미 존재함.")

    # 2. SQLite 데이터 읽기 (원래 방식 그대로 fetchall)
    print("[2/3] SQLite 데이터 읽는 중...")
    sqlite_conn = sqlite3.connect(os.getenv('SQLITE_DB_PATH'))
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute("""
        SELECT report_id, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
               ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
               DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
               GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL 
        FROM data_main_daily_send
    """)
    rows = sqlite_cursor.fetchall()
    
    # 3. 원래 가장 빨랐던 2a2a579 버전의 MERGE 쿼리
    sql = """
        MERGE INTO DATA_MAIN_DAILY_SEND dest
        USING (
            SELECT :1 AS REPORT_ID, :2 AS SEC_FIRM_ORDER, :3 AS ARTICLE_BOARD_ORDER, 
                   :4 AS FIRM_NM, :5 AS ATTACH_URL, :6 AS ARTICLE_TITLE, :7 AS ARTICLE_URL, 
                   :8 AS SEND_USER, :9 AS MAIN_CH_SEND_YN, :10 AS DOWNLOAD_STATUS_YN, 
                   :11 AS DOWNLOAD_URL, :12 AS SAVE_TIME, :13 AS REG_DT, :14 AS WRITER, 
                   :15 AS "KEY", :16 AS TELEGRAM_URL, :17 AS MKT_TP, 
                   :18 AS GEMINI_SUMMARY, :19 AS SUMMARY_TIME, :20 AS SUMMARY_MODEL 
            FROM dual
        ) src ON (dest.REPORT_ID = src.REPORT_ID)
        WHEN MATCHED THEN
            UPDATE SET 
                dest.SEC_FIRM_ORDER=src.SEC_FIRM_ORDER, dest.ARTICLE_BOARD_ORDER=src.ARTICLE_BOARD_ORDER,
                dest.FIRM_NM=src.FIRM_NM, dest.ATTACH_URL=src.ATTACH_URL, dest.ARTICLE_TITLE=src.ARTICLE_TITLE,
                dest.ARTICLE_URL=src.ARTICLE_URL, dest.SEND_USER=src.SEND_USER, dest.MAIN_CH_SEND_YN=src.MAIN_CH_SEND_YN,
                dest.DOWNLOAD_STATUS_YN=src.DOWNLOAD_STATUS_YN, dest.DOWNLOAD_URL=src.DOWNLOAD_URL,
                dest.SAVE_TIME=src.SAVE_TIME, dest.REG_DT=src.REG_DT, dest.WRITER=src.WRITER,
                dest."KEY"=src."KEY", dest.TELEGRAM_URL=src.TELEGRAM_URL, dest.MKT_TP=src.MKT_TP,
                dest.GEMINI_SUMMARY=src.GEMINI_SUMMARY, dest.SUMMARY_TIME=src.SUMMARY_TIME, 
                dest.SUMMARY_MODEL=src.SUMMARY_MODEL
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
    
    params = [
        (
            r[0], r[1] or 0, r[2] or 0, r[3] or ' ', r[4] or ' ', r[5] or ' ', r[6] or ' ',
            r[7] or ' ', r[8] or ' ', r[9] or ' ', r[10] or ' ', r[11] or ' ', r[12] or ' ',
            r[13] or ' ', r[14] or ' ', r[15] or ' ', r[16] or ' ', r[17] or ' ', r[18] or ' ', r[19] or ' '
        ) for r in rows
    ]
    
    print(f"[3/3] Oracle 전송 시작 ({len(rows)}건)...")
    start = time.time()
    cursor.executemany(sql, params, batcherrors=True)
    conn.commit()
    print(f"[+] 완료! 소요시간: {time.time()-start:.2f}초")

    cursor.close()
    conn.close()
    sqlite_conn.close()

if __name__ == "__main__":
    sync_fast()
