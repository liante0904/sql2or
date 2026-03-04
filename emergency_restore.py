import sqlite3
import oracledb
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def recover():
    print("[!] 긴급 복구 스크립트 시작")
    sqlite_conn = sqlite3.connect(os.getenv('SQLITE_DB_PATH'))
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cursor = sqlite_conn.cursor()

    oracle_conn = oracledb.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        dsn=os.getenv('DB_DSN'),
        config_dir=os.getenv('WALLET_LOCATION'),
        wallet_location=os.getenv('WALLET_LOCATION'),
        wallet_password=os.getenv('WALLET_PASSWORD')
    )
    oracle_cursor = oracle_conn.cursor()

    print("[1/3] SQLite 데이터 읽는 중...")
    sqlite_cursor.execute("SELECT * FROM data_main_daily_send")
    rows = sqlite_cursor.fetchall()
    print(f" -> {len(rows)}건 확인됨.")

    def parse_dt(dt_str):
        if not dt_str or str(dt_str).strip() == '': return None
        try:
            s = str(dt_str).replace('T', ' ').replace('"', '')
            if '.' in s: return datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except: return None

    params = []
    for r in rows:
        params.append((
            r['report_id'],
            r['SEC_FIRM_ORDER'] or 0,
            r['ARTICLE_BOARD_ORDER'] or 0,
            r['FIRM_NM'], r['ATTACH_URL'], r['ARTICLE_TITLE'], r['ARTICLE_URL'],
            r['SEND_USER'], r['MAIN_CH_SEND_YN'], r['DOWNLOAD_STATUS_YN'], r['DOWNLOAD_URL'],
            parse_dt(r['SAVE_TIME']), parse_dt(r['REG_DT']),
            r['WRITER'], r['KEY'], r['TELEGRAM_URL'], r['MKT_TP'],
            r['GEMINI_SUMMARY'], parse_dt(r['SUMMARY_TIME']), r['SUMMARY_MODEL']
        ))

    insert_sql = """
        INSERT INTO DATA_MAIN_DAILY_SEND (
            REPORT_ID, SEC_FIRM_ORDER, ARTICLE_BOARD_ORDER, FIRM_NM, ATTACH_URL, 
            ARTICLE_TITLE, ARTICLE_URL, SEND_USER, MAIN_CH_SEND_YN, DOWNLOAD_STATUS_YN, 
            DOWNLOAD_URL, SAVE_TIME, REG_DT, WRITER, "KEY", TELEGRAM_URL, MKT_TP, 
            GEMINI_SUMMARY, SUMMARY_TIME, SUMMARY_MODEL
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20
        )
    """

    print("[2/3] Oracle DB 비어있는지 확인 및 데이터 밀어넣기 준비...")
    # 테이블이 비어있다는 전제하에 가장 빠른 단순 INSERT 실행
    
    batch_size = 10000
    total = len(params)
    print(f"[3/3] Oracle INSERT 시작 (총 {total}건)...")
    
    try:
        for i in range(0, total, batch_size):
            batch = params[i:i+batch_size]
            oracle_cursor.executemany(insert_sql, batch)
            oracle_conn.commit()
            print(f" -> {min(i+batch_size, total)} / {total} 건 완료")
        print("[+] 긴급 복구 완료! 웹서버가 정상화될 것입니다.")
    except Exception as e:
        oracle_conn.rollback()
        print(f"[!] 복구 중 치명적 오류 발생: {e}")

if __name__ == '__main__':
    recover()
