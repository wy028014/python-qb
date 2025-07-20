import os
from ftplib import FTP, error_perm
import pymysql
import schedule
from datetime import datetime
from time import sleep, time
from tqdm import tqdm
import re

# 配置目录
DOWNLOAD_DIR = os.path.join("C:", "Personal", "Outputs", "FTP数据")
MERGE_DIR = os.path.join("C:", "Personal", "Outputs", "FTP合并数据")

# 数据库配置
DB_CONFIG = {
    'host': '10.3.32.239',
    'port': 3306,
    'user': 'wangye',
    'password': 'Wy028014.',
    'database': '情报',
    'charset': 'utf8mb4',
    'local_infile': 1
}

class DBHandler:
    """数据库操作类"""
    def __init__(self, config):
        self.config = config

    def _connect(self):
        conn = pymysql.connect(**self.config)
        return conn, conn.cursor()

    def execute(self, sql, params=(), fetch=False, commit=False):
        conn, cursor = self._connect()
        result = None
        try:
            cursor.execute(sql, params)
            if commit:
                conn.commit()
            if fetch:
                result = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
        return result

class FTPHandler:
    """FTP 操作类"""
    def __init__(self, host, user, passwd, cwd, timeout=30):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.cwd = cwd
        self.timeout = timeout

    def _connect(self):
        ftp = FTP(host=self.host, timeout=self.timeout)
        ftp.login(user=self.user, passwd=self.passwd)
        ftp.cwd(self.cwd)
        return ftp

    def list_txt_files(self):
        ftp = self._connect()
        files = [f for f in ftp.nlst() if f.endswith('.txt')]
        ftp.quit()
        return files

    def download(self, filename, local_dir):
        ftp = self._connect()
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, filename)
        with open(local_path, 'wb') as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
        ftp.quit()
        return local_path

class Formatter:
    """文本格式化类"""
    @staticmethod
    def format(value: str, kind: str) -> str:
        if kind == 'date':
            try:
                return datetime.strptime(value, '%Y%m%d').strftime('%Y-%m-%d')
            except:
                return value
        if kind == 'time':
            try:
                return datetime.strptime(value, '%H%M').strftime('%H:%M:%S')
            except:
                return value
        if kind == 'datetime':
            try:
                return datetime.strptime(value[:19], '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            except:
                return value
        if kind == 'price':
            try:
                return str(float(value) / 10)
            except:
                return value
        if kind == 'from_to':
            try:
                return value.replace(' ', '')
            except:
                return value
        return value


def merge_by_date(download_dir, merge_dir):
    os.makedirs(merge_dir, exist_ok=True)
    grouped = {}
    for fn in os.listdir(download_dir):
        if not fn.endswith('.txt'): continue
        with open(os.path.join(download_dir, fn), 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 4:
                    date = parts[3]
                    grouped.setdefault(date, set()).add(line.strip())
    for date, lines in grouped.items():
        existing = set()
        out_dir = merge_dir
        os.makedirs(out_dir, exist_ok=True)
        out_fp_base = os.path.join(out_dir, f"{date}")
        if os.path.exists(f"{out_fp_base}.txt"):
            with open(f"{out_fp_base}.txt", 'r', encoding='utf-8') as f:
                existing = set(l.strip() for l in f)
        merged = sorted(existing.union(lines))
        out_fp = f"{out_fp_base}_{len(merged)}.txt"
        with open(out_fp, 'w', encoding='utf-8') as f:
            f.write("\n".join(merged))


def job():
    print(f"{datetime.now()} | 现在开始执行程序...")
    db = DBHandler(DB_CONFIG)
    ftp = FTPHandler('10.3.16.197', 'htwa', 'htwa@123', 'haerbin')

    all_files = ftp.list_txt_files()
    pending = [fn for fn in all_files
               if not db.execute("SELECT id FROM ftp_update WHERE 更新文件名=%s", (fn,), fetch=True)]
    cutoff_date = 20250702
    if not pending:
        print(f"{datetime.now()} | 无新文件")
        return
    date_pattern = re.compile(r'(\d{8})')
    for fn in tqdm(pending, desc="下载文件", unit="个"):
        match = date_pattern.search(fn)
        if match:
            date_str = match.group(1)
            if date_str.isdigit():
                date_num = int(date_str)
                if date_num < cutoff_date:
                    print('continue', fn)
                    continue
        print('work', fn)
        # 下载
        t0 = time()
        local_path = ftp.download(fn, DOWNLOAD_DIR)
        print(f"{datetime.now()} | 下载文件 {fn}，耗时 {time()-t0:.2f} 秒")

        # 格式化
        t1 = time()
        count_fmt = 0
        formatted = []
        with open(local_path, 'r', encoding='utf-8') as f:
            for ln in f:
                parts = ln.strip().split(',')
                if len(parts) >= 17:
                    parts = [p.replace('\\', '') for p in parts]
                    parts[3] = Formatter.format(parts[3], 'date')
                    parts[4] = Formatter.format(parts[4], 'time')
                    parts[6] = Formatter.format(parts[6], 'from_to')
                    parts[7] = Formatter.format(parts[7], 'from_to')
                    parts[15] = Formatter.format(parts[15], 'datetime')
                    parts[16] = Formatter.format(parts[16], 'price')
                    formatted.append(','.join(parts))
                    count_fmt += 1
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(formatted))
        print(f"{datetime.now()} | 格式化文件 {fn}，处理 {count_fmt} 条记录，耗时 {time()-t1:.2f} 秒")

        # 导入到 temp
        t2 = time()
        db.execute("TRUNCATE TABLE ftp_temp;", commit=True)
        db.execute(
            "LOAD DATA LOCAL INFILE %s INTO TABLE ftp_temp FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (姓名,证件类型,证件编号,乘车日期,乘车时间,车次,发站,到站,车厢号,座位号,席别,票号,票种,售票处,窗口,售票时间,票价);",
            (local_path,), commit=True
        )
        temp_cnt = db.execute("SELECT COUNT(*) FROM ftp_temp", fetch=True)[0][0]
        print(f"{datetime.now()} | 导入到 temp 表，共计 {temp_cnt} 条数据，耗时 {time()-t2:.2f} 秒")

        # 插入到 data
        t3 = time()
        before = db.execute("SELECT COUNT(*) FROM ftp_data", fetch=True)[0][0]
        db.execute(
            "INSERT INTO ftp_data SELECT * FROM ftp_temp ON DUPLICATE KEY UPDATE 更新时间=CURRENT_TIMESTAMP();",
            commit=True
        )
        after = db.execute("SELECT COUNT(*) FROM ftp_data", fetch=True)[0][0]
        delta = after - before
        db.execute(
            "INSERT INTO ftp_update (更新文件名,更新数量) VALUES (%s,%s);",
            (fn, delta), commit=True
        )
        print(f"{datetime.now()} | 插入 data 表 {delta} 条记录，耗时 {time()-t3:.2f} 秒")

    # 合并
    merge_by_date(DOWNLOAD_DIR, MERGE_DIR)

# 调度
schedule.every().hour.at(":05").do(job)
schedule.every().hour.at(":35").do(job)

if __name__ == '__main__':
    print(f"{datetime.now()} | 调度启动")
    job()
    while True:
        schedule.run_pending()
        sleep(30)
