from datetime import datetime
from os import path, listdir
from pymysql import connect, Error
from sys import exit
from time import time
from tqdm import tqdm

def log(message, print_log=True):
    """日志记录函数"""
    if print_log:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {message}")


def connect_db(print_log):
    """连接数据库并返回连接和游标"""
    try:
        conn = connect(
            host='10.3.32.239',
            port=3306,
            user="wangye",
            password="Wy028014.",
            database="情报",
            charset="utf8mb4",
            local_infile=1
        )
        cursor = conn.cursor()
        # log(f"连接数据库 10.3.32.154:3306 成功", print_log)
        return conn, cursor
    except Error as e:
        log(f"连接数据库失败: {e}", print_log)
        return None, None


def load_data_into_temp_table(conn, cursor, file_path, print_log):
    """使用LOAD DATA INFILE将数据加载到临时表"""
    try:
        start_time = time()
        cursor.execute("TRUNCATE TABLE ftp_temp;")
        conn.commit()
        load_data_query = """
        LOAD DATA LOCAL INFILE %s
        INTO TABLE `ftp_temp`
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        (姓名, 证件类型, 证件编号, 乘车日期, 乘车时间, 车次, 发站, 到站, 车厢号, 座位号, 席别, 票号, 票种, 售票处, 窗口, 售票时间, 票价);
        """
        cursor.execute(load_data_query, (file_path,))
        conn.commit()
        cursor.execute(f"SELECT COUNT(*) FROM ftp_temp;")
        count = cursor.fetchone()[0]
        end_time = time()
        file_name = file_path.split("\\")[-1][4:16]
        log(f"{file_name[0:4]}-{file_name[4:6]}-{file_name[6:8]} {file_name[8:10]}:{file_name[10:12]} 成功加载数据 {count} 条到临时表, 耗时: {end_time - start_time:.2f} 秒", print_log)
        return True
    except Error as e:
        log(f"加载文件到临时表失败: {e}", print_log)
        return False


def process_and_insert_data(conn, cursor, file_path, print_log):
    """处理数据并插入目标表"""
    try:
        start_time = time()
        cursor.execute(f"SELECT COUNT(*) FROM ftp_data;")
        start_count = cursor.fetchone()[0]
        insert_query = """
        INSERT INTO `情报`.`ftp_data`
        (姓名, 证件类型, 证件编号, 乘车日期, 乘车时间, 车次, 发站, 到站, 车厢号, 座位号, 席别, 票号, 票种, 售票处, 窗口, 售票时间, 票价)
        SELECT
            姓名, 证件类型, 证件编号,
            STR_TO_DATE(乘车日期, '%Y%m%d') AS 乘车日期,
            STR_TO_DATE(乘车时间, '%H%i') AS 乘车时间,
            车次, REPLACE(发站, ' ', ''), REPLACE(到站, ' ', ''), 车厢号, 座位号, 席别, 票号, 票种, 售票处, 窗口,
            STR_TO_DATE(SUBSTRING(售票时间, 1, 19), '%Y/%m/%d %H:%i:%S') AS 售票时间,
            CAST(票价 AS SIGNED) / 10 AS 票价
        FROM `ftp_temp`
        WHERE
            `乘车日期` IS NOT NULL
        ON DUPLICATE KEY UPDATE `更新时间` = CURRENT_TIMESTAMP();
        """
        cursor.execute(insert_query)
        cursor.execute(f"SELECT COUNT(*) FROM ftp_data;")
        end_count = cursor.fetchone()[0]
        update_query = """
        INSERT INTO `情报`.`ftp_update`
        (更新文件名, 更新数量)
        VALUES
        (%s, %s)
        """
        cursor.execute(update_query, (file_path.split("\\")[-1], end_count - start_count,))
        conn.commit()
        end_time = time()
        log(f"成功从临时表插入 {end_count - start_count} 条数据到目标表, 耗时: {end_time - start_time:.2f} 秒", print_log)
        return True
    except Error as e:
        log(f"处理数据并插入目标表失败: {e}", print_log)
        return False


def load_txt_to_mariadb(txt_file_path, print_log=True):
    """将本地txt文件加载到MariaDB数据库"""
    log(f"开始处理文件: {txt_file_path.split("\\")[-1]}", print_log)
    conn, cursor = connect_db(print_log)
    if conn and cursor:
        if load_data_into_temp_table(conn, cursor, txt_file_path, print_log):
            process_and_insert_data(conn, cursor, txt_file_path, print_log)
        else:
            log(f"文件 {txt_file_path} 加载到临时表失败", print_log)
        cursor.close()
        conn.close()


if __name__ == "__main__":
    local_dir = path.join("C:\\", "Personal", "Outputs", "FTP数据")
    if not path.exists(local_dir):
        print(f"目录不存在: {local_dir}")
        exit(1)
    txt_files = [file for file in listdir(
        local_dir) if file.endswith(".txt")]
    if not txt_files:
        print(f"目录下没有txt文件: {local_dir}")
        exit(1)
    for file in tqdm(txt_files, desc=f"文件", unit=f"个"):
        txt_file_path = path.join(local_dir, file)
        with open(txt_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        cleaned_content = content.replace('\\', '')
        with open(txt_file_path, 'w', encoding='utf-8') as f:
            f.write(cleaned_content)
        load_txt_to_mariadb(txt_file_path, print_log=True)
