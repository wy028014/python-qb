import os
import re
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
        return conn, cursor
    except Error as e:
        log(f"连接数据库失败: {e}", print_log)
        return None, None

def clean_and_format_txt(file_path, print_log):
    """清理并格式化 txt 文件"""
    try:
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        formatted_lines = []
        for line in lines:
            parts = line.strip().split(',')
            if len(parts) >= 17:
                # 移除反斜杠
                cleaned_parts = [part.replace('\\', '') for part in parts]
                
                # 格式化日期和时间
                try:
                    ride_date = datetime.strptime(cleaned_parts[3], '%Y%m%d').strftime('%Y-%m-%d')
                    cleaned_parts[3] = ride_date
                except ValueError:
                    pass

                try:
                    ride_time = datetime.strptime(cleaned_parts[4], '%H%M').strftime('%H:%M:%S')
                    cleaned_parts[4] = ride_time
                except ValueError:
                    pass

                try:
                    sale_time_str = cleaned_parts[15][:19]  # 提取前19个字符
                    sale_time = datetime.strptime(sale_time_str, '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    cleaned_parts[15] = sale_time
                except ValueError:
                    pass

                # 格式化票价
                try:
                    price = float(cleaned_parts[16]) / 10
                    cleaned_parts[16] = str(price)
                except ValueError:
                    pass

                # 移除发站和到站的空格
                if len(cleaned_parts) >= 8:
                    cleaned_parts[6] = cleaned_parts[6].strip()
                    cleaned_parts[7] = cleaned_parts[7].strip()

                formatted_line = ','.join(cleaned_parts)
                formatted_lines.append(formatted_line)

        # 写入格式化后的内容到新文件
        formatted_file_path = file_path.replace('.txt', '_formatted.txt')
        with open(formatted_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(formatted_lines))

        log(f"文件 {file_path} 已格式化并保存为 {formatted_file_path}", print_log)
        return formatted_file_path
    except Exception as e:
        log(f"格式化文件 {file_path} 时出错: {e}", print_log)
        return None

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
        cursor.execute("SELECT COUNT(*) FROM ftp_temp;")
        count = cursor.fetchone()[0]
        # cursor.execute("UPDATE `ftp_temp` SET `票价` = `票价` * 10;")
        # conn.commit()
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
        cursor.execute("SELECT COUNT(*) FROM ftp_data_new;")
        start_count = cursor.fetchone()[0]
        insert_query = """
        INSERT INTO `情报`.`ftp_data_new`
        (姓名, 证件类型, 证件编号, 乘车日期, 乘车时间, 车次, 发站, 到站, 车厢号, 座位号, 席别, 票号, 票种, 售票处, 窗口, 售票时间, 票价)
        SELECT
            姓名, 证件类型, 证件编号,
            乘车日期, 乘车时间,
            车次, 发站, 到站, 车厢号, 座位号, 席别, 票号, 票种, 售票处, 窗口,
            售票时间, 票价
        FROM `ftp_temp`
        WHERE
            `乘车日期` IS NOT NULL
        ON DUPLICATE KEY UPDATE `更新时间` = CURRENT_TIMESTAMP();
        """
        cursor.execute(insert_query)
        cursor.execute("SELECT COUNT(*) FROM ftp_data_new;")
        end_count = cursor.fetchone()[0]
        update_query = """
        INSERT INTO `情报`.`ftp_update_new`
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
    log(f"开始处理文件: {txt_file_path.split('/')[-1]}", print_log)
    conn, cursor = connect_db(print_log)
    if conn and cursor:
        # 先对文件进行清理和格式化
        # formatted_file_path = clean_and_format_txt(txt_file_path, print_log)
        # if formatted_file_path:
        #     if load_data_into_temp_table(conn, cursor, formatted_file_path, print_log):
        #         process_and_insert_data(conn, cursor, formatted_file_path, print_log)
        #     else:
        #         log(f"文件 {formatted_file_path} 加载到临时表失败", print_log)
        # else:
        #     log(f"文件 {txt_file_path} 格式化失败", print_log)
        if load_data_into_temp_table(conn, cursor, txt_file_path, print_log):
            process_and_insert_data(conn, cursor, txt_file_path, print_log)
        else:
            log(f"文件 {txt_file_path} 加载到临时表失败", print_log)
        cursor.close()
        conn.close()

if __name__ == "__main__":
    local_dir = path.join("C:\\", "Personal", "Outputs", "FTP已入库数据")
    if not path.exists(local_dir):
        print(f"目录不存在: {local_dir}")
        exit(1)
    txt_files = [file for file in listdir(local_dir) if file.endswith(".txt")]
    if not txt_files:
        print(f"目录下没有txt文件: {local_dir}")
        exit(1)
    for file in tqdm(txt_files, desc="文件", unit="个"):
        txt_file_path = path.join(local_dir, file)
        load_txt_to_mariadb(txt_file_path, print_log=True)