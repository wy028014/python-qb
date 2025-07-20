
from datetime import datetime
from ftplib import FTP, error_perm
from time import time, sleep
from tqdm import tqdm
import os
import pymysql
import schedule

def log(message, print_log):
    """日志记录函数"""
    if print_log:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {message}")


def connect_db(print_log):
    """连接数据库并返回连接和游标"""
    try:
        conn = pymysql.connect(
            host='10.3.32.239',
            port=3306,
            user="wangye",
            password="Wy028014.",
            database="情报",
            charset="utf8mb4",
            local_infile=1
        )
        cursor = conn.cursor()
        log(f"连接数据库 10.3.32.239:3306 成功", print_log)
        return conn, cursor
    except pymysql.Error as e:
        log(f"连接数据库失败: {e}", print_log)
        return None, None


def execute_query(query, params, print_log, fetch=False, commit=False):
    """执行SQL查询或更新"""
    conn, cursor = connect_db(print_log)
    if conn and cursor:
        try:
            cursor.execute(query, params)
            if fetch:
                result = cursor.fetchall()
                log(f"查询成功, 返回结果 {len(result)} 条", print_log)
                cursor.close()
                conn.close()
                return result
            if commit:
                conn.commit()
                cursor.close()
                conn.close()
                log(f"执行成功", print_log)
                return True
        except pymysql.Error as e:
            log(f"执行失败: {e}", print_log)
            cursor.close()
            conn.close()
    return False


def connect_ftp(print_log):
    """连接FTP服务器"""
    try:
        ftp = FTP(host='10.3.16.197', timeout=30)
        ftp.login(user='htwa', passwd='htwa@123')
        ftp.cwd('haerbin')
        log("连接ftp 10.3.16.197 成功", print_log)
        return ftp
    except Exception as e:
        log(f"连接ftp失败: {e}", print_log)
        return None


def get_ftp_file_list(print_log):
    """获取FTP文件列表"""
    ftp = connect_ftp(print_log)
    if ftp:
        try:
            file_list = ftp.nlst()
            ftp.quit()
            return [f for f in file_list if f.endswith('.txt')]
        except Exception as e:
            log(f"获取文件列表失败: {e}", print_log)
            ftp.quit()
    return []


def download_ftp_file(local_dir, file_name, print_log):
    """下载文件"""
    ftp = connect_ftp(print_log)
    if ftp:
        try:
            os.makedirs(local_dir, exist_ok=True)
            local_path = os.path.join(local_dir, file_name)
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f"RETR {file_name}", f.write)
            with open(local_path, 'r', encoding='utf-8') as f:
                content = f.read()
            cleaned_content = content.replace('\\', '')
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_content)
            # log(f"下载成功: {file_name}", print_log)
            ftp.quit()
            return True
        except (error_perm, IOError) as e:
            log(f"下载失败: {e}", print_log)
        ftp.quit()
    return False


def check_update_data(print_log):
    """检查需要更新的文件"""
    result_list = []
    file_list = get_ftp_file_list(print_log)
    local_dir = os.path.join("C:\\", "Personal", "Outputs", "FTP数据下载20250506")
    txt_files = [file for file in os.listdir(local_dir) if file.endswith(".txt")]
    if file_list:
        for file_name in file_list:
            if file_name not in txt_files:
                result_list.append(file_name)
    if result_list:
        log(f"{len(result_list)} 个文件需要更新", print_log)
        return result_list
    else:
        log("已是最新", print_log)
        return []


# def load_data_into_temp_table(conn, cursor, file_path, print_log):
#     """使用LOAD DATA INFILE将数据加载到临时表"""
#     try:
#         start_time = time()
#         cursor.execute("TRUNCATE TABLE ftp_temp;")
#         conn.commit()
#         load_data_query = """
#         LOAD DATA LOCAL INFILE %s
#         INTO TABLE `ftp_temp`
#         FIELDS TERMINATED BY ','
#         LINES TERMINATED BY '\n'
#         (姓名, 证件类型, 证件编号, 乘车日期, 乘车时间, 车次, 发站, 到站, 车厢号, 座位号, 席别, 票号, 票种, 售票处, 窗口, 售票时间, 票价);
#         """
#         cursor.execute(load_data_query, (file_path,))
#         conn.commit()
#         cursor.execute(f"SELECT COUNT(*) FROM ftp_temp;")
#         count = cursor.fetchone()[0]
#         end_time = time()
#         file_name = file_path.split("\\")[-1][4:16]
#         log(f"{file_name[0:4]}-{file_name[4:6]}-{file_name[6:8]} {file_name[8:10]}:{file_name[10:12]} 成功加载数据 {count} 条到临时表, 耗时: {end_time - start_time:.2f} 秒", print_log)
#         return True
#     except pymysql.Error as e:
#         log(f"加载文件到临时表失败: {e}", print_log)
#         return False


# def process_and_insert_data(conn, cursor, file_path, print_log):
#     """处理数据并插入目标表"""
#     try:
#         start_time = time()
#         cursor.execute(f"SELECT COUNT(*) FROM ftp_data;")
#         start_count = cursor.fetchone()[0]
#         insert_query = """
#         INSERT INTO `情报`.`ftp_data` (姓名,
#         证件类型,
#         证件编号,
#         乘车日期,
#         乘车时间,
#         车次,
#         发站,
#         到站,
#         车厢号,
#         座位号,
#         席别,
#         票号,
#         票种,
#         售票处,
#         窗口,
#         售票时间,
#         票价 
#         ) SELECT
#         姓名,
#         证件类型,
#         证件编号,
#         STR_TO_DATE(乘车日期, '%Y%m%d' ) AS 乘车日期,
#         STR_TO_DATE(乘车时间, '%H%i' ) AS 乘车时间,
#         车次,
#         REPLACE (发站, ' ', '' ),
#         REPLACE (到站, ' ', '' ),
#         车厢号,
#         座位号,
#         席别,
#         票号,
#         票种,
#         售票处,
#         窗口,
#         STR_TO_DATE( SUBSTRING(售票时间, 1, 19 ), '%Y/%m/%d %H:%i:%S' ) AS 售票时间,
#         CAST(CONCAT(SUBSTRING(票价, 1, LENGTH(票价) - 2), '.', SUBSTRING(票价, -2)) AS DECIMAL(10, 1)) AS 票价
#         FROM
#         `ftp_temp` 
#         WHERE
#         `乘车日期` IS NOT NULL 
#         ON DUPLICATE KEY UPDATE `更新时间` = CURRENT_TIMESTAMP ();
#         """
#         cursor.execute(insert_query)
#         cursor.execute(f"SELECT COUNT(*) FROM ftp_data;")
#         end_count = cursor.fetchone()[0]
#         update_query = """
#         INSERT INTO `情报`.`ftp_update`
#         (更新文件名, 更新数量)
#         VALUES
#         (%s, %s)
#         """
#         cursor.execute(update_query, (file_path.split("\\")[-1], end_count - start_count,))
#         conn.commit()
#         end_time = time()
#         log(f"成功从临时表插入 {end_count - start_count} 条数据到目标表, 耗时: {end_time - start_time:.2f} 秒", print_log)
#         return True
#     except pymysql.Error as e:
#         log(f"处理数据并插入目标表失败: {e}", print_log)
#         return False


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
        formatted_file_path = file_path.replace('.txt', '_formatted.txt').replace('下载', '格式化下载')
        with open(formatted_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(formatted_lines))

        # log(f"文件 {file_path} 已格式化并保存为 {formatted_file_path}", print_log)
        return formatted_file_path
    except Exception as e:
        log(f"格式化文件 {file_path} 时出错: {e}", print_log)
        return None

def ftp_into_mariadb(print_log):
    """主要业务逻辑"""
    try:
        file_list = check_update_data(print_log)
        local_dir = os.path.join("C:\\", "Personal", "Outputs", "FTP数据下载20250506")
        if not file_list:
            return
        for file_name in tqdm(file_list, desc=f"下载文件", unit="个"):
            if download_ftp_file(local_dir, file_name, print_log):
                file_path = os.path.join(local_dir, file_name)
                # conn, cursor = connect_db(print_log)
                # if conn and cursor:
                #     if load_data_into_temp_table(conn, cursor, file_path, print_log):
                #         process_and_insert_data(conn, cursor, file_path, print_log)
                #     else:
                #         log(f"文件 {file_name} 加载到临时表失败", print_log)
                #     cursor.close()
                #     conn.close()
                clean_and_format_txt(file_path, print_log)

    except Exception as e:
        log(f"本次执行发生错误: {e}", print_log)


def job():
    """定时任务执行函数"""
    ftp_into_mariadb(False)


if __name__ == "__main__":
    job()
