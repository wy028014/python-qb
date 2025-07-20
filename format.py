from datetime import datetime
from os import path, listdir
import re
from tqdm import tqdm


def format_date(date_str):
    """格式化日期"""
    try:
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    except ValueError:
        return date_str


def format_time(time_str):
    """格式化时间"""
    try:
        return datetime.strptime(time_str, '%H%M').strftime('%H:%M:%S')
    except ValueError:
        return time_str


def format_datetime(datetime_str):
    """格式化日期时间"""
    try:
        return datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime_str


def format_price(price_str):
    """格式化价格"""
    try:
        return float(price_str[:-1] + '.' + price_str[-1:])
    except (ValueError, IndexError):
        return price_str


def is_line_formatted(line):
    fields = line.strip().split(',')
    if len(fields) >= 17:
        try:
            datetime.strptime(fields[3], '%Y-%m-%d')
            datetime.strptime(fields[4], '%H:%M:%S')
            datetime.strptime(fields[15], '%Y-%m-%d %H:%M:%S.%f')
            float(fields[16])
            return True
        except ValueError:
            return False
    return False


def format_line(line):
    fields = line.strip().split(',')
    if len(fields) >= 17:
        fields[3] = format_date(fields[3])
        fields[4] = format_time(fields[4])
        fields[6] = fields[6].replace(' ', '')
        fields[7] = fields[7].replace(' ', '')
        fields[15] = format_datetime(fields[15])
        fields[16] = str(format_price(fields[16]))
    return ','.join(fields)


# 第一步：读取所有的txt文件名
local_dir = path.join("C:\\", "Personal", "Outputs", "格式化合并")
txt_files = [file for file in listdir(local_dir) if file.endswith(".txt")]

# 第二步：梳理出一共有多少种YYYYMMDD
date_pattern = re.compile(r'(\d{8})')
date_groups = {}
for file in txt_files:
    match = date_pattern.search(file)
    if match:
        date_str = match.group(1)
        if date_str not in date_groups:
            date_groups[date_str] = []
        date_groups[date_str].append(file)

# 第三步：按日期种类和文件个数进行处理
for index, date_str in enumerate(date_groups):
    date_lines = set()
    files = date_groups[date_str]
    for file in tqdm(files, desc=f"处理 {index + 1} / {len(date_groups)} 个 {date_str} 下的文件", unit="个"):
        file_path = path.join(local_dir, file)
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if is_line_formatted(first_line):
                # 如果第一行已经格式化，直接添加到结果集
                date_lines.add(first_line.strip())
                date_lines.update(f.read().splitlines())
            else:
                # 否则，格式化每一行
                formatted_lines = (format_line(line) for line in [first_line] + f.readlines())
                date_lines.update(formatted_lines)

    output_file = path.join(local_dir, f"{date_str}_{len(date_lines)}.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(date_lines))
    