from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import re

def get_month_from_filename(filename):
    """从文件名中提取年月信息(YYYYMM)和日期对象"""
    # 匹配格式为YYYYMMDD_数量.txt的文件名
    match = re.search(r'(\d{8})_\d+\.txt', filename)
    if match:
        date_str = match.group(1)
        try:
            # 提取年月部分
            month = date_str[:6]
            # 将日期字符串转换为datetime对象
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            return month, date_obj
        except ValueError:
            return None, None
    return None, None

# 输入目录（包含YYYYMMDD_数量.txt文件的目录）
input_dir = Path("C:/Personal/Outputs/格式化合并")
# 输出目录（包含YYYYMMDD_数量.txt文件的目录）
output_dir = Path("C:/Personal/Outputs/格式化合并按月")

# 确保输出目录存在
output_dir.mkdir(parents=True, exist_ok=True)

# 第一步：读取所有的YYYYMMDD_数量.txt文件
txt_files = [f for f in input_dir.iterdir() if re.match(r'\d{8}_\d+\.txt', f.name)]

# 第二步：按年月分组文件
month_groups = {}
for file in txt_files:
    month, date_obj = get_month_from_filename(file.name)
    if month:
        if month not in month_groups:
            month_groups[month] = []
        month_groups[month].append((file, date_obj))

# 第三步：处理每个月的文件集合
for month, files in month_groups.items():
    print(f"处理月份: {month}")
    all_lines = set()  # 使用集合自动去重

    # 读取该月所有文件的内容
    for file, date_obj in tqdm(files, desc=f"读取 {month} 文件", unit="个"):
        try:
            with file.open('r', encoding='utf-8') as f:
                for line in f:
                    # 判断日期是否在4月22日之前
                    if date_obj and date_obj < datetime(date_obj.year, 4, 22):
                        parts = line.strip().split(',')
                        if parts:
                            try:
                                # 将最后一个元素乘以10
                                parts[-1] = str(int(parts[-1]) * 10)
                                line = ','.join(parts) + '\n'
                            except ValueError:
                                pass
                    all_lines.add(line.strip())
        except Exception as e:
            print(f"读取文件 {file.name} 时出错: {e}")

    # 生成输出文件名
    output_file = output_dir / f"{month}_{len(all_lines)}.txt"

    # 写入合并后的文件
    try:
        with output_file.open('w', encoding='utf-8') as f:
            f.write('\n'.join(all_lines))
        print(f"已生成文件: {output_file}, 行数: {len(all_lines)}")
    except Exception as e:
        print(f"写入文件 {output_file} 时出错: {e}")

print("所有月份处理完成!")