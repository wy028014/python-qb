if __name__ == "__main__":
    try:
        with open("smz-20250313000100000.txt", 'r', encoding="utf-8") as file:
            lines = file.readlines()
        original_line_count = len(lines)
        unique_lines = list(dict.fromkeys(lines))
        unique_line_count = len(unique_lines)
        print(f"去重前 {original_line_count}, 去重后 {unique_line_count}")
    except Exception as e:
        print(e)