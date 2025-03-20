import re

def extract_sql(input_str):
    pattern = r'^```sql\s*(.*?)\s*```'
    match = re.search(pattern, input_str, flags=re.DOTALL)
    if match:
        return match.group(1).strip()
    return None

def main():
    test_cases = [
        {
            "name": "简单SQL",
            "input": "```sql SELECT * FROM users```",
            "expected": "SELECT * FROM users"
        },
        {
            "name": "带空格的简单SQL",
            "input": "```sql   UPDATE products SET price = 100   ```",
            "expected": "UPDATE products SET price = 100"
        },
        {
            "name": "多行SQL",
            "input": "```sql\nINSERT INTO orders (id, user_id)\nVALUES (1, 'Alice')\n```",
            "expected": "INSERT INTO orders (id, user_id)\nVALUES (1, 'Alice')"
        },
        {
            "name": "复杂换行SQL",
            "input": "```sql\n\nDELETE FROM logs\nWHERE timestamp < '2023-01-01'\nAND status = 'completed'\n\n```",
            "expected": "\nDELETE FROM logs\nWHERE timestamp < '2023-01-01'\nAND status = 'completed'"
        },
        {
            "name": "无效输入（无sql标记）",
            "input": "```plain 这不是SQL```",
            "expected": None
        }
    ]

    for case in test_cases:
        result = extract_sql(case["input"])
        passed = (result == case["expected"])
        print(f"用例 '{case['name']}' 的结果：{'✅ 通过' if passed else '❌ 失败'}")
        print(f"输入：\n{case['input']}")
        print(f"期望输出：\n{case['expected']!r}")
        print(f"实际输出：\n{result!r}\n{'='*50}\n")

if __name__ == "__main__":
    main()
