from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

SOURCE_CONTENT = """Employee_id
10
11
12
13
14
15
Salary
10000
20000
30000
40000
45000
50000
"""

def extract_first3_records():
    lines = [l.strip() for l in SOURCE_CONTENT.strip().splitlines()]
    salary_idx = lines.index("Salary")
    employee_ids = lines[1:salary_idx]
    salaries = lines[salary_idx + 1:]

    first3_ids = employee_ids[:3]
    first3_salaries = salaries[:3]

    output_lines = ["employee_id"] + first3_ids + ["Salary"] + first3_salaries
    output_text = "\n".join(output_lines)

    print("=== OUTPUT FILE ===")
    print(output_text)

with DAG(
    dag_id="load_first3_records",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="extract_and_print_first3",
        python_callable=extract_first3_records,
    )
