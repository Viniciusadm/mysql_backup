import os
import subprocess
from datetime import datetime
import mysql.connector
from tqdm import tqdm
import time

def get_env_variable(variable_name, env_file=".env"):
    with open(env_file) as f:
        for line in f:
            if line.startswith(variable_name):
                return line.strip().split('=')[1].strip().strip('"')
    return None

def calculate_database_size(cursor, db_name, ignore_tables):
    query = f"""
    SELECT table_name, 
           ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
    FROM information_schema.tables 
    WHERE table_schema = '{db_name}' 
    """
    if ignore_tables:
        ignore_tables_str = ','.join([f"'{table}'" for table in ignore_tables])
        query += f" AND table_name NOT IN ({ignore_tables_str})"
    
    cursor.execute(query)
    tables = cursor.fetchall()

    total_size = 0
    table_sizes = {}
    for table, size in tables:
        table_sizes[table] = float(size)
        total_size += table_sizes[table]

    return total_size, table_sizes

db_username = get_env_variable("DB_USERNAME")
db_password = get_env_variable("DB_PASSWORD")
db_database = get_env_variable("DB_DATABASE")
db_host = get_env_variable("DB_HOST")
ignore_tables = get_env_variable("DB_IGNORE_TABLES").split(",")

connection = mysql.connector.connect(
    host=db_host,
    user=db_username,
    password=db_password,
    database=db_database
)
cursor = connection.cursor()

total_size, table_sizes = calculate_database_size(cursor, db_database, ignore_tables)
cursor.close()
connection.close()

current_time = datetime.now().strftime("%Y%m%d%H%M%S")
output_file = f"asa_{current_time}.sql"

command = (
    f"mysqldump -u {db_username} -h {db_host} -p{db_password} {db_database} "
    + ' '.join([f"--ignore-table={db_database}.{table}" for table in ignore_tables])
    + f" > {output_file}"
)

process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

bar_format = "{l_bar}{bar}| {n:.2f}/{total:.2f} MB [{elapsed}<{remaining}, {rate_fmt}]"

with tqdm(total=float(total_size), desc="Backup Progress (MB)", unit="MB", bar_format=bar_format) as pbar:
    previous_size = 0
    while process.poll() is None:
        time.sleep(1)
        try:
            current_size = os.path.getsize(output_file) / (1024 * 1024)
            pbar.update(current_size - previous_size)
            previous_size = current_size
        except FileNotFoundError:
            continue

    process.communicate()

print("Backup concluído com sucesso.")