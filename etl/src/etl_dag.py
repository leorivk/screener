import pandas as pd
import clickhouse_connect

def run_etl():
    print("Starting ETL process...")
    
    # 1. Extract: 원본 데이터(CSV) 읽기
    df = pd.read_csv('data/large_source_data.csv')
    print("E: Raw data extracted from CSV.")

    # 2. Transform: (간단한 변환만 수행)
    df['marketCap'] = df['marketCap'].astype(int)
    df['per'] = df['per'].astype(float)
    print("T: Data transformed.")

    # 3. Load: ClickHouse에 데이터 적재
    client = clickhouse_connect.get_client(host='localhost', port=8123, user='default', password='password123')
    print("Connected to ClickHouse.")

    TABLE_NAME = "stocks"
    
    # 테이블 재생성
    client.command(f'DROP TABLE IF EXISTS {TABLE_NAME}')
    client.command(f'''
        CREATE TABLE {TABLE_NAME} (
            `Code` String,
            `Name` String,
            `market` String,
            `sector` String,
            `marketCap` UInt64,
            `per` Float64
        ) ENGINE = MergeTree()
        ORDER BY Code
    ''')
    print(f"Table '{TABLE_NAME}' created.")

    # 데이터프레임을 테이블에 삽입
    client.insert_df(TABLE_NAME, df)
    print(f"L: {len(df)} rows loaded into ClickHouse table '{TABLE_NAME}'.")
    
    print("ETL process finished successfully.")

if __name__ == "__main__":
    run_etl()
