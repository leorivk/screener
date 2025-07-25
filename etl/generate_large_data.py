import pandas as pd
import numpy as np
import os
import time

# ----- 설정 -----
NUM_ROWS = 5_000_000  # 500만 건의 데이터 생성
# ----------------

def generate_data():
    print(f"Generating {NUM_ROWS:,} rows of sample stock data...")
    start_time = time.time()

    markets = ['KOSPI', 'KOSDAQ', 'NASDAQ']
    sectors = ['IT', 'BIO', 'FINANCE', 'MANUFACTURE', 'SERVICE', 'ENERGY']

    data = {
        'Code': [f'A{i:07d}' for i in range(NUM_ROWS)],
        'Name': [f'Stock-{i}' for i in range(NUM_ROWS)],
        'market': np.random.choice(markets, size=NUM_ROWS),
        'sector': np.random.choice(sectors, size=NUM_ROWS),
        'marketCap': np.random.randint(100, 50000, size=NUM_ROWS),
        'per': np.random.uniform(5.0, 80.0, size=NUM_ROWS).round(2)
    }
    df = pd.DataFrame(data)

    if not os.path.exists('data'):
        os.makedirs('data')
    csv_path = 'data/large_source_data.csv'
    df.to_csv(csv_path, index=False)
    print(f"Data saved to: {csv_path}")

    end_time = time.time()
    print(f"Data generation finished in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    generate_data()
