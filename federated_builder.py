import os
import pandas as pd
import numpy as np
import s3fs
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- CREDENTIALS & CONFIG ---
# Load variables from .env (see .env.sample for the expected keys).
load_dotenv()

BUCKET_NAME = os.environ["BUCKET_NAME"]
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

def generate_full_year():
    # Initialize connection from your workstation
    try:
        s3 = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)
        print("Connected to AWS successfully.")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    start_date = datetime(2026, 1, 1)
    
    print("Starting generation for 365 days (approx. 1MB per day)...")

    for d in range(365):
        current_dt = start_date + timedelta(days=d)
        date_str = current_dt.strftime("%Y-%m-%d")
        
        # Create ~1200 rows to hit the 1MB target with 30 columns
        rows = 1200
        data = {
            "_time": [current_dt + timedelta(seconds=i*60) for i in range(rows)],
            "src_ip": [f"{np.random.randint(1,255)}.1.1.1" for _ in range(rows)],
            "method": np.random.choice(["GET", "POST", "PUT"], rows),
            "uri": np.random.choice(["/login", "/api/v1", "/admin"], rows),
            "action": np.random.choice(["ALLOW", "BLOCK"], rows, p=[0.9, 0.1]),
            "user_agent": ["XSIAM-Federated-Search-Test"] * rows
        }

        # Add dozens of extra fields for a robust search schema
        for i in range(25):
            data[f"waf_metadata_{i}"] = np.random.choice(["external", "internal", "trusted"], rows)

        df = pd.DataFrame(data)

        # Hive Path: wafLogs/ds=yyyy-mm-dd (matches IAM policy scope)
        path = f"s3://{BUCKET_NAME}/wafLogs/ds={date_str}/waf_logs.parquet"
        
        with s3.open(path, 'wb') as f:
            df.to_parquet(f, index=False, compression='snappy')
        
        if d % 30 == 0 or d == 364:
            print(f"Progress: Processed up to {date_str}")

if __name__ == "__main__":
    generate_full_year()
    print("\nAll folders and files for 2026 have been created.")
