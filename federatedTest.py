import os
import pandas as pd
import numpy as np
import s3fs
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIGURATION ---
# Load variables from .env (see .env.sample for the expected keys).
load_dotenv()

BUCKET_NAME = os.environ["BUCKET_NAME"]
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

def run_test():
    try:
        # Initialize connection using your workstation credentials
        s3 = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)
        
        # 1. Create a tiny dataset (approx 1MB)
        # 1200 rows + 30 columns is the "sweet spot" for 1MB Parquet
        rows = 1200
        data = {
            "_time": [datetime.now()] * rows,
            "src_ip": ["192.168.1.1"] * rows,
            "action": np.random.choice(["ALLOW", "BLOCK"], rows),
            "user_agent": ["Workstation-Test-Client"] * rows
        }
        # Add filler fields to match your "dozens of fields" requirement
        for i in range(26):
            data[f"waf_field_{i}"] = "test_value"

        df = pd.DataFrame(data)

        # 2. Define Hive Path for a 2026 date
        test_date = "2026-01-01"
        target_path = f"s3://{BUCKET_NAME}/wafLogs/ds={test_date}/test_log.parquet"

        print(f"Uploading 1MB test file to: {target_path}...")

        # 3. Write the file
        with s3.open(target_path, 'wb') as f:
            df.to_parquet(f, engine='pyarrow', index=False, compression='snappy')

        print("✅ SUCCESS! Check your S3 console for the 'ds=2026-01-01' folder.")

    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    run_test()
