import pandas as pf
import os 
from dotenv import load_dotenv

load_dotenv()

df_temp = os.getenv("MASTER_DATA_PRICING")
print(df_temp)