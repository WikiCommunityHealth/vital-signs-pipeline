import pandas as pd
import sqlite3 

conn = sqlite3.connect("./databases/vital_signs_web.db")
query = "SELECT * FROM vital_signs_metrics LIMIT 10"  
df = pd.read_sql_query(query, conn)  
print(df)
conn.close()