import sqlite3  
import sys


DB_PATH1 = "/opt/airflow/databases/vital_signs_editors.db"  
TABLE_NAME1 = "wiki_editors"
 
TABLE_NAME2 = "wiki_editor_metrics"


DB_PATH3 = "/opt/airflow/databases/vital_signs_web.db"  
TABLE_NAME3 = "vital_signs_metrics"

def print_table_content():
    n = input("editors[1]/ editor_metrics[2]/ vital_signs_metrics[3]: ")
    match n:
        case "1":
            code = input("code: ")
            DB_PATH =  DB_PATH1
            TABLE_NAME = code + TABLE_NAME1
        case "2":
            code = input("code: ")
            DB_PATH = DB_PATH1
            TABLE_NAME = code + TABLE_NAME2
        case "3":
            DB_PATH = DB_PATH3
            TABLE_NAME = TABLE_NAME3
        case _:
            print("Invalid input")
            sys.exit(1)

    try:
        
        conn = sqlite3.connect(DB_PATH)  
        cursor = conn.cursor()

       
        cursor.execute(f"SELECT * FROM {TABLE_NAME}")
        rows = cursor.fetchall()

        
        columns = [desc[0] for desc in cursor.description]
        print("\t".join(columns))

        
        for row in rows:
            print("\t".join(str(cell) if cell is not None else '' for cell in row))

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    print_table_content()
