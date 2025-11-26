import psycopg2
from getpass import getpass
import json
import lz4.block
import os
import sys

def deep_parse_json(obj):
    if isinstance(obj, dict):
        return {k: deep_parse_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_parse_json(item) for item in obj]
    if isinstance(obj, str):
        try:
            if obj.startswith(('{', '[')):
                parsed = json.loads(obj)
                return deep_parse_json(parsed)
        except json.JSONDecodeError:
            pass
        return obj
    return obj

def extract_and_save_json(table_name, record_id):
    host = "stagingdb.int.sensibull.com"
    port = "5432"
    dbname = "sensibull_stage3"
    user = "sensibull_staging"
    password = getpass("Enter database password: ")
    
    dest_folder = f"tv_{table_name}_decompressed"
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()
        
        # Select columns: assume schema id, user_id, content for all tables
        select_cols = 'id, user_id, content'
        cursor.execute(f"SELECT {select_cols} FROM {table_name} WHERE id = %s", (record_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            print(f"No record found with id {record_id} in table {table_name}")
            return
        
        _, user_id, content_blob = row
        
        if content_blob is None:
            print(f"No content for ID {record_id} in {table_name}")
            return
        
        # Decompress
        try:
            content_decompressed = lz4.block.decompress(content_blob)
            print(f"Successfully decompressed {table_name} ID {record_id} with LZ4")
        except Exception as e:
            print(f"LZ4 decompress failed for {table_name} ID {record_id}: {e}")
            return
        
        # Parse JSON
        try:
            content_str = content_decompressed.decode('utf-8')
            json_data = json.loads(content_str)
            fully_parsed = deep_parse_json(json_data)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"Could not decode/parse {table_name} ID {record_id}: {e}")
            return
        
        # Create folder if not exists
        os.makedirs(dest_folder, exist_ok=True)
        
        # Save formatted JSON
        output_file = os.path.join(dest_folder, f"{record_id}.json")
        with open(output_file, 'w') as f:
            json.dump(fully_parsed, f, indent=2)
        
        print(f"Formatted JSON saved to {output_file}")
        
    except psycopg2.Error as e:
        print(f"Database error for {table_name}: {e}")
    except Exception as e:
        print(f"Error processing {table_name} ID {record_id}: {e}")

if __name__ == "__main__":
    print("Select type:")
    print("1 - Drawing (tv_drawings)")
    print("2 - Study Template (tv_study_templates)")
    print("3 - Chart Layout (tv_chart_layouts)")
    
    choice = input("Enter choice (1/2/3): ").strip()
    
    if choice == '1':
        table = 'tv_drawings'
    elif choice == '2':
        table = 'tv_study_templates'
    elif choice == '3':
        table = 'tv_chart_layouts'
    else:
        print("Invalid choice")
        sys.exit(1)
    
    record_id = input(f"Enter record ID for {table}: ").strip()
    
    extract_and_save_json(table, record_id)
