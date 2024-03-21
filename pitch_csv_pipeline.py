from ftplib import FTP, error_perm, error_temp
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import sql
import os
import time

def create_file_list(ftp, remote_dir='.', file_paths=[]):
    try:
        # Change to the remote directory
        ftp.cwd(remote_dir)

        # List all files and directories in the current remote directory
        file_list = []
        ftp.retrlines('LIST', file_list.append)

        for line in file_list:
            # Extract filename from the line
            filename = os.path.basename(line.split(None, 8)[-1])

            if filename in ['.', '..']:
                continue
            # Check if it's a directory
            if line.startswith('d'):
                # Recursively create file list from the subdirectory
                create_file_list(ftp, f'{remote_dir}/{filename}', file_paths)
            else:
                # Check if the file is a CSV file
                if filename.lower().endswith('.csv'):
                    file_paths.append(f'{remote_dir}/{filename}')
    except error_perm as e:
        print(f"Permission error: {e}")
    except TimeoutError as e:
        print(f"Timeout error: {e}")

    return file_paths


# Replace these with your FTP server details
ftp_host = '****'
ftp_user = '****'
ftp_password = '****'
database_name = '****'
table_name = '****'
postgres_user = '****'
postgres_password = '****'
postgres_host = '****'
postgres_port = '****'

connection_string = 'postgresql://' +postgres_user+':'+postgres_password+'@'+postgres_host+'/'+database_name

# Replace these with your SQL database details
db_engine = create_engine(connection_string)  # Use your SQL database connection string
conn = db_engine.connect()

# Connect to the FTP server
connected = False
while not connected:
    try:
        with FTP(ftp_host, timeout=120) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_password)
            connected = True

            # Create list of file paths
            print("Creating file list")
            file_paths = create_file_list(ftp, remote_dir='')
    except (TimeoutError, error_temp) as e:
        print(f"FTP connection timeout: {e}")
        time.sleep(30)  # Wait for 30 seconds before reconnecting
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Loop through the file paths and download the files
valid_file_paths = [k for k in file_paths if 'playerpositioning' not in k]
print("beginning file sync")
while valid_file_paths:
    try:
        with FTP(ftp_host, timeout=60) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_password)

            for file_path in valid_file_paths:
                # Download file using FTP
                print(file_path)
                with BytesIO() as buffer:
                    ftp.retrbinary(f'RETR {file_path}', buffer.write)
                    buffer.seek(0)

                    df = pd.read_csv(buffer)

                    # Process the DataFrame as needed
                    
                    # Insert DataFrame into SQL database
                    df.to_sql('Pitch', con=conn, index=False, if_exists='append', method='multi', chunksize=1000)
                valid_file_paths.remove(file_path)
    except (TimeoutError, error_temp) as e:
        print(f"FTP connection timeout: {e}")
        time.sleep(30)  # Wait for 30 seconds before reconnecting
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
