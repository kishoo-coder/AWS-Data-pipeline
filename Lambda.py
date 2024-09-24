import boto3
import csv
from io import StringIO
from datetime import datetime
import redshift_connector

# Initialize SNS client
sns_client = boto3.client('sns')

# SNS topic ARN (replace with your SNS topic ARN)
SNS_TOPIC_ARN = 'arn:aws:sns:eu-north-1:160885267705:success_failure_message'

def send_sns_notification(message, subject="Lambda Execution Notification"):
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject=subject
    )
    print(f"SNS Notification sent: {response['MessageId']}")

def lambda_handler(event, context):
    # S3 client
    s3 = boto3.client('s3')
    
    # S3 bucket details
    bucket_name = 'collectingdata'
    folder_prefix = 'project/'  # Folder where CSV files are stored
    update_folder = 'updates/'
    insert_folder = 'inserts/'
    archived_folder = 'archives/'  # Archive folder to move processed files
    
    # Redshift Serverless connection details
    redshift_host = 'default-workgroup.160885267705.eu-north-1.redshift-serverless.amazonaws.com'
    redshift_db = 'dev'
    redshift_user = 'admin'
    redshift_password = 'Admin_1234'
    redshift_port = 5439
    
    # Table names
    stage_table = 'stage'
    production_table = 'production'
    audit_log_table = 'audit_log'
    
    try:
        # List all files in the specified folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        if 'Contents' not in response:
            print("No files found in the specified folder.")
            return {
                'statusCode': 200,
                'body': "No CSV files found."
            }

        # Connect to Redshift
        conn = redshift_connector.connect(
            host=redshift_host,
            database=redshift_db,
            user=redshift_user,
            password=redshift_password,
            port=redshift_port
        )
        cur = conn.cursor()
        
        # Track inserted and updated rows
        updated_rows = []
        inserted_rows = []
        
        # Capture the start time of the process
        start_time = datetime.now()
        
        # Loop through the files and process each CSV file
        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key.endswith('.csv'):
                print(f"Processing file: {file_key}")
                
                # Get the CSV file from S3
                csv_response = s3.get_object(Bucket=bucket_name, Key=file_key)
                file_content = csv_response['Body'].read().decode('utf-8')
                
                # Use the csv module to read the CSV content
                csv_file = StringIO(file_content)
                csv_reader = csv.reader(csv_file)
                
                # Skip the header row (if the CSV contains headers)
                header = next(csv_reader, None)
                
                # Insert CSV data into the staging table
                print("Inserting data into Redshift staging table...")
                for row in csv_reader:
                    cur.execute(f"""
                        INSERT INTO {stage_table} (Employee_ID, Employee_Name, Role, Shift_Type, Enter_Date, Salary)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """, row)
                
                # Commit the insertion to staging
                conn.commit()

                # Capture inserted rows by selecting them from the staging table
                cur.execute(f"""
                    SELECT s.Employee_ID, s.Employee_Name, s.Role, s.Shift_Type, s.Enter_Date, s.Salary
                    FROM {stage_table} s
                    LEFT JOIN {production_table} p
                    ON s.Employee_ID = p.Employee_ID
                    WHERE p.Employee_ID IS NULL;
                """)
                inserted_rows.extend(cur.fetchall())

                # Insert new records into production
                print("Inserting new records into production table...")
                cur.execute(f"""
                    INSERT INTO {production_table} (Employee_ID, Employee_Name, Role, Shift_Type, Enter_Date, Salary)
                    SELECT s.Employee_ID, s.Employee_Name, s.Role, s.Shift_Type, s.Enter_Date, s.Salary
                    FROM {stage_table} s
                    LEFT JOIN {production_table} p
                    ON s.Employee_ID = p.Employee_ID
                    WHERE p.Employee_ID IS NULL;
                """)
                conn.commit()

                # Capture updated rows by selecting them from the production table
                cur.execute(f"""
                    SELECT s.Employee_ID, s.Employee_Name, s.Role, s.Shift_Type, s.Enter_Date, s.Salary
                    FROM {stage_table} s
                    JOIN {production_table} p
                    ON s.Employee_ID = p.Employee_ID
                    WHERE s.Employee_Name != p.Employee_Name 
                       OR s.Role != p.Role 
                       OR s.Shift_Type != p.Shift_Type 
                       OR s.Enter_Date != p.Enter_Date 
                       OR s.Salary != p.Salary;
                """)
                updated_rows.extend(cur.fetchall())
                
                # Update records in production where data has changed
                print("Updating records in production table...")
                cur.execute(f"""
                    UPDATE {production_table} p
                    SET Employee_Name = s.Employee_Name,
                        Role = s.Role,
                        Shift_Type = s.Shift_Type,
                        Enter_Date = s.Enter_Date,
                        Salary = s.Salary
                    FROM {stage_table} s
                    WHERE p.Employee_ID = s.Employee_ID
                    AND (s.Employee_Name != p.Employee_Name 
                         OR s.Role != p.Role 
                         OR s.Shift_Type != p.Shift_Type 
                         OR s.Enter_Date != p.Enter_Date 
                         OR s.Salary != p.Salary);
                """)
                conn.commit()
                
                # Move the processed file to the archive folder with a unique timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                file_name = file_key.split('/')[-1].replace('.csv', '')  # Extract the file name without the extension
                archived_file_key = f"{archived_folder}{file_name}_{timestamp}.csv"  # Append timestamp to the file name

                print(f"Archiving file: {file_key} to {archived_file_key}")
                s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=archived_file_key)
                
                # Delete the original file from the project folder
                print(f"Deleting file: {file_key}")
                s3.delete_object(Bucket=bucket_name, Key=file_key)

        # Count inserted and updated rows
        inserted_count = len(inserted_rows)
        updated_count = len(updated_rows)

        # Capture the end time
        end_time = datetime.now()

        # Insert audit log without end_date
        cur.execute(f"""
            INSERT INTO {audit_log_table} (updated, inserted, start_date)
            VALUES (%s, %s, %s)
        """, (updated_count, inserted_count, start_time))
        conn.commit()

        # Fetch the AID from the latest audit log entry
        cur.execute(f"SELECT MAX(AID) FROM {audit_log_table}")
        latest_aid = cur.fetchone()[0]

        # Update the end_time in the audit_log
        cur.execute(f"""
            UPDATE {audit_log_table}
            SET end_date = %s
            WHERE AID = %s
        """, (end_time, latest_aid))
        conn.commit()

        # Update production table with the AID for inserted rows
        inserted_ids = [str(row[0]) for row in inserted_rows]

        if inserted_ids:
            placeholders = ','.join(['%s'] * len(inserted_ids))
            cur.execute(f"""
                UPDATE {production_table} p
                SET AID = %s
                WHERE p.Employee_ID IN ({placeholders})
            """, [latest_aid] + inserted_ids)
            conn.commit()

        # Update production table with the AID for updated rows
        updated_ids = [str(row[0]) for row in updated_rows]

        if updated_ids:
            placeholders = ','.join(['%s'] * len(updated_ids))
            cur.execute(f"""
                UPDATE {production_table} p
                SET AID = %s
                WHERE p.Employee_ID IN ({placeholders})
            """, [latest_aid] + updated_ids)
            conn.commit()

        # Truncate the staging table for future insertions
        print("Truncating the staging table...")
        cur.execute(f"TRUNCATE TABLE {stage_table};")
        conn.commit()

        # Close the connection
        cur.close()
        conn.close()

        # Write updated rows to a CSV file
        if updated_rows:
            updated_file_name = f"{update_folder}updated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            updated_csv = StringIO()
            writer = csv.writer(updated_csv)
            writer.writerow(header)  # Write the header
            writer.writerows(updated_rows)  # Write the updated rows
            s3.put_object(Bucket=bucket_name, Key=updated_file_name, Body=updated_csv.getvalue())
            print(f"Updated rows written to: {updated_file_name}")
        
        # Write inserted rows to a CSV file
        if inserted_rows:
            inserted_file_name = f"{insert_folder}inserted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            inserted_csv = StringIO()
            writer = csv.writer(inserted_csv)
            writer.writerow(header)  # Write the header
            writer.writerows(inserted_rows)  # Write the inserted rows
            s3.put_object(Bucket=bucket_name, Key=inserted_file_name, Body=inserted_csv.getvalue())
            print(f"Inserted rows written to: {inserted_file_name}")

        # Send SNS notification upon successful execution
        success_message = f"Lambda executed successfully: {inserted_count} rows inserted, {updated_count} rows updated."
        send_sns_notification(success_message)

        return {
            'statusCode': 200,
            'body': success_message
        }
        
    except Exception as e:
        error_message = f"Error occurred during Lambda execution: {str(e)}"
        print(error_message)
        send_sns_notification(error_message, subject="Lambda Execution Error")
        return {
            'statusCode': 500,
            'body': error_message
        }
