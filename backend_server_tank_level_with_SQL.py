import imaplib
import email
import time
import paho.mqtt.client as mqtt
from io import StringIO
import csv
import time
import pyodbc
from datetime import datetime

# Configuration for IMAP email
IMAP_SERVER = 'xyz@abc.com' # Your Email server
EMAIL_USER = 'hgghg@abc.com' #your email id
EMAIL_PASSWORD = 'ADASDDSDD%123' # Your Password
EMAIL_FOLDER = 'INBOX'

# Configuration for SQL Database
DB_HOST = 'xtrffff\\SQLEXPRESS' #your DB Host
DB_USER = '' #user name
DB_PASSWORD = '' #password
DB_NAME = 'UpWork' #db Name


# Configuration for MQTT
MQTT_BROKER = 'broker.hivemq.com' #your MQTT broker
MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = 'tankAutomation/data'

# Email filtering criteria
EXPECTED_SENDER = "abc@gmail.com" #sender email is
REQUIRED_SUBJECT_KEYWORD = "Tank Data"
REQUIRED_BODY_TOKEN = "TANK9999"

# Connect to the email server
def connect_to_email():
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_USER, EMAIL_PASSWORD)
        return mail
    except Exception as e:
        print(f"Error connecting to email: {e}")
        raise

# Check for new emails from the particular sender and subject
def fetch_latest_email(mail):
    try:
        mail.select(EMAIL_FOLDER)
        # Search for emails that match the sender and subject criteria
        search_criteria = f'(UNSEEN FROM "{EXPECTED_SENDER}" SUBJECT "{REQUIRED_SUBJECT_KEYWORD}")'
        status, messages = mail.search(None, search_criteria)
        email_ids = messages[0].split()
        if not email_ids:
            return None
        latest_email_id = email_ids[-1]  # Get the latest unread email
        _, data = mail.fetch(latest_email_id, '(RFC822)')
        raw_email = data[0][1]
        return latest_email_id, raw_email
    except Exception as e:
        print(f"Error fetching emails: {e}")
        return None

# Extract the sender, subject, and body from the email
def extract_email_data(msg):
    sender = msg.get('From')
    subject = msg.get('Subject')
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            if "attachment" not in content_disposition and content_type == "text/plain":
                body += part.get_payload(decode=True).decode()
    else:
        body = msg.get_payload(decode=True).decode()
    return sender, subject, body

# Extract the attachment and save CSV data
def extract_csv_attachment(msg):
    for part in msg.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        if 'attachment' in part.get('Content-Disposition'):
            filename = part.get_filename()
            if filename.lower().endswith('.csv'):
                return part.get_payload(decode=True).decode()  # Return decoded CSV content
    return None

# Parse the CSV data into a list of dictionaries
def parse_csv_data(csv_data):
    csv_file = StringIO(csv_data)
    csv_reader = csv.DictReader(csv_file)
    return [row for row in csv_reader]

def get_tank_levels(data, tank_descriptions):
    try:
        # Initialize a dictionary with default values
        tank_levels = {desc: "000.00" for desc in tank_descriptions}
        
        # Update the dictionary with actual values from the CSV
        for row in data:
            if row['Tank Description'].strip() in tank_levels:
                # Get the Volume % value and clean it
                volume_percentage = row['Volume %'].strip().replace('%', '')
                try:
                    volume_percentage = float(volume_percentage)
                    if volume_percentage < 0:
                        volume_percentage = 0
                except ValueError:
                    volume_percentage = 0
                
                # Convert the cleaned value to float, scale, and format
                tank_levels[row['Tank Description'].strip()] = f"{volume_percentage:06.2f}"
        
        # Return the tank levels as a comma-separated string
        result = ','.join(tank_levels[desc] for desc in tank_descriptions)
        return result
    except Exception as e:
        print(f"Error extracting tank levels: {e}")
        return ""

# Validate email against the criteria
def is_valid_email(sender, subject, body):
    return EXPECTED_SENDER in sender and REQUIRED_SUBJECT_KEYWORD in subject and REQUIRED_BODY_TOKEN in body

# Send data to MQTT
def send_data_to_mqtt(client, topic, data):
    try:
        # Specify the tank descriptions to filter
        tank_descriptions = ["Paul 1", "Paul 2", "Paul 3", "Paul 4", "Paul 5", "Paul 6"]
        
        # Extract tank levels as a comma-separated string
        tank_levels = get_tank_levels(data, tank_descriptions)

        if tank_levels:
            # Send the tank levels as a string
            result = client.publish(topic, payload=tank_levels, qos=1, retain=False)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"Successfully published tank levels: {tank_levels}")
            else:
                print(f"Failed to publish tank levels: {result.rc}")
        else:
            print("No matching tank levels found.")
    except Exception as e:
        print(f"Error sending tank levels to MQTT: {e}")

# MQTT Connection callback
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {rc}")

# MQTT Publish callback
def on_publish(client, userdata, mid):
    print(f"Message {mid} has been published.")


def create_table_if_not_exists():
    try:
        connection = connect_to_database()
        cursor = connection.cursor()

        # SQL query to check if the table exists and create it if not
        check_query = """
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tank_data')
        BEGIN
            CREATE TABLE tank_data (
                id INT IDENTITY(1,1) PRIMARY KEY,
                unit_name VARCHAR(255),
                tank_description VARCHAR(255),
                tank_number VARCHAR(50),
                volume FLOAT,
                volume_percentage FLOAT,
                status VARCHAR(50),
                last_updated DATETIME,
                insertion_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        END
        """
        cursor.execute(check_query)
        connection.commit()
        print("Table is ready (created if not existing).")
    except Exception as e:
        print(f"Error ensuring table exists: {e}")
    finally:
        if connection:
            connection.close()

def connect_to_database():
    try:
        # Connect to SQL Server with Windows Authentication
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"  # Ensure ODBC Driver 17 or higher is installed
            f"SERVER={DB_HOST};"
            f"DATABASE={DB_NAME};"
            f"Trusted_Connection=yes;"
        )
        print("Database connection successful.")
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

# Store CSV data into the database using bulk insert with insertion timestamp
def store_csv_to_database(data):
    connection = None
    try:
        # Establish database connection
        connection = connect_to_database()
        cursor = connection.cursor()

        # SQL query for bulk insert with SQL Server parameter placeholders
        insert_query = """
            INSERT INTO tank_data (unit_name, tank_description, tank_number, volume, volume_percentage, status, last_updated, insertion_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Prepare bulk data as a list of tuples
        bulk_data = []
        for row in data:
            unit_name = row['Unit Name'].strip()
            tank_description = row['Tank Description'].strip()
            tank_number = row['Tank Number'].strip()
            volume = row['Volume'].strip()
            volume_percentage = row['Volume %'].strip().replace('%', '')
            status = row['Status'].strip()
            last_updated = row['Last Updated'].strip()
            insertion_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')  # Current timestamp

            # Handle invalid or missing data
            try:
                volume = volume.strip() if volume else None
                volume_percentage = volume_percentage.strip().replace('%', '') if volume_percentage else None
                last_updated = last_updated.strip() if last_updated else None
                # Now, attempt conversion to the correct types
                if volume:
                 volume = float(volume)
                else:
                 volume = None
    
                if volume_percentage:
                   volume_percentage = float(volume_percentage)
                else:
                   volume_percentage = None
    
                if last_updated:
                # Update the format to match 'DD-MM-YYYY HH:MM'
                   last_updated = datetime.strptime(last_updated, '%d-%m-%Y %H:%M') if last_updated else None
                else:
                   last_updated = None
            except ValueError:
                volume = None
                volume_percentage = None
                last_updated = None

            bulk_data.append((
                unit_name,
                tank_description,
                tank_number,
                volume,
                volume_percentage,
                status,
                last_updated,
                insertion_timestamp
            ))

        # Perform the bulk insert
        cursor.executemany(insert_query, bulk_data)

        # Commit the transaction
        connection.commit()
        print(f"Successfully inserted {len(bulk_data)} rows into the database.")

    except Exception as e:
        # Rollback the transaction in case of error
        if connection:
            connection.rollback()
        print(f"Error storing CSV data to the database: {e}")
    finally:
        # Close the connection
        if connection:
            connection.close()

# Main function
def main():
    try:
        create_table_if_not_exists()
        # Connect to MQTT broker
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_publish = on_publish
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()  # Start MQTT loop in a background thread

        # Connect to email
        mail = connect_to_email()

        while True:
            try:
                print("Checking for new emails...")
                result = fetch_latest_email(mail)
                if result:
                    email_id, raw_email = result
                    msg = email.message_from_bytes(raw_email)
                    sender, subject, body = extract_email_data(msg)
                    print(f"Sender: {sender}")
                    print(f"Subject: {subject}")
                    print(f"Body: {body}")
                    
                    if is_valid_email(sender, subject, body):
                        print("Email is valid. Processing...")
                        csv_data = extract_csv_attachment(msg)
                        if csv_data:
                            data = parse_csv_data(csv_data)
                             # Store data in the database
                            store_csv_to_database(data)
                            send_data_to_mqtt(client, 'tankAutomation/data', data)
                    else:
                        print("Email is ignored as it doesn't match the criteria.")
                else:
                    print("No new unread emails.")
                time.sleep(30)  # Check emails every 30 seconds
            except Exception as e:
                print(f"Error in processing loop: {e}")
                time.sleep(60)

    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
