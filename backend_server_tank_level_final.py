import imaplib
import email
import time
import paho.mqtt.client as mqtt
from io import StringIO
import csv

# Configuration for IMAP email
IMAP_SERVER = 'YOUR EMAIL SERVER' # To be changed as per your actual data
EMAIL_USER = 'YOUR EMAIL ID' # To be changed as per your actual data
EMAIL_PASSWORD = 'YOUR PASSWORD' # To be changed as per your actual data
EMAIL_FOLDER = 'INBOX'

# Configuration for MQTT
MQTT_BROKER = 'MQTT BROKER NAME' # To be changed as per your actual data
MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = 'tankAutomation/data'

# Email filtering criteria
EXPECTED_SENDER = "Sender email id" # To be changed as per your actual data
REQUIRED_SUBJECT_KEYWORD = "Tank Data" # Can be changed as per your actual data if required
REQUIRED_BODY_TOKEN = "TANK9999" # Can be changed as per your actual data if required

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

# Main function
def main():
    try:
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
