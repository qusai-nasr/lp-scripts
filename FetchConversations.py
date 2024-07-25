import requests
import json
import csv
import logging
from datetime import datetime
from time import sleep
from requests_oauthlib import OAuth1


# Documentation: https://developers.liveperson.com/messaging-interactions-api-methods-conversations.html

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
URL_TEMPLATE = "https://lo.msghist.liveperson.net/messaging_history/api/account/52375911/conversations/search?offset={offset}&limit={limit}&NC=true"
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': '*/*',
}
BATCH_SIZE = 100
CSV_FILE_PATH = 'Output/Conversations.csv'
JSON_FILE_PATH = 'Output/Conversations.json'

# OAuth credentials
APP_KEY = "37494a2a0505440aa1b57858e50490ad"
APP_SECRET = "20f2d2a82af2525b"
ACCESS_TOKEN = "64372f41b8d945ad9ae74fcfb0ba21d4"
ACCESS_TOKEN_SECRET = "607da79ee284ebc6"

# Set up OAuth1 authentication
AUTH = OAuth1(APP_KEY, APP_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


def payload(from_date, to_date, skill_ids):
    from_timestamp = int(datetime.strptime(from_date, '%Y-%m-%d').timestamp() * 1000)
    to_timestamp = int(datetime.strptime(to_date, '%Y-%m-%d').timestamp() * 1000)
    payload = json.dumps({
        "start": {
            "from": from_timestamp,
            "to": to_timestamp
        },
        "status": ["OPEN"],
        "contentToRetrieve": [
            "sdes",
            "messageStatuses"
        ],
        # "skillIds": skill_ids
    })
    # print(payload)
    return payload


def fetch_conversations(from_date, to_date, skill_ids, offset=0, limit=50):
    url = URL_TEMPLATE.format(offset=offset, limit=limit)
    payload = payload(from_date, to_date, skill_ids)
    try:
        response = requests.post(url, auth=AUTH, headers=HEADERS, data=payload)
        response.raise_for_status()
        return response.json().get("conversationHistoryRecords", [])
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return []


def write_csv_header():
    with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "Conversation ID", "Start Time", "Date", "Duration", "Status", "Last Skill", "Last Skill Name",
            "Latest Agent Full Name", "Company Branch", "IMEI", "Last Delivery Status"
        ])


def append_to_csv(records):
    with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for record in records:
            writer.writerow(record)


def process_conversations(conversations):
    processed_records = []
    target_conversations = 0

    for conversation in conversations:
        conversation_id = conversation['info'].get('conversationId', 'N/A')
        skill = conversation['info'].get('latestSkillId', 'N/A')
        status = conversation['info'].get('status', 'N/A')
        start_time = conversation['info'].get('startTime', 'N/A')
        date = datetime.fromtimestamp(conversation['info'].get('startTimeL', 0) / 1000).strftime('%Y-%m-%d')
        last_skill_name = conversation['info'].get('latestSkillName', 'N/A')
        latest_agent_full_name = conversation['info'].get('latestAgentFullName', 'N/A')
        duration = conversation['info'].get('duration', 'N/A')
        company_branch = 'N/A'
        imei = 'N/A'

        for event in conversation.get('sdes', {}).get('events', []):
            if event.get('sdeType') == 'CUSTOMER_INFO':
                company_branch = event['customerInfo']['customerInfo'].get('companyBranch', 'N/A')
                imei = event['customerInfo']['customerInfo'].get('imei', 'N/A')

        last_message = conversation.get('messageStatuses', [])[-1] if conversation.get('messageStatuses') else {}
        last_participant_type = last_message.get('participantType', 'N/A')
        last_delivery_status = last_message.get('messageDeliveryStatus', 'N/A')

        # Only log conversations where the last participant is a Consumer
        if last_participant_type == 'Consumer':
            if status == 'OVERDUE':
                target_conversations += 1

            processed_records.append(
                (conversation_id, start_time, date, duration, status, skill, last_skill_name, latest_agent_full_name,
                 company_branch, imei, last_delivery_status))

    return processed_records, target_conversations


def extract_and_save_conversations(from_date, to_date, skill_ids):
    offset = 0
    total_conversations = 0
    target_conversations = 0
    all_records = []
    all_conversations = []

    write_csv_header()

    while True:
        conversations = fetch_conversations(from_date, to_date, skill_ids, offset=offset, limit=BATCH_SIZE)
        if not conversations:
            break

        logging.info(f"Retrieved {len(conversations)} conversations - Offset: {offset}")
        total_conversations += len(conversations)

        processed_records, batch_target_conversations = process_conversations(conversations)
        all_records.extend(processed_records)
        all_conversations.extend(conversations)
        target_conversations += batch_target_conversations

        offset += BATCH_SIZE

    save_conversations_to_csv(all_records)
    save_conversations_to_json(all_conversations)
    success_rate = (total_conversations - target_conversations) / total_conversations if total_conversations > 0 else 0
    return total_conversations, target_conversations, success_rate


def save_conversations_to_csv(records):
    while records:
        batch = records[:BATCH_SIZE]
        append_to_csv(batch)
        records = records[BATCH_SIZE:]
        sleep(5)


def save_conversations_to_json(conversations):
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(conversations, f, ensure_ascii=False, indent=4)


def main(from_date, to_date, skill_ids):
    total_conversations, target_conversations, success_rate = extract_and_save_conversations(from_date, to_date, skill_ids)
    logging.info(f"Data extraction completed and saved to CSV and JSON.")
    logging.info(f"Total Conversations: {total_conversations}")
    logging.info(f"Target Conversations (Status: OVERDUE or awaiting agent response): {target_conversations}")
    logging.info(f"Success Rate: {success_rate * 100:.2f}%")


if __name__ == "__main__":
    # Example dynamic values
    from_date = '2024-07-20'
    to_date = '2024-07-24'
    skill_ids = []  # Example skill IDs

    main(from_date, to_date, skill_ids)