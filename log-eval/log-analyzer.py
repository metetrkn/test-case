import re
import csv
from collections import defaultdict
import json
import os

STATE_FILE = 'state.json'

def get_report_id():
    """
    Reads the current report number, updates it for each reporting
    """
    with open(STATE_FILE, 'r') as f:
        try:
            state = json.load(f)
        except json.JSONDecodeError as e:
            print(e.msg)

    current_count = state.get('report_count')
    new_count = current_count + 1
    state['report_count'] = new_count


    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)
        
    return current_count


def analyze_logs_to_csv(input_file, output_file):
    # Regex to capture Topic, Consumer, Created, and Sent
    log_pattern = re.compile(
        r"Topic:\s*(?P<topic>[^|]+)\s*\|\s*"
        r"Consumer:\s*(?P<consumer>[^|]+)\s*\|\s*"
        r"Created:\s*(?P<created>\d+)\s*\|\s*"
        r"Sent:\s*(?P<sent>\d+)"
    )

    # Key: (topic, consumer), Value: [list of execution times]
    stats = defaultdict(list)

    report_id = get_report_id()

    try:
        # 1. Read and Parse
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                match = log_pattern.search(line)
                if match:
                    topic = match.group('topic').strip()
                    consumer = match.group('consumer').strip()
                    created_ts = int(match.group('created'))
                    sent_ts = int(match.group('sent'))

                    exec_time = sent_ts - created_ts
                    stats[(topic, consumer)].append(exec_time)

        # 2. Write to CSV
        file_exists = os.path.isfile(output_file) and os.path.getsize(output_file) > 0

        with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            # Only write header if the file didn't exist or was empty
            if not file_exists:
                writer.writerow(['report_id', 'topic', 'consumer', 'total_mails', 'average_execution_time_(ms)', 'max_execution_time_(ms)'])

            for (topic, consumer), times in stats.items():
                total_count = len(times)
                avg_time = sum(times) / total_count
                max_time = max(times)
                
                writer.writerow([report_id, topic, consumer, total_count, f"{avg_time:.2f}", max_time])

        print(f"Success! Report updated: {output_file}")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")


# --- Usage ---
if __name__ == "__main__":
    analyze_logs_to_csv('C:/Users/mete/Desktop/staj/test-case/test/javaConsumer/javaConsumer/logs/email-consumer.log',
                         'log_report.csv')