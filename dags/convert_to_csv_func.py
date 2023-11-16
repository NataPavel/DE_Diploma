import csv
import json


def jsonl_to_csv():
    up_json = '/opt/airflow/data/user_profiles/user_profiles.jsonl'
    up_csv = '/opt/airflow/data/user_profiles/user_profiles.csv'

    with open(up_json, 'r') as jsonl_file, open(up_csv, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['email', 'full_name', 'state', 'birth_date', 'phone_number'])

        for jsonline in jsonl_file:
            data = json.loads(jsonline)

            csv_writer.writerow([
                data['email'],
                data['full_name'],
                data['state'],
                data['birth_date'],
                data['phone_number']
            ])
