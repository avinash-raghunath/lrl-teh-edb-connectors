import json
import yaml
import pandas as pd

# Load the YAML configuration
with open('Atom5_Studies/dtsconfig.yaml', 'r') as yaml_file:
    yaml_config = yaml.safe_load(yaml_file)

# Load the JSON data
with open('Atom5_Studies/s3_processed_layer/manifest.json', 'r') as json_file:
    json_data = json.load(json_file)

# Function to validate JSON data against YAML configuration
def validate_json(json_data, yaml_config):
    global_columns = {col['column_name']: col for col in yaml_config['global_columns']['columns']}
    file_columns = {filename: {col['column_name']: col for col in file['columns']} for filename, file in yaml_config['files'].items()}

    report_data = []

    for data_file in json_data['data']:
        filename = data_file['filename']
        if filename not in file_columns:
            report_data.append([filename, 'File Validation', 'File found in YAML', 'File not found in YAML', 'Fail'])
            continue

        # Scenario 1: Validate study, site, and subject
        study_result = 'Pass'
        site_result = 'Pass'
        subject_result = 'Pass'
        if 'study' not in data_file or data_file['study'] != 'STUDYID':
            study_result = 'Fail'
        if 'site' not in data_file or data_file['site'] != 'SITE':
            site_result = 'Fail'
        if 'subject' not in data_file or data_file['subject'] != 'SUBJID':
            subject_result = 'Fail'
        report_data.append([filename, 'Study Validation', 'STUDYID', data_file.get('study', 'Missing'), study_result])
        report_data.append([filename, 'Site Validation', 'SITE', data_file.get('site', 'Missing'), site_result])
        report_data.append([filename, 'Subject Validation', 'SUBJID', data_file.get('subject', 'Missing'), subject_result])

        # Scenario 2: Validate event column
        event_result = 'Pass'
        if 'event' not in data_file or 'column' not in data_file['event'] or data_file['event']['column'] != 'VISITNUM':
            event_result = 'Fail'
        report_data.append([filename, 'VISITNUM Validation', 'VISITNUM', data_file.get('event', {}).get('column', 'Missing'), event_result])

        # Scenario 3: Validate items
        for item_name, item in data_file['items'].items():
            if item_name in global_columns:
                expected_type = global_columns[item_name].get('data_type', '').lower()
                expected_length = global_columns[item_name].get('column_length')
            elif item_name in file_columns[filename]:
                expected_type = file_columns[filename][item_name].get('data_type', '').lower()
                expected_length = file_columns[filename][item_name].get('column_length')
            else:
                report_data.append([filename, 'Item Validation', f'{item_name} in YAML', 'Not found in YAML', 'Fail'])
                continue

            item_result = 'Pass'
            if item['type'] != expected_type:
                item_result = 'Fail'
            if expected_length and int(item.get('length', 0)) != expected_length:
                item_result = 'Fail'

            expected = f'{item_name}, {expected_type}, {expected_length}'
            actual = f'{item_name}, {item["type"]}, {item.get("length", "Missing")}'
            report_data.append([filename, 'Item Validation', expected, actual, item_result])

    return report_data

# Validate the JSON data
report_data = validate_json(json_data, yaml_config)

# Create DataFrame for the report
report_df = pd.DataFrame(report_data, columns=['Filename', 'Scenario', 'Expected', 'Actual', 'Result'])

# Write the DataFrame to an Excel file
with pd.ExcelWriter('Atom5_Studies/reports/manifest_validation_report.xlsx') as writer:
    report_df.to_excel(writer, sheet_name='Report', index=False)
print("manifest file validation report generated successfully.")