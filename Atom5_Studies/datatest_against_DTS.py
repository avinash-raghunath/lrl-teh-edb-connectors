
import os
import pandas as pd
import yaml

# Load the configuration file
with open('dtsconfig.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Define the folder containing the files
folder_path = 's3_raw_layer'

# Print file names in the folder
print("Files in folder:", folder_path)
for file_name in os.listdir(folder_path):
    if os.path.isfile(os.path.join(folder_path, file_name)):
        print(file_name)

# Initialize reports
file_presence_report = []
column_presence_report = []
data_type_report = []
data_length_report = []

# Function to validate data type
def validate_data_type(expected_type, actual_type):
    if expected_type == 'DateTime':
        return actual_type in ['datetime64[ns]', 'object']
    elif expected_type == 'Date':
        return actual_type in ['datetime64[ns]', 'object']
    elif expected_type == 'Time':
        return actual_type in ['datetime64[ns]', 'object']
    elif expected_type == 'Integer':
        return actual_type in ['int64', 'float64']
    elif expected_type == 'Text':
        return actual_type == 'object'
    return False

# Function to validate data length
def validate_data_length(expected_length, actual_length):
    return actual_length <= expected_length

# Validate files and columns
for file_name, file_info in config['files'].items():
    file_path = os.path.join(folder_path, file_name)
    file_exists = os.path.isfile(file_path)
    
    # File presence report
    file_presence_report.append({
        'filename': file_name,
        'presence': 'pass' if file_exists else 'fail'
    })
    
    if file_exists:
        df = pd.read_csv(file_path)
        columns = config['global_columns']['columns'] + file_info['columns']
        
        # Convert DataFrame columns to lowercase
        # df.columns = map(str.lower, df.columns)
        
        for column in columns:
            # column_name = column['column_name'].lower()
            column_name = column['column_name']
            expected_type = column.get('data_type', None)
            expected_length = column.get('column_length', None)
            
            column_exists = column_name in df.columns
            column_presence_report.append({
                'filename': file_name,
                'column_name': column['column_name'],
                'presence': 'pass' if column_exists else 'fail'
            })
            
            if column_exists and expected_type:
                actual_type = str(df[column_name].dtype)
                data_type_report.append({
                    'filename': file_name,
                    'column_name': column['column_name'],
                    'expected_datatype': expected_type,
                    'actual_datatype': actual_type,
                    'result': 'pass' if validate_data_type(expected_type, actual_type) else 'fail'
                })
                
                if expected_length:
                    max_length = df[column_name].astype(str).map(len).max()
                    data_length_report.append({
                        'filename': file_name,
                        'column_name': column['column_name'],
                        'expected_length': expected_length,
                        'result': 'pass' if validate_data_length(expected_length, max_length) else 'fail'
                    })

# Create a Pandas Excel writer using XlsxWriter as the engine.
with pd.ExcelWriter('reports/data_validation_reports.xlsx', engine='xlsxwriter') as writer:
    # Convert the reports to DataFrames
    file_presence_df = pd.DataFrame(file_presence_report)
    column_presence_df = pd.DataFrame(column_presence_report)
    data_type_df = pd.DataFrame(data_type_report)
    data_length_df = pd.DataFrame(data_length_report)
    
    # Write each DataFrame to a different worksheet.
    file_presence_df.to_excel(writer, sheet_name='File Presence', index=False)
    column_presence_df.to_excel(writer, sheet_name='Column Presence', index=False)
    data_type_df.to_excel(writer, sheet_name='Data Type', index=False)
    data_length_df.to_excel(writer, sheet_name='Data Length', index=False)

print("Reports have been consolidated into 'validation_reports.xlsx'")