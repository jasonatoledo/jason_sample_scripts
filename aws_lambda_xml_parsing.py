import json
import os
import boto3
import xmltodict
import re
import boto3
import logging
from datetime import datetime
from collections import Counter

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# definition of industry codes
industry_codes = {
    "BC": "Bank Card",
    "HZ": "Home Improvement Loans",
    "BZ": "Auto Loans",
    "FZ": "Consumer Finance",
    "BS": "Banking Services",
    "QU": "Utilities",
    "B": "Mortgage"
}

# definition of payment status codes
payment_status_codes = {
    "C": "Current or Paid as Agreed",
    "U": "Unavailable or No Data",
    "0": "Too New to Rate",
    "1": "30 Days Late",
    "2": "60 Days Late",
    "3": "90 Days Late",
    "4": "120+ Days Late",
    "5": "Collection or Charge-Off",
    "7": "Repossession",
    "8": "Foreclosure",
    "9": "Charge-Off"
}

# create s3 and redshift objects
s3 = boto3.client('s3')
redshift = boto3.client('redshift-data')  # Ensure you have necessary permissions

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    
    Args:
        event (dict): Event data passed by AWS Lambda.
        context (LambdaContext): Runtime information provided by Lambda.
    """
    # Log the incoming event for debugging
    print("Lambda Event Received:", json.dumps(event, indent=4))

    # Call run_event with the received event
    try:
        run_event(event)
    except Exception as e:
        print(f"Error in run_event: {e}")
        return {
            "statusCode": 500,
            "body": f"Error in run_event: {e}"
        }
    return {
        "statusCode": 200,
        "body": "Processing completed successfully."
    }

def extract_file_content_from_event(event):
    """
    Extracts the file content from the S3 PUT event.
    
    Args:
        event (dict): The event data passed from Lambda.
    
    Returns:
        str: File content as a string.
    """
    try:
        # Extract bucket and key from the event
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        object_key = event["Records"][0]["s3"]["object"]["key"]
        print(f"Processing file from bucket: {bucket_name}, key: {object_key}")

        # Get file content from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response["Body"].read().decode("utf-8")
        return file_content
    except Exception as e:
        print(f"Error extracting file content: {e}")
        raise

def generate_snake_case_variables(data_dict):
    """
    Converts dictionary keys to snake_case and assigns them as global variables.
    
    Args:
        data_dict (dict): The dictionary containing data with keys to convert.
    """
    snake_case_vars = {}  # To store the converted snake_case variables for debugging

    for key, value in data_dict.items():
        # Convert CamelCase to snake_case
        snake_case_key = ''.join(['_' + c.lower() if c.isupper() else c for c in key]).lstrip('_')
        globals()[snake_case_key] = value  # Dynamically assign the variable
        snake_case_vars[snake_case_key] = value  # Save for debugging or verification

def clean_keys(dirty_data):
    """Recursively remove @ symbols from keys in a dictionary."""
    if isinstance(dirty_data, dict):
        new_dict = {}
        for key, value in dirty_data.items():
            new_key = key.lstrip("@")  # Remove the '@' prefix
            new_dict[new_key] = clean_keys(value)  # Recurse into the value
        return new_dict
    elif isinstance(dirty_data, list):
        return [clean_keys(item) for item in dirty_data]  # Process lists recursively
    else:
        return dirty_data  # Return as is for base types

def preprocess_and_parse_xml(xml_content):
    try:
        # Remove problematic tags (e.g., OriginalData)
        xml_content = re.sub(r"<OriginalData>.*?</OriginalData>", "", xml_content, flags=re.DOTALL)

        # Wrap content in a single root element
        wrapped_content = f"<root>{xml_content}</root>"

        # Parse the wrapped XML content
        parsed_data = xmltodict.parse(wrapped_content)
        logger.info("Data Parsed Successfully!")

        new_dict = {}
        for key, value in parsed_data.items():
            new_key = key.lstrip("@")  # Remove the '@' prefix
            new_dict[new_key] = clean_keys(value)  # Recurse into the value        

        return new_dict

    except Exception as e:
        print(f"An error occurred: {e}")
        return None



# create and display the snapshot dicitonary
def create_and_display_snapshot(parsed_data):
    """
    Create a snapshot dictionary from parsed data and display it.
    Dynamically generate variable names in snake_case for each key.

    Args:
        parsed_data (dict): The parsed data containing a 'Snapshot' key.

    Returns:
        dict: The snapshot dictionary with keys converted to integers where applicable.
    """
    # Create snapshot dictionary
    snapshot_dict = {}
    for key, val in parsed_data['root']['Snapshot'].items():
        if val.isdigit():
            snapshot_dict[key] = int(val)
        else:
            snapshot_dict[key] = val

    # Additional computations
    if snapshot_dict.get('Utilization') == 0:
        snapshot_dict['Utilization'] = '100%'

    snapshot_dict['closed_account_pct'] = round(
        snapshot_dict['totalClosedAccounts'] / snapshot_dict['TotalAccounts'], 2
    )
    snapshot_dict['open_account_pct'] = round(
        snapshot_dict['OpenAccounts'] / snapshot_dict['TotalAccounts'], 2
    )
    snapshot_dict['deragatory_account_pct'] = round(
        snapshot_dict['DerogatoryAccounts'] / snapshot_dict['OpenAccounts'], 2
    )

    # Dynamically generate variable names in snake_case
    for key, value in snapshot_dict.items():
        # Convert CamelCase to snake_case
        snake_case_key = ''.join(['_' + c.lower() if c.isupper() else c for c in key]).lstrip('_')
        globals()[snake_case_key] = value

    # Display snapshot summary
    print("Snapshot Summary:\n")
    for key, value in snapshot_dict.items():
        print(f"{key}: {value}")
    print("\n")

    return snapshot_dict

def split_objects_by_keys(parsed_data, keys_to_split):
    """
    Split a dictionary into sub-dictionaries based on specific prefixes in the keys.

    Args:
        parsed_data (dict): The parsed data dictionary to process.
        keys_to_split (list): A list of prefixes to split the dictionary on.

    Returns:
        dict: A dictionary where each key corresponds to a prefix, and the value is a sub-dictionary
              containing all the matching keys from the original dictionary.
    """
    # Initialize a dictionary to hold the split objects
    split_objects = {key: {} for key in keys_to_split}

    # Iterate through the original parsed data
    for key, value in parsed_data.items():
        for split_key in keys_to_split:
            # Match the key and clean the prefix
            if key.startswith(split_key):
                cleaned_key = key.replace(f"{split_key}_", "", 1)  # Remove the prefix from the key
                split_objects[split_key][cleaned_key] = value  # Add the cleaned key-value pair

    return split_objects

def process_truelink_data(parsed_data):
    """
    Process and flatten data from the 'TrueLinkCreditReportType' key.
    """
    big_dict = {}
    bureaus_dict = {} 

    # parse truelink data
    truelink_data = parsed_data['root']['TrueLinkCreditReportType']

    # Flag to start parsing from SB168Frozen
    start_parsing = False

    # Iterate through the first level
    for key, val in parsed_data['root']['TrueLinkCreditReportType'].items():
        if key == "SB168Frozen":
            start_parsing = True  # Start processing

        if start_parsing:
            # Combine SB168Frozen keys into a nested dictionary
            if key == "SB168Frozen" and isinstance(val, dict):
                bureaus_dict.update(val) 
            elif key == "Borrower" and isinstance(val, dict):
                for sub_key, sub_val in val.items():
                    if sub_key == "CreditScore":
                        big_dict[f"{key}_{sub_key}"] = sub_val
                    big_dict[f"{key}_{sub_key}"] = sub_val 
            elif isinstance(val, dict):  # Generic dictionary handling
                for sub_key, sub_val in val.items():
                    big_dict[f"{key}_{sub_key}"] = sub_val
            else:
                big_dict[key] = val  # Add non-dictionary values at top level

    # Add the consolidated 'bureaus' dictionary
    if bureaus_dict:
        big_dict['SB168Frozen'] = bureaus_dict
      
    return big_dict

def extract_user_info(big_dict):
    """Extract user-related information like name, address, age, etc."""
    # get inquiry date
    inquiry_date = big_dict['Sources_Source'].get('InquiryDate','')

    # extract borrower name
    borrower_name = big_dict.get('Borrower_BorrowerName', '')

    # username updated logic
    if isinstance(borrower_name, dict):
        # Handle dictionary case
        name_data = borrower_name.get('Name', {})
        first = name_data.get('first', '')
        middle = name_data.get('middle', '')
        last = name_data.get('last', '')
    elif isinstance(borrower_name, list) and len(borrower_name) > 0:
        # Handle list case, process the first entry
        name_data = borrower_name[0].get('Name', {})
        first = name_data.get('first', '')
        middle = name_data.get('middle', '')
        last = name_data.get('last', '')
    else:
        # Handle missing or invalid Borrower_BorrowerName
        return None

    # create username variable
    username = " ".join(part for part in [first, middle, last] if part) or None

    # Days at current address
    try:
        days_current_address = (datetime.today() - datetime.strptime(
            big_dict['Borrower_BorrowerAddress']['dateReported'], '%Y-%m-%d')).days
    except:
        days_current_address = None

    # Number of previous addresses
    try:
        num_previous_addresses = len(big_dict.get('Borrower_PreviousAddress', []))
    except:
        num_previous_addresses = None

    # User's age
    try:
        users_age = round((datetime.today() - datetime.strptime(
            big_dict['Borrower_Birth']['date'], '%Y-%m-%d')).days / 365, 2)
    except:
        users_age = None
      
    return inquiry_date, username, days_current_address, num_previous_addresses, users_age    

def extract_employers(big_dict):
    """
    Extract employer data from the Borrower_Employer field.
    
    Args:
        big_dict (dict): The main dictionary containing Borrower_Employer.
    
    Returns:
        dict: A dictionary of employers with their names and dateUpdated values.
    """
    recent_employers = {}

    # Check if Borrower_Employer exists in the dictionary
    if 'Borrower_Employer' in big_dict and big_dict['Borrower_Employer']:
        # Handle list of employers
        if isinstance(big_dict['Borrower_Employer'], list):
            for idx, employer in enumerate(big_dict['Borrower_Employer'], start=1):
                name = employer.get('name', 'Unknown')  # Default to 'Unknown' if name is missing
                date_updated = employer.get('dateUpdated', 'Unknown')  # Default to 'Unknown' if dateUpdated is missing
                recent_employers[f"employer_{idx}"] = {'name': name, 'dateUpdated': date_updated}
        # Handle a single employer object
        elif isinstance(big_dict['Borrower_Employer'], dict):
            name = big_dict['Borrower_Employer'].get('name', 'Unknown')
            date_updated = big_dict['Borrower_Employer'].get('dateUpdated', 'Unknown')
            recent_employers['employer_1'] = {'name': name, 'dateUpdated': date_updated}
        else:
            print("Borrower_Employer is neither a list nor a dictionary.")
    else:
        recent_employers = {}
    
    return recent_employers


def extract_risk_score_and_factors(big_dict):
    """Extract risk score and factor type counts."""
    risk_score = int(big_dict['Borrower_CreditScore']['riskScore'])
    factor_type_counts = {}
    for factor in big_dict['Borrower_CreditScore'].get('CreditScoreFactor', []):
        factor_type = factor.get('FactorType', '')
        if factor_type:
            factor_type_counts[factor_type] = factor_type_counts.get(factor_type, 0) + 1

    return risk_score, factor_type_counts

def extract_tradeline_stats(big_dict):
    """Extract tradeline statistics."""
    tradeline_list = big_dict.get('TradeLinePartition', [])
    account_type_counts = dict(Counter(item['accountTypeDescription'] for item in tradeline_list))

    # Initialize counters
    counters = {key: Counter() for key in [
        'AccountCondition', 'AccountDesignator', 'DisputeFlag', 'IndustryCode',
        'OpenClosed', 'PayStatus', 'VerificationIndicator', 'CreditType',
        'PaymentFrequency', 'TermType', 'WorstPayStatus', 'PayStatusHistory']}
    counters['MissingPayStatusHistory'] = 0

    # Process tradelines
    for item in tradeline_list:
        tradeline = item.get('Tradeline', {})
        for key in counters:
            if key == 'PayStatusHistory':
                pay_status_history = tradeline.get('GrantedTrade', {}).get('PayStatusHistory', {})
                if isinstance(pay_status_history, dict):
                    status_string = pay_status_history.get('status', '')
                    if status_string:
                        counters[key].update(status_string)
            elif key in ['WorstPayStatus', 'CreditType', 'PaymentFrequency', 'TermType']:
                specific_field = tradeline.get('GrantedTrade', {}).get(key, {})
                value = specific_field.get('abbreviation') or specific_field.get('description')
                if value:
                    counters[key].update([value])
            else:
                value = tradeline.get(key, {}).get('abbreviation') or tradeline.get(key, {}).get('description')
                if value:
                    counters[key].update([value])

    converted_data = {key: dict(val) if isinstance(val, Counter) else val for key, val in counters.items()}

    # Dictionary to store snake_case variable names and their values
    dynamic_variables = {}

    # Map keys from `converted_data` to variables with underscores
    for key in counters.keys():
        # Convert CamelCase to snake_case with underscores
        snake_case_key = ''.join(['_' + c.lower() if c.isupper() else c for c in key]).lstrip('_')
        # Dynamically create variables with the snake_case names
        globals()[snake_case_key] = converted_data[key]

    # Map keys from `converted_data` to snake_case variable names
    for key in counters.keys():
        # Convert CamelCase to snake_case
        snake_case_key = ''.join(['_' + c.lower() if c.isupper() else c for c in key]).lstrip('_')
        # Add to the dictionary
        dynamic_variables[snake_case_key] = converted_data[key]

    # Iterate and print results
    for variable_name, value in dynamic_variables.items():
        formatted_name = variable_name.replace('_', '').capitalize()
        # print(f"{formatted_name}: {value}")     

    return account_type_counts, dynamic_variables

def parse_messages(big_dict):
    """
    Parse 'Message' tags from the dictionary and count symbol occurrences.

    Args:
        big_dict (dict): The dictionary containing 'Message' tags.

    Returns:
        Counter: A Counter object with counts of each symbol in the 'Code' tags.
    """
    message_symbols = Counter()

    # Ensure 'Message' key exists and is a list
    messages = big_dict.get('Message', [])

    # Iterate through each message
    for message in messages:
        if isinstance(message, dict):  # Ensure each message is a dictionary
            # Extract the 'symbol' from the 'Code' tag
            code = message.get('Code')
            if isinstance(code, dict):  # Ensure 'Code' is a dictionary
                symbol = code.get('symbol')
                if symbol:  # Only count if the symbol exists
                    message_symbols.update([symbol])

    return dict(message_symbols)

def prepare_output_data(**kwargs):
    """Prepare the output JSON data."""
    return {key: value for key, value in kwargs.items()}    

# Example Integration
def process_and_generate_variables(snapshot_dict):
    """
    Process snapshot and big_dict to generate snake_case variables and handle errors.
    
    Args:
        snapshot_dict (dict): Snapshot data dictionary.
        big_dict (dict): Main dictionary containing parsed data.
    """
    generate_snake_case_variables(snapshot_dict)

def process_tradeline_partition(big_dict):
    """
    Process the TradeLinePartition in the big_dict and generate statistics.
    
    Args:
        big_dict (dict): The dictionary containing TradeLinePartition and related data.
    
    Returns:
        dict: A dictionary with processed counters and account_type_counts.
    """
    tradeline_list = big_dict['TradeLinePartition']    
    account_type_counts = dict(Counter(item['accountTypeDescription'] for item in tradeline_list))

    # Initialize counters and account type counts
    counters = {
        'AccountCondition': Counter(),
        'AccountDesignator': Counter(),
        'DisputeFlag': Counter(),
        'IndustryCode': Counter(),
        'OpenClosed': Counter(),
        'PayStatus': Counter(),
        'VerificationIndicator': Counter(),
        'CreditType': Counter(),
        'PaymentFrequency': Counter(),
        'TermType': Counter(),
        'WorstPayStatus': Counter(),
        'PayStatusHistory': Counter(),
        'MissingPayStatusHistory': 0
    }

    # Process TradeLinePartition
    tradeline_list = big_dict.get('TradeLinePartition', [])
    account_type_counts = dict(Counter(item.get('accountTypeDescription', '') for item in tradeline_list))

    for item in tradeline_list:
        tradeline = item.get('Tradeline', {})  # Access the nested 'Tradeline' key
        for key in counters:
            if key == 'PayStatusHistory':
                # Safely access PayStatusHistory
                pay_status_history = tradeline.get('GrantedTrade', {}).get('PayStatusHistory', {})
                if isinstance(pay_status_history, dict):
                    # Process 'status' string if available
                    status_string = pay_status_history.get('status', '')
                    if status_string:
                        counters['PayStatusHistory'].update(status_string)

                    # Process 'MonthlyPayStatus' if available
                    monthly_status_list = pay_status_history.get('MonthlyPayStatus', [])
                    if isinstance(monthly_status_list, list):
                        for monthly_status in monthly_status_list:
                            status = monthly_status.get('status')
                            if status:  # Only update if status is valid
                                counters['PayStatusHistory'].update([status])
                else:
                    # Log missing or invalid PayStatusHistory
                    counters['MissingPayStatusHistory'] += 1

            elif key in ['WorstPayStatus', 'CreditType', 'PaymentFrequency', 'TermType']:
                # Safely access the relevant key under GrantedTrade
                specific_field = tradeline.get('GrantedTrade', {}).get(key, {})
                if isinstance(specific_field, dict):
                    # Check for abbreviation or description
                    value = specific_field.get('abbreviation') or specific_field.get('description')
                    if value:
                        counters[key].update([value])

            else:
                # Safely access abbreviation or description
                value = tradeline.get(key, {}).get('abbreviation') or tradeline.get(key, {}).get('description')
                if value:  # Ensure value is not None
                    counters[key].update([value])

    # Convert counters to dictionaries for easier processing
    converted_data = {key: dict(val) if isinstance(val, Counter) else val for key, val in counters.items()}

    return {
        "account_type_counts": account_type_counts,
        "converted_data": converted_data
    }


def upload_to_s3(file_path, bucket_name, s3_key):
    """
    Upload a file to an S3 bucket.

    Args:
        file_path (str): Path of the file to upload.
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): Key (path) in the bucket where the file will be stored.

    Returns:
        None
    """
    try:
        global s3
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {file_path} to S3. Error: {e}")


def run_event(event):
    """
    Process a real PUT event triggered by S3.
    
    Args:
        event (dict): The event object passed by AWS Lambda.
    """
    # Validate event structure
    if not event or "Records" not in event or len(event["Records"]) == 0:
        raise ValueError("Invalid event structure or missing Records.")

    # Extract file content using the Lambda event
    file_content = extract_file_content_from_event(event)

    # Process and parse XML content
    if isinstance(file_content, str):
        parsed_data = preprocess_and_parse_xml(file_content)

        if parsed_data:
            # Process TrueLinkCreditReportType
            big_dict = process_truelink_data(parsed_data)

            process_tradeline_partition(big_dict)

            account_type_counts, dynamic_variables = extract_tradeline_stats(big_dict)

            # Verify and split data for further processing
            keys_to_split = ["root_TrueLinkCreditReportType", "root_Snapshot"]
            split_result = split_objects_by_keys(parsed_data, keys_to_split)

            # Create snapshot dictionary for further processing
            snapshot_dict = create_and_display_snapshot(parsed_data)

            # Generate snake_case variables from both dictionaries
            process_and_generate_variables(snapshot_dict)

            # Extract user-level information
            inquiry_date, username, days_current_address, num_previous_addresses\
                , users_age = extract_user_info(big_dict)
            recent_employers = extract_employers(big_dict)

            # Extract risk score and factor counts
            risk_score, factor_type_counts = extract_risk_score_and_factors(big_dict)

            # Process TradeLinePartition
            tradeline_stats = process_tradeline_partition(big_dict)

            # Parse messages
            messages = parse_messages(big_dict)
            # print(f"messages: {messages}")

            output_data = {
                "total_accounts": total_accounts,
                "total_closed_accounts": total_closed_accounts,
                "delinquent_accounts": delinquent_accounts,
                "derogatory_accounts": derogatory_accounts,
                "open_accounts": open_accounts,
                "total_balances": total_balances,
                "total_monthly_payments": total_monthly_payments,
                "number_of_inquiries": number_of_inquiries,
                "total_public_records": total_public_records,
                "balance_open_revolving_accounts": balance_open_revolving_accounts,
                "total_open_revolving_accounts": total_open_revolving_accounts,
                "balance_open_installment_accounts": balance_open_installment_accounts,
                "total_open_installment_accounts": total_open_installment_accounts,
                "balance_open_mortgage_accounts": balance_open_mortgage_accounts,
                "total_open_mortgage_accounts": total_open_mortgage_accounts,
                "balance_open_collection_accounts": balance_open_collection_accounts,
                "total_open_collection_accounts": total_open_collection_accounts,
                "balance_open_other_accounts": balance_open_other_accounts,
                "total_open_other_accounts": total_open_other_accounts,
                "available_credit": available_credit,
                "utilization": utilization,
                "on_time_payment_percentage": on_time_payment_percentage,
                "late_payment_percentage": late_payment_percentage,
                "date_of_oldest_trade": date_of_oldest_trade,
                "age_of_credit": age_of_credit,
                "closed_account_pct": closed_account_pct,
                "open_account_pct": open_account_pct,
                "deragatory_account_pct": deragatory_account_pct,
                "inquiry_date":inquiry_date,
                "username": username,
                "days_current_address": days_current_address,
                "num_previous_addresses": num_previous_addresses,
                "users_age": users_age,
                "recent_employers": recent_employers,
                "risk_score": risk_score,
                "factor_type_counts": factor_type_counts,
                "account_type_counts": account_type_counts,
                "account_condition": account_condition,
                "account_designator": account_designator,
                "dispute_flag": dispute_flag,
                "industry_code": industry_code,
                "open_closed": open_closed,
                "pay_status": pay_status,
                "verification_indicator": verification_indicator,
                "credit_type": credit_type,
                "payment_frequency": payment_frequency,
                "term_type": term_type,
                "worst_pay_status": worst_pay_status,
                "pay_status_history": pay_status_history,
                "messages": messages
            }

            # Log or process the final output
            print("Output Data:", json.dumps(output_data, indent=4))

            # Extract original filename (without .xml)
            original_key = event["Records"][0]["s3"]["object"]["key"]
            filename = os.path.basename(original_key).replace(".xml", "")
            file_path = f"/tmp/{filename}.json"

            try:
                with open(file_path, "w") as json_file:
                    json.dump(output_data, json_file, indent=4)
                print(f"JSON file created at {file_path}")
            except Exception as e:
                print(f"Error creating JSON file: {e}")
                return

            # Upload to S3
            bucket_name = "tomo-xml-cr-bucket-output"
            s3_key = f"test-output-folder1/{filename}.json"
            upload_to_s3(file_path, bucket_name, s3_key)   

        else:
            print("Failed to parse the XML content.")
    else:
        print("Failed to retrieve file content.")

# debugging test
# run_event()
