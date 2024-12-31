import pandas as pd
import psycopg2
import boto3
from datetime import date, datetime
import time
import os,sys,inspect
import logging

"""
This sample script covers the incremental ETL from a product AWS Postgres instance (Aurora) and
moves the data to S3 for both archival and Redshift retrieval
"""
# Get logging folder from environment variable or default to "./logs" on ec2 instance
log_folder = os.getenv("LOG_FOLDER", "./logs")
os.makedirs(log_folder, exist_ok=True)

# Setup logging to capture any issues within the pipeline
log_filename = os.path.join(log_folder, f"jasons_etl_script_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    )

# Add a console handler to print to the terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

# Redshift connection parameters
REDSHIFT_CONN_PARAMS = {
    "database": "jasons_redshift_db",
    "user": "jasontoledo",
    "password": "jasons_redshift_pw",
    "host": "jasons_redshift_host.us-west-1.redshift.amazonaws.com",
    "port": "5439",
}

# PostgreSQL connection parameters
POSTGRES_CONN_PARAMS = {
    "database": "jasons_pg_db",
    "user": "jasontoledo",
    "password": "jasons_pg_pw",
    "host": "jasons_pg_host.us-west-1.rds.amazonaws.com",
    "port": "5432",
}

# define schema constant
SCHEMA = 'public'

def get_latest_id_from_redshift(table_name):
    """
    This function retrieves the most recent id value from the Redshift table
    """
    logging.info(f"Fetching latest updated_at for {table_name}")
    redshift_conn = psycopg2.connect(**REDSHIFT_CONN_PARAMS)
    cur = redshift_conn.cursor()

    sql_query = f"SELECT COALESCE(MAX(id), 0) FROM {table_name};"
    cur.execute(sql_query)
    result = cur.fetchone()
    redshift_conn.close()

    return result[0]


def transform(data, col_names, table_name, target_date):
    """
    This function cleans and standardizes data types for ingestion into S3 and Redshift.
    """
    print("Transforming data for table:" + table_name)

    # Create a DataFrame
    data = pd.DataFrame(data, columns=col_names)
    logging.info("Data frame created!")

    # Standardize datetime columns
    data["created_at"] = pd.to_datetime(data["created_at"].dt.strftime('%Y-%m-%d %H:%M:%S'))
    data["updated_at"] = pd.to_datetime(data["updated_at"].dt.strftime('%Y-%m-%d %H:%M:%S'))

    # Convert datetime columns to strings for JSON
    data = data.assign(**data.select_dtypes(["datetime"]).astype(str).to_dict("list"))

    # Save as JSON object and write to temp folder
    data = data.to_json(orient="records", lines=True)
    with open(f"/tmp/{table_name}_{target_date}.json", "w") as f:
        f.write(data)


def fetch_source_table_incremental(target_date, table_name):
    """
    This function retrieves the most recent id value from the production postgres table
    """
    logging.info(f"Start fetching data for table: {table_name}")

    # Connect to PostgreSQL
    postgres_conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
    cur = postgres_conn.cursor()

    # Get the latest updated_at value from Redshift
    latest_id = get_latest_id_from_redshift(table_name)
    logging.info(f"Latest ID value in Redshift for {table_name}: {latest_id}")

    # Fetch new/updated rows from PostgreSQL
    sql_string = f"""
    SELECT * FROM {SCHEMA}.{table_name}
    WHERE id > {latest_id};
    """
    cur.execute(sql_string)

    # if the new data exceeds 500k rows, increment in chunks
    col_names = [elt[0] for elt in cur.description]
    chunk_size = 500000
    total_rows = 0

    while True:
        data = cur.fetchmany(chunk_size)
        if not data:
            break

        print(f"Fetched {len(data)} rows for table: {table_name}")
        transform(data, col_names, table_name, target_date)
        move_to_aws_s3(target_date, table_name)

        total_rows += len(data)

    postgres_conn.close()
    logging.info(f"Total rows fetched for table {table_name}: {total_rows}")        


def move_to_aws_s3(target_date, table_name):
    """
    This function uploads the transformed JSON file to S3 with event_date folder structure.
    """
    print(f"Start moving data to S3 for table: {table_name}")

    file_path = f"/tmp/{table_name}_{target_date}.json"
    bucket_name = "jasons-fictitious-bucket"
    s3_key = f"{table_name}/event_date={target_date}/{table_name}.json"

    # Upload to S3
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).upload_file(file_path, s3_key)

    logging.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")


def load_target_table_from_s3(target_date, table_name):
    """
    Load data from S3 into a Redshift temporary table using a copy query
    """
    logging.info(f"Start loading data into Redshift for table: {table_name}")

    # Establish connection to Redshift
    redshift_conn = psycopg2.connect(**REDSHIFT_CONN_PARAMS)
    cur = redshift_conn.cursor()

    # S3 bucket and keys
    bucket_name = "jasons-fictitious-bucket"
    s3_key = f"{table_name}/event_date={target_date}/{table_name}.json"

    # copy query to insert data from s3 into redshift table
    copy_query = f"""
    COPY {table_name}_temp
    FROM 's3://{bucket_name}/{s3_key}'
    credentials 'aws_iam_role=arn:aws:iam::1234567890:role/Redshift_IAM_Role'
    json 's3://{bucket_name}/{table_name}/{table_name}_jpath.json'
    TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
    ACCEPTINVCHARS '^' TRUNCATECOLUMNS TRIMBLANKS;
    """

    try:
        cur.execute(copy_query)
        redshift_conn.commit()
        logging.info(f"Data loaded into Redshift table {table_name}_temp from s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.info(f"Error loading data into Redshift for table {table_name}: {e}")
        redshift_conn.rollback()
        raise

    logging.info(f"Data loaded into Redshift table {table_name} from s3://{bucket_name}/{s3_key}")


def merge_temp_to_main_table(table_name):
    """
    This function merges data from the temporary table into the main table in Redshift.
    Ensures no duplicate rows are added.
    """
    logging.info(f"Start merging data from {table_name}_temp to {table_name}")

    # Establish connection to Redshift
    redshift_conn = psycopg2.connect(**REDSHIFT_CONN_PARAMS)
    cur = redshift_conn.cursor()

    # Merge query: Insert only new rows
    merge_query = f"""
    BEGIN TRANSACTION;

    -- Insert rows from the temp table that don't already exist in the main table
    INSERT INTO {table_name}
    SELECT *
    FROM {table_name}_temp
    WHERE id > (SELECT COALESCE(MAX(id), 0) FROM {table_name});
    ;

    -- Clear the temporary table after the merge to regain storage
    TRUNCATE TABLE {table_name}_temp;

    END TRANSACTION;
    """

    # run merge and rollback if there is an exception
    try:
        cur.execute(merge_query)
        redshift_conn.commit()
        logging.info(f"Successfully merged data into {table_name} and cleared {table_name}_temp")
    except Exception as e:
        logging.info(f"Error merging data into {table_name}: {e}")
        redshift_conn.rollback()
        raise


def main():
    """
    The main() function runs the etl script
    """
    logging.info("Starting the ETL...")

    try:
        # start timer to check script performance
        start_time = time.time()
        start_time_utc = pd.to_datetime(start_time, unit="s", utc=True).strftime('%Y-%m-%d %H:%M:%S UTC')
        logging.info(f"Starting ETL process at {start_time_utc}")

        # create target date and query list
        target_date = str(date.today())
        TABLES = ["incremental_table1","incremental_table2"]

        # Iterate over tables in QUERYLIST
        for table_name in TABLES:
            # Fetch and process data incrementally
            fetch_source_table_incremental(target_date, table_name)

            # Load data from S3 into Redshift
            load_target_table_from_s3(target_date, table_name)

            # merge data from temp table into the primary table
            merge_temp_to_main_table(table_name)

        # create end time to calculate how long the script took to run
        end_time = time.time()
        end_time_utc = pd.to_datetime(end_time, unit="s", utc=True).strftime('%Y-%m-%d %H:%M:%S UTC')
        elapsed_time = end_time - start_time
        logging.info(f"ETL process completed at {end_time_utc} in {elapsed_time:.2f} seconds")


    except Exception as e:
        logging.error("==== Failed while executing ETL process ====")
        exec_type, exec_obj, exec_tb = sys.exc_info()
        err = (
            f"Error in {inspect.currentframe().f_code.co_name} "
            f"at line: {exec_tb.tb_lineno} -> {str(e)}"
        )
        logging.error(err)
        raise ValueError(err)

# run the whole script by calling main()
if __name__ == "__main__":
    main()
