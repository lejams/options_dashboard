import pandas as pd
from google.cloud import storage
from io import StringIO
import os
import gcsfs

# # Function to download CSV from Google Cloud Storage and load it into a pandas DataFrame
# def load_csv_from_gcs(bucket_name, folder_path, file_name, index_col=None):
#     client = storage.Client()
#     bucket = client.get_bucket(bucket_name)
#     blob = bucket.blob(f"{folder_path}/{file_name}")
    
#     # Download file content as string
#     data_string = blob.download_as_string()
    
#     # Load CSV into a pandas DataFrame, with the option to set index_col
#     df = pd.read_csv(StringIO(data_string.decode('utf-8')), index_col=index_col)
#     return df


def load_csv_from_gcs(bucket_name, folder_path, file_name, index_col=None):
    fs = gcsfs.GCSFileSystem()
    file_path = f"{bucket_name}/{folder_path}/{file_name}"
    with fs.open(file_path, 'r') as f:
        df = pd.read_csv(f, index_col=index_col)
    return df

def list_files_in_gcs(bucket_name, prefix):
    """
    List all files in a Google Cloud Storage bucket with a specific prefix (directory).
    :param bucket_name: GCS bucket name
    :param prefix: The "directory" path in GCS (use empty string for root)
    :return: A list of filenames in that GCS bucket "directory", without the prefix
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # List all files under the prefix
    blobs = bucket.list_blobs(prefix=prefix)
    
    # Collect the file names and strip the prefix using os.path.basename to remove any directory part
    file_names = [os.path.basename(blob.name) for blob in blobs]
    return file_names


def upload_to_gcs(bucket_name, destination_blob_name, csv_data):
    """
    Upload CSV data to Google Cloud Storage (GCS).
    :param bucket_name: Your GCS bucket name
    :param destination_blob_name: Path within the bucket to save the file
    :param csv_data: The CSV data to upload
    """
    # Initialize the GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload the CSV content as a string (in-memory)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"File uploaded to {bucket_name}/{destination_blob_name}.")


def write_csv_to_gcs(df, bucket_name, destination_blob_name):
    """
    Convert the DataFrame to CSV and upload it to GCS.
    :param df: Pandas DataFrame to write
    :param bucket_name: GCS bucket name
    :param destination_blob_name: Path in GCS where the CSV will be saved
    """
    # Convert the DataFrame to a CSV string in-memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Upload the CSV data to GCS
    upload_to_gcs(bucket_name, destination_blob_name, csv_data)


def create_gcs_directory(bucket_name, directory_prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Check if the "directory" exists
    blobs = list(bucket.list_blobs(prefix=directory_prefix))
    
    if len(blobs) == 0:
        # The "directory" does not exist, simulate its creation by uploading an empty placeholder
        blob = bucket.blob(f"{directory_prefix}/placeholder")
        blob.upload_from_string('')  # Upload an empty string as a placeholder
        print(f"Created directory {directory_prefix} in bucket {bucket_name}")
    else:
        print(f"Directory {directory_prefix} already exists in bucket {bucket_name}")



def upload_log_to_gcs(bucket_name, log_dir, file_name, log_content):
    """Upload log file content to a specific GCS path."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{log_dir}/{file_name}")
    
    # Upload log content
    blob.upload_from_string(log_content)
    print(f"Log {file_name} uploaded to GCS at {log_dir}/{file_name}.")