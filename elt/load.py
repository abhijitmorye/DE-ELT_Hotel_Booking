import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from datetime import datetime
import logging
import pyodbc
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:az-de-sql-server-01.database.windows.net,1433;Database=az-de-db-001;Uid=azdesqlserver001;Pwd=azdesqlserver@001;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'


def load_folder_path_to_azure(folder_name):
    try:
        logging.info('Connecting to AZURE SQL DB and updating control table')
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(f"update dbo.controltable set folderName='{folder_name}' where id = 1")
        conn.commit()
        logging.info('Updated successfully')
    except Exception:
        logging.error('Connection failed')

    

def load(ti, connection_string, container_name):

    # setting total start time
    total_start_time = datetime.now()

    # setting current directory variable to current directory
    parent_dir =  os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
    curr_dir  = os.path.dirname(os.path.realpath(__file__))

    # setting log directory variable to log directory
    logging_dir = curr_dir + f'/logs/{str(datetime.now().date())}'

    # creating log directory if not present
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)

    # initialize global logging setting
    logging.basicConfig(filename= logging_dir + f'/load_raw_data_log_{datetime.now().date()}.log'\
                    ,format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S'\
                        , level=logging.INFO\
                            ,force=True)
    
    logging.info(f'Parent directory is {parent_dir}')
    logging.info(f'Current directory is {curr_dir}')

    logging.info('Starting load raw data task..')

    # initializing connection string
    connection_string = connection_string

    # creating BlobServiceClient instance from connection string
    blobServiceClient = BlobServiceClient.from_connection_string(connection_string)

    # initializing container name and passing to BlobServiceClient instance
    container = container_name
    blobServiceClient.get_container_client(container)

    # overwrite to false, change if you want to overwrite the file
    overwrite = False
    success = True

    # current time of upload, this will act as root folder inside the raw container for raw data
    datetimefolder = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
    print(datetimefolder)

    # getting current raw folder from previous task to upload to azure raw container
    csv_data_path = ti.xcom_pull(task_ids='transform_hotel_api_data_task', key='csv_data_path')
    logging.info(f'Input raw csv data path -- {csv_data_path}')

    # uploading a file from location
    for root, directory, file in os.walk(csv_data_path):
        if file:
            for data_file in file:
                blob_path = os.path.join(root.replace('/'.join(root.split('/')[0:-1]), ''), datetimefolder, data_file)[1:]
                logging.info(f'Container Upload full path -- {blob_path}')
                blob_obj = blobServiceClient.get_blob_client(container=container, blob=blob_path)

                try:
                    with open(os.path.join(root, data_file), 'rb') as data:
                        blob_obj.upload_blob(data, overwrite=overwrite)
                except ResourceExistsError:
                    success=False
                    logging.error('Raw data already existed')
                    break
        
    if success:
        load_folder_path_to_azure('/'.join(blob_path.split('/')[0:-1]))
        ti.xcom_push(key='recent_raw_container_path', value='/'.join(blob_path.split('/')[0:-1]))
        total_end_time = datetime.now()
        logging.info(f'Raw data load completed, time taken {total_end_time - total_start_time}')
    else:
        logging.error('Error occured, please check the load_raw_data_log')
        

    


if __name__ == '__main__':
    '''
        replace connection_string with your azure connection string and cotainer with your azure
        container name
    
    '''
    # load('connection_string', 'container_name')
    load_folder_path_to_azure('testFolder')