from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, ArrayType
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import logging
import json
import glob
import re

def xTransform(file):
     data = open(file, 'r').read()
        
     data = data.replace('{\'', '{"')\
                            .replace('\':', '":')\
                            .replace(', \'', ', "')\
                            .replace('\': \'', '": "')\
                            .replace('\'},', '"},')\
                            .replace('\': "', '": "')\
                            .replace('\'}]', '"}]')\
                            .replace('": \'', '": "')\
                            .replace('\', "', '", "')\
                            .replace(': False', ': "False"')\
                            .replace(': True', ': "True"')\
                            .replace(': None', ': "None"')\
                            .replace('"Plato restaurant"', '\'Plato restaurant\'')\
                            .replace('26" flat-screen TV', '26\' flat-screen TV')
     
     open(file, 'w').write(json.dumps(json.loads(data.encode('utf-8').decode('unicode_escape'))))
     return file

def transform(ti):

    '''
        Here idea is to covert JSON file into spark data frame and then store them into csv format
        these csv files are then loaded onto Azure blob storage in the form of raw data, in raw container

        Note - In json data for citites and district, cities id column is negative value,
               we will convert them into positive value before storing them as csv files
    
    '''
    # getting json data path
    json_data_path = ti.xcom_pull(task_ids='extract_hotel_api_data_task', key='json_data_directory')

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

    # setting csv data path to write csv files
    csv_dir = curr_dir + f'/csv_data/{str(datetime.now().date())}'

    # creating csv data directory if not present
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    # initialize global logging setting
    logging.basicConfig(filename= logging_dir + f'/transform_log_{datetime.now().date()}.log'\
                    ,format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S'\
                        , level=logging.INFO\
                            ,force=True)
    
    logging.info(f'Parent directory is {parent_dir}')
    logging.info(f'Current directory is {curr_dir}')
    logging.info(f'JSON input directory is {json_data_path}')
    logging.info(f'CSV Data directory is {csv_dir}')

    # creating sprak sesion
    spark = SparkSession.builder.appName('Transform data Spark').getOrCreate()
    logging.info('Transform Spark session is created, processing further')

    # getting list of all file to be processed
    files = glob.glob(f'{json_data_path}/*.json')

    for file in files:
            
        try:
            start_time = datetime.now()

            # getting file name
            file_name = re.findall('[a-z_]*', file.split('/')[-1].split('.')[0])[0]
            logging.info(f'Transforming {file_name} into csv format')

            
            df = spark.read.option('multiline', 'true').json(file).cache()
            print(df.show(2))

            # getting list of columns
            columns = df.columns

            # changing city id column to possitive value
            if 'city_id' in columns:
                logging.info(f'City id column found in {file_name}')
                df = df.withColumn('city_id', col('city_id').cast('int') * -1)

            # setting output csv file name
            op_csv_file_name = f'{csv_dir}/{file_name}{datetime.now().date()}.csv'
            
            # dumping csv file as output
            df.toPandas().to_csv(op_csv_file_name, mode='w', header=True, index=False)

            end_time = datetime.now()
            logging.info(f'Transformation of {file_name} completed, time taken {end_time - start_time}')

        except Exception as err:
            logging.error(f'Error occured while transforming file, {file_name} with spark, must be some encoding issue, trying xTransform appraoch')

            try:
                file = xTransform(file)
                df = pd.read_json(file)

                # setting output csv file name
                file_name = re.findall('[a-z_]*', file.split('/')[-1].split('.')[0])[0]
                op_csv_file_name = f'{csv_dir}/{file_name}{datetime.now().date()}.csv'
                
                df.to_csv(op_csv_file_name, mode='w', header=True, index=False)

            except Exception as err1:
                logging.error(err1)
            
            


    # stopping spark sesion
    spark.stop()
    logging.info('Stopping spark sesion')

    # logging end time
    ti.xcom_push(key='csv_data_path', value=csv_dir)
    total_end_time = datetime.now()
    logging.info(f'Transformation completed, time taken {total_end_time - total_start_time}')


if __name__ == '__main__':
    transform('/home/abhijitmorye/DE-ELT_Hotel_Booking/elt/json_data/2024-04-11')