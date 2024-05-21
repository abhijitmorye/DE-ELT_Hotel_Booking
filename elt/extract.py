import logging
from datetime import datetime, timedelta
import os
import requests
from airflow.models import Variable
import traceback
import json

def extract(ti, api_host_name, api_key):

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

    # setting json data path to write json file after extracting APIs response
    json_dir = curr_dir + f'/json_data/{str(datetime.now().date())}'

    # creating json data directory if not present
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)


    # initialize global logging setting
    logging.basicConfig(filename= logging_dir + f'/extract_log_{datetime.now().date()}.log'\
                    ,format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S'\
                        , level=logging.INFO\
                            ,force=True)
    
    logging.info(f'Parent directory is {parent_dir}')
    logging.info(f'Current directory is {curr_dir}')
    logging.info(f'Json Data directory is {json_dir}')


    # defining headers and additionnal params
    headers = {
	"X-RapidAPI-Key": str(api_key),
	"X-RapidAPI-Host": str(api_host_name)
    }

    # defining json file output format
    json_op_format = f"{datetime.strftime(datetime.now().date(), '%Y%M%d')}.json"


    # extracting city data
    try:
        logging.info('Extraction of cities data started')
        start_time = datetime.now()
        cities_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/cities',\
                                        headers=headers\
                                        )
        cities_op_file = open(f'{json_dir}/cities_{json_op_format}', 'w')
        cities_op_file.write(str(cities_response.json()['result']))
        cities_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of cities data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting hotel data
    try:
        logging.info('Extraction of hotels data started')
        start_time = datetime.now()
        hotels_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/hotels',\
                                        headers=headers\
                                        )
        hotels_op_file = open(f'{json_dir}/hotels_{json_op_format}', 'w')
        hotels_op_file.write(str(hotels_response.json()['result']))
        hotels_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of hotels data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting chain types data
    try:
        logging.info('Extraction of chain types data started')
        start_time = datetime.now()
        chain_types_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/chain-types',\
                                        headers=headers,\
                                        )
        chain_types_op_file = open(f'{json_dir}/chain_types_{json_op_format}', 'w')
        chain_types_op_file.write(str(chain_types_response.json()['result']))
        chain_types_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of chain types data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    # extracting regions data
    try:
        logging.info('Extraction of regions data started')
        start_time = datetime.now()
        regions_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/regions',\
                                        headers=headers\
                                        )
        regions_op_file = open(f'{json_dir}/regions_{json_op_format}', 'w')
        regions_op_file.write(str(regions_response.json()['result']))
        regions_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of regions data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())
    
    # extracting hotel types data
    try:
        logging.info('Extraction of hotel types data started')
        start_time = datetime.now()
        hotel_types_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/hotel-types',\
                                        headers=headers,\
                                        )
        hotel_types_op_file = open(f'{json_dir}/hotel_types_{json_op_format}', 'w')
        hotel_types_op_file.write(str(hotel_types_response.json()['result']))
        hotel_types_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of hotel types data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting room-facility-types data
    try:
        logging.info('Extraction of room facility types data started')
        start_time = datetime.now()
        room_facility_types_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/room-facility-types',\
                                        headers=headers,\
                                        )
        room_facility_types_op_file = open(f'{json_dir}/room_facility_types_{json_op_format}', 'w')
        room_facility_types_op_file.write(str(room_facility_types_response.json()['result']))
        room_facility_types_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of room facility types data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting payment-types data
    try:
        logging.info('Extraction of payemnt types data started')
        start_time = datetime.now()
        payment_types_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/payment-types',\
                                        headers=headers,\
                                        )
        payment_types_op_file = open(f'{json_dir}/payment_types_{json_op_format}', 'w')
        payment_types_op_file.write(str(payment_types_response.json()['result']))
        payment_types_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of payment types data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting disticts data
    try:
        logging.info('Extraction of districts data started')
        start_time = datetime.now()
        districts_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/districts',\
                                        headers=headers\
                                        )
        district_op_file = open(f'{json_dir}/district_{json_op_format}', 'w')
        district_op_file.write(str(districts_response.json()['result']))
        district_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of districts data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting countries data
    try:
        logging.info('Extraction of countries data started')
        start_time = datetime.now()
        country_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/country',\
                                        headers=headers,\
                                        )
        country_op_file = open(f'{json_dir}/country_{json_op_format}', 'w')
        country_op_file.write(str(country_response.json()['result']))
        country_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of countries data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting room-types data
    try:
        logging.info('Extraction of room-types data started')
        start_time = datetime.now()
        room_types_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/room-types',\
                                        headers=headers,\
                                        )
        room_types_op_file = open(f'{json_dir}/room_types_{json_op_format}', 'w')
        room_types_op_file.write(str(room_types_response.json()['result']))
        room_types_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of room-types data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting hotel-facility-types data
    try:
        logging.info('Extraction of hotel-facility-types data started')
        start_time = datetime.now()
        hotel_facility_types_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/hotel-facility-types',\
                                        headers=headers,\
                                        )
        hotel_facility_types_op_file = open(f'{json_dir}/hotel_facility_types_{json_op_format}', 'w')
        hotel_facility_types_op_file.write(str(hotel_facility_types_response.json()['result']))
        hotel_facility_types_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of hotel-facility-types data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # extracting facility-types data
    try:
        logging.info('Extraction of facility-types data started')
        start_time = datetime.now()
        facility_types_response = requests.get(f'https://booking-com.p.rapidapi.com/v1/static/facility-types',\
                                        headers=headers,\
                                        )
        facility_types_op_file = open(f'{json_dir}/facility_types_{json_op_format}', 'w')
        facility_types_op_file.write(str(facility_types_response.json()['result']))
        facility_types_op_file.close()
        end_time = datetime.now()
        logging.info(f'Extraction of facility-types data completed, time taken - {end_time - start_time}')
    except Exception:
        logging.error(traceback.print_exc())

    
    # pushing current json data directory for further ETL processsing
    # comment this line if doing unit test
    ti.xcom_push(key='json_data_directory', value=json_dir)

    total_end_time = datetime.now()
    logging.info(f'Extraction of data completed. Time taken - {total_end_time - total_start_time}')



if __name__ == '__main__':
    extract(Variable.get('rapid_hotel_api_host_name'), Variable.get('rapid_hotel_api_key'))