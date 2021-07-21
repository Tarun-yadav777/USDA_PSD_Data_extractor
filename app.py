import os
import sys
import requests
import pandas as pd
import pymongo
import logging
import configparser
from datetime import datetime


def initLogs():
    cwd = os.getcwd()
    logPath = cwd + '/logs'
    if not os.path.isdir(logPath):
        print('logs Folder not present')
        os.mkdir(logPath)
    else:
        print('logs Folder present')
    currentTimeStamp = datetime.now()
    logFileName = 'logs/logger_' + currentTimeStamp.strftime('%Y_%h_%d_%H_%M') + '.log';
    logging.basicConfig(filename=logFileName, level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def get_data(url, headers_dict):
    response = requests.get(url=url, headers=headers_dict)
    if response.status_code == 200:
        return response.json()
    else:
        logging.info("Error Occured in Api calling, Error Code: {}".format(response.status_code))


def data_prerpocessing(df, unit_data, attribute_data):
    for i in range(len(df['unitId'])):
        for value in unit_data:
            if df['unitId'].iloc[i] == value["unitId"]:
                df['unitId'].iloc[i] = value["unitDescription"]

    for i in range(len(df['unitId'])):
        for value in attribute_data:
            if df['attributeId'].iloc[i] == value["attributeId"]:
                df['attributeId'].iloc[i] = value["attributeName"]

    df.rename({"attributeId": "attribute", "unitId": "units"}, axis=1, inplace=True)
    df.drop('month', axis=1, inplace=True)
    df['marketYear'] = df['marketYear'].apply(int)
    df['calendarYear'] = df['calendarYear'].apply(int)


def db_connect(dbLink, dbName):
    client = pymongo.MongoClient(dbLink)
    logging.info("Connected to MongoDB...")
    if dbName not in client.list_databases():
        logging.info("Creating DataBase...")
        mydb = client[dbName]
    else:
        logging.info("Found Existed DataBase...")
        mydb = client.get_database(dbName)
    return mydb


def fetch_status(mydb, commodity_name, attribute_name, country_name, market_year, collection_name):
    if collection_name not in mydb.list_collection_names():
        fetch_check_obj = mydb[collection_name]
    else:
        fetch_check_obj = mydb.get_collection(collection_name)

    if len(list(fetch_check_obj.find(
            {"commodity_name": commodity_name, "attribute_name": attribute_name, "country_name": country_name,
             "market_year": market_year}))) == 0:
        fetch_check_obj.insert_one({"commodity_name": commodity_name, "attribute_name": attribute_name, "country_name": country_name,
             "market_year": market_year})
        return False
    else:
        return True


def add_data_to_db(mydb, data, collection_name):
    if collection_name not in mydb.list_collection_names():
        logging.info("Creating data store collection...")
        data_store_obj = mydb[collection_name]
    else:
        logging.info("Found existed data store collection...")
        data_store_obj = mydb.get_collection(collection_name)
    logging.info("Inserting values in data store collection... ")
    data_store_obj.insert_many(data)
    return "Done"


def main():
    initLogs()
    config = configparser.ConfigParser()
    config.read('config.ini')
    commodity_name = input("Enter Commodity: ")
    attribute_name = input("Enter attribute: ")
    country_name = input("Enter Country: ")
    market_year = int(input("Enter market year: "))

    api_key = config["API_KEY"]['apiKey']
    headers_dict = {"API_KEY": api_key}

    dbLink = config['DB']['dbLink']
    dbName = config['DB']['dbName']

    collection_validation = config['Collections']['usda_collection_validation']
    collection_data = config['Collections']['usda_psd_data_collection']

    url_commodities = "https://apps.fas.usda.gov/OpenData/api/psd/commodities"
    url_attributes = "https://apps.fas.usda.gov/OpenData/api/psd/commodityAttributes"
    url_countries = "https://apps.fas.usda.gov/OpenData/api/psd/countries"
    url_units = "https://apps.fas.usda.gov/OpenData/api/psd/unitsOfMeasure"

    logging.info("Connecting to MongoDB...")
    mydb = db_connect(dbLink, dbName)

    logging.info("Checking fetch status...")
    status = fetch_status(mydb, commodity_name, attribute_name, country_name, market_year, collection_validation)
    logging.info("Got the fetch status...")
    if status:
        logging.info("Data for specified Date already in DataBase....")
    else:
        logging.info("Calling Api's")
        commodity_data = get_data(url_commodities, headers_dict)
        attribute_data = get_data(url_attributes, headers_dict)
        country_data = get_data(url_countries, headers_dict)
        unit_data = get_data(url_units, headers_dict)

        for value in commodity_data:
            if value["commodityName"].lower() == commodity_name.lower():
                commodity_code = value['commodityCode']

        for value in country_data:
            if value["countryName"].lower() == country_name.lower():
                country_code = value['countryCode']

        url_data = "https://apps.fas.usda.gov/OpenData/api/psd/commodity/{}/country/{}/year/{}".format(commodity_code,
                                                                                                       country_code,
                                                                                                       market_year)
        main_data = get_data(url_data, headers_dict)
        logging.info("Api call Sucessful, data gathered...")

        df = pd.DataFrame(main_data)
        logging.info("Processing Data..")
        data_prerpocessing(df, unit_data, attribute_data)
        data = df.to_dict('records')
        logging.info("Inserting Data into Data_strore collection...")
        query = add_data_to_db(mydb, data, collection_data)
        if query == "Done":
            logging.info("Values Inserted...")
            logging.info("All Done!")
        else:
            logging.info("Some error occured in main function")


if __name__ == "__main__":
    main()
