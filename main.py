import xml.etree.ElementTree as ET
import requests
import asyncio
import aiohttp
from configparser import ConfigParser
import argparse
import os
import sys
import time
import sqlite3
import mysql.connector


def parse_xml(xml_data, sitename, section, db_tools):
    result = []

    # root = ET.parse(filename).getroot()
    root = ET.fromstring(xml_data)
    if root.find('./header/name').text != sitename:
        db_tools.drop_db_on_error(section)
    else:
        for row in root.iter('row'):
            pre_dict = {}
            for key in row:
                pre_dict[key.tag] = key.text
            result.append(pre_dict)
        return result


class DbTooling():

    def __init__(self, config):
        self.db_type = ''
        try:
            if config['General']['db_sqlite'] == 'yes':
                self.db_connect = sqlite3.connect('sqlite3.db')
                self.cursor = self.db_connect.cursor()
                self.db_type = 'sqlite'
            else:
                self.db_connect = mysql.connector.MySQLConnection(host=config['General']['db_address'],    # your host, usually localhost
                                                 user=config['General']['db_username'],         # your username
                                                 passwd=config['General']['db_password'],  # your password
                                                 database='asterix')        # name of the data base
                self.cursor = self.db_connect.cursor()
                self.db_type = 'mysql'
        except Exception as err:
            print(err)

    def __del__(self):
        # close connection
        self.cursor.close()
        self.db_connect.close()

    def create_db(self, xml_data, section):
        # prepare data
        fields = []
        fieldsn = []
        values = []
        for k in xml_data[0]:
            fields.append(k + ' integer')
            fieldsn.append(k)

        for k in xml_data:
            values.append(tuple(k.values()))

        # first need to drop and create new db every time
        sql_drop_statement = '''DROP TABLE IF EXISTS {table_name};'''.format(table_name=section)
        sql_create_statement = '''CREATE TABLE {table_name} ({fields});'''.format(
            table_name=section, fields=', '.join(fields))
        try:
            self.cursor.execute(sql_drop_statement)
            self.cursor.execute(sql_create_statement)
        except sqlite3.DatabaseError as err:
            print("Error: ", err)

        # insert each row
        for x in values:
            sql_insert_statement = '''INSERT INTO {table_name} ({fields})'''.format(
                table_name=section, fields=', '.join(fieldsn)) + ''' VALUES {values}'''.format(values=(tuple(x)))
            try:
                self.cursor.execute(sql_insert_statement)
            except sqlite3.DatabaseError as err:
                print("Error: ", err)

        # fetch all
        self.db_connect.commit()

    def drop_db_on_error(self, section):
        print("DROPING DB!")
        # db connection part
        sql_drop_statement = '''DROP TABLE IF EXISTS {table_name};'''.format(table_name=section)
        self.db_connect.commit()


def download_xml_file(config, section, db_tools):
    url = config[section]['link']
    try:
        response = requests.get(url, timeout=1.5)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        db_tools.drop_db_on_error(section)
        return None
    else:
        return response.text

@asyncio.coroutine
def routine(config, section, db_tools):
    xlm_response = download_xml_file(config, section, db_tools)
    if xlm_response:
        xlm_parsed = parse_xml(xlm_response, config[section]['name'], section, db_tools)
        db_tools.create_db(xlm_parsed, section)

def process(opts):
    """

    :param opts:
    :return:
    """

    if not os.path.exists(opts.config_file):
        sys.stderr.write("Config `%s` not found or inaccessible" % (
            opts.config_file,))
        return -1
    config = ConfigParser()
    config.read(opts.config_file)

    # init db tooling class
    db_tools = DbTooling(config)

    # use process only for www_ sections
    sections = []
    for section in config.sections():
        if not section.startswith('www_'):
            continue
        sections.append(section)
        # routine(config, section, db_tools)

    exe_looper = [routine(config, section, db_tools) for section in sections]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(exe_looper))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config_file',
                        required=False, metavar='config file',
                        default='config.ini',
                        help='configuration file placement')
    parser.add_argument('-v', '--verbose', dest='verbosity',
                        help='verbose level', default=0,
                        type=int,
                        required=False)
    arguments = parser.parse_args()

    return process(arguments)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
