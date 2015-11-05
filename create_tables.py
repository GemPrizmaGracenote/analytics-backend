#!/usr/bin/python

import json

import boto.exception
from boto.dynamodb2 import table
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.types import NUMBER

CONFIG_FILENAME = "config.json"
TABLE_PERIOD = 'hourly'
DEFAULT_READ_THROUGHPUT = 20
DEFAULT_WRITE_THROUGHPUT = 20

def main():
  config_file = file(CONFIG_FILENAME)
  config = json.load(config_file)
  for breakdown in config['breakdowns']:
    maybe_create_table(breakdown)

def maybe_create_table(breakdown):
  table_name = '_'.join([breakdown['name'], TABLE_PERIOD])
  try:
    table.Table.create(table_name, schema=[
        HashKey('key'),
        RangeKey('timestamp', data_type=NUMBER)
      ], throughput={
        'read': DEFAULT_READ_THROUGHPUT,
        'write': DEFAULT_WRITE_THROUGHPUT
        })
    print 'Creating table %s' % table_name
  except boto.exception.JSONResponseError as e:
    pass

if __name__ == '__main__':
  main()
