import sys
import os
import os.path
import simplejson as json
import singer
import requests
import math
import dateutil.parser
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


LOGGER = singer.get_logger()

class TelemetricsReportingService:

    def __init__(self, stream, schema, config):
        self.config = config
        self.stream = stream
        self.props = schema["properties"]
        self.api_url = 'https://api.telmetrics.com/v3/api/calls'
        self.access_token = config['organizationToken']

    
    def get_reports(self):
        date_ranges = self.parse_range(self.config['dateRange'])
        for date_range in date_ranges:
            self.retrieve_report_by_range(date_range)

    def retrieve_report_by_range(self, date_range):
        LOGGER.info(f'Retrieving page 1 for {date_range["start"]} - {date_range["end"]}')
        params = {
            'pagenumber': 1,
            'pagesize': 1000,
            'startdateutc': date_range['start'],
            'enddateutc': date_range['end']
        }
        headers = {
            'x-organization-token': self.access_token
        }

        resp = requests.get(self.api_url, params, headers=headers).json()
        self.process_data(resp, 1)
        total_pages = math.ceil(resp['paging']['total']/params['pagesize'])
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(lambda index: self.retrieve_page(index, total_pages, params, headers), list(range(2, total_pages + 1)))

    def retrieve_page(self, index, total_pages, params, headers):
        LOGGER.info(f'Retrieving data for page {index} of {total_pages}')
        params['pagenumber'] = index
        resp = requests.get(self.api_url, params, headers=headers).json()
        self.process_data(resp, index)

    def process_data(self, data, index):
        prop_list = list(map(lambda x: x[0], self.props.items()))
        items = data['results']
        for item in items:
            record = {}
            for field in prop_list:
                record[field] = ''
            self.scan(item, '', record, index)
            singer.write_record(self.stream, record)

    def parse_range(self, date_range):
        output = []

        if date_range == 'YESTERDAY':
            yesterday_date = datetime.today() - timedelta(days=1)
            output.append(self.format_range(yesterday_date, yesterday_date))
        else:
            range_size = 5
            range_parts = date_range.split(',')
            date_format = "%Y%m%d"
            start = datetime.strptime(range_parts[0], date_format)
            end = datetime.strptime(range_parts[1], date_format)

            while (end - start).days >= (range_size - 1):
                new_start = start + timedelta(days=range_size - 1)
                output.append(self.format_range(start, new_start))
                start = new_start + timedelta(days=1)

            if (end - start).days > 0:
                output.append(self.format_range(start, end))

        return output

    def format_range(self, start, end):
        return {
            'start': start.replace(microsecond=0, second=0, minute=0, hour=0).isoformat(),
            'end': end.replace(microsecond=0, second=59, minute=59, hour=23).isoformat()
        }

    def scan(self, obj, prefix, output, index):
        prefix = f'{prefix}_' if prefix else ''
        for prop in obj.items():
            name = prop[0]
            tp = type(prop[1]).__name__
            full_name = f'{prefix}{name}'
            if tp not in ['dict', 'list']:
                output[full_name] = self.map_value(full_name, prop[1], index)
            if tp == 'dict':
                self.scan(prop[1], name, output, index)

    def map_value(self, key, value, index):
        if key in self.props:
            prop_type = self.props[key]['type']

            if prop_type == 'integer':
                value = int(value)
            elif prop_type== 'number':
                value = float(value)
            
            return value
        else:
            LOGGER.info(f'Key {key} not found on page {index}')