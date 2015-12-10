
import datetime
import elasticsearch as es
from elasticsearch import helpers
import uuid

cli = es.Elasticsearch(['localhost', '192.168.122.223'])

user_id = ['40ed36e162704308a60f90c178e64502',
           '236f91d276384f6aab93445fd8c4d2a4',
           '99e130ad66b74865b0c34f5a1895559a',
           '763983a38ab74da49e5ed62e6efec2ad',
           'cb7d4a4000934e349e13bfbefdc19790',
           '167850fef75a4f578464fbbb9748cb31']
project_id = ['2df45f4975d742ecbbd8d517a1cb7bda',
              '63a09ce332674142a6603216d0c2af63']
metadata = {"state_description": "state_is_good",
            "event_type": "compute.instance.update",
            "availability_zone": "everywhere"}


def create_resources(resources_number=None):
    index = 'resource'
    r_type = 'openstack'
    ts = datetime.datetime.now().isoformat()

    def _build_bulk_resources(resources_number):
        for i in range(resources_number):
            yield {
                '_op_type': 'create',
                '_index': index,
                '_type': r_type,
                '_id': str(uuid.uuid4()),
                '_source': {
                    'start_sample_timestamp': ts,
                    'last_sample_timestamp': (datetime.datetime.now().
                                              isoformat()),
                    'project_id': project_id[i % len(project_id)],
                    'user_id': user_id[i % len(user_id)],
                    'metadata': metadata
                }
            }
    for ok, result in helpers.streaming_bulk(
            cli, _build_bulk_resources(resources_number)):
        if not ok:
            __, result = result.popitem()
            print result
    return 'created %s resources' % resources_number


def create_meters(meters_number):
    index = 'meter'
    id = 'meter_name_'
    def _build_bulk_meters(meters_number):
        for i in range(meters_number):
            yield {
                '_op_type': 'create',
                '_index': index,
                '_type': str(uuid.uuid4()),
                '_id': id + str(i),
                '_source': {
                    'project_id': project_id[i % len(project_id)],
                    'user_id': user_id[i % len(user_id)],
                    'meter_type': 'integer',
                    'meter_unit': i,
                    'source': 'openstack',
                    'metadata': metadata
                }
            }
    for ok, result in helpers.streaming_bulk(
            cli, _build_bulk_meters(meters_number)):
        if not ok:
            __, result = result.popitem()
            print result
    return 'created %s meters' % meters_number

if __name__ == "__main__":
    print create_resources(1000)
    print create_meters(100)
