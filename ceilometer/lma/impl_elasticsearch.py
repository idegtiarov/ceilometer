#
# Copyright 2015 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import datetime
import operator

import elasticsearch as es
from elasticsearch import helpers
from oslo_log import log
from oslo_utils import netutils
from oslo_utils import timeutils
import six

from ceilometer.storage import base
from ceilometer.storage import models
from ceilometer.i18n import _LE, _LI
from ceilometer import storage
from ceilometer import utils

LOG = log.getLogger(__name__)

AVAILABLE_CAPABILITIES = {
    'meters': {'query': {'simple': True,
                         'metadata': True}},
    'resources': {'query': {'simple': True,
                            'metadata': True}},
}


AVAILABLE_STORAGE_CAPABILITIES = {
    'storage': {'production_ready': True},
}


class Connection(base.Connection):
    """Put the resource data into an ElasticSearch db.

    Resources in ElasticSearch are indexed by source and stored by resource_id.
    An example document::

      {"_index":"resource",
       "_type":"<source>",
       "_id":"54245d86-5ab3-4cbb-91f6-ef3e6f422490",
       "_source":{"first_sample_timestamp": "2015-09-01T12:02:08.265497",
                  "last_sample_timestamp": "2015-12-09T21:47:53.234554",
                  "project_id": "69be3916711344acbabba65be7411193",
                  "user_id": "45dcc01d00234f01aba1ea68cb57495c",
                  "metadata": {
                      "state_description" : "",
                      "event_type" : "compute.instance.update",
                      "availability_zone" : null,
                      ...
                      }
                 }
      }

    Meters in ElasticSearch are indexed by resource_id and stored by meter name
    An example document::

      {"_index":"meter",
       "_type":"54245d86-5ab3-4cbb-91f6-ef3e6f422490",
       "_id":"<counter_name>"
       "_source":{"project_id": "69be3916711344acbabba65be7411193",
                  "user_id": "45dcc01d00234f01aba1ea68cb57495c",
                  "meter_type": "<counter_type>",
                  "meter_unit": "<counter_unit>"
                  "source": <source>,
                  "metadata": {
                      "state_description" : "",
                      "event_type" : "compute.instance.update",
                      "availability_zone" : null,
                      ...
                      }
      }
    """

    CAPABILITIES = utils.update_nested(base.Connection.CAPABILITIES,
                                       AVAILABLE_CAPABILITIES)
    STORAGE_CAPABILITIES = utils.update_nested(
        base.Connection.STORAGE_CAPABILITIES,
        AVAILABLE_STORAGE_CAPABILITIES,
    )

    index_resource = 'resource'
    index_meter = 'meter'

    def __init__(self, url):
        url_split = netutils.urlsplit(url)
        self.conn = es.Elasticsearch(url_split.netloc)
        # self.conn = es.Elasticsearch(url)

    def upgrade(self):
        iclient = es.client.IndicesClient(self.conn)
        template = {
            'template': '*',
            'mappings': {'_default_':
                         {'properties':
                          {'first_sample_timestamp': {'type': 'date'},
                           'last_sample_timestamp': {'type': 'date'},
                           'metadata': {'type': 'nested'}
                           }
                          }
                         }
        }
        iclient.put_template(name='resource_template', body=template)

    def get_resources(self, user=None, project=None, source=None,
                      start_timestamp=None, start_timestamp_op=None,
                      end_timestamp=None, end_timestamp_op=None,
                      metaquery=None, resource=None, limit=None):
        """Return an iterable of models.Resource instances

        :param user: Optional ID for user that owns the resource.
        :param project: Optional ID for project that owns the resource.
        :param source: Optional source filter.
        :param start_timestamp: Optional modified timestamp start range.
        :param start_timestamp_op: Optional start time operator, like gt, ge.
        :param end_timestamp: Optional modified timestamp end range.
        :param end_timestamp_op: Optional end time operator, like lt, le.
        :param metaquery: Optional dict with metadata to match on.
        :param resource: Optional resource filter.
        :param limit: Maximum number of results to return.
        """
        if limit == 0:
            return

        q_args = {}
        filters = []

        if limit is not None:
            q_args['size'] = limit

        q_args['index'] = self.index_resource

        if source is not None:
            q_args['doc_type'] = source
        if resource is not None:
            q_args['_id'] = resource
        if user is not None:
            filters.append({'term': {'user_id': user}})
        if project is not None:
            filters.append({'term': {'project_id': project}})

        if start_timestamp or end_timestamp:
            # time_filter = {'last_sample_timestamp': {}}
            ts_filter = {}
            st_op = start_timestamp_op or 'gte'
            et_op = end_timestamp_op or 'lt'
            if start_timestamp:
                ts_filter[st_op] = start_timestamp
            if end_timestamp:
                ts_filter[et_op] = end_timestamp
            filters.append({'range': {'last_sample_timestamp': ts_filter}})
        if metaquery is not None:
            nest_filter = []
            for key, value in six.iteritems(metaquery):
                nest_filter.append({'term': {'metadata.%s' % key: value}})
            filters.append({
                'nested': {
                    'path': 'metadata',
                    'query': {
                        'filtered': {
                            'filter': {'bool': {'must': nest_filter}}}}
            }})
        q_args['body'] = {'query': {'filtered':
                                    {'filter': {'bool': {'must': filters}}}}}
        results = self.conn.search(fields=['_type', '_id', '_source'],
                                   **q_args)
        for record in results['hits']['hits']:
            yield models.Resource(
                resource_id=record['_id'],
                first_sample_timestamp=(record['_source'].get('first_sample_timestamp')),
                last_sample_timestamp=(record['_source']
                                       ['last_sample_timestamp']),
                source=record['_type'],
                project_id=record['_source'].get('project_id'),
                user_id=record['_source'].get('user_id'),
                metadata=record['_source']['metadata']
            )

    def get_meters(self, user=None, project=None, resource=None, source=None,
                   metaquery=None, limit=None):
        """Return an iterable of models.Meter instances

        :param user: Optional ID for user that owns the resource.
        :param project: Optional ID for project that owns the resource.
        :param resource: Optional resource filter.
        :param source: Optional source filter.
        :param metaquery: Optional dict with metadata to match on.
        :param limit: Maximum number of results to return.
        """
        if limit == 0:
            return

        q_args = {}
        filters = []

        if limit is not None:
            q_args['size'] = limit
        q_args['index'] = self.index_meter

        if resource is not None:
            q_args['doc_type'] = resource
        if user is not None:
            filters.append({'term': {'user_id': user}})
        if project is not None:
            filters.append({'term': {'project_id': project}})
        if source is not None:
            filters.append({'term': {'source': source}})

        if metaquery is not None:
            nest_filter = []
            for key, value in six.iteritems(metaquery):
                nest_filter.append({'term': {'metadata.%s' % key: value}})
            filters.append({
                'nested': {
                    'path': 'metadata',
                    'query': {
                        'filtered': {
                            'filter': {'bool': {'must': nest_filter}}}}
            }})
        q_args['body'] = {'query': {'filtered':
                                    {'filter': {'bool': {'must': filters}}}}}
        results = self.conn.search(fields=['_type', '_id', '_source'],
                                   **q_args)
        for record in results['hits']['hits']:
            yield models.Meter(
                name=record['_id'],
                type=record['_source']['meter_type'],
                unit=record['_source']['meter_unit'],
                resource_id=record['_type'],
                source=record['_source']['source'],
                project_id=record['_source'].get('project_id'),
                user_id=record['_source'].get('user_id'),
            )
