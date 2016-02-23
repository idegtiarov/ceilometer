#
# Copyright 2016 Mirantis Inc.
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
import collections
import datetime
import re
import uuid

import oslo_cache as cache
from oslo_config import cfg
from oslo_log import log

from ceilometer.event.storage import models
from ceilometer import transformer
from ceilometer import utils

LOG = log.getLogger(__name__)

cache_opts = [
    cfg.StrOpt('backend',
               default='dogpile.cache.memcached',
               help='Cache backend'),
    cfg.StrOpt('backend_url',
               default='localhost:11211',
               help='Url for cache database connection')
    ]
cfg.CONF.register_opts(cache_opts, group='cache')
region = cache.create_region()

mcache = region.configure(cfg.CONF.cache.backend,
                          arguments={'url': cfg.CONF.cache.backend_url})


class EventBracketerTransformer(transformer.TransformerBase):
    """Transformer to handle event sequence"""

    grouping_keys = None

    event_param_match = re.compile(r'\$([\w\.]+)\(([\w:_]+)\)')

    def __init__(self, events=None, target=None):
        """Initialized transformer with configured parameters

        :param events: list of dicts, each dict contain type of required event
        and optional conditions for other events fields
        :param target: dict containing target event type and trait definitions
        """
        self.events = events or []
        self.target = target or {}
        self.sequence = ([ev['event_type'] for ev in events]
                         if events else [])
        self.target_event_params = self._required_params(target)

    def _required_params(self, target=None):
        if not target:
            return
        params = collections.defaultdict(list)
        for trait in target["traits"]:
            for item in self.event_param_match.findall(trait["value"]):
                params[item[0]].append(item[1])
        return params

    @staticmethod
    def _event_resource(resource, traits):
        for trait in traits:
            if trait.name == resource:
                return trait.value

    def _creat_marker(self):
        return [None for i in self.sequence]

    def _create_target_dict(self):
        return dict()

    def _fill_cache_params(self, event=None):
        params_cached = {}
        for param in self.target_event_params[event.event_type]:
            if param.startswith('trait:'):
                for trait in event.traits:
                    if trait.name == param[len('trait:'):]:
                        params_cached.update({trait.name: trait.value})
            elif param == 'generated':
                date = int(utils.dt_to_decimal(getattr(event, param)))
                params_cached.update({param: str(date)})
            else:
                params_cached.update({param: str(getattr(event, param))})
        return params_cached

    def _create_transform_event(self, tgt_cache_params=None):

        traits = []

        def _replace(match):
            event_type = match.group(1)
            p = tgt_cache_params[event_type]
            if match.group(2) in p:
                return p[match.group(2)]

        for trait in self.target["traits"]:
            value = re.sub(self.event_param_match, _replace, trait['value'])

            traits.append(models.Trait(name=trait["name"], dtype=trait["type"],
                                       value=eval(value)))

        return models.Event(event_type=self.target["event_type"],
                            generated=datetime.datetime.utcnow(),
                            message_id=uuid.uuid4(),
                            traits=traits,
                            raw=self.target.get('raw', {}))

    def handle_sample(self, context, event):

        if event.event_type not in self.sequence:
            # TODO(idegtiarov): add optional conditions checking
            return
        resource = self._event_resource('instance_id', event.traits)

        mkr_key = 'brt_marker_' + resource
        tgt_key = 'brk_tgt_' + resource
        marker = mcache.get_or_create(mkr_key, self._creat_marker)

        ev_position = self.sequence.index(event.event_type)

        marker[ev_position] = 1
        mcache.set(mkr_key, marker)

        tgt_cache_params = mcache.get_or_create(tgt_key,
                                                self._create_target_dict)
        if event.event_type in self.target_event_params:
            tgt_cache_params[event.event_type] = self._fill_cache_params(
                    event
            )
            mcache.set(tgt_key, tgt_cache_params)

        if (None not in marker and
                len(tgt_cache_params) == len(self.target_event_params)):
            transformed_event = self._create_transform_event(
                    tgt_cache_params
            )
            mcache.delete(tgt_key)
            mcache.delete(mkr_key)
            return transformed_event

    def flush(self, context):
        pass
