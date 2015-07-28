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

import functools

import novaclient
from novaclient import client as nova_client
from oslo_config import cfg
from oslo_log import log


OPTS = [
    cfg.BoolOpt('nova_http_log_debug',
                default=False,
                help='Allow novaclient\'s debug log output.'),
]

SERVICE_OPTS = [
    cfg.StrOpt('nova',
               default='compute',
               help='Nova service type.'),
]

cfg.CONF.register_opts(OPTS)
cfg.CONF.register_opts(SERVICE_OPTS, group='service_types')
cfg.CONF.import_opt('http_timeout', 'ceilometer.service')
cfg.CONF.import_group('service_credentials', 'ceilometer.service')

LOG = log.getLogger(__name__)


def logged(func):

    @functools.wraps(func)
    def with_logging(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            LOG.exception(e)
            raise

    return with_logging


class Client(object):
    """A client which gets information via python-novaclient."""

    def __init__(self, bypass_url=None, auth_token=None):
        """Initialize a nova client object."""
        conf = cfg.CONF.service_credentials
        tenant = conf.os_tenant_id or conf.os_tenant_name
        self.nova_client = nova_client.Client(
            version=2,
            username=conf.os_username,
            api_key=conf.os_password,
            project_id=tenant,
            auth_url=conf.os_auth_url,
            auth_token=auth_token,
            region_name=conf.os_region_name,
            endpoint_type=conf.os_endpoint_type,
            service_type=cfg.CONF.service_types.nova,
            bypass_url=bypass_url,
            cacert=conf.os_cacert,
            insecure=conf.insecure,
            timeout=cfg.CONF.http_timeout,
            http_log_debug=cfg.CONF.nova_http_log_debug,
            no_cache=True)

    def _with_flavor_and_image(self, instances):
        flavor_cache = {}
        image_cache = {}
        for instance in instances:
            self._with_flavor(instance, flavor_cache)
            self._with_image(instance, image_cache)

        return instances

    def _with_flavor(self, instance, cache):
        fid = instance.flavor['id']
        if fid in cache:
            flavor = cache.get(fid)
        else:
            try:
                flavor = self.nova_client.flavors.get(fid)
            except novaclient.exceptions.NotFound:
                flavor = None
            cache[fid] = flavor

        attr_defaults = [('name', 'unknown-id-%s' % fid),
                         ('vcpus', 0), ('ram', 0), ('disk', 0),
                         ('ephemeral', 0)]

        for attr, default in attr_defaults:
            if not flavor:
                instance.flavor[attr] = default
                continue
            instance.flavor[attr] = getattr(flavor, attr, default)

    def _with_image(self, instance, cache):
        try:
            iid = instance.image['id']
        except TypeError:
            instance.image = None
            instance.kernel_id = None
            instance.ramdisk_id = None
            return

        if iid in cache:
            image = cache.get(iid)
        else:
            try:
                image = self.nova_client.images.get(iid)
            except novaclient.exceptions.NotFound:
                image = None
            cache[iid] = image

        attr_defaults = [('kernel_id', None),
                         ('ramdisk_id', None)]

        instance.image['name'] = (
            getattr(image, 'name') if image else 'unknown-id-%s' % iid)
        image_metadata = getattr(image, 'metadata', None)

        for attr, default in attr_defaults:
            ameta = image_metadata.get(attr) if image_metadata else default
            setattr(instance, attr, ameta)

    @logged
    def instance_get_all_by_host(self, hostname):
        """Returns list of instances on particular host."""
        search_opts = {'host': hostname, 'all_tenants': True}
        return self._with_flavor_and_image(self.nova_client.servers.list(
            detailed=True,
            search_opts=search_opts))

    @logged
    def instance_get_all(self, since=None):
        """Returns list of all instances.

        If since is supplied, it will return the instances changes since that
        datetime. since should be in ISO Format '%Y-%m-%dT%H:%M:%SZ'
        """
        search_opts = {'all_tenants': True}
        if since:
            search_opts['changes-since'] = since
        return self.nova_client.servers.list(
            detailed=True,
            search_opts=search_opts)

    @logged
    def floating_ip_get_all(self):
        """Returns all floating ips."""
        return self.nova_client.floating_ips.list()
