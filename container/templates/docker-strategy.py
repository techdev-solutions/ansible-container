# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import os
import sys
import hashlib

my_linear = sys.modules['ansible.plugins.strategy.linear']
del sys.modules['ansible.plugins.strategy.linear']
from ansible.plugins.strategy.linear import StrategyModule as LinearStrategyModule
sys.modules['ansible.plugins.strategy.linear'] = my_linear
try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()
from ansible.playbook.role import Role

import docker
from docker.utils import kwargs_from_env

NOOP_TASKS = [
    'TASK: setup',
    'TASK: meta (flush_handlers)'
]

def debug(msg):
    if os.environ.get('ANSIBLE_CONTAINER_DEBUG'):
        display.display('ac-strategy-debug: %s' % msg)

class StrategyModule(LinearStrategyModule):

    last_role_per_host = {}
    last_block_per_host = {}
    fingerprint_hash = {}
    _client = None
    project_name = 'ansible'

    def get_client(self):
        if not self._client:
            # To ensure version compatibility, we have to generate the kwargs ourselves
            client_kwargs = kwargs_from_env(assert_hostname=False)
            self._client = docker.AutoVersionClient(**client_kwargs)
            self.api_version = self._client.version()['ApiVersion']
            # Set the version in the env so it can be used elsewhere
            os.environ['DOCKER_API_VERSION'] = self.api_version
        return self._client

    def container_name_for_host(self, host):
        return 'ansible_%s_1' % (host,)

    def running_image_id_for_host(self, host):
        client = self.get_client()
        running_containers = client.containers(all=True, limit=1,
                                               filters={'name': self.container_name_for_host(host)})
        debug('%s' % running_containers)
        running_container = running_containers[0]
        return running_container['ImageID']

    def container_id_for_host(self, host):
        client = self.get_client()
        container_id, = client.containers(
            filters={'name': self.container_name_for_host(host)},
            limit=1, all=True, quiet=True
        )
        return container_id

    def commit_layer(self, host, layer_fingerprint):
        """
        Kills the running container for host and commits an image based on it,
        marking what the parent image ID is.
        """
        debug('commit_layer(%s), fingerprint %s' % (host, layer_fingerprint))
        client = self.get_client()
        parent_id = self.running_image_id_for_host(host)
        client.kill(self.container_name_for_host(host))
        image_config = dict(
            LABEL='com.ansible.container.parent_image="%s" '
                  'com.ansible.container.fingerprint="%s"' % (
                parent_id,
                layer_fingerprint)
        )
        display.display('Committing image...')
        result = client.commit(self.container_id_for_host(host),
                               message='Built using Ansible Container',
                               changes=u'\n'.join(
                                   [u'%s %s' % (k, v)
                                    for k, v in image_config.items()]
                               ))
        image_id = result['Id']
        return image_id

    def switch_to_image(self, host, image_id):
        """
        Destroys the existing container for the host and replaces it with a
        running container with the same configuration but using the provided image
        """
        client = self.get_client()
        container_config = client.inspect_container(self.container_id_for_host(host))['Config']
        # debug('%s' % container_config)
        name = self.container_name_for_host(host)
        client.remove_container(name)
        container_config['Image'] = image_id
        result = client.create_container_from_config(container_config, name=name)
        new_container_id = result['Id']
        client.start(container=new_container_id)

    def _get_next_task_lockstep(self, hosts, iterator):
        """
        Returns a list of (host, task) tuples, where the task may
        be a noop task to keep the iterator in lock step across
        all hosts.
        """
        if self._tqm._unreachable_hosts or self._tqm._failed_hosts:
            display.display('Build failed! Aborting')
            sys.exit(127)
        parent_results = super(StrategyModule, self)._get_next_task_lockstep(hosts, iterator)
        parent_results_dict = dict(parent_results)
        for host in hosts:
            display.display('Getting next task for %s' % host)
            task = parent_results_dict.get(host, None)
            debug('Host = %s, task = %s' % (host, task))
            role = getattr(task, '_role', None)
            # This changed between 2.1 and 2.2, so support both
            block = getattr(task, '_parent',
                            getattr(task, '_block', None) or getattr(task, '_task_include', None))
            debug('Current task role: %s, current task block: %s' % (
                role, block))
            if os.environ.get('ANSIBLE_CONTAINER_DEBUG') and repr(task) not in NOOP_TASKS:
                role_hash = self.fingerprint_hash.get(host, hashlib.sha256()).copy()
                block_hash = self.fingerprint_hash.get(host, hashlib.sha256()).copy()
                if role:
                    self.hash_role(role_hash, role)
                    debug('Role hash: %s' % role_hash.hexdigest())
                if block:
                    self.hash_block(block_hash, block)
                    debug('Block hash: %s' % block_hash.hexdigest())
            # So commit after every role, every play that has tasks that
            # are not in roles, and every include
            role_changed = (self.last_role_per_host.get(host, None) and
                           self.last_role_per_host[host] != role)
            # The linear strategy spits out a None task at the end of a play
            is_new_block = task is None or (not role and role_changed
                                            and repr(task) not in NOOP_TASKS)
            is_new_role = role and role_changed
            debug('is_new_block: %s, is_new_role: %s' % (is_new_block, is_new_role))
            if is_new_block or is_new_role:
                # Initialize the fingerprint hash with the current layer's
                # hash
                hash_obj = self.fingerprint_hash.get(host, hashlib.sha256())
                if is_new_role:
                    self.hash_role(hash_obj, self.last_role_per_host[host])
                else:
                    self.hash_block(hash_obj, self.last_block_per_host[host])
                image_id = self.commit_layer(host, hash_obj.hexdigest())
                self.switch_to_image(host, image_id)
                self.fingerprint_hash[host] = hash_obj
            # flush_handlers appears as a different block
            if repr(task) not in NOOP_TASKS:
                self.last_block_per_host[host] = block
            self.last_role_per_host[host] = role

        return parent_results

    def hash_task(self, hash_obj, task):
        # debug(u'Task attributes: %s' % task._attributes)
        hash_obj.update(repr(task._attributes))

    def hash_block(self, hash_obj, play):
        # debug(u'Play attributes: %s' % play._attributes)
        hash_obj.update(repr(play._attributes))
        for task in play.block:
            self.hash_task(hash_obj, task)

    def hash_file(self, hash_obj, file_path):
        blocksize = 64 * 1024
        with open(file_path, 'rb') as ifs:
            while True:
                data = ifs.read(blocksize)
                if not data:
                    break
                hash_obj.update(data)

    def hash_dir(self, hash_obj, dir_path):
        # debug('hash_dir: dir_path = %s' % (dir_path,))
        for root, dirs, files in os.walk(dir_path, topdown=True):
            for file_path in files:
                abs_file_path = os.path.join(root, file_path)
                # debug('hash_dir: abs_file_path = %s' % (abs_file_path,))
                hash_obj.update(abs_file_path)
                self.hash_file(hash_obj, abs_file_path)

    def hash_role(self, hash_obj, role):
        # A role is easy to hash - the hash of the role content with the
        # hash of any role dependencies it has
        role_path = role._role_path
        # debug('hash_role: role_path = %s' % (role_path,))
        self.hash_dir(hash_obj, role_path)
        for dependency in role._dependencies:
            self.hash_role(hash_obj, dependency)



