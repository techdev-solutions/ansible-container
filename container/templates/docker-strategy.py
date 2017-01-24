# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import os
import sys
import hashlib
import collections

my_linear = sys.modules['ansible.plugins.strategy.linear']
del sys.modules['ansible.plugins.strategy.linear']
from ansible.plugins.strategy.linear import StrategyModule as LinearStrategyModule
sys.modules['ansible.plugins.strategy.linear'] = my_linear
try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()
from ansible.playbook.task import Task

import docker
from docker.utils import kwargs_from_env

NOOP_TASKS = [
    'TASK: setup',
    'TASK: Gathering Facts',
    'TASK: meta (flush_handlers)'
]

def debug(msg):
    if os.environ.get('ANSIBLE_CONTAINER_DEBUG'):
        display.display('ac-strategy-debug: %s' % msg)

class StrategyModule(LinearStrategyModule):

    last_role_per_host = {}
    last_block_per_host = {}
    fingerprint_hash = collections.defaultdict(hashlib.sha256)
    cache_is_busted = collections.defaultdict(lambda: False)
    is_new_play = collections.defaultdict(lambda: True)
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

    def commit_layer(self, host):
        """
        Kills the running container for host and commits an image based on it,
        marking what the parent image ID is.
        """
        layer_fingerprint = self.fingerprint_hash[host].hexdigest()
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
        container_info = client.inspect_container(self.container_id_for_host(host))
        container_config = container_info['Config']
        # debug('%s' % container_config)
        name = self.container_name_for_host(host)
        if container_info['State']['Running']:
            client.kill(name)
        client.remove_container(name)
        container_config['Image'] = image_id
        result = client.create_container_from_config(container_config, name=name)
        new_container_id = result['Id']
        client.start(container=new_container_id)

    def get_cached_layer(self, host, fingerprint):
        client = self.get_client()
        try:
            image_id = client.images(
                all=True, quiet=True,
                filters={'label': 'com.ansible.container.fingerprint=%s'
                                  % fingerprint})[0]
            # Ensure the parent matches too
            # image_data = client.inspect_image(image_id)
            # parent_image_label = image_data['Config']['Labels']['com.ansible.container.parent_image']
            # assert parent_image_label == parent_id
            return image_id
        except IndexError:
            # There was no image returned by Docker
            return None
        except KeyError:
            # The image didn't have the parent_image label
            debug('Missing parent_image label on image %s' % image_id)
            return None
        except AssertionError:
            # The parent_image label didn't match the parent image ID
            # debug('Mismatched parent_image label on image %s - %s != %s' % (
            #     image_id, parent_id, parent_image_label
            # ))
            return None

    def make_noop_task(self, loader):
        noop_task = Task()
        noop_task.action = 'meta'
        noop_task.args['_raw_params'] = 'noop'
        noop_task.set_loader(loader)
        return noop_task

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
            # debug('Host state for %s is %s' % (host, iterator.get_host_state(host)))
            task = parent_results_dict.get(host, None)
            debug('Host = %s, task = %s' % (host, task))
            role = getattr(task, '_role', None)
            # This changed between 2.1 and 2.2, so support both
            block = getattr(task, '_parent',
                            getattr(task, '_block', None) or getattr(task, '_task_include', None))
            debug('Current task role: %s, current task block: %s' % (
                role, block))
            # things like flush_handlers are special and don't impact layering
            if repr(task) in NOOP_TASKS:
                continue

            current_layer_hash = self.fingerprint_hash[host].copy()
            role_changed = (self.last_role_per_host.get(host, None) and
                           self.last_role_per_host[host] != role)
            if role_changed:
                self.hash_role(current_layer_hash,
                               self.last_role_per_host[host])
            if role:
                self.hash_role(current_layer_hash, role)
                debug('Role hash: %s' % current_layer_hash.hexdigest())
            elif block:
                self.hash_block(current_layer_hash, block)
                debug('Block hash: %s' % current_layer_hash.hexdigest())

            is_new_play = self.is_new_play[host]

            # At the beginning of new plays, if cache isn't busted yet, we should
            # check to see if we need to load a cached image
            if is_new_play and not self.cache_is_busted[host]:
                image_id = self.get_cached_layer(host, current_layer_hash.hexdigest())
                if image_id:
                    # There's an image already built
                    debug('Found previously cached image for this layer: %s' % image_id)
                    self.switch_to_image(host, image_id)
                else:
                    # There's no image built
                    debug('Did not find previously cached image for this layer.')
                    self.cache_is_busted[host] = True

            # So commit after every play and after every role that is not the
            # last thing in a play

            # The linear strategy spits out a None task at the end of a play
            # So the first part of the "or" is for that. The second part covers
            # the case where we have tasks as well as roles in a play, the last
            # role finished but the play isn't over.
            is_new_block = task is None or (not role and role_changed)
            is_new_role = role and role_changed
            debug('is_new_block: %s, is_new_role: %s' % (is_new_block, is_new_role))
            if is_new_block or is_new_role:
                if role_changed:
                    self.hash_role(self.fingerprint_hash[host],
                                   self.last_role_per_host[host])
                else:
                    self.hash_block(self.fingerprint_hash[host],
                                    self.last_block_per_host[host])
                # Only commit if cache is busted for this host
                if self.cache_is_busted[host]:
                    debug('Cache already busted for host %s, so committing' % host)
                    image_id = self.commit_layer(host)
                    self.switch_to_image(host, image_id)
                else:
                    # If task is None, we finished stepping through what we knew
                    # was cached, but we don't know whether there's a cached layer
                    # for what's next, and we won't know that until the next iteration.
                    # However if we ended a role and that's why we're here, the
                    # task we're about to return is part of the next layer, so we
                    # _can_ check to see if cache is preserved.
                    if task is not None:
                        image_id = self.get_cached_layer(host,
                                                         current_layer_hash.hexdigest())
                        if image_id:
                            # cache is still intact
                            debug('Switching host %s to cached image %s' % (host, image_id))
                            self.switch_to_image(host, image_id)
                        else:
                            # cache is sooooo busted!
                            debug('No image found for new block! Cache busted for %s' % host)
                            self.cache_is_busted[host] = True

            self.is_new_play[host] = task is None
            self.last_block_per_host[host] = block
            self.last_role_per_host[host] = role


        # return [(host, task if self.cache_is_busted.get(host, False) else noop_task)
        #         for (host, task) in parent_results]
        return [(host, (parent_results_dict[host]
                        if self.cache_is_busted[host] or task is None
                        else self.make_noop_task(iterator._play._loader)))
                for host in hosts]

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
