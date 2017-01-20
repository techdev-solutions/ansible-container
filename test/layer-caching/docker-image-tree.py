#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import os
import sys
import docker
from docker.utils import kwargs_from_env

client_kwargs = kwargs_from_env(assert_hostname=False)
client = docker.AutoVersionClient(**client_kwargs)
api_version = client.version()['ApiVersion']
# Set the version in the env so it can be used elsewhere
os.environ['DOCKER_API_VERSION'] = api_version

image_id_or_name = sys.argv[1]

image_id, = client.images(image_id_or_name, quiet=True)

while image_id:
    print 'Image:', image_id
    image_data = client.inspect_image(image_id)
    if image_data['RepoTags']:
        print 'Name:', image_data['RepoTags'][0]
    print 'Fingerprint:', image_data['Config']['Labels'].get('com.ansible.container.fingerprint', '*missing*')
    parent = image_data['Config']['Labels'].get('com.ansible.container.parent_image', '')
    print 'Parent Image:', parent or '*missing*'
    image_id = parent
    print ''

