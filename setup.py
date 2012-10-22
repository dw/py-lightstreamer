#!/usr/bin/env python
#
# Copyright 2012, the py-lightstreamer authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""py-lightstreamer distutils script.
"""

from setuptools import setup


setup(
    name='py-lightstreamer',
    version='0.2',
    description='Lightstreamer HTTP client for Python.',
    author='David Wilson',
    author_email='dw@botanicus.net',
    license='Apache 2',
    url='http://github.com/dw/py-lightstreamer/',
    py_modules=['lightstreamer'],
    install_requires=[
        'requests'
    ]
)
