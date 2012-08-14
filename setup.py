#!/usr/bin/env python

"""py-lightstreamer distutils script.
"""

from distutils.core import setup


setup(
    name =          'py-lightstreamer',
    version =       '0.1',
    description =   'Lightstreamer HTTP client for Python.',
    author =        'David Wilson',
    author_email =  'dw@botanicus.net',
    license =       'AGPL3',
    url =           'http://github.com/dw/py-lightstreamer/',
    py_modules =    ['lightstreamer']
)
