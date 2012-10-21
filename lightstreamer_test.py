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

import threading
import unittest

from lightstreamer import TransientError
from lightstreamer import Dispatcher
from lightstreamer import LsClient
from lightstreamer import WorkQueue
from lightstreamer import _decode_field
from lightstreamer import _replace_url_host


class DispatcherTestCase(unittest.TestCase):
    def setUp(self):
        self.fired1 = []
        self.fired2 = []
        self.disp = Dispatcher()

    def _callback1(self, *args, **kwargs):
        self.fired1.append((args, kwargs))

    def _callback2(self, *args, **kwargs):
        self.fired2.append((args, kwargs))

    def _crash(self):
        raise RuntimeError('ignore this error')

    def test_fire_no_listeners(self):
        assert self.disp.listening('eek') == 0
        self.disp.dispatch('eek')

    def test_single_fire(self):
        self.disp.listen('eek', self._callback1)
        assert self.disp.listening('eek') == 1
        self.disp.dispatch('eek', 1, a=2)
        assert self.fired1 == [((1,), {'a': 2})]

    def test_multiple_fire(self):
        self.disp.listen('eek', self._callback1)
        self.disp.listen('eek', self._callback2)
        assert self.disp.listening('eek') == 2
        self.disp.dispatch('eek', 1, a=2)
        assert self.fired1 == [((1,), {'a': 2})]
        assert self.fired2 == [((1,), {'a': 2})]

    def test_crash(self):
        self.disp.listen('eek', self._crash)
        self.disp.dispatch('eek')

    def test_ignore_dups(self):
        self.disp.listen('eek', self._callback1)
        self.disp.listen('eek', self._callback1)
        assert self.disp.listening('eek') == 1
        self.disp.dispatch('eek')
        assert self.fired1 == [((), {})]

    def test_unlisten(self):
        self.disp.listen('eek', self._callback1)
        self.disp.unlisten('eek', self._callback1)
        assert self.disp.listening('eek') == 0
        self.disp.dispatch('eek', 1, a=2)
        assert self.fired1 == []


class WorkQueueTestCase(unittest.TestCase):
    def setUp(self):
        self.wq = WorkQueue()

    def tearDown(self):
        self.wq.stop()

    def _capture_thread(self):
        self.invoking_thread = threading.currentThread()
        self.sem.release()

    def test_fire(self):
        self.sem = threading.Semaphore(0)
        self.wq.push(self._capture_thread)
        self.sem.acquire()
        assert self.invoking_thread != threading.currentThread()


class DecodeFieldTestCase(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(_decode_field('$'), '')
        self.assertEqual(_decode_field('$', '$'), '')

    def test_null(self):
        self.assertTrue(_decode_field('#') is None)
        self.assertTrue(_decode_field('#', '#') is None)

    def test_prev(self):
        self.assertTrue(_decode_field('', None) is None)
        self.assertTrue(_decode_field('', '') == '')
        self.assertTrue(_decode_field('', '#') == '#')

    def test_prefix_strip(self):
        self.assertEqual(_decode_field('##'), '#')
        self.assertEqual(_decode_field('#$'), '$')
        self.assertEqual(_decode_field('$#'), '#')
        self.assertEqual(_decode_field('$$'), '$')

    def test_unicode_escape(self):
        self.assertEqual(_decode_field(r'\u2603'), u'\N{SNOWMAN}')


class ReplaceUrlHostTestCase(unittest.TestCase):
    def repl(self, old, new, repl=None):
        self.assertEqual(new, _replace_url_host(old, repl))

    def test_replace_url_host(self):
        self.repl('http://google.com/', 'http://google.com/')
        self.repl('http://google.com/', 'http://go-ogle.com/', 'go-ogle.com')
        self.repl('ftp://fish:9188/', 'ftp://localhost/', 'localhost')


class LsClientTestCase(unittest.TestCase):
    def test_is_transient_error(self):
        import socket
        import urllib2
        is_transient_error = LsClient('', None)._is_transient_error
        assert is_transient_error(socket.error())
        assert is_transient_error(urllib2.URLError(socket.error()))
        assert is_transient_error(TransientError('eek'))
        assert not is_transient_error(urllib2.URLError('apocalypse'))


if __name__ == '__main__':
    unittest.main()
