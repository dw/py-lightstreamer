#
# py-Lightstreamer
# Copyright (C) 2012 David Wilson
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import threading
import unittest

from lightstreamer import TransientError
from lightstreamer import Dispatcher
from lightstreamer import LsClient
from lightstreamer import WorkQueue
from lightstreamer import _decode_field


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
