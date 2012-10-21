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

"""Quick'n'dirty Lightstreamer HTTP client.

Example:
    def connect():
        client.create_session(username='me', adapter_set='MyAdaptor')
        table = lightstreamer.Table(client, item_ids='my_id', schema='my_schema')
        table.on_update(on_update)

    def on_connection_state(state):
        print 'CONNECTION STATE:', state
        if state == lightstreamer.STATE_DISCONNECTED:
            connect()

    def on_update(item_id, data):
        print 'UPDATE!', data

    client = lightstreamer.LsClient('http://www.example.com/')
    client.on_connection_state(on_connection_state)

    connect()
    while True:
        signal.pause()
"""

from __future__ import absolute_import

import Queue
import collections
import logging
import socket
import threading
import time
import urllib
import urllib2
import urlparse

import requests


LOG = logging.getLogger('lightstreamer')


# Minimum time to wait between retry attempts, in seconds. Subsequent
# reconnects repeatedly double this up to a maximum.
RETRY_WAIT_SECS = 0.125

# Maximum time in seconds between reconnects. Repeatedly failing connects will
# cap the retry backoff at this maximum.
RETRY_WAIT_MAX_SECS = 30.0

# Create and activate a new table. The item group specified in the LS_id
# parameter will be subscribed to and Lightstreamer Server will start sending
# realtime updates to the client immediately.
OP_ADD = 'add'

# Creates a new table. The item group specified in the LS_id parameter will be
# subscribed to but Lightstreamer Server will not start sending realtime
# updates to the client immediately.
OP_ADD_SILENT = 'add_silent'

# Activate a table previously created with an "add_silent" operation.
# Lightstreamer Server will start sending realtime updates to the client
# immediately.
OP_START = 'start'

# Deletes the specified table. All the related items will be unsubscribed to
# and Lightstreamer Server will stop sending realtime updates to the client
# immediately.
OP_DELETE = 'delete'

# Session management; forcing closure of an existing session.
OP_DESTROY = 'destroy'


# All the itemEvent coming from the Data Adapter must be sent to the client
# unchanged.
MODE_RAW = 'RAW'

# The source provides updates of a persisting state (e.g. stock quote updates).
# The absence of a field in an itemEvent is interpreted as "unchanged data".
# Any "holes" must be filled by copying each field with the value in the last
# itemEvent where the field had a value. Not all the itemEvents from the Data
# Adapter need to be sent to the client.
MODE_MERGE = 'MERGE'

# The source provides events of the same type (e.g. statistical samplings or
# news). The itemEvents coming from the Data Adapter must be sent to the client
# unchanged. Not all the itemEvents need to be sent to the client.
MODE_DISTINCT = 'DISTINCT'

# The itemEvents are interpreted as commands that indicate how to progressively
# modify a list. In the schema there are two fields that are required to
# interpret the itemEvent. The "key" field contains the key that unequivocally
# identifies a line of the list generated from the Item. The "command" field
# contains the command associated with the itemEvent, having a value of "ADD",
# "UPDATE" or "DELETE".
MODE_COMMAND = 'COMMAND'

# A session does not yet exist, we're in the process of connecting for the
# first time. Control messages cannot be sent yet.
STATE_CONNECTING = 'connecting'

# Connected and forwarding messages.
STATE_CONNECTED = 'connected'

# A session exists, we're just in the process of reconnecting. A healthy
# connection will alternate between RECONNECTING and CONNECTED states as
# LS_content_length is exceeded.
STATE_RECONNECTING = 'reconnecting'

# Could not connect and will not retry because the server indicated a permanent
# error. After entering this state the thread stops, and session information is
# cleared. You must call create_session() to restart the session.  This is the
# default state.
STATE_DISCONNECTED = 'disconnected'

# Called when the server indicates its internal message queue overflowed.
EVENT_OVERFLOW = 'on_overflow'

# Called when an attempted push message could not be delivered.
EVENT_PUSH_ERROR = 'on_push_error'


class Error(Exception):
    """Raised when any operation fails for objects in this module."""
    def __init__(self, fmt=None, *args):
        if args:
            fmt %= args
        Exception.__init__(self, fmt or self.__doc__)


class TransientError(Error):
    """A request failed, but a later retry may succeed (e.g. network error)."""


class PermanentError(Error):
    """A request failed, and retrying it is futile."""


class SessionExpired(PermanentError):
    """Server indicated our session has expired."""


def make_dict(pairs):
    """Make a dict out of the given key/value pairs, but only include values
    that are not None."""
    return dict((k, v) for k, v in pairs if v is not None)


def _replace_url_host(url, hostname=None):
    """Return the given URL with its host part replaced with `hostname` if it
    is not None, otherwise simply return the original URL."""
    if not hostname:
        return url
    parsed = urlparse.urlparse(url)
    new = [parsed[0], hostname] + list(parsed[2:])
    return urlparse.urlunparse(new)


def _decode_field(s, prev=None):
    """Decode a single field according to the Lightstreamer encoding rules.
        1. Literal '$' is the empty string.
        2. Literal '#' is null (None).
        3. Literal '' indicates unchanged since previous update.
        4. If the string starts with either '$' or '#', but is not length 1,
           trim the first character.
        5. Unicode escapes of the form '\uXXXX' are unescaped.

    Returns the decoded Unicode string.
    """
    if s == '$':
        return u''
    elif s == '#':
        return None
    elif s == '':
        return prev
    elif s[0] in '$#':
        s = s[1:]
    return s.decode('unicode_escape')


def run_and_log(func, *args, **kwargs):
    """Invoke a function, logging any raised exceptions. Returns False if an
    exception was raised."""
    try:
        func(*args, **kwargs)
    except Exception:
        LOG.exception('While invoking %r(*%r, **%r)', func, args, kwargs)
        remove.append(func)


def dispatch(lst, *args, **kwargs):
    """Invoke every function in `lst` as func(*args, **kwargs), logging any
    exceptions that are thrown."""
    for func in list(lst):
        if run_and_log(func, *args, **kwargs):
            list.remove(func)


class WorkQueue(object):
    """Manage a thread and associated queue. The thread executes functions on
    the queue as requested."""
    def __init__(self):
        """Create an instance."""
        self.log = logging.getLogger('WorkQueue')
        self.queue = Queue.Queue()
        self.thread = threading.Thread(target=self._main)
        self.thread.setDaemon(True)
        self.thread.start()

    def stop(self):
        """Request the thread stop, then wait for it to comply."""
        self.queue.put(None)
        self.thread.join()

    def push(self, func, *args, **kwargs):
        """Request the thread execute func(*args, **kwargs)."""
        self.queue.put((func, args, kwargs))

    def _main(self):
        """Thread main; sleep waiting for a function to dispatch."""
        while True:
            tup = self.queue.get()
            if tup is None:
                self.log.info('Got shutdown semaphore; exitting.')
                return
            run_and_log(*tup)


class Table(object):
    """Lightstreamer table."""
    def __init__(self, client, item_ids, mode=None, data_adapter=None,
            buffer_size=None, row_factory=None, max_frequency=None,
            schema=None, selector=None, silent=False, snapshot=True):
        """Create a new table. See LsClient.send_control() for descriptions of
        most parameters, except:

            silent: If True, server won't start delivering events until
                start() is called.
            row_factory: Passed a sequence of strings-or-None for each row
                received, expected to return some object representing the row.
                Defaults to tuple().
        """
        self.client = client
        self.table_id = client.allocate(self._dispatch_update)
        self.row_factory = row_factory or tuple
        self._last_item_map = {}
        self._callback_map = {}

        client.send_control(
            op=OP_ADD_SILENT if silent else OP_ADD,
            buffer_size=buffer_size,
            data_adapter=data_adapter,
            item_ids=item_ids,
            max_frequency=max_frequency,
            mode=mode or MODE_MERGE,
            schema=schema,
            selector=selector,
            snapshot=snapshot,
            table_id=self.table_id
        )

    def on_update(self, func):
        """Subscribe `func` to be called when the client receives a new update
        message (i.e. data). Receives 2 arguments: item_id, and msg."""
        self._callback_map.setdefault('update', []).append(func)

    def on_end_of_snapshot(self, func):
        """Subscribe `func` to be called when the server indicates the first
        set of update messages representing a snapshot have been sent
        successfully."""
        self._callback_map.setdefault('end_of_snapshot', []).append(func)

    def start(self):
        """If the table was created with silent=True, instruct the server to
        start delivering updates."""
        self.client.send_control(OP_START, self.table_id)

    def delete(self):
        """Instruct the server and LsClient to discard this table."""
        self.client.send_control(OP_DELETE, self.table_id)
        self.client.deallocate(self.table_id)

    def _dispatch_update(self, item_id, item):
        """Called by LsClient to dispatch a table update line."""
        if item == 'EOS':
            dispatch(self._callback_map.get('end_of_snapshot', []))
            return
        last = dict(enumerate(self._last_item_map.get(item_id, [])))
        fields = [_decode_field(s, last.get(i)) for i, s in enumerate(item)]
        self._last_item_map[item_id] = fields
        dispatch(self._callback_map.get('update', []), item_id,
            self.row_factory(fields))


class LsClient(object):
    """Lightstreamer client.

    This presents an asynchronous interface to the user, accepting messages and
    responding to them using events raised through a Dispatcher instance; refer
    to comments around the various STATE_* constants.
    """
    def __init__(self, base_url, work_queue=None, content_length=None):
        """Create an instance using `base_url` as the root of the Lightstreamer
        server."""
        self.base_url = base_url
        self._lock = threading.Lock()
        self._control_url = None
        self._work_queue = work_queue or WorkQueue()
        self.content_length = content_length
        self.log = logging.getLogger('lightstreamer.LsClient')
        self._state_funcs = []
        self._table_id = 0
        self._table_cb_map = {}
        self._session = {}
        self._state = STATE_DISCONNECTED
        self._control_queue = collections.deque()
        self._thread = None
        # Prevent enqueued control messages from being sent.
        self._uncorked = threading.Event()

    def on_connection_state(self, func):
        """Register `func` to be called when the connection state changes. Sole
        argument, `state`. Sole argument, `state`, is # one of the STATE_*
        constants."""
        self._state_funcs.append(func)

    def _set_state(self, state):
        """Emit an event indicating the connection state has changed, taking
        care not to emit duplicate events."""
        if self._state == state:
            return

        self._state = state
        if state == STATE_DISCONNECTED:
            self._uncorked.clear()
            self._control_queue.clear()
        elif state == STATE_CONNECTED:
            self._uncorked.set()

        self.log.debug('New state: %r', state)
        dispatch(self._state_funcs, state)

    def _post(self, suffix, data, base_url=None):
        """Perform an HTTP post to `suffix`, logging before and after. If an
        HTTP exception is thrown, log an error and return the exception."""
        url = urlparse.urljoin(base_url or self.base_url, suffix)
        try:
            return requests.post(url, data=data, prefetch=False)
        except urllib2.HTTPError, e:
            self.log.error('HTTP %d for %r', e.getcode(), url)
            return e

    def _dispatch_update(self, line):
        """Parse an update event line from Lightstreamer, merging it into the
        previous version of the row it represents, then dispatch it to the
        table's associated listener."""
        if not line:
            return
        bits = line.rstrip('\r\n').split('|')
        if bits[0].count(',') < 1:
            self.log.warning('Dropping strange update line: %r', line)
            return

        table_info = bits[0].split(',')
        table_id, item_id = map(int, table_info[:2])
        func = self._table_cb_map.get(table_id)
        if not func:
            self.log.warning('Unknown table %r; dropping row', table_id)
            return

        if table_info[-1] == 'EOS':
            func(item_id, 'EOS')
        else:
            func(item_id, bits[1:])

    def _recv_line(self, line):
        """Parse a line from Lightstreamer and act accordingly. Returns True to
        keep the connection alive, False to indicate time to reconnect, or
        raises Terminated to indicate the server doesn't like us any more."""
        if line.startswith('PROBE'):
            self.log.debug('Received server probe.')
            return True
        elif line.startswith('LOOP'):
            self.log.debug('Server indicated length exceeded; reconnecting.')
            return False
        elif line.startswith('END'):
            self.log.error('Server permanently closed our session! %r', line)
            raise PermanentError('Session closed permanently by server.')
        else:
            # Update event.
            self._dispatch_update(line)
            return True

    def _do_recv(self):
        """Connect to bind_session.txt and dispatch messages until the server
        tells us to stop or an error occurs."""
        self.log.debug('Attempting to connect..')
        self._set_state(STATE_CONNECTING)
        req = self._post('bind_session.txt', urllib.urlencode(make_dict((
            ('LS_session', self._session['SessionId']),
            ('LS_content_length', self.content_length)
        ))), base_url=self._control_url)
        line_it = req.iter_lines(chunk_size=1)
        self._parse_and_raise_status(req, line_it)
        self._parse_session_info(line_it)
        self._set_state(STATE_CONNECTED)
        self.log.debug('Server reported Content-length: %s',
            req.headers.get('Content-length'))
        for line in line_it:
            if not self._recv_line(line):
                return True

    def _is_transient_error(self, e):
        if isinstance(e, urllib2.URLError) \
                and isinstance(e.reason, socket.error):
            return True
        return isinstance(e, (socket.error, TransientError))

    def _recv_main(self):
        """Receive thread main function. Calls _do_recv() in a loop, optionally
        delaying if a transient error occurs."""
        self.log.debug('receive thread running.')
        fail_count = 0
        running = True
        while running:
            try:
                running = self._do_recv()
                fail_count = 0
            except Exception, e:
                if not self._is_transient_error(e):
                    self.log.exception('_do_recv failure')
                    break
                fail_wait = min(RETRY_WAIT_MAX_SECS,
                    RETRY_WAIT_SECS * (2 ** fail_count))
                fail_count += 1
                self.log.info('Error: %s: %s (reconnect in %.2fs)',
                    e.__class__.__name__, e, fail_wait)
                self._set_state(STATE_CONNECTING)
                time.sleep(fail_wait)

        self._set_state(STATE_DISCONNECTED)
        self._thread = None
        self._session.clear()
        self._control_url = None
        self.log.info('Receive thread exiting')

    def allocate(self, func):
        """Allocate a table identifier and set `func` as its update callback.
        Returns the new identifier."""
        with self._lock:
            self._table_id += 1
            self._table_cb_map[self._table_id] = func
            return self._table_id

    def deallocate(self, table_id):
        """Discard state relating to the given table ID."""
        self._table_cb_map.pop(table_id, None)

    def _parse_and_raise_status(self, req, line_it):
        """Parse the status part of a control/session create/bind response.
        Either a single "OK", or "ERROR" followed by the error description. If
        ERROR, raise RequestFailed.
        """
        if req.status_code != 200:
            raise TransientError('HTTP status %d', req.status_code)
        status = next(line_it)
        if status.startswith('SYNC ERROR'):
            raise SessionExpired()
        if not status.startswith('OK'):
            raise TransientError('%s %s: %s' %
                (status, next(line_it), next(line_it)))

    def _parse_session_info(self, line_it):
        """Parse the headers from `fp` sent immediately following an OK
        message, and store them in self._session."""
        # Requests' iter_lines() has some issues with \r.
        blanks = 0
        for line in line_it:
            if line:
                blanks = 0
                key, value = line.rstrip().split(':', 1)
                self._session[key] = value
            else:
                blanks += 1
                if blanks == 2:
                    break

        self.control_url = _replace_url_host(self.base_url,
            self._session.get('ControlAddress'))
        assert self._session, 'Session parse failure'

    def _create_session_impl(self, dct):
        """Worker for create_session()."""
        assert self._state == STATE_DISCONNECTED
        self._set_state(STATE_CONNECTING)
        try:
            req = self._post('create_session.txt', urllib.urlencode(dct))
            line_it = req.iter_lines(chunk_size=1)
            self._parse_and_raise_status(req, line_it)
        except Exception:
            self._set_state(STATE_DISCONNECTED)
            raise
        self._parse_session_info(line_it)
        self._thread = threading.Thread(target=self._recv_main)
        self._thread.setDaemon(True)
        self._thread.start()

    def create_session(self, username, adapter_set, password=None,
            max_bandwidth_kbps=None, content_length=None, keepalive_ms=None):
        """Begin authenticating with Lightstreamer and start the receive
        thread.

        `username` is the Lightstreamer username (required).
        `adapter_set` is the adapter set name to use (required).
        `password` is the Lightstreamer password.
        `max_bandwidth_kbps` indicates the highest transmit rate of the
            server in Kbps. Server's default is used if unspecified.
        `content_length` is the maximum size of the HTTP entity body before the
            server requests we reconnect; larger values reduce jitter. Server's
            default is used if unspecified.
        `keepalive_ms` is the minimum time in milliseconds between PROBE
            messages when the server otherwise has nothing to say. Server's
            default is used if unspecified.
        """
        assert self._state == STATE_DISCONNECTED,\
            "create_session() called while state %r" % self._state
        self._work_queue.push(self._create_session_impl, make_dict((
            ('LS_user', username),
            ('LS_adapter_set', adapter_set),
            ('LS_report_info', 'true'),
            ('LS_polling', 'true'),
            ('LS_polling_millis', 0),
            ('LS_password', password),
            ('LS_requested_max_bandwidth', max_bandwidth_kbps),
            ('LS_content_length', content_length),
            ('LS_keepalive_millis', keepalive_ms)
        )))

    def destroy(self):
        """Request the server destroy our session."""
        self.send_control(OP_DESTROY, 0)

    def join(self):
        """Wait for the receive thread to terminate."""
        if self._thread:
            self._thread.join()

    def _send_control_impl(self):
        """Worker function for send_control()."""
        assert self._session['SessionId']
        if not self._control_queue:
            return

        # Sleep waiting for uncork. On each wake ensure connection hasn't died.
        # If it has, we're blocking the thread needed for a reconnect, so get
        # out of the way.
        while not self._uncorked.wait(0.3):
            if self._state == STATE_DISCONNECTED:
                return

        limit = int(self._session.get('RequestLimit', '50000'))
        bits = []
        size = 0
        with self._lock:
            while self._control_queue:
                op = self._control_queue[0]
                op['LS_session'] = self._session['SessionId']
                encoded = urllib.urlencode(op)
                if (size + len(encoded) + 2) > limit:
                    break
                bits.append(encoded)
                size += len(encoded) + 2
                self._control_queue.popleft()

        req = self._post('control.txt', data='\r\n'.join(bits),
            base_url=self._control_url)
        self._parse_and_raise_status(req, req.iter_lines())
        self.log.debug('Control message successful.')

    def send_control(self, op, table_id, data_adapter=None, item_ids=None,
            schema=None, selector=None, mode=None, buffer_size=None,
            max_frequency=None, snapshot=None):
        """Enqueue a control message for sending to the server.

        `op` is the OP_* constant describing the operation.
        `table_id` is the ID of the table to which the operation applies.
        `data_adapter` is the optional data adapter name.
        `item_ids` is the ID of the item group that the table contains.
        `schema` is the ID of the schema table items should conform to.
        `selector` is the optional ID of a selector for table items.
        `mode` is the MODE_* constant describing the subscription mode.
        `buffer_size` is the requested size of the transmit queue measured in
            events; defaults to 1 for MODE_MERGE and MODE_DISTINCT. Set to 0 to
            indicate 'maximum possible size'.
        `max_frequency` is the requested maximum updates per second for table
            items; set to "unfiltered" to forward all messages without loss
            (only valid for MODE_MERGE, MODE_DISTINCT, MODE_COMMAND), set to 0
            for "no frequency limit", or integer number of updates per second. 
        `snapshot` indicates whether server should send a snapshot at
            subscription time. False for no, True for yes, or integer >= 1 for
            'yes, but only send N items.
        """
        assert op in (OP_ADD, OP_ADD_SILENT, OP_START, OP_DELETE, OP_DESTROY)
        assert mode in (None, MODE_RAW, MODE_MERGE, MODE_DISTINCT, MODE_COMMAND)
        if op in (OP_ADD, OP_ADD_SILENT):
            assert item_ids, 'item_ids required for ADD/ADD_SILENT'
            assert mode, 'mode required for ADD/ADD_SILENT'

        with self._lock:
            self._control_queue.append(make_dict((
                ('LS_table', table_id),
                ('LS_op', op),
                ('LS_data_adapter', data_adapter),
                ('LS_id', item_ids),
                ('LS_schema', schema),
                ('LS_selector', selector),
                ('LS_mode', mode),
                ('LS_requested_buffer_size', buffer_size),
                ('LS_requested_max_frequency', max_frequency),
                ('LS_snapshot', snapshot and 'true')
            )))
        self._work_queue.push(self._send_control_impl)
