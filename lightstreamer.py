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

"""Quick'n'dirty blocking Lightstreamer HTTP streaming client for Python.

Example:
    def on_connection_state(state):
        print 'CONNECTION STATE:', state

    def on_update(table_id, item_id, data):
        print 'UPDATE!', data

    disp = lightstreamer.Dispatcher()
    disp.listen(lightstreamer.EVENT_STATE, on_connection_state)
    disp.listen(lightstreamer.EVENT_UPDATE, on_update)

    client = LsClient('http://www.example.com/', disp)
    client.create_session(username='me', adapter_set='MyAdaptor')
    table_id = client.make_table(disp)
    client.send_control([
        make_add(table=table_id, id_='my_id', schema='my_schema')
    ])
"""

import Queue
import httplib
import logging
import socket
import threading
import time
import urllib
import urllib2
import urlparse


# Creates and activate a new table. The item group specified in the LS_id
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

# In the process of connecting. A healthy connection will alternate between
# CONNECTING and CONNECTED states as LS_content_length is exceeded.
STATE_CONNECTING = 'connecting'

# Connected and forwarding messages.
STATE_CONNECTED = 'connected'

# Cannot connect, but will retry shortly.
STATE_RECONNECTING = 'reconnecting'

# Could not connect and will not retry because the server indicated a permanent
# error. After entering this state the thread stops, and session information is
# cleared. You must call create_session() to restart the session.  This is the
# default state.
STATE_DISCONNECTED = 'disconnected'

# Called when the receive connection state changes. Sole argument, `state`, is
# one of the STATE_* constants."""
EVENT_STATE = 'on_connection_state'

# Fired when the client receives a new update message (i.e. data). Receives 3
# arguments: table_id, item_id, and msg.
EVENT_UPDATE = 'on_update'

# Called when the server indicates the first set of update messages
# representing a snapshot have been sent successfully.
EVENT_END_OF_SNAPSHOT = 'on_end_of_snapshot'

# Called when the server indicates its internal message queue overflowed.
EVENT_OVERFLOW = 'on_overflow'

# Called when an attempted push message could not be delivered.
EVENT_PUSH_ERROR = 'on_push_error'


class Error(Exception):
    """Raised when any operation fails for objects in this module."""
    def __init__(self, fmt, *args):
        if args:
            fmt %= args
        Exception.__init__(self, fmt)


class TransientError(Error):
    """A request failed, but a later retry may succeed (e.g. network error)."""


class PermanentError(Error):
    """A request failed, and retrying it is futile."""


def make_dict(pairs):
    """Make a dict out of the given key/value pairs, but only include values
    that are not None."""
    return dict((k, v) for k, v in pairs if v is not None)


def make_op(op, table, data_adapter=None, id_=None, schema=None, selector=None,
        mode=None, buffer_size=None, max_frequency=None, snapshot=None):
    """Return a dict describing a control channel operation. The dict should be
    passed to `LsClient.send_control()`.

    `op` is the OP_* constant describing the operation.
    `table` is the ID of the table to which the operation applies.
    `data_adapter` is the optional data adapter name.
    `id_` is the ID of the item group that the table contains.
    `schema` is the ID of the schema table items should conform to.
    `selector` is the optional ID of a selector for table items.
    `mode` is the MODE_* constant describing the subscription mode.
    `buffer_size` is the requested size of the transmit queue measured in
        events; defaults to 1 for MODE_MERGE and MODE_DISTINCT. Set to 0 to
        indicate 'maximum possible size'.
    `max_frequency` is the requested maximum updates per second for table
        items; set to "unfiltered" to forward all messages without loss (only
        valid for MODE_MERGE, MODE_DISTINCT, MODE_COMMAND), set to 0 for "no
        frequency limit", or deicmal number of updates per second. 
    `snapshot` indicates whether server should send a snapshot at subscription
        time. False for no, True for yes, or integer >= 1 for 'yes, but only
        send N items.
    """
    assert op in (OP_ADD, OP_ADD_SILENT, OP_START, OP_DELETE)
    assert mode in (None, MODE_RAW, MODE_MERGE, MODE_DISTINCT, MODE_COMMAND)
    assert id_ or op not in (OP_ADD,), \
        'id_ parameter required for OP_ADD.'
    return make_dict((
        ('LS_table', table),
        ('LS_op', op),
        ('LS_data_adapter', data_adapter),
        ('LS_id', id_),
        ('LS_schema', schema),
        ('LS_selector', selector),
        ('LS_mode', mode),
        ('LS_requested_buffer_size', buffer_size),
        ('LS_requested_max_frequency', max_frequency),
        ('LS_snapshot', snapshot and 'true')
    ))


def make_add(*args, **kwargs):
    """Like `make_op()`, but assumed operation is OP_ADD.
    """
    return make_op(OP_ADD, *args, **kwargs)


def make_add_silent(*args, **kwargs):
    """Like `make_op()`, but assumed operation is OP_ADD_SILENT.
    """
    return make_op(OP_ADD_SILENT, *args, **kwargs)


def make_start(*args, **kwargs):
    """Like `make_op()`, but assumed operation is OP_START.
    """
    return make_op(OP_START, *args, **kwargs)


def make_delete(*args, **kwargs):
    """Like `make_op()`, but assumed operation is OP_DELETE.
    """
    return make_op(OP_DELETE, *args, **kwargs)


def _encode_op(dct, session_id):
    """Encode an op dict for sending to the server.
    """
    dct['LS_session'] = session_id
    return urllib.urlencode(dct)


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


class UnbufferedHTTPConnection(httplib.HTTPConnection):
    """Python >= 2.6 made block buffering default in urllib2. Unfortunately
    this breaks .readline() on a streamy HTTP response. This classes force it
    off."""
    def getresponse(self, buffering=False):
        return httplib.HTTPConnection.getresponse(self, False)


class UnbufferedHTTPHandler(urllib2.HTTPHandler):
    """Like UnbufferedHTTPConnection."""
    def http_open(self, req):
        socket._fileobject.default_bufsize = 1
        try:
            return self.do_open(UnbufferedHTTPConnection, req)
        finally:
            socket._fileobject.default_bufsize = 8192


class UnbufferedHTTPSConnection(httplib.HTTPSConnection):
    """Like UnbufferedHTTPConnection."""
    def getresponse(self, buffering=False):
        return httplib.HTTPSConnection.getresponse(self, False)


class UnbufferedHTTPSHandler(urllib2.HTTPSHandler):
    """Like UnbufferedHTTPConnection."""
    def https_open(self, req):
        socket._fileobject.default_bufsize = 1
        try:
            return self.do_open(UnbufferedHTTPSConnection, req)
        finally:
            socket._fileobject.default_bufsize = 8192


class Dispatcher(object):
    """Base implementation for listener classes. Dispatches messages in the
    client receive loop thread. Note long running callbacks may result in
    server-side overflows and therefore dropped messages.
    """
    def __init__(self):
        """Create an instance."""
        self.log = logging.getLogger(self.__class__.__name__)
        # name -> [list, of, listener, funcs]
        self._event_map = {}

    def listening(self, event):
        """Return a count of subscribers to `event`."""
        return len(self._event_map.get(event, []))

    def listen(self, event, func):
        """Subscribe `func` to be called when `event` is dispatched. The
        function will be called with the arguments documented for `event`."""
        listeners = self._event_map.setdefault(event, [])
        if func in listeners:
            self.log.warning('%r already subscribed to %r', func, event)
        else:
            listeners.append(func)

    def unlisten(self, event, func):
        """Unsubscribe `func` from `event`."""
        listeners = self._event_map.get(event, [])
        try:
            listeners.remove(func)
        except ValueError:
            self.log.warning('%r was not subscribed to %r', func, event)

    def dispatch(self, event, *args, **kwargs):
        """Decide how to dispatch `method(*args, **kwargs)`. By default, we
        simply call it immediately."""
        for listener in self._event_map.get(event, []):
            try:
                listener(*args, **kwargs)
            except Exception:
                self.log.exception('While invoking %r(*%r, **%r)',
                    event, args, kwargs)


class ThreadedDispatcher(Dispatcher):
    """Like Dispatcher, except dispatch messages in a private thread, to avoid
    blocking the caller."""
    def __init__(self):
        """Create an instance."""
        super(ThreadedDispatcher, self).__init__()
        self.queue = Queue.Queue()
        self.thread = threading.Thread(target=self._main)
        self.thread.setDaemon(True)
        self.thread.start()

    def stop(self):
        """Tell the dispatch thread to shut down."""
        self.queue.put(None)

    def dispatch(self, method, *args, **kwargs):
        """Push the dispatched item onto our thread's queue."""
        self.queue.put((method, args, kwargs))

    def _main(self):
        """Thread queue implementation; sleep, trying to get functions to
        dispatch, dispatch them, and log any errors."""
        while True:
            tup = self.queue.get()
            if tup is None:
                self.log.info('Got shutdown semaphore; exitting.')
                return
            funcname, args, kwargs = tup
            super(ThreadedDispatcher, self).dispatch(
                funcname, *args, **kwargs)


class LsClient(object):
    """Lightstreamer client. Control messages (create_session(),
    send_control()) block the thread that calls them, however incoming messages
    are dispatched asynchronously on a dedicated thread.

    The receive thread is a daemon thread, therefore when the program's main
    thread exits, the receive thread dies. In order to ensure correct
    operation, the main thread should not be allowed to exit 
    """
    def __init__(self, base_url, daemon=True):
        """Create an instance using `base_url` as the root of the Lightstreamer
        server. If `daemon` is True, the client shuts down when the program's
        main thread exits, otherwise the program will not exit until the client
        is explicitly shut down."""
        self.base_url = base_url
        self.daemon = daemon
        self.log = logging.getLogger('LsClient')
        self._table_id = 0
        # table_id -> ListenerBase instance.
        self._table_listener_map = {}
        # table_id, row_id -> ["last", "complete", "row"]
        self._last_item_map = {}
        self._session = {}
        self._state = STATE_DISCONNECTED
        self._thread = None
        self.opener = urllib2.build_opener(
            UnbufferedHTTPHandler, UnbufferedHTTPSHandler)

    def _set_state(self, state):
        """Emit an event indicating the connection state has changed, taking
        care not to emit duplicate events."""
        if self._state != state:
            self._state = state
            self.dispatcher.dispatch(EVENT_STATE, state)

    def _post(self, suffix, data):
        """Perform an HTTP post to `suffix`, logging before and after. If an
        HTTP exception is thrown, log an error and return the exception."""
        url = urlparse.urljoin(self.base_url, suffix)
        self.log.debug('POST %r %r', url, data)
        req = urllib2.Request(url, data=data)
        try:
            return self.opener.open(req)
        except urllib2.HTTPError, e:
            self.log.error('HTTP %d for %r', e.getcode(), url)
            return e
        finally:
            self.log.debug('POST %r complete.', url)

    def _dispatch_line(self, line):
        """Parse an update event line from Lightstreamer, merging it into the
        previous version of the row it represents, then dispatch it to the
        table's associated listener."""
        bits = line.rstrip('\r\n').split('|')
        assert len(bits) > 1 and ',' in bits[0], bits

        table_id, item_id = map(int, bits[0].split(','))
        listener = self._table_listener_map.get(table_id)
        if not listener:
            self.log.warning('Table %r not in map; dropping row', table_id)
            return

        tup = (table_id, item_id)
        last_map = dict(enumerate(self._last_item_map.get(tup, [])))
        fields = [_decode_field(s, last_map.get(i))
                  for i, s in enumerate(bits[1:])]
        self._last_item_map[tup] = fields
        listener.dispatch('on_update', table_id, item_id, fields)

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
            self._dispatch_line(line)
            return True

    def _do_recv(self):
        """Connect to bind_session.txt and dispatch messages until the server
        tells us to stop or an error occurs."""
        self.log.debug('Attempting to connect..')
        self.set_state(STATE_CONNECTING)
        fp = self._post('bind_session.txt', urllib.urlencode({
            'LS_session': self._session['SessionId']
        }))
        self._parse_and_raise_status(fp)
        self._parse_session_info(fp)
        self._set_state(STATE_CONNECTED)
        try:
            for line in fp:
                if not self._recv_line(line):
                    return True
        finally:
            fp.close()

    def _recv_main(self):
        """Receive thread main function. Calls _do_recv() in a loop, optionally
        delaying if a transient error occurs."""
        self.log.debug('receive thread running.')
        fail_start = 0.0
        fail_count = 0
        running = True
        while running:
            try:
                running = self._do_recv()
                fail_start = 0.0
                continue
            except TransientError, e:
                self.log.exception('')
                self._set_state(STATE_CONNECTING)
                fail_start = fail_start or time.time()
                time.sleep(min(60, 1 ** int(time.time() - fail_start)))
            except Exception, e:
                self.log.exception('')
                break

        self._set_state(STATE_DISCONNECTED)
        self._thread = None
        self._session.clear()
        self.log.info('Receive thread exiting')

    def join(self):
        """Wait for the receive thread to terminate."""
        if self._thread:
            self._thread.join()

    def make_table(self, dispatcher=None):
        """Allocate a table ID and associate it with the given `listener`. The
        new table ID is returned. If `dispatcher` is given, use it instead of
        the session's Dispatcher instance to dispatch update events for this
        table."""
        self._table_id += 1
        dispatcher = dispatcher or self.dispatcher
        self._table_listener_map[self._table_id] = dispatcher
        return self._table_id

    def _parse_and_raise_status(self, fp, fail_state=None):
        """Parse the status part of a control/session create/bind response.
        Either a single "OK", or "ERROR" followed by the error description. If
        ERROR, raise RequestFailed. If `fail_state` is provided, emit an event
        indicating the given state on failure.
        """
        if fp.getcode() != 200:
            if fail_state:
                self._set_state(fail_state)
            raise TransientError('HTTP status %d', fp.getcode())
        more = lambda: fp.readline().rstrip('\r\n')
        if not more().startswith('OK'):
            if fail_state:
                self._set_state(fail_state)
            raise TransientError('%s: %s' % (more(), more()))

    def _parse_session_info(self, fp):
        """Parse the headers from `fp` sent immediately following an OK
        message, and store them in self.session."""
        self._session = {}
        for line in fp:
            if not line.rstrip():
                break
            bits = line.rstrip().split(':', 1)
            self._session[bits[0]] = bits[1]
        self.log.debug('Session: %r', self._session)

    def create_session(self, username, adapter_set, password=None,
            max_bandwidth_kbps=None, content_length=None, keepalive_ms=None):
        """Authenticate with Lightstreamer and start the receive thread.

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
        assert not self._thread

        dct = make_dict((
            ('LS_user', username),
            ('LS_adapter_set', adapter_set),
            ('LS_report_info', 'true'),
            ('LS_polling', 'true'),
            ('LS_polling_millis', 1),
            ('LS_password', password),
            ('LS_requested_max_bandwidth', max_bandwidth_kbps),
            ('LS_content_length', content_length),
            ('LS_keepalive_millis', keepalive_ms)
        ))

        fp = self._post('create_session.txt', urllib.urlencode(dct))
        self._parse_and_raise_status(fp)
        self._parse_session_info(fp)
        self._thread = threading.Thread(target=self._recv_main)
        self._thread.setDaemon(self.daemon)
        self._thread.start()

    def send_control(self, ops):
        """Send one or more control messages to the server. `ops` is either a
        single dict returned by `make_op()`, or a list of dicts."""
        assert self._session['SessionId']
        if not isinstance(ops, list):
            ops = [ops]

        bits = (_encode_op(op, self._session['SessionId']) for op in ops)
        fp = self._post('control.txt', data='\r\n'.join(bits))
        try:
            self._parse_and_raise_status(fp)
            self.log.debug('Control message successful.')
        finally:
            fp.close()
