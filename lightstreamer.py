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

"""Quick'n'dirty Lightstreamer HTTP client.

Example:
    def on_connection_state(state):
        print 'CONNECTION STATE:', state
        if state == lightstreamer.STATE_DISCONNECTED:
            connect()

    def on_update(table_id, item_id, data):
        print 'UPDATE!', data

    def connect():
        client.create_session(username='me', adapter_set='MyAdaptor')
        table_id = client.make_table(disp)
        client.send_control([
            make_add(table=table_id, id_='my_id', schema='my_schema')
        ])

    disp = lightstreamer.Dispatcher()
    disp.listen(lightstreamer.EVENT_STATE, on_connection_state)
    disp.listen(lightstreamer.EVENT_UPDATE, on_update)
    client = lightstreamer.LsClient('http://www.example.com/', disp)
    connect()
    while True:
        signal.pause()
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

# Called when the receive connection state changes. Sole argument, `state`, is
# one of the STATE_* constants.
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

    if op in (OP_ADD, OP_ADD_SILENT):
        if not id_:
            raise ValueError('id_ must be specified for ADD/ADD_SILENT')
        if not mode:
            raise ValueError('mode must be specified for ADD/ADD_SILENT')

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
    """Like `make_op()`, but assumed operation is OP_ADD."""
    return make_op(OP_ADD, *args, **kwargs)


def make_add_silent(*args, **kwargs):
    """Like `make_op()`, but assumed operation is OP_ADD_SILENT."""
    return make_op(OP_ADD_SILENT, *args, **kwargs)


def make_start(*args, **kwargs):
    """Like `make_op()`, but assumed operation is OP_START."""
    return make_op(OP_START, *args, **kwargs)


def make_delete(*args, **kwargs):
    """Like `make_op()`, but assumed operation is OP_DELETE."""
    return make_op(OP_DELETE, *args, **kwargs)


def _encode_op(dct, session_id):
    """Encode an op dict for sending to the server."""
    dct['LS_session'] = session_id
    return urllib.urlencode(dct)


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


class UnbufferedHTTPConnection(httplib.HTTPConnection):
    """Python >= 2.6 made block buffering default in urllib2. Unfortunately
    this breaks .readline() on a streamy HTTP response. This class forces it
    off."""
    def getresponse(self, buffering=False):
        try:
            return httplib.HTTPConnection.getresponse(self, False)
        except:
            # Python <= 2.5 compatibility.
            return httplib.HTTPConnection.getresponse(self)


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
        try:
            return httplib.HTTPSConnection.getresponse(self, False)
        except:
            # Python <= 2.5 compatibility.
            return httplib.HTTPSConnection.getresponse(self)


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
        listeners = self._event_map.get(event)
        if not listeners:
            self.log.debug('got %r but nobody is listening for that', event)
            return
        for listener in listeners:
            try:
                listener(*args, **kwargs)
            except Exception:
                self.log.exception('While invoking %r(*%r, **%r)',
                    event, args, kwargs)


class WorkQueue(object):
    """Manage a thread and associated queue. The thread executes functions on
    the queue as requested."""
    def __init__(self, daemon=True):
        """Create an instance."""
        self.log = logging.getLogger('WorkQueue')
        self.queue = Queue.Queue()
        self.thread = threading.Thread(target=self._main)
        self.thread.setDaemon(daemon)
        self.thread.start()

    def stop(self):
        """Request the thread stop, then wait for it to comply."""
        self.queue.put(None)
        self.thread.join()

    def push(self, func, *args, **kwargs):
        """Request the thread execute func(*args, **kwargs)."""
        self.queue.put((func, args, kwargs))

    def _run_one(self, (func, args, kwargs)):
        """Execute a function, logging and ignoring any exception that
        occurs."""
        try:
            func(*args, **kwargs)
        except Exception:
            self.log.exception('While invoking %r(*%r, **%r)',
                func, args, kwargs)

    def _main(self):
        """Thread main; sleep waiting for a function to dispatch."""
        while True:
            tup = self.queue.get()
            if tup is None:
                self.log.info('Got shutdown semaphore; exitting.')
                return
            self._run_one(tup)


class LsClient(object):
    """Lightstreamer client.

    This presents an asynchronous interface to the user, accepting messages and
    responding to them using events raised through a Dispatcher instance; refer
    to comments around the various STATE_* constants.
    """
    def __init__(self, base_url, dispatcher, daemon=True, content_length=None):
        """Create an instance using `base_url` as the root of the Lightstreamer
        server. If `daemon` is True, the client shuts down when the program's
        main thread exits, otherwise the program will not exit until the client
        is explicitly shut down."""
        self.base_url = base_url
        self._control_url = None
        self._workqueue = WorkQueue(daemon)
        self.dispatcher = dispatcher
        self.daemon = daemon
        self.content_length = content_length
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

    def _dispatch(self, dispatcher, *args):
        """Convenience method to push an event dispatch on the work queue."""
        self._workqueue.push(dispatcher.dispatch, *args)

    def _set_state(self, state):
        """Emit an event indicating the connection state has changed, taking
        care not to emit duplicate events."""
        if self._state != state:
            self._state = state
            self.log.debug('New state: %r', state)
            self._dispatch(self.dispatcher, EVENT_STATE, state)

    def _post(self, suffix, data, base_url=None):
        """Perform an HTTP post to `suffix`, logging before and after. If an
        HTTP exception is thrown, log an error and return the exception."""
        url = urlparse.urljoin(base_url or self.base_url, suffix)
        self.log.debug('POST %r %r', url, data)
        req = urllib2.Request(url, data=data)
        try:
            return self.opener.open(req)
        except urllib2.HTTPError, e:
            self.log.error('HTTP %d for %r', e.getcode(), url)
            return e
        finally:
            self.log.debug('POST %r complete.', url)

    def _dispatch_update(self, line):
        """Parse an update event line from Lightstreamer, merging it into the
        previous version of the row it represents, then dispatch it to the
        table's associated listener."""
        bits = line.rstrip('\r\n').split('|')
        if len(bits) < 2 or bits[0].count(',') != 1:
            self.log.warning('Dropping strange update line: %r', line)
            return

        it = iter(bits)
        table_id, item_id = map(int, it.next().split(','))
        dispatcher = self._table_listener_map.get(table_id)
        if not dispatcher:
            self.log.warning('Table %r not in map; dropping row', table_id)
            return

        tup = (table_id, item_id)
        last_map = dict(enumerate(self._last_item_map.get(tup, [])))
        fields = [_decode_field(s, last_map.get(i)) for i, s in enumerate(it)]
        self._last_item_map[tup] = fields
        self._dispatch(dispatcher, EVENT_UPDATE, table_id, item_id, fields)

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
        fp = self._post('bind_session.txt', urllib.urlencode(make_dict((
            ('LS_session', self._session['SessionId']),
            ('LS_content_length', self.content_length)
        ))), base_url=self._control_url)
        self._parse_and_raise_status(fp)
        self._parse_session_info(fp)
        self._set_state(STATE_CONNECTED)
        self.log.debug('Server reported Content-length: %s',
            fp.headers.get('Content-length'))
        try:
            for line in fp:
                if not self._recv_line(line):
                    return True
        finally:
            fp.close()

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

    def _parse_and_raise_status(self, fp):
        """Parse the status part of a control/session create/bind response.
        Either a single "OK", or "ERROR" followed by the error description. If
        ERROR, raise RequestFailed.
        """
        if fp.getcode() != 200:
            raise TransientError('HTTP status %d', fp.getcode())
        more = lambda: fp.readline().rstrip('\r\n')
        status = more()
        if status.startswith('SYNC ERROR'):
            raise SessionExpired()
        if not status.startswith('OK'):
            raise TransientError('%s %s: %s' % (word, more(), more()))

    def _parse_session_info(self, fp):
        """Parse the headers from `fp` sent immediately following an OK
        message, and store them in self.session."""
        for line in fp:
            if not line.rstrip():
                break
            bits = line.rstrip().split(':', 1)
            self._session[bits[0]] = bits[1]
        self.control_url = _replace_url_host(self.base_url,
            self._session.get('ControlAddress'))
        self.log.debug('Session: %r', self._session)

    def _create_session_impl(self, dct):
        """Worker for create_session()."""
        assert self._state == STATE_DISCONNECTED
        self._set_state(STATE_CONNECTING)
        try:
            fp = self._post('create_session.txt', urllib.urlencode(dct))
            self._parse_and_raise_status(fp)
        except Exception:
            self._set_state(STATE_DISCONNECTED)
            raise
        self._parse_session_info(fp)
        self._thread = threading.Thread(target=self._recv_main)
        self._thread.setDaemon(self.daemon)
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
        self._workqueue.push(self._create_session_impl, make_dict((
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

    def _send_control_impl(self, ops):
        """Worker function for send_control()."""
        assert self._session['SessionId']
        if not isinstance(ops, list):
            ops = [ops]

        bits = (_encode_op(op, self._session['SessionId']) for op in ops)
        fp = self._post('control.txt', data='\r\n'.join(bits),
            base_url=self._control_url)
        try:
            self._parse_and_raise_status(fp)
            self.log.debug('Control message successful.')
        finally:
            fp.close()

    def send_control(self, ops):
        """Send one or more control messages to the server. `ops` is either a
        single dict returned by `make_op()`, or a list of dicts."""
        self._workqueue.push(self._send_control_impl, ops)
