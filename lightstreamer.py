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
    class MyListener(lightstreamer.ThreadedListenerBase):
        def on_connection_state(self, state):
            print 'CONNECTION STATE:', state

        def on_update(self, data):
            print 'UPDATE!', data

    client = LsClient('http://www.example.com/')
    client.create_session(username='me', adapter_set='MyAdaptor'
    table_id = client.make_table(MyListener())
    client.send_control([
        make_add(table=table_id, id_='my_id', schema='my_schema')
    ])
"""

import Queue
import httplib
import logging
import socket
import threading
import urllib
import urllib2
import urlparse


#
# LS_op constants.
#

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


#
# LS_mode constants.
#

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


#
# Exceptions.
#

class Error(Exception):
    """Raised when any operation fails for objects in this module.
    """
    def __init__(self, fmt, *args):
        if args:
            fmt %= args
        Exception.__init__(self, fmt)

class Terminated(Error):
    """Raised when the LightStreamer is terminated by the server.
    """

class CreateSessionFailed(Error):
    """Raised when create_session() cannot establish a session.
    """

class ControlMessageFailed(Error):
    """Raised when a send_control() call failed.
    """


#
# Functions.
#

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

    dct = {
        'LS_table': table,
        'LS_op': op,
    }
    def add_if(key, val, formatter=None):
        if val is not None:
            dct[key] = formatter() if formatter else val

    add_if('LS_data_adapter', data_adapter)
    add_if('LS_id', id_)
    add_if('LS_schema', schema)
    add_if('LS_selector', selector)
    add_if('LS_mode', mode)
    add_if('LS_requested_buffer_size', buffer_size)
    add_if('LS_requested_max_frequency', max_frequency)
    add_if('LS_snapshot', snapshot, lambda: str(snapshot).lower())
    return dct


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


#
# Python >= 2.6 made block buffering default in urllib2. Unfortunately this
# breaks .readline() on a streamy HTTP response. These classes force it off.
#

class UnbufferedHTTPConnection(httplib.HTTPConnection):
    def getresponse(self, buffering=False):
        return httplib.HTTPConnection.getresponse(self, False)

class UnbufferedHTTPHandler(urllib2.HTTPHandler):
    def http_open(self, req):
        socket._fileobject.default_bufsize = 1
        try:
            return self.do_open(UnbufferedHTTPConnection, req)
        finally:
            socket._fileobject.default_bufsize = 8192

class UnbufferedHTTPSConnection(httplib.HTTPSConnection):
    def getresponse(self, buffering=False):
        return httplib.HTTPSConnection.getresponse(self, False)

class UnbufferedHTTPSHandler(urllib2.HTTPSHandler):
    def https_open(self, req):
        socket._fileobject.default_bufsize = 1
        try:
            return self.do_open(UnbufferedHTTPSConnection, req)
        finally:
            socket._fileobject.default_bufsize = 8192

#
# Event listener implementations.
#

class ListenerBase(object):
    """Base implementation for listener classes. Dispatches messages in the
    client receive loop thread. Note long running callbacks may result in
    server-side overflows and therefore dropped messages.
    """
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    def dispatch(self, method, *args, **kwargs):
        """Decide how to dispatch `method(*args, **kwargs)`. By default, we
        simply call it immediately."""
        try:
            method(*args, **kwargs)
        except Exception:
            self.log.exception('While invoking %r(*%r, **%r)',
                method, args, kwargs)

    def on_connection_state(self, state):
        """Called when the connection state changes (e.g. receive loop
        connected, receive loop disconnected)."""
        self.log.debug('on_connection_state(): %r', state)

    def on_update(self, table, item, msg):
        """Called when the client receives a new update message (i.e. data)."""
        self.log.debug('on_update(): (%r, %r): %r', table, item, msg)

    def on_end_of_snapshot(self):
        """Called when the server indicates the first set of update messages
        representing a snapshot have been sent successfully."""
        self.log.debug('on_end_of_snapshot()')

    def on_overflow(self):
        """Called when the server indicates its internal message queue
        overflowed."""
        self.log.debug('on_overflow()')

    def on_push_error(self):
        """Called when an attempted push message could not be delivered."""
        self.log.debug('on_push_error()')


class ThreadedListenerBase(ListenerBase):
    """Like ListenerBase, except dispatch messages in a private thread, to
    avoid blocking the receive loop."""
    _STOP = object()

    def __init__(self):
        """Create an instance.
        """
        self.log = logging.getLogger(self.__class__.__name__)
        self.queue = Queue.Queue()
        self.thread = threading.Thread(target=self._main)
        self.thread.setDaemon(True)
        self.thread.start()

    def stop(self):
        """Inform the dispatch thread to shut down.
        """
        self.queue.put(self._STOP)

    def dispatch(self, method, *args, **kwargs):
        """Push the dispatched item onto our thread's queue.
        """
        self.queue.put((method, args, kwargs))

    def _main(self):
        """Thread queue implementation; sleep, trying to get functions to
        dispatch, dispatch them, and log any errors.
        """
        while True:
            tup = self.queue.get()
            if tup is self._STOP:
                self.log.info('Got shutdown semaphore; exitting.')
                return
            try:
                func, args, kwargs = tup
                func(*args, **kwargs)
            except Exception:
                self.log.exception('While dispatching %r', tup)


class LsClient(object):
    def __init__(self, base_url):
        """Create an instance, using `base_url` as the root URL for the
        Lightstreamer installation, and `listener` as the ListenerBase instance
        to receive messages.
        """
        self.base_url = base_url
        self.log = logging.getLogger('LsClient')
        self._table_id = 0
        self._table_listener_map = {}
        # (table_id, row_id) -> ["last", "complete", "row"]. For reconstructing
        # partial updates.
        self._last_item_map = {}
        self._running = True
        self._session = None
        self._thread = None
        self.opener = urllib2.build_opener(
            UnbufferedHTTPHandler, UnbufferedHTTPSHandler)

    def _url(self, suffix, *args, **kwargs):
        if args:
            suffix %= args
        if kwargs:
            encoded = urllib.urlencode(kwargs)
            suffix += '&' if '?' in suffix else '?'
            suffix += encoded
        return urlparse.urljoin(self.base_url, suffix)

    def _post(self, suffix, **kwargs):
        url = self._url(suffix)
        req = urllib2.Request(url, data=urllib.urlencode(kwargs))
        self.log.debug('POST %r %r', url, kwargs)
        try:
            return self.opener.open(req)
        except urllib2.HTTPError, e:
            self.log.exception(e.read())
            return e
        finally:
            self.log.debug('POST %r complete.', url)

    def _dispatch_line(self, line):
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
        listener.dispatch(listener.on_update, table_id, item_id, fields)

    def _recv_line(self, line):
        if line.startswith('PROBE'):
            self.log.debug('Received server probe.')
            return True
        elif line.startswith('LOOP'):
            self.log.debug('Server indicated length exceeded; reconnecting.')
            return False
        elif line.startswith('END'):
            self.log.error('Server permanently closed our session! %r', line)
            raise Terminated
        else:
            # Update event.
            self._dispatch_line(line)

    def _do_recv(self):
        try:
            for line in iter(self._fp.readline, ''):
                self._recv_line(line)
        finally:
            self._fp.close()

    def _recv_main(self):
        while True:
            self.log.debug('receive thread running.')
            try:
                self._do_recv()
            except Terminated:
                break

    def _start_recv(self):
        self._thread = threading.Thread(target=self._recv_main)
        self._thread.setDaemon(True)
        self._thread.start()

    def join(self):
        """Wait for the receive thread to terminate.
        """
        assert self._thread
        self._thread.join()

    def make_table(self, listener):
        """Allocate a table ID and associate it with the given `listener`. The
        new table ID is returned.
        """
        self._table_id += 1
        self._table_listener_map[self._table_id] = listener
        return self._table_id

    def _readline(self):
        return self._fp.readline().rstrip('\r\n')

    def _parse_create_session_response(self):
        status = self._readline()
        if status.startswith('ERROR'):
            raise CreateSessionFailed('%s: %s' %\
                (self._readline(), self._readline()))
        self._session = dict(line.split(':', 1)
                             for line in iter(self._readline, ''))
        self.log.debug('Create session: %r %r', status, self._session)

    def create_session(self, username, adapter_set, password=None,
            max_bandwidth_kbps=None, content_length=None, keepalive_ms=None,
            report_info=None):
        assert not self._thread

        dct = {
            'LS_user': username,
            'LS_adapter_set': adapter_set
        }
        if password:
            dct['LS_password'] = password
        if max_bandwidth_kbps:
            dct['LS_requested_max_bandwidth'] = max_bandwidth_kbps
        if content_length:
            dct['LS_content_length'] = content_length
        if keepalive_ms:
            dct['LS_keepalive_millis'] = keepalive_ms
        if report_info:
            dct['LS_report_info'] = int(bool(report_info))

        self.log.debug('POST %r', dct)
        self._fp = self._post('create_session.txt', **dct)
        self._parse_create_session_response()
        self._start_recv()

    def _parse_send_control_response(self, s):
        it = iter(s.split('\r\n'))
        if next(it) == 'OK':
            self.log.debug('Control message successful.')
            return
        raise ControlMessageFailed('\n'.join(it))

    def send_control(self, ops):
        """Send one or more control messages to the server. `ops` is either a
        single dict returned by `make_op()`, or a list of dicts.
        """
        assert self._session['SessionId']
        if not isinstance(ops, list):
            ops = [ops]

        bits = (_encode_op(op, self._session['SessionId']) for op in ops)
        data = '\r\n'.join(bits)
        self.log.debug('Sending controls: %r', data)
        req = urllib2.Request(self._url('control.txt'), data)

        fp = self.opener.open(req)
        try:
            return self._parse_send_control_response(fp.read())
        finally:
            fp.close()
