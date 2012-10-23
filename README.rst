py-lightstreamer
================

This is a basic Python client library for `Lightstreamer <http://www.lightstreamer.com/>`_'s HTTP text protocol implemented using threads.

Required Parameters
-------------------
Before consuming a Lightstreamer service you must collect a few requisite settings. These can easily be found by observing an existing application's HTTP requests, e.g. via Firebug or Wireshark.

**Adapter Set**
   This is the name of the collection of data adapters for which a connection will instantiate tables. It is passed as a POST parameter ``LS_adapter_set`` to ``create_session.txt`` or ``create_session.js``.

**Data Adapter**
   This is the name of the server-side driver responsible for producing table data. In some configurations it may not be specified, otherwise it appears as the ``LS_data_adapter`` or ``LS_adapter`` (Lightstreamer < 4.1) POST parameter to ``control.txt`` or ``control.js``.

**Item Group**
  This string is parsed by the data adapter and is usually a list of space or pipe-separated identifiers; it identifies individual keys to subscribe to, but in certain cases it may be a static string such as ``ALL``. It is passed as the ``LS_id`` POST parameter to ``control.txt`` or ``control.js`` when ``LS_op=add``.

**Schema**
  This string is parsed by the data adapter and is usually a list of space or pipe-separated identifiers; it identifies the list of fields to subscribe to for each item in the item group. It is passed as the ``LS_schema`` POST parameter to ``control.txt`` or ``control.js`` when ``LS_op=add``.

**Table Mode**
  This specifies the expected update mode for the target table, it is passed as the ``LS_mode`` POST parameter to ``control.txt`` or ``control.js`` when ``LS_op=add``.

**Username and Password**
  Your Lightstreamer server might not require a username and password, but if it does, these fields are visible as the ``LS_user`` and ``LS_password`` POST parameters to ``create_session.txt`` or ``create_session.js``.


**Server URL**
  This is the absolute URL to the Lightstreamer installation, usually ending with "``/lightstreamer``". It can easily be observed as the prefix to ``create_session.txt`` or ``create_session.js`` HTTP calls.


Synopsis
--------

The library exports ``LsClient`` and ``Table`` as its main classes. Both classes are expected to be consumed by event-driven code, where it's natural to make use of callbacks for receiving data.

Callbacks are always invoked from a single thread private to each ``LsClient``. For this reason any long running code for responding to an event should be deferred to another thread, otherwise you will block the ``LsClient`` implementation.

Consumer code creates a session and subscribes to data by:

1. Instantiating an ``LsClient``:

::

    client = lightstreamer.LsClient(MY_LIGHTSTREAMER_URL)

2. Optionally subscribing to the ``on_connection_state()`` event:

::

    def on_connection_state(state):
        print 'New state:', state

    client.on_connection_state(state)

3. Call ``create_session()`` to initialize the connection:

::

    client.create_session(adapter_set='my_adapter_set',
        username='my_username', password='my_password')

Session creation runs on a private thread, so ``create_session()`` will return control to the caller immediately. For this reason you should subscribe to ``on_connection_state()``, where  ``lightstreamer.STATE_CONNECTED`` will be reported once creation succeeds.

4. Instantiate one or more ``Table`` instances, optionally including a ``row_factory`` to deserialize incoming rows:

::

    # Subscribe to bank balance. Supply a row_factory that converts the
    # incoming list of strings to a tuple of floats.
    table = lightstreamer.Table(client,
        data_adapter='AccountInfoAdapter',
        item_ids='account_1|account_2',
        schema='total_credits|total_debits',
        row_factory=lambda row: tuple(float(v) for v in row)
    )

5. Subscribe to the ``on_update()`` event:

::

    def on_bank_balance_changed(item_id, row):
        print 'Total credits:', row[0]
        print 'Total debits:', row[1]

    table.on_update(on_bank_balance_changed)

6. Consume data as desired until it becomes uninteresting. To cancel a subscription to a single table, use ``table.delete()``, or alternatively ``client.destroy()`` followed by ``client.join()`` to shut down the entire client.

**Warning**: never invoke ``client.join()`` from a Lightstreamer callback, as this will result in deadlock.


General Upset
-------------

The current implementation is threaded, which sucks. Unfortunately the only alternative solutions to asynchronous networking suck also, as they impose huge frameworks or runtime constraints on consumer code. So for the time being threads prevail.

Integration with Twisted can be achieved by simply wrapping all callbacks in ``twisted.internet.reactor.callFromThread()``:

::

    def wrap_callback(func, *args, **kwargs):
        return lambda: reactor.callFromThread(func, *args, **kwargs)

    client.on_connection_state(wrap_callback(self._on_connection_state))
    table.on_update(wrap_callback(self._on_update))
    # etc.

A future version of the library might tidy this up a little.
