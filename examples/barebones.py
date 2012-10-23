
import lightstreamer


def on_connection_state(state):
    print 'CONNECTION STATE:', state
    if state == lightstreamer.STATE_DISCONNECTED:
        connect()

def on_update(item_id, row):
    print 'UPDATE!', row

def connect():
    client.create_session(username='me', adapter_set='MyAdaptor')
    table = lightstreamer.Table(client, item_ids='my_id', schema='my_schema')
    table.on_update(on_update)

client = lightstreamer.LsClient('http://www.example.com/')
client.on_connection_state(on_connection_state)

connect()
raw_input('Press CR to exit')
client.destroy()
client.join()
