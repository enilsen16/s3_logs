from faker import Faker
import sys
import time
import wallaroo.experimental

fake = Faker()

def generate_log_string():
    timestamp = fake.iso8601(tzinfo=None, end_datetime=None)
    string = fake.pystr(min_chars=None, max_chars=2000)
    return "[{}] {}".format(timestamp, string)

connector = wallaroo.experimental.SourceConnector(required_params=[],
                                                  optional_params=[])
connector.connect()

while True:
    message = generate_log_string()
    connector.write(message.encode("utf-8"))
    print "message sent!: {}".format(message)
    time.sleep(.001)
