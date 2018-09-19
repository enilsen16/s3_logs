import string
import struct
import wallaroo
import wallaroo.experimental
import cPickle

def application_setup(args):
    ab = wallaroo.ApplicationBuilder("Send log files to S3")
    ab.source_connector("text", encoder=encode_text, decoder=decode_text)
    ab.sink_connector("batch", encoder=encode_batch, decoder=decode_batch)

    ab.new_pipeline("Filter and Upload", "text")
    ab.to(filter_events)
    ab.to_stateful(maybe_upload, LogFile, "log files")
    ab.to_sink("batch")
    return ab.build()


@wallaroo.computation(name="Filter log events")
def filter_events(data):
    return data


@wallaroo.state_computation(name="Maybe upload")
def maybe_upload(event, log_of_events):
    log_of_events.add(event)

    if log_of_events.file_size >= 1000000:
        log_file = log_of_events.file
        log_of_events.clear_file()
        return (log_file, True)
    else:
        print log_of_events.file_size
        return (None, False)


class LogFile(object):
    def __init__(self):
        self.file = "" # Create a file-like object
        self.file_size = 0

    def add(self, event):
        self.file += (event.encode('utf-8'))
        self.file_size += len(event.encode('utf-8'))

    def clear_file(self):
        self.__init__()

@wallaroo.experimental.stream_message_decoder
def decode_text(message):
    return message.decode("utf-8")

@wallaroo.experimental.stream_message_encoder
def encode_text(message):
   return message.encode("utf-8")

@wallaroo.experimental.stream_message_decoder
def decode_batch(message):
   return cPickle.loads(message)

@wallaroo.experimental.stream_message_encoder
def encode_batch(message):
    return cPickle.dumps((message.encode("utf-8")), -1)
