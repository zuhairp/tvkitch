import json

from enum import Enum
from typing import Dict, NamedTuple, io, Any
from collections import namedtuple
from struct import pack, unpack, calcsize

# (direction, recordType, dataSize, data)
RECORD_METADATA = 'BBi'
RECORD_METADATA_SIZE = calcsize(RECORD_METADATA)

class Direction(Enum):
    REQUEST = 1
    RESPONSE = 2

class RecordType(Enum):
    LISTING = 1
    EXTRACT = 2

class Record(NamedTuple):
    direction: Direction
    record_type: RecordType
    data: Any


def create_request(requestType: RecordType, data: Any) -> Record:
    return Record(Direction.REQUEST, requestType, data)

def create_response(responseType: RecordType, data: Any) -> Record:
    return Record(Direction.RESPONSE, responseType, data)

def write_record(stream: io.IO[bytes], record: Record):
    payload = json.dumps(record.data).encode()
    metadata = pack(
        RECORD_METADATA, 
        record.direction.value,
        record.record_type.value, 
        len(payload))

    bytes_written = stream.write(metadata)
    assert(bytes_written == len(metadata))

    bytes_written = stream.write(payload)
    assert(bytes_written == len(payload))

    stream.flush()

def read_record(stream: io.IO[bytes]) -> Record:
    metadata_bytes = stream.read(RECORD_METADATA_SIZE)
    direction, rec_type, data_size = unpack(RECORD_METADATA, metadata_bytes)
    data = stream.read(data_size)
    return Record(
        Direction(direction), 
        RecordType(rec_type), 
        json.loads(data.decode()))

if __name__ == "__main__":
    from io import BytesIO
    orig = Record(Direction.RESPONSE, RecordType.LISTING, {"hello": "world"})
    buffer = BytesIO()
    write_record(buffer, orig)

    buffer.seek(0)
    cloned = read_record(buffer)
    print(cloned)