import json

from typing import NamedTuple, List, Optional
from testlib.data import KafkaTopicFileChoice

KafkaAuditInquiry = NamedTuple('KafkaAuditInquiry',
                               [('startms', int), ('endms', int), ('topics_file_choice', KafkaTopicFileChoice)])


class KafkaAuditInquiryCollection(object):
    """A keyed collection of KafkaAuditInquiry objects where every key
    can be associated with one or more KafkaAuditInquiry.

    It is an implementation detail of KafkaAuditInquiryStore and should
    not be used directly.

    When serialized to JSON, an example collection would look like:
    {
        "key-1": [
            {
                "startms": 1559706228,
                "endms": 1591328628,
                "topics_file_choice": "KafkaTopicFileChoice.EXPERIMENT_VOYAGER_SEAS"
            }
        ],
        "key-2": [
            {
                "startms": 1275709428,
                "endms": 3290246280,
                "topics_file_choice": "KafkaTopicFileChoice.EXPERIMENT_VOYAGER"
            },
            {
                "startms": 1906861428,
                "endms": 6445574280,
                "topics_file_choice": "KafkaTopicFileChoice.VOYAGER"
            }
        ]
    }
    """

    def __init__(self, data: dict = None):
        self._data: dict = data or {}

    def add(self, key: str, inquiry: KafkaAuditInquiry):
        inquiries = self._data.get(key, [])
        inquiries += [dict(startms=inquiry.startms, endms=inquiry.endms,
                           topics_file_choice=str(inquiry.topics_file_choice))]
        self._data[key] = inquiries

    def remove(self, key) -> Optional[List[KafkaAuditInquiry]]:
        return self._data.pop(key, None)

    def get(self, key) -> Optional[List[KafkaAuditInquiry]]:
        inquiries: List[dict] = self._data.get(key, None)
        if inquiries is None:
            return None
        return [KafkaAuditInquiry(**KafkaAuditInquiryCollection.convert(i)) for i in inquiries]

    def to_json(self) -> str:
        return json.dumps(self._data, indent=4)

    @staticmethod
    def convert(inquiry: dict) -> dict:
        """A crude deserializer of topics_file_choice from str to KafkaTopicFileChoice"""
        topics_file_choice_prop = 'topics_file_choice'
        value = inquiry.get(topics_file_choice_prop)
        inquiry[topics_file_choice_prop] = KafkaTopicFileChoice[value.rsplit('.', maxsplit=1)[-1]]
        return inquiry

    @staticmethod
    def from_json(s: str):
        return KafkaAuditInquiryCollection(json.loads(s) if s else None)


class KafkaAuditInquiryStore(object):
    """A persistent store for KafkaAuditInquiry objects

    This implementation stores the data to a file on disk. For this
    reason, it is strongly recommended that this store is not accessed
    or updated simultaneously (e.g. from different processes) because
    that could cause file access violations.

    The current implementation is deliberately intended to make this
    store a tiny abstraction over a JSON file that is stored on disk.
    Therefore, it deliberately reads and writes the data directly from/to
    the filesystem so we do not have to turn this class into a global
    mutable singleton that:
     - has to be initialized and uninitialized at appropriate times, or
     - can be accessed and manipulated from any piece of code in the entire
       package (possibly across different processes as well)

    Inefficient as this is from an I/O perspective, it is
    somewhat acceptable since we do not perform frequent reads or writes
    to this store but it also implies that performance will suffer should
    this store be used in a tight loop or with significant amounts of data"""

    DEFAULT_AUDIT_TIMESTAMPS_FILENAME = 'kafka-audit-inquiry-data.json'

    def __init__(self, filename=DEFAULT_AUDIT_TIMESTAMPS_FILENAME):
        self._filename = filename

    def add_inquiry(self, key, inquiry: KafkaAuditInquiry):
        inquiries = self._read_data()
        inquiries.add(key, inquiry)
        self._write_data(inquiries)

    def remove_inquiry(self, key):
        inquiries = self._read_data()
        if inquiries.remove(key) is not None:
            self._write_data(inquiries)

    def get_inquiries(self, key) -> Optional[List[KafkaAuditInquiry]]:
        inquiries = self._read_data()
        return inquiries.get(key)

    def _read_data(self) -> KafkaAuditInquiryCollection:
        data = ''
        try:
            with open(self._filename, 'r') as f:
                data = f.read()
        except FileNotFoundError:
            # don't create the file if it doesn't exist
            # we may never need to write anything to it
            pass
        return KafkaAuditInquiryCollection.from_json(data)

    def _write_data(self, inquiries: KafkaAuditInquiryCollection):
        with open(self._filename, 'w') as f:
            f.write(inquiries.to_json())
