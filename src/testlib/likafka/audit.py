import json
import logging

from typing import NamedTuple, List, Optional
from unittest import TestCase
from testlib.core.runner import TestRunner, TestRunnerBuilder
from testlib.core.teststeps import RunPythonCommand, TestStep
from testlib.core.utils import typename
from testlib.data import KafkaTopicFileChoice

KafkaAuditInquiry = NamedTuple('KafkaAuditInquiry',
                               [('startms', int), ('endms', int), ('topics_file_choice', KafkaTopicFileChoice)])

log = logging.getLogger(__name__)

TEN_MINUTES_IN_MILLISECONDS = 600000


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

    def remove_inquiries(self, key):
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


class RunKafkaAudit(RunPythonCommand):
    """Test step for running Kafka audit. If the round_timestamp option is set to true, the test rounds up the
    start time to the next 10 minute boundary, and rounds down the end time to the previous 10 minute boundary.
    Audit counts are generated every ten minutes, so this prevents test flakiness due to picking up a partial time
    interval during which the test may not have been running."""

    def __init__(self, starttime_getter, endtime_getter, topics_file_choice: KafkaTopicFileChoice,
                 round_timestamps=True, per_topic_validation=False):
        super().__init__()
        if not topics_file_choice:
            raise ValueError(f'Invalid topics file choice: {topics_file_choice}')

        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of the two time getters is invalid')

        self.topics_file_choice = topics_file_choice
        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter
        self.round_timestamps = round_timestamps
        self.per_topic_validation = per_topic_validation

    @property
    def main_command(self):
        startms = self.starttime_getter() * 1000
        endms = self.endtime_getter() * 1000
        if self.round_timestamps:
            # Round up the start timestamp to the next 10 minute window
            if (startms % TEN_MINUTES_IN_MILLISECONDS) > 0:
                startms = startms - (startms % TEN_MINUTES_IN_MILLISECONDS) + TEN_MINUTES_IN_MILLISECONDS

            # Round down the end timestamp to the previous 10 minute window
            endms = endms - (endms % TEN_MINUTES_IN_MILLISECONDS)

            if startms >= endms:
                raise ValueError(f'Error on rounding up {self.starttime_getter() * 1000} and rounding down '
                                 f'{self.endtime_getter() * 1000}. The start time, {startms}, '
                                 f'should be less than the end time, {endms}.')

        log.info(f'Original start time: {self.starttime_getter() * 1000}, original end time: '
                 f'{self.endtime_getter() * 1000}. Rounded start time: {startms}, rounded end time: {endms}.')
        audit_command = 'kafka-audit-v2.py ' \
                        f'--topicsfile {self.topics_file_choice.value} ' \
                        f'--startms {startms} ' \
                        f'--endms {endms}'
        if self.per_topic_validation:
            audit_command += ' --pertopicvalidation'
        return audit_command

    def __str__(self):
        return f'{typename(self)}(topics_file_choice: {self.topics_file_choice})'


class AddDeferredKafkaAuditInquiry(TestStep):
    """Writes Kafka audit inquiry info that can be used by Kafka audit tests to persistent storage"""

    def __init__(self, test_name, starttime_getter, endtime_getter, topics_file_choice: KafkaTopicFileChoice):
        super().__init__()
        if not test_name:
            raise ValueError(f'Invalid test name: {test_name}')
        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of the two time getters is invalid')
        if not topics_file_choice:
            raise ValueError(f'Invalid topics_file_choice: {topics_file_choice}')

        self.test_name = test_name
        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter
        self.topics_file_choice = topics_file_choice

    def run_test(self):
        store = KafkaAuditInquiryStore()
        store.add_inquiry(key=self.test_name, inquiry=KafkaAuditInquiry(startms=self.starttime_getter(),
                                                                        endms=self.endtime_getter(),
                                                                        topics_file_choice=self.topics_file_choice))


class KafkaAuditTestCaseBase(TestCase):
    """Base class of any TestCase that wishes to run deferred Kafka audit inquiries

    Extenders of this class are BMM test cases that wish to perform deferred data
    completeness checks by querying Kafka audit. These checks are deferred in the
    sense that they are performed after the test case that requested them passes
    and all other tests execute. Deferring these audit checks is desirable primarily
    because the service behind Kafka audit produces more reliable results when given
    more time to finish counting events.

    Extenders are expected to be using a TestRunner to execute their testing logic,
    and are allowed to request deferred audit checks by running the
    AddDeferredKafkaAuditInquiry test step as many times as desired to capture all
    the parameters of their Kafka audit checks.

    Extenders are required to use doRunTest() defined on this class to run their
    tests or else all requested Kafka audit checks will not be executed. This is
    necessary so this class can guarantee that audit checks are only executed if
    the tests that requested them actually passed.

    Methods and attributes on this class use lowerCamelCase for consistency with
    the parent type unittest.TestCase"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._auditInquiryStore = KafkaAuditInquiryStore()
        self._success = False

    def setUp(self):
        # Make sure to clear any old Kafka audit inquiries
        # added by past executions of this test so they
        # don't get used in the Kafka audit tests executed
        # after this test is complete
        self._removeAuditInquiries()

    def tearDown(self):
        if not self._success:
            # We don't want to query Kafka audit if the test fails
            self._removeAuditInquiries()

    def doRunTest(self, runner: TestRunner):
        self._success = runner.run()
        self.assertTrue(self._success, "Running test failed")

    @property
    def testName(self):
        """Returns the name of the test without the module name (e.g. BasicTests.test_basic)"""
        return self.id().split('.', maxsplit=1)[1]

    def _removeAuditInquiries(self):
        self._auditInquiryStore.remove_inquiries(self.testName)


class KafkaAuditInquiryTest(TestCase):
    """A testcase for hitting Kafka audit to perform a data completeness check

    This test case is not intended to be used directly. It is used by the
    CustomTestLoader to schedule deferred Kafka audit tests."""

    def __init__(self, inquiry_key: str):
        self._inquiry_key = inquiry_key
        # Create an attribute whose name = inquiry_key and
        # set it to run_test so that method is executed when
        # the test case is run. This will cause the test case
        # to show up with that name in the final test report
        # so we have a way to know which audit tests passed,
        # failed, were skipped ... etc.
        setattr(self, inquiry_key, self.run_test)
        super().__init__(methodName=inquiry_key)

    def run_test(self):
        audit_inquiry_store = KafkaAuditInquiryStore()
        inquiries = audit_inquiry_store.get_inquiries(self._inquiry_key)
        if inquiries is None:
            self.skipTest(f'No Kafka audit inquiry info found for {self._inquiry_key}')

        for i, inquiry in enumerate(inquiries):
            with self.subTest(audit_inquiry_number=i + 1, topics_file_choice=inquiry.topics_file_choice):
                self._run_audit(inquiry)

    def _run_audit(self, inquiry):
        runner = TestRunnerBuilder(self.id()) \
            .skip_pretest_steps() \
            .add_sequential(RunKafkaAudit(starttime_getter=lambda: inquiry.startms,
                                          endtime_getter=lambda: inquiry.endms,
                                          topics_file_choice=inquiry.topics_file_choice)) \
            .build()
        self.assertTrue(runner.run(), "Kafka audit test failed")
