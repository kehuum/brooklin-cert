from unittest import TestCase, TestLoader, TestSuite
from testlib.likafka.audit import KafkaAuditInquiryTest, KafkaAuditTestCaseBase


class CustomTestLoader(TestLoader):
    """Intercepts loading tests to add deferred Kafka audit tests

    See KafkaAuditTestCaseBase for more details"""

    def loadTestsFromModule(self, *args, **kwargs):
        """Invoked to load all tests in the module"""
        tests = super().loadTestsFromModule(*args, **kwargs)
        return CustomTestLoader._augment_tests(tests)

    def loadTestsFromNames(self, names, module=None):
        """Invoked to load tests that match a certain set of names"""
        tests = super().loadTestsFromNames(names, module)
        return CustomTestLoader._augment_tests(tests)

    @staticmethod
    def _augment_tests(tests):
        tests_to_run = TestSuite()
        kafka_audit_tests = []
        for testcase in CustomTestLoader._iter_testcases(tests):
            tests_to_run.addTest(testcase)
            if isinstance(testcase, KafkaAuditTestCaseBase):
                # For all test cases of type KafkaAuditTestCaseBase,
                # create a corresponding KafkaAuditInquiryTest
                audit_test = KafkaAuditInquiryTest(testcase.testName)
                kafka_audit_tests.append(audit_test)
        # Append all KafkaAuditInquiryTests so they are executed last
        tests_to_run.addTests(kafka_audit_tests)
        return tests_to_run

    @staticmethod
    def _iter_testcases(suite: TestSuite):
        """Recursively iterates through a test suite looking for test cases"""
        for test in suite:
            if isinstance(test, TestSuite):
                yield from CustomTestLoader._iter_testcases(test)
            elif isinstance(test, TestCase):
                yield test
            else:
                raise ValueError(f'Encountered a test of an unexpected type: {test}')

