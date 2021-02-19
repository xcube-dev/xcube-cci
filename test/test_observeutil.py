from unittest import TestCase

from xcube_cci.observeutil import maybe_worked
from xcube_cci.observeutil import will_work
from xcube.util.progress import observe_progress

class ObserveUtilTest(TestCase):

    def test_maybe_worked(self):
        with observe_progress('Doing some testing', 10) as reporter:
            self.assertEqual(0.0, reporter.state.completed_work)
            maybe_worked(reporter, 5)
            self.assertEqual(5.0, reporter.state.completed_work)
            maybe_worked(reporter, 5)
            self.assertEqual(5.0, reporter.state.completed_work)
            maybe_worked(reporter, 5, 5)
            self.assertEqual(10.0, reporter.state.completed_work)

    def test_will_work(self):
        with observe_progress('Doing some testing', 10) as reporter:
            will_work(reporter, 2)
            with observe_progress('Nested', 1) as nested_reporter:
                nested_reporter.worked(1)
            self.assertEqual(2.0, reporter.state.completed_work)
            will_work(reporter, will_work_if_not_worked=4, will_work_if_worked=3)
            with observe_progress('Nested', 1) as nested_reporter:
                nested_reporter.worked(1)
            self.assertEqual(5.0, reporter.state.completed_work)
            will_work(reporter, will_work_if_not_worked=4, will_work_if_worked=3, threshold=5.0)
            with observe_progress('Nested', 1) as nested_reporter:
                nested_reporter.worked(1)
            self.assertEqual(9.0, reporter.state.completed_work)
