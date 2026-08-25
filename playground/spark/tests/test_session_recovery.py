import unittest
from unittest.mock import MagicMock, patch

from app import main
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext


def fake_session(*, running: bool) -> MagicMock:
    session = MagicMock()
    java_context = MagicMock()
    java_context.sc.return_value.isStopped.return_value = not running
    session.sparkContext._jsc = java_context
    return session


class SparkSessionRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instantiated_session = SparkSession._instantiatedSession
        self.active_session = SparkSession._activeSession
        self.sql_context = SQLContext._instantiatedContext
        main.spark_session = None

    def tearDown(self) -> None:
        main.spark_session = None
        SparkSession._instantiatedSession = self.instantiated_session
        SparkSession._activeSession = self.active_session
        SQLContext._instantiatedContext = self.sql_context

    def test_get_spark_reuses_a_running_session(self) -> None:
        current = fake_session(running=True)
        main.spark_session = current

        with patch.object(main, "build_spark_session") as build:
            result = main.get_spark()

        self.assertIs(result, current)
        build.assert_not_called()

    def test_get_spark_replaces_a_stopped_context(self) -> None:
        stale = fake_session(running=False)
        replacement = fake_session(running=True)
        main.spark_session = stale

        with patch.object(main, "build_spark_session", return_value=replacement) as build:
            result = main.get_spark()

        self.assertIs(result, replacement)
        stale.sparkContext.stop.assert_called_once_with()
        stale._jvm.SparkSession.clearActiveSession.assert_called_once_with()
        stale._jvm.SparkSession.clearDefaultSession.assert_called_once_with()
        build.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
