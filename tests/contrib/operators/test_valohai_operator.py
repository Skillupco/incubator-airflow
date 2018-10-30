from datetime import datetime
import unittest

from airflow.models import DAG
from airflow.contrib.operators.valohai_operator import ValohaiSubmitExecutionOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestValohaiSubmitExecutionOperator(unittest.TestCase):

    def __construct_operator(self):
        dag = DAG('test_dag', start_date=datetime.now())
        return ValohaiSubmitExecutionOperator(
            dag=dag,
            task_id='valohai-operator-test',
            project_id='123',
            step='Train my model',
            inputs=[],
            parameters={},
            environment='provider-2x-huge-test',
            valohai_conn_id='valohai_test_conn'
        )

    @mock.patch('airflow.contrib.operators.valohai_operator.ValohaiHook')
    def test_execute_pass(self, hook_class_mock):
        operator = self.__construct_operator()
        hook_mock = hook_class_mock.return_value

        operator.execute(None)

        hook_class_mock.assert_called_once_with(operator.valohai_conn_id)
        hook_mock.submit_execution.assert_called()
