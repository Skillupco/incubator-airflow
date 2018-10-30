import logging

from airflow.models import BaseOperator
from airflow.contrib.hooks.valohai_hook import ValohaiHook


class ValohaiSubmitExecutionOperator(BaseOperator):
    ui_color = '#002f6c'
    ui_fgcolor = '#fff'

    def __init__(
        self,
        project_id,
        step,
        inputs,
        parameters,
        environment,
        commit=None,
        branch=None,
        tags=None,
        previous_outputs=None,
        valohai_conn_id='valohai_default',
        *args,
        **kwargs
    ):
        """
        Args:
            previous_outputs (list):
                [{
                    'task_id': 'REPLACE WITH TASK NAME',
                    'output_name': 'REPLACE WITH PREVIOUS TASK OUTPUT NAME',
                    'input_name': 'REPLACE WITH CURRENT TASK INPUT NAME'
                }]
        """
        if previous_outputs is None:
            previous_outputs = []
        super(ValohaiSubmitExecutionOperator, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.step = step
        self.inputs = inputs
        self.parameters = parameters
        self.environment = environment
        self.commit = commit
        self.branch = branch
        self.tags = tags
        self.previous_outputs = previous_outputs
        self.valohai_conn_id = valohai_conn_id

    def get_hook(self):
        return ValohaiHook(
            self.valohai_conn_id
        )

    def add_task_outputs_to_input(self, task_outputs, context):
        """
        Allows to pass the outputs from a previous task as the inputs of the current one
        """
        extra_inputs = {}

        for output in task_outputs:
            execution_details = context['ti'].xcom_pull(
                task_ids=output['task_id'],
                dag_id=output['dag_id'],
                include_prior_dates=True
            )
            # TODO: check that execution details is not None
            for previous_output in execution_details['outputs']:
                if previous_output['name'] == output['output_name']:
                    extra_inputs[output['input_name']] = ['datum://{}'.format(previous_output['id'])]

        logging.info('Adding extra inputs: {}'.format(extra_inputs))

        new_inputs = self.inputs.copy()
        new_inputs.update(extra_inputs)
        return new_inputs

    def execute(self, context):
        hook = self.get_hook()

        # Add previous task outputs to inputs
        if self.previous_outputs[0]:  # TODO fix this: ([],), why airflow wraps this list around a tuple ?
            self.inputs = self.add_task_outputs_to_input(self.previous_outputs[0], context)

        # Pushes execution status to XCOM
        return hook.submit_execution(
            self.project_id,
            self.step,
            self.inputs,
            self.parameters,
            self.environment,
            self.commit,
            self.branch,
            self.tags,
            self.previous_outputs,
        )
