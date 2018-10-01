import time
import logging
from urllib.request import urlretrieve

import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


SUBMIT_EXECUTION_ENDPOINT = 'api/v0/executions/'
LIST_REPOSITORIES_ENDPOINT = 'api/v0/repositories/'
LIST_COMMITS_ENDPOINT = 'api/v0/commits/'
EXECUTION_ENDPOINT = 'api/v0/executions/{execution_id}/'
FETCH_REPOSITORY_ENDPOINT = 'api/v0/projects/{project_id}/fetch/'
EXECUTION_TAGS_ENDPOINT = 'api/v0/executions/{execution_id}/tags/'

incomplete_execution_statuses = {
    'created',
    'queued',
    'started',
    'stopping',
}

fail_execution_statuses = {
    'error',
    'crashed',
    'stopped',
}

success_execution_statuses = {
    'complete',
}


def download_execution_outputs(task_id, output_path, **context):
    """
    Downloads and replaces execution outputs locally using the S3 url
    with authentication details from the Valohai execution details API.

    Execution details are pulled from the XCOM variable of the last sucessful model task.
    """
    execution_details = context['ti'].xcom_pull(task_ids=task_id)
    for output in execution_details['outputs']:
        logging.info('Downloading output: {}'.format(output['name']))
        urlretrieve(output['url'], os.path.join(output_path, output['name']))


class ValohaiHook(BaseHook):
    """
    Interact with Valohai.
    """
    def __init__(self, valohai_conn_id='valohai_default'):
        self.valohai_conn = self.get_connection(valohai_conn_id)
        self.host = self.valohai_conn.host
        if 'token' in self.valohai_conn.extra_dejson:
            logging.info('Using token autorization.')
            self.headers = {
                'Authorization': 'Token {}'.format(self.valohai_conn.extra_dejson['token'])
            }

    def get_respository_id(self, project_id):
        url = 'https://{host}/{endpoint}'.format(
            host=self.host,
            endpoint=LIST_REPOSITORIES_ENDPOINT,
        )
        response = requests.get(
            url,
            headers=self.headers,
            params={'limit': 10000}
        )
        for repository in response.json()['results']:
            if repository['project']['id'] == project_id:
                return repository['id']

    def fetch_repository(self, project_id):
        """
        Make Valohai fetch the latest commit for the fetch reference
        in the Valohai UI repository settings.
        """
        url = 'https://{host}/{endpoint}'.format(
            host=self.host,
            endpoint=FETCH_REPOSITORY_ENDPOINT.format(project_id=project_id)
        )

        response = requests.post(
            url,
            headers=self.headers,
        )

        # TODO: handle project not found error
        return response.json()

    def get_latest_commit(self, project_id, branch):
        repository_id = self.get_respository_id(project_id)

        url = 'https://{host}/{endpoint}'.format(
            host=self.host,
            endpoint=LIST_COMMITS_ENDPOINT
        )
        response = requests.get(
            url,
            headers=self.headers,
            params={'limit': 10000, 'ordering': '-commit_time'}
        )

        for commit in response.json()['results']:
            if commit['repository'] == repository_id and commit['ref'] == branch:
                return commit['identifier']

    def get_execution_details(self, execution_id):
        url = 'https://{host}/{endpoint}'.format(
            host=self.host,
            endpoint=EXECUTION_ENDPOINT.format(execution_id=execution_id)
        )
        response = requests.get(
            url,
            headers=self.headers,
        )
        return response.json()

    def add_execution_tags(self, tags, execution_id):
        url = 'https://{host}/{endpoint}'.format(
            host=self.host,
            endpoint=EXECUTION_TAGS_ENDPOINT.format(execution_id=execution_id)
        )
        response = requests.post(
            url,
            headers=self.headers,
            json={'tags': tags}
        )
        return response.json()

    def submit_execution(
        self,
        project_id,
        step,
        inputs,
        parameters,
        environment,
        commit,
        branch,
        tags,
        previous_outputs,
        polling_period_seconds=30,
    ):
        """
        Submits an execution to valohai and checks the status until the execution succeeds or fails.

        Returns the execution details if the execution completed successfully.
        """
        self.polling_period_seconds = polling_period_seconds
        if branch:
            response = self.fetch_repository(project_id)
            logging.info('Fetched latest commits with response: {}'.format(response))
            commit = self.get_latest_commit(project_id, branch)
            logging.info('Using latest {} branch commit: {}'.format(branch, commit))

        url = 'https://{host}/{endpoint}'.format(
            host=self.host,
            endpoint=SUBMIT_EXECUTION_ENDPOINT
        )
        payload = {
            'project': project_id,
            'commit': commit,
            'step': step,
            'inputs': inputs,
            'parameters': parameters,
            'environment': environment
        }
        response = requests.post(
            url,
            json=payload,
            headers=self.headers
        )
        # TODO: handle errors when post
        logging.info('Got response: {}'.format(response.json()))

        execution_id = response.json()['id']
        execution_url = response.json()['urls']['display']
        logging.info('Started execution: {}'.format(execution_url))

        if tags:
            self.add_execution_tags(tags, execution_id)
            logging.info('Added execution tags: {}'.format(tags))
        while True:
            time.sleep(self.polling_period_seconds)
            execution_details = self.get_execution_details(execution_id)
            status = execution_details['status']
            if status in incomplete_execution_statuses:
                logging.info('Incomplete execution with status: {}'.format(status))
                continue
            elif status in fail_execution_statuses:
                raise AirflowException('Execution failed with status: {}'.format(status))
            elif status in success_execution_statuses:
                logging.info('Execution completed sucessfully')
                return execution_details
            else:
                raise AirflowException('Found a not handled status: {}'.format(status))
