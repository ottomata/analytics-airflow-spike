from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import AirflowException
from airflow.utils.dates import *

import canary_events

with DAG(
    dag_id='produce_canary_events',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1)
) as dag:
    # Get all stream names that have canary_events_enabled: true
    stream_names = [
        str(es.streamName()) for es in canary_events.get_event_streams(
            stream_settings={
                'canary_events_enabled': 'true',
                # (in Prod we won't need to also restrict to eventgate-analytics-external)
                'destination_event_service': 'eventgate-analytics-external'
            }
        )
    ]

    # Declare produce_canary_event tasks for each stream name
    for stream_name in stream_names:
        task = PythonOperator(
            task_id=stream_name,
            python_callable=produce_canary_event,
            op_args=[stream_name]
        )


def produce_canary_event(stream_name, test_mode):
    """
    Produces canary events for the provided stream name
    test_mode is set automatically if running a task test via the CLI.
    """

    # TODO: Parameterize this.
    canary_event_producer = canary_events.get_canary_event_producer()

    result = canary_events.produce_canary_events(
        canary_event_producer,
        [stream_name],
        # run DRY-RUN if in test mode.
        dry_run=test_mode
    )

    if not result:
        raise AirflowException(f'Failed producing canary event for stream {stream_name}')

    return result