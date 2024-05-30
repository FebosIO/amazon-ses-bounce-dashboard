import os

import boto3

from typing import Union

import json

client = boto3.client('events')
WEBHOOK_EVENT_BUS = os.environ.get('WEBHOOK_EVENT_BUS', 'webhooks')
SOURCE_EVENT = os.environ.get('SOURCE_EVENT', 'io.febos.ses')


def send_event(
        event_type: str,
        event_data: Union[str, dict],
        source=SOURCE_EVENT,
        event_bus_name=WEBHOOK_EVENT_BUS
):
    """
    Send event to AWS EventBridge.
    """
    if isinstance(event_data, dict):
        event_data = json.dumps(event_data)

    # Send event
    response = client.put_events(
        Entries=[
            {
                'Source': source,
                'DetailType': str(event_type).lower(),
                'Detail': event_data,
                'EventBusName': event_bus_name
            },
        ]
    )
    return response
