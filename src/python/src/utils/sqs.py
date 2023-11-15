import json
import uuid

import boto3

sqs = boto3.resource('sqs')


def enviar_mensaje(
        nombre_cola: str,
        mensaje,
        MessageGroupId: str = None,
        MessageDeduplicationId: str = None,
        DelaySeconds: int = 0
):
    queue = sqs.get_queue_by_name(QueueName=nombre_cola)
    args = {
        'DelaySeconds': DelaySeconds,
    }
    if '.fifo' in nombre_cola:
        args['MessageGroupId'] = str(MessageGroupId) if MessageGroupId is not None else str(uuid.uuid4())
        args['MessageDeduplicationId'] = str(MessageDeduplicationId) if MessageDeduplicationId is not None else str(uuid.uuid4())
    if isinstance(mensaje, dict):
        args['MessageBody'] = json.dumps(mensaje)
        response = queue.send_message(
            **args
        )
    else:
        args['MessageBody'] = mensaje
        response = queue.send_message(
            **args
        )
    return response