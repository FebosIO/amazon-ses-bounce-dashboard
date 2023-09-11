import datetime
import json
import os

from utils import s3
from utils.dynamo import get_dynamo_client
from utils.logic import value_or_default
from utils.s3 import s3_get_object_string
from utils.ses_client import SesClient

table_email_name = os.environ.get('TABLE_EMAIL_NAME') or 'ses-email'
table_event_name = os.environ.get('TABLE_EVENT_NAME') or 'ses-event'
table_email_suppression_name = os.environ.get('TABLE_EMAIL_SUPPRESSION_NAME') or 'ses-email-suppression'

dynamo_client = get_dynamo_client()

table_email = dynamo_client.Table(table_email_name)
table_event = dynamo_client.Table(table_event_name)
table_suppression = dynamo_client.Table(table_email_suppression_name)


def handler(message, context):
    id = None
    params = None
    try:
        sqsBody = json.loads(message['Records'][0]['body'])
        id = value_or_default(sqsBody, 'id')
        params = {
            'id': id,
        }

        item = table_email.get_item(Key=params)
        item = item['Item']
        destinatarios = value_or_default(sqsBody, 'destinatarios', [])
        copias = value_or_default(sqsBody, 'copias', [])
        manifiesto = value_or_default(sqsBody, 'manifiesto')
        ConfigurationSetName = value_or_default(sqsBody, 'ConfigurationSetName', 'default')
        respuesta_email, subject, tiene_adjuntos, sender = sen_notification_from_manifest(
            destinatarios,
            manifiesto,
            ConfigurationSetName,
            item,
            copias=copias
        )

        messageId = respuesta_email['MessageId']

        response = table_email.update_item(
            Key=params,
            UpdateExpression="set messageId = :messageId, subject = :subject, estado = :status, tieneAdjuntos = :tieneAdjuntos, sender= :sender",
            ExpressionAttributeValues={
                ':messageId': messageId,
                ':status': "sended",
                ':subject': subject,
                ':sender': sender,
                ':tieneAdjuntos':tiene_adjuntos
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            'statusCode': 200,
            'headers': {}
        }
    except Exception as e:
        response = table_email.update_item(
            Key=params,
            UpdateExpression="set estado = :status",
            ExpressionAttributeValues={
                ':status': "error",
            },
            ReturnValues="UPDATED_NEW"
        )
        print(message)
        print(e)
        raise e


def sen_notification_from_manifest(
        destinatario,
        manifiesto,
        ConfigurationSetName="default",
        item={},
        copias=[]
):
    empresa = "0"
    if 'empresa' in item:
        empresa = item['empresa']

    destinatarios = []
    if isinstance(destinatario, list):
        destinatarios = destinatario
    else:
        destinatarios = destinatario.split(',')
    _copias = []
    if isinstance(copias, list):
        _copias = copias
    else:
        _copias = copias.split(',')
    contenido = s3_get_object_string(manifiesto)[0]
    configuracion_manifiesto = json.loads(contenido)
    attachments = []
    adjuntos = value_or_default(configuracion_manifiesto, 'adjuntos', [])
    for adjunto in adjuntos:
        file_path = s3.s3_get_object_file(adjunto['ruta'], adjunto['nombreArchivo'])[0]
        attachments.append({
            'file_name': adjunto['nombreArchivo'],
            'file_path': file_path,
            'subtype': adjunto['mediatype'] + '/' + adjunto['submediatype']
        })

    emailField = pasar_campos_en_manifiesto_a_objeto(configuracion_manifiesto)

    tags = []
    if 'empresa' in item:
        tags.append({'Name': 'empresa', 'Value': empresa})
    destinatarios = verificar_correos_suprimidos(item['id'], empresa, destinatarios)
    client = SesClient(config_set_name=ConfigurationSetName)
    return client.send_email(
        to_addresses=destinatarios,
        cc_addresses=_copias,
        sender_email=emailField['from']['value'],
        subject=emailField['subject']['value'],
        body_text=value_or_default(value_or_default(emailField, 'text', {}), 'value', '-'),
        body_html=emailField['html']['value'],
        attachments=attachments,
        tags=tags
    ) , emailField['subject']['value'], len(attachments) > 0, emailField['from']['value']


def pasar_campos_en_manifiesto_a_objeto(manifiesto):
    output = {}
    for field in manifiesto['campos']:
        output[field['key']] = field
    return output


def verificar_correos_suprimidos(messageId, empresa_id='0', correos=[]):
    indice = 0
    prefijo = "id"
    valores = {

    }
    in_expresion = []
    for correo in correos:
        expresion_id = f":{prefijo}{indice}"
        valores[expresion_id] = correo
        in_expresion.append(expresion_id)
        indice = indice + 1
    response = table_suppression.scan(
        FilterExpression=f'id IN ( {", ".join(in_expresion)} )',
        ExpressionAttributeValues=valores
    )
    # Verificar si se encontraron elementos en la tabla
    if response['Count'] > 0:
        # Eliminamos correos suprimidos y agregamos el evento de que no se envio por supresion previa
        for item in response['Items']:
            data = {
                "id": messageId,
                "timestamp": datetime.datetime.now().isoformat(),
                "type": 'Suppression',
                "event": {
                    "suppression": item['id']
                },
                "mail": {}
            }
            table_event.put_item(Item=data)
            correos.remove(item['id'])
    return correos


if __name__ == '__main__':
    handler({
        "Records": [
            {
                "messageId": "fc007d17-1859-48db-a6ac-6d233d5f9fdc",
                "receiptHandle": "AQEBWwiNg8BidZOZfzPAyGO+8WdqqKXmf+TfuSCpSCgNypgVUudJSpxh6oVqf9uxkexRlSJd4xhYSKL/7Px0KsukmoXiaLhaNFUaUpJ067UnLmuasrMJIXAHtv/qgNIOQdXQRca19m+XCXGT21zjD07bwSMQODfsFcKMlhz5RZw/eM90+e/EEF/kovHygaEGw1PYSP71dxJTMUomcOrHhSg0amFi8bNJOes/PFw+9vm76dJO4MK0gHRXhgJU7YU584BELX7I9/C7PQST9Q1LNPBBET1Dg2qqearCfqSXVHSiyNc=",
                "body": json.dumps({
                    "id": "025d1f7521b4f249c9290b42db4eaec3b0db",
                    "application": "FEB",
                    "ConfigurationSetName": "default",
                    "copias": [
                        "cronosunder@gmail.com"
                    ],
                    "destinatarios": [
                        "claudio@febos.cl"
                    ],
                    "documentoId": "ab3b498d27dcc244d7288db2ca7b088d991e",
                    "domain": "empresas.febos.cl",
                    "empresa": "0",
                    "manifiesto": "febos-io/chile/pruebas/email/025d1f7521b4f249c9290b42db4eaec3b0db/025d1f7521b4f249c9290b42db4eaec3b0db.json",
                    "messageId": "010001899d30f079-8a09325a-b070-4a0a-ad1f-678a0d22dbd2-000000",
                    "pais": "chile",
                    "proceso": "",
                    "servicio": "",
                    "stage": "pruebas",
                    "timestamp": "2023-07-28T15:49:35.368Z"
                }),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "AWSTraceHeader": "Root=1-64c2a77c-4be00280440053e50bd0ae29;Parent=496b18fb298db3a4;Sampled=0;Lineage=74085691:0",
                    "SentTimestamp": "1690478462058",
                    "SequenceNumber": "18879506559996399616",
                    "MessageGroupId": "bd40367a-5423-4d7c-abb1-03323d9dd605",
                    "SenderId": "AROA4CUYL4XDWRQJ5I45V:ses-send-email",
                    "MessageDeduplicationId": "bd40367a-5423-4d7c-abb1-03323d9dd605",
                    "ApproximateFirstReceiveTimestamp": "1690478462058"
                },
                "messageAttributes": {},
                "md5OfBody": "5c22591780274d17f5d216336d2f74a8",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:830321976775:ses-send-email.fifo",
                "awsRegion": "us-east-1"
            }
        ]
    }, None)
