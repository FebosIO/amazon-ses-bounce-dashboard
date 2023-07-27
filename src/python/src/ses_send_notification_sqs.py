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
    try:
        sqsBody = json.loads(message['Records'][0]['body'])

        id = value_or_default(sqsBody, 'id')

        item = table_email.get_item(Key={
            'id': id
        })
        item = item['Item']
        destinatarios = value_or_default(sqsBody, 'destinatarios')
        manifiesto = value_or_default(sqsBody, 'manifiesto')
        ConfigurationSetName = value_or_default(sqsBody, 'ConfigurationSetName', 'default')
        respuesta_email = sen_notification_from_manifest(
            destinatarios,
            manifiesto,
            ConfigurationSetName,
            item
        )

        messageId = respuesta_email['MessageId']
        print("MessageId: " + messageId)
        params = {
            'id': id,
        }
        response = table_email.update_item(
            Key=params,
            UpdateExpression="set messageId = :messageId",
            ExpressionAttributeValues={
                ':messageId': messageId
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            'statusCode': 200,
            'headers': {}
        }
    except Exception as e:
        print(message)
        print(e)
        raise e


def sen_notification_from_manifest(
        destinatario,
        manifiesto,
        ConfigurationSetName="default",
        item={}
):
    empresa = "0"
    if 'empresa' in item:
        empresa = item['empresa']

    destinatarios = []
    if isinstance(destinatario, list):
        destinatarios = destinatario
    else:
        destinatarios = destinatario.split(',')
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
        sender_email=emailField['from']['value'],
        subject=emailField['subject']['value'],
        body_text=value_or_default(value_or_default(emailField, 'text', {}), 'value', '-'),
        body_html=emailField['html']['value'],
        attachments=attachments,
        tags=tags
    )


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
    if True:
        handler({'Records': [{'messageId': 'fc007d17-1859-48db-a6ac-6d233d5f9fdc',
                              'receiptHandle': 'AQEBWwiNg8BidZOZfzPAyGO+8WdqqKXmf+TfuSCpSCgNypgVUudJSpxh6oVqf9uxkexRlSJd4xhYSKL/7Px0KsukmoXiaLhaNFUaUpJ067UnLmuasrMJIXAHtv/qgNIOQdXQRca19m+XCXGT21zjD07bwSMQODfsFcKMlhz5RZw/eM90+e/EEF/kovHygaEGw1PYSP71dxJTMUomcOrHhSg0amFi8bNJOes/PFw+9vm76dJO4MK0gHRXhgJU7YU584BELX7I9/C7PQST9Q1LNPBBET1Dg2qqearCfqSXVHSiyNc=',
                              'body': '{"id":"bd40367a-5423-4d7c-abb1-03323d9dd605","documentoId":null,"messageId":null,"pais":"chile","stage":"desarrollo","domain":"empresas.febos.cl","manifiesto":"/febos-io/chile/desarrollo/email/31a14ff528ee1247b0289b72ffe75481625d/31a14ff528ee1247b0289b72ffe75481625d.json","empresa":"629fc292-8769-11e7-97e3-129dbb41877c","destinatarios":["claudio.noexiste@febos.cl","claudio+siexiste@febos.cl"],"servicio":"DTE","proceso":"test","application":"FEB","timestamp":"2023-07-27T17:21:01.135Z","ConfigurationSetName":"default"}',
                              'attributes': {'ApproximateReceiveCount': '1',
                                             'AWSTraceHeader': 'Root=1-64c2a77c-4be00280440053e50bd0ae29;Parent=496b18fb298db3a4;Sampled=0;Lineage=74085691:0',
                                             'SentTimestamp': '1690478462058', 'SequenceNumber': '18879506559996399616',
                                             'MessageGroupId': 'bd40367a-5423-4d7c-abb1-03323d9dd605',
                                             'SenderId': 'AROA4CUYL4XDWRQJ5I45V:ses-send-email',
                                             'MessageDeduplicationId': 'bd40367a-5423-4d7c-abb1-03323d9dd605',
                                             'ApproximateFirstReceiveTimestamp': '1690478462058'},
                              'messageAttributes': {},
                              'md5OfBody': '5c22591780274d17f5d216336d2f74a8', 'eventSource': 'aws:sqs',
                              'eventSourceARN': 'arn:aws:sqs:us-east-1:830321976775:ses-send-email.fifo',
                              'awsRegion': 'us-east-1'}]},
                {})
