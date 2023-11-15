import datetime
import json
import os
import time
from _decimal import Decimal

from utils.dynamo import get_dynamo_client
from utils.logic import value_or_default
from utils.s3 import s3_get_object_string, s3_get_object_file
from utils.ses_client import SesClient

TTL = int(os.environ.get('TTL') or '525600')
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
    messageId = None
    vencimiento: datetime.datetime = agregar_minutos(datetime.datetime.now(), TTL)
    expiration = Decimal(time.mktime(vencimiento.timetuple()))
    try:
        sqsBody = json.loads(message['Records'][0]['body'])
        id = value_or_default(sqsBody, 'id')
        params = {
            'id': id,
        }

        item = table_email.get_item(Key=params)
        item = item['Item']
        destinatarios = value_or_default(item, 'destinatarios', [])
        copias = value_or_default(item, 'copias', [])
        manifiesto = value_or_default(item, 'manifiesto')
        ConfigurationSetName = value_or_default(item, 'ConfigurationSetName', 'default')
        respuesta_email, subject, tiene_adjuntos, sender, status = sen_notification_from_manifest(
            destinatarios,
            manifiesto,
            ConfigurationSetName,
            item,
            copias=copias,
            expiration=expiration
        )
        if respuesta_email:
            messageId = respuesta_email['MessageId']
        response = table_email.update_item(
            Key=params,
            UpdateExpression="set messageId = :messageId, expiration = :expiration, subject = :subject, estado = :status, tieneAdjuntos = :tieneAdjuntos, sender= :sender",
            ExpressionAttributeValues={
                ':messageId': messageId,
                ':status': status,
                ':expiration': expiration,
                ':subject': subject,
                ':sender': sender,
                ':tieneAdjuntos': tiene_adjuntos
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
            UpdateExpression="set estado = :status, expiration = :expiration",
            ExpressionAttributeValues={
                ':status': "error",
                ':expiration': expiration,
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
        copias=[],
        expiration=None
):
    status = "error"
    response = None
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
        file_path = s3_get_object_file(adjunto['ruta'], adjunto['nombreArchivo'])[0]
        attachments.append({
            'file_name': adjunto['nombreArchivo'],
            'file_path': file_path,
            'subtype': adjunto['mediatype'] + '/' + adjunto['submediatype']
        })

    emailField = pasar_campos_en_manifiesto_a_objeto(configuracion_manifiesto)

    tags = []
    if 'empresa' in item:
        tags.append({'Name': 'empresa', 'Value': empresa})

    destinatarios = verificar_correos_suprimidos(item['id'], empresa, destinatarios, expiration=expiration)

    if len(destinatarios) == 0:
        return response, emailField['subject']['value'], len(attachments) > 0, emailField['from']['value'], status

    client = SesClient(config_set_name=ConfigurationSetName)
    response = client.send_email(
        to_addresses=destinatarios,
        cc_addresses=_copias,
        sender_email=emailField['from']['value'],
        subject=emailField['subject']['value'],
        body_text=value_or_default(value_or_default(emailField, 'text', {}), 'value', '-'),
        body_html=emailField['html']['value'],
        attachments=attachments,
        tags=tags
    )
    status = "sended"
    return response, emailField['subject']['value'], len(attachments) > 0, emailField['from']['value'], status


def pasar_campos_en_manifiesto_a_objeto(manifiesto):
    output = {}
    for field in manifiesto['campos']:
        output[field['key']] = field
    return output


def verificar_correos_suprimidos(messageId, empresa_id='0', correos=[], expiration=None):
    if not correos or len(correos) == 0:
        return correos
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
                "messageId": messageId,
                "timestamp": datetime.datetime.now().isoformat(),
                "type": 'Suppression',
                "event": {
                    "suppression": item['id']
                },
                "mail": {}
            }
            if expiration:
                data['expiration'] = expiration
            table_event.put_item(Item=data)
            correos.remove(item['id'])
    return correos


def agregar_minutos(fecha: datetime, aAgregar=0):
    return fecha + datetime.timedelta(minutes=aAgregar)


if __name__ == '__main__':
    handler({'Records': [{'messageId': '5e772cb7-5190-4d17-8cb1-6f815d238cd8',
                          'receiptHandle': 'AQEBsJFP0Vw2Re0qQ4Dz0gYa6FfbCvqMVzYCUWt/tMdj5m/Sy8UPXqWi3LYiyyZUQz2ChM7cIB/ioLIHfmnwJKYOfz7ER2BhsY0BsKIhdklt95Uwot97JH2gR2nVrHTJuwNYQ2jy3mW45N5amAvkoHvD1pLY90YQqBWHmonBNXNjwMaLBg3FCHpJDEcS6xq/Q49Hcu2w31zOsElY4irkjgWf6YH2jJb0warmDcavoJ60i55AKJ57H6+T4Sie07vIgNAsH8JqPCCBz1Vl484H6kqOs7LNk+NHui5qY2pgOfda2TY=',
                          'body': '{"id":"4a9a111023a0a249372a46f28743d8fa4c14","documentoId":null,"messageId":null,"pais":"chile","stage":"pruebas","domain":"empresas.febos.cl","manifiesto":"febos-io/chile/pruebas/email/1952b9e428024246d52834f20427f6f9ce26/1952b9e428024246d52834f20427f6f9ce26.json","empresa":"72235100-2","destinatarios":["ninosca@febos.cl"],"copias":[],"servicio":"administracion","proceso":"","application":"ED","timestamp":"2023-11-13T20:32:42.224Z","ConfigurationSetName":"default"}',
                          'attributes': {'ApproximateReceiveCount': '3',
                                         'AWSTraceHeader': 'Root=1-655287e9-32b47c444148d2740624d93e;Parent=5ac8941106c35d23;Sampled=1;Lineage=de78eaf3:0|74085691:0',
                                         'SentTimestamp': '1699907562237', 'SequenceNumber': '18881920409642225664',
                                         'MessageGroupId': '9f3ad5b0-e2fc-49f8-be7e-7a51a765d828',
                                         'SenderId': 'AROA4CUYL4XDWRQJ5I45V:ses-send-email',
                                         'MessageDeduplicationId': '1952b9e428024246d52834f20427f6f9ce26',
                                         'ApproximateFirstReceiveTimestamp': '1699907562237'}, 'messageAttributes': {},
                          'md5OfBody': 'a51e9bbe4cf25669e8a70aa299d2a8a1', 'eventSource': 'aws:sqs',
                          'eventSourceARN': 'arn:aws:sqs:us-east-1:830321976775:ses-send-email.fifo',
                          'awsRegion': 'us-east-1'}]}, None)
