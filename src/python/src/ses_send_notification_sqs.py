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
        ConfigurationSetName = value_or_default(sqsBody, 'ConfigurationSetName', 'default')
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
    handler({'Records': [{'messageId': 'bf6de63a-2b11-459f-a16b-009c7bdcafc7',
                          'receiptHandle': 'AQEBsqMCvZCLh9p8dUF8hcNbhYtNSaOB/OFW1xXFne7ezYfa7jRTivzaqeT90dVRjD1qsgeXCXde11aMrFp5ABfXWhSqXZR5aG0Eg1Vd7hpBnlUy4xa0pB4N2mY5CJwWtfNAQ1utTu4GxXuedu3znTry3700YLNE5rBr2GtiaAnKpEa2Q9wCZ+mH3zFBycc+ILZrm7w2aTW18vNXoFn3UV5Cw8tAR49IRQulRrBqQYXFWjRZ+FV8tryios/taU56NRoGa9kqTSbvaxRDYB0VX6BNPlLiB7w5P7yXh65Spp1cSE0=',
                          'body': '{"id":"8694d2892425e24de52942925de736519f32"}',
                          'attributes': {'ApproximateReceiveCount': '4',
                                         'AWSTraceHeader': 'Root=1-64ff80a6-632b58284837d71d7c99be29;Parent=15c25d854ec8f782;Sampled=0;Lineage=aae260cf:0|74085691:0',
                                         'SentTimestamp': '1694466216643', 'SequenceNumber': '18880527425170161152',
                                         'MessageGroupId': '487e7ef9-5b08-41fb-8533-4169c3864385',
                                         'SenderId': 'AROA4CUYL4XDWRQJ5I45V:ses-send-email',
                                         'MessageDeduplicationId': '61f0d42c2570324f692a62123435daee3cd1',
                                         'ApproximateFirstReceiveTimestamp': '1694466216643'}, 'messageAttributes': {},
                          'md5OfBody': '0895bd622f2c5a3a12613d193ecf4f8e', 'eventSource': 'aws:sqs',
                          'eventSourceARN': 'arn:aws:sqs:us-east-1:830321976775:ses-send-email.fifo',
                          'awsRegion': 'us-east-1'}]}, None)
