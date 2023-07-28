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
        destinatarios = value_or_default(sqsBody, 'destinatarios', [])
        copias = value_or_default(sqsBody, 'copias', [])
        manifiesto = value_or_default(sqsBody, 'manifiesto')
        ConfigurationSetName = value_or_default(sqsBody, 'ConfigurationSetName', 'default')
        respuesta_email = sen_notification_from_manifest(
            destinatarios,
            manifiesto,
            ConfigurationSetName,
            item,
            copias = copias
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