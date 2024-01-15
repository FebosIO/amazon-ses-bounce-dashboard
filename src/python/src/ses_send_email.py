import datetime
import json
import os
import traceback

from utils.dynamo import get_dynamo_client
from utils.logic import value_or_default
from utils.sqs import enviar_mensaje

TTL = int(os.environ.get('TTL') or '525600')
SQS_URL = os.environ.get('SQS_URL') or 'ses-event-manager-test-SESSendEmailSQS-Ncf1xugc5qBH.fifo'
table_email_name = os.environ.get('TABLE_EMAIL_NAME') or 'ses-email'

dynamo_client = get_dynamo_client()

table_email = dynamo_client.Table(table_email_name)


def handler(event, context):
    event['id'] = value_or_default(event, 'id', context.aws_request_id)
    event['documentoId'] = value_or_default(event, 'documentoId')
    event['messageId'] = value_or_default(event, 'messageId')
    event['empresa'] = value_or_default(event, 'empresa', '0')
    event['aplicacion'] = value_or_default(event, 'aplicacion', 'FEB')
    event['servicio'] = value_or_default(event, 'servicio', '')
    event['proceso'] = value_or_default(event, 'proceso', '')
    event['destinatarios'] = value_or_default(event, 'destinatarios', [])
    event['copias'] = value_or_default(event, 'copias', [])
    event['ConfigurationSetName'] = value_or_default(event, 'ConfigurationSetName', 'default')
    event['estado'] = 'queqed'
    event['timestamp'] = datetime.datetime.now().isoformat()

    id = event.get('id', context.aws_request_id)
    print(f"EMAIL_ID : {id}")
    documento_id = event.get('documentoId')
    message_id = event.get('messageId')
    pais = event['pais']
    ambiente = event['ambiente']
    dominio = event['dominio']
    manifiesto = event['manifiesto']
    empresa = event.get('empresa', '0')
    aplicacion = event.get('aplicacion', 'FEB')
    servicio = event.get('servicio', '')
    proceso = event.get('proceso', '')
    destinatarios = event.get('destinatarios', [])
    copias = event.get('copias', [])
    ConfigurationSetName = event.get('ConfigurationSetName', 'default')

    timestamp = datetime.datetime.now().isoformat()

    save_data = {
        'id': id,
        'estado': 'queqed',
        'documentoId': documento_id,
        'messageId': message_id,
        'pais': pais,
        'stage': ambiente,
        'domain': dominio,
        'manifiesto': manifiesto,
        'empresa': empresa,
        'destinatarios': destinatarios,
        'copias': copias,
        'servicio': servicio,
        'proceso': proceso,
        'application': aplicacion,
        'timestamp': timestamp,
        'ConfigurationSetName': ConfigurationSetName,
    }
    table_email.put_item(Item=save_data)
    params = {
        "MessageDeduplicationId": id,
        "MessageGroupId": id,
        "MessageBody": json.dumps(save_data),
        "QueueUrl": SQS_URL
    }
    print(f"SQS_URL :  {SQS_URL}")
    sqs_response = enviar_mensaje(
        nombre_cola=SQS_URL,
        mensaje=save_data,
        MessageGroupId=id,
        MessageDeduplicationId=id
    )

    return save_data


if __name__ == '__main__':
    class Contexto:
        aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

        def __int__(self):
            self.aws_request_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'


    ids = """""".split("\n")
    for id in ids:
        try:
            response = handler({
                "tipo": "enviar",
                "servicio": "DTE",
                "ambiente": "produccion",
                "proceso": "reporte",
                "dominio": "empresas.febos.cl",
                "aplicacion": "FEB",
                "pais": "chile",
                "manifiesto": "febos-io/chile/produccion/email/631221102c6992487e2abc52a45303933e28/631221102c6992487e2abc52a45303933e28.json",
                "destinatarios": [
                    "claudio@febos.cl"
                ],
                "copias": [],
                "id": "631221102c6992487e2abc52a45303933e28",
                "empresa": "76130655-3"
            }, Contexto())
            print(response)
        except:
            traceback.print_exc()
