import traceback

NOMBRE_COLA = 'ses-send-email.fifo'


def llamar_correos_no_enviados():
    import requests
    import json
    procesados = False
    url = "https://search.escritoriodigital.cl/ses-email/_search?"
    null = None
    true = True
    payload = json.dumps({
        "from": 0,
        "size": 10000,
        "timeout": "1m",
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "filter": [
                                {
                                    "term": {
                                        "estado.keyword": {
                                            "value": "error",
                                            "boost": 1.0
                                        }
                                    }
                                },
                                {
                                    "term": {
                                        "stage.keyword": {
                                            "value": "produccion",
                                            "boost": 1.0
                                        }
                                    }
                                }
                            ],
                            "adjust_pure_negative": true,
                            "boost": 1.0
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "from": 1718755200064,
                                "to": null,
                                "include_lower": true,
                                "include_upper": true,
                                "boost": 1.0
                            }
                        }
                    }
                ],
                "adjust_pure_negative": true,
                "boost": 1.0
            }
        },
        "_source": {
            "includes": [
                "ConfigurationSetName",
                "servicio",
                "application",
                "stage",
                "manifiesto",
                "empresa",
                "subject",
                "id",
                "messageId",
                "copias",
                "sender",
                "domain",
                "tieneAdjuntos",
                "expiration",
                "timestamp",
                "pais",
                "documentoId",
                "proceso",
                "estado",
                "destinatarios"
            ],
            "excludes": []
        },
        "sort": [
            {
                "timestamp": {
                    "order": "desc",
                    "missing": "_last"
                }
            }
        ]
    })
    headers = {
        'Authorization': 'Basic c3VwZXJhZG1pbjpJQSQwbHV0aTBuJCoq',
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    try:
        total = data['hits']['total']['value']
        data = data['hits']['hits']
        indice = 1
        for dato in data:
            print(f'procesando [{total}/{indice}]')
            indice += 1
            id = dato['_id']
            destinatarios = dato['_source'].get('destinatarios')
            timestamp = dato['_source'].get('timestamp')
            proceso = dato['_source'].get('proceso')
            servicio = dato['_source'].get('servicio')
            if not destinatarios or len(destinatarios) == 0 and servicio not in ['']:
                continue
            print(id, timestamp, destinatarios)
            procesados = True
            from src.python.src.utils import sqs
            response = sqs.enviar_mensaje(
                NOMBRE_COLA,
                {
                    'id': id
                }
            )
            print(response)
            # import time
            # time.sleep(1)
    except:
        print(data)
        traceback.print_exc()
        pass
    return procesados


def encolar_por_manifiesto():
    pass


if __name__ == '__main__':
    # while llamar_correos_no_enviados():
    llamar_correos_no_enviados()
    print("siguiente")
