NOMBRE_COLA = 'ses-send-email.fifo'


def llamar_correos_no_enviados():
    import requests
    import json

    url = "https://search.escritoriodigital.cl/ses-email/_search?"
    null = None
    true = True
    payload = json.dumps({
        "from": 0,
        "size": 10000,
        "timeout": "2m",
        "query": {
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
                    },
                    {
                        "range": {
                            "timestamp": {
                                "from": '2023-09-11T00:00:00.000',
                                "to": null,
                                "include_lower": true,
                                "include_upper": true,
                                "boost": 1.0
                            }
                        }
                    },
                    {
                        "exists": {
                            "field": "destinatarios"
                        }
                    }
                ],
                "adjust_pure_negative": true,
                "boost": 1.0
            }
        },
        "_source": {
            "includes": [
                "estado",
                "proceso",
                "destinatarios",
                "id",
                "stage",
                "timestamp"
            ],
            "excludes": []
        },
        "sort": [
            {
                "timestamp": {
                    "order": "asc",
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
            if not destinatarios or len(destinatarios) == 0:
                continue
            print(id, timestamp, destinatarios)
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
        # print(data)
        pass


if __name__ == '__main__':
    llamar_correos_no_enviados()
