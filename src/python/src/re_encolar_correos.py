import time

NOMBRE_COLA='ses-send-email.fifo'

def llamar_correos_no_enviados():
    import requests
    import json

    url = "https://search.escritoriodigital.cl/ses-email/_search?"

    payload = json.dumps({
        "from": 0,
        "size": 10000,
        "timeout": "1m",
        "query": {
            "bool": {
                "filter": [
                    {
                        "script": {
                            "script": {
                                "source": "rO0ABXNyADRvcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5mdW5jdGlvbi5GdW5jdGlvbkRTTCQyPc501CEBPWwCAAVMAA12YWwkYXJndW1lbnRzdAAQTGphdmEvdXRpbC9MaXN0O0wADHZhbCRmdW5jdGlvbnQAP0xvcmcvb3BlbnNlYXJjaC9zcWwvZXhwcmVzc2lvbi9mdW5jdGlvbi9TZXJpYWxpemFibGVCaUZ1bmN0aW9uO0wAEHZhbCRmdW5jdGlvbk5hbWV0ADVMb3JnL29wZW5zZWFyY2gvc3FsL2V4cHJlc3Npb24vZnVuY3Rpb24vRnVuY3Rpb25OYW1lO0wAFnZhbCRmdW5jdGlvblByb3BlcnRpZXN0ADtMb3JnL29wZW5zZWFyY2gvc3FsL2V4cHJlc3Npb24vZnVuY3Rpb24vRnVuY3Rpb25Qcm9wZXJ0aWVzO0wADnZhbCRyZXR1cm5UeXBldAAnTG9yZy9vcGVuc2VhcmNoL3NxbC9kYXRhL3R5cGUvRXhwclR5cGU7eHIAMG9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLkZ1bmN0aW9uRXhwcmVzc2lvbrIqMNPcdWp7AgACTAAJYXJndW1lbnRzcQB+AAFMAAxmdW5jdGlvbk5hbWVxAH4AA3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAF3BAAAAAFzcgAxb3JnLm9wZW5zZWFyY2guc3FsLmV4cHJlc3Npb24uUmVmZXJlbmNlRXhwcmVzc2lvbvgA7SvFa8yQAgADTAAEYXR0cnQAEkxqYXZhL2xhbmcvU3RyaW5nO0wABXBhdGhzcQB+AAFMAAR0eXBlcQB+AAV4cHQABmVzdGFkb3NyABpqYXZhLnV0aWwuQXJyYXlzJEFycmF5TGlzdNmkPL7NiAbSAgABWwABYXQAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0cCAAB4cAAAAAFxAH4ADX5yADpvcmcub3BlbnNlYXJjaC5zcWwub3BlbnNlYXJjaC5kYXRhLnR5cGUuT3BlblNlYXJjaERhdGFUeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAXT1BFTlNFQVJDSF9URVhUX0tFWVdPUkR4c3IAM29yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLmZ1bmN0aW9uLkZ1bmN0aW9uTmFtZQuoOE3O9meXAgABTAAMZnVuY3Rpb25OYW1lcQB+AAt4cHQAB2lzIG51bGxxAH4ACXNyACFqYXZhLmxhbmcuaW52b2tlLlNlcmlhbGl6ZWRMYW1iZGFvYdCULCk2hQIACkkADmltcGxNZXRob2RLaW5kWwAMY2FwdHVyZWRBcmdzcQB+AA9MAA5jYXB0dXJpbmdDbGFzc3QAEUxqYXZhL2xhbmcvQ2xhc3M7TAAYZnVuY3Rpb25hbEludGVyZmFjZUNsYXNzcQB+AAtMAB1mdW5jdGlvbmFsSW50ZXJmYWNlTWV0aG9kTmFtZXEAfgALTAAiZnVuY3Rpb25hbEludGVyZmFjZU1ldGhvZFNpZ25hdHVyZXEAfgALTAAJaW1wbENsYXNzcQB+AAtMAA5pbXBsTWV0aG9kTmFtZXEAfgALTAATaW1wbE1ldGhvZFNpZ25hdHVyZXEAfgALTAAWaW5zdGFudGlhdGVkTWV0aG9kVHlwZXEAfgALeHAAAAAGdXIAE1tMamF2YS5sYW5nLk9iamVjdDuQzlifEHMpbAIAAHhwAAAAAXNxAH4AGgAAAAZ1cQB+AB0AAAAAdnIAR29yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLm9wZXJhdG9yLnByZWRpY2F0ZS5VbmFyeVByZWRpY2F0ZU9wZXJhdG9yAAAAAAAAAAAAAAB4cHQAO29yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL2Z1bmN0aW9uL1NlcmlhbGl6YWJsZUZ1bmN0aW9udAAFYXBwbHl0ACYoTGphdmEvbGFuZy9PYmplY3Q7KUxqYXZhL2xhbmcvT2JqZWN0O3QAR29yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL29wZXJhdG9yL3ByZWRpY2F0ZS9VbmFyeVByZWRpY2F0ZU9wZXJhdG9ydAAYbGFtYmRhJGlzTnVsbCQzYjM0NzY2NCQxdABUKExvcmcvb3BlbnNlYXJjaC9zcWwvZGF0YS9tb2RlbC9FeHByVmFsdWU7KUxvcmcvb3BlbnNlYXJjaC9zcWwvZGF0YS9tb2RlbC9FeHByVmFsdWU7cQB+ACh2cgAyb3JnLm9wZW5zZWFyY2guc3FsLmV4cHJlc3Npb24uZnVuY3Rpb24uRnVuY3Rpb25EU0wAAAAAAAAAAAAAAHhwdAA9b3JnL29wZW5zZWFyY2gvc3FsL2V4cHJlc3Npb24vZnVuY3Rpb24vU2VyaWFsaXphYmxlQmlGdW5jdGlvbnEAfgAkdAA4KExqYXZhL2xhbmcvT2JqZWN0O0xqYXZhL2xhbmcvT2JqZWN0OylMamF2YS9sYW5nL09iamVjdDt0ADJvcmcvb3BlbnNlYXJjaC9zcWwvZXhwcmVzc2lvbi9mdW5jdGlvbi9GdW5jdGlvbkRTTHQAFmxhbWJkYSRpbXBsJDhkNTg2Y2RjJDF0AMwoTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL2Z1bmN0aW9uL1NlcmlhbGl6YWJsZUZ1bmN0aW9uO0xvcmcvb3BlbnNlYXJjaC9zcWwvZXhwcmVzc2lvbi9mdW5jdGlvbi9GdW5jdGlvblByb3BlcnRpZXM7TG9yZy9vcGVuc2VhcmNoL3NxbC9kYXRhL21vZGVsL0V4cHJWYWx1ZTspTG9yZy9vcGVuc2VhcmNoL3NxbC9kYXRhL21vZGVsL0V4cHJWYWx1ZTt0AI8oTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL2Z1bmN0aW9uL0Z1bmN0aW9uUHJvcGVydGllcztMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvbW9kZWwvRXhwclZhbHVlOylMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvbW9kZWwvRXhwclZhbHVlO3EAfgAYc3IAOW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLmZ1bmN0aW9uLkZ1bmN0aW9uUHJvcGVydGllcxcimo7B4U9WAgACTAANY3VycmVudFpvbmVJZHQAEkxqYXZhL3RpbWUvWm9uZUlkO0wACm5vd0luc3RhbnR0ABNMamF2YS90aW1lL0luc3RhbnQ7eHBzcgANamF2YS50aW1lLlNlcpVdhLobIkiyDAAAeHB3BgcAA1VUQ3hzcQB+ADV3DQIAAAAAZVTI1gTlmeB4fnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5cGUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cQB+ABR0AAdCT09MRUFO",
                                "lang": "opensearch_query_expression"
                            },
                            "boost": 1
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "from": None,
                                "to": 1700044376915,
                                "include_lower": True,
                                "include_upper": False,
                                "boost": 1
                            }
                        }
                    }
                ],
                "adjust_pure_negative": True,
                "boost": 1
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
    data = data['hits']['hits']
    for dato in data:
        id = dato['_id']
        print(id)
        from src.python.src.utils import sqs
        response = sqs.enviar_mensaje(
            NOMBRE_COLA,
            {
                'id': id
            }
        )
        print(response)
        #wait 0.5 seg
        # time.sleep(1)


if __name__ == '__main__':
    llamar_correos_no_enviados()