import re

import boto3

from utils.files import byte_to_file

_s3 = boto3.client('s3')


def s3_normalizar_url(url: str):
    new_url = re.sub(".s3.amazonaws.com", "", url)
    new_url = new_url.replace("http:/", "/")
    new_url = new_url.replace("https:/", "/")
    new_url = re.sub(r"/{2,4}", "/", new_url)
    new_url = str(new_url).split("?")[0]
    if new_url.startswith("/"):
        new_url = new_url[1:]
    return new_url


def s3_obtener_buket_desde_ruta(url: str):
    return url.split("/")[0]


def s3_obtener_key_desde_ruta(url: str):
    return re.match("^(.*?)/(.*)$", url)[2]


def s3_get_object_file(ruta: str, name=None) -> ():
    salida = s3_get_object_bytes(ruta)
    file_name = ruta if name is None else name
    return (byte_to_file(salida[0], nombre=file_name), salida[1])


def s3_get_object_string(ruta: str, encode="utf-8") -> ():
    salida = s3_get_object_bytes(ruta)
    return (str(salida[0], encode), salida[1], salida[2])


def s3_get_object_bytes(ruta: str) -> ():
    ruta = s3_normalizar_url(ruta)
    response = _s3.get_object(Bucket=s3_obtener_buket_desde_ruta(ruta), Key=s3_obtener_key_desde_ruta(ruta))
    return (response['Body'].read(), response['Metadata'], response['ResponseMetadata'])


def upload_file(file_path: str, key: str, bucket: str, **kargs):
    parametros = {
        "Bucket": bucket,
        "Key": s3_normalizar_url(key),
        **kargs
    }
    with open(file_path, "rb") as file:
        parametros["Body"] = file
        response = _s3.put_object(**parametros)
        return response


def upload_text(text: str, ruta: str, bucket: str, **kargs):
    return _s3.put_object(
        Body=text,
        Bucket=bucket,
        Key=s3_normalizar_url(ruta),
        **kargs
    )


def upload_bytes(bytes_: bytes, ruta: str, bucket: str, **kargs):
    if isinstance(bytes_, str):
        encode = 'utf-8'
        if 'ContentEncoding' in kargs:
            encode = kargs['ContentEncoding']
        bytes_ = bytes_.encode(encode)

    return _s3.put_object(
        Body=bytes_,
        Bucket=bucket,
        Key=s3_normalizar_url(ruta),
        **kargs
    )


def put_object(*args, **kargs):
    return _s3.put_object(*args, **kargs)
