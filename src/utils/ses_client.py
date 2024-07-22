import os
import uuid
from datetime import datetime
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr

import boto3
from botocore.exceptions import ClientError

# Specify a configuration set. If you do not want to use a configuration
# set, comment the following variable, and the
# ConfigurationSetName=CONFIGURATION_SET argument below.
CONFIGURATION_SET = "default"
# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
# The character encoding for the email.
CHARSET = "UTF-8"
HEADER_CHARSET = 'ISO-8859-1'


class SesClient(object):
    def __init__(self, config_set_name=CONFIGURATION_SET):
        self.client = boto3.client('ses')
        self.config_set_name = config_set_name

    def send_email(self,
                   *,
                   to_addresses=None,
                   cc_addresses=None,
                   bcc_addresses=None,
                   sender_email=None,
                   subject=None,
                   body_text=None,
                   body_html=None,
                   charset=CHARSET,
                   attachments=[],
                   tags=[],
                   headers=[]
                   ):
        # if False and (not attachments or len(attachments) == 0):
        #     ## eliminado por que no soporta cabeceras en la version 1 del api
        #     response = self.send_plain_mail(
        #         bcc_addresses,
        #         body_html,
        #         body_text,
        #         cc_addresses,
        #         charset,
        #         sender_email,
        #         subject,
        #         to_addresses,
        #         tags=tags,
        #         headers=headers
        #     )
        # else:
        response = self.send_attachments_mail(
            bcc_addresses,
            body_html,
            body_text,
            cc_addresses,
            charset,
            sender_email,
            subject,
            to_addresses,
            attachments,
            tags=tags,
            headers=headers
        )
        return response

    def send_attachments_mail(
            self,
            bcc_addresses,
            body_html,
            body_text,
            cc_addresses,
            charset,
            sender_email,
            subject,
            to_addresses=[],
            attachments=[],
            tags=[],
            headers=[]
    ):
        msg = MIMEMultipart('mixed')

        # Add subject, from and to lines.
        source_email_name, source_email_address = parseaddr(sender_email)
        msg.add_header('From', formataddr((str(Header(source_email_name, 'utf-8')), source_email_address)))
        # msg['From'] = sender_email

        msg.add_header('Subject', subject)
        msg.add_header('To', ','.join(to_addresses))
        if cc_addresses:
            msg.add_header('Cc', ','.join(cc_addresses))
            to_addresses = to_addresses + cc_addresses
        if bcc_addresses:
            msg.add_header('Bcc', ','.join(bcc_addresses))
            to_addresses = to_addresses + bcc_addresses
        if headers:
            for header in headers:
                msg.add_header(header['Name'], header['Value'])
        # Create a multipart/alternative child container.
        msg_body = MIMEMultipart('alternative')

        # Encode the text and HTML content and set the character encoding. This step is
        # necessary if you're sending a message with characters outside the ASCII range.
        if body_text:
            textpart = MIMEText(body_text.encode(CHARSET), 'plain', CHARSET)
            msg_body.attach(textpart)

        # Add the text and HTML parts to the child container.
        htmlpart = MIMEText(body_html.encode(CHARSET), 'html', CHARSET)
        msg_body.attach(htmlpart)

        # Attach the multipart/alternative child container to the multipart/mixed
        # parent container.
        msg.attach(msg_body)

        for attachment in attachments:
            file_name = attachment['file_name']
            file_path = attachment['file_path']
            atachment_data = open(file_path, 'rb').read()
            att = MIMEApplication(atachment_data)
            att.add_header('Content-Disposition', 'attachment', filename=file_name)
            if not os.path.exists(file_name):
                print("File exists")
            msg.attach(att)

        try:
            # Provide the contents of the email.
            response = self.client.send_raw_email(
                Source=source_email_address,
                Destinations=to_addresses,
                RawMessage={
                    'Data': msg.as_string(),
                },
                ConfigurationSetName=self.config_set_name,
                Tags=tags
            )
        except ClientError as e:
            raise e
        else:
            return response


if __name__ == '__main___':
    sesClient = SesClient()
    sender_mail = 'Phoqo <cases@inbox.phoqo.cl>'
    reply_to = "<CAM4-89+R47EEOc_iQcfwEm4TbpKa479RD5HOp6AGr3vjC8+g+A@mail.gmail.com>"
    references = [
        "<01000190db3773a7-c6db566b-eef8-4c2d-a55a-edb77f2a5730-000000@email.amazonses.com>",
        reply_to
    ]
    headers = []
    if reply_to:
        headers.append({'Name': 'References', 'Value': ' '.join(references)})
        headers.append({'Name': 'In-Reply-To', 'Value': reply_to})
    headers.append({'Name': 'Return-To', 'Value': sender_mail})

    # para mail de mac, el to_debe tener el mismo nombre/correo que en el mensaje anterior
    sesClient.send_email(
        to_addresses=['Claudio Miranda Pizarro <cronosunder@gmail.com>'],
        cc_addresses=['Claudio Miranda <claudio@febos.cl>'],
        sender_email=sender_mail,
        subject='RE: Test email Caso 2015',
        body_html=f'OK, enviare todo aparte',
        headers=headers
    )
