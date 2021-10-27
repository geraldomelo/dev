import smtplib, ssl, email

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from settings import EMAIL
import os

from settings import ARQUIVOS_TEMPORARIOS


def send_email(receiver, filename, subject, body):
    sender_email = EMAIL["sender_email"]
    receiver_email = receiver
    port = 465
    password = EMAIL["password"]

    message = MIMEMultipart()

    message["From"] =sender_email
    message["To"] = receiver_email
    message["Subject"]=subject
    message["Bcc"] = receiver_email

    message.attach(MIMEText(body, 'plain'))
    
    filepath = os.path.join(ARQUIVOS_TEMPORARIOS , filename)

    with open(filepath, "rb") as attachment:
        part = MIMEBase("text", "csv")
        part.set_payload(attachment.read())


    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )


    message.attach(part)

    text = message.as_string()

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(EMAIL["smtp_server"], port, context=context) as server:
        server.login(sender_email, password=password)
        server.sendmail(sender_email, receiver_email, text)