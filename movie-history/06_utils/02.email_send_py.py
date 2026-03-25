# Databricks notebook source
import smtplib
from email.mime.text import MIMEText

subject = 'Pipline error'
body = 'El proceso de Pipeline a caido en error debido a que no se encuentra la carpeta'
sender = 'alejandrorivera.arp@gmail.com'
my_password = 'mxswupxmonmruysd'
recipients = [sender]

def send_mail(subject, body, sender, recipients):
    msg = MIMEText(body)
    msg['subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, my_password)
        smtp_server.sendmail(sender, recipients, msg.as_string())
    print("Message sent!")

send_mail(subject, body, sender, recipients)