# Databricks notebook source
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import collections
import yaml

# COMMAND ----------

# Carrega a lista de emails que está em um arquivo YAML no Blob Storage
pathNotificacaoEmails = '/dbfs/mnt/sellout/manual/notificacoes/emails.yaml'

with open(pathNotificacaoEmails) as yaml_file:
  emailDestination = yaml.load(yaml_file, Loader=yaml.FullLoader)

# COMMAND ----------

#  Subject Pattern:
#      [Sellout Cube] - [Error]: <description>
#      [Sellout Cube] - [Warning]: <description>
#      [Sellout Cube]: <description>

# COMMAND ----------

def sendEmail(emailDestinationId, subject, html_content ):
  
  # limitador de caractere por email
  if len(html_content) > 135000:
    html_content = html_content[:135000] + " ................."
  
  lakeAccountName = dbutils.secrets.get(scope = "Databricks_Sellout", key = "lakeAccountName")
  message = Mail(
      from_email = 'sellout_cube@no-reply-ambev.com.br',
      to_emails = emailDestination[emailDestinationId],
      subject = '[dev]' + subject if lakeAccountName == 'dlsnonprodiris' else '' + subject,
      html_content = '[dev]' + html_content if lakeAccountName == 'dlsnonprodiris' else '' + html_content)
  try:
      sg = SendGridAPIClient(dbutils.secrets.get(scope = "Databricks_Sellout", key = "SelloutSendgrid"))
      response = sg.send(message)
      print('Email Enviado')
  except Exception as e:
      print(e)