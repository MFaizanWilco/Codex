""" Class to make log file and send email"""

import os
import datetime
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
# import schedule
# import time
import sys
from Config import Configuration


def generate_log(function_name, exception, at_request):
    print(function_name,exception,at_request)
    logf = open('sp_error.log', 'a')
    dictionary = {
        'Date: ': str(datetime.datetime.now().date()), 'Time: ': str(datetime.datetime.now().time()).split('.')[0],
        'Method Name: ': function_name, 'Exception: ': exception, ' At Request: ': str(at_request)}
    # logf.write('DateTime: ' + str(datetime.datetime.now()) + ' Method Name: ' + function_name +
    #            ' Exception: ' + exception + ' At Request: ' + str(at_request) + '\n')
    logf.write(str(dictionary) + '\n')
    logf.close()
    #email_logfile()


def email_logfile():
    sender = Configuration().GetData()['EmailID']
    gmail_password = Configuration().GetData()['Password']
    dr_tariq_team = Configuration().GetData()['team_emails']
    COMMASPACE = ', '
    recipients = dr_tariq_team

    # Create the enclosing (outer) message
    outer = MIMEMultipart()
    outer['Subject'] = 'dpdmlog @ ' + str(datetime.datetime.now().date())
    outer['To'] = COMMASPACE.join(recipients)
    outer['From'] = sender
    outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'

    # List of attachments
    attachments = ['sp_error.log']

    # Add the attachments to the message
    for file in attachments:
        try:
            with open(file, 'rb') as fp:
                msg = MIMEBase('application', "octet-stream")
                msg.set_payload(fp.read())
            encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file))
            outer.attach(msg)
        except:
            print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
            raise

    composed = outer.as_string()

    # Send the email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(sender, gmail_password)
            s.sendmail(sender, recipients, composed)
            s.close()
        # print("Email sent!")
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])
        raise

# def daily_email():
#     print("Run-----")
#     schedule.every().day.at("09:00").do(email_logfile)
#
#     while True:
#         # print('True')
#         schedule.run_pending()
#         time.sleep(60)     # wait one minute


