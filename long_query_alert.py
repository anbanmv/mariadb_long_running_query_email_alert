# Python script to rotate MongoDB log files
# Author: Anban Malarvendan
# License: GNU GENERAL PUBLIC LICENSE Version 3 + 
#          Section 7: Redistribution/Reuse of this code is permitted under the 
#          GNU v3 license, as an additional term ALL code must carry the 
#          original Author(s) credit in comment form.
# This script requires below mentioned performance schema consumer to be enabled:
# UPDATE performance_schema.setup_consumers SET enabled = 1 WHERE name = 'events_statements_history_long';

import mysql.connector
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def execute_query():
    connection = mysql.connector.connect(
        user='<your_username>',
        password='<your_password>',
        host='<your_host>',
        database='<your_database>'
    )

    cursor = connection.cursor()

    query = """
    SELECT left(digest_text, 64)
         , ROUND(SUM(timer_end-timer_start)/1000000000, 1) AS tot_exec_ms
         , ROUND(SUM(timer_end-timer_start)/1000000000/COUNT(*), 1) AS avg_exec_ms
         , ROUND(MIN(timer_end-timer_start)/1000000000, 1) AS min_exec_ms
         , ROUND(MAX(timer_end-timer_start)/1000000000, 1) AS max_exec_ms
         , ROUND(SUM(timer_wait)/1000000000, 1) AS tot_wait_ms
         , ROUND(SUM(timer_wait)/1000000000/COUNT(*), 1) AS avg_wait_ms
         , ROUND(MIN(timer_wait)/1000000000, 1) AS min_wait_ms
         , ROUND(MAX(timer_wait)/1000000000, 1) AS max_wait_ms
         , ROUND(SUM(lock_time)/1000000000, 1) AS tot_lock_ms
         , ROUND(SUM(lock_time)/1000000000/COUNT(*), 1) AS avglock_ms
         , ROUND(MIN(lock_time)/1000000000, 1) AS min_lock_ms
         , ROUND(MAX(lock_time)/1000000000, 1) AS max_lock_ms
         , MIN(LEFT(DATE_SUB(NOW(), INTERVAL (isgs.VARIABLE_VALUE - TIMER_START*10e-13) second), 19)) AS first_seen
         , MAX(LEFT(DATE_SUB(NOW(), INTERVAL (isgs.VARIABLE_VALUE - TIMER_START*10e-13) second), 19)) AS last_seen
         , COUNT(*) as cnt
      FROM performance_schema.events_statements_history_long
      JOIN information_schema.global_status AS isgs
     WHERE isgs.variable_name = 'UPTIME'
     GROUP BY LEFT(digest_text,64)
     ORDER BY tot_exec_ms DESC
    """

    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    connection.close()

    return result

def send_email(body):
    sender_email = "<your_email>"
    receiver_email = "<your_email>"
    password = "<email_password>"

    subject = "Long Running Queries Report"
    message = MIMEMultipart()
    message.attach(MIMEText(body, "plain"))
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())

if __name__ == "__main__":
    result = execute_query()

    # Format the result for email
    email_body = "Long Running Queries Report:\n\n"
    for row in result:
        email_body += str(row) + "\n"

    # Send email with the query result
    send_email(email_body)
