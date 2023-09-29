# библиотеки для отправки email
import smtplib
import subprocess
import sys
from typing import Tuple, Union
import logging 
import pandas as pd

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)


# ----- send email -------------

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Tuple, Union, List

# ------- gluing --------------

import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame

#-------------------------------
DEF_SPARK_PARAMS = {
    "spark.dynamicAllocation.maxExecutors": "100",
    "spark.executor.cores": "2",
    "spark.executor.memory": "10g",
    "spark.executor.memoryOverhead": "5g",
    "spark.driver.memoryOverhead": "1g",
    "spark.scheduler.mode": "FAIR",
}


def build_spark_pure(app_name: str, params: dict = None) -> SparkSession:
    """
    Creates a Spark Session object
    Parameters
    ----------
    app_name: str
        spark application name
    params: dict
        spark session params
    Returns
    -------
    spark: SparkSession
        A new spark session object
    """
    spark_conf = SparkConf()
    if params is not None:
        for key, value in params.items():
            spark_conf.set(key, value)

    spark = SparkSession.builder.appName(app_name).config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sql('set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict')

    return spark


def build_spark(
        app_name: str,
        user: str,
        spark_home: str,
        params: dict = None
) -> SparkSession:
    findspark.init(spark_home)
    spark_params = DEF_SPARK_PARAMS.copy()
    spark_params.update({"spark.yarn.queue": f"root.users.{user}"})
    spark_params.update(
        {'spark.jars': ('...')}
    )
    if params is not None:
        spark_params.update(params)
    spark = build_spark_pure(app_name, spark_params)
    return spark

    
def run_cmd(
    cmd: Union[list, str], 
    operation_name: str,
    verbose: bool = False
) -> None:
    
    shell_cmd = True if isinstance(cmd, str) else False
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=shell_cmd,
        universal_newlines=True,
    )
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    if s_return == 0:
        logging.info(f"Success {operation_name}")
        if verbose and s_output:
            sys.stdout.write(s_output)
        if verbose and s_err:
            sys.stdout.write(s_err)
    else:
        raise RuntimeError(f"Error while {operation_name}: {s_err}")
    
    return s_output, s_err



def change_cmd_output(
    s_output: str, 
    output_type: str
):
    """Converts string output from cmd to pandas dataframe
    Args:
        s_output: output from cmd command
        output_type: variable for formatting
    """
    if output_type == 'dirs':
        col_name = 'user'
        col_num = 2
    elif output_type == 'default':
        col_name = 'table_or_db'
        col_num = 4
    else:
        col_name = 'unknown'
        col_num = 0
        
    empty_df = pd.DataFrame([], columns=['num_files', 'memory', 'memory_per_file', col_name])
    
    if len(s_output) == 0:
        return empty_df
    
    to_df = list()
    for line in s_output.split('\n')[:-1]:
        line = line.replace(' ', '').replace(',','').split('\t')
        while '' in line:
            line.remove('')
        to_df.append(line)
        

    
    to_df = pd.DataFrame(to_df, columns=['num_files', 'memory', 'memory_per_file', col_name])
    to_df[col_name] = to_df[col_name].str.split('/', expand= True)[col_num]
    to_df = to_df[[col_name, 'num_files', 'memory_per_file', 'memory']]
    return to_df


def send_email(
        mail_name: str,
        mail_pass: str,
        send_text: str,
        subject: str,
        host: str,
        to: List[str],
        sending_format: str = 'html'
):
    """Sending email from smtp. If you use msk-smtp.megafon.ru on msk-hdp-dn172,
    you don't need login and password.

    Args:
        mail_name: any sender name (you can choose your own)
        mail_pass: optional
        send_text: text in the letter
        subject: theme of the letter
        host: host for smtp sender
        to: list of emails to send
    """
    
    send_text = f"""  
              Hi <br>
            <html>
              <head></head>
              <body>
                {send_text.to_html(index=False)}
              </body>
            </html>
        """
    
    msg = MIMEMultipart()
    msg['From'] = mail_name
    msg['To'] = to
    msg['Subject'] = subject
    
    if sending_format == 'html':
        msg.attach(MIMEText(send_text, 'html'))
        
    else:
        msg.attach(MIMEText(send_text, 'plain'))

    with smtplib.SMTP(host) as server:
        server.login(mail_name, mail_pass)
        server.starttls()
        message = msg.as_string()
        server.sendmail(mail_name, to, message)
        server.quit()