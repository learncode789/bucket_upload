from google.cloud import bigquery
from google.cloud import pubsub_v1
import sqlalchemy
import pandas as pd
import gcsfs
import json
import os
import sys
from google.cloud import secretmanager
from time import sleep
from configparser import ConfigParser

project_id = os.environ.get("PROJECT_ID")
db_user = None
db_key = None
data_set = " "
db_retry_count = 5
db_retry_sleep_seconds = 5
REQUIRED_FIELD_KEY = "R_"
SENDER_INFO_FOR_PUBSUB = "common_bucket_data_load_cloud_function"
KEY = "AR_FILE"
CONFIG_SECTION = "AR_FILE_DATA"
FUNC_NAME = "Service_API"
config = ConfigParser()
config.read("./config.ini")







class MessageToAR_FILEFiles:
    def __init__(self, function_name, events, loc):
        self.function_name = function_name
        self.events = events
        self.loc = loc


def publish_message_to_pubsub(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    try:
        request_json = json.dumps(message)
        print("request_json:", str(request_json))
        data = request_json.encode("utf-8")
        future = publisher.publish(topic_path, data, origin="sre_dataload_from_bucket", username="admin")
        message_id = future.result()
        print("pubsub message_id: " + message_id)
        return message_id
    except Exception as e:
        print("Exception in publish_message_to_pubsub:" + str(e))
        return 0


def handle_exception(e):
    print(str(e))
    exception_group = str(os.environ.get("EXCEPTION_GROUP"))
    alertTO = AlertTO(os.environ.get("ENVIRONMENT") + ": Exception occurred in sre_dataload_from_bucket cloud function", exception_group, e, 3,
                      os.environ.get("EXCEPTION_MAILER"))
    publish_message_to_pubsub(project_id, os.environ.get("ALERTS_SEND_TOPIC"), alertTO)


def AccessSecretVersion(project_id, secret_id, version_id="latest"):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = client.secret_version_path(project_id, secret_id, version_id)
        response = client.access_secret_version(name)
        payload = response.payload.data.decode("UTF-8")
        return str(payload)
    except Exception as e:
        handle_exception("Not able to get the secrets: " + str(e))
    return None


def set_secrets():
    global db_user
    global db_key
    if db_user is not None and db_key is not None:
        return
    db_user = AccessSecretVersion(project_id, os.environ.get("DB_USER_KEY"))
    db_key = AccessSecretVersion(project_id, os.environ.get("DB_PASSWORD_KEY"))


def get_schema_field_type(s):
    if s is not None:
        s = s.replace(REQUIRED_FIELD_KEY, "")
    return s


def is_required_mode(s):
    return s is not None and REQUIRED_FIELD_KEY in s



def upload_to_bigquery(file_path, table_name, db_object, csv_data):
    global data_set
    try:
        client = bigquery.Client(str(project_id))
        data_set = client.dataset(get_data_set_name(table_name.upper(), db_object))
        table_ref = data_set.table(table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.autodetect = True
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = "|"
        schema_array = []
        for k, v in get_table_schema(table_name, db_object).items():
            if is_required_mode(v):
                schema_array.append(bigquery.SchemaField(k, get_schema_field_type(v), mode="REQUIRED"))
            else:
                schema_array.append(bigquery.SchemaField(k, v))

            job_config.schema = schema_array
        load_job = client.load_table_from_uri(file_path, table_ref, job_config=job_config)
        print("BigQuery data upload status:", load_job.result().state)
    except Exception as e:
        print("Upload to bigquery failed with exception {}  trying again with JSON message".format(e))
        data_to_publish = {"dataset_name": data_set.dataset_id, "table_name": table_name, "table_data": csv_data}
        message_id = publish_message_to_pubsub(project_id, os.environ["TOPIC_ID_FOR_DATA_LOAD"], data_to_publish)
        print("PubSub published message id for data load: ", message_id)
        sys.exit("Exiting the process")


def get_db_object():
    print("Creating new db instance...")
    for i in range(db_retry_count):
        try:
            conn_str = "mysql+pymysql://" + str(db_user) + ":" + str(db_key) + "@" + os.environ["DB_PRIVATE_IP"] + "/" + \
                       os.environ["DB_NAME"] + "?host=" + os.environ["DB_PRIVATE_IP"] + "?port=3306"
            engine = sqlalchemy.create_engine(conn_str)
            db_obj = engine.connect()
            return db_obj
        except Exception as e:
            if i < (db_retry_count - 1):
                print("Retrying the db connection...")
                sleep(db_retry_sleep_seconds)
                continue
            handle_exception("Exception in get_db_object: " + str(e))


def perform_db_operation(query, db_object):
    try:
        stmt = sqlalchemy.text(query)
        with db_object.connect() as conn:
            return conn.execute(stmt)
    except Exception as e:
        print("Query:" + query)
        handle_exception("Exception in perform_db_operation: " + str(e))


def check_if_table_is_allowed(p_name, p_value, db_object):
    try:
        s = 'select count(*) as count from spectare_properties where p_name="' + p_name + '" and p_value="' + p_value + '"'
        print("query>>>>" + s)
        stmt = sqlalchemy.text(s)
        with db_object.connect() as conn:
            results = conn.execute(stmt)
            for result in results:
                for column, value in result.items():
                    return (value > 0)
    except Exception as e:
        handle_exception("Exception in check_if_table_is_allowed: " + str(e))


def get_table_schema(table_name, db_object):
    schema = {}
    try:
        query = 'select p_value as table_schema from spectare_properties where p_name="' + str(table_name) + '_SCHEMA"'
        result = perform_db_operation(query, db_object)
        for r in result:
            return json.loads(str(r['table_schema']).replace("'", '"'))
    except Exception as e:
        handle_exception("Exception in get_table_schema:" + str(e))
    return schema


def get_alert_config_for_identifier_from_db(identifier, db_object):
    query = 'select a.config_id, a.criteria_columns, a.column_rules, a.rule_type, a.column_operations, a.distribution_list, a.sleep_time, a.last_alert_time, a.priority, b.info_id,b.name,b.body,b.subject,b.message,b.workstream, b.system_name, b.application,b.severity from alert_configurations a left join alert_info b on a.alert_info_id=b.info_id where a.Status="Active" and a.identifier="' + identifier + '"'
    return perform_db_operation(query, db_object)


def read_property_value(property_name, db_object):
    query = 'select p_value from spectare_properties where p_name="' + property_name + '"'
    return perform_db_operation(query, db_object)


def get_csv_data(bucket_name, csv_name):
    fs = gcsfs.GCSFileSystem(project=project_id)
    with fs.open(bucket_name + '/' + csv_name) as f:
        df = pd.read_csv(f, delimiter="|")
        return df.to_dict('records')


def get_table_name(filename, db_object):
    key = "__"
    table_name = filename.split(key)[0].split(".")[0]
    property_data = read_property_value(table_name+"_TABLE", db_object)
    for r in property_data:
        table_name = str(r['p_value'])
    return table_name


def get_data_set_name(table_name, db_object):
    final_data_set = os.environ.get("DATASET_NAME")
    property_data = read_property_value(table_name + "_DATASET", db_object)
    for r in property_data:
        final_data_set = str(r['p_value'])
    print("dataset to be used>>>" + str(final_data_set))
    return final_data_set


def start_batch_process_for_AR_FILE(file_name):
    event_to_publish = []
    key = "__"
    if KEY in file_name and key in file_name:
        print("Processing AR_FILE file:", file_name)
        AR_FILE_file_name = file_name.split(key)[1].split(".")[0]
        for key, value in config[ONFIG_SECTION].items():
            if AR_FILE_file_name.lower() in key:
                if "," in config[ONFIG_SECTION].get(key):
                    event_to_publish = config[CONFIG_SECTION].get(key).split(",")
                else:
                    event_to_publish.append(config[CONFIG_SECTION].get(key))
                message_to_publish_for_FILES = MessageToAR_FILEFiles(FUNC_NAME, event_to_publish, "US")
                topic_id = os.environ.get("TOPIC_ID_FOR_FILES")
                message_id = publish_message_to_pubsub(project_id, topic_id, message_to_publish_for_FILES.__dict__)
                print("PubSub published message id for FILES_PROCESS: ", message_id)
                break


def process_bucket_files(data, context):
    file_name = data['name']
    if "/" in file_name:
        return "OK"
    try:
        print("Received file:" + file_name)
        set_secrets()
        db_object = get_db_object()
        bq_table_name = get_table_name(file_name, db_object)
        valid_table = check_if_table_is_allowed("BQ_TABLE_NAME", bq_table_name, db_object)
        if not valid_table:
            print("Invalid file uploaded, do not proceed. File Name: " + file_name + " and Table Name: " + bq_table_name)
            db_object.close()
            return "Invalid File"
        print("Destination Table: " + bq_table_name + " for File Name: " + file_name)
        start_batch_process_for_AR_FILE(file_name)
        bucket_name = data['bucket']
        file_path = "gs://" + bucket_name + "/" + file_name
        csv_data = get_csv_data(bucket_name, file_name)
        if len(csv_data) < 1:
            db_object.close()
            sys.exit("process_bucket_files: This is an empty file, quitting the process.")
        upload_to_bigquery(file_path, bq_table_name, db_object, csv_data)
        email_configs = get_alert_config_for_identifier_from_db(bq_table_name, db_object)
        rows = email_configs.fetchall()
        if len(rows) < 1:
            print("Nothing to publish to queue as table: " + bq_table_name + " does not "
                  "exist in alert configurations")
            db_object.close()
            sys.exit("process_bucket_files: nothing to publish to queue quitting the process.")
        topic_id = os.environ.get("ALERTS_DECISION_TOPIC")
        message_to_publish = {"identifier": bq_table_name, "data": csv_data}
        message_id = publish_message_to_pubsub(project_id, topic_id, message_to_publish)
        print("PubSub published message id for Alerts processing: ", message_id + " and Topic Name: " + topic_id)
        db_object.close()
    except SystemExit as s:
        print(str(s))
    except Exception as e:
        handle_exception("Exception in process_bucket_files: " + str(e))
