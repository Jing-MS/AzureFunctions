import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, ReportLevel
from azure.kusto.ingest.status import KustoIngestStatusQueues

app = func.FunctionApp()

@app.schedule(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 

def QueryTaskMonitoringLog(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    credential = DefaultAzureCredential() # after pushing to the Azure cloud, this function will use the MSI instead. Please remember to assign the masterreader's role to the MSI. 
    logs_query_client = LogsQueryClient(credential)
    
    start_of_last_utc_day = (datetime.now(pytz.utc)-timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    duration = timedelta(days=1)

    query = """
    AppEvents
    | extend logtime = todatetime(Properties.LogTime)
    | where logtime between (startofday(now(-1d)) .. endofday(now(-1d)))
    | where Properties.ExperimentationGroup has "Bing"
    | where Name == "SparkJobUtilization"
    | where 
        Properties.AnalysisType == "AAStream" and 
        Properties.PipelineType == "Adf"
    | where Properties.JobState == "SUCCEEDED"
    | distinct
        jobUrl = tostring(Properties.JobUrl),
        runtime = totimespan(Properties.TotalRunningTime) / 1h,
        Day = startofday(logtime)
    | summarize 
        IngestionTime = now(),
        Name = "SucceededAA_P90RunTime",
        Value = percentile(runtime, 90)
        by Day
    """

    response = logs_query_client.query_workspace(
        workspace_id="42be50a4-118c-4aca-81ae-a59709b406e0", 
        query=query,
        timespan=(start_of_last_utc_day, duration)
        )
    if response.status == LogsQueryStatus.SUCCESS:
        data = response.tables
    else:
        # LogsQueryPartialResult
        error = response.partial_error
        data = response.partial_data
        logging.info(f"ERROR: \n{error}")

    for table in data:
        df = pd.DataFrame(data=table.rows, columns=table.columns)
        logging.info(f"DATA: \n{df}")
    
    writeToKusto(df)   


    

# Write data to Kusto
def writeToKusto(data: pd.DataFrame):

    # Ingest data to Kusto
    cluster = "https://ingest-ane.kusto.windows.net/"

    #kcsb = KustoConnectionStringBuilder.with_interactive_login(cluster) # for local testing
    kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster) # .add database Logs ingestors ('a5f3c0e6-24b2-40b1-912a-083332f3c5d3')
    
    client = QueuedIngestClient(kcsb)
    ingestion_props = IngestionProperties(
        database="Logs",
        table="test_jijing", # .create table test_jijing(Day:datetime, Name:string, Value:decimal)
        data_format=DataFormat.CSV, 
        report_level=ReportLevel.FailuresAndSuccesses
    )
    client.ingest_from_dataframe(data, ingestion_properties=ingestion_props)

    # Check the status of the ingestion
    qs = KustoIngestStatusQueues(client)    
    MAX_BACKOFF = 30
    backoff = 1
    while qs.success.is_empty() and qs.failure.is_empty():
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
        logging.info(f"No new messages. backing off for {backoff} seconds")

    if qs.success.is_empty():
        logging.info(f"FAILURE : {qs.failure.pop(10)}")
    else:
        logging.info(f"SUCCESS : {qs.success.pop(10)}")