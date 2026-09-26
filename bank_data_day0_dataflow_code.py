import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems

class ParseMultiTableFile(beam.DoFn):
    """
    A DoFn that streams a multi-table sequential file from GCS,
    parsing it into table name and row dictionary pairs.
    """
    def process(self, file_path):
        with FileSystems.open(file_path) as f:
            current_table = None
            headers = None
            for line in f:
                line = line.decode('utf-8').strip()
                if not line:
                    continue

                # Detect HDR|tableName to start a new table buffer
                header_match = re.match(r"^HDR\|([a-zA-Z0-9_]+)$", line)
                if header_match:
                    current_table = header_match.group(1).lower()
                    headers = None
                    continue

                # Detect FTR| to close/reset the active table buffer
                if re.match(r"^FTR\|", line):
                    current_table = None
                    headers = None
                    continue

                # Append row data to the currently active table buffer
                if current_table is not None:
                    if headers is None:
                        # The first line of data is the header row
                        headers = line.split('|')
                    else:
                        values = line.split('|')
                        row_dict = dict(zip(headers, values))
                        yield (current_table, row_dict)

class WriteToBigQueryFn(beam.DoFn):
    """
    A DoFn that dynamically batches and writes list of rows to BigQuery.
    """
    def __init__(self, dataset_id, project_id=None):
        if project_id is None and "." in dataset_id:
            self.project_id, self.dataset_id = dataset_id.split(".", 1)
        else:
            self.project_id = project_id
            self.dataset_id = dataset_id

    def process(self, element):
        table_name, rows = element
        rows_list = list(rows)
        if not rows_list:
            return

        from google.cloud import bigquery
        bq_client = bigquery.Client(project=self.project_id)
        table_ref = f"{self.dataset_id}.{table_name}"
        if self.project_id:
            table_ref = f"{self.project_id}.{table_ref}"

        # Configure the BigQuery load job with autodetect schema
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )

        load_job = bq_client.load_table_from_json(
            rows_list, table_ref, job_config=job_config
        )
        load_job.result()  # Block and wait for the load job to complete
        print(f"Successfully loaded {len(rows_list)} rows into BigQuery table '{table_ref}'.")

def copy_multi_table_file(bucket_name, blob_name, dataset_id, project_id=None):
    """
    Reads a multi-table text file from GCS using Apache Beam, parses the content,
    and loads each table's dataset dynamically into BigQuery.
    """
    if not bucket_name or bucket_name.strip() in ("gs://", "gs:/", "gs"):
        raise ValueError(f"Invalid GCS bucket path: '{bucket_name}'. It must contain a valid GCS bucket name.")

    # Clean prefix and safely extract the actual bucket name and nested path
    raw_path = bucket_name.replace("gs://", "").strip("/")
    path_parts = raw_path.split("/", 1)
    
    bucket = path_parts[0]
    folder_prefix = path_parts[1] if len(path_parts) > 1 else ""
    clean_blob = blob_name.strip("/")
    
    if folder_prefix:
        gcs_path = f"gs://{bucket}/{folder_prefix}/{clean_blob}"
    else:
        gcs_path = f"gs://{bucket}/{clean_blob}"

    options = PipelineOptions()
    
    print(f"Starting Apache Beam pipeline for {gcs_path}...")
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Create GCS Path" >> beam.Create([gcs_path])
            | "Parse Multi-Table File" >> beam.ParDo(ParseMultiTableFile())
            | "Group By Table" >> beam.GroupByKey()
            | "Write To BigQuery" >> beam.ParDo(WriteToBigQueryFn(dataset_id, project_id))
        )

if __name__ == "__main__":
    # Example Usage:
    copy_multi_table_file(
        bucket_name="gs://hackathon_noida/Harsh_Girish_Choudhary/source_data/",
        blob_name="BankEvalDB_Day0_Interleaved.txt",
        dataset_id="dbs-data-ai-ai-core.bronze_harsh"
    )
