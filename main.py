from google.cloud import bigquery

def csv_loader(data, context):
        client = bigquery.Client()
        project_id = 'qwiklabs-gcp-02-b4c45e26bb81'
        dataset_name = 'csvtestdataset'
        bucket_name = 'csvtestbuckett'
        version = 'v14'
        
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = ';'
        job_config.write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE

        # get the URI for uploaded CSV in GCS from 'data'
        uri = 'gs://' + bucket_name + '/' + data['name']

        if data['name'] == 'ELAVON_CDT.csv':
                table_id = project_id + '.' + dataset_name + '.' + 'ELAVON_CDT'
        
        if data['name'] == 'ELAVON_CGT.csv':
                table_id = project_id + '.' + dataset_name + '.' + 'ELAVON_CGT'
        
        if data['name'] == 'ELAVON_CP_CNP.csv':
                table_id = project_id + '.' + dataset_name + '.' + 'ELAVON_CP_CNP'
        
        if data['name'] == 'Interchange_Rate_lookup.csv':
                table_id = project_id + '.' + dataset_name + '.' + 'Interchange_Rate_lookup'

        # lets do this
        load_job = client.load_table_from_uri(
                uri,
                table_id,
                job_config=job_config)

        print('Starting job {}'.format(load_job.job_id))
        print('Function=csv_loader, Version=' + version)
        print('File: {}'.format(data['name']))

        load_job.result()  # wait for table load to complete.
        print('Job finished.')

        destination_table = client.get_table(table_id)
        print('Loaded {} rows.'.format(destination_table.num_rows))