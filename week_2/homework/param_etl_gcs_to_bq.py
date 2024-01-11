import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('zoomcamp-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    return Path(gcs_path)



@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    '''
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")'''
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load('zoomcamp-gcp-creds')
    df.to_gbq(
        destination_table='dezoomcamp_prefect.yellow_taxi_trips',
        project_id='dtc-de-408913',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into BigQuery"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    
@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = 'yellow') -> None:
    for month in months:
        etl_gcs_to_bq(year, month, color)
        
if __name__ == '__main__':

    etl_parent_flow()
    
    
    