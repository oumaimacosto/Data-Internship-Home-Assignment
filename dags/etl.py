from datetime import timedelta, datetime
import json
from typing import List, Dict, Any  
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator




TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""




@task()
def extract(read_csv: pd.DataFrame = pd.read_csv) -> pd.DataFrame:
    """Extract data from jobs.csv and save each item to staging/extracted as a text file."""
    df = read_csv("source/jobs.csv")

    # Extract the context column data and save each item to staging/extracted as a text file
    for index, row in df.iterrows():
        item_text = f"context: {row['context']}"
        with open(f"staging/extracted/item_{index}.txt", 'w') as file:
            file.write(item_text)

    return df



@task()
def transform(data: pd.DataFrame) -> List[Dict[str, Any]]:
    """Clean and convert extracted elements to the desired schema and save to staging/transformed."""

    transformed_data = []

    for index, row in data.iterrows():
        
        cleaned_description = clean_description(row['context'])

        # Transform data to schema
        transformed_item = {
            "job": {
                "title": row['title'],
                "industry": row['industry'],
                "description": cleaned_description,
                "employment_type": row['employment_type'],
                "date_posted": row['date_posted'],
            },
            "company": {
                "name": row['company_name'],
                "link": row['company_linkedin_link'],
            },
            "education": {
                "required_credential": row['job_required_credential'],
            },
            "experience": {
                "months_of_experience": row['job_months_of_experience'],
                "seniority_level": row['seniority_level'],
            },
            "salary": {
                "currency": row['salary_currency'],
                "min_value": row['salary_min_value'],
                "max_value": row['salary_max_value'],
                "unit": row['salary_unit'],
            },
            "location": {
                "country": row['country'],
                "locality": row['locality'],
                "region": row['region'],
                "postal_code": row['postal_code'],
                "street_address": row['street_address'],
                "latitude": row['latitude'],
                "longitude": row['longitude'],
            },
        }

        # Save the transformed item as a JSON file
        with open(f"staging/transformed/item_{index}.json", 'w') as file:
            json.dump(transformed_item, file)

        transformed_data.append(transformed_item)

    return transformed_data


@task()
def load(transformed_data: List[Dict[str, Any]]) -> None:
    """Load data to SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for index, transformed_item in enumerate(transformed_data):
      
        
        job_insert_query = """
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
        """
        company_insert_query = """
            INSERT INTO company (name, link)
            VALUES (?, ?)
        """
        education_insert_query = """
            INSERT INTO education (required_credential)
            VALUES (?)
        """
        experience_insert_query = """
            INSERT INTO experience (months_of_experience, seniority_level)
            VALUES (?, ?)
        """
        salary_insert_query = """
            INSERT INTO salary (currency, min_value, max_value, unit)
            VALUES (?, ?, ?, ?)
        """
        location_insert_query = """
            INSERT INTO location (country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        # Execute the insert queries with parameters
        sqlite_hook.run(job_insert_query, parameters=[
            transformed_item['job']['title'],
            transformed_item['job']['industry'],
            transformed_item['job']['description'],
            transformed_item['job']['employment_type'],
            transformed_item['job']['date_posted'],
        ])

        sqlite_hook.run(company_insert_query, parameters=[
            transformed_item['company']['name'],
            transformed_item['company']['link'],
        ])

        sqlite_hook.run(education_insert_query, parameters=[
            transformed_item['education']['required_credential'],
        ])

        sqlite_hook.run(experience_insert_query, parameters=[
            transformed_item['experience']['months_of_experience'],
            transformed_item['experience']['seniority_level'],
        ])

        sqlite_hook.run(salary_insert_query, parameters=[
            transformed_item['salary']['currency'],
            transformed_item['salary']['min_value'],
            transformed_item['salary']['max_value'],
            transformed_item['salary']['unit'],
        ])

        sqlite_hook.run(location_insert_query, parameters=[
            transformed_item['location']['country'],
            transformed_item['location']['locality'],
            transformed_item['location']['region'],
            transformed_item['location']['postal_code'],
            transformed_item['location']['street_address'],
            transformed_item['location']['latitude'],
            transformed_item['location']['longitude'],
        ])



DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load_op = load(transformed_data)

    create_tables >> extracted_data >> transformed_data >> load_op

etl_dag()


