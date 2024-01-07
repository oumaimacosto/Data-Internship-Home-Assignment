import pandas as pd
import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timedelta
from dags.etl import extract, transform, load, etl_dag
from airflow.providers.sqlite.hooks.sqlite import SqliteHook




@pytest.fixture
def sample_data():
    # data for testing
    return pd.DataFrame({
        'datePosted': ['2022-01-01', '2022-01-02'],
        'description': ['Description 1', 'Description 2'],
        'employmentType': ['Full-time', 'Part-time'],
        'hiringOrganization': ['Company A', 'Company B'],
        
    })

@patch('dags.etl.pd.read_csv', autospec=True)
def test_extract(mock_read_csv):
    # Mock the read_csv function to return sample_data
    mock_read_csv.return_value = sample_data
    
    # Call the extract function
    result = extract(mock_read_csv)

    # Assertions
    mock_read_csv.assert_called_once_with("source/jobs.csv")
    assert isinstance(result, pd.DataFrame)

    


def test_transform(sample_data):
    # Call the transform function
    result = transform(sample_data)

    # Assertions
    assert isinstance(result, list)
    assert len(result) == 2

    

@patch('dags.etl.SqliteHook', autospec=True)
@patch('dags.etl.SqliteOperator', autospec=True)
def test_load(mock_sqlite_operator, mock_sqlite_hook):
    # Mock the necessary objects
    mock_hook_instance = Mock()
    mock_sqlite_hook.return_value.get_conn.return_value = mock_hook_instance

    # Sample transformed data
    transformed_data = [
        {
            "job": {"title": "Title 1", "industry": "Industry 1", "description": "Desc 1", "employment_type": "Full-time", "date_posted": "2022-01-01"},
            "company": {"name": "Company A", "link": "Link A"},
           
        },
        {
            "job": {"title": "Title 2", "industry": "Industry 2", "description": "Desc 2", "employment_type": "Part-time", "date_posted": "2022-01-02"},
            "company": {"name": "Company B", "link": "Link B"},
        
        },
    ]

    # Call the load function
    load(transformed_data)

    # Assertions
    mock_sqlite_hook.assert_called_once_with(sqlite_conn_id='sqlite_default')
    mock_sqlite_hook.return_value.get_conn.assert_called_once()
    assert mock_sqlite_operator.call_count == 6  
    
    
    
def test_etl_dag_definition():
    # Call the etl_dag function to get the DAG instance
    dag = etl_dag()

    # Assertions
    assert dag.dag_id == "etl_dag"
    assert len(dag.tasks) == 4  

    # Check task dependencies
    assert dag.get_task('create_tables').upstream_task_ids == set()
    assert dag.get_task('extract').upstream_task_ids == {'create_tables'}
    assert dag.get_task('transform').upstream_task_ids == {'extract'}
    assert dag.get_task('load').upstream_task_ids == {'transform'}

