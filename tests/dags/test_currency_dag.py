"""Basic checks for the currency ETL DAG. Run with:  astro dev pytest"""

import pytest
from airflow.models import DagBag

DAG_ID = "currency_exchange_etl"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"


def test_dag_is_loaded(dagbag):
    assert DAG_ID in dagbag.dags


def test_has_three_tasks(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert sorted(dag.task_ids) == ["extract_rates", "load_rates", "transform_rates"]


def test_task_order(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert dag.get_task("extract_rates").downstream_task_ids == {"transform_rates"}
    assert dag.get_task("transform_rates").downstream_task_ids == {"load_rates"}
    assert dag.get_task("load_rates").downstream_task_ids == set()


def test_dag_has_tags_and_retries(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert dag.tags
    for t in dag.tasks:
        assert t.retries >= 1
