from fastapi.testclient import TestClient

from durable_workflows.app import create_app
from durable_workflows.config import Settings


def test_readiness_detects_removed_database(client, settings):
    assert client.get('/health').status_code == 200
    settings.db_path.rename(settings.db_path.with_suffix('.removed'))
    assert client.get('/health').status_code == 503
    assert not settings.db_path.exists()


def test_readiness_detects_missing_schema_and_write_lock(client):
    database = client.app.state.database
    with database.session() as connection:
        connection.execute('BEGIN IMMEDIATE')
        assert client.get('/health').status_code == 503
        connection.rollback()
        connection.execute('DROP TABLE side_effects')
    assert client.get('/health').status_code == 503


def test_readiness_detects_missing_embedded_worker(tmp_path):
    app = create_app(Settings(db_path=tmp_path/'worker.db'))
    with TestClient(app) as client:
        assert client.get('/health').status_code == 200
        original = app.state.worker_threads
        app.state.worker_threads = []
        assert client.get('/health').status_code == 503
        app.state.worker_threads = original
