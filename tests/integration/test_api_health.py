from fastapi.testclient import TestClient

from hots_connector.api.app import create_app
from hots_connector.config.settings import Settings


def test_health():
    app = create_app(settings=Settings())
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
