from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

HOST = "127.0.0.1"
PORT = 8111
BASE_URL = f"http://{HOST}:{PORT}"
TOKEN = "demo-alpha-token"


def request(method: str, path: str, body: dict[str, object] | None = None) -> tuple[int, object]:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers.update({"Content-Type": "application/json", "Idempotency-Key": "demo-http-0001"})
    call = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(call, timeout=2) as response:
        return response.status, json.loads(response.read())


def port_available() -> bool:
    with socket.socket() as sock:
        return sock.connect_ex((HOST, PORT)) != 0


def main() -> None:
    if not port_available():
        raise SystemExit(f"port {PORT} is already in use")
    with tempfile.TemporaryDirectory(prefix="durable-http-demo-") as temp_dir:
        environment = {
            **os.environ,
            "DW_DB_PATH": str(Path(temp_dir) / "demo.db"),
            "HOST": HOST,
            "PORT": str(PORT),
        }
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "durable_workflows.app:app",
                "--host",
                HOST,
                "--port",
                str(PORT),
            ],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(50):
                try:
                    with urllib.request.urlopen(BASE_URL + "/health", timeout=0.2) as response:
                        if response.status == 200:
                            break
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.05)
            else:
                raise RuntimeError("server did not become healthy")

            status, created = request(
                "POST",
                "/jobs",
                {"kind": "data_import", "records": [{"value": 4}, {"value": 8}, {"value": 15}]},
            )
            job_id = created["id"]
            for _ in range(50):
                _, job = request("GET", f"/jobs/{job_id}")
                if job["state"] in {"succeeded", "failed", "cancelled"}:
                    break
                time.sleep(0.05)
            _, events = request("GET", f"/jobs/{job_id}/events")
            print(json.dumps({"http_status": status, "job": job, "events": events}, indent=2))
            if job["state"] != "succeeded":
                raise SystemExit(1)
        finally:
            server.terminate()
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


if __name__ == "__main__":
    main()
