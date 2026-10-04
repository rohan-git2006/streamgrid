import json
import subprocess
import sys
import time
import urllib.request

BASE = "http://localhost:8080"
REGION = "asia-south"
SESSIONS = 10
TIMEOUT = 120


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def kubectl(*args):
    subprocess.run(["kubectl", *args], check=True, capture_output=True)


def statuses(ids):
    return [call("GET", f"/sessions/{i}")["status"] for i in ids]


def wait_until(ids, want, label):
    start = time.time()
    while time.time() - start < TIMEOUT:
        if all(s == want for s in statuses(ids)):
            return time.time() - start
        time.sleep(0.5)
    print(f"TIMEOUT waiting for {label}")
    sys.exit(1)


def main():
    print(f"1. creating {SESSIONS} sessions in {REGION}")
    ids = [call("POST", "/sessions", {"user_id": f"chaos-{i}", "region": REGION})["id"]
           for i in range(SESSIONS)]
    wait_until(ids, "RUNNING", "initial placement")
    print("   all RUNNING")

    print("2. killing the fleet (agent scaled to 0)")
    kubectl("scale", "deployment/agent", "--replicas=0")
    detect = wait_until(ids, "QUEUED", "failure detection")
    print(f"   all sessions re-queued after {detect:.1f}s")

    print("3. restoring the fleet (agent scaled to 1)")
    kubectl("scale", "deployment/agent", "--replicas=1")
    recover = wait_until(ids, "RUNNING", "recovery")
    print(f"   all sessions RUNNING again after {recover:.1f}s")

    for i in ids:
        try:
            call("DELETE", f"/sessions/{i}")
        except Exception:
            pass

    print()
    print("RESULT")
    print(f"  sessions affected : {SESSIONS}")
    print(f"  failure detection : {detect:.1f}s")
    print(f"  full recovery     : {recover:.1f}s")


main()