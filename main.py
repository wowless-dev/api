import google.auth
from flask import abort, jsonify
from uuid import uuid4
from google.cloud import storage
from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound
from google.auth.transport import requests
from base64 import urlsafe_b64decode
from datetime import timedelta

client = storage.Client()
bucket = client.bucket("wowless.dev")
target = "https://wowless-pxnmni7wma-uc.a.run.app/wowless"
sa = "wowless-invoker@www-wowless-dev.iam.gserviceaccount.com"

parent = "projects/www-wowless-dev/locations/us-central1/queues/wowless"
tasks_client = tasks_v2.CloudTasksClient()

credentials, _ = google.auth.default()

p2v = {
    "wow": "Mainline",
    "wowt": "Mainline",
    "wow_classic": "TBC",
    "wow_classic_era": "Vanilla",
    "wow_classic_era_ptr": "Vanilla",
    "wow_classic_ptr": "TBC",
}


def handle_post(req):
    j = req.json
    if j is None or "zip" not in j or "products" not in j:
        abort(400)
    z = j["zip"]
    if not isinstance(z, str):
        abort(400)
    ps = j["products"]
    if not isinstance(ps, list):
        abort(400)
    ps = set(ps)
    for p in ps:
        if p not in p2v:
            abort(400)
    lev = j["loglevel"] if "loglevel" in j else 0
    if not isinstance(lev, int):
        abort(400, description="invalid loglevel")
    out = {}
    for p in ps:
        uuid = str(uuid4()).replace("-", "")
        v = p2v[p]
        bucket.blob(f"addons/{uuid}-{v}.zip").upload_from_string(
            urlsafe_b64decode(z), content_type="application/zip"
        )
        tasks_client.create_task(
            parent=parent,
            task={
                "http_request": {
                    "oidc_token": {
                        "audience": target,
                        "service_account_email": sa,
                    },
                    "url": f"{target}?product={p}&addon={uuid}&loglevel={lev}",
                },
                "name": f"{parent}/tasks/{uuid}",
            },
        )
        out[p] = uuid
    return jsonify(out)


def handle_get(req):
    if "runid" not in req.args:
        abort(400)
    runid = req.args["runid"]
    try:
        if tasks_client.get_task(name=f"{parent}/tasks/{runid}"):
            return jsonify({"status": "pending"})
    except NotFound as e:
        if not str(e).startswith("404 The task no longer exists"):
            abort(404, description="unknown/expired runid")
    files = []
    for p in p2v:
        files.extend(
            client.list_blobs("wowless.dev", prefix=f"logs/{p}-{runid}-")
        )
    rawlogurls = {}
    r = requests.Request()
    credentials.refresh(r)
    for f in files:
        key = f.name.split("-")[-2]
        rawlogurls[key] = f.generate_signed_url(
            expiration=timedelta(minutes=10),
            service_account_email=credentials.service_account_email,
            access_token=credentials.token,
        )
    return jsonify({"status": "done", "rawlogurls": rawlogurls})


def api(req):
    if req.method == "POST":
        return handle_post(req)
    elif req.method == "GET":
        return handle_get(req)
    else:
        abort(400)
