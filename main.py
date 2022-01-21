import google.auth
from flask import abort, jsonify
from uuid import uuid4
from google.cloud import storage
from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound
from google.auth.transport import requests
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


def handle_put(req):
    j = req.json
    if j is None or "product" not in j:
        abort(400, description="missing product")
    p = j["product"]
    if p not in p2v:
        abort(400, description="invalid product")
    v = p2v[p]
    runid = str(uuid4()).replace("-", "")
    r = requests.Request()
    credentials.refresh(r)
    url = bucket.blob(f"addons/{runid}-{v}.zip").generate_signed_url(
        expiration=timedelta(minutes=10),
        service_account_email=credentials.service_account_email,
        access_token=credentials.token,
        content_type="application/zip",
        method="PUT",
    )
    return jsonify({"runid": runid, "url": url})


def handle_post(req):
    j = req.json
    if j is None:
        abort(400, description="missing body")
    if "products" not in j:
        abort(400, description="missing products")
    ps = j["products"]
    if not isinstance(ps, list):
        abort(400, description="products must be a list")
    ps = set(ps)
    for p in ps:
        if p not in p2v:
            abort(400, description="invalid product")
    lev = j["loglevel"] if "loglevel" in j else 0
    if not isinstance(lev, int):
        abort(400, description="invalid loglevel")
    if "runid" not in j:
        abort(400, description="missing runid")
    runid = j["runid"]
    if not isinstance(runid, str):
        abort(400, description="invalid runid")
    vs = [p2v[p] for p in ps]
    for v in vs:
        if not bucket.blob(f"addons/{runid}-{v}.zip").exists():
            abort(400, description="missing zip")
    out = {}
    for p in ps:
        url = f"{target}?product={p}&addon={runid}&loglevel={lev}"
        tasks_client.create_task(
            parent=parent,
            task={
                "http_request": {
                    "oidc_token": {
                        "audience": target,
                        "service_account_email": sa,
                    },
                    "url": url,
                },
                "name": f"{parent}/tasks/{runid}",
            },
        )
        out[p] = runid
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
    elif req.method == "PUT":
        return handle_put(req)
    else:
        abort(400)
