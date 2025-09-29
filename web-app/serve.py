from flask import Flask, request, send_from_directory, jsonify, make_response
import requests
import os
import re
import secrets
from urllib.parse import urlparse

APP_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=APP_DIR, static_url_path="")

# Edit per environment
ALLOWED_HOSTS = [
    "*.tractionguest.com",
]

SENSITIVE = re.compile(r"(?i)authorization|api[-_ ]?key|x-api-key|token|secret")


@app.after_request
def security(resp):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self'; "
        "connect-src 'self' https:; "
        "base-uri 'none'; "
        "form-action 'self'"
    )
    return resp


@app.before_request
def ensure_csrf():
    if request.path == "/proxy" and request.method == "POST":
        # JSON only
        if request.mimetype != "application/json":
            return jsonify({"error": "JSON required"}), 415

        # Same-origin check using Origin header
        origin = request.headers.get("Origin") or ""
        host_url = request.host_url.rstrip("/")
        if not origin.startswith(host_url):
            return jsonify({"error": "Invalid origin"}), 403


@app.route("/")
def root():
    # Serve index and set a simple CSRF-ish cookie (not validated server-side here)
    resp = make_response(send_from_directory(APP_DIR, "index.html"))
    try:
        token = secrets.token_hex(16)
    except Exception:
        token = "na"
    resp.set_cookie("csrftoken", token, samesite="Lax", secure=False, httponly=False)
    return resp


@app.route("/app.js")
def app_js():
    return send_from_directory(APP_DIR, "app.js")


@app.post("/proxy")
def proxy():
    payload = request.get_json(force=True) or {}
    method = (payload.get("method") or "GET").upper()
    url = payload.get("url") or ""
    headers = payload.get("headers") or {}
    body = payload.get("body", None)
    timeout_s = float(payload.get("timeout", 30))

    # Validate URL and host
    u = urlparse(url)
    if u.scheme not in ("https", "http") or not u.netloc:
        return jsonify({"error": "Invalid URL"}), 400
    if ALLOWED_HOSTS:
        hostname = (u.hostname or "").lower()
        allowed = False
        for allowed_host in ALLOWED_HOSTS:
            allowed_host_lower = allowed_host.lower()
            if allowed_host_lower.startswith("*."):
                # Wildcard pattern: check if hostname ends with the domain part
                domain = allowed_host_lower[2:]  # Remove "*."
                if hostname.endswith("." + domain) or hostname == domain:
                    allowed = True
                    break
            else:
                # Exact match
                if hostname == allowed_host_lower:
                    allowed = True
                    break
        if not allowed:
            return jsonify({"error": f"Host not allowed: {u.hostname}"}), 400

    # Never log sensitive headers
    safe_hdrs = {k: v for k, v in headers.items() if not SENSITIVE.search(k)}

    try:
        resp = requests.request(
            method=method,
            url=url,
            headers=headers,
            data=body if isinstance(body, (str, bytes)) else None,
            json=body if isinstance(body, (dict, list)) else None,
            timeout=timeout_s,
        )
        out = {"status": resp.status_code, "headers": dict(resp.headers)}
        try:
            out["body"] = resp.json()
        except Exception:
            out["body"] = resp.text
        return jsonify(out)
    except requests.RequestException as e:
        return (
            jsonify(
                {
                    "error": str(e),
                    "method": method,
                    "url": url,
                    "headers": safe_hdrs,
                }
            ),
            502,
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    print(f"Open http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=False)

