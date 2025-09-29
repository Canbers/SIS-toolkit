## SIS Toolkit

Simple, no-install web tools for working with SIS-related tasks. A production instance is publicly available at [sis.canby.ca](https://sis.canby.ca).

### Live site

- Visit: [sis.canby.ca](https://sis.canby.ca)

### What's in this repo

- `web-app/index.html` — The single-page UI.
- `web-app/app.js` — Main client-side logic.
- `web-app/scripts/permissionReassign.js` — Helper script for bulk permission reassignment.
- `web-app/serve.py` — Tiny Flask server used for local development and a safe HTTP proxy for API calls.

### Quick start (local)

You only need Python to run locally. The server serves the static files and provides a restricted proxy for HTTPS requests.

Prerequisites:

- Python 3.9+ and `pip`

Steps:

```bash
cd web-app
python3 -m venv .venv
source .venv/bin/activate
pip install Flask requests

# Optional: choose a different port (defaults to 8000)
export PORT=8000

python3 serve.py
# Open http://127.0.0.1:$PORT in your browser
```

### Making API calls during local dev

The local proxy at `/proxy` is locked down for safety:

- Only JSON POST requests from the same origin are accepted.
- Outbound requests are limited to specific hosts via `ALLOWED_HOSTS` in `web-app/serve.py` (defaults to `*.tractionguest.com`).

If you need to talk to other domains while developing locally, edit `ALLOWED_HOSTS` and add the allowed domain(s).

### Security notes

- Strict Content Security Policy is set for local serving.
- A simple anti-CSRF check is enforced on the proxy using the `Origin` header.
- Sensitive header names are filtered from logs.

### Deployment

- The live site is hosted at [sis.canby.ca](https://sis.canby.ca).
- The app itself is static HTML/CSS/JS; the small Flask proxy is only required when you need server-side calls to external APIs with host allow-listing.

### License

Apache License 2.0 — see `LICENSE` for details.

### Contributing

Issues and pull requests are welcome. Please keep contributions small and focused.


