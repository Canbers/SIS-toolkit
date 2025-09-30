## SIS Toolkit


### Available Scripts

The toolkit includes two main bulk operation scripts:

#### 1. Users: reassign permission bundles
- **Purpose**: Replace permission bundles for users who have specific target bundles
- **How it works**: Finds all users with any of the selected target bundles, removes those bundles, and assigns them a replacement bundle instead
- **Use case**: Migrating users from old permission bundles to new consolidated ones

#### 2. Users: assign user group (by bundles or locations)
- **Purpose**: Add users to a user group based on their permission bundles or locations
- **How it works**: 
  - **By bundles**: Finds users who have any of the selected permission bundles and adds them to the chosen user group
  - **By locations**: Finds users who have any of the selected locations and adds them to the chosen user group
- **Use case**: Organizing users into groups for scalable control of location visibility

### What's in this repo

- `web-app/index.html` — The single-page UI.
- `web-app/app.js` — Main client-side logic.
- `web-app/scripts/permissionReassign.js` — Helper script for bulk permission reassignment.
- `web-app/scripts/userGroupAssign.js` — Helper script for assigning users to user groups.
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

**Adding new scripts**: The toolkit supports custom script UIs through the hybrid approach in `app.js`. Each script can define its own fields and custom UI layout. See the existing scripts in `web-app/scripts/` for examples.


