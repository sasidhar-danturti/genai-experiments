# Document Review UI Deployment Guide

This guide outlines end-to-end steps for deploying the human-in-the-loop console
in two different hosting models:

1. A traditional Django deployment (container, VM, or PaaS).
2. A lightweight Flask deployment suitable for Posit Connect.

Both entry points share the same models, data sources, and event publishers.

---

## 1. Django deployment (container, VM, or PaaS)

### Prerequisites

* Python 3.10+
* Database server (PostgreSQL recommended) accessible from the service
* Credentials for Databricks or Redshift (depending on where documents are
  stored)
* Queue destination (Amazon SQS or logging sink) for completed reviews
* Corporate identity provider details if integrating with ADFS/SSO

### Step-by-step

1. **Check out the source**
   ```bash
   git clone <repo-url>
   cd genai-experiments/services/document_review_ui
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r flask_app/requirements.txt gunicorn
   ```
   The shared `requirements.txt` covers Django, Flask, and optional libraries for
   Databricks, Redshift, and SQS.

3. **Configure environment variables** (example `.env` snippet):
   ```bash
   export DJANGO_SECRET_KEY=change-me
   export DJANGO_ALLOWED_HOSTS=review.example.com
   export DJANGO_DB_BACKEND=postgres
   export DATABASE_URL=postgres://user:password@hostname:5432/review_ui
   export DOCUMENT_REVIEW_SOURCE=databricks  # or redshift
   export DOCUMENT_REVIEW_EVENT_PUBLISHER=sqs
   export DOCUMENT_REVIEW_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/.../queue
   export DOCUMENT_REVIEW_SQS_REGION=us-east-1
   export DOCUMENT_REVIEW_DATABRICKS_HOST=...  # Only if using Databricks
   export DOCUMENT_REVIEW_DATABRICKS_TOKEN=...
   export DOCUMENT_REVIEW_DATABRICKS_ENDPOINT=...
   export DOCUMENT_REVIEW_DATABRICKS_CATALOG=...
   export DOCUMENT_REVIEW_DATABRICKS_SCHEMA=...
   export DOCUMENT_REVIEW_DATABRICKS_TABLE=...
   export DOCUMENT_REVIEW_DATABRICKS_JOB_ID=...
   export ENABLE_ADFS_AUTH=true  # optional SSO integration
   export DOCUMENT_REVIEW_AD_ACCESS_MAP='{"DocumentReviewers": "reviewers"}'
   ```
   When using PostgreSQL/`DATABASE_URL`, configure `dj-database-url` in
   `document_review_ui/settings.py` or set individual `DJANGO_DB_*`
   environment variables if preferred.

4. **Run database migrations**
   ```bash
   python manage.py migrate
   python manage.py createsuperuser  # optional for admin access
   ```

5. **Collect static files (if serving through Whitenoise or CDN)**
   ```bash
   python manage.py collectstatic --no-input
   ```

6. **Launch the application with Gunicorn**
   ```bash
   gunicorn document_review_ui.wsgi:application --bind 0.0.0.0:8000 --workers 3
   ```
   Behind a reverse proxy (NGINX, ALB, etc.), ensure TLS termination and header
   forwarding for the chosen SSO strategy.

7. **Configure health checks and background sync**
   * The home view automatically calls `ReviewService.sync_pending_reviews` on
     each request, but you can schedule a periodic job (e.g. cron invoking
     `python manage.py sync_reviews`) if you add a custom management command.
   * Expose `/admin/` (optionally restricted) for manual user provisioning.

8. **Wire the feedback loop**
   * Confirm that the configured event publisher (logging or SQS) can reach the
     downstream IDP consumer. For SQS, ensure IAM permissions are scoped to
     `sqs:SendMessage` on the target queue.

9. **Observability**
   * Configure `LOGGING` in `document_review_ui/settings.py` to ship application
     logs to your observability stack (CloudWatch, Datadog, etc.).
   * Monitor Django metrics (request latency, error rates) using your preferred
     APM agent.

---

## 2. Posit Connect deployment (Flask interface)

The Flask interface exposes the same review workflow while embracing Posit
Connect's Python deployment model.

### Prerequisites

* Posit Connect account with deployment permissions
* `rsconnect-python` CLI installed locally
* API key created in Posit Connect (User > API Keys)
* Access to the same database and data sources described above

### Step-by-step

1. **Create and activate a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r services/document_review_ui/flask_app/requirements.txt
   pip install rsconnect-python
   ```

2. **Run database migrations once** (either locally with VPN access or via a
   temporary deployment job):
   ```bash
   cd services/document_review_ui
   export DJANGO_SETTINGS_MODULE=document_review_ui.settings
   python manage.py migrate
   ```
   > **Tip:** If the database is only reachable from Posit Connect, deploy a
   > one-off job using `rsconnect deploy python` that runs `manage.py migrate`.

3. **Define deployment metadata**
   ```bash
   rsconnect config write --server <https://connect.example.com> --api-key <API_KEY>
   ```

4. **Deploy the Flask app**
   ```bash
   rsconnect deploy flask \
     --entrypoint services.document_review_ui.flask_app:app \
     --server <https://connect.example.com> \
     --api-key <API_KEY> \
     --title "Document Review Console"
   ```
   The CLI bundles `flask_app/requirements.txt` automatically. Ensure you run the
   command from the repository root so package paths resolve correctly.

5. **Configure environment variables in Posit Connect**
   After the first deployment, open the application settings in the Posit Connect
   UI and add the same variables used in the Django deployment (database
   credentials, Databricks/Redshift configuration, SQS destination). Posit will
   inject authenticated user details via headers:
   * `X-Connect-Username`
   * `X-Connect-Email`
   * `X-Connect-Groups`

   Override these header names using `POSIT_USER_HEADER`, `POSIT_EMAIL_HEADER`,
   or `POSIT_GROUP_HEADER` if your SSO configuration differs. Restrict access by
   setting `POSIT_ALLOWED_GROUPS=GroupA,GroupB`.

6. **Set secrets**
   * `FLASK_SECRET_KEY` – random string for session signing.
   * `DJANGO_SECRET_KEY` – shared with the Django project.

7. **Test the deployment**
   * Visit the Posit Connect URL and confirm that the review queue loads.
   * Complete a sample review and verify that an event reaches your downstream
     queue (CloudWatch/SQS logs).

8. **Operational tasks**
   * Configure scheduled refresh jobs if you need to pre-populate the review
     queue by adding a Posit Connect Python job that runs
     `python manage.py sync_reviews` (after implementing the command).
   * Monitor application logs via the Posit Connect dashboard.

### Header and identity mapping

The Flask interface automatically provisions Django users using the incoming
headers. For local development without SSO, set:

```bash
export FLASK_DEFAULT_USERNAME=local_reviewer
export POSIT_DEFAULT_EMAIL_DOMAIN=example.com
```

Set `POSIT_ALLOWED_GROUPS` to a comma-separated list of AD/SSO groups permitted
to access the UI. If the incoming request lacks any authorised group, the app
returns HTTP 403.

---

## Environment variable reference

| Variable | Purpose |
| --- | --- |
| `DJANGO_SECRET_KEY` | Secret key for Django and Flask session signing. |
| `DJANGO_ALLOWED_HOSTS` | Comma-separated hosts for Django deployment. |
| `DJANGO_DB_BACKEND` | Database backend (`sqlite`, `postgres`, etc.). |
| `DATABASE_URL`/`DJANGO_DB_*` | Database connection information. |
| `DOCUMENT_REVIEW_SOURCE` | `databricks` or `redshift` data source selector. |
| Databricks vars | `DOCUMENT_REVIEW_DATABRICKS_HOST`, `TOKEN`, `ENDPOINT`, `CATALOG`, `SCHEMA`, `TABLE`, `JOB_ID`. |
| Redshift vars | `DOCUMENT_REVIEW_REDSHIFT_HOST`, `PORT`, `DATABASE`, `USER`, `PASSWORD`, `TABLE`. |
| `DOCUMENT_REVIEW_EVENT_PUBLISHER` | `logging` or `sqs`. |
| SQS vars | `DOCUMENT_REVIEW_SQS_QUEUE_URL`, `DOCUMENT_REVIEW_SQS_REGION`. |
| `ENABLE_ADFS_AUTH` | Enable Django ADFS/SSO backend. |
| `DOCUMENT_REVIEW_AD_ACCESS_MAP` | JSON mapping of AD group → Django group name. |
| `FLASK_SECRET_KEY` | Session signing key for Flask deployment. |
| `POSIT_USER_HEADER` / `POSIT_EMAIL_HEADER` / `POSIT_GROUP_HEADER` | Override header names supplied by Posit. |
| `POSIT_ALLOWED_GROUPS` | Comma-separated groups authorised to use the UI. |
| `POSIT_DEFAULT_EMAIL_DOMAIN` | Domain appended to usernames when email header missing. |
| `FLASK_DEFAULT_USERNAME` | Development-only fallback username when headers are absent. |

Refer back to [`services/document_review_ui/reviews/services`](./reviews/services)
for further configuration details on data sources and event publishers.
