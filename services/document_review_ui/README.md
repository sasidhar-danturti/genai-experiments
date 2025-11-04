# Document Review UI

This Django service hosts the human-in-the-loop console for documents that
require manual validation before progressing through the IDP workflow. The UI is
intentionally isolated from the core ingestion services while still consuming
canonical outputs, standardised tables, and extracted insights. A lightweight
Flask companion app is included for environments such as Posit Connect where a
WSGI-compatible micro-framework is preferred.

## Features

* Displays a queue of documents that failed quality, fraud, or correctness
  validation.
* Supports assignment to reviewers (self-assign or explicit selection) and
  tracks review state transitions.
* Shows canonical payloads, standardised outputs, and enrichment insights in a
  single view with support for attachments and nested content.
* Persists reviewer comments, corrected payloads, and emits the results to the
  IDP event queue so the original extraction can be invalidated.
* Integrates with corporate SSO/ADFS by configuring the authentication backend
  through environment variables (`ENABLE_ADFS_AUTH=true`).
* Ships with a Posit-friendly Flask interface (`services/document_review_ui/flask_app`)
  that reuses the same data sources, review workflow, and storage models.
* Retrieves documents from either Databricks Delta tables or Amazon Redshift via
  the pluggable data-source abstraction.

## Configuration

The service reads its configuration from environment variables:

| Variable | Description |
| --- | --- |
| `DJANGO_SECRET_KEY` | Secret key used by Django. |
| `DJANGO_DEBUG` | Enable debug mode (`true`/`false`). |
| `DJANGO_ALLOWED_HOSTS` | Comma-separated host whitelist. |
| `DJANGO_DB_BACKEND` | `sqlite`, `databricks`, `redshift`, or `postgres`. |
| `ENABLE_ADFS_AUTH` | Enable Azure AD/ADFS authentication backend. |
| `DOCUMENT_REVIEW_SOURCE` | `databricks` (default) or `redshift`. |
| `DOCUMENT_REVIEW_EVENT_PUBLISHER` | `logging` (default) or `sqs`. |
| `DOCUMENT_REVIEW_AD_ACCESS_MAP` | JSON object whose keys are AD group names allowed to sign in. |

See `document_review_ui/document_review_ui/settings.py` and
`reviews/services/data_sources.py` for the complete list of options, including
credentials for Databricks, Redshift, and SQS.

## Event flow

1. The view layer calls `ReviewService.sync_pending_reviews` to pull candidate
   documents from the configured data source.
2. Reviewers inspect the canonical payload and submit corrections via the UI.
3. The completed review is written to Django's database, and a payload is
   dispatched through the configured event publisher.
4. Downstream IDP components consume the event, invalidate the previous extract,
   and replay the canonical ingestion with the corrected content.

## Development

Install Django and the optional dependencies for your chosen backend, then run
migrations and start the development server:

```bash
pip install django boto3 psycopg2-binary
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```

Use Django's built-in admin to create users or integrate with the corporate SSO
provider. The home screen lives at `/` once you have authenticated.

## Running the Flask interface (Posit Connect compatible)

The Flask entrypoint lives in `services/document_review_ui/flask_app`. It shares
the same Django models and services to ensure a consistent review experience
while remaining deployable to environments such as Posit Connect.

```bash
pip install -r services/document_review_ui/flask_app/requirements.txt
python manage.py migrate  # ensure the database schema exists
export FLASK_APP=services.document_review_ui.flask_app
export FLASK_DEFAULT_USERNAME=local_tester  # local development only
flask run --host=0.0.0.0 --port=8000
```

The Flask UI expects Posit Connect (or your reverse proxy) to inject the
authenticated username, email, and group claims via the `X-Connect-Username`,
`X-Connect-Email`, and `X-Connect-Groups` headers. During local development you
can set `FLASK_DEFAULT_USERNAME` and `POSIT_DEFAULT_EMAIL_DOMAIN` to bypass
external SSO requirements.

## Deployment guides

Step-by-step instructions for production roll-outs (including container-based
Django deployment and Posit Connect hosting) are documented in
[`DEPLOYMENT.md`](./DEPLOYMENT.md).
