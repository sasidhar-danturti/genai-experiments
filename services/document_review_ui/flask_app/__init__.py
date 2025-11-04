"""Flask interface for the document review UI."""
from __future__ import annotations

import os

import django
from django.apps import apps

# Ensure Django settings are available so the ORM models can be reused by Flask.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "document_review_ui.settings")
if not apps.ready:
    django.setup()

from .app import create_app  # noqa: E402

# Expose a module-level application object for WSGI servers and Posit Connect.
app = create_app()

__all__ = ["app", "create_app"]
