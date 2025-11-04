"""ASGI config for the document review UI project."""
from __future__ import annotations

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "document_review_ui.settings")

application = get_asgi_application()
