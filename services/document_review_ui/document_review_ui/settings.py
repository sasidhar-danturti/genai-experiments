"""Django settings for the document review UI project."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from django.core.exceptions import ImproperlyConfigured

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "unsafe-development-key")

DEBUG = os.environ.get("DJANGO_DEBUG", "false").lower() == "true"

ALLOWED_HOSTS: List[str] = [host.strip() for host in os.environ.get("DJANGO_ALLOWED_HOSTS", "*").split(",") if host]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "document_review_ui.reviews",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "document_review_ui.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "reviews" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "document_review_ui.wsgi.application"
ASGI_APPLICATION = "document_review_ui.asgi.application"


def _default_database() -> Dict[str, Any]:
    return {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }


def _database_from_env() -> Dict[str, Any]:
    """Build a Django database config from environment variables."""
    backend = os.environ.get("DJANGO_DB_BACKEND")
    if not backend:
        return _default_database()

    if backend == "sqlite":
        return _default_database()

    name = os.environ.get("DJANGO_DB_NAME")
    if not name:
        raise ImproperlyConfigured("DJANGO_DB_NAME must be set when DJANGO_DB_BACKEND is configured")

    host = os.environ.get("DJANGO_DB_HOST")
    port = os.environ.get("DJANGO_DB_PORT")
    user = os.environ.get("DJANGO_DB_USER")
    password = os.environ.get("DJANGO_DB_PASSWORD")

    if backend == "databricks":
        return {
            "ENGINE": "django.db.backends.mysql",
            "NAME": name,
            "HOST": host,
            "PORT": port,
            "USER": user,
            "PASSWORD": password,
            "OPTIONS": {
                "driver": os.environ.get("DJANGO_DB_DRIVER", "Simba Spark ODBC Driver"),
            },
        }
    if backend == "redshift":
        return {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": name,
            "HOST": host,
            "PORT": port or "5439",
            "USER": user,
            "PASSWORD": password,
            "OPTIONS": {
                "options": os.environ.get("DJANGO_DB_OPTIONS", ""),
            },
        }
    if backend == "postgres":
        return {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": name,
            "HOST": host,
            "PORT": port,
            "USER": user,
            "PASSWORD": password,
        }

    raise ImproperlyConfigured(f"Unsupported DJANGO_DB_BACKEND '{backend}'")


DATABASES = {"default": _database_from_env()}

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = os.environ.get("DJANGO_TIME_ZONE", "UTC")
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "static"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "/accounts/login/"
LOGIN_REDIRECT_URL = "/"
LOGOUT_REDIRECT_URL = "/"

AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
]

if os.environ.get("ENABLE_ADFS_AUTH", "false").lower() == "true":
    AUTHENTICATION_BACKENDS.insert(0, "django_auth_adfs.backend.AdfsAuthCodeBackend")


AD_GROUP_ACCESS_MAP: Dict[str, str] = {}
if access_map := os.environ.get("DOCUMENT_REVIEW_AD_ACCESS_MAP"):
    try:
        AD_GROUP_ACCESS_MAP = json.loads(access_map)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive logging path
        raise ImproperlyConfigured("DOCUMENT_REVIEW_AD_ACCESS_MAP must be valid JSON") from exc

