"""Application configuration for document reviews."""
from __future__ import annotations

from django.apps import AppConfig


class ReviewsConfig(AppConfig):
    """Application configuration for the reviews app."""

    name = "document_review_ui.reviews"
    verbose_name = "Document Reviews"
