"""Admin registrations for review models."""
from __future__ import annotations

from django.contrib import admin

from .models import DocumentReview, ReviewComment, ReviewEvent


@admin.register(DocumentReview)
class DocumentReviewAdmin(admin.ModelAdmin):
    """Admin configuration for document reviews."""

    list_display = (
        "document_id",
        "trigger",
        "status",
        "assigned_to",
        "latest_reviewed_at",
    )
    list_filter = ("trigger", "status", "outcome")
    search_fields = ("document_id", "job_id", "title")
    autocomplete_fields = ("assigned_to",)


@admin.register(ReviewComment)
class ReviewCommentAdmin(admin.ModelAdmin):
    """Admin configuration for review comments."""

    list_display = ("review", "author", "created_at")
    search_fields = ("review__document_id", "comment")
    autocomplete_fields = ("review", "author")


@admin.register(ReviewEvent)
class ReviewEventAdmin(admin.ModelAdmin):
    """Admin configuration for review events."""

    list_display = ("review", "status", "created_at", "sent_at")
    search_fields = ("review__document_id",)
    list_filter = ("status",)
