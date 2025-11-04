"""Initial migration for the reviews app."""
from __future__ import annotations

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    """Create the tables required for document reviews."""

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="DocumentReview",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("document_id", models.CharField(max_length=256)),
                ("job_id", models.CharField(blank=True, max_length=256, null=True)),
                ("title", models.CharField(blank=True, max_length=512)),
                (
                    "trigger",
                    models.CharField(
                        choices=[
                            ("low_extraction_quality", "Low extraction quality"),
                            ("fraud_signal", "Fraud checks failed"),
                            ("correctness_failure", "Correctness checks failed"),
                            ("manual", "Manual submission"),
                        ],
                        default="low_extraction_quality",
                        max_length=64,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending", "Pending"),
                            ("in_progress", "In progress"),
                            ("completed", "Completed"),
                            ("returned", "Returned to workflow"),
                        ],
                        default="pending",
                        max_length=32,
                    ),
                ),
                (
                    "outcome",
                    models.CharField(
                        blank=True,
                        choices=[
                            ("approved", "Approved"),
                            ("corrected", "Corrected"),
                            ("rejected", "Rejected"),
                        ],
                        max_length=32,
                        null=True,
                    ),
                ),
                ("canonical_document", models.JSONField(help_text="Canonical document payload to review")),
                ("standardized_output", models.JSONField(blank=True, null=True)),
                ("insights", models.JSONField(blank=True, null=True)),
                ("reviewed_canonical_document", models.JSONField(blank=True, null=True)),
                ("reviewed_standardized_output", models.JSONField(blank=True, null=True)),
                ("reviewed_insights", models.JSONField(blank=True, null=True)),
                ("assigned_at", models.DateTimeField(blank=True, null=True)),
                ("latest_reviewed_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "assigned_to",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="assigned_document_reviews",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
                "unique_together": {("document_id", "trigger")},
            },
        ),
        migrations.CreateModel(
            name="ReviewComment",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("comment", models.TextField()),
                ("proposed_changes", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "author",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL),
                ),
                (
                    "review",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="comments",
                        to="reviews.documentreview",
                    ),
                ),
            ],
            options={"ordering": ["created_at"]},
        ),
        migrations.CreateModel(
            name="ReviewEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("payload", models.JSONField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("sent_at", models.DateTimeField(blank=True, null=True)),
                ("status", models.CharField(default="pending", max_length=32)),
                ("error_message", models.TextField(blank=True)),
                (
                    "review",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="events",
                        to="reviews.documentreview",
                    ),
                ),
            ],
            options={"ordering": ["-created_at"]},
        ),
        migrations.AddIndex(
            model_name="documentreview",
            index=models.Index(fields=["document_id"], name="review_document_idx"),
        ),
        migrations.AddIndex(
            model_name="documentreview",
            index=models.Index(fields=["status", "trigger"], name="review_status_trigger_idx"),
        ),
    ]
