"""Database models backing the document review UI."""
from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone


class ReviewTrigger(models.TextChoices):
    """Reasons a document appears in the human-in-the-loop queue."""

    LOW_EXTRACTION_QUALITY = "low_extraction_quality", "Low extraction quality"
    FRAUD_SIGNAL = "fraud_signal", "Fraud checks failed"
    CORRECTNESS_FAILURE = "correctness_failure", "Correctness checks failed"
    MANUAL = "manual", "Manual submission"


class DocumentReviewStatus(models.TextChoices):
    """State machine for the review lifecycle."""

    PENDING = "pending", "Pending"
    IN_PROGRESS = "in_progress", "In progress"
    COMPLETED = "completed", "Completed"
    RETURNED = "returned", "Returned to workflow"


class ReviewOutcome(models.TextChoices):
    """Possible outcomes for a completed review."""

    APPROVED = "approved", "Approved"
    CORRECTED = "corrected", "Corrected"
    REJECTED = "rejected", "Rejected"


class DocumentReview(models.Model):
    """Metadata for a document that requires human review."""

    document_id = models.CharField(max_length=256)
    job_id = models.CharField(max_length=256, blank=True, null=True)
    title = models.CharField(max_length=512, blank=True)
    trigger = models.CharField(
        max_length=64,
        choices=ReviewTrigger.choices,
        default=ReviewTrigger.LOW_EXTRACTION_QUALITY,
    )
    status = models.CharField(
        max_length=32,
        choices=DocumentReviewStatus.choices,
        default=DocumentReviewStatus.PENDING,
    )
    outcome = models.CharField(
        max_length=32,
        choices=ReviewOutcome.choices,
        blank=True,
        null=True,
    )
    canonical_document = models.JSONField(help_text="Canonical document payload to review")
    standardized_output = models.JSONField(blank=True, null=True)
    insights = models.JSONField(blank=True, null=True)
    reviewed_canonical_document = models.JSONField(blank=True, null=True)
    reviewed_standardized_output = models.JSONField(blank=True, null=True)
    reviewed_insights = models.JSONField(blank=True, null=True)
    assigned_to = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name="assigned_document_reviews",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    assigned_at = models.DateTimeField(blank=True, null=True)
    latest_reviewed_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["document_id"], name="review_document_idx"),
            models.Index(fields=["status", "trigger"], name="review_status_trigger_idx"),
        ]
        ordering = ["-created_at"]
        unique_together = ("document_id", "trigger")

    def __str__(self) -> str:  # pragma: no cover - trivial representation
        return f"{self.document_id} ({self.get_trigger_display()})"

    def mark_assigned(self, user: settings.AUTH_USER_MODEL) -> None:
        """Assign the document to a reviewer."""

        self.assigned_to = user
        self.assigned_at = timezone.now()
        self.status = DocumentReviewStatus.IN_PROGRESS
        self.save(update_fields=["assigned_to", "assigned_at", "status", "updated_at"])

    def complete(
        self,
        *,
        reviewer: settings.AUTH_USER_MODEL,
        outcome: ReviewOutcome,
        reviewed_canonical: dict | None = None,
        reviewed_standardized: dict | None = None,
        reviewed_insights: dict | None = None,
    ) -> None:
        """Persist the reviewed artefacts and mark the review completed."""

        self.assigned_to = reviewer
        self.status = DocumentReviewStatus.COMPLETED
        self.outcome = outcome
        self.latest_reviewed_at = timezone.now()
        if reviewed_canonical is not None:
            self.reviewed_canonical_document = reviewed_canonical
        if reviewed_standardized is not None:
            self.reviewed_standardized_output = reviewed_standardized
        if reviewed_insights is not None:
            self.reviewed_insights = reviewed_insights
        self.save()


class ReviewComment(models.Model):
    """Free-form comments captured during review."""

    review = models.ForeignKey(DocumentReview, on_delete=models.CASCADE, related_name="comments")
    author = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    comment = models.TextField()
    proposed_changes = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["created_at"]

    def __str__(self) -> str:  # pragma: no cover - trivial representation
        return f"Comment by {self.author} on {self.review}"


class ReviewEvent(models.Model):
    """Audit log of events sent back to the ingestion workflow."""

    review = models.ForeignKey(DocumentReview, on_delete=models.CASCADE, related_name="events")
    payload = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(blank=True, null=True)
    status = models.CharField(max_length=32, default="pending")
    error_message = models.TextField(blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:  # pragma: no cover - trivial representation
        return f"Event {self.id} for review {self.review_id}"
