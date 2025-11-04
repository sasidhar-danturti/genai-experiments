"""Domain services orchestrating document reviews."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

from ..models import DocumentReview, ReviewComment, ReviewEvent, ReviewOutcome, ReviewTrigger
from .data_sources import DocumentDataSource
from .event_bus import ReviewEventPublisher

User = get_user_model()


@dataclass
class ReviewFilters:
    """Filter parameters for the review queue."""

    trigger: Optional[str] = None
    status: Optional[str] = None
    assignee: Optional[int] = None


class ReviewService:
    """Facade encapsulating review operations."""

    def __init__(self, data_source: DocumentDataSource, publisher: ReviewEventPublisher) -> None:
        self._data_source = data_source
        self._publisher = publisher

    def sync_pending_reviews(self, *, limit: int = 100) -> Iterable[DocumentReview]:
        """Ensure the database reflects the pending review queue."""

        synced = []
        for document in self._data_source.iter_pending(limit=limit):
            review, created = DocumentReview.objects.get_or_create(
                document_id=document.document_id,
                trigger=document.trigger or ReviewTrigger.LOW_EXTRACTION_QUALITY,
                defaults={
                    "canonical_document": document.canonical,
                    "standardized_output": document.standardized,
                    "insights": document.insights,
                    "job_id": document.job_id,
                    "title": document.canonical.get("title", ""),
                },
            )
            if not created:
                review.canonical_document = document.canonical
                review.standardized_output = document.standardized
                review.insights = document.insights
                review.job_id = document.job_id
                review.title = document.canonical.get("title", review.title)
                review.save(update_fields=[
                    "canonical_document",
                    "standardized_output",
                    "insights",
                    "job_id",
                    "title",
                    "updated_at",
                ])
            synced.append(review)
        return synced

    def assign_review(self, review: DocumentReview, user_id: int) -> DocumentReview:
        """Assign a review to a user and mark it in progress."""

        reviewer = User.objects.get(pk=user_id)
        review.mark_assigned(reviewer)
        return review

    def submit_comment(
        self,
        review: DocumentReview,
        *,
        author_id: int,
        comment: str,
        proposed_changes: dict | None = None,
    ) -> ReviewComment:
        """Persist a comment for a review."""

        author = User.objects.get(pk=author_id)
        return ReviewComment.objects.create(
            review=review, author=author, comment=comment, proposed_changes=proposed_changes
        )

    def complete_review(
        self,
        review: DocumentReview,
        *,
        reviewer_id: int,
        outcome: ReviewOutcome,
        reviewed_canonical: dict | None,
        reviewed_standardized: dict | None,
        reviewed_insights: dict | None,
    ) -> DocumentReview:
        """Persist the completed review and dispatch the payload to the queue."""

        reviewer = User.objects.get(pk=reviewer_id)
        with transaction.atomic():
            review.complete(
                reviewer=reviewer,
                outcome=outcome,
                reviewed_canonical=reviewed_canonical,
                reviewed_standardized=reviewed_standardized,
                reviewed_insights=reviewed_insights,
            )
            payload = self._build_event_payload(review)
            event = ReviewEvent.objects.create(review=review, payload=payload)
            try:
                self._publisher.publish(payload)
            except Exception as exc:  # pragma: no cover - queue failure should persist
                event.status = "failed"
                event.error_message = str(exc)
                event.save(update_fields=["status", "error_message"])
                raise
            else:
                event.status = "sent"
                event.sent_at = timezone.now()
                event.save(update_fields=["status", "sent_at"])
        return review

    def _build_event_payload(self, review: DocumentReview) -> dict:
        """Construct payload for downstream IDP ingestion."""

        canonical = review.reviewed_canonical_document or review.canonical_document
        standardized = review.reviewed_standardized_output or review.standardized_output
        insights = review.reviewed_insights or review.insights
        return {
            "document_id": review.document_id,
            "job_id": review.job_id,
            "trigger": review.trigger,
            "outcome": review.outcome,
            "canonical_document": canonical,
            "standardized_output": standardized,
            "insights": insights,
            "review_id": review.id,
        }


def build_review_service(data_source: DocumentDataSource, publisher: ReviewEventPublisher) -> ReviewService:
    """Factory helper used by views."""

    return ReviewService(data_source, publisher)
