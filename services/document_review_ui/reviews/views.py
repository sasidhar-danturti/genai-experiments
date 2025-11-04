"""Views powering the document review UI."""
from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Dict

from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.views import View
from django.views.generic import DetailView, ListView

from .auth import ReviewAccessRequiredMixin
from .forms import AssignmentForm, ReviewCommentForm, ReviewCompletionForm
from .models import DocumentReview, DocumentReviewStatus, ReviewOutcome, ReviewTrigger
from .services.data_sources import data_source_from_env
from .services.event_bus import event_publisher_from_env
from .services.review_service import ReviewService, build_review_service


@lru_cache
def _get_review_service() -> ReviewService:
    data_source = data_source_from_env()
    publisher = event_publisher_from_env()
    return build_review_service(data_source, publisher)


class HomeView(LoginRequiredMixin, ReviewAccessRequiredMixin, ListView):
    """Landing page listing documents awaiting review."""

    model = DocumentReview
    template_name = "reviews/home.html"
    context_object_name = "reviews"
    paginate_by = 25

    def get_queryset(self):  # type: ignore[override]
        service = _get_review_service()
        try:
            service.sync_pending_reviews(limit=200)
        except Exception as exc:  # pragma: no cover - surface configuration issues to the UI
            messages.error(self.request, f"Unable to refresh queue: {exc}")
        queryset = DocumentReview.objects.select_related("assigned_to")
        trigger = self.request.GET.get("trigger")
        status = self.request.GET.get("status")
        assignee = self.request.GET.get("assignee")
        if trigger:
            queryset = queryset.filter(trigger=trigger)
        if status:
            queryset = queryset.filter(status=status)
        if assignee == "unassigned":
            queryset = queryset.filter(assigned_to__isnull=True)
        elif assignee == "me" and self.request.user.is_authenticated:
            queryset = queryset.filter(assigned_to=self.request.user)
        return queryset

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:  # type: ignore[override]
        context = super().get_context_data(**kwargs)
        context["status_choices"] = DocumentReviewStatus.choices
        context["outcome_choices"] = ReviewOutcome.choices
        context["trigger_choices"] = ReviewTrigger.choices
        return context


class DocumentDetailView(LoginRequiredMixin, ReviewAccessRequiredMixin, DetailView):
    """Detailed view allowing reviewers to inspect the canonical payload."""

    model = DocumentReview
    template_name = "reviews/document_detail.html"
    slug_field = "document_id"
    slug_url_kwarg = "document_id"
    context_object_name = "review"

    def get_queryset(self):  # type: ignore[override]
        return (
            DocumentReview.objects.select_related("assigned_to")
            .prefetch_related("comments__author")
            .all()
        )

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:  # type: ignore[override]
        context = super().get_context_data(**kwargs)
        review: DocumentReview = context["review"]
        context["canonical_pretty"] = json.dumps(review.canonical_document, indent=2, sort_keys=True)
        context["standardized_pretty"] = (
            json.dumps(review.standardized_output, indent=2, sort_keys=True)
            if review.standardized_output
            else None
        )
        context["insights_pretty"] = (
            json.dumps(review.insights, indent=2, sort_keys=True) if review.insights else None
        )
        context["attachments"] = review.canonical_document.get("attachments", []) if isinstance(
            review.canonical_document, dict
        ) else []
        context["completion_form"] = ReviewCompletionForm()
        context["comment_form"] = ReviewCommentForm()
        context["assignment_form"] = AssignmentForm(instance=review)
        return context


class AssignReviewView(LoginRequiredMixin, ReviewAccessRequiredMixin, View):
    """Assign a review to a user (defaulting to the current reviewer)."""

    def post(self, request: HttpRequest, document_id: str) -> HttpResponse:
        review = get_object_or_404(DocumentReview, document_id=document_id)
        if request.POST.get("assign_to") == "self" or "assigned_to" not in request.POST:
            service = _get_review_service()
            service.assign_review(review, request.user.id)
            messages.success(request, "Document assigned to you.")
            return redirect(reverse("reviews:document-detail", args=[document_id]))

        form = AssignmentForm(request.POST, instance=review)
        if form.is_valid():
            review.assigned_to = form.cleaned_data["assigned_to"]
            if review.assigned_to:
                from django.utils import timezone

                review.assigned_at = timezone.now()
                review.status = DocumentReviewStatus.IN_PROGRESS
            else:
                review.assigned_at = None
                review.status = DocumentReviewStatus.PENDING
            review.save(update_fields=["assigned_to", "assigned_at", "status", "updated_at"])
            messages.success(request, "Document assignment updated.")
        else:
            messages.error(request, "Unable to update assignment. Please review the form.")
        return redirect(reverse("reviews:document-detail", args=[document_id]))


class CompleteReviewView(LoginRequiredMixin, ReviewAccessRequiredMixin, View):
    """Handle submission of completed reviews."""

    def post(self, request: HttpRequest, document_id: str) -> HttpResponse:
        review = get_object_or_404(DocumentReview, document_id=document_id)
        form = ReviewCompletionForm(request.POST)
        if not form.is_valid():
            messages.error(request, "Unable to complete review. Please correct the highlighted errors.")
            return redirect(reverse("reviews:document-detail", args=[document_id]))

        service = _get_review_service()
        cleaned = form.cleaned_data
        reviewed_canonical = cleaned.get("reviewed_canonical_document")
        reviewed_standardized = cleaned.get("reviewed_standardized_output")
        reviewed_insights = cleaned.get("reviewed_insights")
        try:
            service.complete_review(
                review,
                reviewer_id=request.user.id,
                outcome=ReviewOutcome(cleaned["outcome"]),
                reviewed_canonical=reviewed_canonical,
                reviewed_standardized=reviewed_standardized,
                reviewed_insights=reviewed_insights,
            )
        except Exception as exc:
            messages.error(request, f"Unable to dispatch review: {exc}")
            return redirect(reverse("reviews:document-detail", args=[document_id]))
        messages.success(request, "Review completed and sent to workflow queue.")
        return redirect("reviews:home")


class AddCommentView(LoginRequiredMixin, ReviewAccessRequiredMixin, View):
    """Capture reviewer comments associated with a document."""

    def post(self, request: HttpRequest, document_id: str) -> HttpResponse:
        review = get_object_or_404(DocumentReview, document_id=document_id)
        form = ReviewCommentForm(request.POST)
        if not form.is_valid():
            messages.error(request, "Unable to add comment. Please review the form errors.")
            return redirect(reverse("reviews:document-detail", args=[document_id]))
        service = _get_review_service()
        service.submit_comment(
            review,
            author_id=request.user.id,
            comment=form.cleaned_data["comment"],
            proposed_changes=form.cleaned_data.get("proposed_changes"),
        )
        messages.success(request, "Comment added to the review.")
        return redirect(reverse("reviews:document-detail", args=[document_id]))


class ReviewDispatchAPIView(LoginRequiredMixin, ReviewAccessRequiredMixin, View):
    """Allow the IDP workflow to request a reviewed payload for ingestion."""

    def post(self, request: HttpRequest, document_id: str) -> JsonResponse:
        review = get_object_or_404(DocumentReview, document_id=document_id)
        payload = {
            "document_id": review.document_id,
            "canonical_document": review.reviewed_canonical_document or review.canonical_document,
            "standardized_output": review.reviewed_standardized_output or review.standardized_output,
            "insights": review.reviewed_insights or review.insights,
            "outcome": review.outcome,
            "status": review.status,
        }
        return JsonResponse(payload)
