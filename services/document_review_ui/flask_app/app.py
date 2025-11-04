"""Flask views for reviewing documents on Posit Connect or similar hosts."""
from __future__ import annotations

import json
import os
from functools import lru_cache

from flask import (
    Flask,
    abort,
    flash,
    g,
    redirect,
    render_template,
    request,
    url_for,
)
from django.db import transaction
from django.shortcuts import get_object_or_404

from ..reviews.models import (
    DocumentReview,
    DocumentReviewStatus,
    ReviewComment,
    ReviewOutcome,
    ReviewTrigger,
)
from ..reviews.services.data_sources import data_source_from_env
from ..reviews.services.event_bus import event_publisher_from_env
from ..reviews.services.review_service import ReviewService, build_review_service
from .auth import AccessDenied, Identity, resolve_identity


def create_app() -> Flask:
    """Application factory used by WSGI servers and tests."""

    app = Flask(__name__, template_folder="templates")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.environ.get("DJANGO_SECRET_KEY", "change-me"))
    app.config.setdefault("TEMPLATES_AUTO_RELOAD", True)

    @app.template_filter("pretty_json")
    def _pretty_json(value) -> str:
        try:
            return json.dumps(value, indent=2, sort_keys=True)
        except TypeError:
            return json.dumps(str(value), indent=2)

    @lru_cache
    def _review_service() -> ReviewService:
        data_source = data_source_from_env()
        publisher = event_publisher_from_env()
        return build_review_service(data_source, publisher)

    @app.before_request
    def _inject_identity() -> None:
        try:
            identity = resolve_identity(request.headers.items())
        except AccessDenied as exc:
            abort(403, description=str(exc))
        g.identity = identity

    @app.context_processor
    def _inject_globals():
        identity: Identity | None = getattr(g, "identity", None)
        return {
            "current_identity": identity,
            "status_choices": DocumentReviewStatus.choices,
            "outcome_choices": ReviewOutcome.choices,
            "trigger_choices": ReviewTrigger.choices,
        }

    @app.route("/")
    def home():
        service = _review_service()
        try:
            service.sync_pending_reviews(limit=200)
        except Exception as exc:  # pragma: no cover - surface configuration problems in the UI
            flash(f"Unable to refresh queue: {exc}", "error")

        queryset = DocumentReview.objects.select_related("assigned_to")
        status = request.args.get("status")
        trigger = request.args.get("trigger")
        mine = request.args.get("mine")

        if status:
            queryset = queryset.filter(status=status)
        if trigger:
            queryset = queryset.filter(trigger=trigger)
        if mine == "true":
            identity: Identity = g.identity
            queryset = queryset.filter(assigned_to=identity.user)

        reviews = list(queryset.order_by("-created_at")[:200])
        return render_template("home.html", reviews=reviews)

    @app.route("/reviews/<int:review_id>")
    def review_detail(review_id: int):
        review = get_object_or_404(
            DocumentReview.objects.select_related("assigned_to").prefetch_related("comments__author"),
            pk=review_id,
        )
        return render_template("document_detail.html", review=review)

    @app.post("/reviews/<int:review_id>/assign")
    def assign_review(review_id: int):
        review = get_object_or_404(DocumentReview, pk=review_id)
        identity: Identity = g.identity
        action = request.form.get("action")
        if action == "self":
            service = _review_service()
            service.assign_review(review, identity.user.id)
            flash("Document assigned to you.", "success")
        elif action == "unassign":
            review.assigned_to = None
            review.assigned_at = None
            review.status = DocumentReviewStatus.PENDING
            review.save(update_fields=["assigned_to", "assigned_at", "status", "updated_at"])
            flash("Document returned to queue.", "success")
        else:
            flash("Unsupported assignment action.", "error")
        return redirect(url_for("review_detail", review_id=review.id))

    @app.post("/reviews/<int:review_id>/comments")
    def add_comment(review_id: int):
        review = get_object_or_404(DocumentReview, pk=review_id)
        comment = request.form.get("comment", "").strip()
        if not comment:
            flash("Comment text is required.", "error")
            return redirect(url_for("review_detail", review_id=review.id))
        proposed_raw = request.form.get("proposed_changes", "").strip()
        proposed_changes = None
        if proposed_raw:
            try:
                proposed_changes = json.loads(proposed_raw)
            except json.JSONDecodeError as exc:
                flash(f"Unable to parse proposed changes JSON: {exc}", "error")
                return redirect(url_for("review_detail", review_id=review.id))

        identity: Identity = g.identity
        ReviewComment.objects.create(
            review=review,
            author=identity.user,
            comment=comment,
            proposed_changes=proposed_changes,
        )
        flash("Comment added to the review.", "success")
        return redirect(url_for("review_detail", review_id=review.id))

    def _load_json_field(field_name: str):
        raw = request.form.get(field_name, "").strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Field {field_name} contains invalid JSON: {exc}") from exc

    @app.post("/reviews/<int:review_id>/complete")
    def complete_review(review_id: int):
        review = get_object_or_404(DocumentReview, pk=review_id)
        outcome_value = request.form.get("outcome")
        if not outcome_value:
            flash("Outcome is required to complete a review.", "error")
            return redirect(url_for("review_detail", review_id=review.id))

        identity: Identity = g.identity
        service = _review_service()
        try:
            reviewed_canonical = _load_json_field("reviewed_canonical_document")
            reviewed_standardized = _load_json_field("reviewed_standardized_output")
            reviewed_insights = _load_json_field("reviewed_insights")
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("review_detail", review_id=review.id))

        try:
            with transaction.atomic():
                service.complete_review(
                    review,
                    reviewer_id=identity.user.id,
                    outcome=ReviewOutcome(outcome_value),
                    reviewed_canonical=reviewed_canonical,
                    reviewed_standardized=reviewed_standardized,
                    reviewed_insights=reviewed_insights,
                )
        except Exception as exc:  # pragma: no cover - surfaces queue failures to UI
            flash(f"Unable to dispatch review: {exc}", "error")
            return redirect(url_for("review_detail", review_id=review.id))

        flash("Review completed and dispatched.", "success")
        return redirect(url_for("home"))

    return app

