"""Forms used in the review UI."""
from __future__ import annotations

from django import forms

from .models import DocumentReview, ReviewComment, ReviewOutcome


class AssignmentForm(forms.ModelForm):
    """Assign or reassign a review to a reviewer."""

    assigned_to = forms.ModelChoiceField(
        queryset=None, required=False, help_text="Select the reviewer responsible for this document."
    )

    class Meta:
        model = DocumentReview
        fields = ["assigned_to"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from django.contrib.auth import get_user_model

        User = get_user_model()
        self.fields["assigned_to"].queryset = User.objects.filter(is_active=True).order_by("username")


class ReviewCompletionForm(forms.Form):
    """Capture the outcome of a review and the corrected artefacts."""

    outcome = forms.ChoiceField(choices=ReviewOutcome.choices)
    reviewed_canonical_document = forms.JSONField(required=False)
    reviewed_standardized_output = forms.JSONField(required=False)
    reviewed_insights = forms.JSONField(required=False)


class ReviewCommentForm(forms.ModelForm):
    """Capture reviewer comments."""

    class Meta:
        model = ReviewComment
        fields = ["comment", "proposed_changes"]
