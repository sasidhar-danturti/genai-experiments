"""URL declarations for the review UI."""
from __future__ import annotations

from django.urls import path

from . import views

app_name = "reviews"

urlpatterns = [
    path("", views.HomeView.as_view(), name="home"),
    path("documents/<str:document_id>/", views.DocumentDetailView.as_view(), name="document-detail"),
    path(
        "documents/<str:document_id>/assign/",
        views.AssignReviewView.as_view(),
        name="document-assign",
    ),
    path(
        "documents/<str:document_id>/complete/",
        views.CompleteReviewView.as_view(),
        name="document-complete",
    ),
    path(
        "documents/<str:document_id>/comment/",
        views.AddCommentView.as_view(),
        name="document-comment",
    ),
    path(
        "api/documents/<str:document_id>/dispatch/",
        views.ReviewDispatchAPIView.as_view(),
        name="api-document-dispatch",
    ),
]
