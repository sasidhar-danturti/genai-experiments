"""URL configuration for the document review UI."""
from __future__ import annotations

from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("document_review_ui.reviews.urls")),
]
