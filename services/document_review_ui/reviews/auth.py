"""Access helpers for Active Directory group-based authorisation."""
from __future__ import annotations

from django.conf import settings
from django.contrib.auth.mixins import UserPassesTestMixin


def user_has_review_access(user) -> bool:
    """Return whether the user belongs to an authorised AD group."""

    if not getattr(user, "is_authenticated", False):
        return False
    access_map = getattr(settings, "AD_GROUP_ACCESS_MAP", {}) or {}
    if not access_map:
        return True
    user_groups = {group.name for group in user.groups.all()}
    for group_name in access_map.keys():
        if group_name in user_groups:
            return True
    return False


class ReviewAccessRequiredMixin(UserPassesTestMixin):
    """Mixin ensuring the user belongs to an authorised AD group."""

    raise_exception = True

    def test_func(self) -> bool:
        return user_has_review_access(self.request.user)

    def handle_no_permission(self):  # type: ignore[override]
        from django.http import HttpResponseForbidden

        return HttpResponseForbidden("You do not have permission to access the document review console.")
