"""Resolve authenticated identities for the Flask interface."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable, Set, Tuple

from django.contrib.auth import get_user_model


class AccessDenied(Exception):
    """Raised when the incoming request is not authorised to use the console."""


@dataclass(frozen=True)
class Identity:
    """Represents the resolved reviewer identity."""

    user: Any  # Django user instance
    username: str
    email: str | None
    groups: Set[str]


_POSIT_USER_HEADER = os.environ.get("POSIT_USER_HEADER", "X-Connect-Username")
_POSIT_EMAIL_HEADER = os.environ.get("POSIT_EMAIL_HEADER", "X-Connect-Email")
_POSIT_GROUP_HEADER = os.environ.get("POSIT_GROUP_HEADER", "X-Connect-Groups")
_POSIT_ALLOWED_GROUPS = {
    value.strip()
    for value in os.environ.get("POSIT_ALLOWED_GROUPS", "").split(",")
    if value.strip()
}
_DEFAULT_EMAIL_DOMAIN = os.environ.get("POSIT_DEFAULT_EMAIL_DOMAIN")
_DEFAULT_USERNAME = os.environ.get("FLASK_DEFAULT_USERNAME")


def _normalise_groups(raw_value: str | None) -> Set[str]:
    if not raw_value:
        return set()
    separators = [",", ";", "|"]
    values = [raw_value]
    for separator in separators:
        if separator in raw_value:
            values = raw_value.split(separator)
            break
    return {value.strip() for value in values if value.strip()}


def resolve_identity(headers: Iterable[Tuple[str, str]]) -> Identity:
    """Resolve or provision the Django user for the incoming request."""

    header_map = {name: value for name, value in headers}
    username = header_map.get(_POSIT_USER_HEADER) or _DEFAULT_USERNAME
    if not username:
        raise AccessDenied(
            "Request is missing the username header configured via POSIT_USER_HEADER; "
            "set FLASK_DEFAULT_USERNAME for local testing."
        )

    email = header_map.get(_POSIT_EMAIL_HEADER)
    if not email and _DEFAULT_EMAIL_DOMAIN:
        email = f"{username}@{_DEFAULT_EMAIL_DOMAIN}"

    groups = _normalise_groups(header_map.get(_POSIT_GROUP_HEADER))
    if _POSIT_ALLOWED_GROUPS and not (groups & _POSIT_ALLOWED_GROUPS):
        raise AccessDenied(
            "User does not belong to any authorised groups. Update POSIT_ALLOWED_GROUPS to grant access."
        )

    user_model = get_user_model()
    user, created = user_model.objects.get_or_create(
        username=username,
        defaults={"email": email or ""},
    )
    if not created and email and user.email != email:
        user.email = email
        user.save(update_fields=["email"])

    return Identity(user=user, username=username, email=email, groups=groups)
