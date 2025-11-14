"""Compatibility wrapper for the legacy routing module path.

The routing implementation has been extracted into the top-level
``idp_router`` package so that it can be packaged and reused independently of
``idp_service``.  This module simply re-exports the public surface for
backwards compatibility with existing imports.
"""

from idp_router.router import *  # noqa: F401,F403
from idp_router.router import __all__  # noqa: F401

