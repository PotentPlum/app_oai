"""Entry point for the EcoPulse desktop application.

This module wires together the application service layer and the Tkinter UI.
Keeping the top-level script tiny and well-documented makes it easy to debug
start-up issues (e.g., missing dependencies, database connectivity) without
needing to read through the UI code itself.
"""

import logging
import os

from src.app_service import AppService
from src.ui.dashboard import Dashboard


def main() -> None:
    """Bootstrap the service layer and launch the Tkinter dashboard.

    Logging defaults to ``INFO`` to surface operational events such as fetch
    runs and scheduler activity. Set the ``LOG_LEVEL`` environment variable to
    ``DEBUG`` to see per-request diagnostics emitted throughout the codebase.
    """

    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    service = AppService()
    root = Dashboard(service)
    root.mainloop()


if __name__ == "__main__":
    main()
