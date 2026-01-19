from __future__ import annotations

from sekoia_automation.module import Module

from anozrway_modules.connector import AnozrwayDomainSearchConnector


class AnozrwayModule(Module):
    """Anozrway module entrypoint."""

    name = "Anozrway"
    description = "Anozrway provides domain-based breach and leak intelligence through a secured API."

    connectors = [AnozrwayDomainSearchConnector]
