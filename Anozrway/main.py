from sekoia_automation.module import Module

from anozrway_modules.domain_search import DomainSearch
from anozrway_modules.historical_connector import AnozrwayHistoricalConnector

if __name__ == "__main__":
    module = Module()

    # Action (playbook)
    module.register(DomainSearch, "domain_search")

    # Connector (intake)
    module.register(AnozrwayHistoricalConnector, "anozrway_historical")

    module.run()
