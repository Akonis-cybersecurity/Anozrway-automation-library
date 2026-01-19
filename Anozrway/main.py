from sekoia_automation.module import Module
from anozrway_modules.domain_search import DomainSearch

if __name__ == "__main__":
    module = Module()
    module.register(DomainSearch, "domain_search")
    module.run()
