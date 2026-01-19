from anozrway_modules import AnozrwayModule
from anozrway_modules.domain_search import DomainSearch

if __name__ == "__main__":
    module = AnozrwayModule()
    module.register(DomainSearch, "domain_search")
    module.run()
