from sekoia_automation.module import Module

from anozrway_modules.domain_search import DomainSearch


class AnozrwayModule(Module):
    name = "Anozrway"
    description = "Anozrway provides domain-based breach and leak intelligence through a secured API."

    # Register actions by their docker_parameters string
    actions = {
        "domain_search": DomainSearch,
    }


if __name__ == "__main__":
    module = AnozrwayModule()
    module.run()
