from sekoia_automation.module import Module
from anozrway_modules.models import AnozrwayModuleConfiguration


class AnozrwayModule(Module):
    configuration: AnozrwayModuleConfiguration
