import json
from vnpy.gateway.ctp import CtpGateway

GatewayTypes = {
    "ctp": CtpGateway
}


class Configuration(object):
    def __init__(self, file_name):
        with open(file_name, 'r', encoding='utf-8') as f:
            self._attr: dict = json.load(f, encoding='utf-8')

    def __getitem__(self, item):
        if item not in self._attr:
            raise KeyError(f'"{item}" is not set in the configuration file.')
        return self._attr.get(item, None)

    def __setitem__(self, key, value):
        if key not in self._attr:
            raise KeyError(f'"{key}" is available in the configuration file.')
        self._attr[key] = value


def load_configuration(file_name):
    return Configuration(file_name)




