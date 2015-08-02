import yaml

class ConfigurationLoader(object):
    def __init__(self, type, path):
        self.config_file = "%s/%s.yaml" % (path, type)
        self._config = None

    def load_config(self):
        """
            Loads yaml configuration file and returns yaml file contents as python object.
        """

        try:
            if not self._config:
                with open(self.config_file) as f:
                    self._config = yaml.load(f)
                    print self._config
        except Exception as e:
            raise e #TODO add logging

        return self._config
