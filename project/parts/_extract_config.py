class Extract_from_Config:
    def __init__(self, config_file_name: str, config_tag: str) -> None:
        self.config = self._load_config(
                                        self._get_path(config_file_name),
                                        config_tag)


    def _get_path(self, config_file_name: str) -> str:
        """Get path for parts_dag folder use name and os.path

        Args:
            config_file_name (str): full name file with format(.ini)

        Returns:
            str: full path file
        """
        from os.path import abspath, dirname, join
        return join(abspath(dirname(__file__)).rsplit('\\', 1)[0], config_file_name)


    def _load_config(self, file_name: str, config_tag: str) -> dict:
        """From Config ini extract data for url 'https://search.wb.ru/exactmatch' site
        Args:
            file_name (str): Name Config.ini file
            config_tag (str): Tag in Config.ini file
        Returns:
            Dict: Config to RAM dict use python
        """
        from configparser import ConfigParser

        config_parser = ConfigParser()
        config_parser.read(file_name, encoding="utf-8")
        return \
        {
            i: config_parser[config_tag][i]
            for i in config_parser[config_tag]
        }
