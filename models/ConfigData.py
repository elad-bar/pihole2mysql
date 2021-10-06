import json
import logging
import os
import sys
from typing import Optional, Any


class ConfigData:
    mysql_username: str
    mysql_password: str
    mysql_host: str
    mysql_database: str
    mysql_table: str
    pihole_db_path: str
    pihole_enrich_batch_size: int
    pihole_enrich_cycle_interval: float
    pihole_counter_cycle_interval: float
    is_debug: bool

    def __init__(self):
        self._config = self._get_config_data()

        self.mysql_username = self.get_config_item("MYSQL_USERNAME")
        self.mysql_password = self.get_config_item("MYSQL_PASSWORD")
        self.mysql_host = self.get_config_item("MYSQL_HOST")
        self.mysql_database = self.get_config_item("MYSQL_DATABASE")
        self.mysql_table = self.get_config_item("MYSQL_TABLE")
        self.pihole_db_path = self.get_config_item("PIHOLE_DB_PATH")

        debug = self.get_config_item("DEBUG", False)

        self.is_debug = str(debug).lower() == str(True).lower()

        self.pihole_enrich_batch_size = int(self.get_config_item("PIHOLE_ENRICH_BATCH_SIZE", 75000))
        self.pihole_enrich_cycle_interval = float(self.get_config_item("PIHOLE_ENRICH_CYCLE_INTERVAL", 60))
        self.pihole_counter_cycle_interval = float(self.get_config_item("PIHOLE_COUNTER_CYCLE_INTERVAL", 60))

        log_level = logging.INFO

        if self.is_debug:
            log_level = logging.DEBUG

        root = logging.getLogger()
        root.setLevel(log_level)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s', datefmt='%b %d %Y %H:%M:%S')
        handler.setFormatter(formatter)
        root.addHandler(handler)

    def get_config_item(self, key, default: Optional[Any] = None):
        item_json = self._config.get(key, default)
        item_env = os.getenv(key, item_json)

        return item_env

    @staticmethod
    def _get_config_data() -> dict:
        data = {}

        if os.path.isfile("./config.json"):
            with open("./config.json") as f:
                content = f.read()

                data = json.loads(content)

        return data

    def __repr__(self):
        data = {
            "mysql_username": self.mysql_username,
            "mysql_password": self.mysql_password,
            "mysql_host": self.mysql_host,
            "mysql_database": self.mysql_database,
            "mysql_table": self.mysql_table,
            "pihole_db_path": self.pihole_db_path,
            "pihole_enrich_batch_size": self.pihole_enrich_batch_size,
            "pihole_enrich_cycle_interval": self.pihole_enrich_cycle_interval,
            "pihole_counter_cycle_interval": self.pihole_counter_cycle_interval
        }

        to_string = f"{data}"

        return to_string
