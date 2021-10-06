import logging
import queue
import sqlite3
import sys

from . import get_total_seconds, to_date, millify

from datetime import datetime

from models.ConfigData import ConfigData
from models.const import *

from threading import Timer
from typing import Optional

_LOGGER = logging.getLogger(__name__)


class PiHoleDBManager:
    load_queue: queue.Queue
    config_data: ConfigData
    last_query_id: int
    total_queries: Optional[int]

    def __init__(self, config_data: ConfigData, load_queue: queue.Queue, query_id: Optional[int] = 0):
        self.load_queue = load_queue
        self.total_queries = None
        self.last_query_id = query_id
        self.config_data = config_data

        self._timer_update_counter: Optional[Timer] = None
        self._enrich_load_data: Optional[Timer] = None

        self._running = False

    def initialize(self):
        self._running = True

        self._timer_update_counter = Timer(1.0, self._update_counter_thread)
        self._timer_update_counter.start()

        self._enrich_load_data = Timer(1.0, self._enrich_data_thread)
        self._enrich_load_data.start()

    def terminate(self):
        self._running = False

        if self._timer_update_counter is not None:
            self._timer_update_counter.cancel()
            self._timer_update_counter = None

        if self._enrich_load_data is not None:
            self._enrich_load_data.cancel()
            self._enrich_load_data = None

    def _get_db_cursor(self):
        cursor = None

        try:
            _LOGGER.debug("Connecting to PiHole DB")

            connection = sqlite3.connect(self.config_data.pihole_db_path)
            cursor = connection.cursor()
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line = exc_tb.tb_lineno

            _LOGGER.error(f"Failed to connect to PiHole DB, Error: {ex}, Line: {line}")

        return cursor

    def _update_counter_thread(self):
        cursor = self._get_db_cursor()
        is_connected = cursor is not None

        if is_connected and self._running:
            self._update_counter(cursor)

            self._timer_update_counter = Timer(self.config_data.pihole_counter_cycle_interval,
                                               self._update_counter_thread)

            self._timer_update_counter.start()

    def _enrich_data_thread(self):
        cursor = self._get_db_cursor()
        is_connected = cursor is not None

        if is_connected and self._running:
            self._enrich_data(cursor)

            self._enrich_load_data = Timer(self.config_data.pihole_enrich_cycle_interval,
                                           self._enrich_data_thread)
            self._enrich_load_data.start()

    def _enrich_data(self, cursor):
        queries = None
        started = datetime.now()

        try:
            query_cmd = PIHOLE_LOAD_QUERY
            query_cmd = query_cmd.replace(PLACEHOLDER_QUERY_ID, str(self.last_query_id))
            query_cmd = query_cmd.replace(PLACEHOLDER_LIMIT, str(self.config_data.pihole_enrich_batch_size))

            queries = cursor.execute(query_cmd).fetchall()

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line = exc_tb.tb_lineno

            _LOGGER.error(f"Failed to enrich, Error: {ex}, Line: {line}")

        completed = get_total_seconds(started)

        timing = {
            "enriched": completed
        }

        if len(queries) > 0:
            self._transform(queries, timing)

    def _transform(self, queries: [], timing: dict):
        started = datetime.now()
        data_items = []

        for query in queries:
            data = self._transform_query(query)

            if data is None:
                break

            data_items.append(data)

        completed = get_total_seconds(started)

        timing["transform"] = completed

        if len(data_items) == len(queries):
            last_item = data_items[len(data_items) - 1]
            last_query_id = last_item.get("query_id")

            migration_data = {
                "count": self.total_queries,
                "items": data_items,
                "from": self.last_query_id,
                "to": last_query_id,
                "timing": timing
            }

            self.last_query_id = last_query_id

            self.load_queue.put(migration_data)

        else:
            _LOGGER.error(
                f"{len(queries):,.0f}/{len(queries):,.0f} queries transformed, "
                f"Duration: {completed:,.3f}"
            )

    @staticmethod
    def _transform_query(query):
        data = None

        try:
            data = {}
            key_id = 0

            for query_item in query:
                key = MYSQL_QUERIES_FIELDS_MAPPING.get(key_id, None)
                key_id += 1

                if key is not None:
                    key_type = key.get("type")
                    key_name = key.get("name")
                    data_item = query_item

                    if key_type == "timestamp":
                        data_item = to_date(query_item)

                    elif key_type == "int":
                        data_item = int(query_item)

                    data[key_name] = data_item

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line = exc_tb.tb_lineno

            _LOGGER.error(
                f"Failed to transform data, Query: {query}, Error: {ex}, Line: {line}"
            )

        return data

    def _update_counter(self, cursor):
        _LOGGER.debug("Loading PiHole metadata from SQLite")
        started = datetime.now()

        data = cursor.execute("SELECT COUNT(id) from queries").fetchall()

        for item in data:
            if item is not None and item[0] is not None:
                self.total_queries = item[0]

        completed = get_total_seconds(started)

        _LOGGER.info(
            f"{millify(self.total_queries, 3)} queries found in PiHole DB, "
            f"Duration: {completed:,.3f}"
        )
