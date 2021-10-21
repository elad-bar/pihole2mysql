import logging
import queue
import sys

import mysql.connector

from datetime import datetime

from managers import get_total_seconds, millify
from models.ConfigData import ConfigData
from models.const import *
from models.exceptions import AbortedException

from threading import Timer
from typing import Optional

_LOGGER = logging.getLogger(__name__)


class MySQLDBManager:
    config_data: ConfigData
    last_query_timestamp: Optional[int]
    total_queries: Optional[int]
    load_queue: queue.Queue

    def __init__(self, config_data: ConfigData):
        self.load_queue = queue.Queue()
        self.total_queries = None
        self.last_query_timestamp = None
        self.config_data = config_data

        self._insert_command = self._get_insert_command()

        self._connection = None
        self._cursor = None

        self._running = False

        self._timer_load: Optional[Timer] = None

    def initialize(self):
        self._running = True

        self._connect()
        self._update_initial_statistics()

        self._timer_load = Timer(1.0, self._load_data_thread)
        self._timer_load.start()

    def terminate(self):
        self._running = False

        if self._timer_load is not None:
            self._timer_load.cancel()
            self._timer_load = None

        self.load_queue.put({})

    def _connect(self):
        try:
            _LOGGER.debug("Connecting to MySQL")

            if self._connection is not None:
                self._connection.close()

            self._connection = mysql.connector.connect(
                user=self.config_data.mysql_username,
                password=self.config_data.mysql_password,
                host=self.config_data.mysql_host,
                database=self.config_data.mysql_database)

            self._cursor = self._connection.cursor()
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line = exc_tb.tb_lineno

            _LOGGER.error(f"Failed to connect to databases, Error: {ex}, Line: {line}")

    def _load_data_thread(self):
        if self._running:
            item = self.load_queue.get()

            items = item.get("items", [])
            items_count = 0 if items is None else len(items)
            count = item.get("count", 0)
            timing = item.get("timing", {})

            if items_count > 0 and self._running:
                self._load_data(items)

                self._update_statistics(count, timing, items_count)

                self.load_queue.task_done()

            self._timer_load_data = Timer(0, self._load_data_thread)
            self._timer_load_data.start()

    def _load_data(self, items):
        started = datetime.now()
        count = 0 if items is None else len(items)

        try:
            if self._running and count > 0:
                self._cursor.executemany(self._insert_command, items)

                self._connection.commit()

        except Exception as ex:
            if self._connection is not None:
                self._connection.rollback()

            exc_type, exc_obj, exc_tb = sys.exc_info()
            line = exc_tb.tb_lineno

            _LOGGER.error(f"Failed to load data, Error: {ex}, Line: {line}")

            raise AbortedException()

        completed = get_total_seconds(started)

        return completed

    def _update_statistics_from_db(self):
        cursor = self._connection.cursor()

        select_last_query_command = SQL_MIGRATION_TABLE_MAX_QUERY_ID
        select_last_query_command = select_last_query_command.replace(PLACEHOLDER_TABLE, self.config_data.mysql_table)

        cursor.execute(select_last_query_command)

        for item in cursor:
            if item is not None and item[0] is not None:
                self.last_query_timestamp = item[0].timestamp()

        select_count_command = SQL_MIGRATION_TABLE_COUNT
        select_count_command = select_count_command.replace(PLACEHOLDER_TABLE, self.config_data.mysql_table)

        cursor.execute(select_count_command)
        for item in cursor:
            if item is not None and item[0] is not None:
                self.total_queries = item[0]

    def _update_initial_statistics(self):
        if self._running:
            started = datetime.now()

            self._update_statistics_from_db()

            completed = get_total_seconds(started)

            timing = {
                "stats": completed
            }

            timing_arr = []
            for timing_item in timing:
                timing_arr.append(f"{timing_item}={timing[timing_item]:.3f}")

            timing_str = " / ".join(timing_arr)

            _LOGGER.info(
                f"Database contains {millify(self.total_queries, 3)} queries, "
                f"Duration: {timing_str}"
            )

    def _update_statistics(self,
                           count: int,
                           timing: dict,
                           migrated: int):

        if self._running:
            started = datetime.now()

            self._update_statistics_from_db()

            completed = get_total_seconds(started)

            timing["stats"] = completed

            timing_batch = 0
            for key in timing:
                current_timing = timing.get(key)
                timing_batch += current_timing

            timing["batch"] = timing_batch

            batch_rate = migrated / timing_batch

            timing_arr = []
            for timing_item in timing:
                timing_arr.append(f"{timing_item}={timing[timing_item]:.3f}")

            timing_str = " / ".join(timing_arr)

            progress_str = ""

            if count is not None and count > 0:
                progress = self.total_queries / count

                progress_str = f" - {progress:.3%} [{millify(self.total_queries, 3)}/{millify(count, 3)}]"

            operation = "back-filled" if self.config_data.is_back_filling else "migrated"

            _LOGGER.info(
                f"{millify(migrated)} queries {operation} at {millify(batch_rate, 3)}/s{progress_str}, "
                f"Duration: {timing_str}"
            )

    def _get_insert_command(self):
        fields = []
        values = []
        for key in MYSQL_QUERIES_FIELDS_MAPPING:
            item = MYSQL_QUERIES_FIELDS_MAPPING[key]

            name = item.get("name")

            fields.append(name)
            values.append(f"%({name})s")

        columns_str = ", ".join(fields)
        values_str = ", ".join(values)

        placeholders = {
            INSERT_COLUMNS: columns_str,
            INSERT_VALUES: values_str,
            PLACEHOLDER_TABLE: self.config_data.mysql_table
        }

        insert_command = SQL_COMMAND_MIGRATE

        for placeholder in placeholders:
            insert_command = insert_command.replace(placeholder, placeholders.get(placeholder))

        return insert_command
