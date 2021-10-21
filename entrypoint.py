import asyncio
import logging
from typing import Optional

from managers.MySQLDBManager import MySQLDBManager
from managers.PiHoleDBManager import PiHoleDBManager
from models.ConfigData import ConfigData
from models.exceptions import AbortedException

_LOGGER = logging.getLogger(__name__)


loop = asyncio.get_event_loop()
mysql_manager: Optional[MySQLDBManager] = None
pihole_manager: Optional[PiHoleDBManager] = None

try:

    config_data = ConfigData()

    mysql_manager = MySQLDBManager(config_data)
    mysql_manager.initialize()

    pihole_manager = PiHoleDBManager(config_data, mysql_manager.load_queue, mysql_manager.last_query_timestamp)
    pihole_manager.initialize()

    loop.run_forever()

except AbortedException:
    _LOGGER.debug("Migration aborted")

except KeyboardInterrupt:
    _LOGGER.debug("Migration cancelled")

finally:
    _LOGGER.debug("Completed")

    if mysql_manager is not None:
        mysql_manager.terminate()

    if pihole_manager is not None:
        pihole_manager.terminate()

    loop.close()

