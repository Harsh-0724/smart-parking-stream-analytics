"""Stream processor entry point: python -m processor.main"""

import os
import socket

from common.config import Settings
from common.logging import configure_logging
from processor.app import ProcessorApp


def main() -> None:
    instance = os.environ.get("HOSTNAME") or socket.gethostname()
    log = configure_logging("processor").bind(instance=instance)
    ProcessorApp(Settings.from_env(), instance, log).run()


if __name__ == "__main__":
    main()
