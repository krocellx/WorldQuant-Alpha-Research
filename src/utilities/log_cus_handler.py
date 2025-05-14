import logging
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import os
import queue
from pathlib import Path
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
LOGS_DIR = os.path.join(ROOT_DIR, "logs")


class CusQueueHandler(logging.Handler):
    """
    Custom queue-based logging handler that uses a background thread for file I/O,
    eliminating Windows file locking issues.
    """

    def __init__(self, file_name_prefix, mode='a', maxBytes=10 * 1024 * 1024, backupCount=5):
        """
        Initialize the queue handler with a background listener.

        Args:
            file_name_prefix: Prefix for the log file name
            mode: Mode in which to open the log file
            maxBytes: Maximum size of log file before rotation
            backupCount: Number of backup files to keep
        """
        super().__init__()

        # Create log directory
        path = LOGS_DIR
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print(f"Error creating log directory {path}: {e}")
            path = os.path.join(os.getcwd(), "logs")
            print(f"Using fallback log directory: {path}")
            Path(path).mkdir(parents=True, exist_ok=True)

        # Set up file path
        now = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S-%f')
        if len(file_name_prefix) > 0:
            file_name = os.path.join(path, file_name_prefix + now + ".log")
        else:
            file_name = os.path.join(path, f"{now}.log")

        # Create queue and handlers
        self.log_queue = queue.Queue(-1)  # No limit on queue size
        self.queue_handler = QueueHandler(self.log_queue)

        # Create the actual file handler that will run in background
        self.file_handler = RotatingFileHandler(
            filename=file_name,
            mode=mode,
            maxBytes=maxBytes,
            backupCount=backupCount
        )

        # Set the same formatter for both handlers
        self.queue_handler.setFormatter(self.formatter)
        self.file_handler.setFormatter(self.formatter)

        # Create and start the listener
        self.listener = QueueListener(
            self.log_queue,
            self.file_handler,
            respect_handler_level=True
        )
        self.listener.start()

    def emit(self, record):
        """Pass the record to the queue handler."""
        self.queue_handler.emit(record)

    def close(self):
        """Stop the listener and close handlers."""
        if hasattr(self, 'listener') and self.listener is not None:
            try:
                self.listener.stop()
            except Exception:
                # Ignore errors during shutdown
                pass
        if hasattr(self, 'file_handler'):
            self.file_handler.close()
        super().close()

    def setFormatter(self, formatter):
        """Set formatter for both handlers."""
        super().setFormatter(formatter)
        if hasattr(self, 'queue_handler'):
            self.queue_handler.setFormatter(formatter)
        if hasattr(self, 'file_handler'):
            self.file_handler.setFormatter(formatter)

if __name__ == "__main__":
    pass
