"""
Logging configuration for ETL pipeline.
"""

import logging


def setup_logger(name: str) -> logging.Logger:
    """
    Set up logger with consistent format.

    Args:
        name: Logger name (e.g., 'load_dimensions', 'load_facts')

    Returns:
        Configured logger instance
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    return logging.getLogger(name)
