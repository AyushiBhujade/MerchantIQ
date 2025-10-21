"""
Logging utilities for MerchantIQ system.
"""

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional
import time
import sys


# Configure logging format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('merchantiq.log')
        ]
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.
    
    Args:
        name: Logger name (typically module name)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(f"merchantiq.{name}")


@contextmanager
def AgentActionContext(agent_name: str, action: str, context_id: Optional[str] = None):
    """Context manager for tracking agent actions.
    
    Args:
        agent_name: Name of the agent performing the action
        action: Action being performed
        context_id: Optional context identifier
    """
    logger = get_logger(f"agent.{agent_name}")
    
    context_info = f"{action}"
    if context_id:
        context_info += f" [{context_id}]"
    
    logger.info(f"Starting {context_info}")
    start_time = time.time()
    
    try:
        yield
        duration = time.time() - start_time
        logger.info(f"Completed {context_info} in {duration:.2f}s")
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Failed {context_info} after {duration:.2f}s: {e}")
        raise


def performance_log(func):
    """Decorator for automatic performance logging.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        logger = get_logger("performance")
        func_name = func.__name__
        
        logger.debug(f"Starting {func_name}")
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"Completed {func_name} in {duration:.3f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Failed {func_name} after {duration:.3f}s: {e}")
            raise
    
    return wrapper


class PerformanceTimer:
    """Performance timer for detailed timing measurements."""
    
    def __init__(self, name: str):
        """Initialize timer.
        
        Args:
            name: Timer name for logging
        """
        self.name = name
        self.logger = get_logger("performance")
        self.start_time: Optional[float] = None
        self.checkpoints = []
    
    def start(self):
        """Start the timer."""
        self.start_time = time.time()
        self.logger.debug(f"Timer '{self.name}' started")
    
    def checkpoint(self, label: str):
        """Add a checkpoint.
        
        Args:
            label: Checkpoint label
        """
        if self.start_time is None:
            self.logger.warning(f"Timer '{self.name}' not started")
            return
        
        elapsed = time.time() - self.start_time
        self.checkpoints.append((label, elapsed))
        self.logger.debug(f"Timer '{self.name}' checkpoint '{label}': {elapsed:.3f}s")
    
    def stop(self):
        """Stop the timer and log results."""
        if self.start_time is None:
            self.logger.warning(f"Timer '{self.name}' not started")
            return
        
        duration = time.time() - self.start_time
        self.logger.info(f"Timer '{self.name}' completed in {duration:.3f}s")
        
        if self.checkpoints:
            self.logger.info(f"Timer '{self.name}' checkpoints:")
            for label, elapsed in self.checkpoints:
                self.logger.info(f"  {label}: {elapsed:.3f}s")
        
        return duration