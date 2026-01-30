import logging
from rich.logging import RichHandler

_configured = False

def setup_logging(level: int = logging.WARNING):
    """Setup logging with Rich handler. Call once at startup."""
    global _configured
    if _configured:
        return
    
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
    )
    _configured = True

def get_logger(name: str):
    """Get a logger instance."""
    if not _configured:
        setup_logging()
    return logging.getLogger(name)