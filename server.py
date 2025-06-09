"""Entry point for the Legacy ETL to Python MCP Server."""
import logging
import sys
from src.server import LegacyEtlMCPServer
from src.config import LegacyEtlMcpConfig

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def main() -> int:
    """Run the MCP server.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    setup_logging()
    logger.info("Starting Legacy ETL to Python MCP Server...")
    
    try:
        config = LegacyEtlMcpConfig.load_from_env()
        server = LegacyEtlMCPServer()
        
        if not server.initialize(config):
            logger.critical("Failed to initialize server")
            return 1
        
        logger.info("Server initialized successfully")
        server.run()
        return 0
        
    except ValueError as e:
        logger.critical(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
