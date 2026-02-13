import logging
from opensearchpy import OpenSearch
from .schemas import OpensearchConfig

logger = logging.getLogger(__name__)

"""
opensearch.py
====================================
Purpose:
    Provides a connector for OpenSearch clusters, handling URL building 
    and connection health checks.
"""

class OpensearchConnector:
    """
    Purpose:
        A wrapper for the OpenSearch client to manage connection settings 
        and cluster pings.
    """

    def __init__(self, config: dict):
        """
        Purpose:
            Initializes the OpensearchConnector with cluster details.

        Args:
            config (dict): Contains 'schema', 'host', 'port', and 'verify_certs'.
        """
        self.config = OpensearchConfig(**config)
        self._client = None

    def __call__(self):
        """
        Purpose:
            Ensures connection and returns the OpenSearch client instance.

        Returns:
            opensearchpy.OpenSearch: The active OpenSearch client.
        """
        self.connect()
        return self._client

    def connect(self):
        """
        Purpose:
            Initializes the OpenSearch client and verifies the connection via ping.

        Args:
            None

        Returns:
            None

        Raises:
            ConnectionError: If the OpenSearch cluster is unreachable.
        """
        protocol = self.config.schema_type
        host = self.config.host
        port = self.config.port
        opensearch_host = f"{protocol}://{host}:{port}"

        #Fali-fast TLS validation
        if self.config.verify_certs and protocol != "https":
            raise ValueError(
                "Invalid OpenSearch configuration: "
                "verify_certs=True requires schema_type='https'."
            )

        
        #Security posture logging
        if self.config.verify_certs:
            if self.config.ca_certs:
                logger.info(
                    "OpenSearch TLS verification ENABLED (custom CA): %s",self.config.ca_certs
                )
            else:
                logger.info(
                    "OpenSearch TLS verification ENABLED (system CA trust store)"
                )
        else:
            logger.warning(
                "OpenSearch TLS verification DISABLED (insecure, dev-only)"
            )
    
        
        logger.info(f"Attempting to connect to OpenSearch at {opensearch_host}")

        client_kwargs = {
           "hosts": [opensearch_host],
            "verify_certs": self.config.verify_certs
        }

        #adding CA cetificates only when verification is enabled
        if self.config.verify_certs and self.config.ca_certs:
            client_kwargs["ca_certs"] = self.config.ca_certs
        
        self.client = OpenSearch(**clinet_kwargs)

        if not self._client.ping():
            error_msg = f"Could not connect to OpenSearch at {opensearch_host}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        
        logger.info("OpenSearch connection verified.")