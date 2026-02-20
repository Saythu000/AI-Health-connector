from pydantic import BaseModel, ConfigDict
from typing import Optional, List

class AirflowConnectionSchema(BaseModel):
    """
    Validates and masks sensitive data fetched from Airflow.
    """
    # This tells Pydantic it's okay if we use names that might overlap with internal protected names.

    model_config = ConfigDict(protected_namespaces=(), extra="forbid")
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None  # Airflow DB name or ES Protocol
    type: Optional[str] = None    # SQLAlchemy dialect+driver
    verify_certs: Optional[bool] = None
    ca_certs: Optional[str] = None

    # Explicit Gmail/OAuth2 Fields (Found in your logs)
    refresh_token: Optional[str] = None
    token_uri: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    scopes: Optional[List[str]] = None
    universe_domain: Optional[str] = None
    account: Optional[str] = None
    expiry: Optional[str] = None

    region_name: Optional[str] = None


    """
1.Open Airflow UI

2.Go to Admin → Connections

3.Open the connection named:

   elasticsearch

4.Scroll to Extras (JSON)

5.Paste ONE of the following (choose based on your setup)
public:
 {
  "verify_certs": true
 }

InternalCA (enterprise):
 secure with internal CA :
 {
  "verify_certs": true,
  "ca_certs": "/etc/ssl/certs/internal-ca.pem"
}
 

 logs you need to see:

 Verify from logs (this is mandatory)
1 Trigger the DAG

In Airflow UI:

Go to DAGs

Trigger health_data_pipeline

Open logs for this task: load_to_es

Expected (SUCCESS case):

Attempting to connect to Elasticsearch at: https://<host>:<port>
Successfully connected to Elasticsearch.

Expected (FAILURE case — also OK):
If your CA path is wrong or missing, you will see:
SSLCertVerificationError
    """