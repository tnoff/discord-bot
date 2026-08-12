'''
Result of a broker checkout.

Lives in types/ rather than interfaces/broker_protocols.py because it is a plain
dataclass that both broker clients need, while that module imports the broker
ENGINE's dependencies (VideoCacheClient -> sqlalchemy, integrations.s3 -> boto3).
Keeping it here is what lets clients/http_broker_client.py stay importable in the
slim search image — same split, and same reason, as interfaces/result_queue.py.
It is re-exported from broker_protocols so existing imports keep working.
'''
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CheckoutResult:
    '''
    Result of a broker checkout operation.

    Exactly one of local_path or s3_key will be set. local_path means the file
    is already staged on local disk and ready to play. s3_key means the file
    lives in S3; bucket_name is set alongside it so the caller can download
    without needing separate S3 configuration.
    '''
    local_path: Path | None = None
    s3_key: str | None = None
    bucket_name: str | None = None
