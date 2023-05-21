
import logging

logging.basicConfig(level=logging.INFO)

botocorelogger = logging.getLogger('botocore').setLevel(logging.INFO)
botocore_auth = logging.getLogger('botocore.auth').setLevel(logging.INFO)
urlliblogger = logging.getLogger('urllib3').setLevel(logging.DEBUG)
