
# Making this available directly via esgfpid.check_pid_queue_availability
# instead of esgfpid.check.check_pid_queue_availability
from .check import check_pid_queue_availability

# Making this available directly via esgfpid.make_handle_from_drsid_and_versionnumber
# instead of esgfpid.utils.make_handle_from_drsid_and_versionnumber
from .utils import make_handle_from_drsid_and_versionnumber

# Making this available directly via esgfpid.Connector
# instead of esgfpid.connector.Connector
from .connector import Connector

