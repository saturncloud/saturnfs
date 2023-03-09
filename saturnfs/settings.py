import os

from saturnfs.utils import Units

SATURN_BASE_URL = os.environ["SATURN_BASE_URL"]
SATURN_TOKEN = os.environ["SATURN_TOKEN"]

S3_MIN_PART_SIZE = 5 * Units.MiB
S3_MAX_PART_SIZE = 5 * Units.GiB
S3_MAX_NUM_PARTS = 10000

SATURNFS_FILE_PREFIX = "sfs://"
