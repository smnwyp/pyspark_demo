from enum import Enum

APP_NAME = "pyspark_demo"

Gaussian_loc = 0
Gaussian_scale = 1
Poisson_iam = 1.0

COL_NAME = "col"
HOME_ENV = "HOME"
UNIT_SIZE = 1000

ID_NAME = "id"
PRICE_NAME = "price"
SALES_NAME = "sales"

OUTPUT_PATH = "res"

DIST_INDEX = "Index"
DIST_GAUSS = "Gaussian"
DIST_POISSON = "Poisson"

OUTPUT_MERGE_KEY = "merge"

JOIN_TYPES = ["inner", "outer", "left", "right", "left_semi", "left_anti"]


class DistriType(Enum):
    Gaussian = 1
    Poisson = 2
    Index = 3