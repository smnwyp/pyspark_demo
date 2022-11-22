from enum import Enum

Gaussian_loc = 0
Gaussian_scale = 1
Poisson_iam = 1.0


class DistriType(Enum):
    gaussian = 1
    poisson = 2
    index = 3