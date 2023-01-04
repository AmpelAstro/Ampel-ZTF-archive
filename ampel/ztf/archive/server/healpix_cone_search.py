import numpy as np
from astropy import units as u
from astropy_healpix.high_level import healpix_cone_search


def ranges_for_cone(
    ra: float, dec: float, radius: float, nside: int = 64, order: str = "nested"
) -> list[tuple[int, int]]:
    """
    Get healpix pixels that lie within the given circle

    :returns: open intervals at the given nside
    """
    ipix = np.sort(
        healpix_cone_search(
            ra * u.deg, dec * u.deg, radius * u.deg, nside=nside, order=order
        )
    )
    changepoints = np.concatenate(
        [np.where(ipix != np.roll(ipix, 1) + 1)[0], [len(ipix)]]
    )
    return [
        (int(ipix[left]), int(ipix[right - 1]))
        for left, right in (zip(changepoints[:-1], changepoints[1:]))
    ]
