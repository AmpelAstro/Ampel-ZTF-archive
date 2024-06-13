from astropy import units as u
from astropy_healpix import pixel_resolution_to_nside
from astropy_healpix.high_level import healpix_cone_search

from .skymap import gen_ranges, multirange


def ranges_for_cone(
    ra: float, dec: float, radius: float, max_nside: int = 64, order: str = "nested"
) -> tuple[int, multirange[int]]:
    """
    Get healpix pixels that lie within the given circle

    :returns: half-open intervals at the given nside
    """
    # pick an nside that minimizes the number of ranges to search
    nside = min(max_nside, pixel_resolution_to_nside(radius * u.deg))
    ipix = healpix_cone_search(
        ra * u.deg, dec * u.deg, radius * u.deg, nside=nside, order=order
    )
    return nside, multirange(gen_ranges(ipix))
