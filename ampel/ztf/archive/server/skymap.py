import math
from collections import defaultdict

def deres(nside, ipix, min_nside=1):
    """
    Decompose a set of (nested) HEALpix indices into sets of complete superpixels at lower resolutions.
    
    :param nside: nside of given indices
    :param ipix: pixel indices
    :min_nside: minimum nside of complete pixels
    """
    remaining_pixels = set(ipix)
    decomposed = defaultdict(list)
    for log2_nside in range(int(math.log2(min_nside)), int(math.log2(nside))+1):
        super_nside = 2**log2_nside
        # number of base_nside pixels per nside superpixel
        scale = (nside // super_nside)**2
        # sort remaining base_nside pixels by superpixel
        by_superpixel = defaultdict(list)
        for pix in remaining_pixels:
            by_superpixel[pix // scale].append(pix)
        # represent sets of pixels that fill a superpixel
        # as a single superpixel, and remove from the working set
        for superpix, members in by_superpixel.items():
            if len(members) == scale:
                decomposed[super_nside].append(superpix)
                remaining_pixels.difference_update(members)
                
    return dict(decomposed)