import bisect
import math
from collections import defaultdict
from collections.abc import Generator, Iterable
from typing import Generic, TypeVar

T = TypeVar("T", bound=int)


def deres(nside, ipix, min_nside=1):
    """
    Decompose a set of (nested) HEALpix indices into sets of complete superpixels at lower resolutions.

    :param nside: nside of given indices
    :param ipix: pixel indices
    :min_nside: minimum nside of complete pixels
    """
    remaining_pixels = set(ipix)
    decomposed = defaultdict(list)
    for log2_nside in range(int(math.log2(min_nside)), int(math.log2(nside)) + 1):
        super_nside = 2**log2_nside
        # number of base_nside pixels per nside superpixel
        scale = (nside // super_nside) ** 2
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


class multirange(Generic[T]):
    """
    An ordered collection of non-overlapping, half-open intervals.
    """

    def __init__(self, intervals: Iterable[tuple[T, T]] = []):
        if intervals:
            self.lefts, self.rights = (list(side) for side in zip(*intervals))
        else:
            self.lefts = []
            self.rights = []

    def __len__(self) -> int:
        return len(self.lefts)

    def __iter__(self):
        return zip(self.lefts, self.rights)

    def add(self, left: T, right: T):
        assert right > left
        # index of the first interval completely to the left (rights[i] < left)
        i = bisect.bisect_left(self.rights, left)
        # index of the first interval completely to the right (lefts[j] > right)
        j = bisect.bisect_right(self.lefts, right)
        assert j >= i
        if i != j:
            if i < len(self.lefts):
                left = min((left, self.lefts[i]))
            if j > 0:
                right = max((right, self.rights[j - 1]))
            del self.lefts[i:j]
            del self.rights[i:j]
        self.lefts.insert(i, left)
        self.rights.insert(i, right)


def gen_ranges(items: Iterable[int]) -> Generator[tuple[int, int], None, None]:
    """Convert to a sequence of half-open intervals"""
    ordered = sorted(items)
    i = 0
    for j in range(1, len(ordered)):
        if ordered[j] > ordered[j - 1] + 1:
            yield ordered[i], ordered[j - 1] + 1
            i = j
    yield ordered[i], ordered[-1] + 1


def pixel_map_to_range(pixels: dict[int, list[int]]) -> tuple[int, multirange[int]]:
    """
    Express a multi-resolution map as a multirange at the highest nside

    inspired by https://arxiv.org/abs/2112.06947
    """
    ranges: multirange[int] = multirange()
    target_nside = max(pixels.keys())
    for nside, ipix in pixels.items():
        scale = (target_nside // nside) ** 2
        for left, right in gen_ranges(ipix):
            ranges.add(left * scale, right * scale)
    return target_nside, ranges
