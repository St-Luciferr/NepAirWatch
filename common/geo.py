"""Location helpers: great-circle distance and nearest monitored city."""
import math

from common.config import CITIES

EARTH_KM = 6371.0


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_KM * math.asin(math.sqrt(a))


def nearest_city(lat, lon, max_km=None):
    """Return (city, province, distance_km) of the closest monitored city, or (None, None, None)."""
    best = min(((haversine_km(lat, lon, la, lo), c, p) for c, p, la, lo in CITIES), key=lambda x: x[0])
    if max_km is not None and best[0] > max_km:
        return None, None, None
    return best[1], best[2], round(best[0], 1)
