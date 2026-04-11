"""Political district lookups via live ArcGIS FeatureServer queries.

Fetches boundary polygons and elected official names from:
- Boston City Council (2023-2032): Boston ArcGIS
- Boston Police Districts: Boston ArcGIS
- MA State House Representatives (2021): MassGIS ArcGIS
- MA State Senate (2021): MassGIS ArcGIS

Boundaries are cached in S3 with a 30-day TTL. Elected names update
whenever the cache refreshes.
"""

import json
import logging
import urllib.request
from datetime import UTC, datetime
from typing import Any

from shapely.geometry import Point, mapping, shape
from shapely.prepared import prep

from pipeline import storage

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 90
CACHE_KEY = "districts/boundaries.json"
DISPLAY_KEY = "districts/boundaries_display.json"
ASSIGNMENTS_KEY = "districts/assignments.json"

# ArcGIS FeatureServer endpoints (query for all features as GeoJSON)
_ARCGIS_QUERY = "?where=1%3D1&outFields={fields}&f=geojson&outSR=4326"

ENDPOINTS: dict[str, dict[str, str | None]] = {
    "council": {
        "url": "https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/"
        "CityCouncilDistricts_2023_5_25/FeatureServer/0/query",
        "fields": "DISTRICT,Councilor",
        "id_field": "DISTRICT",
        "name_field": "Councilor",
    },
    "police": {
        "url": "https://gisportal.boston.gov/arcgis/rest/services/"
        "PublicSafety/OpenData/MapServer/5/query",
        "fields": "DISTRICT",
        "id_field": "DISTRICT",
        "name_field": None,
    },
    "state_rep": {
        "url": "https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/"
        "AGOL/House2021/FeatureServer/1/query",
        "fields": "REP_DIST,REP",
        "id_field": "REP_DIST",
        "name_field": "REP",
    },
    "state_senate": {
        "url": "https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/"
        "AGOL/Senate2021/FeatureServer/1/query",
        "fields": "SEN_DIST,SENATOR",
        "id_field": "SEN_DIST",
        "name_field": "SENATOR",
    },
}


def _last_name(full_name: str) -> str:
    """Extract last name from a full name like 'Michael F. Rush (D)'."""
    # Strip party suffix like "(D)" or "(R)"
    name = full_name.strip()
    if name.endswith(")"):
        paren = name.rfind("(")
        if paren > 0:
            name = name[:paren].strip()
    # Remove trailing comma if present (e.g. "Kennedy,")
    name = name.rstrip(",").strip()
    parts = name.split()
    if not parts:
        return full_name
    return parts[-1]


def _fetch_geojson(url: str, fields: str) -> dict[str, Any]:
    """Fetch GeoJSON from an ArcGIS FeatureServer endpoint."""
    full_url = url + _ARCGIS_QUERY.format(fields=fields)
    req = urllib.request.Request(full_url, headers={"User-Agent": "BostonHazardPipeline/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))  # type: ignore[no-any-return]


def _load_or_fetch_boundaries() -> dict[str, Any]:
    """Load cached boundaries from S3, or fetch fresh from ArcGIS."""
    cached = storage.read_json(CACHE_KEY)
    if cached:
        fetched_at = cached.get("fetched_at", "")
        if fetched_at:
            age = (datetime.now(tz=UTC) - datetime.fromisoformat(fetched_at)).days
            if age < CACHE_TTL_DAYS:
                logger.info("Using cached district boundaries (%d days old)", age)
                # Ensure display boundaries exist
                if not storage.read_json(DISPLAY_KEY):
                    _write_display_boundaries(cached)
                return cached  # type: ignore[no-any-return]

    logger.info("Fetching fresh district boundaries from ArcGIS...")
    result: dict[str, Any] = {"fetched_at": datetime.now(tz=UTC).isoformat()}

    for key, cfg in ENDPOINTS.items():
        try:
            url = cfg["url"]
            fields = cfg["fields"]
            id_field = cfg["id_field"]
            name_field = cfg["name_field"]
            assert url and fields and id_field  # noqa: S101
            geo = _fetch_geojson(url, fields)
            features = geo.get("features", [])
            logger.info("Fetched %d %s boundaries", len(features), key)
            result[key] = {
                "features": [
                    {
                        "geometry": f["geometry"],
                        "id": str(f["properties"].get(id_field, "")),
                        "official": str(f["properties"].get(name_field, "")) if name_field else "",
                    }
                    for f in features
                ],
            }
        except Exception:
            logger.warning("Failed to fetch %s boundaries", key, exc_info=True)
            # Use cached data for this layer if available
            if cached and key in cached:
                result[key] = cached[key]
            else:
                result[key] = {"features": []}

    try:
        storage.write_json(CACHE_KEY, result)
    except Exception:
        logger.debug("Could not cache boundaries to S3 (local dev?)")

    _write_display_boundaries(result)
    return result


def _simplify_geometry(geom_dict: dict[str, Any], tolerance: float = 0.002) -> dict[str, Any]:
    """Simplify a GeoJSON geometry for display purposes."""
    geom = shape(geom_dict)
    simplified = geom.simplify(tolerance, preserve_topology=True)
    result: dict[str, Any] = dict(mapping(simplified))
    return result


def _write_display_boundaries(data: dict[str, Any]) -> None:
    """Write simplified boundary GeoJSON for frontend map display."""
    display: dict[str, Any] = {}
    for key in ("council", "police", "state_rep", "state_senate"):
        features = data.get(key, {}).get("features", [])
        display[key] = [
            {
                "id": f["id"],
                "geometry": _simplify_geometry(f["geometry"]),
            }
            for f in features
            if f.get("geometry")
        ]
    try:
        storage.write_json(DISPLAY_KEY, display)
        size = len(json.dumps(display))
        logger.info("Wrote display boundaries: %d KB", size // 1024)
    except Exception:
        logger.debug("Could not write display boundaries to S3")


class DistrictLookup:
    """Point-in-polygon lookup for political districts.

    Caches assignments per lat/lng coordinate in S3 so lookups only
    happen once per unique location. Subsequent runs skip records
    already in the cache.
    """

    def __init__(self) -> None:
        data = _load_or_fetch_boundaries()

        # Build lookup tables: list of (prepared_polygon, district_id, official_name)
        self._layers: dict[str, list[tuple[Any, str, str]]] = {}
        # District label maps: district_id -> "District X - LastName"
        self._labels: dict[str, dict[str, str]] = {}

        for key in ("council", "police", "state_rep", "state_senate"):
            layer_data = data.get(key, {}).get("features", [])
            polys: list[tuple[Any, str, str]] = []
            labels: dict[str, str] = {}

            for feat in layer_data:
                try:
                    geom = shape(feat["geometry"])
                    prepared = prep(geom)
                    dist_id = feat["id"]
                    official = feat.get("official", "")
                    polys.append((prepared, dist_id, official))

                    # Build label
                    if official:
                        last = _last_name(official)
                        labels[dist_id] = f"{dist_id} - {last}"
                    else:
                        labels[dist_id] = dist_id
                except Exception:
                    continue

            self._layers[key] = polys
            self._labels[key] = labels
            logger.info("Loaded %d %s polygons", len(polys), key)

        # Load cached assignments: "lat,lng" -> {council, police, state_rep, state_senate}
        self._assignments: dict[str, dict[str, str]] = storage.read_json(ASSIGNMENTS_KEY) or {}
        self._dirty = False
        logger.info("Loaded %d cached district assignments", len(self._assignments))

    def _coord_key(self, lat: float, lng: float) -> str:
        """Create a cache key from coordinates (6 decimal places)."""
        return f"{lat:.6f},{lng:.6f}"

    def lookup(self, lat: float, lng: float) -> dict[str, str]:
        """Look up all district assignments for a point.

        Returns from cache if available, otherwise does point-in-polygon
        and caches the result.
        """
        key = self._coord_key(lat, lng)
        cached = self._assignments.get(key)
        if cached is not None:
            return cached

        # Point-in-polygon lookup
        pt = Point(lng, lat)
        result: dict[str, str] = {}
        for layer, polys in self._layers.items():
            result[layer] = ""
            for prepared, dist_id, _official in polys:
                if prepared.contains(pt):
                    result[layer] = dist_id
                    break

        self._assignments[key] = result
        self._dirty = True
        return result

    def save_cache(self) -> None:
        """Write the assignment cache to S3 if it changed."""
        if self._dirty:
            try:
                storage.write_json(ASSIGNMENTS_KEY, self._assignments)
                logger.info("Saved %d district assignments to cache", len(self._assignments))
            except Exception:
                logger.debug("Could not save assignment cache to S3 (local dev?)")
            self._dirty = False

    def label(self, layer: str, dist_id: str) -> str:
        """Get the display label for a district, e.g. '3 - Fernandes'."""
        return self._labels.get(layer, {}).get(dist_id, dist_id)

    def all_labels(self, layer: str) -> list[str]:
        """Get sorted list of all district labels for a layer."""
        return sorted(self._labels.get(layer, {}).values())
