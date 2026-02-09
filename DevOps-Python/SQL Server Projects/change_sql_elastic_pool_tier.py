#!/usr/bin/env python3
import argparse
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

from azure.core.exceptions import HttpResponseError
from azure.identity import AzureCliCredential, DefaultAzureCredential, ChainedTokenCredential
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.sql.models import ElasticPoolPerDatabaseSettings, ElasticPoolUpdate, Sku


# DTU tiers -> SKU names (DTU elastic pools)
DTU_TIER_TO_SKU_NAME = {
    "basic": "BasicPool",
    "standard": "StandardPool",
    "premium": "PremiumPool",
}

# Canonical casing for tier strings (API expects these forms)
DTU_TIER_CANON = {
    "basic": "Basic",
    "standard": "Standard",
    "premium": "Premium",
}


# ----------------------------
# Auth
# ----------------------------
def get_credential(prefer_az_cli: bool):
    """
    - prefer_az_cli=True: use Azure CLI token first (good for AzureCLI@2 tasks)
    - otherwise: use DefaultAzureCredential first (good for service principal / federated creds in CI)
    """
    cli = AzureCliCredential()
    dac = DefaultAzureCredential(exclude_managed_identity_credential=True)

    if prefer_az_cli:
        return ChainedTokenCredential(cli, dac)
    return ChainedTokenCredential(dac, cli)


# ----------------------------
# Generic helpers (object/dict safe)
# ----------------------------
def normalize(s: str) -> str:
    return str(s).strip().lower()


def as_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    try:
        return list(x)
    except TypeError:
        return [x]


def get_any(obj: Any, *names: str, default=None):
    """
    Best-effort getter for SDK models where casing can vary.
    Tries:
      - getattr(obj, name)
      - obj[name] if dict-like
      - obj.as_dict()[name] if available
      - obj.__dict__[name]
    """
    if obj is None:
        return default

    # Direct attribute / dict access
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
        if isinstance(obj, dict) and n in obj:
            v = obj[n]
            if v is not None:
                return v

    # If SDK model supports as_dict(), try keys there
    if hasattr(obj, "as_dict"):
        try:
            d = obj.as_dict()
            for n in names:
                if n in d and d[n] is not None:
                    return d[n]
        except Exception:
            pass

    # Last chance: raw __dict__
    try:
        raw = getattr(obj, "__dict__", {})
        for n in names:
            if n in raw and raw[n] is not None:
                return raw[n]
    except Exception:
        pass

    return default


def to_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def parse_size_to_bytes(s: str) -> int:
    """
    Accepts: "50GB", "500MB", "1024" (bytes).
    Uses binary units: 1GB = 1024^3.
    """
    s = s.strip()
    if re.fullmatch(r"\d+", s):
        return int(s)
    m = re.fullmatch(r"(\d+)\s*(kb|mb|gb|tb)", s, re.IGNORECASE)
    if not m:
        raise ValueError(f"Invalid size format: {s!r} (use e.g. 5GB, 500MB, 5368709120)")
    val = int(m.group(1))
    unit = m.group(2).lower()
    mult = {"kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4}[unit]
    return val * mult


def bytes_to_human(b: Optional[int]) -> str:
    if b is None:
        return "-"
    gb = b / (1024**3)
    if gb >= 1024:
        return f"{gb/1024:.1f}TB"
    return f"{gb:.1f}GB"


# ----------------------------
# Capabilities parsing (robust)
# ----------------------------
def get_location_caps(client: SqlManagementClient, location: str):
    return client.capabilities.list_by_location(location, include="supportedElasticPoolEditions")


def iter_supported_elastic_pool_editions(caps: Any) -> Iterable[Any]:
    """
    Capabilities response shapes vary.
    Sometimes editions are on the root, but commonly they live under supportedServerVersions[*].
    This yields editions from both shapes (dedup by edition name is handled by caller if needed).
    """
    # Root-level (some shapes)
    root_eds = get_any(caps, "supported_elastic_pool_editions", "supportedElasticPoolEditions", default=None)
    for ed in as_list(root_eds):
        yield ed

    # Under server versions (common)
    svs = get_any(caps, "supported_server_versions", "supportedServerVersions", default=None)
    for sv in as_list(svs):
        eds = get_any(sv, "supported_elastic_pool_editions", "supportedElasticPoolEditions", default=None)
        for ed in as_list(eds):
            yield ed


def iter_elastic_pool_perf_levels(edition_cap: Any) -> Iterable[Any]:
    perf = get_any(
        edition_cap,
        "supported_elastic_pool_performance_levels",
        "supportedElasticPoolPerformanceLevels",
        default=None,
    )
    if perf:
        return as_list(perf)

    # Older shape (rare)
    dtus = get_any(edition_cap, "supported_elastic_pool_dtus", "supportedElasticPoolDtus", default=None)
    return as_list(dtus)


def max_size_capability_to_bytes(ms: Any) -> Optional[int]:
    """
    Converts a MaxSizeCapability-like object (limit + unit) to bytes.
    Unit values often include: Megabytes, Gigabytes, Terabytes, Petabytes.
    """
    if ms is None:
        return None
    limit = get_any(ms, "limit", default=None)
    unit = get_any(ms, "unit", default=None)
    if limit is None:
        return None

    limit_f = to_float(limit, None)
    if limit_f is None:
        return None

    unit_s = str(unit).strip().lower()
    mult = {
        "megabytes": 1024**2,
        "gigabytes": 1024**3,
        "terabytes": 1024**4,
        "petabytes": 1024**5,
        "mb": 1024**2,
        "gb": 1024**3,
        "tb": 1024**4,
        "pb": 1024**5,
        "bytes": 1,
        "byte": 1,
    }
    m = mult.get(unit_s)
    if m is None:
        # Unknown unit; best-effort treat as bytes
        return int(limit_f)
    return int(limit_f * m)


def get_supported_pool_max_bytes(perf: Any) -> Optional[int]:
    """
    Reads supported max pool size from perf.supported_max_sizes (ranges),
    falling back to perf.included_max_size if available.
    """
    max_bytes = None

    ranges = get_any(perf, "supported_max_sizes", "supportedMaxSizes", default=None)
    for r in as_list(ranges):
        max_v = get_any(r, "max_value", "maxValue", default=None)
        b = max_size_capability_to_bytes(max_v)
        if b is not None:
            max_bytes = b if max_bytes is None else max(max_bytes, b)

    inc = get_any(perf, "included_max_size", "includedMaxSize", default=None)
    inc_b = max_size_capability_to_bytes(inc)
    if inc_b is not None:
        max_bytes = inc_b if max_bytes is None else max(max_bytes, inc_b)

    return max_bytes


def get_perf_level_dtu(perf: Any) -> Optional[int]:
    """
    perf.performance_level.value is common, sometimes perf.dtu is present.
    """
    pl = get_any(perf, "performance_level", "performanceLevel", default=None)
    val = get_any(pl, "value", default=None)
    if val is None:
        val = get_any(perf, "dtu", "DTU", default=None)
    return to_int(val, None)


def get_db_max_limits(perf: Any) -> List[float]:
    caps = get_any(
        perf,
        "supported_per_database_max_performance_levels",
        "supportedPerDatabaseMaxPerformanceLevels",
        default=None,
    )
    out: List[float] = []
    for c in as_list(caps):
        lim = get_any(c, "limit", default=None)
        f = to_float(lim, None)
        if f is not None:
            out.append(f)
    return sorted(set(out))


def get_db_min_limits_for_max(perf: Any, chosen_db_max: float) -> List[float]:
    """
    Many capability shapes store min-levels nested under each max-level capability.
    If we can't find it, we assume [0.0] as a safe default for DTU pools.
    """
    max_caps = get_any(
        perf,
        "supported_per_database_max_performance_levels",
        "supportedPerDatabaseMaxPerformanceLevels",
        default=None,
    )

    best = None
    best_diff = None
    for c in as_list(max_caps):
        lim = to_float(get_any(c, "limit", default=None), None)
        if lim is None:
            continue
        diff = abs(lim - chosen_db_max)
        if best is None or best_diff is None or diff < best_diff:
            best = c
            best_diff = diff

    if best is None:
        return [0.0]

    mins = get_any(
        best,
        "supported_per_database_min_performance_levels",
        "supportedPerDatabaseMinPerformanceLevels",
        default=None,
    )
    out: List[float] = []
    for m in as_list(mins):
        lim = to_float(get_any(m, "limit", default=None), None)
        if lim is not None:
            out.append(lim)

    return sorted(set(out)) if out else [0.0]


def choose_closest_leq(values: List[float], want: float) -> float:
    leq = [v for v in values if v <= want]
    return max(leq) if leq else min(values)


def choose_closest_leq_int(values: List[int], want: int) -> int:
    leq = [v for v in values if v <= want]
    return max(leq) if leq else min(values)


def list_options_for_location(client: SqlManagementClient, location: str) -> List[Dict[str, Any]]:
    caps = get_location_caps(client, location)

    rows: List[Dict[str, Any]] = []
    seen = set()

    for ed in iter_supported_elastic_pool_editions(caps):
        ed_name = get_any(ed, "name", default=None)
        if not ed_name:
            continue

        tier_n = normalize(ed_name)
        if tier_n not in DTU_TIER_TO_SKU_NAME:
            continue  # only DTU tiers

        sku_name = DTU_TIER_TO_SKU_NAME[tier_n]

        for perf in iter_elastic_pool_perf_levels(ed):
            dtu = get_perf_level_dtu(perf)
            if dtu is None:
                continue

            db_max_limits = get_db_max_limits(perf)
            pool_max_bytes = get_supported_pool_max_bytes(perf)

            key = (tier_n, int(dtu), sku_name, pool_max_bytes, tuple(db_max_limits))
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "tier": DTU_TIER_CANON[tier_n],
                    "dtu": int(dtu),
                    "sku_name": sku_name,
                    "db_max_values": db_max_limits,
                    "pool_max_bytes": pool_max_bytes,
                    "perf_obj": perf,
                }
            )

    return rows


def find_perf_for_tier_and_dtu(
    client: SqlManagementClient, location: str, target_tier_n: str, want_dtu: int
) -> Tuple[Any, List[int]]:
    """
    Returns (perf_obj, available_dtus_for_tier).
    Raises ValueError if tier not found; KeyError if DTU not found.
    """
    rows = list_options_for_location(client, location)
    tier_rows = [r for r in rows if normalize(r["tier"]) == target_tier_n]
    if not tier_rows:
        raise ValueError(f"No DTU elastic pool options found for tier '{target_tier_n}' in location '{location}'.")

    avail_dtus = sorted(set(int(r["dtu"]) for r in tier_rows))
    if want_dtu not in avail_dtus:
        raise KeyError(f"DTU {want_dtu} not available for tier '{target_tier_n}'. Available: {avail_dtus}")

    perf_obj = next(r["perf_obj"] for r in tier_rows if int(r["dtu"]) == want_dtu)
    return perf_obj, avail_dtus


# ----------------------------
# CLI args
# ----------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Scale an Azure SQL DTU Elastic Pool by changing tier/SKU (Basic/Standard/Premium), "
            "DTUs, per-DB min/max, and pool max size.\n"
            "Capabilities API is used for --list-options and --auto-adjust; "
            "if capabilities lookup fails during an update, the script will proceed without validation."
        )
    )

    p.add_argument("--subscription-id", default=os.getenv("AZURE_SUBSCRIPTION_ID"))
    p.add_argument("--resource-group", required=True)
    p.add_argument("--server-name", required=True, help="Logical SQL server name (not the FQDN).")
    p.add_argument("--pool-name", required=True)

    p.add_argument("--target-tier", help="Target DTU tier: Basic, Standard, Premium (case-insensitive).")
    p.add_argument("--pool-dtu", type=int, default=None, help="Target pool eDTUs. If omitted, keeps current.")
    p.add_argument("--db-min-dtu", type=float, default=None, help="Target per-DB min DTU. If omitted, keeps current.")
    p.add_argument("--db-max-dtu", type=float, default=None, help="Target per-DB max DTU. If omitted, keeps current.")
    p.add_argument("--pool-max-size", default=None, help="Target pool max size: e.g. 5GB, 50GB, or bytes.")

    p.add_argument(
        "--auto-adjust",
        action="store_true",
        help="If set, clamps/adjusts DTU + per-DB values + pool size to a valid combination (requires capabilities).",
    )
    p.add_argument(
        "--list-options",
        action="store_true",
        help="List available DTU Elastic Pool options (Basic/Standard/Premium) for this server's region and exit.",
    )
    p.add_argument("--dry-run", action="store_true", help="Show changes without applying.")
    p.add_argument("--no-wait", action="store_true", help="Start update but don't wait.")
    p.add_argument("--prefer-az-cli", action="store_true", help="Prefer Azure CLI auth context (AzureCliCredential).")

    return p.parse_args()


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    args = parse_args()

    if not args.subscription_id:
        print("ERROR: Provide --subscription-id or set AZURE_SUBSCRIPTION_ID.", file=sys.stderr)
        return 2

    cred = get_credential(prefer_az_cli=args.prefer_az_cli)
    client = SqlManagementClient(credential=cred, subscription_id=args.subscription_id)

    # Fetch pool (and location)
    try:
        pool = client.elastic_pools.get(args.resource_group, args.server_name, args.pool_name)
    except HttpResponseError as e:
        print(f"ERROR: Failed to read elastic pool.\n{e}", file=sys.stderr)
        return 1

    location = get_any(pool, "location", default=None)

    sku = get_any(pool, "sku", default=None)
    cur_sku_name = get_any(sku, "name", default=None)
    cur_tier = get_any(sku, "tier", default=None)
    cur_cap = to_int(get_any(sku, "capacity", default=None), None)

    cur_max = to_int(get_any(pool, "max_size_bytes", "maxSizeBytes", default=None), None)

    cur_pds = get_any(pool, "per_database_settings", "perDatabaseSettings", default=None)
    cur_db_min = to_float(get_any(cur_pds, "min_capacity", "minCapacity", default=None), None)
    cur_db_max = to_float(get_any(cur_pds, "max_capacity", "maxCapacity", default=None), None)

    print(f"Server: {args.server_name} (location: {location})")
    print(f"Elastic Pool: {args.pool_name}")
    print(f"Current SKU: name={cur_sku_name!r}, tier={cur_tier!r}, capacity={cur_cap}")
    print(f"Current per-db: min={cur_db_min}, max={cur_db_max}")
    print(f"Current pool max_size_bytes: {cur_max} ({bytes_to_human(cur_max)})")

    # Rough vCore detection
    if isinstance(cur_sku_name, str) and (
        cur_sku_name.upper().startswith(("GP_", "BC_", "HS_")) or "_GEN" in cur_sku_name.upper()
    ):
        print(
            "ERROR: This looks like a vCore-based elastic pool (GP/BC/HS). "
            "This script is for DTU pools (Basic/Standard/Premium).",
            file=sys.stderr,
        )
        return 1

    if not location:
        print("ERROR: Pool location not found; cannot list/auto-adjust options.", file=sys.stderr)
        if args.list_options or args.auto_adjust:
            return 1

    # List options
    if args.list_options:
        try:
            rows = list_options_for_location(client, location)
        except Exception as e:
            print(f"ERROR: Failed to list options via capabilities API: {e}", file=sys.stderr)
            return 1

        if not rows:
            print("No DTU elastic pool options returned for this region.")
            return 1

        print("\nAvailable DTU Elastic Pool options in this region:")
        print("  TIER      | DTU | SKU_NAME      | DB_MAX_VALUES      | MAX_POOL_SIZE")
        print("  --------- | --- | ------------- | ------------------ | ------------")
        for r in sorted(rows, key=lambda x: (normalize(x["tier"]), x["dtu"])):
            dbvals = r["db_max_values"]
            dbdisp = ", ".join(str(int(v)) if v.is_integer() else str(v) for v in dbvals) if dbvals else "-"
            print(
                f"  {r['tier']:9} | {r['dtu']:3} | {r['sku_name']:13} | "
                f"{dbdisp:18} | {bytes_to_human(r['pool_max_bytes']):12}"
            )
        return 0

    # Require tier for updates
    if not args.target_tier:
        print("ERROR: Provide --target-tier (or use --list-options).", file=sys.stderr)
        return 2

    target_tier_n = normalize(args.target_tier)
    if target_tier_n not in DTU_TIER_TO_SKU_NAME:
        print("ERROR: target tier must be one of: Basic, Standard, Premium (DTU elastic pools).", file=sys.stderr)
        return 2

    target_tier = DTU_TIER_CANON[target_tier_n]
    target_sku_name = DTU_TIER_TO_SKU_NAME[target_tier_n]

    # Defaults: keep current unless overridden
    desired_pool_dtu = args.pool_dtu if args.pool_dtu is not None else (cur_cap if cur_cap is not None else 50)
    desired_db_min = args.db_min_dtu if args.db_min_dtu is not None else (cur_db_min if cur_db_min is not None else 0.0)
    desired_db_max = args.db_max_dtu if args.db_max_dtu is not None else (cur_db_max if cur_db_max is not None else 5.0)

    desired_pool_max_bytes = cur_max
    if args.pool_max_size:
        desired_pool_max_bytes = parse_size_to_bytes(args.pool_max_size)

    # Capabilities-based validation/auto-adjust (optional)
    perf = None
    avail_dtus: List[int] = []
    caps_error: Optional[Exception] = None

    if location:
        try:
            perf, avail_dtus = find_perf_for_tier_and_dtu(client, location, target_tier_n, int(desired_pool_dtu))
        except KeyError as e:
            if args.auto_adjust and location:
                # Pick closest <=
                try:
                    # get list of dtus for tier
                    rows = list_options_for_location(client, location)
                    tier_dtus = sorted(set(int(r["dtu"]) for r in rows if normalize(r["tier"]) == target_tier_n))
                    if tier_dtus:
                        new_dtu = choose_closest_leq_int(tier_dtus, int(desired_pool_dtu))
                        print(f"Requested DTU {desired_pool_dtu} not available for {target_tier}. Auto-adjusting -> {new_dtu}.")
                        desired_pool_dtu = int(new_dtu)
                        perf, avail_dtus = find_perf_for_tier_and_dtu(client, location, target_tier_n, int(desired_pool_dtu))
                    else:
                        raise ValueError("No DTU values found for this tier.")
                except Exception as ee:
                    print(f"ERROR: {ee}", file=sys.stderr)
                    return 1
            else:
                # Proceed without auto-adjust (but still allow update without perf)
                caps_error = e
        except Exception as e:
            caps_error = e

    if caps_error and (args.auto_adjust):
        print(f"ERROR: Capabilities lookup failed; cannot auto-adjust.\n{caps_error}", file=sys.stderr)
        return 1

    # If we have perf, validate/adjust per-db and pool size
    if perf is not None:
        max_limits = get_db_max_limits(perf)
        if max_limits:
            if float(desired_db_max) not in max_limits:
                if args.auto_adjust:
                    new_db_max = choose_closest_leq(max_limits, float(desired_db_max))
                    print(f"Requested db-max-dtu {desired_db_max} not supported. Auto-adjusting -> {new_db_max}.")
                    desired_db_max = float(new_db_max)
                else:
                    print(
                        f"ERROR: db-max-dtu {desired_db_max} not supported for {target_tier} {desired_pool_dtu} DTU. "
                        f"Use --auto-adjust or check --list-options.",
                        file=sys.stderr,
                    )
                    return 1

            min_limits = get_db_min_limits_for_max(perf, float(desired_db_max))
            if min_limits and float(desired_db_min) not in min_limits:
                if args.auto_adjust:
                    new_db_min = choose_closest_leq(min_limits, float(desired_db_min))
                    print(f"Requested db-min-dtu {desired_db_min} not supported. Auto-adjusting -> {new_db_min}.")
                    desired_db_min = float(new_db_min)
                else:
                    print(
                        f"ERROR: db-min-dtu {desired_db_min} not supported for db-max-dtu {desired_db_max}. "
                        f"Use --auto-adjust.",
                        file=sys.stderr,
                    )
                    return 1

        cap_pool_max = get_supported_pool_max_bytes(perf)
        if cap_pool_max is not None and desired_pool_max_bytes is not None and desired_pool_max_bytes > cap_pool_max:
            if args.auto_adjust:
                print(
                    f"Requested pool max size {bytes_to_human(desired_pool_max_bytes)} exceeds supported "
                    f"{bytes_to_human(cap_pool_max)}. Auto-adjusting."
                )
                desired_pool_max_bytes = cap_pool_max
            else:
                print(
                    f"ERROR: pool max size exceeds supported limit ({bytes_to_human(cap_pool_max)}). Use --auto-adjust.",
                    file=sys.stderr,
                )
                return 1
    else:
        # Capabilities not available. Proceed, but warn once (unless dry-run/no-op).
        if caps_error:
            print(
                f"WARNING: Capabilities lookup failed; proceeding without validation.\n{caps_error}",
                file=sys.stderr,
            )

    print("\nTarget:")
    print(f"  SKU: name={target_sku_name!r}, tier={target_tier!r}, capacity(DTU)={desired_pool_dtu}")
    print(f"  per-db: min={desired_db_min}, max={desired_db_max}")
    print(f"  pool max size: {desired_pool_max_bytes} ({bytes_to_human(desired_pool_max_bytes)})")

    # No-op check
    def safe_lower(x: Any) -> str:
        return str(x).lower() if x is not None else ""

    if (
        safe_lower(cur_sku_name) == safe_lower(target_sku_name)
        and safe_lower(cur_tier) == safe_lower(target_tier)
        and int(cur_cap or 0) == int(desired_pool_dtu)
        and float(cur_db_min or 0.0) == float(desired_db_min)
        and float(cur_db_max or 0.0) == float(desired_db_max)
        and int(cur_max or 0) == int(desired_pool_max_bytes or 0)
    ):
        print("No change needed.")
        return 0

    if args.dry_run:
        print("Dry run: not applying changes.")
        return 0

    # Build PATCH update
    update = ElasticPoolUpdate(
        sku=Sku(name=target_sku_name, tier=target_tier, capacity=int(desired_pool_dtu)),
        per_database_settings=ElasticPoolPerDatabaseSettings(
            min_capacity=float(desired_db_min),
            max_capacity=float(desired_db_max),
        ),
    )
    if desired_pool_max_bytes is not None:
        update.max_size_bytes = int(desired_pool_max_bytes)

    # Apply
    try:
        poller = client.elastic_pools.begin_update(
            args.resource_group,
            args.server_name,
            args.pool_name,
            parameters=update,
        )
        if args.no_wait:
            print("Update started (no-wait).")
            return 0
        updated = poller.result()
    except HttpResponseError as e:
        print(
            "ERROR: Elastic pool update failed.\n"
            "Tip: invalid DTU/per-db/storage combos or current usage exceeding new caps can cause this.\n"
            f"{e}",
            file=sys.stderr,
        )
        return 1

    new_sku = get_any(updated, "sku", default=None)
    new_pds = get_any(updated, "per_database_settings", "perDatabaseSettings", default=None)
    new_max = to_int(get_any(updated, "max_size_bytes", "maxSizeBytes", default=None), None)

    print("\nDone.")
    print(
        f"New SKU: name={get_any(new_sku, 'name', default=None)!r}, "
        f"tier={get_any(new_sku, 'tier', default=None)!r}, "
        f"capacity={get_any(new_sku, 'capacity', default=None)}"
    )
    print(
        f"New per-db: min={get_any(new_pds, 'min_capacity', 'minCapacity', default=None)}, "
        f"max={get_any(new_pds, 'max_capacity', 'maxCapacity', default=None)}"
    )
    print(f"New pool max_size_bytes: {new_max} ({bytes_to_human(new_max)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())