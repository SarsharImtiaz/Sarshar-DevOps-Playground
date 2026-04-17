#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Optional, Tuple, Any, List

from azure.core.exceptions import HttpResponseError
from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.mgmt.web import WebSiteManagementClient


def get_credential(prefer_az_cli: bool):
    """
    Auth options:
      - AzureCliCredential: best inside Azure DevOps AzureCLI@2 task (already logged in).
      - DefaultAzureCredential: best with service principal env vars in CI/CD.
    """
    if prefer_az_cli:
        return AzureCliCredential()

    # On build agents, Managed Identity usually isn't available and can slow auth attempts.
    return DefaultAzureCredential(exclude_managed_identity_credential=True)


def parse_args():
    p = argparse.ArgumentParser(
        description="Scale up/down an Azure App Service Plan by changing its SKU (e.g., PremiumV3 <-> Basic)."
    )

    p.add_argument("--subscription-id", default=os.getenv("AZURE_SUBSCRIPTION_ID"))
    p.add_argument("--resource-group", required=True)
    p.add_argument("--plan-name", required=True)

    p.add_argument("--target-sku", help="Target SKU name, e.g. B1, S1, P1v3, P0v3 (case-insensitive).")
    p.add_argument("--capacity", type=int, default=None, help="Optional target instance count (workers).")
    p.add_argument(
        "--auto-adjust-capacity",
        action="store_true",
        help="If set, clamps capacity to the target SKU's min/max allowed workers if needed.",
    )

    p.add_argument("--list-skus", action="store_true", help="List selectable SKUs for this plan and exit.")
    p.add_argument("--dry-run", action="store_true", help="Show what would change without applying it.")
    p.add_argument("--no-wait", action="store_true", help="Start the update but don't wait for completion.")
    p.add_argument("--prefer-az-cli", action="store_true", help="Use Azure CLI auth context (AzureCliCredential).")

    return p.parse_args()


def normalize_sku_name(name: str) -> str:
    # Azure SKUs are typically treated case-insensitively for matching purposes.
    return name.strip().lower()


# -----------------------------
# FIX: Helpers to handle dict OR SDK object results
# -----------------------------
def _dict_get_ci(d: dict, key: str, default=None):
    """Case-insensitive dict getter."""
    if key in d:
        return d[key]
    lk = key.lower()
    for k, v in d.items():
        if isinstance(k, str) and k.lower() == lk:
            return v
    return default


def _selectable_sku_items(selectable: Any) -> List[Any]:
    """
    get_server_farm_skus() may return:
      - dict with a 'value' array
      - SDK model with .value
      - iterable/paged result
    Normalize to a list of sku items.
    """
    if selectable is None:
        return []

    if isinstance(selectable, dict):
        return _dict_get_ci(selectable, "value", []) or []

    if hasattr(selectable, "value"):
        return getattr(selectable, "value") or []

    # Last resort: try iterating it (ItemPaged etc.)
    try:
        return list(selectable)
    except TypeError:
        return []


def _read_sku_info(sku_info: Any) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[int]]:
    """
    Return (sku_name, sku_tier, min_workers, max_workers, default_workers)
    for either dict-based or object-based sku_info.
    """
    # Dict / raw JSON form
    if isinstance(sku_info, dict):
        sku = _dict_get_ci(sku_info, "sku", {}) or {}
        cap = _dict_get_ci(sku_info, "capacity", {}) or {}

        # sku may itself be dict or object
        if isinstance(sku, dict):
            name = _dict_get_ci(sku, "name", None)
            tier = _dict_get_ci(sku, "tier", None)
        else:
            name = getattr(sku, "name", None)
            tier = getattr(sku, "tier", None)

        # cap may itself be dict or object
        if isinstance(cap, dict):
            min_w = _dict_get_ci(cap, "minimum", None)
            max_w = _dict_get_ci(cap, "maximum", None)
            def_w = _dict_get_ci(cap, "default", None)
        else:
            min_w = getattr(cap, "minimum", None)
            max_w = getattr(cap, "maximum", None)
            def_w = getattr(cap, "default", None)

        return name, tier, min_w, max_w, def_w

    # SDK object form
    sku_desc = getattr(sku_info, "sku", None)
    cap = getattr(sku_info, "capacity", None)

    name = getattr(sku_desc, "name", None)
    tier = getattr(sku_desc, "tier", None)

    min_w = getattr(cap, "minimum", None)
    max_w = getattr(cap, "maximum", None)
    def_w = getattr(cap, "default", None)

    return name, tier, min_w, max_w, def_w


def pick_target_from_selectable_skus(
    selectable_skus, target_sku_name: str
) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[int]]:
    """
    Returns:
      (sku_name, sku_tier, min_workers, max_workers, default_workers)
    by searching the selectable SKUs for the requested sku_name.
    """
    want = normalize_sku_name(target_sku_name)

    for sku_info in _selectable_sku_items(selectable_skus):
        name, tier, min_w, max_w, def_w = _read_sku_info(sku_info)
        if name and normalize_sku_name(name) == want:
            return name, tier, min_w, max_w, def_w

    return None, None, None, None, None


def main() -> int:
    args = parse_args()

    if not args.subscription_id:
        print("ERROR: Provide --subscription-id or set AZURE_SUBSCRIPTION_ID.", file=sys.stderr)
        return 2

    cred = get_credential(prefer_az_cli=args.prefer_az_cli)
    client = WebSiteManagementClient(credential=cred, subscription_id=args.subscription_id)

    # Fetch the plan
    try:
        plan = client.app_service_plans.get(args.resource_group, args.plan_name)
    except HttpResponseError as e:
        print(f"ERROR: Failed to get plan '{args.plan_name}' in RG '{args.resource_group}'.\n{e}", file=sys.stderr)
        return 1

    current_sku_name = getattr(getattr(plan, "sku", None), "name", None)
    current_sku_tier = getattr(getattr(plan, "sku", None), "tier", None)
    current_capacity = getattr(getattr(plan, "sku", None), "capacity", None)

    print(f"Plan: {args.plan_name}")
    print(f"Current SKU: name={current_sku_name!r}, tier={current_sku_tier!r}, capacity={current_capacity}")

    # Get selectable SKUs for this plan (best way to validate allowed moves)
    selectable = None
    try:
        selectable = client.app_service_plans.get_server_farm_skus(args.resource_group, args.plan_name)
    except HttpResponseError as e:
        print(
            "WARNING: Could not query selectable SKUs for this plan. "
            "You may have insufficient permissions, or the API is restricted.\n"
            f"{e}",
            file=sys.stderr,
        )

    if args.list_skus:
        if not selectable:
            print("No selectable SKU data available.")
            return 1

        sku_items = _selectable_sku_items(selectable)
        if not sku_items:
            print("Selectable SKU list is empty.")
            return 0

        rows = []
        for sku_info in sku_items:
            name, tier, mi, ma, de = _read_sku_info(sku_info)
            rows.append((name, tier, mi, ma, de))

        # Pretty print
        print("\nSelectable SKUs for this plan:")
        print("  SKU_NAME | TIER       | MIN_WORKERS | MAX_WORKERS | DEFAULT_WORKERS")
        print("  -------- | ---------- | ----------- | ----------- | ---------------")
        for n, t, mi, ma, de in rows:
            print(f"  {str(n):8} | {str(t):10} | {str(mi):11} | {str(ma):11} | {str(de):15}")
        return 0

    if not args.target_sku:
        print("ERROR: Provide --target-sku (or use --list-skus to see options).", file=sys.stderr)
        return 2

    # Choose target SKU details (name + tier) from selectable SKUs when possible.
    target_name = None
    target_tier = None
    min_w = max_w = def_w = None

    if selectable:
        target_name, target_tier, min_w, max_w, def_w = pick_target_from_selectable_skus(selectable, args.target_sku)

    if not target_name:
        # Fall back: accept the user's SKU name as-is, but warn that we couldn't validate.
        target_name = args.target_sku.strip()
        target_tier = None
        print(
            "WARNING: Target SKU wasn't found in selectable SKUs (or selectable SKUs unavailable). "
            "Proceeding without validation.",
            file=sys.stderr,
        )

    # Decide capacity
    desired_capacity = args.capacity if args.capacity is not None else current_capacity
    if desired_capacity is None:
        desired_capacity = 1

    # Enforce min/max if known
    if min_w is not None and desired_capacity < min_w:
        msg = f"Requested capacity {desired_capacity} is below target SKU minimum {min_w}."
        if args.auto_adjust_capacity:
            print(f"{msg} Auto-adjusting to {min_w}.")
            desired_capacity = min_w
        else:
            print(f"ERROR: {msg} Use --auto-adjust-capacity or pass a valid --capacity.", file=sys.stderr)
            return 1

    if max_w is not None and desired_capacity > max_w:
        msg = f"Requested capacity {desired_capacity} exceeds target SKU maximum {max_w}."
        if args.auto_adjust_capacity:
            print(f"{msg} Auto-adjusting to {max_w}.")
            desired_capacity = max_w
        else:
            print(f"ERROR: {msg} Use --auto-adjust-capacity or pass a valid --capacity.", file=sys.stderr)
            return 1

    print(f"Target SKU: name={target_name!r}, tier={target_tier!r} (tier may be None if not validated)")
    print(f"Target capacity: {desired_capacity}")

    # No-op check
    if normalize_sku_name(str(current_sku_name)) == normalize_sku_name(str(target_name)) and desired_capacity == current_capacity:
        print("No change needed.")
        return 0

    if args.dry_run:
        print("Dry run: not applying changes.")
        return 0

    # Apply changes:
    plan.sku.name = target_name
    if target_tier is not None:
        plan.sku.tier = target_tier
    plan.sku.capacity = desired_capacity

    try:
        poller = client.app_service_plans.begin_create_or_update(
            resource_group_name=args.resource_group,
            name=args.plan_name,
            app_service_plan=plan,
        )
        if args.no_wait:
            print("Update started (no-wait).")
            return 0

        updated = poller.result()
    except HttpResponseError as e:
        print(f"ERROR: Plan update failed.\n{e}", file=sys.stderr)
        return 1

    new_sku_name = getattr(getattr(updated, "sku", None), "name", None)
    new_sku_tier = getattr(getattr(updated, "sku", None), "tier", None)
    new_capacity = getattr(getattr(updated, "sku", None), "capacity", None)
    print(f"Done. New SKU: name={new_sku_name!r}, tier={new_sku_tier!r}, capacity={new_capacity}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())