"""Tenant ID -> tenant name lookup.

Logs carry two different tenant IDs:
  * context.TenantId      - the CoreRelate tenant ID (the keys of TENANT_MAP)
  * "User: x Tenant: <id>" - the identity-provider (Entra) tenant ID, embedded in the message

The identity tenant ID is translated to a CoreRelate tenant by learning from rows
that carry both IDs (see analysis.resolve_tenants), so only CoreRelate IDs live here.
"""

# CoreRelate tenant ID -> display name (source: Tenant table)
TENANT_MAP = {
    "1EA154F3-7A3E-48FB-A202-2662661B7961": "TeslaMotorsInc.onmicrosoft.com",
    "24E85180-592E-40D5-A481-C743252E1A70": "Gray Robinson, P.A.",
    "2908DDD6-A098-4E35-B466-03417B6CFAAF": "Tredway Lumsdaine & Doyle LLP",
    "2AA0AF1B-3839-4893-BE11-1FFDC65D7377": "Klein DeNatale Goldner",
    "2D1B17D0-5149-42F7-8E43-8D3115EE3F51": "larsonlawgrouppc.com",
    "2E7D63CF-4D3C-440F-B577-D1841EDF40D3": "brownsims.com",
    "33F7B7D7-BF6B-4672-8CA4-05E28A0883F3": "princelobel.com",
    "418F3279-23F4-4CF3-A640-AF831AEA0D30": "Nilan Johnson Lewis PA",
    "42DE156F-419F-4727-9C9F-A76F43C6EC31": "Leland Parchini",
    "449E37C8-D30F-4A08-AADD-9F4663472DF8": "Dinsmore Shohl",
    "50C6FC65-E8C8-4B36-914B-19C02A24F27E": "Stoll Keenon Ogden PLLC",
    "52AC1B26-82F4-4DEB-9A0D-8F8FD9F7831A": "Sherrard Roe Voigt & Harbison PLC",
    "5A1DF575-A6E5-4747-B3A9-7F4C99874B78": "kiesel.law",
    "6834D390-096C-4848-9C4F-FF13858B0AE4": "Trent & Taylor L.L.P.",
    "7840636A-C5CA-4D5E-B815-563417576DB5": "Kane Russell Coleman Logan PC",
    "7E4F9B60-2FE7-40E8-8E53-0218C4AAE53C": "ferraiuoli.com",
    "7F3C9A8B-2E6F-4D9A-9B6E-8C4F1E2A5D7B": "Richards Buell Sutton",
    "8A3F2D91-6C4E-4B2A-9E7F-1C5A8B9D42EF": "Lasher Law",
    "8D2F6A9E-7C4B-4E13-A5C8-1F0D9B6E247A": "Cotchett Pitre & McCarthy LLP",
    "91D3E494-160A-41C4-827B-9CFDF8278690": "Parker Milliken",
    "97949868-C2FA-4365-AC46-60F96939F13C": "Barley Snyder",
    "9D8BDBEE-1489-4DCE-8B66-C0D02FE45DD6": "Meagher Geer P.L.L.P.",
    "A337DAAB-24D0-4D99-B6D5-B43CF470148D": "Nichols Kaster",
    "D73CC1DC-7D2A-4404-8368-C06B987505FA": "Beveridge & Diamond",
    "EB42997C-21B7-4AD6-8EB1-701E85C8F168": "letofskymcclain.com",
    "EB4712E6-E253-4555-B9E2-3A9FE2164F0A": "GAMMAGE & BURNHAM",
    "F3AABE40-1504-4338-B203-F60E4F4E2B14": "Friday Eldredge & Clark LLP",
    "FF369226-0EDA-4D0D-B48C-6B27F404ECCB": "coralegalgroup.com",
}


def map_tenant_name(tenant_id):
    """Case-insensitive lookup; also accepts a partial (prefix) Tenant ID. Returns None if unknown."""
    if not tenant_id:
        return None

    tenant_id = str(tenant_id).strip().upper()
    if tenant_id in TENANT_MAP:
        return TENANT_MAP[tenant_id]

    for full_id, name in TENANT_MAP.items():
        if full_id.startswith(tenant_id):
            return name

    return None


# User e-mail domain -> CoreRelate tenant ID. Used only when a log row has an identity
# tenant that could not be matched from the data itself. Domains seen alongside a known
# tenant are learned automatically; add aliases here for the rest (see the
# "Unresolved Identities" section of the report).
DOMAIN_MAP = {
    "dinslaw.com": "449E37C8-D30F-4A08-AADD-9F4663472DF8",  # Dinsmore Shohl
    "lpslaw.com": "42DE156F-419F-4727-9C9F-A76F43C6EC31",  # Leland Parchini
    "lasher.com": "8A3F2D91-6C4E-4B2A-9E7F-1C5A8B9D42EF",  # Lasher Law
}
