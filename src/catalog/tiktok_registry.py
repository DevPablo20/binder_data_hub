from dataclasses import dataclass

SUPPORTED_LAYERS = frozenset({"silver", "gold"})


@dataclass(frozen=True)
class BridgeMapping:
    column: str
    target: str


@dataclass(frozen=True)
class TableEntry:
    layer: str
    name: str
    bridge_mappings: tuple[BridgeMapping, ...] = ()

    @property
    def path(self) -> str:
        return f"tiktok/{self.name}"


TIKTOK_TABLES: tuple[TableEntry, ...] = (
    TableEntry(
        "silver",
        "advertisers",
        (BridgeMapping("ad_account_id", "platform_account.external_account_id"),),
    ),
    TableEntry(
        "silver",
        "campaigns",
        (BridgeMapping("campaign_id", "platform_object_map.external_id (campaign)"),),
    ),
    TableEntry(
        "silver",
        "ad_groups",
        (BridgeMapping("ad_group_id", "platform_object_map.external_id (ad_set)"),),
    ),
    TableEntry(
        "silver",
        "ads",
        (BridgeMapping("ad_id", "platform_object_map.external_id (ad)"),),
    ),
    TableEntry("silver", "ads_reports_daily"),
    TableEntry(
        "gold",
        "campaign_daily_metrics",
        (
            BridgeMapping("ad_account_id", "platform_account.external_account_id"),
            BridgeMapping("campaign_id", "platform_object_map.external_id (campaign)"),
        ),
    ),
)

_TABLE_INDEX: dict[tuple[str, str], TableEntry] = {
    (entry.layer, entry.name): entry for entry in TIKTOK_TABLES
}


def get_table(layer: str, name: str) -> TableEntry | None:
    return _TABLE_INDEX.get((layer.lower(), name))


def tables_for_layer(layer: str) -> list[TableEntry]:
    layer = layer.lower()
    return [entry for entry in TIKTOK_TABLES if entry.layer == layer]
