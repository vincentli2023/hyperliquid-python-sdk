import pytest

from hyperliquid.info import Info
from hyperliquid.utils.outcome import (
    OUTCOME_ASSET_BASE,
    format_outcome_time,
    is_outcome_coin,
    outcome_asset,
    outcome_coin,
    outcome_encoding,
    outcome_label,
    outcome_token,
    parse_outcome_coin,
    parse_outcome_description,
    parse_outcome_time_ms,
    settle_time_ms,
)
from hyperliquid.utils.types import Meta, SpotMeta

TEST_META: Meta = {"universe": [{"name": "BTC", "szDecimals": 5}]}
TEST_SPOT_META: SpotMeta = {"universe": [], "tokens": []}

SILVER_DESC = "perp:xyz:SILVER|priceDescription:xyz:SILVER-USDC perp mark|seconds:3|threshold:64.128|time:20260902-2100"
SILVER_META = {
    "outcome": 1346,
    "name": "template:binaryPrice",
    "description": SILVER_DESC,
    "sideSpecs": [{"name": "template:Yes"}, {"name": "template:No"}],
}
TEMPLATES = {"binaryPrice": {"id": "binaryPrice", "name": "{perp} above {threshold} at {time}?"}}


def test_encoding_matches_docs_and_live_order():
    # docs example: outcome 1, side 0 -> "#10" / "+10" / 100000010
    assert outcome_encoding(1, 0) == 10
    assert outcome_coin(1, 0) == "#10"
    assert outcome_token(1, 0) == "+10"
    assert outcome_asset("#10") == 100000010
    # live mainnet order on "#13390" carried a=100013390
    assert outcome_asset("#13390") == OUTCOME_ASSET_BASE + 13390
    assert parse_outcome_coin("#13381") == (1338, 1)
    assert outcome_coin(1338, 1) == "#13381"


def test_is_outcome_coin():
    assert is_outcome_coin("#13380")
    assert not is_outcome_coin("#")
    assert not is_outcome_coin("BTC")
    assert not is_outcome_coin("@107")
    assert not is_outcome_coin("xyz:SKHY")
    assert not is_outcome_coin("+13380")


@pytest.mark.parametrize("bad", [("#abc",), ("BTC",), ("",)])
def test_parse_rejects_non_outcome(bad):
    with pytest.raises(ValueError):
        parse_outcome_coin(bad[0])


def test_side_must_be_binary():
    with pytest.raises(ValueError):
        outcome_encoding(1338, 2)
    with pytest.raises(ValueError):
        outcome_encoding(-1, 0)


def test_description_parsing_keeps_colons_in_values():
    fields = parse_outcome_description(SILVER_DESC)
    assert fields["perp"] == "xyz:SILVER"
    assert fields["priceDescription"] == "xyz:SILVER-USDC perp mark"
    assert fields["time"] == "20260902-2100"
    assert parse_outcome_description("other") == {"other": ""}
    assert parse_outcome_description("") == {}


def test_settle_time():
    assert parse_outcome_time_ms("20260902-2100") == 1788382800000
    assert settle_time_ms(SILVER_DESC) == 1788382800000
    assert settle_time_ms("class:priceBinary|underlying:BTC|expiry:20260903-0600|targetPrice:77635|period:1d") == 1788415200000
    assert settle_time_ms("other") is None
    assert format_outcome_time("20260902-2100") == "2026-09-02 21:00 UTC"
    assert format_outcome_time("garbage") == "garbage"


def test_label_rendering():
    assert outcome_label("#13460", SILVER_META, TEMPLATES) == "xyz:SILVER above 64.128 at 2026-09-02 21:00 UTC? Yes"
    assert outcome_label("#13461", SILVER_META, TEMPLATES) == "xyz:SILVER above 64.128 at 2026-09-02 21:00 UTC? No"
    assert outcome_label("#13461", SILVER_META).startswith("binaryPrice perp:xyz:SILVER")
    assert outcome_label("#13461") == "#13461"
    assert outcome_label("BTC", SILVER_META, TEMPLATES) == "BTC"


def _offline_info(**kwargs) -> Info:
    return Info(skip_ws=True, meta=TEST_META, spot_meta=TEST_SPOT_META, **kwargs)


def test_info_default_tables_unchanged_by_outcome_support(monkeypatch):
    calls = []
    monkeypatch.setattr(Info, "post", lambda self, path, payload=None: calls.append(payload) or {"outcomes": []})
    info = _offline_info()
    explicit = _offline_info(outcome_markets=False)
    assert calls == []  # outcome_markets=False issues no request
    assert info.coin_to_asset == explicit.coin_to_asset == {"BTC": 0}
    assert info.name_to_coin == explicit.name_to_coin == {"BTC": "BTC"}
    assert info.asset_to_sz_decimals == explicit.asset_to_sz_decimals == {0: 5}
    assert info.outcome_meta_by_id == {} and info.outcome_templates_by_id == {}


def test_info_registers_outcome_on_demand():
    info = _offline_info()
    assert "#13390" not in info.name_to_coin
    assert info.name_to_asset("#13390") == 100013390
    assert info.name_to_coin["#13390"] == "#13390"
    assert info.asset_to_sz_decimals[100013390] == 0
    # non-outcome unknown names still raise, exactly as before
    with pytest.raises(KeyError):
        info.name_to_asset("NOPE")
    sub = {"type": "l2Book", "coin": "#13380"}
    info._remap_coin_subscription(sub)
    assert sub["coin"] == "#13380" and info.coin_to_asset["#13380"] == 100013380
    # user subscriptions are untouched
    user_sub = {"type": "userFills", "user": "0xabc"}
    info._remap_coin_subscription(user_sub)
    assert user_sub == {"type": "userFills", "user": "0xabc"}


def test_info_load_outcome_meta_offline(monkeypatch):
    def fake_post(self, path, payload=None):
        if payload["type"] == "outcomeMeta":
            return {"outcomes": [SILVER_META], "questions": [], "deployers": [], "feeScale": "1.0"}
        if payload["type"] == "outcomeTemplates":
            return list(TEMPLATES.values())
        raise AssertionError(payload)

    monkeypatch.setattr(Info, "post", fake_post)
    info = _offline_info(outcome_markets=True)
    assert info.coin_to_asset["#13460"] == 100013460 and info.coin_to_asset["#13461"] == 100013461
    assert info.outcome_label("#13460") == "xyz:SILVER above 64.128 at 2026-09-02 21:00 UTC? Yes"
    assert info.outcome_label("#99990") == "#99990"  # unknown outcome: raw name
    assert info.outcome_label("BTC") == "BTC"
    assert info.outcome_settle_time_ms("#13461") == 1788382800000
    assert info.outcome_settle_time_ms("#99990") is None


@pytest.mark.vcr()
def test_outcome_meta_and_templates_live_shape():
    info = _offline_info()
    meta = info.outcome_meta()
    assert {"outcomes", "questions", "deployers", "feeScale"} <= set(meta)
    templates = info.outcome_templates()
    ids = {t["id"] for t in templates}
    assert {"binaryPrice", "priceTouch"} <= ids
    assert "{perp}" in next(t for t in templates if t["id"] == "binaryPrice")["name"]
