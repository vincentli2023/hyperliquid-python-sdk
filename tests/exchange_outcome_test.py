import json

import eth_account
import pytest

from hyperliquid.exchange import Exchange
from hyperliquid.utils.signing import action_hash, recover_agent_or_user_from_l1_action
from hyperliquid.utils.types import Meta, SpotMeta

TEST_META: Meta = {"universe": []}
TEST_SPOT_META: SpotMeta = {"universe": [], "tokens": []}
WALLET = eth_account.Account.from_key("0x0123456789012345678901234567890123456789012345678901234567890123")


def _exchange(monkeypatch, vault_address=None):
    ex = Exchange(WALLET, meta=TEST_META, spot_meta=TEST_SPOT_META, vault_address=vault_address)
    sent = {}

    def fake_post_action(self, action, signature, nonce):
        sent.update(action=action, signature=signature, nonce=nonce)
        return {"status": "ok", "response": {"type": "default"}}

    monkeypatch.setattr(Exchange, "_post_action", fake_post_action)
    monkeypatch.setattr("hyperliquid.exchange.get_timestamp_ms", lambda: 1788347248485)
    return ex, sent


def test_merge_outcome_action_matches_onchain_serialization(monkeypatch):
    ex, sent = _exchange(monkeypatch)
    ex.merge_outcome(1345, 37)
    # explorer txDetails of a live mainnet merge (hash 0x4aea330c...): exact keys and key order
    assert json.dumps(sent["action"]) == '{"type": "userOutcome", "mergeOutcome": {"outcome": 1345, "amount": "37"}}'
    assert sent["nonce"] == 1788347248485


def test_merge_max_uses_null_amount(monkeypatch):
    ex, sent = _exchange(monkeypatch)
    ex.merge_outcome(1345)
    assert sent["action"] == {"type": "userOutcome", "mergeOutcome": {"outcome": 1345, "amount": None}}
    ex.merge_question(195)
    assert sent["action"] == {"type": "userOutcome", "mergeQuestion": {"question": 195, "amount": None}}


def test_split_and_negate_shapes(monkeypatch):
    ex, sent = _exchange(monkeypatch)
    ex.split_outcome(1346, 100)
    assert sent["action"] == {"type": "userOutcome", "splitOutcome": {"outcome": 1346, "amount": "100"}}
    ex.negate_outcome(195, 1361, 12.5)
    assert sent["action"] == {
        "type": "userOutcome",
        "negateOutcome": {"question": 195, "outcome": 1361, "amount": "12.5"},
    }


def test_amount_rejects_sub_wire_precision(monkeypatch):
    ex, _ = _exchange(monkeypatch)
    with pytest.raises(ValueError):
        ex.split_outcome(1346, 1e-9)


def test_signature_recovers_wallet_and_respects_vault(monkeypatch):
    for vault in (None, "0x1234567890123456789012345678901234567890"):
        ex, sent = _exchange(monkeypatch, vault_address=vault)
        ex.merge_outcome(1345, 37)
        recovered = recover_agent_or_user_from_l1_action(sent["action"], sent["signature"], vault, sent["nonce"], None, True)
        assert recovered == WALLET.address
        # the vault address is part of the signed hash: signing for a vault must not verify as a non-vault action
        other = None if vault else "0x1234567890123456789012345678901234567890"
        assert recover_agent_or_user_from_l1_action(sent["action"], sent["signature"], other, sent["nonce"], None, True) != WALLET.address


def test_action_hash_is_key_order_sensitive():
    a = {"type": "userOutcome", "mergeOutcome": {"outcome": 1345, "amount": "37"}}
    b = {"type": "userOutcome", "mergeOutcome": {"amount": "37", "outcome": 1345}}
    assert action_hash(a, None, 1, None) != action_hash(b, None, 1, None)
