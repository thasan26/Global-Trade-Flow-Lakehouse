from unittest.mock import MagicMock

from tradeflow.ingestion.census_api import CensusTradeClient, month_range


def test_month_range_crosses_year():
    assert month_range("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_fetch_month_normalises_imports_response():
    client = CensusTradeClient(api_key=None)
    resp = MagicMock(status_code=200)
    resp.json.return_value = [
        ["CTY_CODE", "CTY_NAME", "I_COMMODITY", "I_COMMODITY_LDESC", "GEN_VAL_MO", "time", "COMM_LVL"],
        ["5700", "CHINA", "85", "ELECTRICAL MACHINERY", "12345", "2024-01", "HS2"],
    ]
    client.session.get = MagicMock(return_value=resp)

    rows = client.fetch_month("imports", "2024-01")
    assert rows == [{"flow": "imports", "period": "2024-01", "cty_code": "5700", "cty_name": "CHINA",
                     "commodity_code": "85", "commodity_desc": "ELECTRICAL MACHINERY", "value_usd": "12345"}]
    params = client.session.get.call_args.kwargs["params"]
    assert "GEN_VAL_MO" in params["get"] and params["COMM_LVL"] == "HS2"
