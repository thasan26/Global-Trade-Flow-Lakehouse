"""Raw-data ingestion into the landing zone.

Both sources write the same newline-delimited JSON layout, so the Bronze
layer does not care where a file came from:

    <landing>/<flow>/period=<YYYY-MM>.jsonl

Each line holds one record with string fields:
``flow, period, cty_code, cty_name, commodity_code, commodity_desc, value_usd``.
"""

LANDING_FIELDS = [
    "flow",
    "period",
    "cty_code",
    "cty_name",
    "commodity_code",
    "commodity_desc",
    "value_usd",
]
