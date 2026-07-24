#!/usr/bin/env python3
"""Debezium Oracle envelope 형식(JSON converter, schemas.enabled=true) 샘플 레코드 생성.
출력: key|value 한 줄씩 — kafka-console-producer (parse.key=true, key.separator=|)용."""
import json

ROW_FIELDS = [
    {"field": "ID", "type": "int64", "optional": False},
    {"field": "AMOUNT", "type": "string", "optional": True},
    {"field": "STATUS", "type": "string", "optional": True},
]

SOURCE_FIELDS = [
    {"field": "version", "type": "string", "optional": False},
    {"field": "connector", "type": "string", "optional": False},
    {"field": "name", "type": "string", "optional": False},
    {"field": "ts_ms", "type": "int64", "optional": False},
    {"field": "snapshot", "type": "string", "optional": True},
    {"field": "db", "type": "string", "optional": False},
    {"field": "schema", "type": "string", "optional": False},
    {"field": "table", "type": "string", "optional": False},
    {"field": "txId", "type": "string", "optional": True},
    {"field": "scn", "type": "string", "optional": True},
    {"field": "commit_scn", "type": "string", "optional": True},
]

VALUE_SCHEMA = {
    "type": "struct",
    "name": "dz.SRC.ORDERS.Envelope",
    "fields": [
        {"field": "before", "type": "struct", "optional": True,
         "name": "dz.SRC.ORDERS.Value", "fields": ROW_FIELDS},
        {"field": "after", "type": "struct", "optional": True,
         "name": "dz.SRC.ORDERS.Value", "fields": ROW_FIELDS},
        {"field": "source", "type": "struct", "optional": False,
         "name": "io.debezium.connector.oracle.Source", "fields": SOURCE_FIELDS},
        {"field": "op", "type": "string", "optional": False},
        {"field": "ts_ms", "type": "int64", "optional": True},
    ],
}

KEY_SCHEMA = {
    "type": "struct",
    "name": "dz.SRC.ORDERS.Key",
    "fields": [{"field": "ID", "type": "int64", "optional": False}],
}


def source(scn, ts):
    return {"version": "3.6.0.Final", "connector": "oracle", "name": "dz",
            "ts_ms": ts, "snapshot": "false", "db": "XE",
            "schema": "SRC", "table": "ORDERS",
            "txId": "tx-%d" % scn, "scn": str(scn), "commit_scn": str(scn + 5)}


def emit(op, before, after, scn, ts):
    key = json.dumps({"schema": KEY_SCHEMA,
                      "payload": {"ID": (after or before)["ID"]}})
    val = json.dumps({"schema": VALUE_SCHEMA,
                      "payload": {"op": op, "before": before, "after": after,
                                  "source": source(scn, ts), "ts_ms": ts + 3}})
    print(key + "|" + val)


T = 1753305600000
emit("r", None, {"ID": 1, "AMOUNT": "10.00", "STATUS": "OLD"}, 1000, T)
emit("c", None, {"ID": 2, "AMOUNT": "120.50", "STATUS": "NEW"}, 1010, T + 1000)
emit("u", {"ID": 2, "AMOUNT": "120.50", "STATUS": "NEW"},
     {"ID": 2, "AMOUNT": "99.00", "STATUS": "PAID"}, 1020, T + 2000)
emit("d", {"ID": 1, "AMOUNT": "10.00", "STATUS": "OLD"}, None, 1030, T + 3000)
