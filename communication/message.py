from collections import namedtuple

Signal = namedtuple(
    "Signal",
    [
        "latest_signal",
        "signal_change",
        "cmd_nav",
        "engine",
        "ticker",
        "db_engine",
        "engine_intern",
        "st_name"
    ])

Registration = namedtuple(
    "Registration",
    [
        "ip_address",
        "port",
        "account"
    ])

