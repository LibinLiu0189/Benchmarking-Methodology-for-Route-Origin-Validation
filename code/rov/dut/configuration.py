"""Render the existing BRIO/BIRD/Bngblaster configuration without starting services."""

# Render BIRD's RTR, import policy, BGP sessions, and kernel export configuration.
def bird_config(settings, peers):
    log_classes = "warning, error, fatal, bug"
    if settings["measure_full_sync"]:
        log_classes += ", trace"
    lines = [
        f"log stderr {{ {log_classes} }};",
        f"router id {settings['router_id']};",
    ]
    if settings.get("bird_trace_log"):
        trace_path = str(settings["bird_trace_log"])
        if any(char in trace_path for char in ('"', "\\", "\n", "\r")):
            raise ValueError("BIRD trace path contains unsupported configuration characters")
        trace_classes = "trace, info" if settings.get("bird_trace_info") else "trace"
        lines.extend([f'log "{trace_path}" {{ {trace_classes} }};', "timeformat log iso long us;"])
    families = sorted({afi for peer in peers for afi in peer["families"]})
    roa_options = []
    if settings.get("roa_trie", False):
        roa_options.append("trie on;")
    if settings.get("no_roa_settle", False):
        roa_options.extend(("min settle time 0 ms;", "max settle time 0 ms;"))
    roa_table_options = " { " + " ".join(roa_options) + " }" if roa_options else ""
    lines.extend([
        f"roa4 table r4{roa_table_options};",
        f"roa6 table r6{roa_table_options};",
        "protocol device devices {}",
    ])
    for afi in families:
        lines.extend(
            [
                f"protocol kernel fib{afi} {{",
                "  kernel table 254;",
                f"  ipv{afi} {{",
                "    import none;",
                "    export filter {",
                "      if source != RTS_BGP then reject;",
                '      print "ROVLOG RIB|", net, "|", proto, "|", gw, "|", ifname, '
                '"|", bgp_path, "|", bgp_origin, "|", bgp_med;',
                "      accept;",
                "    };",
                "  };",
                "}",
            ]
        )
    lines.extend(
        [
            "protocol rpki rpki_source {",
            *(["  debug { events };"] if settings["measure_full_sync"] else []),
            "  roa4 { table r4; };",
            "  roa6 { table r6; };",
            f"  remote 127.0.0.1 port {settings['rtr_port']};",
            "  max version 1;",
            "}",
        ]
    )
    for afi in families:
        lines.append(f"filter rov_import_v{afi} {{")
        if settings["rov"] == "on":
            lines.append(
                f"  if roa_check(r{afi}, net, bgp_path.last) = ROA_INVALID then reject;"
            )
        lines.extend(["  accept;", "}"])
    for peer in peers:
        local_address = peer["dut_address"]
        lines.extend(
            [
                f"protocol bgp {peer['name']} {{",
                f"  local {local_address} as {settings['dut_asn']};",
                f"  neighbor {peer['source_address']} as {peer['asn']};",
                f"  interface \"{peer['dut_interface']}\";",
                "  direct;",
                "  onlink on;",
                "  enforce first as off;",
                f"  allow as sets {'on' if settings['as_sets'] == 'accept' else 'off'};",
            ]
        )
        for afi in peer["families"]:
            lines.extend(
                [
                    f"  ipv{afi} {{",
                    "    gateway direct;",
                    "    next hop prefer global;",
                    "    import table on;",
                    f"    rpki reload {settings['rov']};",
                    f"    import filter rov_import_v{afi};",
                    "    export none;",
                    "  };",
                ]
            )
        lines.append("}")
    return "\n".join(lines) + "\n"


# Assign each peer's RAW input to its corresponding Bngblaster BGP session.
def generator_config(settings, peers):
    interfaces, sessions = [], []
    for peer in peers:
        source = peer["source_address"]
        dut = peer["dut_address"]
        for endpoint in peer["endpoints"]:
            interface = {
                "interface": endpoint["interface"],
                "gateway-mac": peer["dut_mac"],
                "ipv6-router-advertisement": False,
            }
            if peer["transport_afi"] == 4:
                interface["address"] = f"{endpoint['address']}/32"
                interface["gateway"] = dut
            else:
                interface["address-ipv6"] = f"{endpoint['address']}/128"
                interface["gateway-ipv6"] = dut
            interfaces.append(interface)
            sessions.append(
                {
                    "network-interface": endpoint["interface"],
                    "local-address": source,
                    "peer-address": dut,
                    "local-as": peer["asn"],
                    "peer-as": settings["dut_asn"],
                    "id": peer["router_id"],
                    "family": [f"ipv{afi}-unicast" for afi in peer["families"]],
                    "start-traffic": False,
                    "raw-update-file": peer["raw_file"],
                }
            )
    return {"interfaces": {"network": interfaces}, "bgp": sessions}
