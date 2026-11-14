#!/usr/bin/env python3
"""bd netcheck - can the 4 nodes really reach each other on the ports the cluster needs?

Run it on ALL 4 nodes at the same time (about 2 minutes, `--seconds N` to change).
Each node
  1. opens throwaway listeners on the ports its role must ACCEPT (ports already held by a real
     daemon are skipped: if a probe connects, the daemon is reachable too),
  2. keeps probing every other node's ports until all answer (or time runs out),
  3. sends 20 MB to each other node's bulk port and prints MB/s. A small connect that works while
     the bulk transfer stalls means an MTU / relay problem (set ZT_MTU=1400 in team.env).
It ends with a matrix and a hint for every failure. Exit code 1 if anything failed.
Python standard library only.
"""
import argparse
import os
import socket
import sys
import threading
import time

BULK_PORT = 40030
PORTS = {
    "node1": [40000, 40001, 4040],
    "node2": [9000, 9870, 9866, 9864, 9867, 8081, 40010, 40011, 40012, 40013, 40020],
    "node3": [7077, 8080, 8081, 9866, 9864, 9867, 40000, 40001, 40010, 40011, 40012, 40013,
              40020, 4040, 18080],
    "node4": [8000],
}
NODES = ["node1", "node2", "node3", "node4"]


MY_IP = ""            # set in main(): probes leave from the team IP, so peers see it as the source


def load_team():
    """IPs from the environment (set by bd), else straight from team.env."""
    ips = {n: os.environ.get(f"NODE{n[-1]}_IP", "") for n in NODES}
    if not all(ips.values()):
        path = os.environ.get("BD_TEAM_ENV") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "team.env")
        for line in open(path):
            k, _, v = line.strip().partition("=")
            if k.startswith("NODE") and k.endswith("_IP") and v:
                ips[f"node{k[4]}"] = v
    return ips


class Listeners:
    """Throwaway TCP servers. Remembers which peer IP reached which of our ports."""

    def __init__(self, ip, ports):
        self.ip, self.reached, self.in_use, self.bulk_from, self.lock = ip, {}, [], set(), threading.Lock()
        self.socks = []
        for p in ports + [BULK_PORT]:
            s = socket.socket()
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((ip, p))
            except OSError:
                self.in_use.append(p)
                s.close()
                continue
            s.listen(16)
            self.socks.append(s)
            threading.Thread(target=self._serve, args=(s, p), daemon=True).start()

    def _serve(self, s, port):
        while True:
            try:
                c, addr = s.accept()
            except OSError:
                return
            with self.lock:
                self.reached.setdefault(addr[0], set()).add(port)
            threading.Thread(target=self._handle, args=(c, port, addr[0]), daemon=True).start()

    def _handle(self, c, port, peer):
        try:
            if port == BULK_PORT:                         # accept and discard, then say "k"
                c.settimeout(60)
                while c.recv(1 << 16):
                    pass
                c.sendall(b"k")
                with self.lock:
                    self.bulk_from.add(peer)
        except OSError:
            pass
        finally:
            c.close()


def probe(ip, port, timeout=2):
    try:
        socket.create_connection((ip, port), timeout=timeout, source_address=(MY_IP, 0)).close()
        return True
    except OSError:
        return False


def bulk(ip, mb, timeout=30):
    """Send mb megabytes; return MB/s or None. The receiver answers 'k' after reading everything."""
    chunk = b"x" * (1 << 20)
    try:
        t0 = time.time()
        s = socket.create_connection((ip, BULK_PORT), timeout=5, source_address=(MY_IP, 0))
        s.settimeout(10)
        for _ in range(mb):
            if time.time() - t0 > timeout:
                return None
            s.sendall(chunk)
        s.shutdown(socket.SHUT_WR)
        ok = s.recv(1) == b"k"
        s.close()
        return mb / max(time.time() - t0, 0.001) if ok else None
    except OSError:
        return None


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--seconds", type=int, default=120, help="give up after this many seconds")
    ap.add_argument("--bulk-mb", type=int, default=20)
    args = ap.parse_args()

    ips = load_team()
    me = os.environ.get("BD_NODE", "")
    if me not in NODES:
        sys.exit("netcheck: node not detected (run it through `bd netcheck`)")
    global MY_IP
    my_ip = MY_IP = ips[me]
    peers = [n for n in NODES if n != me]
    ip_to_node = {ips[n]: n for n in NODES}

    try:
        lis = Listeners(my_ip, PORTS[me])
    except OSError as e:
        sys.exit(f"netcheck: cannot listen on {my_ip}: {e}")
    print(f"[netcheck] {me} {my_ip}: listening on {len(lis.socks) - 1} ports + bulk {BULK_PORT}"
          + (f"; already in use by a daemon, skipped: {lis.in_use}" if lis.in_use else ""))
    print(f"[netcheck] now start `bd netcheck` on the other nodes too. Up to {args.seconds} s.")

    ok = {(n, p): False for n in peers for p in PORTS[n]}
    speed = {n: None for n in peers}
    bulk_tried = {n: 0 for n in peers}
    t0, last_msg = time.time(), 0
    need_in = {p for p in PORTS[me] if p not in lis.in_use} | ({BULK_PORT} - set(lis.in_use))
    while time.time() - t0 < args.seconds:
        for (n, p) in ok:
            if not ok[(n, p)]:
                ok[(n, p)] = probe(ips[n], p)
        for n in peers:                                   # bulk once the peer's ports answer
            if speed[n] is None and bulk_tried[n] < 3 and probe(ips[n], BULK_PORT):
                bulk_tried[n] += 1
                speed[n] = bulk(ips[n], args.bulk_mb)
        with lis.lock:
            heard = {ip_to_node.get(ip): set(ports) for ip, ports in lis.reached.items()}
            bulk_in = {ip_to_node.get(ip) for ip in lis.bulk_from}
        all_out = all(ok.values()) and all(s is not None for s in speed.values())
        all_in = all(need_in <= heard.get(n, set()) | ({BULK_PORT} if n in bulk_in else set()) for n in peers)
        if all_out and all_in:
            time.sleep(4)                                 # let slower peers finish with us
            break
        if time.time() - last_msg > 10:
            last_msg = time.time()
            miss = sorted({n for (n, p), v in ok.items() if not v})
            print(f"[netcheck] {int(last_msg - t0):>3}s  waiting for: {', '.join(miss) or 'bulk/inbound'}"
                  f"  (reached me so far: {', '.join(sorted(n for n in heard if n)) or 'nobody'})")
        time.sleep(2)

    # ---- matrix ----------------------------------------------------------------------------
    bad = False
    print(f"\n[netcheck] RESULT as seen from {me} ({my_ip})\n")
    for n in peers:
        marks = " ".join(f"{p}{'✅' if ok[(n, p)] else '❌'}" for p in PORTS[n])
        sp = speed[n]
        bulk_txt = f"bulk {args.bulk_mb} MB: {sp:.1f} MB/s ✅" if sp else "bulk transfer ❌"
        print(f"  {me} -> {n} ({ips[n]}):  {marks}  {bulk_txt}")
    print()
    with lis.lock:
        heard = {ip_to_node.get(ip): set(ports) for ip, ports in lis.reached.items()}
        bulk_in = {ip_to_node.get(ip) for ip in lis.bulk_from}
    for n in peers:
        missing = sorted(need_in - heard.get(n, set()) - ({BULK_PORT} if n in bulk_in else set()))
        print(f"  {n} -> {me}: " + ("✅ reached all my ports" if not missing else f"❌ never reached my ports {missing}"))
        bad |= bool(missing)

    # ---- hints -----------------------------------------------------------------------------
    print()
    wsl = "microsoft" in open("/proc/version").read().lower() if os.path.exists("/proc/version") else False
    for n in peers:
        failed = [p for p in PORTS[n] if not ok[(n, p)]]
        bad |= bool(failed) or speed[n] is None
        if len(failed) == len(PORTS[n]):
            print(f"  ❌ {n}: nothing answers. Is {n} running `bd netcheck`? ping {ips[n]}? "
                  f"ZeroTier authorized? ufw on {n}? (TROUBLESHOOTING.md#ping, #ufw)")
        elif failed:
            print(f"  ❌ {n}: ports {failed} blocked or not open. Most likely ufw on {n} (bd setup there), "
                  f"or {n} is not running netcheck yet. (TROUBLESHOOTING.md#ufw)"
                  + (" If node3 is WSL2: ZeroTier must run INSIDE WSL (TROUBLESHOOTING.md#wsl2)." if n == "node3" else ""))
        if not failed and speed[n] is None:
            print(f"  ❌ {n}: small connections work but the bulk transfer stalls: an MTU / relay problem. "
                  f"Set ZT_MTU=1400 in team.env, run `bd setup` on every node, run netcheck again. (TROUBLESHOOTING.md#mtu)")
    for n in peers:
        missing = need_in - heard.get(n, set()) - ({BULK_PORT} if n in bulk_in else set())
        if missing and len(missing) != 0 and all(ok[(n, p)] for p in PORTS[n]) and speed[n] is not None:
            extra = " This node is WSL2: ZeroTier must run INSIDE WSL (TROUBLESHOOTING.md#wsl2)." if wsl else ""
            print(f"  ❌ {n} cannot reach {me} on {sorted(missing)} although {me} can reach {n}: inbound traffic to "
                  f"{me} is blocked (ufw / firewall / NAT).{extra}")
    if me == "node3" and wsl:
        print("  note: node3 is on WSL2: node2's executors must connect INTO this machine on port 40000.")
    print("\n[netcheck] " + ("some checks FAILED" if bad else "ALL OK: the network is ready for HDFS and Spark."))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
