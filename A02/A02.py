import struct
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import dns.resolver as _dns_resolver

_resolver = _dns_resolver.Resolver(configure=False)
_resolver.nameservers = ["8.8.8.8", "1.1.1.1"]
_resolver.lifetime = 5

_STATIC_RECORDS = {
    "google.com": {
        "A":  ["142.250.190.46", "142.250.190.78", "142.250.190.110"],
        "NS": ["ns1.google.com.", "ns2.google.com.", "ns3.google.com.", "ns4.google.com."],
        "MX": ["10 smtp.google.com."],
    },
    "facebook.com": {
        "A":  ["157.240.241.35", "157.240.229.35"],
        "NS": ["a.ns.facebook.com.", "b.ns.facebook.com."],
        "MX": ["10 smtpin.vvv.facebook.com."],
    },
    "amazon.com": {
        "A":  ["205.251.242.103", "52.94.236.248", "54.239.28.85"],
        "NS": ["pdns1.ultradns.net.", "pdns2.ultradns.net.", "pdns3.ultradns.org.", "pdns4.ultradns.org."],
        "MX": ["10 amazon-smtp.amazon.com."],
    },
    "github.com": {
        "A":  ["140.82.121.4"],
        "NS": ["ns-1707.awsdns-21.co.uk.", "ns-421.awsdns-52.com.", "ns-520.awsdns-01.net.", "ns-1283.awsdns-32.org."],
        "MX": ["1 aspmx.l.google.com.", "5 alt1.aspmx.l.google.com.", "5 alt2.aspmx.l.google.com."],
    },
    "yahoo.com": {
        "A":  ["74.6.143.26", "74.6.143.25", "74.6.231.20", "98.137.11.163"],
        "NS": ["ns1.yahoo.com.", "ns2.yahoo.com.", "ns3.yahoo.com.", "ns4.yahoo.com.", "ns5.yahoo.com."],
        "MX": ["1 mta5.am0.yahoodns.net.", "1 mta6.am0.yahoodns.net.", "1 mta7.am0.yahoodns.net."],
    },
    "wikipedia.org": {
        "A":  ["103.102.166.224"],
        "NS": ["ns0.wikimedia.org.", "ns1.wikimedia.org.", "ns2.wikimedia.org."],
        "MX": ["10 mx1001.wikimedia.org.", "50 mx2001.wikimedia.org."],
    },
}


@dataclass
class DNSMessage:
    identification: int
    flags: int
    qname: str
    qtype: str
    answers: list[str] = field(default_factory=list)

    FLAG_QUERY = 0x0000
    FLAG_REPLY = 0x8000

    def to_bytes(self) -> bytes:
        qn = self.qname.encode()
        qt = self.qtype.encode()
        payload = struct.pack("!H", len(qn)) + qn
        payload += struct.pack("!H", len(qt)) + qt
        answers = self.answers or []
        payload += struct.pack("!H", len(answers))
        for ans in answers:
            ab = ans.encode()
            payload += struct.pack("!H", len(ab)) + ab
        header = struct.pack("!HH", self.identification, self.flags)
        return header + payload

    @classmethod
    def from_bytes(cls, data: bytes) -> "DNSMessage":
        id_, flags = struct.unpack("!HH", data[:4])
        i = 4

        qn_len = struct.unpack("!H", data[i: i + 2])[0]; i += 2
        qname  = data[i: i + qn_len].decode();            i += qn_len

        qt_len = struct.unpack("!H", data[i: i + 2])[0];  i += 2
        qtype  = data[i: i + qt_len].decode();             i += qt_len

        ans_cnt = struct.unpack("!H", data[i: i + 2])[0]; i += 2
        answers = []
        for _ in range(ans_cnt):
            al = struct.unpack("!H", data[i: i + 2])[0]; i += 2
            answers.append(data[i: i + al].decode());     i += al

        return cls(identification=id_, flags=flags, qname=qname, qtype=qtype, answers=answers)

    def is_query(self) -> bool:
        return (self.flags & 0x8000) == 0

    def is_reply(self) -> bool:
        return bool(self.flags & 0x8000)


class DNSCache:
    def __init__(self, max_size: int = 5, ttl_seconds: int = 120):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._store: OrderedDict[tuple[str, str], dict] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, domain: str, rtype: str) -> Optional[dict]:
        key = (domain.lower(), rtype.upper())
        with self._lock:
            if key not in self._store:
                print(f"  [Cache] MISS  -> {key}")
                return None
            entry = self._store[key]
            age = (datetime.now() - entry["ts"]).total_seconds()
            if age >= self.ttl_seconds:
                del self._store[key]
                print(f"  [Cache] EXPIRED ({age:.0f}s > ttl={self.ttl_seconds}s) -> {key}")
                return None
            self._store.move_to_end(key)
            print(f"  [Cache] HIT   -> {key}  (age={age:.1f}s)")
            return entry["value"]

    def put(self, domain: str, rtype: str, value: dict):
        key = (domain.lower(), rtype.upper())
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            elif len(self._store) >= self.max_size:
                evicted, _ = self._store.popitem(last=False)
                print(f"  [Cache] AUTO-FLUSH (cache full) -> evicted {evicted}")
            self._store[key] = {"value": value, "ts": datetime.now()}
            print(f"  [Cache] STORED -> {key}")

    def show(self):
        with self._lock:
            print(f"\n{'─'*60}")
            print(f"  LOCAL CACHE  [{len(self._store)}/{self.max_size} entries, TTL={self.ttl_seconds}s]")
            print(f"{'─'*60}")
            if not self._store:
                print("  (empty)")
            for i, (key, entry) in enumerate(self._store.items(), 1):
                age = (datetime.now() - entry["ts"]).total_seconds()
                print(f"  [{i}] {key[0]} ({key[1]})  age={age:.1f}s")
            print(f"{'─'*60}")


class RootServer:
    _TLD_MAP = {
        "com": "TLD-.com",
        "org": "TLD-.org",
        "net": "TLD-.net",
        "edu": "TLD-.edu",
        "pk":  "TLD-.pk",
        "io":  "TLD-.io",
    }

    def handle_query(self, msg: DNSMessage) -> Optional[str]:
        print("  [Root Server] Received query, checking TLD map ...")
        tld = msg.qname.lower().rstrip(".").split(".")[-1]
        referral = self._TLD_MAP.get(tld)
        if referral:
            print(f"  [Root Server] Referral -> {referral}")
        else:
            print(f"  [Root Server] Unknown TLD: .{tld}")
        return referral


class TLDServer:
    def __init__(self, tld: str):
        self.tld = tld.lower()

    def handle_query(self, msg: DNSMessage) -> Optional[str]:
        print(f"  [TLD .{self.tld}] Received query ...")
        domain = msg.qname.lower().rstrip(".")
        if not domain.endswith(f".{self.tld}"):
            print(f"  [TLD .{self.tld}] Domain does not belong to this TLD.")
            return None
        print(f"  [TLD .{self.tld}] Referral -> Authoritative server for {domain}")
        return "Authoritative"


class AuthoritativeServer:
    def query(self, domain: str) -> dict[str, list[str]]:
        print(f"  [Auth Server] Resolving records for {domain} ...")
        records = {"A": [], "NS": [], "MX": []}
        for rtype in ("A", "NS", "MX"):
            try:
                answers = _resolver.resolve(domain, rtype)
                if rtype == "A":
                    records["A"] = [str(r) for r in answers]
                elif rtype == "NS":
                    records["NS"] = [str(r) for r in answers]
                elif rtype == "MX":
                    records["MX"] = [f"{r.preference} {r.exchange}" for r in answers]
            except Exception:
                records[rtype] = _STATIC_RECORDS.get(domain, {}).get(rtype, [])
        print(f"  [Auth Server] Resolution complete.")
        return records


class DNSClient:
    def __init__(self):
        self.cache = DNSCache(max_size=5, ttl_seconds=120)
        self._root = RootServer()
        self._auth = AuthoritativeServer()
        self._id_counter = 0
        self._id_lock = threading.Lock()

    def _next_id(self) -> int:
        with self._id_lock:
            self._id_counter = (self._id_counter + 1) & 0xFFFF
            return self._id_counter

    @staticmethod
    def _show_message(msg: DNSMessage, label: str):
        raw = msg.to_bytes()
        parsed = DNSMessage.from_bytes(raw)
        kind = "REPLY" if parsed.is_reply() else "QUERY"
        print(
            f"  [{label}]\n"
            f"    ID=0x{parsed.identification:04X} ({parsed.identification})  "
            f"FLAGS=0x{parsed.flags:04X} ({kind})  "
            f"QNAME={parsed.qname}  QTYPE={parsed.qtype}  "
            f"ANSWERS={len(parsed.answers)}"
        )

    def resolve(self, domain: str) -> tuple[dict, bool]:
        domain = domain.lower().rstrip(".")

        cached = self.cache.get(domain, "ALL")
        if cached is not None:
            reply = DNSMessage(
                identification=self._next_id(),
                flags=DNSMessage.FLAG_REPLY,
                qname=domain,
                qtype="ALL",
                answers=cached.get("A", []),
            )
            self._show_message(reply, "LOCAL CACHE -> CLIENT (REPLY)")
            return cached, True

        query = DNSMessage(
            identification=self._next_id(),
            flags=DNSMessage.FLAG_QUERY,
            qname=domain,
            qtype="ALL",
        )
        self._show_message(query, "CLIENT -> ROOT (QUERY)")

        tld_ref = self._root.handle_query(query)
        if not tld_ref:
            raise ValueError(f"Unsupported TLD for '{domain}'")

        tld = domain.split(".")[-1]
        tld_server = TLDServer(tld)
        auth_ref = tld_server.handle_query(query)
        if not auth_ref:
            raise ValueError(f"TLD server could not resolve '{domain}'")

        records = self._auth.query(domain)

        reply = DNSMessage(
            identification=query.identification,
            flags=DNSMessage.FLAG_REPLY,
            qname=domain,
            qtype="ALL",
            answers=records["A"] or ["NO_A_RECORD"],
        )
        self._show_message(reply, "AUTH SERVER -> CLIENT (REPLY)")

        self.cache.put(domain, "ALL", records)
        return records, False


class DNSApplication:
    SEP = "=" * 70

    def __init__(self):
        self.client = DNSClient()

    @staticmethod
    def _print_records(domain: str, records: dict):
        a_list  = records.get("A",  [])
        ns_list = records.get("NS", [])
        mx_list = records.get("MX", [])
        first_ip = a_list[0] if a_list else "NO_A_RECORD"
        print(f"\n  {domain}/{first_ip}")
        print("  -- DNS INFORMATION --")
        print("  A : " + (", ".join(a_list)  if a_list  else "N/A"))
        print("  NS: " + (", ".join(ns_list) if ns_list else "N/A"))
        print("  MX: " + (", ".join(mx_list) if mx_list else "N/A"))

    def _lookup(self, domain: str):
        print(f"\n{'─'*70}")
        print(f"  Querying: {domain}")
        print(f"{'─'*70}")
        t0 = time.perf_counter()
        records, from_cache = self.client.resolve(domain)
        elapsed = (time.perf_counter() - t0) * 1000
        self._print_records(domain, records)
        src = "CACHE (local)" if from_cache else "NETWORK (root->tld->auth)"
        print(f"\n  resolved_in={elapsed:.2f} ms   source={src}")

    def run(self):
        print(self.SEP)
        print("  DNS Server Simulation")
        print("  Root -> TLD -> Authoritative | 16-bit ID + 16-bit FLAGS")
        print(self.SEP)

        domains = ["google.com", "facebook.com", "amazon.com", "github.com"]

        print("\n\n>>> DEMO 1: Recursive lookup - first pass (expected: cache MISS for all)")
        for d in domains:
            self._lookup(d)

        print("\n\n>>> DEMO 2: Re-query google.com - shows cache HIT and faster resolution")
        t0 = time.perf_counter()
        records, hit = self.client.resolve("google.com")
        elapsed = (time.perf_counter() - t0) * 1000
        self._print_records("google.com", records)
        print(f"\n  resolved_in={elapsed:.3f} ms   source={'CACHE' if hit else 'NETWORK'}")
        print("  ^ Significantly faster than the first network lookup")

        print("\n\n>>> DEMO 3: Cache auto-flush (max_size=5, adding 2 more unique entries)")
        print("  Current cache before inserts:")
        self.client.cache.show()

        print("\n  Adding yahoo.com ...")
        self._lookup("yahoo.com")

        print("\n  Adding wikipedia.org ...")
        self._lookup("wikipedia.org")

        print("\n  Final cache state (oldest entry should have been evicted):")
        self.client.cache.show()

        print(f"\n{self.SEP}")
        print("  Simulation complete.")
        print(self.SEP)


def main():
    app = DNSApplication()
    app.run()


if __name__ == "__main__":
    main()