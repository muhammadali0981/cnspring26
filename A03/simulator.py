from __future__ import annotations

import argparse
import heapq
import random
import sys
import string
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple


def checksum_for(seq: int, ack: bool, payload: str) -> int:
    raw = f"{seq}|{int(ack)}|{payload}".encode("utf-8")
    return zlib.crc32(raw) & 0xFFFFFFFF


@dataclass
class Packet:
    seq: int
    ack: bool
    payload: str
    checksum: int

    @staticmethod
    def make_data(seq: int, payload: str) -> "Packet":
        return Packet(seq=seq, ack=False, payload=payload, checksum=checksum_for(seq, False, payload))

    @staticmethod
    def make_ack(seq: int) -> "Packet":
        return Packet(seq=seq, ack=True, payload="", checksum=checksum_for(seq, True, ""))

    def is_corrupt(self) -> bool:
        return self.checksum != checksum_for(self.seq, self.ack, self.payload)

    def clone(self) -> "Packet":
        return Packet(self.seq, self.ack, self.payload, self.checksum)


class EventLoop:
    def __init__(self) -> None:
        self.now_ms = 0
        self._counter = 0
        self._queue: List[Tuple[int, int, int, Callable[[], None]]] = []
        self._canceled: set[int] = set()

    def schedule(self, delay_ms: int, callback: Callable[[], None]) -> int:
        event_id = self._counter
        self._counter += 1
        run_at = self.now_ms + max(0, delay_ms)
        heapq.heappush(self._queue, (run_at, event_id, self._counter, callback))
        return event_id

    def cancel(self, event_id: int) -> None:
        self._canceled.add(event_id)

    def run(self, should_stop: Callable[[], bool], max_time_ms: int) -> bool:
        while self._queue:
            if should_stop():
                return True
            run_at, event_id, _, callback = heapq.heappop(self._queue)
            if event_id in self._canceled:
                self._canceled.remove(event_id)
                continue
            if run_at > max_time_ms:
                self.now_ms = max_time_ms
                return False
            self.now_ms = run_at
            callback()
        return should_stop()


@dataclass
class SimulationStats:
    data_sent: int = 0
    ack_sent: int = 0
    lost: int = 0
    corrupted: int = 0
    delayed: int = 0
    retransmissions: int = 0
    timeouts: int = 0


@dataclass
class SimConfig:
    protocol: str
    packet_count: int
    payload_size: int
    window_size: int
    timeout_ms: int
    loss_prob: float
    corrupt_prob: float
    delay_prob: float
    min_delay_ms: int
    max_delay_ms: int
    seed: int
    max_time_ms: int
    verbose: bool = False


class UnreliableChannel:
    def __init__(
        self,
        loop: EventLoop,
        rng: random.Random,
        config: SimConfig,
        stats: SimulationStats,
        logger: Callable[[str], None],
    ) -> None:
        self.loop = loop
        self.rng = rng
        self.cfg = config
        self.stats = stats
        self.log = logger

    def _schedule_delivery(self, packet: Packet, on_receive: Callable[[Packet], None], kind: str) -> None:
        if self.rng.random() < self.cfg.loss_prob:
            self.stats.lost += 1
            self.log(f"[{self.loop.now_ms:05d} ms] {kind} seq={packet.seq} LOST")
            return

        delivered = packet.clone()
        if self.rng.random() < self.cfg.corrupt_prob:
            delivered.checksum ^= 0xFFFFFFFF
            self.stats.corrupted += 1
            self.log(f"[{self.loop.now_ms:05d} ms] {kind} seq={packet.seq} CORRUPTED")

        delay = self.rng.randint(self.cfg.min_delay_ms, self.cfg.max_delay_ms)
        if self.rng.random() < self.cfg.delay_prob:
            extra = self.rng.randint(self.cfg.max_delay_ms, self.cfg.max_delay_ms * 3)
            delay += extra
            self.stats.delayed += 1
            self.log(f"[{self.loop.now_ms:05d} ms] {kind} seq={packet.seq} EXTRA_DELAY={extra} ms")

        self.loop.schedule(delay, lambda p=delivered: on_receive(p))

    def send_data(self, packet: Packet, on_receive: Callable[[Packet], None]) -> None:
        self.stats.data_sent += 1
        self.log(f"[{self.loop.now_ms:05d} ms] DATA send seq={packet.seq}")
        self._schedule_delivery(packet, on_receive, "DATA")

    def send_ack(self, packet: Packet, on_receive: Callable[[Packet], None]) -> None:
        self.stats.ack_sent += 1
        self.log(f"[{self.loop.now_ms:05d} ms] ACK send seq={packet.seq}")
        self._schedule_delivery(packet, on_receive, "ACK")


class SenderState(Enum):
    READY = "READY"
    WAIT_ACK = "WAIT_ACK"
    DONE = "DONE"


class ReceiverState(Enum):
    WAIT_EXPECTED = "WAIT_EXPECTED"


class BaseProtocol:
    def __init__(self, config: SimConfig, loop: EventLoop, channel: UnreliableChannel, messages: List[str]) -> None:
        self.cfg = config
        self.loop = loop
        self.channel = channel
        self.messages = messages
        self.delivered: List[str] = []

    def start(self) -> None:
        raise NotImplementedError

    def on_data(self, packet: Packet) -> None:
        raise NotImplementedError

    def on_ack(self, packet: Packet) -> None:
        raise NotImplementedError

    def is_complete(self) -> bool:
        raise NotImplementedError


class StopAndWaitProtocol(BaseProtocol):
    def __init__(self, config: SimConfig, loop: EventLoop, channel: UnreliableChannel, messages: List[str], stats: SimulationStats) -> None:
        super().__init__(config, loop, channel, messages)
        self.stats = stats
        self.sender_state = SenderState.READY
        self.receiver_state = ReceiverState.WAIT_EXPECTED
        self.sender_index = 0
        self.receiver_expected = 0
        self.timer_event: Optional[int] = None
        self.outstanding: Optional[Packet] = None

    def start(self) -> None:
        self._try_send_next()

    def _start_timer(self, seq_snapshot: int) -> None:
        self._stop_timer()
        self.timer_event = self.loop.schedule(
            self.cfg.timeout_ms, lambda s=seq_snapshot: self._on_timeout(s)
        )

    def _stop_timer(self) -> None:
        if self.timer_event is not None:
            self.loop.cancel(self.timer_event)
            self.timer_event = None

    def _try_send_next(self) -> None:
        if self.sender_index >= len(self.messages):
            self.sender_state = SenderState.DONE
            return
        if self.sender_state != SenderState.READY:
            return

        packet = Packet.make_data(self.sender_index, self.messages[self.sender_index])
        self.outstanding = packet
        self.sender_state = SenderState.WAIT_ACK
        self.channel.send_data(packet, self.on_data)
        self._start_timer(packet.seq)

    def _on_timeout(self, seq_snapshot: int) -> None:
        if self.sender_state != SenderState.WAIT_ACK or self.outstanding is None:
            return
        if self.outstanding.seq != seq_snapshot:
            return

        self.stats.timeouts += 1
        self.stats.retransmissions += 1
        self.channel.send_data(self.outstanding, self.on_data)
        self._start_timer(seq_snapshot)

    def on_ack(self, packet: Packet) -> None:
        if packet.is_corrupt() or not packet.ack:
            return
        if self.sender_state != SenderState.WAIT_ACK:
            return
        if packet.seq != self.sender_index:
            return

        self._stop_timer()
        self.sender_index += 1
        self.sender_state = SenderState.READY
        self._try_send_next()

    def on_data(self, packet: Packet) -> None:
        if packet.is_corrupt() or packet.ack:
            ack = Packet.make_ack(self.receiver_expected - 1)
            self.channel.send_ack(ack, self.on_ack)
            return

        if packet.seq == self.receiver_expected:
            self.delivered.append(packet.payload)
            ack = Packet.make_ack(packet.seq)
            self.channel.send_ack(ack, self.on_ack)
            self.receiver_expected += 1
        else:
            ack = Packet.make_ack(self.receiver_expected - 1)
            self.channel.send_ack(ack, self.on_ack)

    def is_complete(self) -> bool:
        return self.sender_state == SenderState.DONE and len(self.delivered) == len(self.messages)


class GoBackNProtocol(BaseProtocol):
    def __init__(self, config: SimConfig, loop: EventLoop, channel: UnreliableChannel, messages: List[str], stats: SimulationStats) -> None:
        super().__init__(config, loop, channel, messages)
        self.stats = stats
        self.sender_state = SenderState.READY
        self.receiver_state = ReceiverState.WAIT_EXPECTED
        self.base = 0
        self.next_seq = 0
        self.expected_seq = 0
        self.timer_event: Optional[int] = None
        self.packets = [Packet.make_data(i, msg) for i, msg in enumerate(messages)]

    def start(self) -> None:
        self._send_window()

    def _send_packet(self, seq: int, retransmission: bool = False) -> None:
        self.channel.send_data(self.packets[seq], self.on_data)
        if retransmission:
            self.stats.retransmissions += 1

    def _start_timer(self) -> None:
        self._stop_timer()
        self.timer_event = self.loop.schedule(self.cfg.timeout_ms, lambda b=self.base: self._on_timeout(b))

    def _stop_timer(self) -> None:
        if self.timer_event is not None:
            self.loop.cancel(self.timer_event)
            self.timer_event = None

    def _send_window(self) -> None:
        while self.next_seq < len(self.messages) and self.next_seq < self.base + self.cfg.window_size:
            if self.base == self.next_seq:
                self._start_timer()
            self._send_packet(self.next_seq)
            self.next_seq += 1
        if self.base >= len(self.messages):
            self.sender_state = SenderState.DONE
            self._stop_timer()
        else:
            self.sender_state = SenderState.WAIT_ACK

    def _on_timeout(self, base_snapshot: int) -> None:
        if self.base != base_snapshot:
            return
        if self.base >= self.next_seq:
            return

        self.stats.timeouts += 1
        for seq in range(self.base, self.next_seq):
            self._send_packet(seq, retransmission=True)
        self._start_timer()

    def on_ack(self, packet: Packet) -> None:
        if packet.is_corrupt() or not packet.ack:
            return

        ack_seq = packet.seq
        if ack_seq < self.base:
            return
        if ack_seq >= len(self.messages):
            return

        self.base = ack_seq + 1
        if self.base == self.next_seq:
            self._stop_timer()
        else:
            self._start_timer()
        self._send_window()

    def on_data(self, packet: Packet) -> None:
        if packet.is_corrupt() or packet.ack:
            ack = Packet.make_ack(self.expected_seq - 1)
            self.channel.send_ack(ack, self.on_ack)
            return

        if packet.seq == self.expected_seq:
            self.delivered.append(packet.payload)
            self.expected_seq += 1
            ack = Packet.make_ack(packet.seq)
            self.channel.send_ack(ack, self.on_ack)
        else:
            ack = Packet.make_ack(self.expected_seq - 1)
            self.channel.send_ack(ack, self.on_ack)

    def is_complete(self) -> bool:
        return self.base >= len(self.messages) and len(self.delivered) == len(self.messages)


class SelectiveRepeatProtocol(BaseProtocol):
    def __init__(self, config: SimConfig, loop: EventLoop, channel: UnreliableChannel, messages: List[str], stats: SimulationStats) -> None:
        super().__init__(config, loop, channel, messages)
        self.stats = stats
        self.sender_state = SenderState.READY
        self.receiver_state = ReceiverState.WAIT_EXPECTED

        self.base = 0
        self.next_seq = 0
        self.acked = [False] * len(messages)
        self.packets = [Packet.make_data(i, msg) for i, msg in enumerate(messages)]
        self.timers: Dict[int, int] = {}

        self.rcv_base = 0
        self.rcv_buffer: Dict[int, str] = {}

    def start(self) -> None:
        self._send_window()

    def _send_packet(self, seq: int, retransmission: bool = False) -> None:
        self.channel.send_data(self.packets[seq], self.on_data)
        if retransmission:
            self.stats.retransmissions += 1

    def _start_timer(self, seq: int) -> None:
        self._stop_timer(seq)
        self.timers[seq] = self.loop.schedule(self.cfg.timeout_ms, lambda s=seq: self._on_timeout(s))

    def _stop_timer(self, seq: int) -> None:
        event_id = self.timers.pop(seq, None)
        if event_id is not None:
            self.loop.cancel(event_id)

    def _send_window(self) -> None:
        while self.next_seq < len(self.messages) and self.next_seq < self.base + self.cfg.window_size:
            self._send_packet(self.next_seq)
            self._start_timer(self.next_seq)
            self.next_seq += 1

        if self.base >= len(self.messages):
            self.sender_state = SenderState.DONE
            for seq in list(self.timers.keys()):
                self._stop_timer(seq)
        else:
            self.sender_state = SenderState.WAIT_ACK

    def _on_timeout(self, seq: int) -> None:
        if seq >= len(self.messages):
            return
        if self.acked[seq]:
            return

        self.stats.timeouts += 1
        self._send_packet(seq, retransmission=True)
        self._start_timer(seq)

    def on_ack(self, packet: Packet) -> None:
        if packet.is_corrupt() or not packet.ack:
            return

        ack_seq = packet.seq
        if ack_seq < 0 or ack_seq >= len(self.messages):
            return

        if not self.acked[ack_seq]:
            self.acked[ack_seq] = True
            self._stop_timer(ack_seq)

        while self.base < len(self.messages) and self.acked[self.base]:
            self.base += 1

        self._send_window()

    def on_data(self, packet: Packet) -> None:
        if packet.is_corrupt() or packet.ack:
            return

        seq = packet.seq
        window_end = self.rcv_base + self.cfg.window_size

        if self.rcv_base <= seq < window_end:
            if seq not in self.rcv_buffer:
                self.rcv_buffer[seq] = packet.payload
            self.channel.send_ack(Packet.make_ack(seq), self.on_ack)

            while self.rcv_base in self.rcv_buffer:
                self.delivered.append(self.rcv_buffer.pop(self.rcv_base))
                self.rcv_base += 1
        elif seq < self.rcv_base:
            self.channel.send_ack(Packet.make_ack(seq), self.on_ack)

    def is_complete(self) -> bool:
        return self.base >= len(self.messages) and len(self.delivered) == len(self.messages)


@dataclass
class SimulationResult:
    protocol: str
    scenario: str
    success: bool
    delivered_count: int
    expected_count: int
    elapsed_ms: int
    stats: SimulationStats


def make_messages(count: int, payload_size: int, rng: random.Random) -> List[str]:
    alphabet = string.ascii_letters + string.digits
    messages: List[str] = []
    for i in range(count):
        prefix = f"P{i:04d}-"
        remaining = max(0, payload_size - len(prefix))
        body = "".join(rng.choice(alphabet) for _ in range(remaining))
        msg = (prefix + body)[:payload_size]
        messages.append(msg)
    return messages


def run_once(config: SimConfig, scenario_name: str = "custom") -> SimulationResult:
    rng = random.Random(config.seed)
    messages = make_messages(config.packet_count, config.payload_size, rng)

    loop = EventLoop()
    stats = SimulationStats()

    def logger(message: str) -> None:
        if config.verbose:
            print(message)

    channel = UnreliableChannel(loop, rng, config, stats, logger)

    if config.protocol == "rdt":
        protocol = StopAndWaitProtocol(config, loop, channel, messages, stats)
    elif config.protocol == "gbn":
        protocol = GoBackNProtocol(config, loop, channel, messages, stats)
    elif config.protocol == "sr":
        protocol = SelectiveRepeatProtocol(config, loop, channel, messages, stats)
    else:
        raise ValueError(f"Unsupported protocol: {config.protocol}")

    protocol.start()
    finished = loop.run(protocol.is_complete, config.max_time_ms)

    success = finished and protocol.is_complete() and protocol.delivered == messages
    return SimulationResult(
        protocol=config.protocol,
        scenario=scenario_name,
        success=success,
        delivered_count=len(protocol.delivered),
        expected_count=len(messages),
        elapsed_ms=loop.now_ms,
        stats=stats,
    )


def scenario_overrides() -> Dict[str, Dict[str, float]]:
    return {
        "clean": {"loss_prob": 0.0, "corrupt_prob": 0.0, "delay_prob": 0.0},
        "loss": {"loss_prob": 0.20, "corrupt_prob": 0.0, "delay_prob": 0.0},
        "corruption": {"loss_prob": 0.0, "corrupt_prob": 0.20, "delay_prob": 0.0},
        "delay": {"loss_prob": 0.0, "corrupt_prob": 0.0, "delay_prob": 0.60},
    }


def run_required_tests(base_config: SimConfig) -> List[SimulationResult]:
    results: List[SimulationResult] = []
    protocols = ["rdt", "gbn", "sr"]
    scenarios = scenario_overrides()

    run_index = 0
    for proto in protocols:
        for name, overrides in scenarios.items():
            cfg = SimConfig(
                protocol=proto,
                packet_count=base_config.packet_count,
                payload_size=base_config.payload_size,
                window_size=base_config.window_size,
                timeout_ms=base_config.timeout_ms,
                loss_prob=overrides["loss_prob"],
                corrupt_prob=overrides["corrupt_prob"],
                delay_prob=overrides["delay_prob"],
                min_delay_ms=base_config.min_delay_ms,
                max_delay_ms=base_config.max_delay_ms,
                seed=base_config.seed + run_index,
                max_time_ms=base_config.max_time_ms,
                verbose=False,
            )
            run_index += 1
            results.append(run_once(cfg, scenario_name=name))
    return results


def print_result(result: SimulationResult) -> None:
    status = "PASS" if result.success else "FAIL"
    s = result.stats
    print(
        f"[{status}] protocol={result.protocol:>3} scenario={result.scenario:<10} "
        f"delivered={result.delivered_count}/{result.expected_count} "
        f"time={result.elapsed_ms:5d}ms data_sent={s.data_sent:4d} ack_sent={s.ack_sent:4d} "
        f"lost={s.lost:3d} corrupt={s.corrupted:3d} delayed={s.delayed:3d} "
        f"timeouts={s.timeouts:3d} retrans={s.retransmissions:3d}"
    )


def print_results_table(results: List[SimulationResult]) -> None:
    headers = [
        "Status",
        "Protocol",
        "Scenario",
        "Delivered",
        "Time(ms)",
        "Data",
        "ACK",
        "Lost",
        "Corrupt",
        "Delayed",
        "Timeouts",
        "Retrans",
    ]

    rows: List[List[str]] = []
    for result in results:
        s = result.stats
        rows.append(
            [
                "PASS" if result.success else "FAIL",
                result.protocol,
                result.scenario,
                f"{result.delivered_count}/{result.expected_count}",
                str(result.elapsed_ms),
                str(s.data_sent),
                str(s.ack_sent),
                str(s.lost),
                str(s.corrupted),
                str(s.delayed),
                str(s.timeouts),
                str(s.retransmissions),
            ]
        )

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def make_sep(char: str = "-") -> str:
        return "+" + "+".join(char * (w + 2) for w in col_widths) + "+"

    def make_row(cells: List[str]) -> str:
        parts = [f" {cell:<{col_widths[i]}} " for i, cell in enumerate(cells)]
        return "|" + "|".join(parts) + "|"

    print(make_sep("="))
    print(make_row(headers))
    print(make_sep("="))
    for row in rows:
        print(make_row(row))
        print(make_sep("-"))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reliable Data Transfer simulator: rdt 3.0, GBN, and SR")
    parser.add_argument("--protocol", choices=["rdt", "gbn", "sr"], default="rdt")
    parser.add_argument("--packet-count", type=int, default=20)
    parser.add_argument("--payload-size", type=int, default=16)
    parser.add_argument("--window-size", type=int, default=4)
    parser.add_argument("--timeout-ms", type=int, default=120)
    parser.add_argument("--loss", type=float, default=0.1, help="packet loss probability in [0, 1]")
    parser.add_argument("--corrupt", type=float, default=0.1, help="packet corruption probability in [0, 1]")
    parser.add_argument("--delay", type=float, default=0.2, help="extra delay probability in [0, 1]")
    parser.add_argument("--min-delay-ms", type=int, default=10)
    parser.add_argument("--max-delay-ms", type=int, default=40)
    parser.add_argument("--seed", type=int, default=3001)
    parser.add_argument("--max-time-ms", type=int, default=100_000)
    parser.add_argument("--run-tests", action="store_true", help="run required scenarios for all protocols")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_all_by_default = len(sys.argv) == 1

    config = SimConfig(
        protocol=args.protocol,
        packet_count=args.packet_count,
        payload_size=args.payload_size,
        window_size=args.window_size,
        timeout_ms=args.timeout_ms,
        loss_prob=args.loss,
        corrupt_prob=args.corrupt,
        delay_prob=args.delay,
        min_delay_ms=args.min_delay_ms,
        max_delay_ms=args.max_delay_ms,
        seed=args.seed,
        max_time_ms=args.max_time_ms,
        verbose=args.verbose,
    )

    if args.run_tests or run_all_by_default:
        results = run_required_tests(config)
        print_results_table(results)
        all_passed = all(r.success for r in results)
        print(f"Overall: {'PASS' if all_passed else 'FAIL'}")
        return

    result = run_once(config, scenario_name="custom")
    print_results_table([result])


if __name__ == "__main__":
    main()
