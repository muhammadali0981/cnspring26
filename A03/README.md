# Assignment 3: Reliable Data Transfer (CS3001)

This folder contains a full simulation-based implementation of:
- rdt 3.0 (Stop-and-Wait)
- Go-Back-N (GBN)
- Selective Repeat (SR)

The simulator models a unidirectional data transfer channel (sender to receiver) while control information (ACKs) flows back from receiver to sender.

## Files
- `simulator.py`: main implementation and CLI runner.
- `report.md`: brief report, FSM diagrams, and testing results.
- `Assignment-III.pdf`: original assignment document.
- `Assignment-III.txt`: extracted text copy of the PDF (for convenience).

## Run Requirements
- Python 3.9+ (no external dependency required).

## Quick Start
From the workspace root:

```powershell
python A03/simulator.py --run-tests
```

This executes all required scenarios (clean, packet loss, packet corruption, and delayed packets) for all three protocols.

## Run a Single Protocol Manually
Examples:

```powershell
# rdt 3.0 (Stop-and-Wait)
python A03/simulator.py --protocol rdt --packet-count 20 --payload-size 16 --timeout-ms 120 --loss 0.1 --corrupt 0.1 --delay 0.2

# Go-Back-N with window size 5
python A03/simulator.py --protocol gbn --window-size 5 --packet-count 30 --payload-size 24

# Selective Repeat with custom delay behavior
python A03/simulator.py --protocol sr --window-size 6 --delay 0.5 --min-delay-ms 10 --max-delay-ms 50
```

## Main CLI Options
- `--protocol {rdt,gbn,sr}`: protocol to simulate.
- `--packet-count INT`: number of packets to transfer.
- `--payload-size INT`: payload size per packet.
- `--window-size INT`: sender/receiver window size for GBN and SR.
- `--timeout-ms INT`: retransmission timeout.
- `--loss FLOAT`: random packet loss probability in [0, 1].
- `--corrupt FLOAT`: random packet corruption probability in [0, 1].
- `--delay FLOAT`: probability of additional delay in [0, 1].
- `--min-delay-ms INT`, `--max-delay-ms INT`: base transmission delay bounds.
- `--seed INT`: random seed for reproducible runs.
- `--run-tests`: run required test matrix across all protocols.
- `--verbose`: print detailed event logs.

## Demo Notes
For a 4-5 minute demo:
1. Show one quick custom run for each protocol.
2. Run `--run-tests` and highlight PASS summary.
3. Open `report.md` and explain FSM and scenario results.
