# ROV Benchmarking Experiments

This repository contains the implementation and inputs for the experiments in Appendix B of [draft-lg-bmwg-benchmarking-methodology-for-rov-04]

## Repository layout

| Path | Contents |
| --- | --- |
| `rov/` | Experiment runners, data generation, monitoring, and offline analysis |
| `data/baseline/` | Shared BGP and VRP baseline data |
| `data/inputs/` | Experiment inputs |
| `data/statistics.csv` | Input statistics |
| `data/exp65-single-batch/` | Pre-update snapshots and monitoring targets for the Experiment 6.5 single-batch controls |
| `result.csv` | Summary statistics for the experiments reported in Appendix B |
| `software/` | Tool source code, build scripts, and local dependencies; binaries are built locally |
| `runtime/config/` | Configuration files generated during initialization |

## Experiment inputs

Paths in this table are relative to `data/inputs/`.

| Experiment | Input |
| --- | --- |
| 6.1 | `vrp-single/{50k,100k,500k,1m}/` |
| 6.2 | `vrp-prefix/prefixes-{000100,000500,001000}/{localized,distributed}/` |
| 6.3A | `vrp-prefix/prefixes-001000/distributed/` |
| 6.3B | `bgp/{mixed,withdraw_best,add_valid,add_invalid,invalid_to_valid,best_to_invalid}/` |
| 6.4 | The four `vrp-single/` inputs used by Experiment 6.1 |
| 6.5 | `vrp-batch/{500k-300-batches,batch500,batch1000}/` |
| 6.6 | `vrp-batch/500k-first60/` |
| 6.7, scenario 1 | `vrp-single/500k/` |
| 6.7, scenario 3 | The four `vrp-single/` inputs used by Experiment 6.1 |
| 6.7, scenario 4 | `vrp-single/500k/` |

The three Experiment 6.5 workloads share the same initial set of 500k VRPs. Each contains 300 batches, with target sizes of 100, 500, or 1,000 VRP operations per batch. Experiment 6.6 reuses the first 60 batches of the 100-operation workload.

## Building the tools

Run all commands below from `code/`, the directory containing this README.
Build the executables before running experiments.

The bundled dependency sysroot targets x86-64 Linux and requires a compatible
host system. Install the host build tools on Ubuntu/Debian:

```sh
sudo apt update
sudo apt install build-essential autoconf automake libtool cmake pkg-config python3
```

Build BIRD, its CLI client, Bngblaster, and BRIO from the bundled sources:

```sh
python3 software/build.py --jobs 4
```

This creates `software/bin/{bird,birdc,bngblaster,brio_rc}`. Then build the
instrumented BRIO executable in both directories used by the experiment runners:

```sh
sh software/build_brio_disconnect.sh "$PWD/software/bin-brio-first-vrp"
sh software/build_brio_disconnect.sh
```

Despite its name, this script compiles the current BRIO source, which includes
both first-VRP timing and disconnect/reset support. Each output directory links
`bird`, `birdc`, and `bngblaster` to `software/bin/`. Experiments 6.1–6.6 and the
full-reset and paired-incremental runs in 6.7 use `bin-brio-first-vrp`; the
disconnect run uses `bin-brio-disconnect`.

## Running the experiments

Run the commands below from this directory after building the tools. Make the bundled shared libraries available to the loader:

```sh
export LD_LIBRARY_PATH="$PWD/software/sysroot/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
mkdir -p new-results
```

### Experiment 6.1

Replace `50k` with another scale as needed. Use `--mode full` for full-table resynchronization. The positional argument `1` identifies the repetition.

```sh
python3 -m rov.experiments.exp61 data/inputs/vrp-single/50k/sequence/sequence.json 1 \
  --bgp data/baseline/bgp --mode incremental \
  --bin-dir software/bin-brio-first-vrp --results-dir new-results/exp61-50k
```

### Experiment 6.2

Select the prefix count and either `localized` or `distributed`. The draft uses ROA-table tries.

```sh
python3 -m rov.experiments.exp62 data/inputs/vrp-prefix/prefixes-000100/distributed/sequence.json \
  --output new-results/exp62-100-distributed --roa-trie --bin-dir software/bin-brio-first-vrp
```

### Experiment 6.3A

```sh
python3 -m rov.experiments.exp63a data/inputs/vrp-prefix/prefixes-001000/distributed/scenario.json \
  --output new-results/exp63a --bin-dir software/bin-brio-first-vrp
```

### Experiment 6.3B

Select a BGP workload and set `--rov` to `on` or `off`.

```sh
python3 -m rov.experiments.exp63b data/inputs/bgp/mixed/scenario.json \
  --rov on --output new-results/exp63b-mixed-on --bin-dir software/bin-brio-first-vrp
```

### Experiment 6.4

Replace `50k` with another scale as needed. The default steady-state resource sampling period is 30 seconds.

```sh
python3 -m rov.experiments.exp64 data/inputs/vrp-single/50k/scenario.json \
  --output new-results/exp64-50k --bin-dir software/bin-brio-first-vrp
```

### Experiment 6.5

Measure ROV and RIB/FIB in separate runs. Continuous runs monitor batches 10, 20, ..., 300 and remove completed targets every five seconds. ROV queries use one CLI connection, with eight commands per group. FIB probes use a global limit of 10,000 packets per second.

ROA-table settle timers remain enabled for ROV measurements and are disabled for RIB/FIB measurements. Both runners select this setting automatically from `--monitoring-mode`; `--roa-settle` and `--no-roa-settle` override it.

The following commands use the 1,000-operation workload. For 100 or 500 operations, replace `batch1000` with `500k-300-batches` or `batch500`, respectively.

```sh
python3 -m rov.experiments.exp65 data/inputs/vrp-batch/batch1000/scenario.json \
  --output new-results/exp65-1000-rov --monitoring-mode vrp rov \
  --monitor-every 10 --completion-check-interval 5 --timeout 1000 --steady-seconds 30

python3 -m rov.experiments.exp65 data/inputs/vrp-batch/batch1000/scenario.json \
  --output new-results/exp65-1000-rib-fib --monitoring-mode vrp rib fib \
  --monitor-every 10 --completion-check-interval 5 --timeout 1000 --pps 10000
```

For an isolated single-batch control, use the corresponding pre-update snapshot:

```sh
python3 -m rov.experiments.exp65.diagnostics.single_batch data/exp65-single-batch/batch1000/batch-010/scenario.json \
  --output new-results/exp65-single-1000-010-rov --monitoring-mode vrp rov

python3 -m rov.experiments.exp65.diagnostics.single_batch data/exp65-single-batch/batch1000/batch-010/scenario.json \
  --output new-results/exp65-single-1000-010-rib-fib --monitoring-mode vrp rib fib
```

### Experiment 6.6

The default steady-state resource sampling period is 30 seconds.

```sh
python3 -m rov.experiments.exp66 data/inputs/vrp-batch/500k-first60/scenario.json \
  --output new-results/exp66 --pps 10000
```

### Experiment 6.7

Scenario 1 uses `--mode disconnect`. Scenario 3 uses `--mode full-reset`; replace the scale as needed. For scenario 4, run the normal group first and pass its output to the malformed-update group.

```sh
python3 -m rov.experiments.exp67 data/inputs/vrp-single/500k/scenario.json \
  --output new-results/exp67-scenario1 --mode disconnect

python3 -m rov.experiments.exp67 data/inputs/vrp-single/50k/scenario.json \
  --output new-results/exp67-scenario3-50k --mode full-reset

python3 -m rov.experiments.exp67.scenarios.paired_incremental data/inputs/vrp-single/500k/scenario.json \
  --output new-results/exp67-scenario4-normal --group normal

python3 -m rov.experiments.exp67.scenarios.paired_incremental data/inputs/vrp-single/500k/scenario.json \
  --output new-results/exp67-scenario4-mixed --group mixed \
  --normal-result new-results/exp67-scenario4-normal/result.json
```

## Data generation

`rov.prepare` provides baseline sampling, format conversion, and initial VRP-state preparation. `rov.data_generation` provides random VRP updates, prefix-selected workloads, the mixed and five individual BGP workloads, batch slicing, and input statistics.

```sh
python3 -m rov.data_generation.nested_random --help
python3 -m rov.data_generation.prefix_workloads --help
python3 -m rov.data_generation --help
```

The supplied inputs are ready to use.

