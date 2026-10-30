---
stand_alone: true
ipr: trust200902
cat: info # Check
submissiontype: IETF
area: Operations and Management
wg: BMWG

docname: draft-lg-bmwg-benchmarking-methodology-for-rov-03

title: Benchmarking Methodology for Route Origin Validation (ROV)
abbrev: ROV Benchmarking
lang: en

author:
- ins: 
  name: Libin Liu
  org: Zhongguancun Laboratory
  city: Beijing
  country: China
  email: liulb@zgclab.edu.cn

- ins: 
  name: Nan Geng
  org: Huawei Technologies
  city: Beijing
  country: China
  email: gengnan@huawei.com

- ins: 
  name: Yanqi Chen
  org: Zhongguancun Laboratory
  city: Beijing
  country: China
  email: chenyq@zgclab.edu.cn

normative:
  RFC2119:
  RFC8174:
  RFC6811:
  RFC8210:
  RFC1242:
  RFC2285:
  RFC2544:
  RFC2889:
  RFC3918:
 
informative:
  RFC7942:

...

--- abstract

This document defines a benchmarking methodology for routers that implement Route Origin Validation (ROV). The methodology focuses on device-level behavior, including processing of validated Route Origin Authorization (ROA) payload (VRP) updates, the interaction between ROV and BGP, resource utilization, and the scalability of ROV under varying operational conditions. The procedures described here follow the principles and constraints of the Benchmarking Methodology Working Group (BMWG) and are intended to produce repeatable and comparable results across implementations.

--- middle

# Introduction

Route Origin Validation (ROV), as specified in {{RFC6811}}, allows routers to use validated Route Origin Authorization (ROA) information, which is distributed via the RPKI-to-Router (RTR) protocol defined in {{RFC8210}}, to classify BGP routes as Valid, Invalid, or NotFound. Deployments of ROV continue to increase across networks, and router vendors have implemented ROV processing as part of their control-plane functions.

While operational experience is growing, there is currently no standardized methodology for measuring the performance impact and behavioral characteristics of ROV on routing devices. As with other protocol features evaluated by the Benchmarking Methodology Working Group (BMWG), a consistent and repeatable test framework is essential for:

* Comparing router implementations,

* Evaluating scalability under controlled conditions,

* Characterizing the control-plane costs of ROV processing, and

* Understanding how ROV influences BGP convergence and routing stability.

This document defines a benchmarking methodology for routers that implement ROV, which builds upon the foundational benchmarking principles defined in {{RFC1242}}, {{RFC2285}}, {{RFC2544}}, {{RFC2889}}, and {{RFC3918}}. The methodology focuses on the Device Under Test (DUT) and uses controlled, reproducible inputs to isolate the effects of ROV from external dependencies. In particular, the benchmarking framework assumes the presence of an RTR update source, which may be an RPKI Cache Server or an RTR traffic generator capable of delivering synthetic Validated ROA Payloads (VRPs).

The objective of this document is to define a set of metrics and procedures to quantify:

* The latency of ROV state updates within the DUT,

* The impact of ROV on BGP control-plane performance,

* The scalability of ROV processing under varying VRP and BGP table sizes, and

* The resource utilization associated with enabling ROV.

By providing a consistent framework, this document enables vendors, operators, and researchers to evaluate ROV functionality under controlled and repeatable conditions, improving understanding of implementation performance and supporting informed deployment decisions.

## Requirements Language

{::boilerplate bcp14-tagged}

# Scope and Goals

This document specifies a laboratory-based benchmarking methodology for evaluating the performance of router implementations of ROV as defined in {{RFC6811}}. The scope of this benchmarking methodology includes:

* **ROV processing performance**: Measurement of the time and resources required for a router to process VRP updates received via the RTR protocol.  

* **Impact on BGP control-plane performance**: Quantification of how enabling ROV affects BGP convergence times and routing table stability.  

* **Scalability under controlled conditions**: Evaluation of the router's ability to handle large VRP sets, rapid VRP churn, and BGP updates influenced by ROV.  

* **Resource utilization**: Measurement of system CPU utilization, system memory consumption, and relevant control-plane process load associated with ROV processing.  

The goals of this document are:  

* To define a repeatable, controlled methodology for benchmarking ROV-enabled routers.

* To provide standardized metrics that allow for comparison across implementations.


# Terminology

The terminology used in this document follows the conventions of {{RFC1242}}, {{RFC2285}}, and subsequent BMWG publications. The following terms are used with specific meanings in the context of ROV benchmarking.

Route Origin Validation (ROV): A procedure defined in {{RFC6811}} that compares the origin AS of a BGP announcement with the set of authorized origins derived from validated ROA objects. ROV results in one of three states: Valid, Invalid, or NotFound.

Validated ROA Payload (VRP): The processed output from a relying party containing prefix-origin pairs that routers use for ROV decisions. VRPs are transported via the RTR protocol.

RPKI-to-Router Session: A protocol session between a router and an RPKI Cache Server. In benchmarking, RTR sessions may be emulated or generated using traffic/test tools to deliver synthetic VRP updates.

ROV Update Processing Latency: The time from when a router receives new VRP data (via RTR) until the updated ROV state is reflected in the router's local Routing Information Base (RIB) or implemented in routing decisions.

VRP-Triggered Revalidation Latency:
The time interval between completion of VRP installation and the moment all affected prefixes have updated validation states.

BGP-Triggered ROV Validation Latency:
The time interval between receipt of a BGP UPDATE message and completion of the ROV validation procedure for that route.

BGP Convergence Time: The time required for the router's control plane to process BGP updates and reach a stable routing state, while ROV validation is active.

Resource Utilization: System CPU utilization, system memory consumption, and, when observable, per-process utilization of the ROV process and the BGP or routing process while the router performs ROV-related tasks, including processing of VRP updates and applying ROV policy.

ROV Churn: A burst of VRP changes (e.g., many ROA additions or withdrawals) that may trigger significant revalidation and BGP recalculation, which is used in stress tests.

ROV Scalability Limit: The maximum number of VRPs, RTR sessions, or ROV-triggered BGP changes that the router can process while maintaining normal operational performance.


# Test Setup and Laboratory Environment

This section describes the required test topology, equipment, DUT configuration, RPKI data emulation, and traffic generation conditions. The goal of the test environment is to isolate the DUT and subject it to clearly defined RPKI-RTR and BGP tests, while providing accurate timing and state measurements.

## Test Topology

~~~~~~~~~~
+-------------------+    RTR    +----------------------+
|    RTR Emulator   |---------->|          DUT         |
|(RTR Update Source)|           |     (ROV Enabled)    |
+-------------------+           +----------------------+
                                 /\          /\
			       				  |           | Data-plane Traffic
                              BGP |  +-----------------+    
+---------------------+           |  |      Tester     |
|BGP Traffic Generator|-----------+  |(Data-plane Load)|
+---------------------+              +-----------------+
~~~~~~~~~~
{: #test-topo title="The test topology for ROV benchmarking."}

The test topology consists of four primary components: the DUT, an RPKI-RTR update source, a BGP traffic generator, and a tester for generating data-plane traffic load. The DUT is a router equipped with ROV capabilities, supporting the RPKI-RTR protocol and applying ROV policies to received BGP routes. The RPKI-RTR update source may be either a real RPKI cache implementation running in isolated mode or a dedicated emulator capable of producing arbitrary VRP sets and update patterns. This RTR source connects directly to the DUT using the RTR protocol and provides controlled VRP updates, including serial increments, cache resets, and bursty or delayed update sequences. 

The BGP traffic generator establishes one or more BGP peering sessions with the DUT and is responsible for delivering a full routing table together with controlled withdrawal or re-announcement events. Because IPv4 and IPv6 tables differ in scale and may exercise different implementation paths, the test setup must state the number of IPv4 routes and the number of IPv6 routes separately. A test MAY use a mixed baseline table (for example, 1,000,000 IPv4 routes and 250,000 IPv6 routes) or MAY benchmark each address family identifier (AFI) separately. In either case, the chosen route counts and AFIs under test must remain fixed across repeated runs for the same condition. The generator should be capable of presenting both stable baseline routing conditions and timed ROV-affected prefixes whose validation status will change in response to VRP updates.
A tester is connected to the DUT to introduce controlled data-plane load during benchmarking. When present, the tester should generate stable and deterministic traffic loads so that the impact of forwarding load on ROV processing can be evaluated. When data-plane load is applied, its rate, frame size, traffic pattern, and address-selection rules needs be documented in the test report.

## DUT Configuration Requirements

The DUT must be configured with ROV enabled on all BGP sessions receiving test routes. The router needs to establish a stable and fully functional RPKI-RTR session with the RTR emulator. To ensure that performance results are attributable solely to ROV behavior, all non-essential features on the DUT, such as additional routing protocols, unnecessary telemetry mechanisms, and unused services, should be disabled. Logging related to ROV may remain enabled for debugging purposes but can be rate-limited to avoid skewing CPU measurements or affecting test repeatability. All system parameters relevant to routing performance, such as multipath behavior or maximum-prefix limits, is suggested to be documented prior to testing.

## RTR Data Source Emulation

The RTR emulator is used to generate synthetic VRP data sets with user-defined characteristics. This includes the ability to create arbitrary combinations of prefixes and ASNs, overlapping VRPs, conflicting VRPs, and other edge cases relevant to validation logic. The VRP datasets should mimic realistic global distributions where appropriate, but can also support scaling tests where VRP volumes are substantially higher than today's norm. The data source needs to be able to further support generating controlled bursts of VRP updates, ranging from 100 to 10,000 VRP changes per second, and can allow for both additive updates and withdrawals.

## BGP Traffic Generation Requirements

The BGP traffic generator should be able to present the DUT with a stable baseline routing table prior to initiating any benchmark. This ensures that the DUT begins each test run in a known, converged state with predictable CPU and memory utilization. The generator can also provide a set of ROV-affected prefixes whose origin AS can be manipulated in concert with VRP updates from the RTR emulator. These prefixes should span a range of prefix lengths and originate from diverse ASes to reflect realistic routing conditions. The traffic generator can support deterministic convergence triggers, such as the precise injection of BGP updates following a VRP change or the simultaneous application of both BGP and VRP events.

## Traffic Profile Parameters

When data-plane traffic is used, the following parameters should be specified:

- Fixed frame size used for the measurement. For convergence measurements, a small fixed packet size SHOULD be used to improve time resolution. A 128-byte packet at Layer 3 is one practical choice. Other fixed sizes MAY be used when required by the traffic generator or encapsulation overhead, but the selected size needs to be documented.  
- Traffic rate in packets per second (PPS). For convergence measurements, the traffic should use a constant rate so that packet arrival times map directly to time resolution. Burst traffic MAY be used in stress scenarios, but such tests must be reported separately from constant-rate convergence tests.  
- Traffic pattern. For convergence measurements, constant-rate traffic is RECOMMENDED. 
- Source and destination IP address selection rules. When the setup sends one stream per tested prefix, the destination address for that stream should be selected from within the tested prefix. The first usable address is one valid example. The selection rule needs to be documented and applied consistently across runs.  
- Whether traffic matches ROV-affected prefixes.  

Each frame size and PPS combination should be reported separately.

# Benchmarking Methodology

This section describes the general methodology for benchmarking ROV behavior on a DUT. The goal is to ensure that all tests are repeatable, comparable across different environments, and representative of realistic deployment conditions. The methodology defines how to establish a controlled and stable test environment, how to specify and vary input conditions, and how to measure key performance metrics associated with ROV processing.

## General Considerations

Before any measurements are taken, the DUT must reach a well-defined steady state in which the RPKI-RTR session is fully established, the VRP set has been completely synchronized, and the BGP control plane has converged. A warm-up period is recommended to eliminate any cold-start effects that could bias measurement results.

All sources of measurement noise should be avoided. Features such as logging, real-time telemetry export, or periodic background tasks can interfere with timing-sensitive measurements; therefore, such features should be disabled or rate-limited during benchmarking. CPU clock scaling, thermal throttling, or other variable-performance modes should be minimized if the test setup allows it.

## Test Control and Input Conditions

Accurate benchmarking depends on precise control of the input conditions applied to the DUT. All tests should begin from a consistent baseline consisting of:

* A predefined ROA and VRP population, including the total number of ROAs and the total number of resulting VRPs. When synthetic ROAs are used to generate VRPs, the relationship between ROA count and VRP count must be stated.

* A stable and realistic baseline BGP RIB-in with IPv4 and IPv6 route counts documented separately (for example, 1,000,000 IPv4 routes and 250,000 IPv6 routes in a mixed table, or equivalent AFI-specific baselines).

* A defined coverage level of the VRP dataset relative to the announced routing space. Coverage SHOULD be reported as the percentage of announced prefixes, or the percentage of announced address space, for which at least one matching VRP exists. The selected coverage definition neds to be stated and applied consistently across runs.

* Defined structural properties of the VRP dataset, including overlap characteristics. At minimum, the test should specify the fraction or probability that a VRP is covered by another VRP, for example where a more-specific VRP falls within the prefix range of a less-specific VRP. Tests MAY additionally report the distribution of covering depth, prefix lengths, and authorized origin AS diversity when those properties are relevant to the implementation under test.

From this baseline, input variables may be modified to stress different aspects of ROV behavior. These variables include the VRP churn rate, ranging from steady incremental updates to high-intensity bursts, the coverage level of the VRP dataset, the overlap probability among VRPs, and the type of RPKI-RTR updates provided to the DUT, such as incremental updates versus full-table refreshes. Each of these conditions may trigger different processing strategies within the DUT, and therefore needs to be explicitly controlled and documented.

## Metrics and Measurements

Benchmarking ROV behavior requires collecting quantitative performance metrics that reflect how the DUT processes validation information and incorporates it into the BGP decision process. Therefore, this document proposes key performance metrics including ROV update processing latency, ROV validation latency, BGP convergence time, VRP storage size, system CPU and memory utilization, per-process utilization where available, and ROV state rebuild time.

**ROV update processing latency** measures the time from receipt of an RTR update (incremental or full) until the DUT has fully updated its internal validation state. This metric captures the efficiency of ROV data structures and algorithms.

**ROV validation latency** measures the time interval between a router's receipt of a BGP UPDATE message that contains a new or changed route, and the completion of the ROV procedure for that route, producing a validation state of Valid, Invalid, or NotFound. This metric isolates the internal validation step, excluding the larger BGP convergence process, and provides insight into the responsiveness of the DUT's validation engine.

**BGP convergence time** with ROV enabled measures how long the DUT takes to converge on BGP prefixes whose validation states change due to VRP updates. This reflects the real operational behavior of ROV as it interacts with the control plane.

The **VRP storage size** inside the DUT should also be recorded to evaluate the scalability of the implementation when operating with large VRP datasets. Alongside this, **resource utilization** SHOULD be monitored to identify performance limits or resource-intensive operations triggered by ROV. At minimum, the benchmark should collect system-level CPU utilization and system-level memory consumption. When the DUT exposes process-level counters, the benchmark should also collect utilization for the ROV process and for the BGP process or main routing process.

A recovery-related measurement, **ROV state rebuild time** after RTR session reset, quantifies the time needed for the DUT to re-establish a complete and correct ROV validation state after an RTR session reset or cache outage. This metric reflects robustness and recovery behavior under fault or restart scenarios.

Finally, the DUT should be evaluated under high-pressure scenarios by measuring its behavior when processing VRP bursts, such as surges of 100-10,000 VRPs per second. This measurement reveals whether the implementation can sustain abrupt workload increases without dropping updates, stalling, or entering unstable states.

# Benchmark Tests

This section defines the individual benchmark tests used to evaluate the performance and behavior of a DUT implementing ROV. Each test focuses on a specific aspect of the ROV processing pipeline, including VRP ingestion, validation, interaction with BGP, scalability limits, and robustness under stress and failure conditions. All tests assume the laboratory setup and input conditions described previously.

## ROV Update Processing Latency {#test-rov-update}

**Objective**: Measure the latency from the arrival of an RTR PDU until the new VRP information is installed in the DUT's internal ROV tables.

The **test procedures** for ROV update processing latency are listed below:

1. Prepare baseline state  

    - Establish RTR session between DUT and the RTR emulator.  

    - Preload DUT with a selected baseline VRP size (e.g., 100k VRPs).  

    - Ensure BGP is fully converged.  
	
2. Inject controlled RTR update  

    - From the emulator, send a controlled incremental update. The test must state whether the update affects a single VRP or a batch of multiple VRPs.  

    - For each changed VRP, document whether the operation is an addition, withdrawal, or modification.  

    - Document the expected validation-state change triggered by the update, such as Valid-to-Invalid due to origin-AS mismatch, Valid-to-Invalid due to prefix-length violation, Invalid-to-Valid, Valid-to-NotFound, Invalid-to-NotFound, or NotFound-to-Valid.  
    
    - Alternatively, for full-refresh tests, send a full VRP set replacement PDU sequence and document the number and type of VRP changes contained in the replacement.  

3. Timestamp PDU transmission  

    - Record the exact moment the first update PDU is sent.  

4. Monitor DUT internal state  

    - Use device instrumentation (API, CLI, or telemetry) to detect the exact moment the VRP table reflects the update.  
    
    - Confirm the VRP entry has been added, removed, or modified as expected.  

5. Calculate latency  

    - The latency is the time difference between the moment the RTR PDU is sent and the moment the VRP is applied on the DUT. For batch updates, the report should distinguish first-VRP installation latency, last-VRP installation latency, and aggregate batch processing time when the DUT exposes sufficient instrumentation.  

6. Repeat for multiple VRP table sizes  

    - E.g., 50k, 100k, 500k, and 1M VRPs.  

7. Repeat no less than 10 independent runs per condition  

    - Compute mean and standard deviation.  

## ROV Validation Latency {#test-rov-validation}

**Objective**: Measure how long the DUT takes to apply updated VRPs to the validation states of affected BGP prefixes.  

The **test procedures** for ROV validation latency are listed below:  

1. Establish baseline  

    - Load the baseline BGP table with IPv4 and IPv6 route counts stated explicitly (for example, 1,000,000 IPv4 routes and 250,000 IPv6 routes, or an AFI-specific baseline).  
    
    - Ensure all prefixes have a known baseline validation state.  

2. Select a controlled prefix set  

    - Pick a set of prefixes (e.g., 1,000) whose origin AS is tied to specific VRPs.  

    - Control and report the distribution of affected prefixes across the validation structure. At minimum, include one scenario in which affected prefixes are localized to the same region of the validation structure (for example, sharing a common covering prefix, trie branch, or address block) and one scenario in which affected prefixes are distributed across different regions.  

    - The locality rule, address-family mix, prefix-length distribution, and number of affected prefixes per region needs to be documented.  

3. Trigger validation update  

    - Modify VRPs so that these prefixes change validation state (Valid->Invalid or Invalid->Valid).  

    - The test must document whether each validation change is caused by an origin-AS mismatch, a prefix-length violation, loss of covering VRP, addition of a covering VRP, or another explicitly stated condition.  

4. Timestamp VRP installation completion  

    - As measured in {{test-rov-update}}.  

5. Monitor DUT validation table  

    - Continuously query validation state for selected prefixes.  
    
    - Note the timestamp when all prefixes reflect the updated state.  

6. Compute latency  

    - The validation latency is the time difference between the moment the VRP installation is completed and the moment all affected validation states have been updated.      

7. Repeat with varying set sizes  

    - E.g., 10 prefixes, 100 prefixes, 1,000 prefixes.  

## BGP Convergence with ROV Enabled

**Objective**: Measure BGP convergence time for routes impacted by ROV state changes, and compare ROV-triggered convergence with BGP-only convergence.

This test includes two cases. The first case measures convergence caused by VRP changes delivered through RTR, where the intent is to trigger ROV revalidation and then observe the resulting BGP behavior. The second case measures the propagation and convergence time of the same BGP routing event with ROV enabled and with ROV disabled.

### Case A: VRP-triggered BGP convergence

The **test procedures** for VRP-triggered BGP convergence are listed below:  

1. Prepare baseline  

    - Establish full-table BGP adjacency.  

    - Enable ROV on the DUT.  
    
    - Ensure stable initial convergence.  

2. Select test prefixes  

    - Choose prefixes that will transition validation state once VRP updates are applied, such as Valid-to-Invalid or Invalid-to-Valid.  

    - Document the cause of the validation change, such as origin-AS mismatch or prefix-length violation.  

3. Trigger VRP state change  

    - Send VRP modifications via RTR. The purpose of this step is to trigger convergence through VRP changes delivered by RTR and then observe the BGP control-plane behavior that follows from the changed validation state.  

4. Monitor BGP behavior  
 
    - Observe best-path selection changes.  
    
    - Timestamp withdrawal, replacement, or re-installation of affected prefixes.  

5. Measure convergence  

    - End-to-end convergence timer: This timer should start when the RTR emulator sends the first relevant VRP update and should end when both the BGP RIB and Forwarding Information Base (FIB) reach stable state. This metric captures RTR delivery, VRP installation, ROV revalidation, and the resulting BGP convergence.  

    - Post-installation BGP convergence timer: This timer should start when the DUT completes installation of the relevant VRP update and should end when both the BGP RIB and FIB reach stable state. This metric isolates BGP convergence after VRP installation.  

    - If only one timer is reported, the report needs to state which timer definition is used.  

6. Record:  

    - Time to withdraw or replace routes that become Invalid.  
    
    - Time to install or select routes that become Valid.  
    
    - Time until new best paths stabilize.

### Case B: BGP-triggered convergence with and without ROV

The **test procedures** for comparing BGP-triggered convergence are listed below:

1. Prepare baseline conditions

    - Prepare two otherwise identical baseline conditions: one with ROV enabled and one with ROV disabled.
    
    - Ensure that the baseline BGP table, VRP dataset, DUT configuration, data-plane traffic conditions, and measurement instrumentation are identical unless explicitly varied by the test.

2. Inject the BGP event

    - Apply the same BGP event in both conditions.
    
    - Example events include announcing a new route, withdrawing a route, changing an origin AS, or changing BGP attributes that affect best-path selection.

3. Control affected prefixes and distribution

    - Ensure that the route count, affected prefix set, prefix-length distribution, AFI mix, and structural distribution of affected prefixes are identical between the ROV-enabled and ROV-disabled runs.
    
    - If data-plane traffic is used, the traffic rate, frame size, destination-prefix mapping, and address-selection rules must also remain identical.

4. Measure convergence time

    - Measure the time from BGP event injection to the point at which both the BGP RIB and FIB reach stable state.
    
    - The timer start and timer end conditions must be identical for the ROV-enabled and ROV-disabled runs.

5. Record and compare results

    - Record the absolute convergence time for both runs.
    
    - Report the relative difference introduced by enabling ROV, including minimum, average, maximum, and at least P95 values across repeated runs.

## VRP Scalability Tests

**Objective**: Evaluate DUT performance with varying VRP table sizes.  

The **test procedures** for VRP scalability tests are listed below:  

1. Generate VRP datasets at sizes:  

    - E.g., 50k, 100k, 500k, 1M.  
    
2. Load each dataset into the RTR emulator.  

3. For each dataset, measure:

    - Full-table synchronization time.  
    
    - VRP update processing latency (from {{test-rov-update}}).  
    
    - ROV validation latency (from {{test-rov-validation}}).  
    
    - System memory consumption.  
    
    - System CPU utilization during sync and steady state.  
    
    - Per-process utilization for the ROV process and the BGP or routing process, when available.

4. Record failures  

    - Session drops  
    
    - Timeouts  
    
    - Missing VRPs  
    
    - ROV process crashes

5. Repeat 10 times per size for statistical stability.  

## VRP Churn and Stress Tests

**Objective**: Stress-test the DUT under rapid VRP changes to measure stability, performance, and correctness.

The **test procedures** for VRP churn and stress tests are listed below:  

1. Baseline setup  

    - Load a stable VRP table (e.g., 500k).  
    
    - Establish the baseline BGP table with IPv4 and IPv6 route counts stated explicitly.  
    
2. Generate controlled churn patterns  

    - Rapid add or remove spikes: 100-10,000 VRPs per second.  

    - Sustained churn: continuous modifications for 5-10 minutes.  

    - Mixed churn: adds, removes, and changes simultaneously.  

3. Measure DUT behavior  

    - VRP update backlog or queueing.  
    
    - ROV validation delays.  
    
    - System CPU spikes and, when available, spikes in the ROV process and the BGP or routing process.  
    
    - BGP convergence degradation.  
    
    - Missed or dropped VRP updates.  
    
4. Check correctness  

    - Verify that no stale or inconsistent ROV states remain.  
    
5. Record crash, stall, or throttling events.  

## Resource Utilization

**Objective**: Measure resource consumption under various ROV workloads.  

The **test procedures** for RTR session behavior tests are listed below:  

1. Establish monitoring tools  

    - System CPU sampling (100-500 ms interval).  
    
    - System memory usage tracking.  

    - Per-process sampling for the ROV process and the BGP or routing process, when available.  

    - Hardware counters if available.  

2. Measure under conditions  

    - Idle ROV.  

    - Full VRP sync.  

    - VRP churn.  
    
    - BGP convergence triggered by ROV events.  

3. Record  

    - System CPU load curves.  

    - Peak and steady-state system memory consumption.  

    - Process-level load curves for the ROV process and the BGP or routing process, when available.  
    
    - Any evidence of saturation (e.g., 100% CPU, memory exhaustion).  

4. Identify thresholds  

    - Points where performance degrades or ROV processing becomes unstable.

## RTR Session Behavior Tests

**Objective**: Evaluate robustness and recovery of DUT under RTR failure and failover scenarios, including both RTR session recovery and the subsequent BGP recalculation caused by restored or changed ROV state.

The **test procedures** for RTR session behavior tests are listed below:  

1. Session reset test  

    - Establish normal RTR session.  
    
    - Trigger forced session reset from emulator.  
    
    - Measure time to reestablish RTR session, ROV state rebuild time, and time until validation state becomes consistent again.  

    - After validation state consistency is restored, measure any resulting BGP recalculation and convergence using the post-installation BGP convergence timer defined in the BGP convergence test.  

2. Cache failover test  

    - Configure DUT with two RTR servers (primary + secondary).  
    
    - Terminate primary RTR connection.  
    
    - Measure failover time and data consistency after switch.  

    - Measure the time until affected BGP routes reach stable RIB and FIB state after the restored or failover VRP state is applied.  
    
3. Full resynchronization timing  

    - From emulator, force a full Reset Query sequence.  
    
    - Measure full VRP reload time.  

    - Measure subsequent ROV-triggered BGP convergence when the resynchronized VRP set changes validation states for installed BGP routes.  
    
    - Compare across different VRP scales.  

4. Incremental update performance

    - Send controlled incremental PDUs.  
    
    - Measure processing latency and correctness.  
    
    - Introduce occasional malformed or unexpected PDUs to test robustness.  

# Reporting Requirements 

An ROV benchmarking report must provide enough details to allow reproducibility and meaningful comparison across different DUTs. Each report needs to include the following elements:

* **Test environment description**: The report must specify the DUT hardware and software versions, the testbed topology, and all ROV-related configuration parameters required to replicate the setup.  

* **Input conditions**: The report must document the ROA count, VRP count, VRP coverage relative to the announced routing space, VRP overlap characteristics, the RIB-in size with IPv4 and IPv6 counts stated separately, the presence and rate of VRP churn, and whether RTR updates were incremental or full.  

* **Metrics and results**: Each measured metric must include its definition, a brief description of the measurement procedure, and results presented in tabular numerical form (including minimum, average, maximum, and at least P95 values). Graphs MAY be included for clarification.

* **Deviations and anomalies**: Any deviation from the expected behavior must be described, including the conditions under which it occurred and whether the test was repeated.  

* **Summary of observations**: The report must include a concise summary of overall DUT performance, scalability limits observed, and any significant effects of enabling ROV on BGP behavior. 

In addition, the report is suggested to include, at minimum, the following parameters:

- DUT hardware model, CPU architecture, memory size, and software version.  
- Complete DUT configuration relevant to ROV and BGP.  
- Testbed topology description.  
- Total number of ROAs and total number of resulting VRPs.  
- VRP table size.  
- VRP coverage relative to the announced routing space, including the definition used for coverage.  
- VRP overlap characteristics, including the probability or fraction that a VRP is covered by another VRP.  
- VRP churn rate.  
- For VRP update tests, whether each update affects a single VRP or multiple VRPs, and the operation type for each update.  
- For validation-change tests, the validation-change type, such as origin-AS mismatch, prefix-length violation, or loss/addition of a covering VRP.  
- For locality-sensitive tests, whether affected prefixes map to the same or different regions of the validation structure, and the locality rule used.  
- RIB-in size, with IPv4 and IPv6 route counts stated separately.   
- AFIs under test (IPv4, IPv6, or mixed).  
- Number of RTR sessions.  
- RTR timer configuration. 
- Presence and parameters of data-plane traffic (if used), including fixed frame size, PPS rate, traffic pattern, and address-selection rule.     
- ROV policy mode (e.g., reject Invalid).  
- System CPU sampling interval and any process-level sampling interval.  
- Measurement repetition count.  
- For BGP convergence tests, whether the reported timer is end-to-end from VRP update transmission, post-installation BGP convergence, or BGP-triggered convergence with ROV enabled/disabled.  

For each metric, the report must provide:

- Metric definition.  
- Measurement method.  
- Minimum, average, maximum, and at least P95 values.  
- Number of samples collected.  

# Security Considerations

This document defines a benchmarking methodology for evaluating ROV on routing devices. As such, it does not introduce new protocols, modify existing security mechanisms, or create new vulnerabilities within the RPKI system or BGP itself. All benchmarking activities are intended to take place in isolated laboratory environments. Nevertheless, a number of security considerations apply to the execution and interpretation of the tests described in this document.

Benchmarking ROV necessarily involves the generation, manipulation, and replay of RPKI objects. These test artifacts MUST NOT be injected into production RPKI repositories, production RPKI caches, or live BGP routing systems. Test-generated RPKI data sets SHOULD be clearly separated from real-world trust anchors, and laboratory RPKI caches SHOULD use isolated test Trust Anchors to prevent accidental propagation.

Similarly, BGP routing information used in the tests including simulated full tables, invalid prefixes, or artificially crafted origin-AS combinations, MUST NOT leak into production routing domains. All BGP sessions used for testing MUST be confined to a closed environment without external connectivity.

Tests involving stress conditions, such as high churn rates or large-scale VRP updates, may cause elevated CPU or memory consumption on the DUT. Operators performing such tests SHOULD ensure that the DUT is not simultaneously connected to any production network to avoid unintended service degradation.

# Implementation Status

This section summarizes the current implementation and experimental coverage of this methodology, following the reporting approach in {{RFC7942}}. It provides implementation experience for IETF discussion.

A prototype implementation of the benchmarking methodology described in this document supports the experiments reported in {{implementation-detail}}. It uses BIRD as the routing and ROV implementation, the Linux kernel for forwarding, a locally modified BRIO as the RTR source, and Bngblaster for BGP traffic generation.

Implemented:

- Incremental VRP updates, full-table resynchronization, and localized and distributed ROV validation tests.
- VRP-triggered BGP convergence and BGP-triggered convergence comparisons with ROV enabled and disabled.
- VRP scalability, churn and stress, and resource-utilization tests.
- RTR session recovery, Cache Reset resynchronization, and malformed-PDU handling tests.

Not yet implemented or incomplete:

- Comparison of localized and distributed ROV validation workloads with the same number of BGP routes undergoing ROV state changes.
- Evaluation on additional routing implementations and dedicated hardware routers; the reported experiments use a BIRD-based DUT under WSL2.
- Primary-to-secondary RTR cache failover measurements. In the current BIRD setup, both caches contribute authorizations concurrently, so loss of one session does not exercise the standby-cache activation required by this scenario.
- Hardware performance-counter measurements, which are unavailable in the current WSL2 environment.

# IANA Considerations

This document has no actions for IANA.

--- back

# Acknowledgements {#ack}
{: numbered="false"}

Many thanks to Giuseppe Fioccola, Christian Giese, and Maria Matejka for their review, comments, and suggestions.

# Appendix A. Summary of Changes {#changes}
{: numbered="false"}

This appendix summarizes the major changes introduced in previous revisions of this document.

## Appendix A.3. Changes from -02 to -03
{: numbered="false"}

Version -03 adds this appendix to summarize the major changes introduced across document revisions and improve revision traceability. 
No technical changes to the benchmarking methodology or benchmark procedures are introduced in this revision.

## Appendix A.2. Changes from -01 to -02
{: numbered="false"}

The major changes from version -01 to version -02 are as follows:

* Expanded the controlled input conditions to distinguish ROA and VRP populations and to characterize VRP coverage relative to the announced routing space and overlap properties within the VRP dataset.  
* Refined the ROV update processing latency test to distinguish single-VRP and batch updates, document addition, withdrawal, and modification operations and the expected validation-state transitions, and report first-VRP installation latency, last-VRP installation latency, and aggregate batch processing time when observable.  
* Refined the ROV validation latency test by introducing the structural distribution of affected prefixes as a controlled variable. The methodology now distinguishes localized and distributed prefix sets and requires the locality rule, AFI mix, prefix-length distribution, and cause of each validation-state change to be documented.  
* Reworked the BGP convergence benchmark into two cases: VRP-triggered BGP convergence and BGP-triggered convergence with and without ROV. The revision also introduced separate end-to-end and post-VRP-installation convergence timers and more explicit controls for comparing ROV-enabled and ROV-disabled runs.  
* Extended RTR session recovery, cache failover, and full resynchronization tests to include the subsequent BGP recalculation and convergence caused by restored or changed ROV state.  
* Expanded the reporting requirements to cover the newly introduced VRP dataset characteristics, update types, validation-change causes, locality conditions, and convergence timer definitions, and refined the use of normative requirement language throughout the methodology.  

## Appendix A.3. Changes from -00 to -01
{: numbered="false"}
The major changes from version -00 to version -01 are as follows:

* Refined the test topology and baseline BGP input conditions to distinguish IPv4 and IPv6 route counts and to support mixed or AFI-specific benchmarking under fixed and repeatable conditions.  
* Refined the data-plane traffic profile for convergence measurements by specifying a fixed frame size, PPS-based constant-rate traffic, separate treatment of burst traffic, and deterministic source and destination address selection rules.  
* Clarified resource utilization metrics and measurement procedures by distinguishing system-level CPU and memory consumption from per-process utilization of the ROV process and the BGP or routing process, when such counters are available.  
* Updated the scalability, churn, resource utilization, and reporting procedures to consistently document AFIs under test, data-plane traffic parameters, and system-level and process-level resource measurements.

# Appendix B. Implementation Details {#implementation-detail}
{: numbered="false"}

This appendix describes the application of the benchmark tests in {{benchmark-tests}} to a BIRD-based DUT. It presents the test environment, shared input datasets, measurement procedures, and results.

## Appendix B.1. Test Environment {#report-environment}
{: numbered="false"}

The Device Under Test (DUT) consists of BIRD for routing and ROV processing, together with the Linux kernel for packet forwarding. BRIO supplies Validated ROA Payloads (VRPs) over the RPKI-to-Router (RTR) protocol, with local modifications providing first-Prefix-PDU send timestamps and controlled client disconnection. Bngblaster establishes BGP sessions and advertises the baseline and update routes, with a local modification for IPv6 neighbor emulation. A separate ingress sends UDP probes through the DUT; receivers at the emulated BGP peers identify the forwarding next hop.

The test setup uses BRIO for direct control over incremental VRP additions and withdrawals in the VRP churn and stress test ({{vrp-churn-and-stress-tests}}). By comparison, widely used RTR tools, such as Routinator and StayRTR, derive incremental updates by comparing complete VRP sets internally. The additional processing delays transmission, making this approach unsuitable for continuous monitoring in this test.

~~~
+==========================+=================================================+
| Component                | Configuration                                   |
+==========================+=================================================+
| Operating environment    | Linux under WSL2; kernel                        |
|                          | `6.18.33.2-microsoft-standard-WSL2`             |
+--------------------------+-------------------------------------------------+
| CPU                      | Intel Core 5 220H                               |
+--------------------------+-------------------------------------------------+
| Memory available to WSL2 | 15.37 GiB                                       |
+--------------------------+-------------------------------------------------+
| Routing software         | BIRD 2.19.2                                     |
+--------------------------+-------------------------------------------------+
| BGP traffic generator    | Bngblaster 0.9.39, with local modifications     |
+--------------------------+-------------------------------------------------+
| RTR source               | BRIO b7.1.2, with local modifications           |
+--------------------------+-------------------------------------------------+
~~~

## Appendix B.2. BGP and VRP Data {#report-data}
{: numbered="false"}

**Initial BGP data.** Routes are sampled from the RIPE RIS RRC00 MRT snapshot of 9 September 2026 at 00:00 UTC. Sampling is performed separately for IPv4 and IPv6, with quotas allocated across peers, targeting 1,000,000 IPv4 and 250,000 IPv6 routes. Conversion preserves the original BGP path attributes. Two filters are applied:

- Routes whose NEXT_HOP differs from the advertising peer's address are excluded to avoid exceeding the interface capacity of the test setup when emulating additional next hops. The Bngblaster build supports at most 255 TCP-enabled interfaces.
- Routes with AS_PATH text too long to be displayed in full are excluded to prevent truncation in BIRD's CLI output and RIB change logs.

The resulting baseline contains 999,125 IPv4 and 222,006 IPv6 candidate routes from 99 peers, covering 805,052 distinct prefixes. Different peers' advertisements of the same prefix are retained as separate candidates. All experiments use this BGP baseline.

**Initial VRP data.** VRPs originate from the RPKIViews snapshot of 8 September 2026 at 23:47:23 UTC. AS0 records are removed, and additional authorizations are generated to bring the numbers of Invalid and NotFound BGP routes closer together. The resulting baseline contains 1,060,368 VRPs: 820,699 IPv4 and 239,669 IPv6. Experiments use either this complete baseline or sampled subsets.

The inputs comprise four datasets:

- **Single-batch VRP updates:** Nested initial sets of 50k, 100k, 500k, and 1M VRPs. Randomly select BGP candidates and change their ROV states until reaching 1,000 VRP operations.
- **Multi-batch VRP updates:** Apply the same generation method over 300 batches, starting with 500k VRPs and targeting 100, 500, or 1,000 operations per batch.
- **Prefix-selected VRP updates:** Select 100, 500, or 1,000 distinct prefixes and change a selected candidate's ROV state for each.
- **BGP updates:** Keep the complete VRP baseline fixed and generate 1,000 BGP operations per workload. Each operation is selected to change the best path in at least one of the configurations with ROV enabled or disabled.

A single ROV transition may require multiple VRP operations and affect additional routes. Consequently, a prefix-selected workload targeting 1,000 prefixes may require more than 1,000 VRP operations, and the resulting ROV state changes may extend to BGP routes for other prefixes.

The ROV validation latency test in {{test-rov-validation}} requires both localized and distributed scenarios. The localized scenario draws prefixes from as few IPv4 /8 or IPv6 /32 address regions as needed to meet the target count. The distributed scenario samples prefixes randomly from the entire eligible pool without restricting address regions.

The BGP update dataset includes mixed workloads and workloads containing a single operation type. The five operation types are:

- **Withdraw best:** Withdraw the existing best route.
- **Add Valid:** Announce a new route with a Valid ROV state that can become the best path.
- **Add Invalid:** Announce a new Invalid route that can become the best path.
- **Invalid to Valid:** Change an existing Invalid route to Valid so that it becomes the best path.
- **Best to Invalid:** Change the existing best route to Invalid.

## Appendix B.3. Measurement Methods {#report-measurement}
{: numbered="false"}

The measurements distinguish authorization installation, validation queries, routing decisions, and forwarding behavior.

~~~
+========================+=========================+=========================+
| Observable             | Collection method       | Observation time        |
+========================+=========================+=========================+
| VRP installation or    | BIRD VRP change logs    | Timestamp associated    |
| withdrawal             |                         | with the observed log   |
|                        |                         | event                   |
+------------------------+-------------------------+-------------------------+
| ROV state              | BIRD CLI queries for a  | Receipt of the query    |
|                        | prefix and origin ASN   | result                  |
+------------------------+-------------------------+-------------------------+
| RIB changes            | BIRD best-path change   | Timestamp associated    |
|                        | logs                    | with the observed log   |
|                        |                         | event                   |
+------------------------+-------------------------+-------------------------+
| FIB changes            | UDP probes sent through | Receipt of a probe, or  |
|                        | a separate ingress and  | expiry of its           |
|                        | received at candidate   | observation deadline    |
|                        | next hops               | where applicable        |
+------------------------+-------------------------+-------------------------+
~~~

The collector queries ROV states through BIRD's command-line interface over a persistent UNIX domain socket connection. It sends query commands in groups and receives all replies before submitting the next group. In the ordinary convergence experiments, a new observation is compared with the previous state, and only a state change creates an event.

FIB probes are sent continuously at the configured aggregate packet rate, without waiting for the preceding probe to arrive. In the ordinary convergence experiments, a probe with no reception within one second produces a no-reception observation. The observed forwarding state is compared with the previous state, and only a change creates a new event.

Some experiments measure short intervals that are particularly sensitive to pauses in the measurement process. The measurement software therefore disables Python's automatic garbage collection during timed collection in all experiments and restores its previous setting afterward.

## Appendix B.4. General Conventions {#report-conventions}
{: numbered="false"}

ROV queries use a default group size of eight commands. Convergence experiments use a ten-second quiet period and, where forwarding is monitored, an aggregate probe limit of 10,000 packets/s with a one-second packet deadline. Collection stops after the quiet period, with pending logs and events drained. In ordinary convergence experiments, expected-state matching and final correctness checks are performed after timed collection. The reported last-change endpoint excludes the subsequent quiet period. The VRP churn and stress test ({{vrp-churn-and-stress-tests}}) retains pending targets across subsequent batches and checks their completion periodically during collection. The RTR session behavior tests ({{rtr-session-behavior-tests}}) additionally require scenario-specific protocol events.

The default origin for transmission-based intervals is BRIO's monotonic timestamp immediately before the first successful Prefix PDU send call for the relevant response. Continuous-update VRP latency measurements in {{report-vrp-churn}} instead start at the corresponding Serial Notify send time: multiple batches may share one RTR response, preventing its first Prefix PDU from providing a separate origin for each batch. ROV, RIB, and FIB latencies in that experiment use a post-installation origin. Post-installation intervals and scenario-specific origins are defined in the corresponding experiments.

Latency values are expressed in milliseconds unless another unit is specified. Standard deviation is the sample standard deviation. P95 is the 95th percentile using linear interpolation between ordered samples. The sample count denotes repeated runs, different inputs, or observed batches as specified in each section. An em dash denotes an unavailable measurement; N/A denotes an inapplicable measurement.

## Appendix B.5. ROV Update Processing Latency {#report-vrp-update}
{: numbered="false"}

This experiment implements the update-processing test in {{test-rov-update}} using nested initial sets of 50k, 100k, 500k, and 1M VRPs, with 1,000 VRP operations at each scale. It measures installation latency for incremental and full-table updates. Each condition is repeated ten times. The results are as follows.

~~~
+============+=================+=================+=================+=================+=================+
|  Initial   |    Mean (ms)    |  Std. dev. (ms) |   Minimum (ms)  |   Maximum (ms)  |     P95 (ms)    |
|    VRPs    |                 |                 |                 |                 |                 |
+------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|            | Incr.  |  Full  | Incr.  |  Full  | Incr.  |  Full  | Incr.  |  Full  | Incr.  |  Full  |
+============+========+========+========+========+========+========+========+========+========+========+
|        50k | 47.489 |160.076 |  2.434 |  3.513 | 45.186 |154.845 | 51.302 |164.765 | 51.170 |164.759 |
+------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|       100k | 46.460 |319.486 |  3.047 |  7.385 | 43.172 |305.609 | 51.389 |331.359 | 51.323 |329.472 |
+------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|       500k | 42.628 |1631.664|  2.713 | 16.436 | 38.670 |1598.010| 47.795 |1659.967| 47.325 |1653.157|
+------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|         1M | 38.704 |3366.851|  3.245 | 54.778 | 32.703 |3299.205| 44.141 |3447.183| 43.178 |3444.970|
+------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
~~~

Incr. denotes incremental delivery; Full denotes full-table resynchronization. Incremental update latency is short and similar across the tested table sizes, averaging approximately 40–50 ms. Full-table resynchronization latency increases as the VRP table grows.

## Appendix B.6. ROV Validation Latency {#report-rov-validation}
{: numbered="false"}

This experiment implements the validation-latency test in {{test-rov-validation}} using 1,060,368 initial VRPs and updates targeting 100, 500, or 1,000 selected prefixes in localized and distributed groups. With BIRD's ROA-table trie enabled, it measures validation latency from VRP installation completion. The results are as follows.

~~~
+============+=====================+=====================+=====================+=====================+=====================+
|  Selected  |      Mean (ms)      |    Std. dev. (ms)   |     Minimum (ms)    |     Maximum (ms)    |       P95 (ms)      |
|  prefixes  |                     |                     |                     |                     |                     |
+------------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
|            |  Local.  | Distrib. |  Local.  | Distrib. |  Local.  | Distrib. |  Local.  | Distrib. |  Local.  | Distrib. |
+============+==========+==========+==========+==========+==========+==========+==========+==========+==========+==========+
|        100 |   71.847 |  104.377 |    5.244 |   17.235 |   66.315 |   87.147 |   79.560 |  141.479 |   78.635 |  134.556 |
+------------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
|        500 |  133.143 |  622.376 |   11.623 |   38.711 |  118.246 |  565.663 |  157.540 |  705.357 |  151.338 |  681.772 |
+------------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
|      1,000 |  174.169 | 2119.743 |   33.637 |  961.166 |  144.346 |  906.791 |  256.348 | 3934.232 |  231.466 | 3501.198 |
+------------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+
~~~

Local. denotes localized inputs; Distrib. denotes distributed inputs. ROV validation latency increases with the number of selected prefixes in both scenarios. For the same selected prefix count, localized workloads show lower observed latency. However, selecting the same number of prefixes does not mean that the same number of prefixes is actually affected. For example, both workloads select 1,000 prefixes, but ROV states change for 3,640 prefixes in the localized scenario and 23,777 prefixes in the distributed scenario. The longer latency in the distributed scenario is therefore associated with both a different prefix distribution and more prefixes to process.

## Appendix B.7. BGP Convergence with ROV Enabled {#report-bgp-convergence}
{: numbered="false"}

The following experiments correspond to the two cases in {{bgp-convergence-with-rov-enabled}}.

### Appendix B.7.1. Case A: VRP-triggered BGP convergence {#report-vrp-bgp-convergence}
{: numbered="false"}

This experiment implements the test in {{case-a-vrp-triggered-bgp-convergence}} using 1,060,368 initial VRPs and the distributed workload with 1,000 selected prefixes, requiring 1,230 VRP operations. It monitors RIB and FIB changes and reports end-to-end and post-installation convergence times below.

~~~
+===================+==========+=========+==========+==========+==========+
| Timer             | Mean     | Std.    | Minimum  | Maximum  | P95 (ms) |
|                   | (ms)     | dev.    | (ms)     | (ms)     |          |
|                   |          | (ms)    |          |          |          |
+===================+==========+=========+==========+==========+==========+
| End-to-end        | 5027.552 | 267.088 | 4826.382 | 5736.282 | 5469.557 |
| convergence       |          |         |          |          |          |
+-------------------+----------+---------+----------+----------+----------+
| Post-installation | 4813.556 | 363.372 | 4540.745 | 5703.936 | 5432.527 |
| BGP convergence   |          |         |          |          |          |
+-------------------+----------+---------+----------+----------+----------+
~~~

The final FIB change determines the convergence endpoint in all ten runs. Mean end-to-end convergence is 5.03 s, of which 4.81 s occurs after VRP installation. Thus, most of the measured convergence interval follows installation, with FIB convergence taking longer than RIB convergence.

### Appendix B.7.2. Case B: BGP-triggered convergence with and without ROV {#report-bgp-update-convergence}
{: numbered="false"}

This experiment implements the test in {{case-b-bgp-triggered-convergence-with-and-without-rov}} with a fixed set of 1,060,368 VRPs and 1,000 BGP operations per workload. One mixed workload and five individual operation-type workloads are replayed with ROV enabled and disabled. RIB and FIB convergence is measured from the first BGP UPDATE send to the last observed change. The results are as follows.

**Mixed.**

~~~
+============+===========+==============+==============+==========+
| ROV policy | Mean (ms) | Minimum (ms) | Maximum (ms) | P95 (ms) |
+============+===========+==============+==============+==========+
| Enabled    | 10591.995 |    10532.102 |    10664.151 | 10662.097|
+------------+-----------+--------------+--------------+----------+
| Disabled   | 10580.443 |    10539.504 |    10648.550 | 10632.916|
+------------+-----------+--------------+--------------+----------+
~~~

**Withdraw best.**

~~~
+============+===========+==============+==============+==========+
| ROV policy | Mean (ms) | Minimum (ms) | Maximum (ms) | P95 (ms) |
+============+===========+==============+==============+==========+
| Enabled    |  9791.912 |     9168.918 |    11690.974 | 11275.438|
+------------+-----------+--------------+--------------+----------+
| Disabled   |  9599.031 |     9066.639 |    10978.189 | 10714.374|
+------------+-----------+--------------+--------------+----------+
~~~

**Add Valid.**

~~~
+============+===========+==============+==============+==========+
| ROV policy | Mean (ms) | Minimum (ms) | Maximum (ms) | P95 (ms) |
+============+===========+==============+==============+==========+
| Enabled    | 10158.053 |     9685.791 |    10586.965 | 10506.822|
+------------+-----------+--------------+--------------+----------+
| Disabled   | 10361.078 |    10173.760 |    11069.728 | 10893.730|
+------------+-----------+--------------+--------------+----------+
~~~

**Add Invalid.**

~~~
+============+===========+==============+==============+==========+
| ROV policy | Mean (ms) | Minimum (ms) | Maximum (ms) | P95 (ms) |
+============+===========+==============+==============+==========+
| Enabled    |       N/A |          N/A |          N/A |      N/A |
+------------+-----------+--------------+--------------+----------+
| Disabled   |  9962.983 |     9685.891 |    10177.751 | 10176.955|
+------------+-----------+--------------+--------------+----------+
~~~

**Invalid to Valid.**

~~~
+============+===========+==============+==============+==========+
| ROV policy | Mean (ms) | Minimum (ms) | Maximum (ms) | P95 (ms) |
+============+===========+==============+==============+==========+
| Enabled    |  8686.659 |     8111.456 |     9856.441 | 9621.860 |
+------------+-----------+--------------+--------------+----------+
| Disabled   |  8011.848 |     8005.596 |     8031.975 | 8027.183 |
+------------+-----------+--------------+--------------+----------+
~~~

**Best to Invalid.**

~~~
+============+===========+==============+==============+==========+
| ROV policy | Mean (ms) | Minimum (ms) | Maximum (ms) | P95 (ms) |
+============+===========+==============+==============+==========+
| Enabled    |  9298.554 |     9105.652 |     9568.952 | 9558.411 |
+------------+-----------+--------------+--------------+----------+
| Disabled   |  8385.576 |     8006.559 |     9898.653 | 9520.511 |
+------------+-----------+--------------+--------------+----------+
~~~

The mixed workload produces similar convergence times with ROV enabled and disabled. For Withdraw best and Add Valid, the operations have similar effects on route selection in both configurations, and the measured convergence times are also similar. For Add Invalid, ROV rejects the added routes, so no RIB or forwarding changes are observed with ROV enabled and no last-change latency is reported. The larger differences occur for Invalid to Valid and Best to Invalid: the validation-state transitions do not change route eligibility with ROV disabled, whereas they require route reselection with ROV enabled. These two workloads therefore show larger differences in convergence time between the configurations.

## Appendix B.8. VRP Scalability Tests {#report-vrp-scalability}
{: numbered="false"}

This experiment implements the scalability test in {{vrp-scalability-tests}} using 50k, 100k, 500k, and 1M initial VRPs, with 1,000 VRP operations at each scale. It measures initial synchronization, VRP update latency, validation latency after VRP installation, and BIRD CPU and memory consumption. Each condition is repeated ten times. The results are as follows.

~~~
+==========+=================+==========+============+===========+===========+
| Initial  | Full-table      | VRP      | ROV        | BIRD CPU  | BIRD RSS  |
| VRPs     | synchronization | update   | validation | (%)       | (MiB)     |
|          | (ms)            | latency  | latency    |           |           |
|          |                 | (ms)     | (ms)       |           |           |
+==========+=================+==========+============+===========+===========+
| 50k      |          17.104 |   48.578 |    206.269 |    38.366 |   462.971 |
+----------+-----------------+----------+------------+-----------+-----------+
| 100k     |          32.779 |   49.258 |    340.882 |    58.298 |   466.205 |
+----------+-----------------+----------+------------+-----------+-----------+
| 500k     |         188.375 |   44.607 |    789.480 |    66.257 |   501.640 |
+----------+-----------------+----------+------------+-----------+-----------+
| 1M       |         422.649 |   41.032 |   3099.493 |    70.451 |   562.093 |
+----------+-----------------+----------+------------+-----------+-----------+
~~~

The CPU column reports BIRD utilization during initial synchronization, with one fully occupied logical CPU equal to 100%. Memory is BIRD's steady-state resident set size (RSS). Values are reported as means across runs. In this test setup, ROV and BGP processing share a single BIRD process, so their resource usage cannot be measured separately and is reported for BIRD as a whole.

Initial synchronization and post-installation ROV validation take longer as the VRP table grows. Mean incremental VRP update latency ranges from 41.032 to 49.258 ms, similar to the results of the update-processing test ({{report-vrp-update}}). Mean BIRD CPU utilization during synchronization rises from 38.4% to 70.5%, while steady-state RSS increases from 463 MiB to 562 MiB. RSS varies little across repeated runs at each scale.

## Appendix B.9. VRP Churn and Stress Tests {#report-vrp-churn}
{: numbered="false"}

This experiment implements the churn test in {{vrp-churn-and-stress-tests}} with 500k initial VRPs and 300 batches at one-second intervals, targeting 100, 500, or 1,000 VRP operations per batch. It measures VRP, ROV, RIB, and FIB latency under continuous updates. Only targets predicted to change once over the complete update sequence are monitored. Delivery delays and aggregation of multiple batches into one RTR response can combine or cancel repeated operations. Within the DUT, subsequent updates may also arrive before earlier changes have propagated to routing and forwarding, so intermediate states may produce neither a RIB change log nor a change in the forwarding next hop observed by probes. Targets with repeated changes are therefore excluded from measurement; their updates remain in the submitted workload.

In practice, continuous updates substantially affected the measurement process. Large numbers of ROV and FIB targets increased the query and probe workload, while high load on the DUT delayed CLI responses and could also delay packet forwarding. BIRD's ROA-table settle timers also defer route revalidation after VRP updates, adding a waiting period before routing and forwarding changes. The measurement procedure therefore includes the following measures to reduce these effects:

- Monitor selected batches rather than every batch: ROV, RIB, and FIB targets are selected from batches 10, 20, and every subsequent tenth batch through batch 300. VRP logs are collected for all batches.
- Measure ROV and FIB in separate runs to reduce interference between CLI queries and forwarding probes.
- Set ROA-table settle timers to zero to remove this additional waiting period.
- Check completion every five seconds and remove completed ROV/FIB targets to avoid continuing queries and probes for those targets.

Monitoring continues across batch boundaries until the expected state is observed. For each component, the batch endpoint is the latest of the selected targets' first observations of their expected resulting states. A complete batch latency requires all selected targets to be observed. Collection ends after all updates have been submitted and all selected targets have completed, or when the overall timeout expires; final correctness checks follow collection.

VRP latency starts at the Serial Notify send time because multiple batches may be combined into one RTR response, whose first Prefix PDU cannot provide a separate timing origin for each batch. ROV, RIB, and FIB post-installation latencies start at the last observed VRP change for the corresponding batch. All 30 selected batches at each size have complete ROV, RIB, and FIB matches.

VRP latency:

~~~
+============+=============+=============+=============+=============+=============+
|   Target   |     Mean    |  Std. dev.  |   Minimum   |   Maximum   |     P95     |
| operations |     (ms)    |     (ms)    |     (ms)    |     (ms)    |     (ms)    |
| per batch  |             |             |             |             |             |
+============+=============+=============+=============+=============+=============+
|    100     |    69.234   |   121.791   |    41.754   |   1856.278  |    61.636   |
+------------+-------------+-------------+-------------+-------------+-------------+
|    500     |    74.930   |   172.700   |    42.722   |   2768.577  |    66.831   |
+------------+-------------+-------------+-------------+-------------+-------------+
|   1,000    |    78.477   |   138.877   |    42.920   |   1901.677  |    76.311   |
+------------+-------------+-------------+-------------+-------------+-------------+
~~~

Post-installation ROV latency:

~~~
+============+=============+=============+=============+=============+=============+
|   Target   |     Mean    |  Std. dev.  |   Minimum   |   Maximum   |     P95     |
| operations |     (ms)    |     (ms)    |     (ms)    |     (ms)    |     (ms)    |
| per batch  |             |             |             |             |             |
+============+=============+=============+=============+=============+=============+
|    100     |   331.377   |   260.100   |   103.695   |   1255.976  |   868.944   |
+------------+-------------+-------------+-------------+-------------+-------------+
|    500     |   714.867   |   320.528   |   464.179   |   2040.389  |   1171.388  |
+------------+-------------+-------------+-------------+-------------+-------------+
|   1,000    |   1815.063  |   1103.538  |   686.447   |   4833.987  |   4185.087  |
+------------+-------------+-------------+-------------+-------------+-------------+
~~~

Post-installation RIB latency:

~~~
+============+=============+=============+=============+=============+=============+
|   Target   |     Mean    |  Std. dev.  |   Minimum   |   Maximum   |     P95     |
| operations |     (ms)    |     (ms)    |     (ms)    |     (ms)    |     (ms)    |
| per batch  |             |             |             |             |             |
+============+=============+=============+=============+=============+=============+
|    100     |   2497.838  |   249.244   |   1683.396  |   3129.184  |   2838.429  |
+------------+-------------+-------------+-------------+-------------+-------------+
|    500     |   3538.866  |   578.829   |   2398.800  |   6240.222  |   3848.731  |
+------------+-------------+-------------+-------------+-------------+-------------+
|   1,000    |   3412.681  |   518.784   |   2099.247  |   4159.981  |   4117.594  |
+------------+-------------+-------------+-------------+-------------+-------------+
~~~

Post-installation FIB latency:

~~~
+============+=============+=============+=============+=============+=============+
|   Target   |     Mean    |  Std. dev.  |   Minimum   |   Maximum   |     P95     |
| operations |     (ms)    |     (ms)    |     (ms)    |     (ms)    |     (ms)    |
| per batch  |             |             |             |             |             |
+============+=============+=============+=============+=============+=============+
|    100     |   3464.875  |   259.310   |   2677.374  |   4132.012  |   3822.033  |
+------------+-------------+-------------+-------------+-------------+-------------+
|    500     |   4552.836  |   620.005   |   3435.884  |   7504.002  |   4888.761  |
+------------+-------------+-------------+-------------+-------------+-------------+
|   1,000    |   4638.344  |   510.229   |   3190.433  |   5435.035  |   5357.773  |
+------------+-------------+-------------+-------------+-------------+-------------+
~~~

Additional single-batch tests measure VRP installation, ROV validation, and BGP convergence latency. Each test initializes the corresponding pre-update state, applies one batch, and measures VRP, ROV, RIB, and FIB latency. ROV and FIB are measured in separate runs, consistent with the continuous-update tests.

~~~
+============+===================+===================+===================+===================+
| Operations |    VRP latency    | Post-installation | Post-installation | Post-installation |
| per batch  |                   |    ROV latency    |    RIB latency    |    FIB latency    |
|            |     Mean (ms)     |     Mean (ms)     |     Mean (ms)     |     Mean (ms)     |
+------------+---------+---------+---------+---------+---------+---------+---------+---------+
|            |  Cont.  |  Single |  Cont.  |  Single |  Cont.  |  Single |  Cont.  |  Single |
+============+=========+=========+=========+=========+=========+=========+=========+=========+
|    100     |  69.234 |  44.654 | 331.377 |  20.659 | 2497.838| 1839.835| 3464.875| 2834.748|
+------------+---------+---------+---------+---------+---------+---------+---------+---------+
|    500     |  74.930 |  45.496 | 714.867 |  48.868 | 3538.866| 2202.871| 4552.836| 3254.135|
+------------+---------+---------+---------+---------+---------+---------+---------+---------+
|   1,000    |  78.477 |  46.229 | 1815.063|  63.922 | 3412.681| 2110.765| 4638.344| 3174.544|
+------------+---------+---------+---------+---------+---------+---------+---------+---------+
~~~

The results show higher VRP, ROV, RIB, and FIB latencies under continuous updates than under single-batch updates. However, the measurement process itself also experienced delays during continuous updates. A monitoring round queries or probes all current targets once. The following table compares the average duration and target count of these rounds between continuous and single-batch tests. The averages use the first complete round after each selected batch adds its targets, including any unfinished targets from earlier batches.

~~~
+============+==============+=============+==============+=============+==============+
| Operations | Update mode  |  ROV round  | ROV targets  |  FIB round  | FIB targets  |
| per batch  |              |  time (ms)  |  per round   |  time (ms)  |  per round   |
+============+==============+=============+==============+=============+==============+
|    100     |  Continuous  |   318.845   |   321.933    |    37.509   |   162.300    |
+------------+--------------+-------------+--------------+-------------+--------------+
|    100     | Single-batch |    24.745   |   321.933    |    31.968   |   162.300    |
+------------+--------------+-------------+--------------+-------------+--------------+
|    500     |  Continuous  |   803.977   |   608.267    |    90.170   |   370.533    |
+------------+--------------+-------------+--------------+-------------+--------------+
|    500     | Single-batch |    46.149   |   608.267    |    71.983   |   367.367    |
+------------+--------------+-------------+--------------+-------------+--------------+
|   1,000    |  Continuous  |   2338.591  |   1796.900   |   370.557   |   1046.067   |
+------------+--------------+-------------+--------------+-------------+--------------+
|   1,000    | Single-batch |    60.419   |   741.667    |    93.249   |   494.233    |
+------------+--------------+-------------+--------------+-------------+--------------+
~~~

At 100 and 500 operations per batch, each ROV query round covers the same number of targets in continuous and single-batch tests, but takes substantially longer in the continuous tests. At 1,000 operations per batch, unfinished targets from earlier batches remain under monitoring alongside new targets; both ROV and FIB rounds cover more targets and take longer than in the single-batch tests. These results show higher observed latency under continuous updates, but the increase includes delays from the measurement process, preventing separate quantification of the increase in DUT processing latency.

Update delivery is also affected. The following table shows how the interval between Serial Notify transmissions changes as the workload progresses.

~~~
+==============+=================================================================+
|  Operations  |           Mean Serial Notify transmission interval (s)          |
|  per batch   |                                                                 |
+--------------+---------------------+---------------------+---------------------+
|              |    Batches 1–100    |   Batches 100–200   |   Batches 200–300   |
+==============+=====================+=====================+=====================+
|     100      |        1.000        |        1.000        |        1.000        |
+--------------+---------------------+---------------------+---------------------+
|     500      |        1.000        |        1.001        |        1.001        |
+--------------+---------------------+---------------------+---------------------+
|    1,000     |        1.008        |        1.277        |        2.087        |
+--------------+---------------------+---------------------+---------------------+
~~~

Notify transmission remains close to the configured one-second interval at 100 and 500 operations per batch. At 1,000 operations per batch, the interval grows as the test progresses, exceeding two seconds on average in the final portion. The RTR source therefore falls behind the submission schedule at the highest load, alongside the slowdown in state observation.

## Appendix B.10. Resource Utilization {#report-resource-utilization}
{: numbered="false"}

This experiment implements the resource-utilization test in {{resource-utilization}} using the multi-batch VRP update dataset. It replays the first 60 batches of the workload targeting 100 VRP operations per batch, with batches submitted at one-second intervals. System and BIRD CPU and memory are measured during initial synchronization, idle operation, and updates with convergence. The results are as follows.

~~~
+==================================+=====================+===================+
| Phase                            | Mean system CPU (%) | Mean BIRD CPU (%) |
+==================================+=====================+===================+
| Initial full-table               |             205.497 |            66.479 |
| synchronization                  |                     |                   |
+----------------------------------+---------------------+-------------------+
| Idle steady state                |              31.161 |             1.541 |
+----------------------------------+---------------------+-------------------+
| Updates and convergence          |             228.661 |            73.150 |
+----------------------------------+---------------------+-------------------+
~~~

~~~
+=================+=============+==============+==============+==============+
| Phase           | Mean system | Mean system  | Mean BIRD    | Mean BIRD    |
|                 | used memory | sampled peak | RSS (MiB)    | RSS sampled  |
|                 | (MiB)       | (MiB)        |              | peak (MiB)   |
+=================+=============+==============+==============+==============+
| Initial         |    4057.007 |     4107.663 |       29.991 |       57.610 |
| full-table      |             |              |              |              |
| synchronization |             |              |              |              |
+-----------------+-------------+--------------+--------------+--------------+
| Idle steady     |    6837.546 |     6869.547 |      501.815 |      501.926 |
| state           |             |              |              |              |
+-----------------+-------------+--------------+--------------+--------------+
| Updates and     |    7531.922 |     7633.124 |      503.106 |      503.501 |
| convergence     |             |              |              |              |
+-----------------+-------------+--------------+--------------+--------------+
~~~


Both CPU columns use one fully occupied logical CPU as 100%; system CPU can exceed 100%. The tables report means over runs, and the memory peak column is the mean of each run's sampled phase peak. Hardware performance counters are not included in this WSL2 campaign. In this test setup, ROV and BGP processing share a single BIRD process, so their resource usage cannot be measured separately and is reported for BIRD as a whole.

BIRD CPU utilization is low during the idle phase and rises during synchronization and continuous updates. Mean utilization reaches 73.2% during updates and convergence, compared with 1.5% while idle. BIRD RSS remains close to 502–503 MiB between the idle and update phases, while system memory consumption increases.

## Appendix B.11. RTR Session Behavior Tests {#report-rtr-session}
{: numbered="false"}

These experiments implement Scenarios 1, 3, and 4 of {{rtr-session-behavior-tests}}. Scenarios 1 and 4 use 500k initial VRPs; Scenario 3 uses 50k, 100k, 500k, and 1M. Each input contains 1,000 VRP operations. Measurements cover RTR recovery, full resynchronization, malformed-update handling, and the resulting ROV, RIB, and FIB changes. Scenario 2 is not performed, as explained below.

~~~
+======================+==========================+========================+
| Scenario             | Trigger                  | Timer origin           |
+======================+==========================+========================+
| 1. Session reset and | Close active RTR         | BRIO disconnect        |
| recovery             | connection               | completion             |
+----------------------+--------------------------+------------------------+
| 3. Full              | Apply updates and send   | Cache Reset send time  |
| resynchronization    | Cache Reset              |                        |
+----------------------+--------------------------+------------------------+
| 4. Normal/malformed  | Replay updates; replace  | First Prefix PDU send  |
| incremental updates  | every tenth Prefix PDU   | time (relay to DUT)    |
|                      | with type 255 for errors |                        |
+----------------------+--------------------------+------------------------+
~~~

**Scenario 1: Session reset test.**

~~~
+=========+============+==========+==============+============+===================+
| Initial | New        | RTR      | VRP          | ROV        | Post-installation |
| VRPs    | connection | recovery | installation | completion | RIB/FIB           |
|         | (ms)       | (ms)     | (ms)         | (ms)       | convergence (ms)  |
+=========+============+==========+==============+============+===================+
| 500k    |   1000.291 | 15568.829|    15588.969 |  17378.615 |          5692.486 |
+---------+------------+----------+--------------+------------+-------------------+
~~~

The first four latency columns start at disconnect completion. New connection ends when BRIO registers BIRD's new TCP connection; RTR recovery ends when BIRD's RTR session is observed to return to the Established state. The last column starts at VRP installation completion. A new connection is established after approximately 1.00 s, while RTR recovery and VRP installation complete after approximately 15.57 s and 15.59 s. ROV completion follows at 17.38 s, and the post-installation RIB/FIB convergence interval averages 5.69 s.

**Scenario 2: Cache failover test.**

The cache failover test in {{rtr-session-behavior-tests}} requires a switch from a primary RTR cache to a secondary cache. In this BIRD setup, both RTR sessions are active and contribute authorizations to the shared IPv4 and IPv6 ROA tables (`r4` and `r6`). The secondary cache's authorizations are already available before the primary connection fails. Disconnecting the primary therefore does not trigger activation of a standby cache or an identifiable primary-to-secondary switchover. Measuring continued service from the remaining cache would not measure the failover required by this scenario, so no failover latency is reported.

**Scenario 3: Full resynchronization timing.**

~~~
+==========+==========+=============+===================+=============+
| Initial  | Full VRP | End-to-end  | Post-installation | Final-state |
| VRPs     | reload   | RIB/FIB     | RIB/FIB           | consistency |
|          | (ms)     | convergence | convergence (ms)  |             |
|          |          | (ms)        |                   |             |
+==========+==========+=============+===================+=============+
| 50k      |  160.380 |    4394.699 |          4234.319 |       10/10 |
+----------+----------+-------------+-------------------+-------------+
| 100k     |  313.001 |    4986.441 |          4673.440 |       10/10 |
+----------+----------+-------------+-------------------+-------------+
| 500k     | 1605.430 |    6794.705 |          5189.275 |       10/10 |
+----------+----------+-------------+-------------------+-------------+
| 1M       | 3472.341 |    8575.873 |          5103.532 |       10/10 |
+----------+----------+-------------+-------------------+-------------+
~~~

Mean full VRP reload time increases from 160.380 ms at 50k VRPs to 3472.341 ms at 1M, similar to the full-table resynchronization results of the update-processing test ({{report-vrp-update}}). Mean end-to-end RIB/FIB convergence increases from 4.395 s to 8.576 s, while mean post-installation convergence ranges from 4.234 s to 5.189 s. Final-state consistency checks pass in all ten runs at every scale.

**Scenario 4: Incremental update performance.**

~~~
+=============================+================+=============================+
| Metric                      | Normal updates | Updates containing          |
|                             |                | malformed PDUs              |
+=============================+================+=============================+
| VRP installation latency    |        126.986 |                         N/A |
| (ms)                        |                |                             |
+-----------------------------+----------------+-----------------------------+
| ROV completion latency (ms) |       1586.566 |                         N/A |
+-----------------------------+----------------+-----------------------------+
| RIB/FIB convergence latency |       6269.012 |                         N/A |
| (ms)                        |                |                             |
+-----------------------------+----------------+-----------------------------+
| Error Report received       |            N/A |                       10/10 |
+-----------------------------+----------------+-----------------------------+
| RTR connection closed after |            N/A |                       10/10 |
| error                       |                |                             |
+-----------------------------+----------------+-----------------------------+
| Authorization cleanup       |            N/A |                       10/10 |
| completed                   |                |                             |
+-----------------------------+----------------+-----------------------------+
| Final VRP table correct     |          10/10 |                       10/10 |
+-----------------------------+----------------+-----------------------------+
| Final ROV states correct    |          10/10 |                       10/10 |
+-----------------------------+----------------+-----------------------------+
~~~

Normal updates complete VRP installation, ROV validation, and RIB/FIB convergence with mean latencies of 126.986 ms, 1.587 s, and 6.269 s, respectively. In all ten malformed-update runs, the DUT sends an Error Report, closes the RTR connection, and completes authorization cleanup. Final VRP and ROV checks pass for both groups.
