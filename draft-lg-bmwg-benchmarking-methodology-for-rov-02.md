---
stand_alone: true
ipr: trust200902
cat: info # Check
submissiontype: IETF
area: Operations and Management
wg: BMWG

docname: draft-lg-bmwg-benchmarking-methodology-for-rov-02

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
A tester is connected to the DUT to introduce controlled data-plane load during benchmarking. When present, the tester should generate stable and deterministic traffic loads so that the impact of forwarding load on ROV processing can be evaluated. When data-plane load is applied, its rate, frame size, traffic pattern, and address-selection rules must be documented in the test report.

## DUT Configuration Requirements

The DUT must be configured with ROV enabled on all BGP sessions receiving test routes. The router must establish a stable and fully functional RPKI-RTR session with the RTR emulator. To ensure that performance results are attributable solely to ROV behavior, all non-essential features on the DUT, such as additional routing protocols, unnecessary telemetry mechanisms, and unused services, should be disabled. Logging related to ROV may remain enabled for debugging purposes but must be rate-limited to avoid skewing CPU measurements or affecting test repeatability. All system parameters relevant to routing performance, such as multipath behavior or maximum-prefix limits, must be documented prior to testing.

## RTR Data Source Emulation

The RTR emulator must be capable of generating synthetic VRP data sets with user-defined characteristics. This includes the ability to create arbitrary combinations of prefixes and ASNs, overlapping VRPs, conflicting VRPs, and other edge cases relevant to validation logic. The VRP datasets should mimic realistic global distributions where appropriate, but must also support scaling tests where VRP volumes are substantially higher than today's norm. The data source must further support generating controlled bursts of VRP updates, ranging from 100 to 10,000 VRP changes per second, and must allow for both additive updates and withdrawals.

## BGP Traffic Generation Requirements

The BGP traffic generator must present the DUT with a stable baseline routing table prior to initiating any benchmark. This ensures that the DUT begins each test run in a known, converged state with predictable CPU and memory utilization. The generator must also provide a set of ROV-affected prefixes whose origin AS can be manipulated in concert with VRP updates from the RTR emulator. These prefixes should span a range of prefix lengths and originate from diverse ASes to reflect realistic routing conditions. The traffic generator must support deterministic convergence triggers, such as the precise injection of BGP updates following a VRP change or the simultaneous application of both BGP and VRP events.

## Traffic Profile Parameters

When data-plane traffic is used, the following parameters should be specified:

- Fixed frame size used for the measurement. For convergence measurements, a small fixed packet size SHOULD be used to improve time resolution. A 128-byte packet at Layer 3 is one practical choice. Other fixed sizes MAY be used when required by the traffic generator or encapsulation overhead, but the selected size must be documented.  
- Traffic rate in packets per second (PPS). For convergence measurements, the traffic should use a constant rate so that packet arrival times map directly to time resolution. Burst traffic MAY be used in stress scenarios, but such tests must be reported separately from constant-rate convergence tests.  
- Traffic pattern. For convergence measurements, constant-rate traffic is RECOMMENDED. 
- Source and destination IP address selection rules. When the setup sends one stream per tested prefix, the destination address for that stream should be selected from within the tested prefix. The first usable address is one valid example. The selection rule must be documented and applied consistently across runs.  
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

* A defined coverage level of the VRP dataset relative to the announced routing space. Coverage SHOULD be reported as the percentage of announced prefixes, or the percentage of announced address space, for which at least one matching VRP exists. The selected coverage definition must be stated and applied consistently across runs.

* Defined structural properties of the VRP dataset, including overlap characteristics. At minimum, the test should specify the fraction or probability that a VRP is covered by another VRP, for example where a more-specific VRP falls within the prefix range of a less-specific VRP. Tests MAY additionally report the distribution of covering depth, prefix lengths, and authorized origin AS diversity when those properties are relevant to the implementation under test.

From this baseline, input variables may be modified to stress different aspects of ROV behavior. These variables include the VRP churn rate, ranging from steady incremental updates to high-intensity bursts, the coverage level of the VRP dataset, the overlap probability among VRPs, and the type of RPKI-RTR updates provided to the DUT, such as incremental updates versus full-table refreshes. Each of these conditions may trigger different processing strategies within the DUT, and therefore must be explicitly controlled and documented.

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

    - The locality rule, address-family mix, prefix-length distribution, and number of affected prefixes per region must be documented.  

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

    - If only one timer is reported, the report must state which timer definition is used.  

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

An ROV benchmarking report must provide enough details to allow reproducibility and meaningful comparison across different DUTs. Each report must include the following elements:

* **Test environment description**: The report must specify the DUT hardware and software versions, the testbed topology, and all ROV-related configuration parameters required to replicate the setup.  

* **Input conditions**: The report must document the ROA count, VRP count, VRP coverage relative to the announced routing space, VRP overlap characteristics, the RIB-in size with IPv4 and IPv6 counts stated separately, the presence and rate of VRP churn, and whether RTR updates were incremental or full.  

* **Metrics and results**: Each measured metric must include its definition, a brief description of the measurement procedure, and results presented in tabular numerical form (including minimum, average, maximum, and at least P95 values). Graphs MAY be included for clarification.

* **Deviations and anomalies**: Any deviation from the expected behavior must be described, including the conditions under which it occurred and whether the test was repeated.  

* **Summary of observations**: The report must include a concise summary of overall DUT performance, scalability limits observed, and any significant effects of enabling ROV on BGP behavior. 

In addition, the report must include, at minimum, the following parameters:

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

# IANA Considerations

This document has no actions for IANA.

--- back

# Acknowledgements {#ack}
{: numbered="false"}

Many thanks to Giuseppe Fioccola, Christian Giese, and Maria Matejka for their review, comments, and suggestions.