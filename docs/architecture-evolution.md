# OceanBase Architecture Evolution: Video Notes

These notes accompany the full **149-second (2:29)** animation linked from the [README](../README.md#system-architecture).
**Facts as of October 14, 2026.** Dates identify historical milestones or the period described, rather than the release date of every component shown.

The animation is a conceptual explanation, not a deployment diagram, benchmark, or upgrade procedure.
The recurring request particle connects separate examples; it does not represent one transaction spanning versions, products, or engines.

## Scene guide

### 01. 00:00-00:06 | Opening

The animation follows OceanBase from development starting in 2010 and its 2011 Taobao Favorites deployment to the architecture described in 2026. Sources: [R01](https://oceanbase.github.io/docs/blogs/showcases/architectural-evolution), [R02](https://en.oceanbase.com/blog/from-distributed-oltp-to-ai-lakebase), [R05](https://www.vldb.org/pvldb/vol15/p3385-xu.pdf).

### 02. 00:06-00:22 | Early roles: 0.x

The mature 0.5 layout separates MergeServer, UpdateServer, ChunkServer, and RootServer roles. Reads spread across machines; writes pass through a primary UpdateServer. Version 0.5 adopts Paxos in 2014. Sources: [R03](https://developer.aliyun.com/article/728094), [R04](https://www.vldb.org/pvldb/vol16/p3728-xu.pdf).

### 03. 00:22-00:38 | Peer nodes: 1.x to 3.x

Version 1.0 unifies node capabilities in 2016, distributing leaders of different partitions across OBServers. Later milestones include Oracle compatibility in 2.0 Enterprise Edition (2018), V2.2 Enterprise Edition TPC-C results (2019/2020), and the modern Community Edition 3.1.0 release (2021). Sources: [R03](https://developer.aliyun.com/article/728094), [R05](https://www.vldb.org/pvldb/vol15/p3385-xu.pdf), [R06](https://gitee.com/oceanbase/tutorials-doc/blob/V1.0.0/zh-CN/1.chapter-1-overview-of-the-oceanbase-database.md), [R07](https://www.tpc.org/tpcc/results/tpcc_result_detail5.asp?id=119100101&lang=), [R08](https://tpc.org/tpcc/results/tpcc_result_detail5.asp?id=120051701).

### 04. 00:38-00:56 | Tablet and Log Stream: 4.0

In 2022, 4.0 separates data partitioning from log organization: multiple Tablets share a Log Stream (LS). Transactions within one LS can use one-phase commit; transactions across LSs need coordinated commit. Sources: [R09](https://www.oceanbase.com/docs/common-oceanbase-database-cn-10000000001577872), [R04](https://www.vldb.org/pvldb/vol16/p3728-xu.pdf), [R10](https://oceanbase.github.io/docs/blogs/arch/miniaturization).

### 05. 00:56-01:06 | One kernel at different scales

The integrated architecture uses the same kernel for single-server and distributed deployments, with local execution optimizations where possible. Cluster expansion still requires data placement and migration. Sources: [R11](https://oceanbase.github.io/docs/blogs/showcases/integrated-architecture), [R10](https://oceanbase.github.io/docs/blogs/arch/miniaturization).

### 06. 01:06-01:21 | Row and column paths: 4.3

Version 4.3 introduces a native columnar engine in 2024; columnstore replicas arrive in 4.3.3. Reads combine visible row-oriented incremental data with the chosen row or column baseline layout. Sources: [R12](https://en.oceanbase.com/blog/12062885632), [R13](https://segmentfault.com/a/1190000044956022), [R14](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001872926).

### 07. 01:21-01:33 | Multimodel search: later 4.x

Structured fields, text, JSON, and vectors support combined filtering and retrieval. The four paths illustrate capabilities, rather than a mandatory execution plan or identical index support across releases. Sources: [R02](https://en.oceanbase.com/blog/from-distributed-oltp-to-ai-lakebase), [R15](https://www.oceanbase.com/product/ai-database).

### 08. 01:33-01:43 | Separate storage branches

Enterprise 4.4.0 introduces shared storage in July 2025. The 2026 V5.0.1 release follows the classic branch and explicitly does not support shared-storage mode. Sources: [R16](https://www.oceanbase.com/en/product/oceanbase-database-rn/releaseNote), [R17](https://www.oceanbase.com/en/docs/common-oceanbase-database-cn-1000000001876361), [R18](https://arxiv.org/html/2602.23571v1).

### 09. 01:43-02:01 | Compute, logs, and object storage

Shared-storage architecture separates compute, durable logging, and object storage. In the single-LS example, log-quorum persistence precedes acknowledgement; object-storage uploads occur in background work. Sources: [R19](https://www.oceanbase.com/docs/common-oceanbase-database-ai-1000000006858238), [R20](https://www.oceanbase.com/docs/common-obd-cn-1000000006717845), [R18](https://arxiv.org/html/2602.23571v1).

### 10. 02:01-02:21 | Open computing and catalogs

The public 2026 AI Lakebase architecture connects SQL, batch processing, and AI work through documented data and catalog interfaces. DataStudio supplies data engineering and governance; DataPilot supplies the data agent. Sources: [R21](https://en.oceanbase.com/blog/oceanbase-ai-lakebase-architecture), [R22](https://www.oceanbase.com/docs/common-oceanbase-database-ai-1000000006779101), [R23](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000006615248), [R02](https://en.oceanbase.com/blog/from-distributed-oltp-to-ai-lakebase).

### 11. 02:21-02:29 | Two product families

OceanBase Database continues to serve transactional and analytical workloads. AI Lakebase combines the Lakebase engine, DataStudio, and DataPilot; its complete product scope is distinct from Community Edition. Sources: [R02](https://en.oceanbase.com/blog/from-distributed-oltp-to-ai-lakebase), [R24](https://en.oceanbase.com/blog/oceanbase-lakebase-unified-data-foundation-for-ai-native-applications), [R25](https://www.oceanbase.com/product/lakebase).

## Version and product boundaries

- **Early versions:** the four-role example describes mature 0.5. Initial SQL support appears in 0.4; the animation does not backdate that SQL architecture to 0.1.
- **Replication and transactions:** one partition has one leader. Paxos replicates one group; two-phase commit coordinates different transaction participants. Log replicas are not separate transaction participants.
- **Tablet and LS:** older partition groups could already contain multiple partitions. Crossing LSs still requires distributed commit even when both LSs are on the same machine.
- **HTAP:** the baseline layout changes between row and column forms; incremental changes remain row-oriented. Vectorized execution and vector similarity search are different capabilities.
- **Edition scope:** Oracle compatibility and the cited TPC-C entries concern Enterprise Edition. The modern Community Edition begins at 3.1.0 in June 2021; this does not deny earlier open-source activity.
- **Storage scope:** shared storage in Enterprise 4.4.0 does not establish the same support in a same-number Community Edition. The V5.0.1 restriction applies to that release, not to the entire product family.
- **Durability:** the three log replicas are an example topology. Acknowledgement requires durable log quorum, not object upload; cross-LS transactions also require atomic commit.
- **Lake access:** external lake queries and the complete AI Lakebase product have different scopes. External Catalog currently supports MySQL mode; file formats, lake table formats, storage services, and catalogs are separate choices.
- **Open computing:** the cited AI Database V4.6.2 interface synchronizes new DDL one way from Internal Catalog to HMS after enablement, without backfilling existing objects. Writes remain on the OceanBase side; the diagram does not imply unrestricted cross-engine writes or global cross-engine ACID.
- **Product family:** AI Lakebase combines Lakebase, DataStudio, and DataPilot. The video is a historical overview across product branches, not a feature inventory of this Community Edition repository.

## Sources

All 25 source URLs are listed below. Research papers describe their published designs; product pages and release notes define their stated edition and version scope.

- **R01** - [OceanBase architecture evolution](https://oceanbase.github.io/docs/blogs/showcases/architectural-evolution)
- **R02** - [From Distributed OLTP to AI Lakebase](https://en.oceanbase.com/blog/from-distributed-oltp-to-ai-lakebase)
- **R03** - [Author retrospective on OceanBase storage architecture (Chinese)](https://developer.aliyun.com/article/728094)
- **R04** - [Paetica paper, PVLDB 2023](https://www.vldb.org/pvldb/vol16/p3728-xu.pdf)
- **R05** - [OceanBase system paper, PVLDB 2022](https://www.vldb.org/pvldb/vol15/p3385-xu.pdf)
- **R06** - [Official database overview and development history (Chinese)](https://gitee.com/oceanbase/tutorials-doc/blob/V1.0.0/zh-CN/1.chapter-1-overview-of-the-oceanbase-database.md)
- **R07** - [TPC-C result: OceanBase V2.2, 2019](https://www.tpc.org/tpcc/results/tpcc_result_detail5.asp?id=119100101&lang=)
- **R08** - [TPC-C result: OceanBase V2.2, 2020](https://tpc.org/tpcc/results/tpcc_result_detail5.asp?id=120051701)
- **R09** - [OceanBase 4.0 architecture documentation (Chinese)](https://www.oceanbase.com/docs/common-oceanbase-database-cn-10000000001577872)
- **R10** - [OceanBase miniaturization design](https://oceanbase.github.io/docs/blogs/arch/miniaturization)
- **R11** - [Integrated single-server and distributed architecture](https://oceanbase.github.io/docs/blogs/showcases/integrated-architecture)
- **R12** - [Exploring OceanBase 4.3](https://en.oceanbase.com/blog/12062885632)
- **R13** - [OceanBase column storage implementation (Chinese)](https://segmentfault.com/a/1190000044956022)
- **R14** - [Community Edition release history (Chinese)](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001872926)
- **R15** - [OceanBase AI Database product overview](https://www.oceanbase.com/product/ai-database)
- **R16** - [OceanBase Database release notes and V5.0.1 limitations](https://www.oceanbase.com/en/product/oceanbase-database-rn/releaseNote)
- **R17** - [Enterprise Edition release history](https://www.oceanbase.com/en/docs/common-oceanbase-database-cn-1000000001876361)
- **R18** - [Bacchus shared-storage architecture paper](https://arxiv.org/html/2602.23571v1)
- **R19** - [AI Database high-availability documentation (Chinese)](https://www.oceanbase.com/docs/common-oceanbase-database-ai-1000000006858238)
- **R20** - [OBD: deploy an AI Database cluster (Chinese)](https://www.oceanbase.com/docs/common-obd-cn-1000000006717845)
- **R21** - [OceanBase AI Lakebase Architecture](https://en.oceanbase.com/blog/oceanbase-ai-lakebase-architecture)
- **R22** - [Catalog data synchronization and limitations (Chinese)](https://www.oceanbase.com/docs/common-oceanbase-database-ai-1000000006779101)
- **R23** - [Data lake overview and access limitations (Chinese)](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000006615248)
- **R24** - [OceanBase Lakebase: unified data foundation for AI-native applications](https://en.oceanbase.com/blog/oceanbase-lakebase-unified-data-foundation-for-ai-native-applications)
- **R25** - [OceanBase AI Lakebase product overview](https://www.oceanbase.com/product/lakebase)

Release notes and product pages may change after the stated fact cutoff. Consult the documentation for the exact edition, release, and deployment you use.
