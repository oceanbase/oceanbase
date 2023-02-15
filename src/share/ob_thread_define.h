/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifdef TG_DEF
#define GET_THREAD_NUM_BY_NPROCESSORS(factor) (sysconf(_SC_NPROCESSORS_ONLN) / (factor) > 0 ? sysconf(_SC_NPROCESSORS_ONLN) / (factor) : 1)
#define GET_THREAD_NUM_BY_NPROCESSORS_WITH_LIMIT(factor, limit) (sysconf(_SC_NPROCESSORS_ONLN) / (factor) > 0 ? min(sysconf(_SC_NPROCESSORS_ONLN) / (factor), limit) : 1)
#define GET_MYSQL_THREAD_COUNT(default_cnt) (GCONF.sql_login_thread_count ? GCONF.sql_login_thread_count : (default_cnt))
TG_DEF(TEST_OB_TH, testObTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1 ,1))
TG_DEF(COMMON_THREAD_POOL, ComTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1 ,1))
TG_DEF(COMMON_QUEUE_THREAD, ComQueueTh, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(1 ,1), 100)
TG_DEF(COMMON_TIMER_THREAD, ComTimerTh, "", TG_STATIC, TIMER)
TG_DEF(Blacklist, Blacklist, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(PartSerMigRetryQt, PartSerMigRetryQt, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
// TG_DEF(PartSerCb, PartSerCb, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(storage::ObCallbackQueueThread::QUEUE_THREAD_NUM, storage::ObCallbackQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
//       (!lib::is_mini_mode() ? OB_MAX_PARTITION_NUM_PER_SERVER : OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER) * 2)
// TG_DEF(PartSerLargeCb, PartSerLargeCb, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(storage::ObCallbackQueueThread::QUEUE_THREAD_NUM, storage::ObCallbackQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
//       (!lib::is_mini_mode() ? OB_MAX_PARTITION_NUM_PER_SERVER : OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER) * 2)
TG_DEF(ReplayEngine, ReplayEngine, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(sysconf(_SC_NPROCESSORS_ONLN), 2),
       !lib::is_mini_mode() ? (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MAX_PARTITION_NUM_PER_SERVER : (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER)
TG_DEF(TransMigrate, TransMigrate, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS(24), 1), 10000)
TG_DEF(StandbyTimestampService, StandbyTimestampService, "", TG_DYNAMIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(WeakReadService, WeakRdSrv, "", TG_DYNAMIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(TransTaskWork, TransTaskWork, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS(12), 1), transaction::ObThreadLocalTransCtx::MAX_BIG_TRANS_TASK)
TG_DEF(DDLTaskExecutor3, DDLTaskExecutor3, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(8, 2))
TG_DEF(TSWorker, TSWorker, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS(12), 1), transaction::ObTsWorker::MAX_TASK_NUM)
TG_DEF(BRPC, BRPC, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(obrpc::ObBatchRpc::MAX_THREAD_COUNT, obrpc::ObBatchRpc::MINI_MODE_THREAD_COUNT))
TG_DEF(RLMGR, RLMGR, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(LeaseQueueTh, LeaseQueueTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(observer::ObSrvDeliver::LEASE_TASK_THREAD_CNT, observer::ObSrvDeliver::MINI_MODE_LEASE_TASK_THREAD_CNT))
TG_DEF(DDLQueueTh, DDLQueueTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(observer::ObSrvDeliver::DDL_TASK_THREAD_CNT, observer::ObSrvDeliver::DDL_TASK_THREAD_CNT))
TG_DEF(MysqlQueueTh, MysqlQueueTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(GET_MYSQL_THREAD_COUNT(observer::ObSrvDeliver::MYSQL_TASK_THREAD_CNT), GET_MYSQL_THREAD_COUNT(observer::ObSrvDeliver::MINI_MODE_MYSQL_TASK_THREAD_CNT)))
TG_DEF(DDLPQueueTh, DDLPQueueTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS_WITH_LIMIT(2, 24), 2))
TG_DEF(DiagnoseQueueTh, DiagnoseQueueTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(observer::ObSrvDeliver::MYSQL_DIAG_TASK_THREAD_CNT, observer::ObSrvDeliver::MINI_MODE_MYSQL_DIAG_TASK_THREAD_CNT))
TG_DEF(DdlBuild, DdlBuild, "", TG_STATIC, ASYNC_TASK_QUEUE, ThreadCountPair(16, 1), 4 << 10)
TG_DEF(LSService, LSService, "", TG_STATIC, REENTRANT_THREAD_POOL, ThreadCountPair(2 ,2))
TG_DEF(SimpleLSService, SimpleLSService, "", TG_STATIC, REENTRANT_THREAD_POOL, ThreadCountPair(1 ,1))
TG_DEF(IntermResGC, IntermResGC, "", TG_STATIC, TIMER)
TG_DEF(ServerGTimer, ServerGTimer, "", TG_STATIC, TIMER)
TG_DEF(FreezeTimer, FreezeTimer, "", TG_STATIC, TIMER)
TG_DEF(SqlMemTimer, SqlMemTimer, "", TG_STATIC, TIMER)
TG_DEF(ServerTracerTimer, ServerTracerTimer, "", TG_STATIC, TIMER)
TG_DEF(RSqlPool, RSqlPool, "", TG_STATIC, TIMER)
TG_DEF(KVCacheWash, KVCacheWash, "", TG_STATIC, TIMER)
TG_DEF(KVCacheRep, KVCacheRep, "", TG_STATIC, TIMER)
TG_DEF(ObHeartbeat, ObHeartbeat, "", TG_STATIC, TIMER)
TG_DEF(PlanCacheEvict, PlanCacheEvict, "", TG_DYNAMIC, TIMER)
TG_DEF(TabletStatRpt, TabletStatRpt, "", TG_STATIC, TIMER)
TG_DEF(PsCacheEvict, PsCacheEvict, "", TG_DYNAMIC, TIMER)
TG_DEF(MergeLoop, MergeLoop, "", TG_STATIC, TIMER)
TG_DEF(SSTableGC, SSTableGC, "", TG_STATIC, TIMER)
TG_DEF(MediumLoop, MediumLoop, "", TG_STATIC, TIMER)
TG_DEF(WriteCkpt, WriteCkpt, "", TG_STATIC, TIMER)
TG_DEF(EXTLogWash, EXTLogWash, "", TG_STATIC, TIMER)
TG_DEF(LineCache, LineCache, "", TG_STATIC, TIMER)
TG_DEF(LocalityReload, LocalityReload, "", TG_STATIC, TIMER)
TG_DEF(MemstoreGC, MemstoreGC, "", TG_STATIC, TIMER)
TG_DEF(DiskUseReport, DiskUseReport, "", TG_STATIC, TIMER)
TG_DEF(CLOGReqMinor, CLOGReqMinor, "", TG_STATIC, TIMER)
TG_DEF(PGArchiveLog, PGArchiveLog, "", TG_STATIC, TIMER)
TG_DEF(CKPTLogRep, CKPTLogRep, "", TG_STATIC, TIMER)
TG_DEF(RebuildRetry, RebuildRetry, "", TG_STATIC, TIMER)
TG_DEF(TableMgrGC, TableMgrGC, "", TG_STATIC, TIMER)
TG_DEF(IndexSche, IndexSche, "", TG_STATIC, TIMER)
TG_DEF(FreInfoReload, FreInfoReload, "", TG_DYNAMIC, TIMER)
TG_DEF(HAGtsMgr, HAGtsMgr, "", TG_STATIC, TIMER)
TG_DEF(HAGtsHB, HAGtsHB, "", TG_STATIC, TIMER)
TG_DEF(RebuildTask, RebuildTask, "", TG_STATIC, TIMER)
TG_DEF(LogDiskMon, LogDiskMon, "", TG_DYNAMIC, TIMER)
TG_DEF(ILOGFlush, ILOGFlush, "", TG_STATIC, TIMER)
TG_DEF(ILOGPurge, ILOGPurge, "", TG_STATIC, TIMER)
TG_DEF(RLogClrCache, RLogClrCache, "", TG_STATIC, TIMER)
TG_DEF(TableStatRpt, TableStatRpt, "", TG_STATIC, TIMER)
TG_DEF(MacroMetaMgr, MacroMetaMgr, "", TG_STATIC, TIMER)
TG_DEF(StoreFileGC, StoreFileGC, "", TG_STATIC, TIMER)
TG_DEF(LeaseHB, LeaseHB, "", TG_STATIC, TIMER)
TG_DEF(ClusterTimer, ClusterTimer, "", TG_STATIC, TIMER)
TG_DEF(MergeTimer, MergeTimer, "", TG_STATIC, TIMER)
TG_DEF(CFC, CFC, "", TG_STATIC, TIMER)
TG_DEF(CCDF, CCDF, "", TG_STATIC, TIMER)
TG_DEF(LogMysqlPool, LogMysqlPool, "", TG_STATIC, TIMER)
TG_DEF(TblCliSqlPool, TblCliSqlPool, "", TG_STATIC, TIMER)
TG_DEF(QueryExecCtxGC, QueryExecCtxGC, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(DtlDfc, DtlDfc, "", TG_STATIC, TIMER)
TG_DEF(LogIOTaskCbThreadPool, LogIOCb, "", TG_STATIC, QUEUE_THREAD,
       ThreadCountPair(palf::LogIOTaskCbThreadPool::THREAD_NUM,
       palf::LogIOTaskCbThreadPool::MINI_MODE_THREAD_NUM),
       palf::LogIOTaskCbThreadPool::MAX_LOG_IO_CB_TASK_NUM)
TG_DEF(ReplayService, ReplaySrv, "", TG_DYNAMIC, QUEUE_THREAD, ThreadCountPair(1, 1),
       !lib::is_mini_mode() ? (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER : (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MINI_MODE_MAX_LS_NUM_PER_TENANT_PER_SERVER)
TG_DEF(LogRouteService, LogRouteSrv, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(1, 1),
       !lib::is_mini_mode() ? (common::MAX_SERVER_COUNT) * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER : (common::MAX_SERVER_COUNT) * OB_MINI_MODE_MAX_LS_NUM_PER_TENANT_PER_SERVER)
TG_DEF(LogRouterTimer, LogRouterTimer, "", TG_STATIC, TIMER)
TG_DEF(PalfBlockGC, PalfGC, "", TG_DYNAMIC, TIMER)
TG_DEF(LSFreeze, LSFreeze, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(storage::ObLSFreezeThread::QUEUE_THREAD_NUM, storage::ObLSFreezeThread::MINI_MODE_QUEUE_THREAD_NUM),
       storage::ObLSFreezeThread::MAX_FREE_TASK_NUM)
TG_DEF(LSFetchLogEngine, FetchLog, "", TG_DYNAMIC, QUEUE_THREAD,
       ThreadCountPair(palf::FetchLogEngine::FETCH_LOG_THREAD_COUNT, palf::FetchLogEngine::MINI_MODE_FETCH_LOG_THREAD_COUNT),
       !lib::is_mini_mode() ? palf::FetchLogEngine::FETCH_LOG_TASK_MAX_COUNT_PER_LS * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER : palf::FetchLogEngine::FETCH_LOG_TASK_MAX_COUNT_PER_LS * OB_MINI_MODE_MAX_LS_NUM_PER_TENANT_PER_SERVER)
TG_DEF(DagScheduler, DagScheduler, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(DagWorker, DagWorker, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(RCService, RCSrv, "", TG_DYNAMIC, QUEUE_THREAD,
       ThreadCountPair(logservice::ObRoleChangeService::MAX_THREAD_NUM,
       logservice::ObRoleChangeService::MAX_THREAD_NUM),
       logservice::ObRoleChangeService::MAX_RC_EVENT_TASK)
TG_DEF(ApplyService, ApplySrv, "", TG_DYNAMIC, QUEUE_THREAD, ThreadCountPair(1, 1),
       !lib::is_mini_mode() ? (common::APPLY_TASK_QUEUE_SIZE + 1) * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER : (common::APPLY_TASK_QUEUE_SIZE + 1) * OB_MINI_MODE_MAX_LS_NUM_PER_TENANT_PER_SERVER)
TG_DEF(GlobalCtxTimer, GlobalCtxTimer, "", TG_STATIC, TIMER)
TG_DEF(StorageLogWriter, StorageLogWriter, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(ReplayProcessStat, ReplayProcessStat, "", TG_STATIC, TIMER)
TG_DEF(ActiveSessHist, ActiveSessHist, "", TG_STATIC, TIMER)
TG_DEF(CTASCleanUpTimer, CTASCleanUpTimer, "", TG_STATIC, TIMER)
TG_DEF(DDLScanTask, DDLScanTask, "", TG_STATIC, TIMER)
TG_DEF(TenantLSMetaChecker, LSMetaCh, "", TG_STATIC, TIMER)
TG_DEF(TenantTabletMetaChecker, TbMetaCh, "", TG_STATIC, TIMER)
TG_DEF(ServerMetaChecker, SvrMetaCh, "", TG_STATIC, TIMER)
TG_DEF(ArbPrimaryGCSTh, ArbGCTimerP, "", TG_STATIC, TIMER)
TG_DEF(ArbStandbyGCSTh, ArbGCTimerS, "", TG_STATIC, TIMER)
TG_DEF(DataDictTimer, DataDictTimer, "", TG_STATIC, TIMER)
TG_DEF(CDCService, CDCSrv, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(LogUpdater, LogUpdater, "", TG_STATIC, TIMER)
TG_DEF(HeartBeatCheckTask, HeartBeatCheckTask, "", TG_STATIC, TIMER)
TG_DEF(RedefHeartBeatTask, RedefHeartBeatTask, "", TG_STATIC, TIMER)
TG_DEF(MemDumpTimer, MemDumpTimer, "", TG_STATIC, TIMER)
#endif
