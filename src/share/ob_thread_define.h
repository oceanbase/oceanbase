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
TG_DEF(TEST_OB_TH, testObTh, THREAD_POOL, 1)
TG_DEF(COMMON_THREAD_POOL, ComTh, THREAD_POOL, 1)
TG_DEF(COMMON_QUEUE_THREAD, ComQueueTh, QUEUE_THREAD, 1, 100)
TG_DEF(COMMON_TIMER_THREAD, ComTimerTh, TIMER)
TG_DEF(Blacklist, Blacklist, THREAD_POOL, 1)
TG_DEF(PartSerMigRetryQt, PartSerMigRetryQt, THREAD_POOL, 1)
// TG_DEF(PartSerCb, PartSerCb, QUEUE_THREAD, ThreadCountPair(storage::ObCallbackQueueThread::QUEUE_THREAD_NUM, storage::ObCallbackQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
//       (!lib::is_mini_mode() ? OB_MAX_PARTITION_NUM_PER_SERVER : OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER) * 2)
// TG_DEF(PartSerLargeCb, PartSerLargeCb, QUEUE_THREAD, ThreadCountPair(storage::ObCallbackQueueThread::QUEUE_THREAD_NUM, storage::ObCallbackQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
//       (!lib::is_mini_mode() ? OB_MAX_PARTITION_NUM_PER_SERVER : OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER) * 2)
// TG_DEF(ReplayEngine, ReplayEngine, QUEUE_THREAD, ThreadCountPair(sysconf(_SC_NPROCESSORS_ONLN), 2),
//        !lib::is_mini_mode() ? (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MAX_PARTITION_NUM_PER_SERVER : (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER)
TG_DEF(TransMigrate, TransMigrate, QUEUE_THREAD, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS(24), 1), 10000)
TG_DEF(StandbyTimestampService, StandbyTimestampService, THREAD_POOL, 1)
TG_DEF(WeakReadService, WeakRdSrv, THREAD_POOL, 1)
TG_DEF(TransTaskWork, TransTaskWork, QUEUE_THREAD, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS(12), 1), transaction::ObThreadLocalTransCtx::MAX_BIG_TRANS_TASK)
TG_DEF(DDLTaskExecutor3, DDLTaskExecutor3, THREAD_POOL, ThreadCountPair(8, 2))
TG_DEF(TSWorker, TSWorker, QUEUE_THREAD, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS(12), 1), transaction::ObTsWorker::MAX_TASK_NUM)
TG_DEF(BRPC, BRPC, THREAD_POOL, ThreadCountPair(obrpc::ObBatchRpc::MAX_THREAD_COUNT, obrpc::ObBatchRpc::MINI_MODE_THREAD_COUNT))
TG_DEF(RLMGR, RLMGR, THREAD_POOL, 1)
TG_DEF(LeaseQueueTh, LeaseQueueTh, THREAD_POOL, ThreadCountPair(observer::ObSrvDeliver::LEASE_TASK_THREAD_CNT, observer::ObSrvDeliver::MINI_MODE_LEASE_TASK_THREAD_CNT))
TG_DEF(DDLQueueTh, DDLQueueTh, THREAD_POOL, ThreadCountPair(observer::ObSrvDeliver::DDL_TASK_THREAD_CNT, observer::ObSrvDeliver::DDL_TASK_THREAD_CNT))
TG_DEF(MysqlQueueTh, MysqlQueueTh, THREAD_POOL, ThreadCountPair(GET_MYSQL_THREAD_COUNT(observer::ObSrvDeliver::MYSQL_TASK_THREAD_CNT), GET_MYSQL_THREAD_COUNT(observer::ObSrvDeliver::MINI_MODE_MYSQL_TASK_THREAD_CNT)))
TG_DEF(DDLPQueueTh, DDLPQueueTh, THREAD_POOL, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS_WITH_LIMIT(2, 24), 2))
TG_DEF(DiagnoseQueueTh, DiagnoseQueueTh, THREAD_POOL, ThreadCountPair(observer::ObSrvDeliver::MYSQL_DIAG_TASK_THREAD_CNT, observer::ObSrvDeliver::MINI_MODE_MYSQL_DIAG_TASK_THREAD_CNT))
TG_DEF(DdlBuild, DdlBuild, ASYNC_TASK_QUEUE, ThreadCountPair(16, 1), 4 << 10)
TG_DEF(LSService, LSService, REENTRANT_THREAD_POOL, 2)
TG_DEF(ObCreateStandbyFromNetActor, ObCreateStandbyFromNetActor, REENTRANT_THREAD_POOL, 1)
TG_DEF(SimpleLSService, SimpleLSService, REENTRANT_THREAD_POOL, 1)
TG_DEF(IntermResGC, IntermResGC, TIMER)
TG_DEF(ServerGTimer, ServerGTimer, TIMER)
TG_DEF(FreezeTimer, FreezeTimer, TIMER)
TG_DEF(SqlMemTimer, SqlMemTimer, TIMER)
TG_DEF(ServerTracerTimer, ServerTracerTimer, TIMER)
TG_DEF(RSqlPool, RSqlPool, TIMER)
TG_DEF(KVCacheWash, KVCacheWash, TIMER)
TG_DEF(KVCacheRep, KVCacheRep, TIMER)
TG_DEF(ObHeartbeat, ObHeartbeat, TIMER)
TG_DEF(PlanCacheEvict, PlanCacheEvict, TIMER)
TG_DEF(TabletStatRpt, TabletStatRpt, TIMER)
TG_DEF(PsCacheEvict, PsCacheEvict, TIMER)
TG_DEF(MergeLoop, MergeLoop, TIMER)
TG_DEF(SSTableGC, SSTableGC, TIMER)
TG_DEF(MediumLoop, MediumLoop, TIMER)
TG_DEF(WriteCkpt, WriteCkpt, TIMER)
TG_DEF(EXTLogWash, EXTLogWash, TIMER)
TG_DEF(LineCache, LineCache, TIMER)
TG_DEF(LocalityReload, LocalityReload, TIMER)
TG_DEF(MemstoreGC, MemstoreGC, TIMER)
TG_DEF(DiskUseReport, DiskUseReport, TIMER)
TG_DEF(CLOGReqMinor, CLOGReqMinor, TIMER)
TG_DEF(PGArchiveLog, PGArchiveLog, TIMER)
TG_DEF(CKPTLogRep, CKPTLogRep, TIMER)
TG_DEF(RebuildRetry, RebuildRetry, TIMER)
TG_DEF(TableMgrGC, TableMgrGC, TIMER)
TG_DEF(IndexSche, IndexSche, TIMER)
TG_DEF(FreInfoReload, FreInfoReload, TIMER)
TG_DEF(HAGtsMgr, HAGtsMgr, TIMER)
TG_DEF(HAGtsHB, HAGtsHB, TIMER)
TG_DEF(RebuildTask, RebuildTask, TIMER)
TG_DEF(LogDiskMon, LogDiskMon, TIMER)
TG_DEF(ILOGFlush, ILOGFlush, TIMER)
TG_DEF(ILOGPurge, ILOGPurge, TIMER)
TG_DEF(RLogClrCache, RLogClrCache, TIMER)
TG_DEF(TableStatRpt, TableStatRpt, TIMER)
TG_DEF(MacroMetaMgr, MacroMetaMgr, TIMER)
TG_DEF(StoreFileGC, StoreFileGC, TIMER)
TG_DEF(LeaseHB, LeaseHB, TIMER)
TG_DEF(ClusterTimer, ClusterTimer, TIMER)
TG_DEF(MergeTimer, MergeTimer, TIMER)
TG_DEF(CFC, CFC, TIMER)
TG_DEF(CCDF, CCDF, TIMER)
TG_DEF(LogMysqlPool, LogMysqlPool, TIMER)
TG_DEF(TblCliSqlPool, TblCliSqlPool, TIMER)
TG_DEF(QueryExecCtxGC, QueryExecCtxGC, THREAD_POOL, 1)
TG_DEF(DtlDfc, DtlDfc, TIMER)
TG_DEF(LogIOTaskCbThreadPool, LogIOCb, QUEUE_THREAD,
       ThreadCountPair(palf::LogIOTaskCbThreadPool::THREAD_NUM,
       palf::LogIOTaskCbThreadPool::MINI_MODE_THREAD_NUM),
       palf::LogIOTaskCbThreadPool::MAX_LOG_IO_CB_TASK_NUM)
TG_DEF(ReplayService, ReplaySrv, QUEUE_THREAD, 1, (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET)
TG_DEF(LogRouteService, LogRouteSrv, QUEUE_THREAD, 1, (common::MAX_SERVER_COUNT) * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET)
TG_DEF(LogRouterTimer, LogRouterTimer, TIMER)
TG_DEF(LogFetcherLSWorker, LSWorker, MAP_QUEUE_THREAD, ThreadCountPair(4, 1))
TG_DEF(LogFetcherIdlePool, LSIdlePool, MAP_QUEUE_THREAD, 1)
TG_DEF(LogFetcherDeadPool, LSDeadPool, MAP_QUEUE_THREAD, 1)
TG_DEF(LogFetcherTimer, LSTimer, TIMER)
TG_DEF(PalfBlockGC, PalfGC, TIMER)
TG_DEF(LSFreeze, LSFreeze, QUEUE_THREAD, ThreadCountPair(storage::ObLSFreezeThread::QUEUE_THREAD_NUM, storage::ObLSFreezeThread::MINI_MODE_QUEUE_THREAD_NUM),
       storage::ObLSFreezeThread::MAX_FREE_TASK_NUM)
TG_DEF(LSFetchLogEngine, FetchLog, QUEUE_THREAD,
       ThreadCountPair(palf::FetchLogEngine::FETCH_LOG_THREAD_COUNT, palf::FetchLogEngine::MINI_MODE_FETCH_LOG_THREAD_COUNT),
       palf::FetchLogEngine::FETCH_LOG_TASK_MAX_COUNT_PER_LS * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET)
TG_DEF(DagScheduler, DagScheduler, THREAD_POOL, 1)
TG_DEF(DagWorker, DagWorker, THREAD_POOL, 1)
TG_DEF(RCService, RCSrv, QUEUE_THREAD,
       ThreadCountPair(logservice::ObRoleChangeService::MAX_THREAD_NUM,
       logservice::ObRoleChangeService::MAX_THREAD_NUM),
       logservice::ObRoleChangeService::MAX_RC_EVENT_TASK)
TG_DEF(ApplyService, ApplySrv, QUEUE_THREAD, 1, (common::APPLY_TASK_QUEUE_SIZE + 1) * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET)
TG_DEF(GlobalCtxTimer, GlobalCtxTimer, TIMER)
TG_DEF(StorageLogWriter, StorageLogWriter, THREAD_POOL, 1)
TG_DEF(ReplayProcessStat, ReplayProcessStat, TIMER)
TG_DEF(ActiveSessHist, ActiveSessHist, TIMER)
TG_DEF(CTASCleanUpTimer, CTASCleanUpTimer, TIMER)
TG_DEF(DDLScanTask, DDLScanTask, TIMER)
TG_DEF(TenantLSMetaChecker, LSMetaCh, TIMER)
TG_DEF(TenantTabletMetaChecker, TbMetaCh, TIMER)
TG_DEF(ServerMetaChecker, SvrMetaCh, TIMER)
#ifdef OB_BUILD_ARBITRATION
TG_DEF(ArbNormalRpcQueueTh, ArbNormalRpcQueueTh, THREAD_POOL, ThreadCountPair(arbserver::ObArbSrvDeliver::get_normal_rpc_thread_num(), arbserver::ObArbSrvDeliver::MINI_MODE_RPC_QUEUE_CNT))
TG_DEF(ArbServerRpcQueueTh, ArbSrvRpcQueueTh, THREAD_POOL, ThreadCountPair(arbserver::ObArbSrvDeliver::get_server_rpc_thread_num(), arbserver::ObArbSrvDeliver::MINI_MODE_RPC_QUEUE_CNT))
#endif
TG_DEF(ArbGCSTh, ArbGCTimerP, TIMER)
#ifdef OB_BUILD_ARBITRATION
TG_DEF(ArbServerTimer, ArbServerTimer, TIMER)
#endif
TG_DEF(DataDictTimer, DataDictTimer, TIMER)
TG_DEF(CDCService, CDCSrv, THREAD_POOL, 1)
TG_DEF(LogUpdater, LogUpdater, TIMER)
TG_DEF(HeartBeatCheckTask, HeartBeatCheckTask, TIMER)
TG_DEF(RedefHeartBeatTask, RedefHeartBeatTask, TIMER)
TG_DEF(MemDumpTimer, MemDumpTimer, TIMER)
TG_DEF(SSTableDefragment, SSTableDefragment, TIMER)
TG_DEF(TenantMetaMemMgr, TenantMetaMemMgr, TIMER)
TG_DEF(IngressService, IngressService, TIMER)
TG_DEF(HeartbeatService, HeartbeatService, REENTRANT_THREAD_POOL, 2)
TG_DEF(DetectManager, DetectManager, THREAD_POOL, 1)
TG_DEF(CONFIG_MGR, ConfigMgr, TIMER, 1024)
TG_DEF(IO_TUNING, IO_TUNING, THREAD_POOL, 1)
TG_DEF(IO_SCHEDULE, IO_SCHEDULE, THREAD_POOL, 1)
TG_DEF(IO_CALLBACK, IO_CALLBACK, THREAD_POOL, 1)
TG_DEF(IO_CHANNEL, IO_CHANNEL, THREAD_POOL, 1)
TG_DEF(IO_HEALTH, IO_HEALTH, QUEUE_THREAD, 1, 100)
TG_DEF(IO_BENCHMARK, IO_BENCHMARK, THREAD_POOL, 1)
TG_DEF(TIMEZONE_MGR, TimezoneMgr, TIMER)
TG_DEF(MASTER_KEY_MGR, MasterKeyMgr, QUEUE_THREAD, 1, 100)
TG_DEF(SRS_MGR, SrsMgr, TIMER, 128)
TG_DEF(InfoPoolResize, InfoPoolResize, TIMER)
TG_DEF(MinorScan, MinorScan, TIMER)
TG_DEF(MajorScan, MajorScan, TIMER)
TG_DEF(TenantTransferService, TransferSrv, REENTRANT_THREAD_POOL, ThreadCountPair(4 ,1))
TG_DEF(WR_TIMER_THREAD, WrTimer, TIMER)

TG_DEF(SvrStartupHandler, SvrStartupHandler, QUEUE_THREAD,
   ThreadCountPair(observer::ObServerStartupTaskHandler::get_thread_num(), observer::ObServerStartupTaskHandler::get_thread_num()),
   observer::ObServerStartupTaskHandler::MAX_QUEUED_TASK_NUM)
TG_DEF(TenantTTLManager, TTLManager, TIMER)
TG_DEF(TenantTabletTTLMgr, TTLTabletMgr, TIMER)
TG_DEF(TntSharedTimer, TntSharedTimer, TIMER)
#endif
