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
#define GET_THREAD_NUM_BY_NPROCESSORS(factor) \
  (sysconf(_SC_NPROCESSORS_ONLN) / (factor) > 0 ? sysconf(_SC_NPROCESSORS_ONLN) / (factor) : 1)
TG_DEF(TEST_OB_TH, testObTh, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(Blacklist, Blacklist, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(SqlDistSched, SqlDistSched, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(sql::ObDistributedSchedulerManager::SCHEDULER_THREAD_NUM,
        sql::ObDistributedSchedulerManager::MINI_MODE_SCHEDULER_THREAD_NUM),
    sql::ObDistributedSchedulerManager::SCHEDULER_THREAD_QUEUE)
TG_DEF(PartSerMigRetryQt, PartSerMigRetryQt, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(storage::ObMigrateRetryQueueThread::QUEUE_THREAD_NUM,
        storage::ObMigrateRetryQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
    OB_MAX_PARTITION_NUM_PER_SERVER * 2)
TG_DEF(PartSerCb, PartSerCb, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(
        storage::ObCallbackQueueThread::QUEUE_THREAD_NUM, storage::ObCallbackQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
    OB_MAX_PARTITION_NUM_PER_SERVER * 2)
TG_DEF(PartSerLargeCb, PartSerLargeCb, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(
        storage::ObCallbackQueueThread::QUEUE_THREAD_NUM, storage::ObCallbackQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
    OB_MAX_PARTITION_NUM_PER_SERVER * 2)
TG_DEF(PartSerSlogWr, PartSerSlogWr, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(storage::ObSlogWriterQueueThread::QUEUE_THREAD_NUM,
        storage::ObSlogWriterQueueThread::MINI_MODE_QUEUE_THREAD_NUM),
    OB_MAX_PARTITION_NUM_PER_SERVER * 2)
TG_DEF(LogScan, LogScan, "", TG_STATIC, TIMER_GROUP,
    ThreadCountPair(clog::ObLogScanRunnable::MAX_THREAD_CNT, clog::ObLogScanRunnable::MINI_MODE_THREAD_CNT))
TG_DEF(ReplayEngine, ReplayEngine, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(sysconf(_SC_NPROCESSORS_ONLN), 2),
    !lib::is_mini_mode() ? (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MAX_PARTITION_NUM_PER_SERVER
                         : (common::REPLAY_TASK_QUEUE_SIZE + 1) * OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER)
TG_DEF(LogCb, LogCb, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(clog::ObCLogMgr::CLOG_CB_THREAD_COUNT, clog::ObCLogMgr::MINI_MODE_CLOG_CB_THREAD_COUNT),
    clog::CLOG_CB_TASK_QUEUE_SIZE)
TG_DEF(SpLogCb, SpLogCb, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(clog::CLOG_SP_CB_THREAD_COUNT, clog::CLOG_SP_CB_THREAD_COUNT), clog::CLOG_SP_CB_TASK_QUEUE_SIZE)
TG_DEF(STableChecksumUp, STableChecksumUp, "", TG_DYNAMIC, OB_THREAD_POOL,
    ThreadCountPair(observer::ObSSTableChecksumUpdater::UPDATER_THREAD_CNT,
        observer::ObSSTableChecksumUpdater::MINI_MODE_UPDATER_THREAD_CNT))
TG_DEF(CLogAdapter, CLogAdapter, "", TG_DYNAMIC, QUEUE_THREAD, ThreadCountPair(GET_THREAD_NUM_BY_NPROCESSORS(12), 1),
    transaction::ObClogAdapter::TOTAL_TASK)
TG_DEF(ClogAggre, ClogAggre, "", TG_DYNAMIC, QUEUE_THREAD,
    ThreadCountPair(clog::ObClogAggreRunnable::THREAD_COUNT, clog::ObClogAggreRunnable::MINI_THREAD_COUNT),
    clog::ObClogAggreRunnable::TOTAL_TASK)
TG_DEF(TransMigrate, TransMigrate, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(4, 1), 10000)
TG_DEF(UsrLocUpdTask, UsrLocUpdTask, "", TG_DYNAMIC, DEDUP_QUEUE,
    ThreadCountPair(GCONF.location_refresh_thread_count, 2),
    !lib::is_mini_mode() ? share::ObLocationCacheQueueSet::USER_TASK_QUEUE_SIZE
                         : share::ObLocationCacheQueueSet::MINI_MODE_USER_TASK_QUEUE_SIZE,
    !lib::is_mini_mode() ? share::ObLocationCacheQueueSet::USER_TASK_MAP_SIZE
                         : share::ObLocationCacheQueueSet::MINI_MODE_USER_TASK_MAP_SIZE,
    ObDedupQueue::TOTAL_LIMIT, ObDedupQueue::HOLD_LIMIT, ObDedupQueue::PAGE_SIZE, ObModIds::OB_LOCATION_CACHE_QUEUE)
TG_DEF(WeakReadService, WeakReadService, "", TG_DYNAMIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(TransTaskWork, TransTaskWork, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(transaction::ObThreadLocalTransCtx::MAX_BIG_TRANS_WORKER,
        transaction::ObThreadLocalTransCtx::MINI_MODE_MAX_BIG_TRANS_WORKER),
    transaction::ObThreadLocalTransCtx::MAX_BIG_TRANS_TASK)
TG_DEF(DDLTaskExecutor1, DDLTaskExecutor1, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(rootserver::ObRSBuildIndexScheduler::DEFAULT_THREAD_CNT,
        rootserver::ObRSBuildIndexScheduler::DEFAULT_THREAD_CNT))
TG_DEF(DDLTaskExecutor2, DDLTaskExecutor2, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(
        storage::ObBuildIndexScheduler::DEFAULT_THREAD_CNT, storage::ObBuildIndexScheduler::MINI_MODE_THREAD_CNT))
TG_DEF(DDLTaskExecutor3, DDLTaskExecutor3, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(storage::ObRetryGhostIndexScheduler::DEFAULT_THREAD_CNT,
        storage::ObRetryGhostIndexScheduler::DEFAULT_THREAD_CNT))
TG_DEF(FetchLogEngine, FetchLogEngine, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(clog::CLOG_FETCH_LOG_THREAD_COUNT, clog::MINI_MODE_CLOG_FETCH_LOG_THREAD_COUNT),
    clog::CLOG_FETCH_LOG_TASK_QUEUE_SIZE)
TG_DEF(GTSWorker, GTSWorker, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(transaction::ObGtsWorker::THREAD_NUM, transaction::ObGtsWorker::MINI_MODE_THREAD_NUM),
    transaction::ObGtsWorker::MAX_TASK_NUM)
TG_DEF(BRPC, BRPC, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(obrpc::ObBatchRpc::MAX_THREAD_COUNT, obrpc::ObBatchRpc::MINI_MODE_THREAD_COUNT))
TG_DEF(LeaseQueueTh, LeaseQueueTh, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(
        observer::ObSrvDeliver::LEASE_TASK_THREAD_CNT, observer::ObSrvDeliver::MINI_MODE_LEASE_TASK_THREAD_CNT))
TG_DEF(DDLQueueTh, DDLQueueTh, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(observer::ObSrvDeliver::DDL_TASK_THREAD_CNT, observer::ObSrvDeliver::DDL_TASK_THREAD_CNT))
TG_DEF(MysqlQueueTh, MysqlQueueTh, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(
        observer::ObSrvDeliver::MYSQL_TASK_THREAD_CNT, observer::ObSrvDeliver::MINI_MODE_MYSQL_TASK_THREAD_CNT))
TG_DEF(DiagnoseQueueTh, DiagnoseQueueTh, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(observer::ObSrvDeliver::MYSQL_DIAG_TASK_THREAD_CNT,
        observer::ObSrvDeliver::MINI_MODE_MYSQL_DIAG_TASK_THREAD_CNT))
TG_DEF(RLogQuery, RLogQuery, "", TG_STATIC, QUEUE_THREAD,
    ThreadCountPair(clog::ObRemoteLogQueryEngine::REMOTE_LOG_QUERY_ENGINE_THREAD_NUM,
        clog::ObRemoteLogQueryEngine::MINI_MODE_REMOTE_LOG_QUERY_ENGINE_THREAD_NUM),
    clog::ObRemoteLogQueryEngine::REMOTE_LOG_QUERY_ENGINE_TASK_NUM)
TG_DEF(IdxBuild, IdxBuild, "", TG_STATIC, ASYNC_TASK_QUEUE, ThreadCountPair(16, 1), 4 << 10)
TG_DEF(ClogHisRep, ClogHisRep, "", TG_STATIC, OB_THREAD_POOL,
    ThreadCountPair(clog::ObClogHistoryReporter::THREAD_NUM, clog::ObClogHistoryReporter::MINI_MODE_THREAD_NUM))
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
TG_DEF(MinorScan, MinorScan, "", TG_STATIC, TIMER)
TG_DEF(MajorScan, MajorScan, "", TG_STATIC, TIMER)
TG_DEF(EXTLogWash, EXTLogWash, "", TG_STATIC, TIMER)
TG_DEF(LineCache, LineCache, "", TG_STATIC, TIMER)
TG_DEF(LocalityReload, LocalityReload, "", TG_STATIC, TIMER)
TG_DEF(MemstoreGC, MemstoreGC, "", TG_STATIC, TIMER)
TG_DEF(CLOGReqMinor, CLOGReqMinor, "", TG_STATIC, TIMER)
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
TG_DEF(CFC, CFC, "", TG_STATIC, TIMER)
TG_DEF(CCDF, CCDF, "", TG_STATIC, TIMER)
TG_DEF(LogMysqlPool, LogMysqlPool, "", TG_STATIC, TIMER)
TG_DEF(TblCliSqlPool, TblCliSqlPool, "", TG_STATIC, TIMER)
TG_DEF(QueryExecCtxGC, QueryExecCtxGC, "", TG_STATIC, OB_THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(DtlDfc, DtlDfc, "", TG_STATIC, TIMER)
TG_DEF(DDLRetryGhostIndex, DDLRetryGhostIndex, "", TG_STATIC, TIMER)
TG_DEF(StoreFileAutoExtend, StoreFileAutoExtend, "", TG_STATIC, TIMER)
TG_DEF(TTLScheduler, TTLScheduler, "", TG_STATIC, TIMER)
TG_DEF(CTASCleanUpTimer, CTASCleanUpTimer, "", TG_STATIC, TIMER)
#endif
