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

#define USING_LOG_PREFIX STORAGE

#include "storage/ob_partition_service.h"

#include "share/ob_debug_sync.h"
#include "clog/ob_clog_config.h"
#include "clog/ob_clog_history_reporter.h"
#include "clog/ob_clog_mgr.h"
#include "clog/ob_partition_log_service.h"
#include "common/storage/ob_freeze_define.h"
#include "lib/container/ob_bit_set.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/thread/thread_mgr.h"
#include "memtable/ob_lock_wait_mgr.h"
#include "ob_table_mgr.h"
#include "ob_warm_up_request.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_index_status_table_operator.h"
#include "share/ob_locality_priority.h"
#include "share/ob_locality_table_operator.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_partition_modify.h"
#include "share/ob_remote_sql_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_server_blacklist.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_replica_filter.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
#include "share/stat/ob_table_stat.h"
#include "sql/ob_end_trans_callback.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_all_server_tracer.h"
#include "storage/ob_build_index_scheduler.h"
#include "storage/ob_build_index_task.h"
#include "storage/ob_dup_replica_checker.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "storage/ob_i_partition_component_factory.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_macro_meta_replay_map.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_pg_index.h"
#include "storage/ob_pg_index.h"
#include "storage/ob_pg_log.h"
#include "storage/ob_range_iterator.h"
#include "storage/ob_server_checkpoint_writer.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_table_scan_iterator.h"
#include "storage/ob_tenant_config_mgr.h"
#include "storage/transaction/ob_trans_result.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/transaction/ob_weak_read_util.h"
#include "storage/ob_interm_macro_mgr.h"
#include "common/log/ob_log_cursor.h"
#include "storage/ob_pg_sstable_garbage_collector.h"
#include "rootserver/ob_root_utils.h"
#include "storage/ob_file_system_util.h"
#include "storage/ob_pg_storage.h"
#include "share/backup/ob_backup_backupset_operator.h"
#include "observer/table/ob_table_ttl_manager.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::common::hash;
using namespace oceanbase::clog;
using namespace oceanbase::blocksstable;
using namespace oceanbase::memtable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;
using namespace oceanbase::logservice;
using namespace oceanbase::obrpc;
using namespace oceanbase::rootserver;
using namespace oceanbase::election;

namespace storage {

#define AUDIT_PARTITION_V2(mem_ctx, op, count)                                        \
  do {                                                                                \
    if (OB_SUCC(ret) && GCONF.enable_record_trace_log) {                              \
      int tmp_ret = OB_SUCCESS;                                                       \
      if (OB_ISNULL((mem_ctx))) {                                                     \
        tmp_ret = OB_ERR_UNEXPECTED;                                                  \
        STORAGE_LOG(WARN, "mem_ctx is null", K(tmp_ret));                             \
      } else if (OB_SUCCESS != (tmp_ret = mem_ctx->audit_partition((op), (count)))) { \
        STORAGE_LOG(WARN, "fail audit partition", K(tmp_ret));                        \
      }                                                                               \
    }                                                                                 \
  } while (0)

const char* ObReplicaOpArg::get_replica_op_type_str() const
{
  const char* str = "";
  switch (type_) {
    case ADD_REPLICA_OP:
      str = "ADD_REPLICA_OP";
      break;
    case MIGRATE_REPLICA_OP:
      str = "MIGRATE_REPLICA_OP";
      break;
    case REBUILD_REPLICA_OP:
      str = "REBUILD_REPLICA_OP";
      break;
    case CHANGE_REPLICA_OP:
      str = "CHANGE_REPLICA_OP";
      break;
    case REMOVE_REPLICA_OP:
      str = "REMOVE_REPLICA_OP";
      break;
    case RESTORE_REPLICA_OP:
      str = "RESTORE_REPLICA_OP";
      break;
    case COPY_GLOBAL_INDEX_OP:
      str = "COPY_GLOBAL_INDEX_OP";
      break;
    case COPY_LOCAL_INDEX_OP:
      str = "COPY_LOCAL_INDEX_OP";
      break;
    case RESTORE_FOLLOWER_REPLICA_OP:
      str = "RESTORE_FOLLOWER_REPLICA_OP";
      break;
    case BACKUP_REPLICA_OP:
      str = "BACKUP_REPLICA_OP";
      break;
    case FAST_MIGRATE_REPLICA_OP:
      str = "FAST_MIGRATE_REPLICA_OP";
      break;
    case VALIDATE_BACKUP_OP:
      str = "VALIDATE_BACKUP_OP";
      break;
    case BACKUP_BACKUPSET_OP:
      str = "BACKUP_BACKUPSET_OP";
      break;
    case BACKUP_ARCHIVELOG_OP:
      str = "BACKUP_ARCHIVELOG_OP";
      break;
    case RESTORE_STANDBY_OP:
      str = "RESTORE_STANDBY_OP";
      break;
    case LINK_SHARE_MAJOR_OP:
      str = "LINK_SHARE_MAJOR_OP";
      break;
    // case BUILD_ONLY_IN_MEMBER_LIST_OP:
    //  str = "BUILD_ONLY_IN_MEMBER_LIST_OP";
    //  break;
    default:
      str = "UNKOWN_REPLICA_OP";
  }
  return str;
}

/**
 * -----------------------------------------------------------ObPartitionService-----------------------------------------------------------------
 */
ObPartitionService::ObPartitionService()
    : ObPartitionMetaRedoModule(),
      is_running_(false),
      batch_rpc_(),
      tx_rpc_proxy_(),
      dup_table_rpc_proxy_(),
      clog_rpc_proxy_(),
      pts_rpc_proxy_(),
      rs_rpc_proxy_(NULL),
      srv_rpc_proxy_(NULL),
      pts_rpc_(),
      clog_mgr_(NULL),
      election_mgr_(NULL),
      rp_eg_wrapper_(NULL),
      rs_cb_(NULL),
      location_cache_(NULL),
      rs_mgr_(NULL),
      freeze_async_worker_(),
      reload_locality_task_(),
      purge_retire_memstore_task_(),
      clog_required_minor_freeze_task_(),
      ext_log_service_stream_timer_task_(),
      line_cache_timer_task_(),
      self_addr_(),
      migrate_retry_queue_thread_(),
      cb_queue_thread_(),
      large_cb_queue_thread_(),
      slog_writer_thread_pool_(),
      warm_up_service_(NULL),
      bandwidth_throttle_(NULL),
      trans_version_lock_(),
      global_max_decided_trans_version_(0),
      garbage_collector_(),
      dup_replica_checker_(),
      gts_mgr_(),
      gts_source_(),
      total_partition_cnt_(0),
      auto_part_scheduler_()
{}

ObPartitionService::~ObPartitionService()
{
  destroy();
}

int ObPartitionService::init(const blocksstable::ObStorageEnv& env, const ObAddr& self_addr,
    ObPartitionComponentFactory* cp_fty, ObMultiVersionSchemaService* schema_service,
    ObIPartitionLocationCache* location_cache, ObRsMgr* rs_mgr, ObIPartitionReport* rs_cb,
    rpc::frame::ObReqTransport* req_transport, obrpc::ObBatchRpc* batch_rpc, ObCommonRpcProxy& rs_rpc_proxy,
    ObSrvRpcProxy& srv_rpc_proxy, ObMySQLProxy& sql_proxy, ObRemoteSqlProxy& remote_sql_proxy,
    ObAliveServerTracer& server_tracer, ObInOutBandwidthThrottle& bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "partition service is inited.", K_(is_inited), K(ret));
  } else if (!self_addr.is_valid() || !env.is_valid() || NULL == cp_fty || NULL == schema_service ||
             NULL == location_cache || NULL == rs_mgr || NULL == rs_cb || NULL == req_transport) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid arguments",
        K(self_addr),
        K(env),
        K(cp_fty),
        K(schema_service),
        K(location_cache),
        K(rs_mgr),
        K(rs_cb),
        K(req_transport),
        K(ret));
  } else if (OB_FAIL(ObServerBlacklist::get_instance().init(self_addr, req_transport, batch_rpc))) {
    STORAGE_LOG(WARN, "fail to init ObServerBlacklist", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::Blacklist, ObServerBlacklist::get_instance()))) {
    STORAGE_LOG(WARN, "fail to start ObServerBlacklist", K(ret));
  } else if (OB_FAIL(tx_rpc_proxy_.init(req_transport, self_addr))) {
    STORAGE_LOG(WARN, "fail to init transaction rpc proxy", K(ret));
  } else if (OB_FAIL(dup_table_rpc_proxy_.init(req_transport, self_addr))) {
    STORAGE_LOG(WARN, "fail to init dup table rpc proxy", K(ret));
  } else if (OB_FAIL(clog_rpc_proxy_.init(req_transport, self_addr))) {
    STORAGE_LOG(WARN, "fail to init clog rpc proxy", K(ret));
  } else if (OB_FAIL(pts_rpc_proxy_.init(req_transport, self_addr))) {
    STORAGE_LOG(WARN, "fail to init partition service rpc proxy", K(ret));
  } else if (OB_FAIL(xa_proxy_.init(req_transport, self_addr))) {
    STORAGE_LOG(WARN, "fail to init xa rpc proxy", K(ret));
  } else if (OB_FAIL(pts_rpc_.init(&pts_rpc_proxy_, this, self_addr, &rs_rpc_proxy))) {
    STORAGE_LOG(WARN, "fail to init partition service rpc", K(ret));
  } else if (OB_FAIL(iter_allocator_.init(1024 * 1024 * 1024, 512 * 1024, common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init iter allocator, ", K(ret));
  } else if (OB_FAIL(partition_map_.init(ObModIds::OB_PG_PARTITION_MAP))) {
    STORAGE_LOG(WARN, "Fail to init pg_partition map", K(ret));
  } else if (OB_FAIL(pg_mgr_.init(cp_fty))) {
    STORAGE_LOG(WARN, "Fail to init partition mgr, ", K(ret));
  } else if (OB_FAIL(pg_index_.init())) {
    STORAGE_LOG(WARN, "Fail to init partition group index", K(ret));
  } else if (OB_FAIL(INTERM_MACRO_MGR.init())) {
    LOG_WARN("fail to init interm macro mgr", K(ret));
  } else if (OB_FAIL(ObPGSSTableGarbageCollector::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init pg sstable garbage collector", K(ret));
  } else if (OB_FAIL(ObPGMemoryGarbageCollector::get_instance().init(cp_fty))) {
    STORAGE_LOG(WARN, "fail to init pg memory garbage collector", K(ret));
  } else if (OB_FAIL(SLOGGER.init(env.log_spec_.log_dir_, env.log_spec_.max_log_size_, &slog_filter_))) {
    STORAGE_LOG(WARN, "Fail to init ObBaseStorageLogger, ", K(ret));
  } else if (OB_FAIL(OB_STORE_CACHE.init(env.index_cache_priority_,
                 env.user_block_cache_priority_,
                 env.user_row_cache_priority_,
                 env.fuse_row_cache_priority_,
                 env.bf_cache_priority_,
                 env.bf_cache_miss_count_threshold_))) {
    STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(env.data_dir_));
  } else if (OB_FAIL(ObStoreFileSystemWrapper::init(env, *this))) {
    STORAGE_LOG(WARN, "init store file system failed.", K(ret), K(env));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.init())) {
    STORAGE_LOG(WARN, "fail to init server file mgr", K(ret));
  } else if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_PARTITION, this))) {
    STORAGE_LOG(WARN, "Fail to register partition meta image, ", K(ret));
  } else if (OB_FAIL(ObClockGenerator::init())) {
    STORAGE_LOG(WARN, "create clock generator failed", K(ret));
  } else if (NULL == (txs_ = cp_fty->get_trans_service())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "create transaction service failed", K(ret));
  } else if (OB_FAIL(txs_->init(self_addr,
                 location_cache,
                 this,
                 &tx_rpc_proxy_,
                 batch_rpc,
                 &dup_table_rpc_proxy_,
                 &xa_proxy_,
                 schema_service,
                 &OB_TS_MGR,
                 server_tracer))) {
    STORAGE_LOG(WARN, "create transaction service failed", K(ret));
  } else if (OB_FAIL(memtable::get_global_lock_wait_mgr().init())) {
    STORAGE_LOG(WARN, "create lock wait mgr failed", K(ret));
  } else if (NULL == (clog_mgr_ = cp_fty->get_clog_mgr())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "create clog manager object failed.", K(ret));
  } else if (NULL == (election_mgr_ = cp_fty->get_election_mgr())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "create election manager failed", K(ret));
  } else if (OB_FAIL(election_mgr_->init(self_addr, batch_rpc, clog_mgr_))) {
    STORAGE_LOG(WARN, "create election manager failed", K(ret));
  } else if (NULL == (rp_eg_ = cp_fty->get_replay_engine())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "create replay engine failed", K(ret));
  } else if (NULL == (rp_eg_wrapper_ = cp_fty->get_replay_engine_wrapper())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "create replay engine wrapper failed", K(ret));
  } else if (OB_FAIL(ObPartitionMigrator::get_instance().init(
                 pts_rpc_proxy_, pts_rpc_, cp_fty, this, bandwidth_throttle, location_cache, schema_service))) {
    STORAGE_LOG(WARN, "init partition migrator failed.", K(ret));
  } else if (OB_FAIL(ObPartGroupMigrator::get_instance().init(this, cp_fty))) {
    STORAGE_LOG(WARN, "init partition group migrator failed.", K(ret));
  } else if (OB_FAIL(ObPartitionScheduler::get_instance().init(*this, *schema_service, *rs_cb))) {
    STORAGE_LOG(WARN, "Fail to init ObPartitionScheduler, ", K(ret));
  } else if (OB_FAIL(ObTmpFileManager::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init temp file manager", K(ret));
  } else if (OB_FAIL(ObMemstoreAllocatorMgr::get_instance().init())) {
    STORAGE_LOG(WARN, "failed to init ObMemstoreAllocatorMgr", K(ret));
  } else if (OB_FAIL(cb_async_worker_.init(this))) {
    STORAGE_LOG(WARN, "init clog cb async worker failed", K(ret));
  } else if (OB_FAIL(freeze_async_worker_.init(this))) {
    STORAGE_LOG(WARN, "init freeze async worker failed", K(ret));
  } else if (OB_FAIL(locality_manager_.init(self_addr, GCTX.sql_proxy_, GCTX.remote_sql_proxy_))) {
    STORAGE_LOG(WARN, "locality manager failed", K(ret));
  } else if (OB_FAIL(split_worker_.init(this))) {
    STORAGE_LOG(WARN, "partition split worker init failed", K(ret));
  } else if (OB_FAIL(refresh_locality_task_queue_.init(
                 1, "LocltyRefTask", REFRESH_LOCALITY_TASK_NUM, REFRESH_LOCALITY_TASK_NUM))) {
    STORAGE_LOG(WARN, "fail to initialize refresh locality task queue", K(ret));
  } else if (OB_FAIL(reload_locality_task_.init(this))) {
    STORAGE_LOG(WARN, "init reload locality task failed", K(ret));
  } else if (OB_FAIL(purge_retire_memstore_task_.init())) {
    STORAGE_LOG(WARN, "init purge_retire_memstore_task failed", K(ret));
  } else if (OB_FAIL(clog_required_minor_freeze_task_.init(clog_mgr_))) {
    STORAGE_LOG(WARN, "init clog_required_minor_freeze_task failed", K(ret));
  } else if (OB_FAIL(ext_log_service_stream_timer_task_.init(clog_mgr_->get_external_log_service()))) {
    STORAGE_LOG(WARN, "ext_log_service stream_timer_task init error", K(ret));
  } else if (OB_FAIL(line_cache_timer_task_.init(clog_mgr_->get_external_log_service()))) {
    STORAGE_LOG(WARN, "line_cache_timer_task_ init error", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::EXTLogWash))) {
    STORAGE_LOG(WARN, "fail to initialize timer", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::LineCache))) {
    STORAGE_LOG(WARN, "fail to initialize line_cache_timer", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::LocalityReload))) {
    STORAGE_LOG(WARN, "fail to initialize locality timer");
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::MemstoreGC))) {
    STORAGE_LOG(WARN, "fail to initialize gc timer");
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::CLOGReqMinor))) {
    STORAGE_LOG(WARN, "fail to initialize clog required freeze timer");
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::RebuildRetry))) {
    STORAGE_LOG(WARN, "fail to initialize rebuild_retry_timer");
  } else if (OB_FAIL(migrate_retry_queue_thread_.init(this, lib::TGDefIDs::PartSerMigRetryQt))) {
    STORAGE_LOG(WARN, "fail to initialize callback queue thread", K(ret));
  } else if (OB_FAIL(cb_queue_thread_.init(this, lib::TGDefIDs::PartSerCb))) {
    STORAGE_LOG(WARN, "fail to initialize callback queue thread", K(ret));
  } else if (OB_FAIL(large_cb_queue_thread_.init(this, lib::TGDefIDs::PartSerLargeCb))) {
    STORAGE_LOG(WARN, "fail to initialize callback queue thread", K(ret));
  } else if (OB_FAIL(slog_writer_thread_pool_.init(this, lib::TGDefIDs::PartSerSlogWr))) {
    STORAGE_LOG(WARN, "fail to initialize callback queue thread", K(ret));
  } else if (OB_FAIL(clog::ObClogHistoryReporter::get_instance().init(GCTX.sql_proxy_, self_addr))) {
    STORAGE_LOG(WARN, "fail to initialize clog history reporter", K(ret));
  } else if (NULL == (warm_up_service_ = cp_fty->get_warm_up_service())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "failed to create warm up service", K(ret));
  } else if (OB_FAIL(warm_up_service_->init(pts_rpc_, server_tracer, bandwidth_throttle))) {
    STORAGE_LOG(WARN, "failed to init warm up service", K(ret));
  } else if (OB_FAIL(ObTableMgr::get_instance().init())) {
    LOG_WARN("failed to init ObTableMgr", K(ret));
  } else if (OB_FAIL(ObBuildIndexScheduler::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init ObBuildIndexScheduler", K(ret));
  } else if (OB_FAIL(ObRetryGhostIndexScheduler::get_instance().init())) {
    LOG_WARN("fail to init ObRetryGhostIndexScheduler", K(ret));
  } else if (OB_FAIL(ObFreezeInfoMgrWrapper::init(sql_proxy, remote_sql_proxy))) {
    STORAGE_LOG(WARN, "fail to init ObFreezeInfoSnapshotMgr", K(ret));
  } else if (OB_FAIL(garbage_collector_.init(this, txs_, schema_service, GCTX.srv_rpc_proxy_, &sql_proxy, self_addr))) {
    STORAGE_LOG(WARN, "garbage_collector_ init failed", K(ret));
  } else if (OB_FAIL(partition_worker_.init(this))) {
    STORAGE_LOG(WARN, "partition worker init failed", K(ret));
  } else if (OB_FAIL(trans_checkpoint_worker_.init(this))) {
    STORAGE_LOG(WARN, "transaction checkpoint worker init failed", K(ret));
  } else if (OB_FAIL(clog_aggre_runnable_.init(this))) {
    STORAGE_LOG(WARN, "clog aggre runnable init failed", K(ret));
  } else if (OB_FAIL(dup_replica_checker_.init(schema_service, &locality_manager_))) {
    STORAGE_LOG(WARN, "dup replica checker init failed", K(ret));
  } else if (OB_FAIL(gts_mgr_.init(GCTX.srv_rpc_proxy_, &sql_proxy, self_addr))) {
    STORAGE_LOG(WARN, "gts_mgr_ init failed", K(ret));
  } else if (OB_FAIL(gts_source_.init(GCTX.srv_rpc_proxy_, &sql_proxy, self_addr))) {
    STORAGE_LOG(WARN, "gts_source_ init failed", K(ret));
  } else if (OB_FAIL(rebuild_replica_service_.init(*this, pts_rpc_proxy_))) {
    STORAGE_LOG(WARN, "failed to init rebuild replica service", K(ret));
  } else if (OB_FAIL(sstable_garbage_collector_.init(txs_))) {
    STORAGE_LOG(WARN, "failed to init sstable_garbage_collector_", K(ret));
  } else if (OB_FAIL(auto_part_scheduler_.init(this, schema_service))) {
    STORAGE_LOG(WARN, "failed to init auto_part_scheduler_", K(ret));
  } else if (OB_FAIL(ObServerCheckpointWriter::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init ObServerCheckpointWriter", K(ret));
  } else {
    // TODO FIXME, we do not have config now,
    // so temporarily define it here
    replayengine::ObILogReplayEngine::ObLogReplayEngineConfig rp_config;
    clog::CLogMgrConfig clog_config;
    static const int64_t PLS_TLIMIT = (int64_t)(1) << 35;  // 32G
    static const int64_t PLS_HLIMIT = (int64_t)(1) << 21;  // 2G
    static const int64_t PLS_PSIZE = 1 << 16;              // 64K
    static const int64_t TLIMIT = 1 << 12;                 // 4K
    static const int64_t HLIMIT = 1 << 12;                 // 4K
    static const int64_t PSIZE = 1 << 12;                  // 4K
    static const int64_t FILE_SIZE = 1 << 26;              // 64M
    static const int64_t TIMEOUT = 100000000;              // 10s
    static const int64_t QUEUE_SIZE = 1024;
    clog_config.pls_tlimit_ = PLS_TLIMIT;
    clog_config.pls_hlimit_ = PLS_HLIMIT;
    clog_config.pls_psize_ = PLS_PSIZE;
    clog_config.tlimit_ = TLIMIT;
    clog_config.hlimit_ = HLIMIT;
    clog_config.psize_ = PSIZE;
    clog_config.le_config_.log_dir_ = env.clog_dir_;
    clog_config.le_config_.index_log_dir_ = env.ilog_dir_;
    clog_config.le_config_.log_shm_path_ = env.clog_shm_path_;
    clog_config.le_config_.index_log_shm_path_ = env.ilog_shm_path_;
    clog_config.le_config_.cache_name_ = "clog_cache";
    clog_config.le_config_.index_cache_name_ = "index_clog_cache";
    clog_config.le_config_.cache_priority_ = env.clog_cache_priority_;
    clog_config.le_config_.index_cache_priority_ = env.index_clog_cache_priority_;
    clog_config.le_config_.file_size_ = FILE_SIZE;
    clog_config.le_config_.read_timeout_ = TIMEOUT;
    clog_config.le_config_.write_timeout_ = TIMEOUT;
    clog_config.le_config_.write_queue_size_ = QUEUE_SIZE;

    const int64_t config_clog_disk_buffer_cnt = ObServerConfig::get_instance()._ob_clog_disk_buffer_cnt;
    const int64_t final_clog_disk_buffer_cnt = config_clog_disk_buffer_cnt;
    const int64_t CLOG_DISK_BUFFER_THRESHOLD = 2000;
    clog_config.le_config_.disk_log_buffer_cnt_ =
        std::min(final_clog_disk_buffer_cnt, CLOG_DISK_BUFFER_THRESHOLD);  // 2000 is the upper limit

    clog_config.le_config_.disk_log_buffer_size_ = CLOG_DISK_BUFFER_SIZE;
    clog_config.le_config_.ethernet_speed_ = env.ethernet_speed_;

    static const int64_t ALLOCATOR_TOTAL_LIMIT = 5L * 1024L * 1024L * 1024L;
    static const int64_t ALLOCATOR_HOLD_LIMIT = static_cast<int64_t>(1.5 * 1024L * 1024L * 1024L);
    static const int64_t ALLOCATOR_PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
    rp_config.total_limit_ = ALLOCATOR_TOTAL_LIMIT;
    rp_config.hold_limit_ = ALLOCATOR_HOLD_LIMIT;
    rp_config.page_size_ = ALLOCATOR_PAGE_SIZE;

    self_addr_ = self_addr;
    if (OB_FAIL(rp_eg_->init(txs_, this, rp_config))) {
      STORAGE_LOG(WARN, "create replay engine failed", K(ret));
    } else if (OB_FAIL(rp_eg_wrapper_->init(rp_eg_))) {
      STORAGE_LOG(WARN, "create replay engine wrapper failed", K(ret));
    } else if (OB_FAIL(clog_mgr_->init(this,
                   rp_eg_wrapper_,
                   election_mgr_,
                   self_addr,
                   batch_rpc,
                   &clog_rpc_proxy_,
                   &sql_proxy,
                   clog_config))) {
      STORAGE_LOG(WARN, "create clog manager object failed", K(ret));
    } else {
      iter_allocator_.set_label(ObModIds::OB_PARTITION_SERVICE);
      cp_fty_ = cp_fty;
      schema_service_ = schema_service;
      file_mgr_ = &OB_SERVER_FILE_MGR;
      location_cache_ = location_cache;
      rs_mgr_ = rs_mgr;
      rs_cb_ = rs_cb;
      init_tenant_bit_set();
      rs_rpc_proxy_ = &rs_rpc_proxy;
      srv_rpc_proxy_ = &srv_rpc_proxy;
      bandwidth_throttle_ = &bandwidth_throttle;
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    destroy();
  }

  return ret;
}

ObPartitionService::ReloadLocalityTask::ReloadLocalityTask() : is_inited_(false), ptt_svr_(NULL)
{}

int ObPartitionService::ReloadLocalityTask::init(ObPartitionService* ptt_svr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ReloadLocalityTask init twice", K(ret));
  } else if (OB_ISNULL(ptt_svr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ptt_svr));
  } else {
    is_inited_ = true;
    ptt_svr_ = ptt_svr;
  }

  return ret;
}

void ObPartitionService::ReloadLocalityTask::destroy()
{
  is_inited_ = false;
  ptt_svr_ = NULL;
}

void ObPartitionService::ReloadLocalityTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ReloadLocalityTask not init", K(ret));
  } else if (OB_FAIL(ptt_svr_->add_refresh_locality_task())) {
    STORAGE_LOG(WARN, "runTimer to refresh locality_info fail", K(ret));
  } else {
    STORAGE_LOG(INFO, "runTimer to refresh locality_info", K(ret));
  }
}

ObPartitionService::PurgeRetireMemstoreTask::PurgeRetireMemstoreTask() : is_inited_(false)
{}

int ObPartitionService::PurgeRetireMemstoreTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "PurgeRetireMemstoreTask init twice", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObPartitionService::PurgeRetireMemstoreTask::destroy()
{
  is_inited_ = false;
}

void ObPartitionService::PurgeRetireMemstoreTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "PurgeRetireMemstoreTask not init", K(ret));
  } else {
    const bool is_disk_full = OB_FILE_SYSTEM.is_disk_full();
    ObIPartitionGroupIterator* iter = NULL;
    if (NULL == (iter = ObPartitionService::get_instance().alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
    } else {
      ObIPartitionGroup* partition = NULL;
      ObPartitionKey pkey;
      ObPartitionReplicaState replica_state = OB_UNKNOWN_REPLICA;

      STORAGE_LOG(INFO, "begin run do purge retire memstore task");
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "scan next partition failed", K(ret));
          }
          break;
        } else if (OB_UNLIKELY(NULL == partition)) {
          ret = OB_PARTITION_NOT_EXIST;
          STORAGE_LOG(WARN, "get partition failed", K(ret));
        } else {
          pkey = partition->get_partition_key();
          if (OB_FAIL(partition->get_replica_state(replica_state))) {
            STORAGE_LOG(WARN, "get partition replica state error", K(ret), K(pkey));
          } else if (OB_NORMAL_REPLICA != replica_state && OB_RESTORE_REPLICA != replica_state) {
            // skip
          } else if (OB_FAIL(partition->retire_warmup_store(is_disk_full))) {
            STORAGE_LOG(WARN, "fail to retire sorted store", K(ret), K(pkey));
          } else {
            // do nothing
          }
        }

        ret = OB_SUCCESS;  // override error code in purpose
      }

      ObPartitionService::get_instance().revert_pg_iter(iter);
      STORAGE_LOG(INFO, "finish purge retire memstores", K(ret));
    }
  }
}

ObPartitionService::ClogRequiredMinorFreezeTask::ClogRequiredMinorFreezeTask() : is_inited_(false), clog_mgr_(NULL)
{}

int ObPartitionService::ClogRequiredMinorFreezeTask::init(clog::ObICLogMgr* clog_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ClogRequiredMinorFreezeTask init twice", K(ret));
  } else if (OB_UNLIKELY(NULL == clog_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(clog_mgr));
  } else {
    clog_mgr_ = clog_mgr;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObPartitionService::ClogRequiredMinorFreezeTask::destroy()
{
  is_inited_ = false;
  clog_mgr_ = NULL;
}

void ObPartitionService::ClogRequiredMinorFreezeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  clog::NeedFreezePartitionArray ptt_array;
  int64_t ptt_cnt = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ClogRequiredMinorFreezeTask not init", K(ret));
  } else if (OB_ISNULL(clog_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "clog_mgr is null", K(ret));
  } else if (OB_FAIL(clog_mgr_->get_need_freeze_partition_array(ptt_array))) {
    STORAGE_LOG(WARN, "get_need_freeze_partition_array from clog mgr failed", K(ret));
  } else if ((ptt_cnt = ptt_array.count()) > 0) {
    STORAGE_LOG(INFO, "clog required minor freeze", K(ptt_array));
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < ptt_cnt; ++i) {
      const clog::NeedFreezePartition& ptt = ptt_array.at(i);
      const ObPartitionKey& pkey = ptt.get_partition_key();
      ObIPartitionGroupGuard guard;
      bool need_freeze = false;
      if (OB_UNLIKELY(!ptt.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "need freeze partition is invalid", K(ptt), K(ret));
      } else if (OB_SUCCESS != (tmp_ret = ObPartitionService::get_instance().get_partition(pkey, guard))) {
        STORAGE_LOG(WARN, "get partition failed", K(pkey), K(tmp_ret));
      } else if (OB_ISNULL(guard.get_partition_group())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition is NULL", K(pkey), K(tmp_ret));
      } else if (OB_SUCCESS !=
                 (tmp_ret = guard.get_partition_group()->need_minor_freeze(ptt.get_log_id(), need_freeze))) {
        STORAGE_LOG(WARN, "check if need freeze failed", K(ptt), K(tmp_ret));
      } else if (need_freeze) {
        if (OB_SUCCESS != (tmp_ret = ObPartitionService::get_instance().minor_freeze(pkey, true))) {
          STORAGE_LOG(WARN, "do minor freeze failed", K(pkey), K(tmp_ret));
        }
      } else {
        // raise the priority of this minor merge task
        if (OB_SUCCESS != (tmp_ret = guard.get_partition_group()->set_emergency_release())) {
          STORAGE_LOG(WARN, "fail to set emergency release", K(pkey), K(tmp_ret));
        }
      }
    }
  }
}

int ObPartitionService::set_region(const ObPartitionKey& pkey, clog::ObIPartitionLogService* pls)
{
  int ret = OB_SUCCESS;
  ObRegion region;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || OB_ISNULL(pls)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), KP(pls));
  } else if (OB_FAIL(locality_manager_.get_local_region(region))) {
    STORAGE_LOG(WARN, "get local region fail", K(ret), K(region));
  } else if (region.is_empty()) {
    STORAGE_LOG(WARN, "local region is empty, skip", K(ret), K(pkey), K(region));
  } else if (OB_FAIL(pls->set_region(region))) {
    STORAGE_LOG(WARN, "set region fail", K(ret), K(pkey), K(region));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::is_local_zone_read_only(bool& is_read_only)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100) {
    // read only zone was deprecated after 225
    is_read_only = false;
  } else if (OB_FAIL(locality_manager_.is_local_zone_read_only(is_read_only))) {
    STORAGE_LOG(WARN, "get local zone read_only info fail", K(ret), K(is_read_only));
  }
  return ret;
}

int ObPartitionService::set_partition_region_priority(const ObPGKey& pkey, clog::ObIPartitionLogService* pls)
{
  int ret = OB_SUCCESS;
  uint64_t region_priority = UINT64_MAX;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || OB_ISNULL(pls)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), KP(pls));
  } else if (OB_FAIL(locality_manager_.get_region_priority(pkey.get_tenant_id(), region_priority))) {
    STORAGE_LOG(WARN, "ObLocalityManager get_region_priority error", K(ret), K(pkey));
  } else {
    pls->set_zone_priority(region_priority);
    STORAGE_LOG(INFO, "ObPartitionLogService set_region_priority", K(pkey), K(region_priority));
  }

  return ret;
}

int ObPartitionService::get_locality_info(ObLocalityInfo& locality_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL((locality_manager_.get_locality_info(locality_info)))) {
    STORAGE_LOG(WARN, "get locality_info fail", K(ret), K(locality_info));
  }
  return ret;
}

int ObPartitionService::start()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "start partition service");
  DEBUG_SYNC(BEFORE_IS_IN_SYNC_SET);
  bool repeat = true;
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(ERROR, "partition service not inited, cannot start.");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_running_)) {
    ret = OB_ERROR;
    STORAGE_LOG(WARN, "partition service has already been running", K(ret));
  } else if (OB_FAIL(txs_->start())) {
    STORAGE_LOG(ERROR, "fail to start transaction service", K(ret));
  } else if (OB_FAIL(memtable::get_global_lock_wait_mgr().start())) {
    STORAGE_LOG(WARN, "lock_wait_mgr start fail", K(ret));
  } else if (OB_FAIL(election_mgr_->start())) {
    STORAGE_LOG(ERROR, "election_mgr start all fail", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.start())) {
    LOG_WARN("tmp file manager start fail", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.start())) {
    STORAGE_LOG(ERROR, "fail to open store file, ", K(ret));
  } else if (OB_FAIL(prepare_all_partitions())) {
    STORAGE_LOG(ERROR, "fail to create_partition_memstore.", K(ret));
  } else if (OB_FAIL(ObTableMgr::get_instance().schedule_gc_task())) {
    STORAGE_LOG(ERROR, "failed to schedule table mgr gc task", K(ret));
  } else if (OB_FAIL(clog_mgr_->start())) {
    STORAGE_LOG(ERROR, "fail to start clog manager");
  } else if (OB_FAIL(split_worker_.start())) {
    STORAGE_LOG(ERROR, "partition split worker start failed", K(ret));
  } else if (OB_FAIL(freeze_async_worker_.start())) {
    STORAGE_LOG(ERROR, "fail to start freeze async worker", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(
                 lib::TGDefIDs::MemstoreGC, purge_retire_memstore_task_, PURGE_RETIRE_MEMSTORE_INTERVAL, repeat))) {
    STORAGE_LOG(ERROR, "fail to schedule purge retire memstore task");
  } else if (OB_FAIL(
                 TG_SCHEDULE(lib::TGDefIDs::LocalityReload, reload_locality_task_, RELOAD_LOCALITY_INTERVAL, repeat))) {
    STORAGE_LOG(ERROR, "fail to schedule reload locality task");
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::CLOGReqMinor,
                 clog_required_minor_freeze_task_,
                 CLOG_REQUIRED_MINOR_FREEZE_INTERVAL,
                 repeat))) {
    STORAGE_LOG(ERROR, "fail to schedule clog required minor freeze task");
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::EXTLogWash,
                 ext_log_service_stream_timer_task_,
                 ObExtLogService::StreamTimerTask::TIMER_INTERVAL,
                 repeat))) {
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::LineCache,
                 line_cache_timer_task_,
                 ObExtLogService::LineCacheTimerTask::TIMER_INTERVAL,
                 repeat))) {
    STORAGE_LOG(WARN, "fail to schedule line_cache_timer_task", K(ret));
  } else if (OB_FAIL(ObFreezeInfoMgrWrapper::start())) {
    STORAGE_LOG(WARN, "failed to start ObFreezeInfoSnapshotMgr", K(ret));
  } else if (OB_FAIL(garbage_collector_.start())) {
    STORAGE_LOG(WARN, "failed to start garbage_collector", K(ret));
  } else if (OB_FAIL(partition_worker_.start())) {
    STORAGE_LOG(WARN, "partition worker start failed", K(ret));
  } else if (OB_FAIL(trans_checkpoint_worker_.start())) {
    STORAGE_LOG(WARN, "transaction checkpoint worker start failed", K(ret));
  } else if (OB_FAIL(clog_aggre_runnable_.start())) {
    STORAGE_LOG(WARN, "clog aggre runnable start failed", K(ret));
  } else if (GCONF._enable_ha_gts_full_service && OB_FAIL(gts_mgr_.start())) {
    STORAGE_LOG(WARN, "gts_mgr_ start failed", K(ret));
  } else if (GCONF._enable_ha_gts_full_service && OB_FAIL(gts_source_.start())) {
    STORAGE_LOG(WARN, "gts_source_ start failed", K(ret));
  } else if (OB_FAIL(sstable_garbage_collector_.start())) {
    LOG_WARN("failed to start sstable garbage collector", K(ret));
  } else if (OB_FAIL(auto_part_scheduler_.start())) {
    STORAGE_LOG(WARN, "fail to start auto_part_scheduler_", K(ret));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.start())) {
    STORAGE_LOG(WARN, "fail to start server file mgr", K(ret));
  } else {
    STORAGE_LOG(INFO, "partition service start successfully, wait for scan clog file.");
    is_running_ = true;
  }

  return ret;
}

int ObPartitionService::wait_start_finish()
{
  int ret = OB_SUCCESS;
  common::ObLogCursor slog_cursor;
  if (OB_FAIL(wait_clog_replay_over())) {
    LOG_ERROR("wait for scan clog file failed.", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObPartitionScheduler::get_instance().enable_auto_checkpoint();
    STORAGE_LOG(INFO, "partition service start successfully!");
  }
  return ret;
}

int ObPartitionService::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "partition service not inited, cannot stop.", K(ret));
  } else {
    STORAGE_LOG(INFO, "stop partition service");
    is_running_ = false;
    TG_STOP(lib::TGDefIDs::MemstoreGC);
    rebuild_replica_service_.stop();
    ObTableMgr::get_instance().stop();
    ObFreezeInfoMgrWrapper::stop();
    ObBuildIndexScheduler::get_instance().stop();
    ObPartitionMigrator::get_instance().stop();
    ObPartGroupMigrator::get_instance().stop();
    ObPartitionScheduler::get_instance().stop();
    TG_STOP(lib::TGDefIDs::PartSerMigRetryQt);
    TG_STOP(lib::TGDefIDs::PartSerCb);
    TG_STOP(lib::TGDefIDs::PartSerLargeCb);
    TG_STOP(lib::TGDefIDs::PartSerSlogWr);
    TG_STOP(lib::TGDefIDs::RebuildRetry);
    TG_STOP(lib::TGDefIDs::CLOGReqMinor);
    TG_STOP(lib::TGDefIDs::LocalityReload);
    TG_STOP(lib::TGDefIDs::EXTLogWash);
    TG_STOP(lib::TGDefIDs::CLOGReqMinor);
    TG_STOP(lib::TGDefIDs::LineCache);
    freeze_async_worker_.stop();
    split_worker_.stop();
    partition_worker_.stop();
    trans_checkpoint_worker_.stop();
    clog_aggre_runnable_.stop();
    garbage_collector_.stop();
    STORAGE_LOG(INFO, "stop clog manager");
    if (clog_mgr_) {
      clog_mgr_->stop();
    } else {
      STORAGE_LOG(ERROR, "null ptr");
    }
    if (rp_eg_) {
      rp_eg_->stop();
    } else {
      STORAGE_LOG(ERROR, "null ptr");
    }
    if (election_mgr_) {
      election_mgr_->stop();
    } else {
      STORAGE_LOG(ERROR, "null ptr");
    }
    memtable::get_global_lock_wait_mgr().stop();
    if (txs_) {
      txs_->stop();
    } else {
      STORAGE_LOG(ERROR, "null ptr");
    }
    TG_STOP(lib::TGDefIDs::Blacklist);
    TG_STOP(lib::TGDefIDs::BRPC);
    if (NULL != warm_up_service_) {
      warm_up_service_->stop();
    } else {
      STORAGE_LOG(ERROR, "null ptr");
    }
    ObClogHistoryReporter::get_instance().stop();
    ObPGSSTableGarbageCollector::get_instance().stop();
    ObPGMemoryGarbageCollector::get_instance().stop();
    OB_SERVER_FILE_MGR.stop();
    OB_FILE_SYSTEM.stop();
    if (GCONF._enable_ha_gts_full_service) {
      gts_mgr_.stop();
      gts_source_.stop();
    }
    sstable_garbage_collector_.stop();
    auto_part_scheduler_.stop();
  }
  return ret;
}

int ObPartitionService::wait()
{
  int ret = OB_SUCCESS;

  ObFreezeInfoMgrWrapper::wait();
  ObBuildIndexScheduler::get_instance().wait();
  ObPartitionMigrator::get_instance().wait();
  ObPartGroupMigrator::get_instance().wait();
  ObPartitionScheduler::get_instance().wait();

  if (clog_mgr_) {
    clog_mgr_->wait();
  } else {
    STORAGE_LOG(ERROR, "null ptr");
  }
  // re_eg_wrapper_->wait();
  if (rp_eg_) {
    rp_eg_->wait();
  } else {
    STORAGE_LOG(ERROR, "null ptr");
  }
  TG_WAIT(lib::TGDefIDs::MemstoreGC);
  if (election_mgr_) {
    if (OB_FAIL(election_mgr_->wait())) {
      STORAGE_LOG(ERROR, "wait election manager error", K(ret));
    }
  } else {
    STORAGE_LOG(ERROR, "null ptr");
  }
  if (txs_) {
    txs_->wait();
  } else {
    STORAGE_LOG(ERROR, "null ptr");
  }
  TG_WAIT(lib::TGDefIDs::Blacklist);
  TG_WAIT(lib::TGDefIDs::BRPC);
  memtable::get_global_lock_wait_mgr().wait();
  TG_WAIT(lib::TGDefIDs::PartSerMigRetryQt);
  TG_WAIT(lib::TGDefIDs::PartSerCb);
  TG_WAIT(lib::TGDefIDs::PartSerLargeCb);
  TG_WAIT(lib::TGDefIDs::PartSerSlogWr);
  TG_WAIT(lib::TGDefIDs::RebuildRetry);
  TG_WAIT(lib::TGDefIDs::CLOGReqMinor);
  TG_WAIT(lib::TGDefIDs::LocalityReload);
  TG_WAIT(lib::TGDefIDs::EXTLogWash);
  TG_WAIT(lib::TGDefIDs::LineCache);
  split_worker_.wait();
  partition_worker_.wait();
  trans_checkpoint_worker_.wait();
  clog_aggre_runnable_.wait();
  ObClogHistoryReporter::get_instance().wait();
  garbage_collector_.wait();
  ObPGSSTableGarbageCollector::get_instance().wait();
  ObPGMemoryGarbageCollector::get_instance().wait();
  OB_SERVER_FILE_MGR.wait();
  OB_FILE_SYSTEM.wait();
  if (GCONF._enable_ha_gts_full_service) {
    gts_mgr_.wait();
    gts_source_.wait();
  }
  sstable_garbage_collector_.wait();
  auto_part_scheduler_.wait();

  return ret;
}

int ObPartitionService::reload_config()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(OB_STORE_CACHE.set_bf_cache_miss_count_threshold(GCONF.bf_cache_miss_count_threshold))) {
    LOG_WARN("set bf_cache_miss_count_threshold fail", K(ret));
  } else if (OB_FAIL(OB_STORE_CACHE.reset_priority(GCONF.index_cache_priority,
                 GCONF.user_block_cache_priority,
                 GCONF.user_row_cache_priority,
                 GCONF.fuse_row_cache_priority,
                 GCONF.bf_cache_priority))) {
    LOG_WARN("set cache priority fail, ", K(ret));
  }
  return ret;
}

void ObPartitionService::rollback_partition_register(const ObPartitionKey& pkey, bool rb_txs, bool rb_rp_eg)
{
  int err = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    err = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "partition service is not initialized", K(err));
  } else {
    if (rb_rp_eg) {
      if (OB_SUCCESS != (err = rp_eg_->remove_partition(pkey))) {
        if (OB_PARTITION_NOT_EXIST != err) {
          STORAGE_LOG(WARN, "rollback partition already been removed", K(err), K(pkey));
        } else if (OB_NOT_RUNNING != err) {
          STORAGE_LOG(ERROR, "rollback partition from replay engine failed", K(pkey), K(err));
        }
      }
    }
    if (rb_txs) {
      const bool graceful = false;
      if (OB_SUCCESS != (err = txs_->remove_partition(pkey, graceful))) {
        if (OB_PARTITION_NOT_EXIST == err) {
          STORAGE_LOG(WARN, "rollback partition already been removed", K(err), K(pkey));
        } else if (OB_NOT_RUNNING != err) {
          STORAGE_LOG(ERROR, "rollback partition from transaction service failed", K(pkey), K(err), K(graceful));
        }
      }
    }
  }
}

// Prepare data structures of all partitions after reboot
int ObPartitionService::prepare_all_partitions()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;
  ObSavedStorageInfoV2 info;
  ObBaseStorageInfo& clog_info = info.get_clog_info();
  ObDataStorageInfo& data_info = info.get_data_info();
  ObVersion mem_version(2, 0);  // TODO : fix or remove
  bool need_restore_trans_table = true;

  if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "cannot allocate scan iterator.", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      need_restore_trans_table = true;
      bool txs_add_success = false;
      bool rp_add_success = false;
      ObIPartitionLogService* pls = NULL;
      ObMigrateStatus migrate_status = OB_MIGRATE_STATUS_MAX;
      bool need_create_memtable = true;

      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "get next partition failed.", K(ret));
        }
      } else if (NULL == partition) {
        ret = OB_PARTITION_NOT_EXIST;
        STORAGE_LOG(WARN, "get partition failed", K(ret));
      } else if (OB_FAIL(partition->get_all_saved_info(info))) {
        STORAGE_LOG(WARN, "failed to get saved info", K(ret));
      } else if (OB_FAIL(partition->get_pg_storage().get_pg_migrate_status(migrate_status))) {
        LOG_WARN("failed to get_migrate_status", K(ret), "pkey", partition->get_partition_key());
      } else if (FALSE_IT(need_create_memtable = !(partition->get_pg_storage().is_restoring_base_data() ||
                                                   partition->get_pg_storage().is_restoring_standby()))) {
      } else if (need_create_memtable && OB_FAIL(partition->create_memtable())) {
        LOG_WARN("failed to create memtable for online", K(ret));
      } else {
        if (!need_create_memtable) {
          need_restore_trans_table = false;
        }
        const ObPartitionKey& pkey = partition->get_partition_key();
        const ObReplicaType replica_type = partition->get_replica_type();
        const ObReplicaProperty replica_property = partition->get_replica_property();

        STORAGE_LOG(INFO, "prepare partition", K(pkey), K(replica_type));

        // NB: BE CAREFUL!!! Think carefully before changing the order of register
        // register to transaction service
        if (OB_SUCC(ret)) {
          if (OB_FAIL(txs_->add_partition(pkey))) {
            STORAGE_LOG(WARN, "fail to add partition to transaction service", K(pkey), K(ret));
          } else {
            txs_add_success = true;
            int64_t restore_snapshot_version = OB_INVALID_TIMESTAMP;
            uint64_t last_restore_log_id = OB_INVALID_ID;
            int64_t last_restore_log_ts = OB_INVALID_TIMESTAMP;
            if (OB_FAIL(txs_->update_publish_version(pkey, data_info.get_publish_version(), true))) {
              STORAGE_LOG(WARN, "failed to set publish version", K(data_info), K(ret));
            } else if (OB_FAIL(partition->get_pg_storage().get_restore_replay_info(
                           last_restore_log_id, last_restore_log_ts, restore_snapshot_version))) {
              STORAGE_LOG(WARN, "failed to get_restore_replay_info", K(pkey), K(ret));
            } else if (OB_FAIL(txs_->update_restore_replay_info(
                           pkey, restore_snapshot_version, last_restore_log_id, last_restore_log_ts))) {
              STORAGE_LOG(WARN,
                  "failed to update_restore_replay_info",
                  K(restore_snapshot_version),
                  K(last_restore_log_id),
                  K(last_restore_log_ts),
                  K(ret));
            } else { /*do nothing*/
            }
          }
        }

        // update clog_history_info
        if (OB_SUCC(ret)) {
          int64_t tmp_ret = OB_SUCCESS;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = partition->report_clog_history_online()))) {
            STORAGE_LOG(WARN, "fail to report clog history online", K(tmp_ret), K(pkey));
          }
        }

        // to replay engine, this partition should be replayed
        if (OB_SUCC(ret)) {
          if (OB_FAIL(rp_eg_->add_partition(pkey))) {
            STORAGE_LOG(WARN, "fail to add partition to replay engine", K(ret), K(pkey));
          } else {
            rp_add_success = true;
          }
        }

        // register to clog and election manager
        if (OB_SUCC(ret)) {
          pls = partition->get_log_service();
          if (NULL != pls) {
            // partition log service is member of partition, no need to remove if unsuccess
            if (OB_FAIL(clog_mgr_->assign_partition(pkey,
                    replica_type,
                    replica_property,
                    clog_info,
                    ObVersion(0, 0) /*freeze version*/,
                    partition->get_pg_storage().get_restore_state(),
                    pls))) {
              STORAGE_LOG(WARN, "fail to create partition for log manager", K(pkey), K(ret));

            } else if (REPLICA_RESTORE_DATA == partition->get_pg_storage().get_restore_state()) {
              // offline clog while restoring baseline, will be online after baseline restored
              if (OB_FAIL(pls->set_offline())) {
                STORAGE_LOG(WARN, "fail to set_offline", K(pkey), K(ret));
              } else {
                need_restore_trans_table = false;
              }
            } else {
              // do nothing
            }
          }
        }

        // set valid
        if (OB_SUCC(ret)) {
          if (OB_FAIL(partition->set_valid())) {
            STORAGE_LOG(WARN, "partition set valid failed", K(pkey), K(ret));
          } else if (OB_MIGRATE_STATUS_ADD_FAIL == migrate_status || OB_MIGRATE_STATUS_MIGRATE_FAIL == migrate_status) {
            LOG_WARN("migrate failed replica, offline it", "pkey", partition->get_partition_key(), K(migrate_status));
            if (OB_FAIL(partition->offline())) {
              LOG_WARN("failed to offline partition", K(ret), K(pkey));
            } else {
              need_restore_trans_table = false;
              LOG_INFO("succeed to offline migrate failed partition during reboot", K(pkey));
            }
          }
        } else {
          rollback_partition_register(pkey, txs_add_success, rp_add_success);
        }

        if (OB_SUCC(ret)) {
          if (need_restore_trans_table) {
            if (OB_FAIL(partition->get_pg_storage().restore_mem_trans_table())) {
              STORAGE_LOG(WARN, "failed to restore mem trans table", K(ret), K(pkey));
            }
          } else {
            FLOG_INFO("no need to restore mem trans table", K(pkey));
          }
        }
      }
    }
  }

  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  return ret;
}

int ObPartitionService::try_revoke_all_leader(const ObElection::RevokeType& revoke_type)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* pg = NULL;
  ObIPartitionLogService* pls = NULL;
  ObIPartitionGroupIterator* pg_iter = NULL;
  if (OB_ISNULL(pg_iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "partition mgr alloc scan iter failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      // traverse ObPartitionService
      if (OB_FAIL(pg_iter->get_next(pg)) || OB_ISNULL(pg)) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(WARN, "pg_iter get_next failed", K(ret), K(pg));
        }
      } else if (OB_UNLIKELY(!pg->is_valid()) || OB_ISNULL(pls = pg->get_log_service())) {
        CLOG_LOG(WARN,
            "pg is invalid while iterate",
            "partition_key",
            pg->get_partition_key(),
            "is_valid",
            pg->is_valid(),
            "state",
            pg->get_partition_state());
        // check ObServerBlackList and try revoke
      } else if (OB_FAIL(pls->check_and_try_leader_revoke(revoke_type))) {
        CLOG_LOG(WARN, "leader_revoke failed", K(ret), K(pls));
      } else {
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (NULL != pg_iter) {
    revert_pg_iter(pg_iter);
    pg_iter = NULL;
  }
  STORAGE_LOG(INFO, "try_revoke_all_leader caused by", K(revoke_type));
  return ret;
}

int ObPartitionService::destroy()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "partition service destroy");
  TG_DESTROY(lib::TGDefIDs::RebuildRetry);
  TG_DESTROY(lib::TGDefIDs::CLOGReqMinor);
  TG_DESTROY(lib::TGDefIDs::LocalityReload);
  TG_DESTROY(lib::TGDefIDs::LineCache);
  TG_DESTROY(lib::TGDefIDs::EXTLogWash);
  rebuild_replica_service_.destroy();
  INTERM_MACRO_MGR.destroy();
  ObBuildIndexScheduler::get_instance().destroy();
  ObPartitionMigrator::get_instance().destroy();
  ObPartGroupMigrator::get_instance().destroy();
  ObPartitionScheduler::get_instance().destroy();
  ObTmpFileManager::get_instance().destroy();

  if (is_running_) {
    if (OB_FAIL(stop())) {
      STORAGE_LOG(WARN, "ObPartitionService stop error", K(ret));
    } else if (OB_FAIL(wait())) {
      STORAGE_LOG(WARN, "ObPartitionService wait error", K(ret));
    } else {
      // do nothing
    }
  }
  if (NULL != warm_up_service_ && NULL != cp_fty_) {
    cp_fty_->free(warm_up_service_);
    warm_up_service_ = NULL;
  }

  // release partitions' data structures first, in case of core dump
  ObPGSSTableGarbageCollector::get_instance().destroy();
  ObPGMemoryGarbageCollector::get_instance().destroy();
  pg_mgr_.destroy();
  iter_allocator_.destroy();
  ObTableMgr::get_instance().destroy();
  if (NULL != cp_fty_) {
    if (NULL != clog_mgr_) {
      // clog_mgr_->destroy();
      cp_fty_->free(clog_mgr_);
      clog_mgr_ = NULL;
    }
    if (NULL != rp_eg_wrapper_) {
      cp_fty_->free(rp_eg_wrapper_);
      rp_eg_wrapper_ = NULL;
    }
    if (NULL != rp_eg_) {
      // rp_eg_->destroy();
      cp_fty_->free(rp_eg_);
      rp_eg_ = NULL;
    }
    if (NULL != txs_) {
      // txs_->destroy();
      cp_fty_->free(txs_);
      txs_ = NULL;
    }
    // destroy election_mgr after txs_
    if (NULL != election_mgr_) {
      // election_mgr_->destroy();
      cp_fty_->free(election_mgr_);
      election_mgr_ = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    ObClockGenerator::destroy();
    location_cache_ = NULL;
    rs_mgr_ = NULL;
  }
  if (OB_SUCC(ret)) {
    ObClogHistoryReporter::get_instance().destroy();
  }
  ObServerCheckpointWriter::get_instance().reset();
  ObServerBlacklist::get_instance().destroy();
  locality_manager_.destroy();
  split_worker_.destroy();
  garbage_collector_.destroy();
  partition_worker_.destroy();
  trans_checkpoint_worker_.destroy();
  OB_SERVER_FILE_MGR.destroy();
  ObStoreFileSystemWrapper::destroy();
  OB_STORE_CACHE.destroy();
  SLOGGER.destroy();
  clog_aggre_runnable_.destroy();
  gts_mgr_.destroy();
  gts_source_.destroy();
  total_partition_cnt_ = 0;
  sstable_garbage_collector_.destroy();
  auto_part_scheduler_.destroy();
  pg_index_.destroy();
  is_inited_ = false;
  return ret;
}

int ObPartitionService::create_new_partition(const common::ObPartitionKey& key, ObIPartitionGroup*& partition)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(key), K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret), K(is_running_));
  } else {
    if (NULL == (partition = cp_fty_->get_partition(key.get_tenant_id()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "fail to create partition");
    } else if (OB_FAIL(partition->init(key, cp_fty_, schema_service_, txs_, rp_eg_, this))) {
      STORAGE_LOG(WARN, "fail to create partition", K(ret), K(key));
    } else if (OB_FAIL(set_partition_region_priority(key, partition->get_log_service()))) {
      STORAGE_LOG(WARN, "fail to set partition region priority", K(key), K(ret));
    }

    // free component when creation failed
    if (OB_SUCCESS != ret && NULL != partition) {
      cp_fty_->free(partition);
      partition = NULL;
    }
  }
  return ret;
}

int ObPartitionService::add_new_partition(ObIPartitionGroupGuard& partition_guard)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  int tmp_ret = OB_SUCCESS;
  int64_t lsn = 0;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_ADD_NEW_PG_TO_PARTITION_SERVICE) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_ADD_NEW_PG_TO_PARTITION_SERVICE", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret), K(is_running_));
  } else if (OB_ISNULL(partition = (partition_guard.get_partition_group()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(partition), K(ret));
  } else if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().lock_tenant_balancer())) {
    STORAGE_LOG(WARN, "failed to lock tenant balancer", K(ret));
  } else {
    lib::ObMutexGuard structure_guard(structure_change_mutex_);
    const ObPartitionKey& pkey = partition->get_partition_key();
    ObStorageFile* file = partition->get_storage_file();
    if (is_partition_exist(pkey)) {
      ret = OB_ENTRY_EXIST;
      STORAGE_LOG(WARN, "The partition has exist, ", K(ret), K(pkey));
    } else if (OB_ISNULL(file)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, file must not be null", K(ret));
    } else if (OB_FAIL(inner_add_partition(
                   *partition, true /*need_check_tenant*/, false /*is_replay*/, false /*allow multi value*/))) {
      // should not happen
      STORAGE_LOG(WARN, "Fail to inner add partition, ", K(ret), K(pkey));
    } else if (OB_FAIL(file_mgr_->add_pg(ObTenantFileKey(file->get_tenant_id(), file->get_file_id()), pkey))) {
      STORAGE_LOG(WARN, "fail to add pg to file mgr", K(ret));
    } else if (OB_FAIL((SLOGGER.commit(lsn)))) {
      STORAGE_LOG(ERROR, "Fail to commit slog, ", K(ret), K(pkey));
      if (OB_SUCCESS != (tmp_ret = inner_del_partition(pkey))) {
        STORAGE_LOG(ERROR, "failed to inner_del_partition", K(tmp_ret), K(ret), K(pkey));
        ob_abort();
      } else if (OB_SUCCESS !=
                 (tmp_ret = file_mgr_->remove_pg(ObTenantFileKey(file->get_tenant_id(), file->get_file_id()), pkey))) {
        STORAGE_LOG(ERROR, "fail to remove pg", K(ret));
        ob_abort();
      }
    }
    omt::ObTenantNodeBalancer::get_instance().unlock_tenant_balancer();
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "add new partition.", K(ret), K(partition->get_partition_key()), K(lsn));
  } else {
    (void)SLOGGER.abort();
    STORAGE_LOG(WARN, "Fail to add new partition, ", K(ret));
  }
  return ret;
}

int ObPartitionService::add_partitions_to_mgr_(ObIPartitionArrayGuard &partitions)
{
  int ret = OB_SUCCESS;

  ObIPartitionGroup* partition = NULL;
  int tmp_ret = OB_SUCCESS;
  lib::ObMutexGuard guard(structure_change_mutex_);

  for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
    if (NULL == (partition = partitions.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "partition is NULL, ", K(ret), K(i));
    } else if (is_partition_exist(partition->get_partition_key())) {
      ret = OB_ENTRY_EXIST;
      STORAGE_LOG(WARN, "partition is exist, ", K(ret), K(partition->get_partition_key()));
    } else if (OB_ISNULL(partition->get_storage_file())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, storage file must not be null", K(ret), K(partition->get_partition_key()));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      ObStorageFile* file = partitions.at(i)->get_storage_file();
      if (OB_FAIL(inner_add_partition(
              *partitions.at(i), true /*need_check_tenant*/, false /*is_replay*/, false /*allow multi value*/))) {
        STORAGE_LOG(WARN, "fail to inner add partition, ", K(ret));
      } else {
        ObTenantFileKey file_key(file->get_tenant_id(), file->get_file_id());
        if (OB_FAIL(file_mgr_->add_pg(file_key, partitions.at(i)->get_partition_key()))) {
          LOG_WARN("fail to remove pg from file mgr", K(ret), K(file_key));
        }
      }
    }
  }

  // commit or abort slog
  if (OB_SUCC(ret)) {
    int64_t lsn = 0;
    if (OB_FAIL((SLOGGER.commit(lsn)))) {
      // fail to commit, rollback
      STORAGE_LOG(WARN, "fail to commit log.", K(ret));
    }
  } else {
    if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
      STORAGE_LOG(WARN, "commit logger failed to abort", K(tmp_ret));
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "add partition groups to partition service success", K(partitions));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_CREATE_PG_AFTER_ADD_PARTITIONS_TO_MGR) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObPartitionService::add_partitions_to_replay_engine_(ObIPartitionArrayGuard &partitions)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t start_log_id = 0;
  int64_t start_log_timestamp = 0;
  int64_t index = 0;
  ObIPartitionGroup* partition = NULL;
  ObPartitionKey pkey;
  for (index = 0; index < partitions.count() && OB_SUCC(ret); ++index) {
    pkey.reset();
    partition = partitions.at(index);
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "partition is NULL.", K(ret));
    } else {
      pkey = partition->get_partition_key();
      STORAGE_LOG(INFO,
          "update clog history info on add partitions to replay",
          K(pkey),
          K(start_log_id),
          K(start_log_timestamp));

      partition->report_clog_history_online();

      if (OB_FAIL(rp_eg_->add_partition(pkey))) {
        STORAGE_LOG(WARN, "fail to add partition to replay engine", K(ret), K(pkey));
      } else if (OB_FAIL(partition->set_valid())) {
        STORAGE_LOG(WARN, "partition set valid failed", K(pkey), K(ret));
      } else {
        if (OB_SUCCESS != (tmp_ret = partition->get_log_service()->switch_state()) && OB_EAGAIN != tmp_ret) {
          STORAGE_LOG(WARN, "switch state failed", K(pkey), K(tmp_ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do some rollback work
    for (index = index - 2; index >= 0; --index) {
      partition = partitions.at(index);
      if (NULL != partition) {
        if (OB_SUCCESS != (tmp_ret = rp_eg_->remove_partition(partition->get_partition_key(), partition))) {
          // should not happen
          STORAGE_LOG(
              ERROR, "rollback partition from replay engine failed", K(partition->get_partition_key()), K(tmp_ret));
        }
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_CREATE_PG_AFTER_ADD_PARTITIONS_TO_REPLAY_ENGINE) OB_SUCCESS;
  }
#endif
  return ret;
}

bool ObPartitionService::reach_tenant_partition_limit(
    const int64_t batch_cnt, const uint64_t tenant_id, const bool is_pg_arg)
{
  return reach_tenant_partition_limit_(batch_cnt, tenant_id, is_pg_arg);
}

bool ObPartitionService::reach_tenant_partition_limit_(
    const int64_t batch_cnt, const uint64_t tenant_id, const bool is_pg_arg)
{
  bool bool_ret = false;
  // total number of partitions = pg_count + stand_alone_partition_count
  const int64_t total_cnt = pg_mgr_.get_total_partition_count() + batch_cnt;
  int64_t pg_cnt = 0;
  int64_t stand_alone_partition_cnt = 0;
  const int64_t max_partition_num_per_server = GCONF._max_partition_cnt_per_server;
  if (is_pg_arg) {
    pg_cnt = pg_mgr_.get_pg_count() + batch_cnt;
  } else {
    stand_alone_partition_cnt = pg_mgr_.get_stand_alone_partition_count() + batch_cnt;
  }
  if (batch_cnt <= 0 || !is_valid_tenant_id(tenant_id)) {
    STORAGE_LOG(WARN, "invalid tenant_id", K(tenant_id));
  } else if (total_cnt > max_partition_num_per_server || pg_cnt > OB_MAX_PG_NUM_PER_SERVER ||
             stand_alone_partition_cnt > OB_MAX_SA_PARTITION_NUM_PER_SERVER) {
    STORAGE_LOG(WARN,
        "too many partitions",
        K(total_cnt),
        K(pg_cnt),
        K(stand_alone_partition_cnt),
        K(OB_MAX_PARTITION_NUM_PER_SERVER));
    bool_ret = true;
  } else {
    int ret = OB_SUCCESS;
    FETCH_ENTITY(TENANT_SPACE, tenant_id)
    {
      ObTenantStorageInfo* tenant_store_info = MTL_GET(ObTenantStorageInfo*);
      const int64_t cur_part_cnt = tenant_store_info->part_cnt_;
      const int64_t cur_pg_cnt = tenant_store_info->pg_cnt_;
      int64_t tenant_mem_limit = INT64_MAX;
      if (OB_SUCCESS != (ret = TMA_MGR_INSTANCE.get_tenant_limit(tenant_id, tenant_mem_limit))) {
        STORAGE_LOG(WARN, "get_tenant_mem_limit failed", K(ret), K(tenant_id));
      } else if (INT64_MAX != tenant_mem_limit) {
        int64_t cur_pg_mem_reserved = cur_pg_cnt * (SINGLE_PART_STATIC_MEM_COST + SINGLE_PG_DYNAMIC_MEM_COST);
        int64_t cur_part_mem_reserved = 0;
        // estimate how many partitions can be accommodated in memory
        int64_t estimated_part_num_limit = (tenant_mem_limit - cur_pg_mem_reserved) /
                                           (SINGLE_PART_STATIC_MEM_COST + (SINGLE_PART_DYNAMIC_MEM_COST / 10));
        bool is_reach_threshold = false;  // whether the number of partitions reaches the threshold
        if (estimated_part_num_limit < TENANT_PART_NUM_THRESHOLD) {
          // if the estimated value is less than the threshold, it will be calculated as if all partitions are active
          is_reach_threshold = false;
          cur_part_mem_reserved = cur_part_cnt * (SINGLE_PART_STATIC_MEM_COST + SINGLE_PART_DYNAMIC_MEM_COST);
        } else {
          is_reach_threshold = true;
          // greater than the threshold, calculated as 10% of the partitions are active
          cur_part_mem_reserved = cur_part_cnt * (SINGLE_PART_STATIC_MEM_COST + (SINGLE_PART_DYNAMIC_MEM_COST / 10));
        }
        int64_t acquired_mem = 0;
        if (is_pg_arg) {
          acquired_mem = batch_cnt * (SINGLE_PART_STATIC_MEM_COST + SINGLE_PG_DYNAMIC_MEM_COST);
        } else {
          if (is_reach_threshold) {
            acquired_mem = batch_cnt * (SINGLE_PART_STATIC_MEM_COST + (SINGLE_PART_DYNAMIC_MEM_COST / 10));
          } else {
            acquired_mem = batch_cnt * (SINGLE_PART_STATIC_MEM_COST + SINGLE_PART_DYNAMIC_MEM_COST);
          }
        }

        if ((cur_part_mem_reserved + cur_pg_mem_reserved + acquired_mem) > tenant_mem_limit) {
          bool_ret = true;
          if (REACH_TIME_INTERVAL(100 * 1000)) {
            STORAGE_LOG(WARN,
                "reach tenant max partition num limit",
                K(tenant_id),
                K(cur_part_cnt),
                K(cur_pg_cnt),
                K(batch_cnt),
                K(cur_part_mem_reserved),
                K(cur_pg_mem_reserved),
                K(acquired_mem),
                K(tenant_mem_limit));
          }
        }
      } else {
        // do nothing
      }
    }
    else
    {
      STORAGE_LOG(WARN, "switch tenant context failed", K(ret));
    }
  }
  return bool_ret;
}

int ObPartitionService::check_partition_state_(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg,
    common::ObIArray<obrpc::ObCreatePartitionArg>& target_batch_arg, common::ObIArray<int>& batch_res)
{
  int ret = OB_SUCCESS;

  if (batch_arg.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), "count", batch_arg.count());
  } else {
    // partition must be follower
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.count(); ++i) {
      ObIPartitionGroupGuard guard;
      ObIPartitionGroup* pg = NULL;
      bool can_create = true;
      if (!batch_arg.at(i).pg_key_.is_pg() || batch_arg.at(i).partition_key_.is_pg()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "unexpected pg or partition key", K(ret), K(i), K(batch_arg.at(i)));
      } else if (OB_FAIL(get_partition(batch_arg.at(i).pg_key_, guard))) {
        STORAGE_LOG(WARN, "get partition group error", K(ret), K(batch_arg.at(i)));
      } else if (OB_ISNULL(pg = guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition group is null, unexpected error", K(ret), K(batch_arg.at(i)));
      } else if (L_WORKING != pg->get_partition_state()) {
        STORAGE_LOG(WARN, "partition group is not master", K(ret), K(batch_arg.at(i)));
        // retry create pg partition:
        // 1. create table will fail if return code is not OB_NOT_MASTER
        // 2. when a follower partition found, skip all partition after, return with OB_NOT_MASTER
        for (int64_t res_index = i; OB_SUCC(ret) && res_index < batch_arg.count(); ++res_index) {
          if (OB_FAIL(batch_res.push_back(OB_NOT_MASTER))) {
            STORAGE_LOG(WARN, "tmp batch res push back error", K(ret));
          }
        }
        break;
      } else if (OB_FAIL(pg->get_pg_storage().check_can_create_pg_partition(can_create))) {
        STORAGE_LOG(WARN, "check can create pg partition error", K(ret), K(batch_arg.at(i)));
      } else if (!can_create) {
        ret = OB_OP_NOT_ALLOW;
        STORAGE_LOG(
            WARN, "create partition on empty pg with non-zero ddl_seq_num_ is not allowed", K(ret), K(batch_arg.at(i)));
      } else if (OB_FAIL(target_batch_arg.push_back(batch_arg.at(i)))) {
        STORAGE_LOG(WARN, "normal batch arg push back error", K(ret), K(batch_arg.at(i)));
      } else if (OB_FAIL(batch_res.push_back(OB_SUCCESS))) {
        STORAGE_LOG(WARN, "tmp batch res push back error", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPartitionService::create_batch_pg_partitions(
    const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res)
{
  ObTimeGuard tg(__func__, 100L * 1000L);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObIPartitionGroup*> partitions;
  ObArray<uint64_t> log_id_arr;
  common::ObArray<obrpc::ObCreatePartitionArg> target_batch_arg;
  batch_res.reuse();
  int64_t start_timestamp = ObTimeUtility::current_time();
  const int64_t CLOG_TIMEOUT = 10 * 1000 * 1000;
  bool revert_cnt = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret), K(is_running_));
  } else if (OB_FAIL(batch_res.reserve(batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserver res array failed, ", K(ret));
  } else if (OB_FAIL(target_batch_arg.reserve(batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserver batch arg array failed, ", K(ret));
  } else if (OB_FAIL(check_partition_state_(batch_arg, target_batch_arg, batch_res))) {
    STORAGE_LOG(WARN, "check partition state erorr", K(ret), K(batch_arg));
  } else if (target_batch_arg.count() == 0) {
    STORAGE_LOG(WARN, "partition group not master, need retry", K(batch_arg));
  } else if (OB_FAIL(try_inc_total_partition_cnt(target_batch_arg.count(), true /*need_check*/))) {
    LOG_WARN("failed to inc total_partition_cnt", K(ret));
  } else if (FALSE_IT(revert_cnt = true)) {
  } else if (OB_FAIL(partitions.reserve(target_batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserve array failed", K(ret), "count", target_batch_arg.count());
  } else if (OB_FAIL(log_id_arr.reserve(target_batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserver log id array failed, ", K(ret));
  } else if (OB_FAIL(submit_add_partition_to_pg_clog_(target_batch_arg, CLOG_TIMEOUT, log_id_arr))) {
    STORAGE_LOG(WARN, "write add partition to pg clog error", K(ret), K(target_batch_arg));
  } else {
    tg.click();
    for (int64_t i = 0; OB_SUCC(ret) && i < target_batch_arg.count(); ++i) {
      const obrpc::ObCreatePartitionArg& arg = target_batch_arg.at(i);
      const uint64_t add_partition_to_pg_log_id = log_id_arr.at(i);
      ObSavedStorageInfoV2 info;
      ObIPartitionGroupGuard guard;
      ObIPartitionGroup* pg = NULL;
      if (arg.table_schemas_.count() < 1) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid arguments", K(ret), K(arg.table_schemas_.count()));
      } else if (OB_FAIL(get_partition(target_batch_arg.at(i).pg_key_, guard))) {
        STORAGE_LOG(WARN, "get partition group error", K(ret), K(target_batch_arg.at(i)));
      } else if (OB_ISNULL(pg = guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition group is null, unexpected error", K(ret), K(target_batch_arg.at(i)));
      } else if (OB_FAIL(partitions.push_back(pg))) {
        STORAGE_LOG(WARN, "Fail to push pg to array, ", K(ret), KP(pg));
      } else if (OB_FAIL(pg->get_log_service()->get_saved_base_storage_info(info.get_clog_info()))) {
        STORAGE_LOG(WARN, "fail to get base storage info", K(ret));
      } else {
        ObVersion data_version;
        data_version.version_ = arg.memstore_version_;

        ObTablesHandle sstables_handle;
        ObBaseStorageInfo& clog_info = info.get_clog_info();
        ObDataStorageInfo& data_info = info.get_data_info();
        data_info.set_last_replay_log_id(clog_info.get_last_replay_log_id());
        data_info.set_publish_version(arg.frozen_timestamp_);
        data_info.set_schema_version(arg.schema_version_);
        data_info.set_created_by_new_minor_freeze();

        const ObTableSchema& first_table = arg.table_schemas_.at(0);
        const uint64_t data_table_id =
            first_table.is_global_index_table() ? first_table.get_data_table_id() : first_table.get_table_id();
        const bool in_slog_trans = false;
        const bool is_replay = false;
        if (OB_FAIL(create_sstables(arg, in_slog_trans, *pg, sstables_handle))) {
          STORAGE_LOG(WARN, "fail to create sstables", K(ret));
        } else if (OB_FAIL(pg->create_pg_partition(arg.partition_key_,
                       data_info.get_publish_version(),
                       data_table_id,
                       arg,
                       in_slog_trans,
                       is_replay,
                       add_partition_to_pg_log_id,
                       sstables_handle))) {
          STORAGE_LOG(WARN, "create pg partition failed.", K(ret));
        } else {
          revert_cnt = false;
        }
      }
    }
  }
  tg.click();

  if (OB_FAIL(ret) && revert_cnt) {
    try_inc_total_partition_cnt(-target_batch_arg.count(), false /*need check*/);
  }

  STORAGE_LOG(INFO,
      "batch create partition to pg result.",
      K(ret),
      "input count",
      batch_arg.count(),
      "cost",
      ObTimeUtility::current_time() - start_timestamp,
      K(batch_arg),
      K(target_batch_arg));

  return ret;
}

int ObPartitionService::submit_add_partition_to_pg_clog_(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg,
    const int64_t timeout, common::ObIArray<uint64_t>& log_id_arr)
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_PG_PARTITION_CHANGE_LOG_US = 1000;
  common::ObSEArray<ObAddPartitionToPGLogCb*, 16> cb_array;
  ObAddPartitionToPGLogCb* cb = NULL;

  if (batch_arg.count() <= 0 || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(timeout), K(batch_arg));
  } else if (OB_FAIL(cb_array.reserve(batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserver res array failed, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.count(); ++i) {
      ObIPartitionGroupGuard guard;
      cb = NULL;
      uint64_t log_id = 0;
      int64_t log_ts = 0;
      if (OB_FAIL(get_partition(batch_arg.at(i).pg_key_, guard)) || OB_ISNULL(guard.get_partition_group())) {
        STORAGE_LOG(WARN,
            "get partition error",
            K(ret),
            "pg_key",
            batch_arg.at(i).pg_key_,
            "pkey",
            batch_arg.at(i).partition_key_);
      } else if (OB_FAIL(guard.get_partition_group()->submit_add_partition_to_pg_log(
                     batch_arg.at(i), this, log_id, log_ts, cb))) {
        STORAGE_LOG(WARN, "submit add partition to pg log error", K(ret), K(batch_arg.at(i)));
      } else if (OB_FAIL(cb_array.push_back(cb))) {
        STORAGE_LOG(ERROR, "cb array push back error", K(ret), KP(cb), "arg", batch_arg.at(i));
      } else if (OB_FAIL(log_id_arr.push_back(log_id))) {
        STORAGE_LOG(ERROR, "log id push back error", K(ret), "arg", batch_arg.at(i));
      } else if (log_ts <= 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected log ts", K(ret), K(log_ts), "arg", batch_arg.at(i));
      } else {
        // use log_ts - 1 to create sstable
        const_cast<obrpc::ObCreatePartitionArg&>(batch_arg.at(i)).frozen_timestamp_ =
            max(log_ts - 1, batch_arg.at(i).frozen_timestamp_);
#ifdef ERRSIM
        ret = E(EventTable::EN_CREATE_PG_PARTITION_FAIL) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          const_cast<obrpc::ObCreatePartitionArg&>(batch_arg.at(i)).memstore_version_ =
              batch_arg.at(i).memstore_version_ - 1;
          ret = OB_SUCCESS;
        }
#endif
      }
    }
    // check all clog sync success, timeout in 10s
    ObPGWriteLogState write_log_state = CB_INIT;
    int64_t succ_cnt = 0;
    int64_t fail_cnt = 0;
    int64_t begin_us = ObTimeUtility::current_time();
    int64_t loop_start_us = 0;
    while (OB_SUCC(ret) && is_running_ && succ_cnt < batch_arg.count()) {
      loop_start_us = ObTimeUtility::current_time();
      write_log_state = CB_INIT;
      succ_cnt = 0;
      fail_cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < cb_array.count(); ++i) {
        if (OB_ISNULL(cb_array.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "is null, unexpected error", K(ret), KP(cb_array.at(i)));
        } else {
          write_log_state = cb_array.at(i)->get_write_clog_state();
          if (CB_SUCCESS == write_log_state) {
            succ_cnt++;
          } else if (CB_FAIL == write_log_state) {
            ret = OB_ERR_SYS;
            fail_cnt++;
          } else {
            // do nothing
          }
        }
      }
      // Wait until all clog sync success
      if (OB_SUCC(ret) && fail_cnt <= 0 && succ_cnt < batch_arg.count()) {
        int64_t loop_end_us = ObTimeUtility::current_time();
        if (loop_end_us - begin_us > timeout) {
          ret = OB_TIMEOUT;
          STORAGE_LOG(WARN, "write add partition to pg log timeout", K(ret), K(succ_cnt), K(batch_arg));
        } else if (loop_end_us - loop_start_us < CHECK_PG_PARTITION_CHANGE_LOG_US) {
          usleep((int)(CHECK_PG_PARTITION_CHANGE_LOG_US - (loop_end_us - loop_start_us)));
        } else {
          PAUSE();
        }
      }
    }
    // release completed callback, mark others as CB_END
    int64_t tmp_ret = OB_SUCCESS;
    bool can_release = false;

    for (int64_t i = 0; i < cb_array.count(); ++i) {
      can_release = false;
      if (NULL != cb_array.at(i)) {
        if (OB_SUCCESS != (tmp_ret = cb_array.at(i)->check_can_release(can_release))) {
          STORAGE_LOG(WARN, "check can release error", K(tmp_ret), KP(cb_array.at(i)));
        } else if (can_release) {
          op_free(cb_array.at(i));
          cb_array.at(i) = NULL;
          STORAGE_LOG(INFO, "release current cb success", K(i), "pkey", batch_arg.at(i).partition_key_);
        } else {
          STORAGE_LOG(INFO, "current cb can not be release right now", K(i), "pkey", batch_arg.at(i).partition_key_);
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::replay_add_partition_to_pg_clog(
    const ObCreatePartitionParam& arg, const uint64_t log_id, const int64_t log_ts, int64_t& schema_version)
{
  ObTimeGuard tg(__func__, 100L * 1000L);
  int ret = OB_SUCCESS;
  ObTablesHandle sstables_handle;
  int64_t start_timestamp = ObTimeUtility::current_time();
  const_cast<ObCreatePartitionParam&>(arg).frozen_timestamp_ = max(log_ts - 1, arg.frozen_timestamp_);
  common::ObSEArray<ObCreatePartitionParam, 1> batch_arg;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = NULL;
  common::ObReplicaType replica_type;
  bool can_replay = true;
  schema_version = arg.schema_version_;
  bool revert_cnt = false;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_REPLAY_ADD_PARTITION_TO_PG_CLOG) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to replay_add_partition_to_pg_clog", K(ret), K(log_id), K(arg));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(get_partition(arg.pg_key_, guard))) {
    STORAGE_LOG(WARN, "get partition group error", K(ret), K(arg));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition group is null, unexpected error", K(ret), K(arg));
  } else if (OB_FAIL(pg->get_pg_storage().check_can_replay_add_partition_to_pg_log(
                 arg.partition_key_, log_id, log_ts, arg.schema_version_, can_replay))) {
    STORAGE_LOG(WARN, "check can replay add partition to pg log error", K(ret), K(arg), K(log_id));
  } else if (!can_replay) {
    STORAGE_LOG(INFO, "no need to replay this log", K(arg), K(log_id));
  } else if (OB_FAIL(ObPartitionService::get_instance().check_standby_cluster_schema_condition(
                 arg.partition_key_, arg.schema_version_))) {
    STORAGE_LOG(WARN, "failed to check_standby_cluster_schema_condition", K(arg), K(log_id));
  } else if (OB_FAIL(try_inc_total_partition_cnt(1, false /*need check*/))) {
    LOG_ERROR("failed to inc total partition cnt", K(ret), K(arg), K(log_id));
  } else if (FALSE_IT(revert_cnt = true)) {
  } else if (OB_FAIL(batch_arg.push_back(arg))) {
    STORAGE_LOG(WARN, "batch arg push back error", K(ret), K(arg), K(log_id));
  } else if (OB_FAIL(pg->get_pg_storage().get_replica_type(replica_type))) {
    STORAGE_LOG(WARN, "get replica type error", K(ret), K(arg));
  } else if (ObReplicaTypeCheck::is_replica_with_ssstore(replica_type) &&
             OB_FAIL(create_sstables(arg, false, *pg, sstables_handle))) {
    STORAGE_LOG(WARN, "failed to create sstables", K(ret), K(arg));
  } else {
    tg.click();
    ObSavedStorageInfoV2 info;
    if (OB_FAIL(pg->get_log_service()->get_saved_base_storage_info(info.get_clog_info()))) {
      STORAGE_LOG(WARN, "fail to get base storage info", K(ret));
    } else {
      ObVersion data_version;
      data_version.version_ = arg.memstore_version_;

      ObBaseStorageInfo& clog_info = info.get_clog_info();
      ObDataStorageInfo& data_info = info.get_data_info();
      data_info.set_last_replay_log_id(clog_info.get_last_replay_log_id());
      data_info.set_publish_version(arg.frozen_timestamp_);
      data_info.set_schema_version(arg.schema_version_);
      data_info.set_created_by_new_minor_freeze();

      if (arg.schemas_.count() < 1) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid arguments", K(ret), K(arg.schemas_.count()));
      } else {
        tg.click();
        DEBUG_SYNC(BEFORE_CREATE_PG_PARTITION);
        const ObCreatePartitionMeta& first_table = arg.schemas_.at(0);
        const uint64_t data_table_id =
            first_table.is_global_index_table() ? first_table.get_data_table_id() : first_table.get_table_id();
        const bool in_slog_trans = false;
        const bool is_replay = true;
#ifdef ERRSIM
        ret = E(EventTable::EN_REPLAY_ADD_PARTITION_TO_PG_CLOG_AFTER_CREATE_SSTABLE) OB_SUCCESS;
#endif
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to replay add partition to pg clog after create sstable", K(ret));
        } else if (sstables_handle.get_count() > 0) {
          if (OB_FAIL(pg->create_pg_partition(arg.partition_key_,
                  data_info.get_publish_version(),
                  data_table_id,
                  arg,
                  in_slog_trans,
                  is_replay,
                  log_id,
                  sstables_handle))) {
            STORAGE_LOG(WARN, "failed to create pg partition", K(ret));
          } else {
            revert_cnt = false;
          }
        } else {
          tg.click();
          ObTablesHandle handle;
          if (OB_FAIL(pg->create_pg_partition(arg.partition_key_,
                  data_info.get_publish_version(),
                  data_table_id,
                  arg,
                  in_slog_trans,
                  is_replay,
                  log_id,
                  handle))) {
            STORAGE_LOG(WARN, "failed to create pg partition", K(ret));
          } else {
            revert_cnt = false;
          }
          tg.click();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    submit_pt_update_task_(arg.partition_key_);
    FLOG_INFO("replay add partition to pg clog succ", K(arg), "cost", ObTimeUtility::current_time() - start_timestamp);
    tg.click();
  } else {
    FLOG_WARN("replay add partition to pg clog error", K(arg), "cost", ObTimeUtility::current_time() - start_timestamp);
    if (revert_cnt) {
      try_inc_total_partition_cnt(-1, false /*need check*/);
    }
  }
  return ret;
}

int ObPartitionService::submit_pt_update_role_task(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_UNLIKELY(nullptr == rs_cb_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rs_cb_ ptr is null", KR(ret), KP(rs_cb_));
  } else if (OB_FAIL(rs_cb_->submit_pt_update_role_task(pkey))) {
    STORAGE_LOG(WARN, "fail to submit pt update task", KR(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::submit_pt_update_task(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(rs_cb_->submit_pt_update_task(pkey))) {
    STORAGE_LOG(WARN, "fail to submit pt update task", K(ret), K(pkey));
  } else {
    // do nothing
  }
  return ret;
}

void ObPartitionService::submit_pt_update_task_(const ObPartitionKey& pkey, const bool need_report_checksum)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;

  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey));
  } else if (pkey.is_pg()) {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed, ", K(ret), K(pkey));
    } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the partition is NULL, ", K(pkey));
    } else if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
      STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(pkey));
    }
    rs_cb_->submit_pt_update_task(pkey, need_report_checksum);
    rs_cb_->submit_pg_pt_update_task(pkeys);
  } else {
    if (OB_FAIL(pkeys.push_back(pkey))) {
      STORAGE_LOG(WARN, "pkeys push back error", K(ret), K(pkey));
    }
    rs_cb_->submit_pt_update_task(pkey, need_report_checksum);
    rs_cb_->submit_pg_pt_update_task(pkeys);
  }
  UNUSED(ret);
}

int ObPartitionService::submit_pg_pt_update_task_(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey));
  } else if (pkey.is_pg()) {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed, ", K(ret), K(pkey));
    } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the partition is NULL, ", K(pkey));
    } else if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
      STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(pkey));
    }
    rs_cb_->submit_pg_pt_update_task(pkeys);
  } else {
    if (OB_FAIL(pkeys.push_back(pkey))) {
      STORAGE_LOG(WARN, "pkeys push back error", K(ret), K(pkey));
    }
    rs_cb_->submit_pg_pt_update_task(pkeys);
  }
  return ret;
}

/**
  create partition group and create partitions
    (1) create pg
      a) begin;
      b) create ObPartitionGroup, write slog: REDO_LOG_ADD_PARTITION_GROUP
      c) commit/abort;

    (2) create pg partition
      a) begin;
      b) create empty sstable, write slog: CHANGE_MACRO_BLOCK_META, REDO_LOG_COMPELTE_SSTABLE
      c) create PgPartition, write slog: REDO_LOG_ADD_PARTITION_TO_PG
      d) create ObPartitionStore, write slog: REDO_LOG_CREATE_PARTITION_STORE
      e) commit/abort;
 */
int ObPartitionService::create_batch_partition_groups(
    const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<int>& batch_res)
{
  ObTimeGuard tg(__func__, 100L * 1000L);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionArrayGuard partition_array_guard;
  bool txs_add_success = false;
  bool rp_eg_add_success = false;
  ObArray<ObStorageFileHandle> files_handle;
  ObStorageFileHandle file_handle;
  bool has_mark_creating = false;
  batch_res.reuse();
  int64_t start_timestamp = ObTimeUtility::current_time();
  const uint64_t tenant_id = batch_arg.at(0).pg_key_.get_tenant_id();
  const bool is_pg = batch_arg.at(0).pg_key_.is_pg();
  ObSEArray<ObPartitionKey, 1> pkey_array;
  ObPartitionKey create_twice_key;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret), K(is_running_));
  } else if (batch_arg.count() <= 0 || !batch_arg.at(0).pg_key_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), "count", batch_arg.count());
  } else if (OB_FAIL(extract_pkeys(batch_arg, pkey_array))) {
    STORAGE_LOG(WARN, "fail to extract pkeys", K(ret));
  } else if (OB_FAIL(create_pg_checker_.mark_pg_creating(pkey_array, create_twice_key))) {
    STORAGE_LOG(WARN, "fail to mark partitions creating", K(ret), K(create_twice_key));
  } else if (FALSE_IT(has_mark_creating = true)) {
  } else if (OB_FAIL(remove_duplicate_partitions(batch_arg))) {
    STORAGE_LOG(WARN, "fail to remove dup partitions", K(ret));
  } else if (reach_tenant_partition_limit_(batch_arg.count(), tenant_id, is_pg)) {
    ret = OB_TOO_MANY_TENANT_PARTITIONS_ERROR;
    STORAGE_LOG(
        WARN, "reach tenant partition count limit, cannot create new partitions", K(ret), "count", batch_arg.count());
  } else if (FALSE_IT(partition_array_guard.set_pg_mgr(pg_mgr_))) {
  } else if (OB_FAIL(partition_array_guard.reserve(batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserve partition guard array failed", K(ret), "count", batch_arg.count());
  } else if (OB_FAIL(batch_res.reserve(batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserver res array failed, ", K(ret));
  } else if (OB_FAIL(batch_prepare_splitting(batch_arg))) {
    STORAGE_LOG(WARN, "batch prepare splitting failed", K(ret));
  } else {
    tg.click();
    for (int64_t i = 1; OB_SUCC(ret) && i < batch_arg.count(); ++i) {
      if (batch_arg.at(i).pg_key_.is_pg() != is_pg) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected batch args", K(i), K(batch_arg));
      } else {
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.count(); ++i) {
      const bool sys_table = is_sys_table(batch_arg.at(i).pg_key_.get_table_id());
      if (OB_FAIL(file_mgr_->alloc_file(tenant_id, sys_table, file_handle))) {
        LOG_WARN("fail to alloc file", K(ret));
      } else if (OB_FAIL(files_handle.push_back(file_handle))) {
        LOG_WARN("fail to add storage file", K(ret));
      } else {
        LOG_INFO("push back a file", K(file_handle.get_storage_file()->get_file_id()));
        file_handle.reset();
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      if (is_pg) {
        if (OB_FAIL(SLOGGER.begin(OB_LOG_CS_CREATE_PARTITION_GROUP))) {
          STORAGE_LOG(WARN, "fail to begin commit partition group log.", K(ret));
        }
      } else {
        if (OB_FAIL(SLOGGER.begin(OB_LOG_CS_CREATE_PARTITION))) {
          STORAGE_LOG(WARN, "fail to begin commit partition log.", K(ret));
        } else {
          // do nothing
        }
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch_register_trans_service(batch_arg))) {
        STORAGE_LOG(WARN, "fail to batch register trans service, ", K(ret));
      } else {
        txs_add_success = true;
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch_register_election_mgr_(is_pg, batch_arg, partition_array_guard, files_handle))) {
        STORAGE_LOG(WARN, "fail to batch register election mgr, ", K(ret));
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_partitions_to_mgr_(partition_array_guard))) {
        STORAGE_LOG(WARN, "add partition to mgr failed.", K(ret));
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_partitions_to_replay_engine_(partition_array_guard))) {
        STORAGE_LOG(WARN, "add partition to replay engine failed.", K(ret));
      } else {
        rp_eg_add_success = true;
      }
    }
    tg.click();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch_start_partition_election_(batch_arg, partition_array_guard))) {
        STORAGE_LOG(WARN, "batch start partition election failed", K(ret));
      }
    }
    tg.click();

    if (OB_FAIL(ret)) {
      // do some rollback work
      SLOGGER.abort();  // abort slogger transaction anyway
      tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < batch_arg.count(); ++i) {
        rollback_partition_register(batch_arg.at(i).partition_key_, txs_add_success, rp_eg_add_success);
      }
      for (int64_t i = 0; i < partition_array_guard.count(); ++i) {
        ObIPartitionGroup *rb_pg = partition_array_guard.at(i);
        if (OB_ISNULL(rb_pg)) {
          STORAGE_LOG(ERROR, "rollback pg is null", K(i));
          ob_abort();
        }
        const ObPGKey &rb_pkey = rb_pg->get_partition_key();
        tmp_ret = remove_pg_from_mgr(rb_pg, true/*write_slog*/);
        if (OB_SUCCESS == tmp_ret) {
          // partition object was released by partition service, do nothing
        } else if (OB_NOT_RUNNING == tmp_ret) {
          // partition service was stopped, ignore
        } else {
          STORAGE_LOG(ERROR, "fail to rollback pg", K(tmp_ret), K(rb_pkey), K(rb_pg));
          ob_abort();
        }
      }
    }
    tg.click();
  }
  if (has_mark_creating) {
    // will never failed.
    create_pg_checker_.mark_pg_created(pkey_array);
  }
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_OBSERVER_CREATE_PARTITION_FAILED) OB_SUCCESS;
  }
  int64_t cost = ObTimeUtility::current_time() - start_timestamp;
  tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < batch_arg.count(); ++i) {
    if (OB_SUCCESS != (tmp_ret = batch_res.push_back(ret))) {
      // should not happen
      STORAGE_LOG(ERROR, "save result list failed.", K(tmp_ret));
    }
  }

  int64_t tenant_part_cnt = 0;
  int64_t tenant_pg_cnt = 0;
  if (OB_INVALID_TENANT_ID != tenant_id) {
    FETCH_ENTITY(TENANT_SPACE, tenant_id)
    {
      ObTenantStorageInfo* tenant_store_info = MTL_GET(ObTenantStorageInfo*);
      tenant_part_cnt = tenant_store_info->part_cnt_;
      tenant_pg_cnt = tenant_store_info->pg_cnt_;
    }
    else
    {
      STORAGE_LOG(WARN, "switch tenant context failed", K(ret), K(tenant_id));
    }
  }
  tg.click();

  STORAGE_LOG(INFO,
      "batch create partition result detail.",
      K(ret),
      K(cost),
      K(tenant_id),
      K(tenant_part_cnt),
      K(tenant_pg_cnt),
      K(batch_arg));

  return ret;
}

int ObPartitionService::remove_duplicate_partitions(const ObIArray<ObCreatePartitionArg>& batch_arg)
{
  int ret = OB_SUCCESS;
  const int MAX_RETRY_TIMES = 50;
  const int USLEEP_TIME = 1000 * 100;  // 100 ms
  bool need_retry = true;
  int retry_times = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_arg.count(); ++i) {
      const ObCreatePartitionArg& arg = batch_arg.at(i);
      const ObPartitionKey& pkey = arg.partition_key_;

      if (OB_UNLIKELY(pkey != arg.pg_key_)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(ERROR, "pg_key not equal to pkey", K(ret), K(pkey), K(arg.pg_key_));
      } else if (arg.ignore_member_list_ && is_partition_exist(pkey) && OB_FAIL(remove_partition(pkey))) {
        STORAGE_LOG(WARN, "fail to remove duplicate partition", K(ret), K(pkey));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      while (need_retry && retry_times < MAX_RETRY_TIMES) {
        need_retry = false;
        for (int i = 0; i < batch_arg.count(); ++i) {
          const ObCreatePartitionArg& arg = batch_arg.at(i);
          const ObPartitionKey& pkey = arg.partition_key_;
          if (arg.ignore_member_list_ && (OB_ENTRY_EXIST == partition_map_.contains_key(pkey))) {
            need_retry = true;
            if (REACH_TIME_INTERVAL(100 * 1000)) {
              int tmp_ret = OB_SUCCESS;
              ObPGPartition *pg_partition = NULL;
              if (OB_SUCCESS != (tmp_ret = partition_map_.get(pkey, pg_partition))) {
                STORAGE_LOG(WARN, "partition still exist, need retry. but get partition failed", K(tmp_ret), K(pkey));
              } else {
                STORAGE_LOG(WARN, "partition still exist, need retry. the left partition is", K(pkey), K(pg_partition));
                partition_map_.revert(pg_partition);
              }
            }
            break;
          }
        }
        if (need_retry) {
          usleep(USLEEP_TIME);  // 100 ms
        }
        retry_times++;
      }
      if (need_retry && retry_times == MAX_RETRY_TIMES) {
        ret = OB_EAGAIN;
      }
    }
  }

  return ret;
}

int ObPartitionService::create_sstables(const ObCreatePartitionParam& create_partition_param, const bool in_slog_trans,
    ObIPartitionGroup& partition_group, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObITable::TableKey table_key;
  ObPGCreateSSTableParam create_sstable_param;
  int64_t total_count = 0;
  bool need_create = create_partition_param.need_create_sstable();
  total_count = create_partition_param.schemas_.count();
  for (int64_t i = 0; need_create && OB_SUCC(ret) && i < total_count; ++i) {
    table_key.reset();
    ObTableSchema table_schema;
    ObCreateSSTableParamWithPartition param;
    uint64_t table_id = create_partition_param.schemas_.at(i).get_table_id();
    ObCreatePartitionMeta meta;
    param.schema_ = &create_partition_param.schemas_.at(i);
    table_key.pkey_ = create_partition_param.partition_key_;
    param.progressive_merge_round_ = create_partition_param.schemas_.at(i).get_progressive_merge_round();
    table_key.table_type_ = ObITable::MAJOR_SSTABLE;

    if (OB_SUCC(ret)) {
      ObTableHandle sstable_handle;
      ObSSTable* sstable = NULL;
      bool new_created = false;
      table_key.table_id_ = table_id;
      table_key.trans_version_range_.multi_version_start_ = create_partition_param.frozen_timestamp_;
      table_key.trans_version_range_.base_version_ = ObVersionRange::MIN_VERSION;
      table_key.trans_version_range_.snapshot_version_ = create_partition_param.frozen_timestamp_;
      table_key.version_ = ObVersion(create_partition_param.memstore_version_ - 1, 0);

      param.table_key_ = table_key;
      param.logical_data_version_ = table_key.version_;
      param.schema_version_ = create_partition_param.schema_version_;
      param.create_snapshot_version_ = create_partition_param.frozen_timestamp_;
      param.checksum_method_ = CCM_VALUE_ONLY;
      param.progressive_merge_step_ = 0;
      param.set_is_inited(true);
      create_sstable_param.with_partition_param_ = &param;

      LOG_INFO("start create single sstable",
          K(table_key),
          K(*param.schema_),
          "table_count",
          create_partition_param.schemas_.count());
      if (OB_FAIL(partition_group.create_sstable(create_sstable_param, sstable_handle, in_slog_trans))) {
        STORAGE_LOG(WARN, "failed to create major sstable", K(ret), K(table_key));
      } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
        STORAGE_LOG(WARN, "failed to get sstable", K(ret));
      } else if (OB_FAIL(handle.add_table(sstable_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionService::create_sstables(const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
    ObIPartitionGroup& partition_group, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObCreatePartitionParam create_partition_param;

  if (OB_FAIL(create_partition_param.extract_from(arg))) {
    LOG_WARN("fail to extract from create partition arg", K(ret));
  } else {
#ifdef ERRSIM
    ret = E(EventTable::EN_CREATE_PARTITION_WITH_OLD_MAJOR_TS) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      const_cast<obrpc::ObCreatePartitionArg&>(arg).frozen_timestamp_ = 1;
      LOG_INFO("[ERRSIM] rewrite frozen_timestamp_ to 1", K(arg));
      ret = OB_SUCCESS;
    }
#endif
    LOG_INFO("start to create sstables", K(arg));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_sstables(create_partition_param, in_slog_trans, partition_group, handle))) {
      LOG_WARN("fail to create sstables", K(ret));
    }
  }
  return ret;
}

int ObPartitionService::batch_register_trans_service(const ObIArray<obrpc::ObCreatePartitionArg>& batch_arg)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  for (index = 0; OB_SUCC(ret) && index < batch_arg.count(); ++index) {
    const obrpc::ObCreatePartitionArg& arg = batch_arg.at(index);
    if (OB_FAIL(txs_->add_partition(arg.pg_key_))) {
      STORAGE_LOG(WARN, "fail to add partition group to transaction service", K(arg), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // rollback
    int tmp_ret = OB_SUCCESS;
    const bool graceful = false;
    for (index = index - 2; index >= 0; --index) {
      if (OB_SUCCESS != (tmp_ret = txs_->remove_partition(batch_arg.at(index).pg_key_, graceful))) {
        // should not happen
        STORAGE_LOG(ERROR,
            "rollback partition from transaction service failed",
            K(batch_arg.at(index).partition_key_),
            K(tmp_ret),
            K(graceful));
      }
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_CREATE_PG_AFTER_REGISTER_TRANS_SERVICE) OB_SUCCESS;
  }
#endif
  return ret;
}

// 1. In the standby-pending state, RS checks whether the partition leader is active
//    (indicating that the member_list has been persisted to the majority)
// 2. During recovery, after setting membe_list, check whether the leader active successfully
int ObPartitionService::batch_check_leader_active(
    const obrpc::ObBatchCheckLeaderArg& arg, obrpc::ObBatchCheckRes& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service has not been inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg));
  } else {
    result.reset();
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    ObIPartitionLogService* pls = NULL;
    const ObPkeyArray& pkey_array = arg.pkeys_;
    int tmp_ret = OB_SUCCESS;
    ObRole role = INVALID_ROLE;
    for (int64_t i = 0; i < pkey_array.count() && OB_SUCC(ret); ++i) {
      bool is_leader_active = false;
      const ObPartitionKey pkey = pkey_array.at(i);
      if (OB_SUCCESS != (tmp_ret = get_partition(pkey, guard))) {
        STORAGE_LOG(WARN, "get partition failed, ", K(pkey), K(tmp_ret));
      } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected error, the partition is NULL, ", K(pkey), K(tmp_ret));
      } else if (NULL == (pls = partition->get_log_service())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get_log_service failed", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = pls->get_role(role))) {
        STORAGE_LOG(WARN, "get_role failed", K(tmp_ret));
      } else if (LEADER == role) {
        is_leader_active = true;
      } else {
        is_leader_active = false;
      }
      if (OB_FAIL(result.results_.push_back(is_leader_active))) {
        STORAGE_LOG(WARN, "results push_back failed", K(ret));
      }
    }

    STORAGE_LOG(INFO, "batch_check_leader_active inished", K(ret), K(arg), K(result));
  }
  return ret;
}

int ObPartitionService::batch_get_protection_level(
    const obrpc::ObBatchCheckLeaderArg& arg, obrpc::ObBatchCheckRes& result)
{
  // check whether the protection_level of standby_leader of the standby cluster is maximum protection
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service has not been inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg));
  } else if (arg.index_.switchover_timestamp_ != GCTX.get_cluster_info_version()) {
    // there is a risk of non-atomic,
    // because cluster_info is updated first, and then protection_level is updated
    ret = OB_EAGAIN;
    LOG_WARN("cluster info not update to date, try again",
        K(ret),
        "cluster_info_version",
        GCTX.get_cluster_info_version(),
        K(arg));
  } else {
    result.reset();
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    ObIPartitionLogService* pls = NULL;
    const ObPkeyArray& pkey_array = arg.pkeys_;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < pkey_array.count() && OB_SUCC(ret); ++i) {
      bool is_max_protection_level = false;
      const ObPartitionKey pkey = pkey_array.at(i);
      uint32_t protection_level = MAXIMUM_PERFORMANCE_LEVEL;
      if (OB_SUCCESS != (tmp_ret = get_partition(pkey, guard))) {
        STORAGE_LOG(WARN, "get partition failed", KR(ret), K(pkey), KR(tmp_ret));
      } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected error, the partition is NULL", KR(ret), K(pkey));
      } else if (NULL == (pls = partition->get_log_service())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get_log_service failed", KR(ret), K(pkey));
      } else if (OB_SUCCESS != (tmp_ret = pls->get_standby_leader_protection_level(protection_level))) {
        STORAGE_LOG(WARN, "get_standby_protection_level failed", K(tmp_ret));
      } else {
        is_max_protection_level = (MAXIMUM_PROTECTION_LEVEL == protection_level);
      }
      if (OB_FAIL(ret)) {
        // failed
      } else if (OB_FAIL(result.results_.push_back(is_max_protection_level))) {
        STORAGE_LOG(WARN, "results push_back failed", KR(ret), K(pkey));
      }
    }
    result.index_ = arg.index_;
    STORAGE_LOG(INFO, "batch get protection level", K(ret), K(arg), K(result));
  }
  return ret;
}

// the master cluster handles protection_mode changes
int ObPartitionService::primary_process_protect_mode_switch()
{
  int ret = OB_SUCCESS;
  int64_t fail_count = 0;
  ObIPartitionGroupIterator* iter = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service has not been inited", K(ret));
  } else if (!GCTX.is_primary_cluster()) {
    // not primary cluster, skip
  } else if (NULL == (iter = ObPartitionService::get_instance().alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionLogService* pls = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "scan next partition failed", K(ret));
        }
      } else if (OB_UNLIKELY(NULL == partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition return NULL");
      } else if (NULL == (pls = partition->get_log_service())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get_log_service return NULL");
      } else {
        int tmp_ret = OB_SUCCESS;
        const common::ObPartitionKey pkey = partition->get_partition_key();
        if (ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
          // private table, skip
        } else if (OB_SUCCESS != (tmp_ret = pls->primary_process_protect_mode_switch())) {
          STORAGE_LOG(WARN, "primary_process_protect_mode_switch faield", K(tmp_ret), K(pkey));
        } else {
          // do nothing
        }

        if (OB_SUCCESS != tmp_ret) {
          fail_count++;
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        fail_count++;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  if (OB_SUCC(ret)) {
    if (fail_count > 0) {
      ret = OB_PARTIAL_FAILED;
    }
  }

  STORAGE_LOG(INFO, "primary_process_protect_mode_switch finished", K(ret), K(fail_count));
  return ret;
}

int ObPartitionService::standby_update_replica_protection_level()
{
  int ret = OB_SUCCESS;
  int64_t fail_count = 0;
  ObIPartitionGroupIterator* iter = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service has not been inited", K(ret));
  } else if (!GCTX.is_standby_cluster()) {
    // not standby cluster, skip
  } else if (NULL == (iter = ObPartitionService::get_instance().alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionLogService* pls = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "scan next partition failed", K(ret));
        }
      } else if (OB_UNLIKELY(NULL == partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition return NULL");
      } else if (NULL == (pls = partition->get_log_service())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get_log_service return NULL");
      } else {
        int tmp_ret = OB_SUCCESS;
        const common::ObPartitionKey pkey = partition->get_partition_key();
        if (ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
          // private table, skip
        } else if (OB_SUCCESS != (tmp_ret = pls->standby_update_protection_level())) {
          STORAGE_LOG(WARN, "standby_update_protection_level faield", K(tmp_ret), K(pkey));
        } else {
          // do nothing
        }

        if (OB_SUCCESS != tmp_ret) {
          fail_count++;
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        fail_count++;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  if (OB_SUCC(ret)) {
    if (fail_count > 0) {
      ret = OB_PARTIAL_FAILED;
    }
  }

  STORAGE_LOG(INFO, "standby_update_replica_protection_level finished", K(ret), K(fail_count));
  return ret;
}

int ObPartitionService::batch_register_election_mgr_(const bool is_pg,
    const common::ObIArray<obrpc::ObCreatePartitionArg> &batch_arg, ObIPartitionArrayGuard &partitions,
    ObIArray<ObStorageFileHandle> &files_handle)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_ADD_PARTITION_GROUP);
  ObChangePartitionLogEntry entry;
  ObSavedStorageInfoV2 info;
  clog::ObIPartitionLogService* pls = NULL;
  ObIPartitionGroup* partition = NULL;
  int64_t publish_version = 0;
  int64_t schema_version = 0;
  ObVersion data_version;
  ObMemberList fake_member_list;
  const ObMemberList* member_list_ptr = NULL;

  for (index = 0; OB_SUCC(ret) && index < batch_arg.count(); ++index) {
    const obrpc::ObCreatePartitionArg& arg = batch_arg.at(index);
    pls = NULL;
    partition = NULL;
    publish_version = arg.frozen_timestamp_;
    schema_version = arg.schema_version_;
    data_version.version_ = arg.memstore_version_;
    const bool for_split = arg.split_info_.is_valid();
    member_list_ptr = &arg.member_list_;
    fake_member_list.reset();
    int64_t split_state = static_cast<ObPartitionSplitStateEnum>(FOLLOWER_INIT);
    const ObPartitionKey& pkey = (is_pg ? arg.pg_key_ : arg.partition_key_);
    ObStorageFileHandle file_handle;
    if (for_split && 1 != arg.split_info_.get_spp_array().count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "split_pair count more than 1", K(ret), K(arg));
    } else if (for_split && !is_partition_exist(arg.split_info_.get_spp_array().at(0).get_source_pkey())) {
      ret = OB_PARTITION_NOT_EXIST;
      STORAGE_LOG(WARN, "source partition not exist", K(ret), K(arg.split_info_));
    } else if (OB_FAIL(create_new_partition(pkey, partition))) {
      STORAGE_LOG(WARN, "fail to create partition", K(arg), K(ret));
    } else if (OB_FAIL(partitions.push_back(partition))) {
      cp_fty_->free(partition);
      partition = NULL;
      STORAGE_LOG(WARN, "Fail to push partition to array, ", K(ret));
    } else if (NULL == (pls = partition->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to alloc partition log service");
    } else if (OB_FAIL(files_handle.at(index, file_handle))) {
      LOG_WARN("fail to get file handle", K(ret));
    } else if (OB_ISNULL(file_handle.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("file handle is null", K(ret), K(file_handle));
    } else {
      entry.partition_key_ = arg.partition_key_;
      entry.replica_type_ = arg.replica_type_;
      entry.pg_key_ = arg.pg_key_;
      if (for_split && ObReplicaTypeCheck::is_replica_need_split(arg.replica_type_)) {
        split_state = static_cast<ObPartitionSplitStateEnum>(FOLLOWER_WAIT_SPLIT);
      }
      bool is_physical_restore = false;
      bool is_restore_standby = false;
      if (arg.restore_ >= REPLICA_RESTORE_DATA && arg.restore_ <= REPLICA_RESTORE_LOG) {
        is_physical_restore = true;
        if (OB_FAIL(ObRestoreFakeMemberListHelper::fake_restore_member_list(arg.replica_num_, fake_member_list))) {
          LOG_WARN("failed to fake restore member list", K(ret));
        } else {
          member_list_ptr = &fake_member_list;
        }
      } else if (REPLICA_RESTORE_STANDBY == arg.restore_) {
        is_restore_standby = true;
      } else {
        // do nothing
      }

      bool need_skip_mlist_check = false;
      if (arg.ignore_member_list_) {
        // the member_list check needs to be skipped when the standby cluster is repeatedly creates a tenant
        need_skip_mlist_check = true;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(clog_mgr_->create_partition(pkey,
                     arg.replica_num_,
                     *member_list_ptr,
                     arg.leader_,
                     arg.lease_start_,
                     data_version,
                     arg.replica_type_,
                     arg.replica_property_,
                     arg.last_submit_timestamp_,
                     arg.last_replay_log_id_,
                     arg.restore_,
                     need_skip_mlist_check,
                     pls))) {
        STORAGE_LOG(WARN, "fail to initiate partition log service.", K(arg), K(ret));
      } else if (OB_FAIL(pls->get_saved_base_storage_info(info.get_clog_info()))) {
        STORAGE_LOG(WARN, "fail to get base storage info", K(ret), K(arg));
      } else if (OB_FAIL(SLOGGER.write_log(subcmd,
                     ObStorageLogAttribute(pkey.get_tenant_id(), file_handle.get_storage_file()->get_file_id()),
                     entry))) {
        STORAGE_LOG(WARN, "fail to write add partition log.", K(ret), K(pkey));
      } else {
        ObBaseStorageInfo& clog_info = info.get_clog_info();
        ObDataStorageInfo& data_info = info.get_data_info();
        data_info.set_last_replay_log_id(clog_info.get_last_replay_log_id());
        data_info.set_publish_version(publish_version);
        data_info.set_schema_version(schema_version);
        data_info.set_created_by_new_minor_freeze();

        ObPartitionSplitInfo split_info;
        if (arg.split_info_.is_valid()) {
          if (OB_FAIL(split_info.init(arg.split_info_.get_schema_version(),
                  arg.split_info_.get_spp_array().at(0),
                  ObPartitionSplitInfo::SPLIT_DEST_PARTITION))) {
            LOG_WARN("failed to assign split info", K(ret), K(arg));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (is_pg) {
          ObCreatePGParam pg_param;
          const bool write_pg_slog = true;
          const int64_t data_version = arg.memstore_version_ - 1;

          if (OB_FAIL(get_create_pg_param(arg,
                  info,
                  data_version,
                  write_pg_slog,
                  split_info,
                  split_state,
                  &file_handle,
                  file_mgr_,
                  pg_param))) {
            LOG_WARN("failed to get create pg param", K(ret), K(arg), K(info));
          } else if (OB_FAIL(partition->create_partition_group(pg_param))) {
            STORAGE_LOG(WARN, "generate partition group meta and memstore failed.", K(ret), K(info), K(arg));
          } else if (!(is_physical_restore || is_restore_standby) &&
                     OB_FAIL(partition->create_memtable(write_pg_slog))) {
            STORAGE_LOG(WARN, "create memtable failed.", K(ret), K(info), K(arg));
          } else {
            // do nothing
          }
        } else if (arg.table_schemas_.count() < 1) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid arguments", K(ret), K(arg.table_schemas_.count()));
        } else {
          const bool write_pg_slog = true;
          ObTablesHandle sstables_handle;
          const ObTableSchema& first_table = arg.table_schemas_.at(0);
          const uint64_t data_table_id =
              first_table.is_global_index_table() ? first_table.get_data_table_id() : first_table.get_table_id();
          int64_t data_version = 0;
          bool need_create = false;
          if (OB_FAIL(arg.check_need_create_sstable(need_create))) {
            LOG_WARN("failed to check_need_create_sstable", K(ret), K(arg));
          } else if (need_create) {
            data_version = arg.memstore_version_ - 1;
          }
          if (OB_SUCC(ret)) {
            const bool in_slog_trans = true;
            const uint64_t unused = 0;
            const bool is_replay = false;
            ObCreatePGParam pg_param;
            if (OB_FAIL(get_create_pg_param(arg,
                    info,
                    data_version,
                    write_pg_slog,
                    split_info,
                    split_state,
                    &file_handle,
                    file_mgr_,
                    pg_param))) {
              LOG_WARN("failed to get create pg param", K(ret), K(arg), K(info));
            } else if (OB_FAIL(partition->create_partition_group(pg_param))) {
              STORAGE_LOG(WARN, "generate partition group meta and memstore failed.", K(ret), K(info), K(arg));
            } else if (!(is_physical_restore || is_restore_standby) &&
                       OB_FAIL(partition->create_memtable(in_slog_trans))) {
              STORAGE_LOG(WARN, "fail to create memtable", K(ret));
            } else if (OB_FAIL(create_sstables(arg, in_slog_trans, *partition, sstables_handle))) {
              STORAGE_LOG(WARN, "fail to create sstable", K(ret));
            } else if (OB_FAIL(partition->create_pg_partition(arg.partition_key_,
                           data_info.get_publish_version(),
                           data_table_id,
                           arg,
                           in_slog_trans,
                           is_replay,
                           unused,
                           sstables_handle))) {
              STORAGE_LOG(WARN, "create stand alone partition error", K(ret), K(arg));
            } else {
              // success do nothing
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      files_handle.at(index).reset();
    }
  }  // end for loop
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_CREATE_PG_AFTER_REGISTER_ELECTION_MGR) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObPartitionService::batch_start_partition_election_(
    const common::ObIArray<obrpc::ObCreatePartitionArg> &batch_arg, ObIPartitionArrayGuard &partitions)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService not init", K(ret));
  } else if (batch_arg.count() != partitions.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "counts of batch_arg and partitions not equal",
        K(ret),
        "arg_count",
        batch_arg.count(),
        "partition count",
        partitions.count());
  } else {
    for (index = 0; OB_SUCC(ret) && index < batch_arg.count(); ++index) {
      const obrpc::ObCreatePartitionArg& arg = batch_arg.at(index);
      ObIPartitionGroup* partition = partitions.at(index);
      if (OB_ISNULL(partition)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "partition is NULL", K(ret));
      } else if (arg.ignore_member_list_) {
        // standby replica's member_list not set, no need start election
      } else if (OB_FAIL(partition->get_log_service()->set_election_leader(arg.leader_, arg.lease_start_))) {
        STORAGE_LOG(WARN, "set_election_leader failed", K(ret), "partition_key", partition->get_partition_key());
      }
    }

    if (OB_FAIL(ret)) {
      // do some rollback work
      int tmp_ret = OB_SUCCESS;
      for (index = index - 2; index >= 0; --index) {
        const obrpc::ObCreatePartitionArg& arg = batch_arg.at(index);
        ObIPartitionGroup* partition = partitions.at(index);
        if (NULL != partition && (NULL != partition->get_log_service()) && !arg.ignore_member_list_) {
          if (OB_SUCCESS != (tmp_ret = partition->get_log_service()->set_offline())) {
            STORAGE_LOG(WARN, "offline clog failed", K(ret), "partition_key", partition->get_partition_key(), K(arg));
          }
        }
      }
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_CREATE_PG_AFTER_BATCH_START_PARTITION_ELECTION) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObPartitionService::batch_prepare_splitting(const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg)
{
  int ret = OB_SUCCESS;
  ObPartitionSplitInfo split_info;

  for (int64_t index = 0; OB_SUCC(ret) && index < batch_arg.count(); ++index) {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    const obrpc::ObCreatePartitionArg& arg = batch_arg.at(index);
    const ObSplitPartition& sp = arg.split_info_;
    if (sp.is_valid()) {
      const ObSplitPartitionPair& spp = sp.get_spp_array().at(0);
      const ObPartitionKey& src_pkey = spp.get_source_pkey();
      split_info.reset();
      if (OB_FAIL(get_partition(src_pkey, guard))) {
        STORAGE_LOG(WARN, "get partition failed", K(ret), K(src_pkey));
      } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected error, the partition is NULL", K(ret), K(src_pkey));
      } else if (OB_FAIL(split_info.init(sp.get_schema_version(), spp, ObPartitionSplitInfo::SPLIT_SOURCE_PARTITION))) {
        STORAGE_LOG(WARN, "init split info failed", K(ret), K(arg));
      } else if (OB_FAIL(partition->prepare_splitting(split_info, arg.member_list_, arg.leader_))) {
        STORAGE_LOG(WARN, "prepare splitting failed", K(ret), K(arg));
      } else {
        STORAGE_LOG(INFO, "prepare splitting success", K(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObPartitionService::remove_pg_from_mgr(const ObIPartitionGroup* partition, const bool write_slog)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "partition is NULL", KP(partition));
  } else {
    lib::ObMutexGuard structure_guard(structure_change_mutex_);

    const ObPartitionKey& pkey = partition->get_partition_key();
    ObChangePartitionLogEntry entry;
    entry.partition_key_ = pkey;
    entry.pg_key_ = pkey;
    entry.replica_type_ = partition->get_replica_type();

    if (!is_partition_exist(pkey)) {
      // partition is not exist any more, do nothing
    } else {
      const ObStorageFile* file = static_cast<const ObIPartitionGroup*>(partition)->get_storage_file();
      const ObTenantFileKey file_key(file->get_tenant_id(), file->get_file_id());
      int64_t lsn = 0;
      if (write_slog) {
        if (OB_FAIL(SLOGGER.begin(OB_LOG_CS_DEL_PARTITION))) {
          STORAGE_LOG(WARN, "fail to begin commit log.", K(ret), K(pkey));
        } else {
          if (OB_FAIL(SLOGGER.write_log(ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_REMOVE_PARTITION),
                  ObStorageLogAttribute(pkey.get_tenant_id(), partition->get_storage_file()->get_file_id()),
                  entry))) {
            STORAGE_LOG(WARN, "fail to write remove partition log.", K(ret), K(pkey));
          } else if (OB_FAIL(file_mgr_->write_remove_pg_slog(file_key, pkey))) {
            STORAGE_LOG(WARN, "fail to write remove pg slog", K(ret));
          } else if (OB_FAIL((SLOGGER.commit(lsn)))) {
            STORAGE_LOG(WARN, "fail to commit log.", K(ret), K(lsn));
          }

          if (OB_FAIL(ret)) {
            (void)SLOGGER.abort();
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(file_mgr_->remove_pg(file_key, pkey))) {
          STORAGE_LOG(ERROR, "fail to remove pg from file mgr", K(ret));
          ob_abort();
        } else if (OB_FAIL(inner_del_partition(pkey))) {
          //should not happen
          STORAGE_LOG(ERROR, "Fail to inner del partition, ", K(ret), K(pkey));
          ob_abort();
        }
      }
    }
  }

  return ret;
}

int ObPartitionService::remove_partition(ObIPartitionGroup* partition, const bool write_slog)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "partition is NULL", KP(partition));
  } else {
    const ObPartitionKey pkey = partition->get_partition_key();
    const ObPartitionState state = partition->get_partition_state();
    ObPartitionArray pkeys;

    STORAGE_LOG(INFO, "remove partition", K(pkey));
    if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
      STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), "pg_key", partition->get_partition_key());
    } else if (REMOVE != state && OB_FAIL(partition->stop())) {
      STORAGE_LOG(WARN, "fail to stop partition", K(ret), K(pkey));
    } else if (OB_FAIL(remove_pg_from_mgr(partition, write_slog))) {
      STORAGE_LOG(WARN, "fail to remove pg from mgr", K(ret), K(pkey));
    } else {
      (void)rs_cb_->submit_pt_update_task(pkey);
      (void)rs_cb_->submit_pg_pt_update_task(pkeys);
    }
  }
  return ret;
}

int ObPartitionService::remove_partition(const ObPartitionKey& pkey, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed, ", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the partition is NULL, ", K(pkey), K(ret));
  } else if (OB_FAIL(remove_partition(partition, write_slog))) {
    STORAGE_LOG(WARN, "fail to remove partition", K(ret), K(pkey));
  } else {
    STORAGE_LOG(INFO, "remove partition succ", K(ret), K(pkey));
  }

  return ret;
}

int ObPartitionService::online_partition(const ObPartitionKey& pkey, const int64_t publish_version,
    const int64_t restore_snapshot_version, const uint64_t last_restore_log_id, const int64_t last_restore_log_ts)
{
  int ret = OB_SUCCESS;
  bool txs_add_success = false;
  bool rp_eg_add_success = false;
  STORAGE_LOG(INFO, "online partition", K(pkey));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || publish_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(publish_version), K(ret));
  }

  // NB: BE CAREFUL!!! Think carefully before changing the order of register
  // register to transaction service
  if (OB_SUCC(ret)) {
    if (OB_FAIL(txs_->add_partition(pkey))) {
      STORAGE_LOG(WARN, "fail to add partition to transaction service", K(pkey), K(ret));
    } else {
      txs_add_success = true;
      if (OB_FAIL(txs_->update_publish_version(pkey, publish_version, true))) {
        STORAGE_LOG(WARN, "update publish version failed", K(pkey), K(publish_version));
      } else if (OB_FAIL(txs_->update_restore_replay_info(
                     pkey, restore_snapshot_version, last_restore_log_id, last_restore_log_ts))) {
        STORAGE_LOG(WARN,
            "failed to update_restore_replay_info",
            K(restore_snapshot_version),
            K(last_restore_log_id),
            K(last_restore_log_ts),
            K(ret));
      } else { /*do nothing*/
      }
    }
  }
  // reigster to replay engine, this partition should be replayed
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rp_eg_->add_partition(pkey))) {
      STORAGE_LOG(WARN, "fail to add partition to replay engine", K(ret), K(pkey));
    } else {
      rp_eg_add_success = true;
    }
  }
  /*
  // register to clog and election manager
  if (OB_SUCC(ret)) {
    if (OB_FAIL(election_mgr_->add_partition(pkey))) {
      STORAGE_LOG(WARN, "fail to add partition to election manager", K(pkey), K(ret));
    } else {
      election_add_success = true;
    }
  }
  */
  int err = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = nullptr;
  if (OB_SUCCESS != (err = get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(err));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (!pg->is_valid() && OB_FAIL(pg->set_valid())) {
    STORAGE_LOG(WARN, "partition set valid failed", K(pkey), K(ret));
  } else {
    // set a mark for the partiion, and cancel the mark after clog is tracked within 500ms
    (void)guard.get_partition_group()->set_migrating_flag(true);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg->get_pg_storage().restore_mem_trans_table())) {
        LOG_WARN("failed to restore trans table in memory", K(ret), K(pkey));
      }
    }
  }

  if (OB_FAIL(ret)) {
    rollback_partition_register(pkey, txs_add_success, rp_eg_add_success);
  } else {
    pg->get_pg_storage().online();
  }
  return ret;
}

int ObPartitionService::push_callback_task(const ObCbTask& task)
{
  ObCallbackQueueThread& queue_thread = task.large_cb_ ? large_cb_queue_thread_ : cb_queue_thread_;
  return queue_thread.push(&task);
}

int ObPartitionService::save_base_schema_version(ObIPartitionGroupGuard& guard)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* ptt = guard.get_partition_group();
  int64_t base_schema_version = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(NULL == ptt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (!ptt->is_pg() && extract_pure_id(ptt->get_partition_key().table_id_) < OB_MIN_USER_TABLE_ID) {
    // SKIP IT
  } else if (OB_FAIL(ptt->get_pg_storage().get_based_schema_version(base_schema_version))) {
    STORAGE_LOG(WARN, "fail to get base schema version", K(ret));
  } else if (OB_FAIL(ptt->set_base_schema_version(base_schema_version))) {
    STORAGE_LOG(WARN, "fail to set schema version", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::check_schema_version(const ObIPartitionArrayGuard& pkey_guard_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (pkey_guard_arr.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey_guard_arr.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_guard_arr.count(); ++i) {
      if (OB_UNLIKELY(NULL == const_cast<ObIPartitionArrayGuard&>(pkey_guard_arr).at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition failed", K(ret));
      } else if (OB_FAIL(const_cast<ObIPartitionArrayGuard&>(pkey_guard_arr)
                             .at(i)
                             ->check_schema_version(schema_service_))) {
        STORAGE_LOG(WARN, "fail to check schema version", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

bool ObPartitionService::is_role_change_done(const common::ObPartitionKey& pkey, const ObPartitionState& state) const
{
  bool bret = false;
  int err = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    err = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(err));
  } else if (!pkey.is_valid()) {
    err = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(err));
  } else if (OB_SUCCESS != (err = get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(err));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    err = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(err));
  } else if (state == guard.get_partition_group()->get_partition_state()) {
    bret = true;
  }
  return bret;
}

bool ObPartitionService::is_take_over_done(const common::ObPartitionKey& pkey) const
{
  return is_role_change_done(pkey, L_TAKEOVERED);
}

bool ObPartitionService::is_revoke_done(const common::ObPartitionKey& pkey) const
{
  return is_role_change_done(pkey, F_WORKING);
}

int ObPartitionService::check_tenant_out_of_memstore_limit(const uint64_t tenant_id, bool& is_out_of_mem)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(common::ObTenantManager::get_instance().check_tenant_out_of_memstore_limit(
                 tenant_id, is_out_of_mem))) {
    STORAGE_LOG(WARN, "fail to check tenant out of mem limt", K(tenant_id), K(ret));
  } else {
    // do noting
  }
  return ret;
}

int ObPartitionService::check_query_allowed(const ObPartitionKey& pkey, const transaction::ObTransDesc& trans_desc,
    ObStoreCtxGuard& ctx_guard, ObIPartitionGroupGuard& guard)
{
  int ret = OB_SUCCESS;

  bool is_out_of_mem = false;

  if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(!trans_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(trans_desc), K(ret));
  } else if (OB_FAIL(check_tenant_out_of_memstore_limit(extract_tenant_id(pkey.get_table_id()), is_out_of_mem))) {
    STORAGE_LOG(
        WARN, "fail to check tenant out of mem limit", "tenant_id", extract_tenant_id(pkey.get_table_id()), K(ret));
  } else {
    // skip inner table, one key reason is to let partition merge going
    if (is_out_of_mem && !is_inner_table(pkey.table_id_)) {
      ret = OB_TENANT_OUT_OF_MEM;
      STORAGE_LOG(WARN,
          "this tenant is already out of memstore limit",
          "tenant_id",
          extract_tenant_id(pkey.get_table_id()),
          K(ret));
    } else if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else {
      const ObPGKey& pg_key = guard.get_partition_group()->get_partition_key();
      if (OB_FAIL(get_trans_ctx_for_dml(pg_key, trans_desc, ctx_guard))) {
        STORAGE_LOG(WARN, "fail to get transaction context");
      } else {
        ctx_guard.get_store_ctx().cur_pkey_ = pkey;
      }
    }
  }

  return ret;
}
int ObPartitionService::table_scan(ObVTableScanParam& vparam, common::ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;
  ObTableScanParam& param = static_cast<ObTableScanParam&>(vparam);
  bool is_out_of_mem = false;
  NG_TRACE(storage_table_scan_begin);
  if (OB_UNLIKELY(!is_running_)) {
    ret = OB_ERROR;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret));
  } else if (!vparam.is_valid() || OB_ISNULL(param.trans_desc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(vparam), K(param.trans_desc_), K(ret));
  } else if (param.for_update_ &&
             OB_FAIL(check_tenant_out_of_memstore_limit(extract_tenant_id(param.pkey_.table_id_), is_out_of_mem))) {
    STORAGE_LOG(
        WARN, "fail to check tenant out of mem limit", "tenant_id", extract_tenant_id(param.pkey_.table_id_), K(ret));
  } else {
    // skip inner table, one key reason is to let partition merge going
    if (is_out_of_mem && !is_inner_table(param.pkey_.table_id_)) {
      ret = OB_TENANT_OUT_OF_MEM;
      STORAGE_LOG(WARN,
          "this tenant is already out of memstore limit",
          "tenant_id",
          extract_tenant_id(param.pkey_.table_id_),
          K(ret));
    } else {
      ObIPartitionGroup* partition = NULL;
      ObIPartitionGroupGuard partition_guard;
      if (OB_ISNULL(partition)) {
        ObIPartitionGroupGuard* guard = param.partition_guard_;
        if (NULL == guard) {
          guard = &partition_guard;
        }
        if (OB_FAIL(txs_->get_cached_pg_guard(param.pkey_, guard))) {
          STORAGE_LOG(WARN, "get cached partition failed", K(param), K(ret));
        } else if (OB_ISNULL(partition = guard->get_partition_group())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get partition failed", K(param.pkey_), K(ret));
        } else {
          param.pg_key_ = partition->get_partition_key();
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL == param.table_param_ || OB_INVALID_ID == param.table_param_->get_table_id()) {
          void* buf = NULL;
          ObTableParam* table_param = NULL;
          ObSchemaGetterGuard schema_guard;
          const ObTableSchema* index_schema = NULL;
          const ObTableSchema* table_schema = NULL;
          const uint64_t fetch_tenant_id =
              is_inner_table(param.index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(param.index_id_);
          const bool check_formal = extract_pure_id(param.index_id_) > OB_MAX_CORE_TABLE_ID;
          if (OB_FAIL(schema_service_->get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
            STORAGE_LOG(WARN, "failed to get schema manager", K(ret), K(fetch_tenant_id));
          } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
            STORAGE_LOG(WARN, "Fail to check formal schema, ", K(param.index_id_), K(ret));
          } else if (OB_FAIL(schema_guard.get_table_schema(param.index_id_, index_schema))) {
            STORAGE_LOG(WARN, "Fail to get index schema, ", K(param.index_id_), K(ret));
          } else if (NULL == index_schema) {
            ret = OB_TABLE_NOT_EXIST;
            STORAGE_LOG(WARN, "table not exist", K(param.index_id_), K(ret));
          } else if (param.scan_flag_.is_index_back()) {
            if (OB_FAIL(schema_guard.get_table_schema(param.pkey_.table_id_, table_schema))) {
              STORAGE_LOG(WARN, "Fail to get table schema", K(param.pkey_), K(ret));
            } else if (NULL == table_schema) {
              ret = OB_TABLE_NOT_EXIST;
              STORAGE_LOG(WARN, "table not exist", K(param.pkey_), K(ret));
            }
          } else {
            table_schema = index_schema;
          }
          if (OB_SUCC(ret)) {
            if (NULL == (buf = param.allocator_->alloc(sizeof(ObTableParam)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
            } else {
              table_param = new (buf) ObTableParam(*param.allocator_);
              if (OB_FAIL(table_param->convert(
                      *table_schema, *index_schema, param.column_ids_, param.scan_flag_.is_index_back()))) {
                STORAGE_LOG(WARN, "Fail to convert table param, ", K(ret));
              } else {
                param.table_param_ = table_param;
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition->table_scan(param, result))) {
          STORAGE_LOG(WARN, "Fail to scan table, ", K(ret), K(param));
        } else {
          NG_TRACE(storage_table_scan_end);
        }
      }
    }
  }

  return ret;
}

int ObPartitionService::table_scan(ObVTableScanParam& vparam, common::ObNewIterIterator*& result)
{
  int ret = OB_SUCCESS;
  ObTableScanParam& param = static_cast<ObTableScanParam&>(vparam);
  bool is_out_of_mem = false;
  NG_TRACE(storage_table_scan_begin);
  if (OB_UNLIKELY(!is_running_)) {
    ret = OB_ERROR;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret));
  } else if (!vparam.is_valid() || OB_ISNULL(param.trans_desc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(vparam), K(param.trans_desc_), K(ret));
  } else if (param.for_update_ &&
             OB_FAIL(check_tenant_out_of_memstore_limit(extract_tenant_id(param.pkey_.table_id_), is_out_of_mem))) {
    STORAGE_LOG(
        WARN, "fail to check tenant out of mem limit", "tenant_id", extract_tenant_id(param.pkey_.table_id_), K(ret));
  } else {
    // skip inner table, one key reason is to let partition merge going
    if (is_out_of_mem && !is_inner_table(param.pkey_.table_id_)) {
      ret = OB_TENANT_OUT_OF_MEM;
      STORAGE_LOG(WARN,
          "this tenant is already out of memstore limit",
          "tenant_id",
          extract_tenant_id(param.pkey_.table_id_),
          K(ret));
    } else {
      ObIPartitionGroup* partition = NULL;
      ObIPartitionGroupGuard partition_guard;
      if (OB_ISNULL(partition)) {
        ObIPartitionGroupGuard* guard = param.partition_guard_;
        if (NULL == guard) {
          guard = &partition_guard;
        }
        if (OB_FAIL(txs_->get_cached_pg_guard(param.pkey_, guard))) {
          STORAGE_LOG(WARN, "get partition failed", K(param), K(ret));
        } else if (OB_ISNULL(partition = guard->get_partition_group())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get partition failed", K(param.pkey_), K(ret));
        } else {
          param.pg_key_ = partition->get_partition_key();
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL == param.table_param_ || OB_INVALID_ID == param.table_param_->get_table_id()) {
          void* buf = NULL;
          ObTableParam* table_param = NULL;
          ObSchemaGetterGuard schema_guard;
          const ObTableSchema* index_schema = NULL;
          const ObTableSchema* table_schema = NULL;
          const uint64_t fetch_tenant_id =
              is_inner_table(param.index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(param.index_id_);
          if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
            STORAGE_LOG(WARN, "failed to get schema manager", K(ret), K(fetch_tenant_id));
          } else if (OB_FAIL(schema_guard.get_table_schema(param.index_id_, index_schema))) {
            STORAGE_LOG(WARN, "Fail to get index schema, ", K(param.index_id_), K(ret));
          } else if (NULL == index_schema) {
            ret = OB_TABLE_NOT_EXIST;
            STORAGE_LOG(WARN, "table not exist", K(param.index_id_), K(ret));
          } else if (param.scan_flag_.is_index_back()) {
            if (OB_FAIL(schema_guard.get_table_schema(param.pkey_.table_id_, table_schema))) {
              STORAGE_LOG(WARN, "Fail to get table schema", K(param.pkey_), K(ret));
            } else if (NULL == table_schema) {
              ret = OB_TABLE_NOT_EXIST;
              STORAGE_LOG(WARN, "table not exist", K(param.pkey_), K(ret));
            }
          } else {
            table_schema = index_schema;
          }
          if (OB_SUCC(ret)) {
            if (NULL == (buf = param.allocator_->alloc(sizeof(ObTableParam)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
            } else {
              table_param = new (buf) ObTableParam(*param.allocator_);
              if (OB_FAIL(table_param->convert(
                      *table_schema, *index_schema, param.column_ids_, param.scan_flag_.is_index_back()))) {
                STORAGE_LOG(WARN, "Fail to convert table param, ", K(ret));
              } else {
                param.table_param_ = table_param;
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition->table_scan(param, result))) {
          STORAGE_LOG(WARN, "Fail to scan table, ", K(ret), K(param));
        } else {
          NG_TRACE(storage_table_scan_end);
        }
      }
    }
  }

  return ret;
}

int ObPartitionService::join_mv_scan(
    ObTableScanParam& left_param, ObTableScanParam& right_param, common::ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_running_)) {
    ret = OB_ERROR;
    LOG_WARN("partition service is not running", K(ret));
  } else if (!left_param.is_valid() || !right_param.is_valid() || left_param.scan_flag_.is_index_back() ||
             right_param.scan_flag_.is_index_back()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_param), K(right_param));
  } else {
    ObIPartitionGroup* lp = NULL;
    ObIPartitionGroup* rp = NULL;
    ObIPartitionGroupGuard lguard;
    ObIPartitionGroupGuard rguard;

    if (OB_FAIL(get_partition(left_param.pkey_, lguard))) {
      LOG_WARN("get partition failed", K(ret), K(left_param));
    } else if (OB_ISNULL(lp = lguard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL partition in guard", K(ret), K(left_param));
    } else if (OB_FAIL(get_partition(right_param.pkey_, rguard))) {
      LOG_WARN("get partition failed", K(ret), K(right_param));
    } else if (OB_ISNULL(rp = rguard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL partition in guard", K(ret), K(right_param));
    } else {
      left_param.pg_key_ = lp->get_partition_key();
      right_param.pg_key_ = rp->get_partition_key();
    }

    if (OB_SUCC(ret) && (NULL == left_param.table_param_ || NULL == right_param.table_param_)) {
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema* left_table = NULL;
      const ObTableSchema* right_table = NULL;
      const uint64_t fetch_tenant_id =
          is_inner_table(left_param.index_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(left_param.index_id_);
      if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
        LOG_WARN("get schema manager failed", K(ret), K(fetch_tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(left_param.index_id_, left_table))) {
        LOG_WARN("get table schema failed", K(ret), K(left_param));
      } else if (OB_ISNULL(left_table)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(left_param));
      } else if (OB_FAIL(schema_guard.get_table_schema(right_param.pkey_.get_table_id(), right_table))) {
        LOG_WARN("get table schema failed", K(ret), K(right_param));
      } else if (OB_ISNULL(right_table)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(right_param));
      }

      if (OB_SUCC(ret) && NULL == left_param.table_param_) {
        void* buf = left_param.allocator_->alloc(sizeof(ObTableParam));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else {
          ObTableParam* tp = new (buf) ObTableParam(*left_param.allocator_);
          if (OB_FAIL(tp->convert(
                  *left_table, *left_table, left_param.column_ids_, left_param.scan_flag_.is_index_back()))) {
            LOG_WARN("failed to convert to table param", K(ret));
          } else {
            left_param.table_param_ = tp;
          }
        }
      }

      if (OB_SUCC(ret) && NULL == right_param.table_param_) {
        void* buf = right_param.allocator_->alloc(sizeof(ObTableParam));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else {
          ObTableParam* tp = new (buf) ObTableParam(*right_param.allocator_);
          if (OB_FAIL(tp->convert_join_mv_rparam(*left_table, *right_table, left_param.column_ids_))) {
            LOG_WARN("failed to convert to join mv right table param", K(ret));
          } else {
            right_param.table_param_ = tp;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(lp->join_mv_scan(left_param, right_param, *rp, result))) {
        LOG_WARN("join mv scan failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionService::revert_scan_iter(common::ObNewRowIterator* iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (iter) {
    switch (iter->get_type()) {
      case ObNewRowIterator::ObTableScanIterator:
        rp_free(static_cast<ObTableScanIterator*>(iter), ObTableScanIterator::LABEL);
        break;
      default:
        iter->~ObNewRowIterator();
        break;
    }
    iter = NULL;
  }
  NG_TRACE(revert_scan_iter);
  return ret;
}

int ObPartitionService::revert_scan_iter(common::ObNewIterIterator* iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (iter) {
    switch (iter->get_type()) {
      case ObNewIterIterator::ObTableScanIterIterator:
        rp_free(static_cast<ObTableScanIterIterator*>(iter), ObTableScanIterIterator::LABEL);
        break;
      default:
        iter->~ObNewIterIterator();
        break;
    }
    iter = NULL;
  }
  NG_TRACE(revert_scan_iter);
  return ret;
}

int ObPartitionService::delete_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(column_ids.count() <= 0) || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(trans_desc), K(dml_param), K(pkey), K(column_ids), KP(row_iter), K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    //@NOTICE:(yuchen.wyc) avoid defensive check problem on foreign key self reference
    if (trans_desc.get_cur_stmt_desc().is_delete_stmt()) {
      const_cast<ObDMLBaseParam &>(dml_param).query_flag_.read_latest_ = 0;
    }
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->delete_rows(
        ctx_guard.get_store_ctx(), dml_param, column_ids, row_iter, affected_rows);
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_DELETE_ROW, affected_rows);
  }
  return ret;
}

int ObPartitionService::delete_row(const ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const ObPartitionKey& pkey, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(column_ids.count() <= 0) || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(trans_desc), K(dml_param), K(pkey), K(column_ids), K(row), K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    //@NOTICE:(yuchen.wyc) avoid defensive check problem on foreign key self reference
    if (trans_desc.get_cur_stmt_desc().is_delete_stmt()) {
      const_cast<ObDMLBaseParam &>(dml_param).query_flag_.read_latest_ = 0;
    }
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->delete_row(ctx_guard.get_store_ctx(), dml_param, column_ids, row);
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_DELETE_ROW, 1);
  }
  return ret;
}

int ObPartitionService::put_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(column_ids.count() <= 0) || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(trans_desc), K(dml_param), K(pkey), K(column_ids), KP(row_iter), K(ret));
  } else if (sql::stmt::T_SELECT == trans_desc.get_cur_stmt_desc().stmt_type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "stmt type is different from the actual operation", K(trans_desc));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->put_rows(
        ctx_guard.get_store_ctx(), dml_param, column_ids, row_iter, affected_rows);
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_UPDATE_ROW, affected_rows);
  }
  return ret;
}

int ObPartitionService::insert_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(column_ids.count() <= 0) || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(trans_desc), K(dml_param), K(pkey), K(column_ids), KP(row_iter), K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->insert_rows(
        ctx_guard.get_store_ctx(), dml_param, column_ids, row_iter, affected_rows);
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_INSERT_ROW, affected_rows);
  }
  return ret;
}

int ObPartitionService::insert_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;

  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(column_ids.count() <= 0) || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(trans_desc), K(dml_param), K(pkey), K(column_ids), K(row), K(ret));
  } else if (sql::stmt::T_SELECT == trans_desc.get_cur_stmt_desc().stmt_type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "stmt type is different from the actual operation", K(trans_desc));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->insert_row(ctx_guard.get_store_ctx(), dml_param, column_ids, row);
    int64_t affected_rows = OB_SUCC(ret) ? 1 : 0;
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_INSERT_ROW, affected_rows);
  }
  return ret;
}

int ObPartitionService::insert_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
    int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(column_ids.count() <= 0) || OB_UNLIKELY(duplicated_column_ids.count() <= 0) || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(trans_desc),
        K(dml_param),
        K(pkey),
        K(column_ids),
        K(duplicated_column_ids),
        K(row),
        K(flag),
        K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->insert_row(ctx_guard.get_store_ctx(),
        dml_param,
        column_ids,
        duplicated_column_ids,
        row,
        flag,
        affected_rows,
        duplicated_rows);
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_INSERT_ROW, 1);
  }
  return ret;
}

int ObPartitionService::fetch_conflict_rows(const ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const ObPartitionKey& pkey, const ObIArray<uint64_t>& in_column_ids, const ObIArray<uint64_t>& out_column_ids,
    ObNewRowIterator& check_row_iter, ObIArray<ObNewRowIterator*>& dup_row_iters)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->fetch_conflict_rows(
        ctx_guard.get_store_ctx(), dml_param, in_column_ids, out_column_ids, check_row_iter, dup_row_iters);
  }
  return ret;
}

int ObPartitionService::revert_insert_iter(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), KP(iter), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else {
    ret = guard.get_partition_group()->revert_insert_iter(pkey, iter);
  }
  return ret;
}

int ObPartitionService::update_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(column_ids.count() <= 0) || OB_UNLIKELY(updated_column_ids.count() <= 0) || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(trans_desc),
        K(dml_param),
        K(pkey),
        K(column_ids),
        K(updated_column_ids),
        KP(row_iter),
        K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->update_rows(
        ctx_guard.get_store_ctx(), dml_param, column_ids, updated_column_ids, row_iter, affected_rows);
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_UPDATE_ROW, affected_rows);
  }
  return ret;
}

int ObPartitionService::lock_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const int64_t abs_lock_timeout, const common::ObPartitionKey& pkey, common::ObNewRowIterator* row_iter,
    const ObLockFlag lock_flag, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(trans_desc),
        K(dml_param),
        K(pkey),
        K(abs_lock_timeout),
        KP(row_iter),
        K(lock_flag),
        K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->lock_rows(
        ctx_guard.get_store_ctx(), dml_param, abs_lock_timeout, row_iter, lock_flag, affected_rows);
  }
  return ret;
}

int ObPartitionService::update_row(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    const common::ObIArray<uint64_t>& updated_column_ids, const ObNewRow& old_row, const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(trans_desc), K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret = guard.get_partition_group()->update_row(
        ctx_guard.get_store_ctx(), dml_param, column_ids, updated_column_ids, old_row, new_row);
    AUDIT_PARTITION_V2(ctx_guard.get_store_ctx().mem_ctx_, PART_AUDIT_UPDATE_ROW, 1);
  }
  return ret;
}

int ObPartitionService::lock_rows(const transaction::ObTransDesc& trans_desc, const ObDMLBaseParam& dml_param,
    const int64_t abs_lock_timeout, const common::ObPartitionKey& pkey, const common::ObNewRow& row,
    const ObLockFlag lock_flag)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(!dml_param.is_valid()) || OB_UNLIKELY(!pkey.is_valid()) ||
      OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(trans_desc),
        K(dml_param),
        K(pkey),
        K(abs_lock_timeout),
        K(row),
        K(lock_flag),
        K(ret));
  } else if (OB_FAIL(check_query_allowed(pkey, trans_desc, ctx_guard, guard))) {
    STORAGE_LOG(WARN, "fail to check query allowed", K(ret));
  } else {
    ctx_guard.get_store_ctx().trans_id_ = trans_desc.get_trans_id();
    ret =
        guard.get_partition_group()->lock_rows(ctx_guard.get_store_ctx(), dml_param, abs_lock_timeout, row, lock_flag);
  }
  return ret;
}

int ObPartitionService::get_table_store_cnt(int64_t& table_cnt)
{
  int ret = OB_SUCCESS;
  table_cnt = 0;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(WARN, "partition service not initialized, cannot get partitions.");
    ret = OB_NOT_INIT;
  } else {
    if (NULL == (iter = alloc_pg_iter())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "can not alloc scan iterator", K(ret));
    } else {
      bool table_exist = false;
      uint64_t table_id = OB_INVALID_ID;
      int64_t table_schema_version = OB_INVALID_VERSION;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            STORAGE_LOG(WARN, "get next partition from btree failed.", K(partition), K(ret));
          }
        } else {
          if (FALSE_IT(table_id = partition->get_partition_key().table_id_)) {
          } else if (OB_FAIL(schema_service_->check_table_exist(table_id, table_schema_version, table_exist))) {
            SERVER_LOG(WARN, "Fail to check if table exist, ", K(ret), K(table_id));
          } else if (!table_exist) {
            SERVER_LOG(INFO, "table is droped, no need to statistics", K(ret), K(table_id));
          } else if (OB_FAIL(partition->get_table_store_cnt(table_cnt))) {
            STORAGE_LOG(WARN, "push to partitions failed.", K(partition), K(ret));
          }
        }
      }
    }
  }

  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  return ret;
}

int ObPartitionService::inner_add_partition(
    ObIPartitionGroup& partition, const bool need_check_tenant, const bool is_replay, const bool allow_multi_value)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t new_partition_cnt = partition.get_pg_storage().get_partition_cnt();

  if (OB_FAIL(try_inc_total_partition_cnt(new_partition_cnt, !is_replay))) {
    LOG_WARN("failed to inc total_partition_cnt", K(ret));
  } else if (OB_FAIL(pg_mgr_.add_pg(partition, need_check_tenant, allow_multi_value))) {
    STORAGE_LOG(WARN, "add partition group error", K(ret));
    try_inc_total_partition_cnt(-new_partition_cnt, false /*need check*/);
  } else if (partition.is_pg()) {
    // do nothing
  } else {
    ObTableStat table_stat;
    const ObPartitionKey& pkey = partition.get_partition_key();
    if (OB_SUCCESS != (tmp_ret = partition.get_table_stat(pkey, table_stat))) {
      if (OB_ENTRY_NOT_EXIST != tmp_ret) {
        STORAGE_LOG(WARN, "fail to get table stat", K(tmp_ret), K(pkey));
      }
    } else if (OB_SUCCESS != (tmp_ret = txs_->set_partition_audit_base_row_count(pkey, table_stat.get_row_count()))) {
      STORAGE_LOG(WARN, "fail to set partition audit base row count", K(tmp_ret), K(pkey));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    // for the non-private table replica of the standby cluster newly added to pg_mgr,
    // the protection_level needs to be updated, because observer may miss it when
    // updating the mode to traverse pg_mgr
    ObIPartitionLogService* pls = NULL;
    if (NULL == (pls = partition.get_log_service())) {
      STORAGE_LOG(WARN, "get_log_service return NULL");
    } else {
      int tmp_ret = OB_SUCCESS;
      const common::ObPartitionKey pkey = partition.get_partition_key();
      if (GCTX.is_primary_cluster() || ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
        // primary cluster or private table, skip
      } else if (OB_SUCCESS != (tmp_ret = pls->standby_update_protection_level())) {
        STORAGE_LOG(WARN, "standby_update_protection_level faield", K(tmp_ret), K(pkey));
      } else {
        // do nothing
      }
    }
  }

  const uint64_t tenant_id = partition.get_partition_key().get_tenant_id();
  int64_t tenant_part_cnt = 0;
  int64_t tenant_pg_cnt = 0;
  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    ObTenantStorageInfo* tenant_store_info = MTL_GET(ObTenantStorageInfo*);
    if (NULL != tenant_store_info) {
      tenant_part_cnt = tenant_store_info->part_cnt_;
      tenant_pg_cnt = tenant_store_info->pg_cnt_;
    } else {
      STORAGE_LOG(WARN, "switch tenant context failed", K(ret), K(tenant_id));
    }
  }

  if (OB_TENANT_NOT_IN_SERVER == ret && !need_check_tenant) {
    ret = OB_SUCCESS;
  }

  STORAGE_LOG(INFO,
      "partition service add partition",
      K(ret),
      K(tenant_id),
      K(tenant_part_cnt),
      K(tenant_pg_cnt),
      "pkey",
      partition.get_partition_key());

  return ret;
}

int ObPartitionService::inner_del_partition_impl(const ObPartitionKey& pkey, const int64_t* file_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = nullptr;

  if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "partition group is null, unexpected error", K(pkey));
  } else {
    const int64_t partition_cnt = pg->get_pg_storage().get_partition_cnt();
    ObIPartitionGroupGuard partition_guard;
    if (OB_FAIL(pg_mgr_.get_pg(pkey, file_id, partition_guard))) {
      STORAGE_LOG(WARN, "pg mgr get partition group error", K(ret), K(pkey));
    } else if (OB_FAIL(partition_guard.get_partition_group()->remove_election_from_mgr())) {
      STORAGE_LOG(WARN, "remove election from election mgr error", K(ret), K(pkey));
    } else if (OB_FAIL(pg_mgr_.del_pg(pkey, file_id))) {
      STORAGE_LOG(WARN, "pg mgr remove partition group error", K(ret), K(pkey));
    } else if (OB_FAIL(pg->get_pg_storage().post_del_pg())) {
      STORAGE_LOG(WARN, "failed to call post del_pg", K(ret), K(pkey));
    } else if (OB_FAIL(pg->get_pg_storage().remove_all_pg_index())) {
      STORAGE_LOG(WARN, "failed to remove all pg index", K(ret), K(pkey));
    } else {
      try_inc_total_partition_cnt(-partition_cnt, false /*need check*/);
    }
  }

  const uint64_t tenant_id = pkey.get_tenant_id();
  int64_t tenant_part_cnt = 0;
  int64_t tenant_pg_cnt = 0;
  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    ObTenantStorageInfo* tenant_store_info = MTL_GET(ObTenantStorageInfo*);
    if (NULL != tenant_store_info) {
      tenant_part_cnt = tenant_store_info->part_cnt_;
      tenant_pg_cnt = tenant_store_info->pg_cnt_;
    } else {
      STORAGE_LOG(WARN, "switch tenant context failed", K(ret), K(tenant_id));
    }
  }

  if (OB_TENANT_NOT_IN_SERVER == ret) {
    ret = OB_SUCCESS;
  }

  STORAGE_LOG(INFO, "partition service delete partition", K(tenant_id), K(tenant_part_cnt), K(tenant_pg_cnt), K(pkey));

  return ret;
}

int ObPartitionService::init_partition_group(ObIPartitionGroup& pg, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg.init(pkey, cp_fty_, schema_service_, txs_, rp_eg_, this))) {
    LOG_WARN("fail to create partition", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::post_replay_remove_pg_partition(const ObChangePartitionLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  // remove pg partition from all_tenant_pg_meta_table
  int tmp_ret = OB_SUCCESS;
  const bool need_report_checksum = false;
  if (OB_SUCCESS != (tmp_ret = rs_cb_->submit_pt_update_task(log_entry.partition_key_, need_report_checksum))) {
    STORAGE_LOG(WARN, "notify pg partition table update failed", K(log_entry), K(tmp_ret));
  }
  try_inc_total_partition_cnt(-1, false /*need_check*/);
  return ret;
}

ObPGPartitionIterator* ObPartitionService::alloc_pg_partition_iter()
{
  ObPGPartitionIterator* pg_partition_iter = NULL;
  void* buf = NULL;
  if (NULL == (buf = iter_allocator_.alloc(sizeof(ObPGPartitionIterator)))) {
    STORAGE_LOG(ERROR, "Fail to allocate memory for partition iterator.");
  } else {
    pg_partition_iter = new (buf) ObPGPartitionIterator();
    pg_partition_iter->set_pg_mgr(pg_mgr_);
  }
  return pg_partition_iter;
}

ObSinglePGPartitionIterator* ObPartitionService::alloc_single_pg_partition_iter()
{
  ObSinglePGPartitionIterator* single_pg_partition_iter = NULL;
  void* buf = NULL;
  if (NULL == (buf = iter_allocator_.alloc(sizeof(ObSinglePGPartitionIterator)))) {
    STORAGE_LOG(ERROR, "Fail to allocate memory for partition iterator.");
  } else {
    single_pg_partition_iter = new (buf) ObSinglePGPartitionIterator();
  }
  return single_pg_partition_iter;
}

void ObPartitionService::revert_pg_partition_iter(ObIPGPartitionIterator* iter)
{
  if (NULL != iter) {
    iter->~ObIPGPartitionIterator();
    iter_allocator_.free(iter);
    iter = NULL;
  }
}

void ObPartitionService::revert_replay_status(ObReplayStatus* replay_status) const
{
  if (NULL != replay_status) {
    if (0 == replay_status->dec_ref()) {
      cp_fty_->free(replay_status);
    }
  }
}

int ObPartitionService::replay_redo_log(const common::ObPartitionKey& pkey, const ObStoreCtx& ctx,
    const int64_t log_timestamp, const int64_t log_id, const char* buf, const int64_t size, bool& replayed)
{
  UNUSED(log_id);
  int ret = common::OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (!ctx.is_valid() || NULL == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "wrong param(s)", K(ctx.mem_ctx_), KP(buf), K(size), K(ret));
  } else {
    // used to order trans nodes that have not determined the commit version during replay
    ctx.mem_ctx_->set_redo_log_timestamp(log_timestamp);
    ctx.mem_ctx_->set_redo_log_id(log_id);
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (!guard.get_partition_group()->need_replay_redo()) {
      STORAGE_LOG(INFO, "ignore replay redo log", K(pkey), K(log_timestamp), K(log_id));
    } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay(ctx, buf, size, replayed))) {
      if (OB_TRANS_WAIT_SCHEMA_REFRESH == ret) {
        STORAGE_LOG(WARN, "replay redo log failed", K(ret), K(pkey));
      } else if (OB_ALLOCATE_MEMORY_FAILED != ret || REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        STORAGE_LOG(ERROR, "replay redo log failed", K(ret), K(pkey));
      } else {
        // do nothing
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartitionService::get_role(const common::ObPartitionKey& pkey, common::ObRole& role) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_role(role))) {
    STORAGE_LOG(WARN, "get role failed", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_role_unsafe(const common::ObPartitionKey& pkey, common::ObRole& role) const
{
  class GetRoleFunctor {
  public:
    GetRoleFunctor() : role_(common::ObRole::INVALID_ROLE)
    {}
    int operator()(const common::ObPartitionKey& pkey, const int64_t* file_id, ObIPartitionGroup* partition)
    {
      UNUSED(pkey);
      UNUSED(file_id);
      return partition->get_role_unsafe(role_);
    }
    common::ObRole role_;
  };

  GetRoleFunctor fn;
  int ret = OB_SUCCESS;
  common::ObPGKey pg_key;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_SUCCESS == (ret = const_cast<ObPGMgr&>(pg_mgr_).operate_pg(pkey, nullptr, fn))) {
    // do nothing
  } else if (OB_UNLIKELY(pkey.is_pg() && !pkey.is_trans_table())) {
    STORAGE_LOG(WARN, "get partition group error", K(ret), K(pkey));
  } else if (OB_UNLIKELY(OB_PARTITION_NOT_EXIST != ret)) {
    STORAGE_LOG(WARN, "get partition group error", K(ret), K(pkey));
  } else if (OB_FAIL(ObPartitionMetaRedoModule::get_pg_key(pkey, pg_key))) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "get pg key error", K(ret), K(pkey), K(pg_key));
    }
  } else if (OB_FAIL(const_cast<ObPGMgr&>(pg_mgr_).operate_pg(pg_key, nullptr, fn))) {
    STORAGE_LOG(WARN, "get role failed", K(pg_key), K(ret));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    role = fn.role_;
  }
  return ret;
}

int ObPartitionService::get_dup_replica_type(
    const common::ObPartitionKey& pkey, const common::ObAddr& server, DupReplicaType& dup_replica_type)
{
  return get_dup_replica_type(pkey.get_table_id(), server, dup_replica_type);
}

int ObPartitionService::get_dup_replica_type(
    const uint64_t table_id, const common::ObAddr& server, DupReplicaType& dup_replica_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id) || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(server));
  } else if (OB_FAIL(dup_replica_checker_.get_dup_replica_type(table_id, server, dup_replica_type))) {
    if (OB_TENANT_SCHEMA_NOT_FULL != ret && OB_TABLE_NOT_EXIST != ret) {
      LOG_WARN("fail to get dup replica type", K(ret));
    }
  }
  return ret;
}

int ObPartitionService::get_replica_status(
    const common::ObPartitionKey& pkey, share::ObReplicaStatus& replica_status) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_replica_status(replica_status))) {
    STORAGE_LOG(WARN, "fail to get replica status", K(ret));
  }
  return ret;
}

int ObPartitionService::get_leader(const common::ObPartitionKey& pkey, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObAddr previous_leader;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_leader(leader))) {
    STORAGE_LOG(WARN, "get leader failed", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_leader_curr_member_list(
    const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObAddr previous_leader;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_leader_curr_member_list(member_list))) {
    STORAGE_LOG(WARN, "get leader member list failed", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_curr_member_list(
    const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObAddr previous_leader;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_curr_member_list(member_list))) {
    STORAGE_LOG(WARN, "get member list failed", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_curr_leader_and_memberlist(const common::ObPartitionKey& pkey, common::ObAddr& leader,
    common::ObRole& role, common::ObMemberList& member_list, common::ObChildReplicaList& children_list,
    common::ObReplicaType& replica_type, common::ObReplicaProperty& property) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  int64_t leader_epoch = INT64_MAX;  // UNUSED, just for interface compatible
  ObAddr previous_leader;
  bool is_elected_by_changing_leader = false;
  replica_type = common::REPLICA_TYPE_MAX;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_curr_leader_and_memberlist(
                 leader, role, member_list, children_list))) {
    STORAGE_LOG(WARN, "get leader and member list failed", K(pkey), K(ret));
  } else if (OB_FAIL(election_mgr_->get_leader(
                 pkey, leader, previous_leader, leader_epoch, is_elected_by_changing_leader))) {
    STORAGE_LOG(WARN, "fail to get leader from election", K(ret), K(pkey));
    if (OB_ELECTION_WARN_INVALID_LEADER == ret || OB_ELECTION_WARN_LEADER_LEASE_EXPIRED == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    replica_type = guard.get_partition_group()->get_replica_type();
    property = guard.get_partition_group()->get_replica_property();
  }
  return ret;
}

int ObPartitionService::get_dst_leader_candidate(
    const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_dst_leader_candidate(member_list))) {
    STORAGE_LOG(WARN, "get dst leader candidate failed", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_dst_candidates_array(const common::ObPartitionIArray& pkey_array,
    const common::ObAddrIArray& dst_server_list, common::ObSArray<common::ObAddrSArray>& candidate_list_array,
    common::ObSArray<obrpc::CandidateStatusList>& candidate_status_array) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (pkey_array.count() <= 0 || dst_server_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), "pkey cnt", pkey_array.count(), K(dst_server_list));
  } else if (OB_FAIL(clog_mgr_->get_candidates_array(
                 pkey_array, dst_server_list, candidate_list_array, candidate_status_array))) {
    STORAGE_LOG(WARN, "get_candidates_array failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_partition_count(int64_t& partition_count) const
{
  int ret = OB_SUCCESS;
  partition_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else {
    partition_count = pg_mgr_.get_total_partition_count();
  }
  return ret;
}

int ObPartitionService::change_leader(const common::ObPartitionKey& pkey, const common::ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->change_leader(leader))) {
    STORAGE_LOG(WARN, "partition change_leader failed", K(leader), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

// change leaders of inner tables for sys tenant.
int ObPartitionService::batch_change_rs_leader(const common::ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObRole role = common::INVALID_ROLE;
  ObIPartitionGroupIterator* partition_iter = alloc_pg_iter();
  if (OB_ISNULL(partition_iter)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObIPartitionGroup* partition = NULL;
      if (OB_FAIL(partition_iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "partition_iter get_next failed", KR(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition is null", KP(partition), KR(ret));
      } else {
        const ObPartitionKey& pkey = partition->get_partition_key();
        if (pkey.get_tenant_id() == OB_SYS_TENANT_ID && is_inner_table(pkey.get_table_id())) {
          if (OB_FAIL(partition->get_role(role))) {
            STORAGE_LOG(WARN, "batch change rs leader: fail to get role", KR(ret));
          } else if (is_leader_by_election(role) && OB_FAIL(change_leader(pkey, leader))) {
            STORAGE_LOG(WARN, "batch change rs leader: fail to change leader", KR(ret), K(pkey), K(role));
          } else {
            STORAGE_LOG(INFO, "batch change rs leader: this change leader success", K(pkey), K(role));
          }
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_ITER_END == ret) {
      STORAGE_LOG(INFO, "batch change rs leader success", K(leader));
      ret = OB_SUCCESS;
    }
  }
  if (NULL != partition_iter) {
    revert_pg_iter(partition_iter);
    partition_iter = NULL;
  }
  return ret;
}

// Change leaders of inner tables for sys tenant without new leader.
int ObPartitionService::auto_batch_change_rs_leader()
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  ObIPartitionGroupIterator* partition_iter = alloc_pg_iter();
  if (OB_ISNULL(partition_iter)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObIPartitionGroup* partition = NULL;
      if (OB_FAIL(partition_iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "partition_iter get_next failed", KR(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition is null", KP(partition), KR(ret));
      } else {
        const ObPartitionKey& pkey = partition->get_partition_key();
        if (pkey.get_tenant_id() == OB_SYS_TENANT_ID && is_inner_table(pkey.get_table_id())) {
          member_list.reset();
          if (OB_FAIL(get_leader_curr_member_list(pkey, member_list))) {
            if (ret == OB_STATE_NOT_MATCH) {
              STORAGE_LOG(INFO, "auto batch change rs leader: self is not leader, ignore partition", KR(ret), K(pkey));
            } else {
              STORAGE_LOG(WARN, "auto batch change rs leader: fail to get leader current member list", KR(ret));
            }
          } else {
            for (int i = 0; i < member_list.get_member_number() && OB_SUCC(ret); i++) {
              common::ObAddr leader;
              if (OB_FAIL(member_list.get_server_by_index(i, leader))) {
                STORAGE_LOG(WARN, "auto batch change rs leader: fail to get server by index", KR(ret), K(i));
              } else if (leader == self_addr_) {
                continue;
              } else {
                int tmp_ret = change_leader(pkey, leader);
                if (OB_SUCCESS != tmp_ret) {
                  STORAGE_LOG(WARN,
                      "auto batch change rs leader: fail to change leader",
                      KR(tmp_ret),
                      K(pkey),
                      K(leader),
                      K(i));
                } else {
                  STORAGE_LOG(INFO,
                      "auto batch change rs leader: this partition change leader success",
                      K(pkey),
                      K(leader),
                      K(member_list),
                      K(i));
                  break;
                }
              }
            }
          }
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_ITER_END == ret) {
      STORAGE_LOG(INFO, "auto batch change rs leader success");
      ret = OB_SUCCESS;
    }
  }
  if (NULL != partition_iter) {
    revert_pg_iter(partition_iter);
    partition_iter = NULL;
  }
  return ret;
}

bool ObPartitionService::is_partition_exist(const ObPartitionKey& pkey) const
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (is_inited_) {
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(pkey, guard))) {
      if (OB_PARTITION_NOT_EXIST != ret) {
        STORAGE_LOG(DEBUG, "get_partition failed", K(pkey), K(ret));
      }
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else {
      exist = true;
    }
  }
  return exist;
}

int ObPartitionService::get_table_stat(const ObPartitionKey& pkey, ObTableStat& tstat)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey));
  } else {
    if (is_virtual_table(pkey.get_table_id())) {
      tstat.reset();
    } else if (OB_FAIL(get_partition(pkey, guard))) {
      if (OB_PARTITION_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
      } else {
        tstat.reset();
      }
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->get_table_stat(pkey, tstat))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // do nothing
      } else if (OB_PARTITION_IS_SPLITTING == ret) {
        STORAGE_LOG(WARN, "get table stat. error", K(pkey), K(ret));
      } else {
        STORAGE_LOG(ERROR, "get table stat. error", K(pkey), K(ret));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartitionService::try_remove_from_member_list(ObIPartitionGroup& partition)
{
  int ret = OB_SUCCESS;

  const ObPartitionKey& pkey = partition.get_partition_key();
  ObAddr leader;
  ObMemberList mlist;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init");
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(partition), K(ret));
  } else if (OB_FAIL(location_cache_->get_leader_by_election(pkey, leader, true))) {
    STORAGE_LOG(DEBUG, "get leader address failed", K(pkey), K(ret));
  } else if (OB_FAIL(retry_get_active_member(leader, pkey, mlist))) {
    STORAGE_LOG(DEBUG, "get member list fail", K(leader), K(pkey), K(ret));
  } else if (mlist.contains(self_addr_)) {
    ObMember member;
    ObBaseStorageInfo clog_info;
    ObVersion unused_version;

    if (OB_FAIL(mlist.get_member_by_addr(self_addr_, member))) {
      STORAGE_LOG(WARN, "get member failed", K(mlist), K(ret));
    } else if (OB_FAIL(partition.get_saved_clog_info(clog_info))) {
      STORAGE_LOG(WARN, "get saved clog info failed", K(pkey), K(ret));
    } else {
      share::ObTaskId dummy_id;
      dummy_id.init(self_addr_);
      ObReplicaMember replica_member(member);
      replica_member.set_replica_type(partition.get_replica_type());
      int64_t dummy_orig_quorum = OB_INVALID_COUNT;  // only used in ObPartitionService::batch_remove_replica_mc

      obrpc::ObMemberChangeArg arg = {
          pkey, replica_member, false, clog_info.get_replica_num(), dummy_orig_quorum, WITHOUT_MODIFY_QUORUM, dummy_id};

      if (OB_FAIL(retry_send_remove_replica_mc_msg(leader, arg))) {
        STORAGE_LOG(WARN, "send remove replica fail", K(leader), K(arg), K(ret));
      } else {
        STORAGE_LOG(INFO, "send remove replica mc msg successfully", K(leader), K(arg));
      }
    }
  }
  return ret;
}

int ObPartitionService::try_remove_from_member_list(const obrpc::ObMemberChangeArg& arg)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  common::ObMemberList mlist;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init");
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(location_cache_->get_leader_by_election(arg.key_, leader, true))) {
    STORAGE_LOG(WARN, "get standby leader address failed", K(arg.key_), K(ret));
  } else if (OB_FAIL(retry_get_active_member(leader, arg.key_, mlist))) {
    STORAGE_LOG(WARN, "get member list fail", K(leader), K(arg.key_), K(ret));
  } else if (!mlist.contains(arg.member_.get_server())) {
    STORAGE_LOG(WARN, "member not in the member list", K(leader), K(arg), K(mlist));
  } else if (OB_FAIL(retry_send_remove_replica_mc_msg(leader, arg))) {
    STORAGE_LOG(WARN, "send remove replica fail", K(leader), K(arg), K(ret));
  } else {
    STORAGE_LOG(INFO, "send remove replica mc msg successfully", K(leader), K(arg));
  }
  return ret;
}

int ObPartitionService::try_add_to_member_list(const obrpc::ObMemberChangeArg& arg)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  common::ObMemberList mlist;
  bool is_mc_allowed = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init");
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(location_cache_->get_leader_by_election(arg.key_, leader, true))) {
    STORAGE_LOG(WARN, "get standby leader address failed", K(arg.key_), K(ret));
  } else if (OB_FAIL(retry_get_active_member(leader, arg.key_, mlist))) {
    STORAGE_LOG(WARN, "get member list fail", K(leader), K(arg.key_), K(ret));
  } else if (mlist.contains(arg.member_.get_server())) {
    ret = OB_ENTRY_EXIST;
    STORAGE_LOG(WARN, "member already in the list", K(ret), K(leader), K(arg), K(mlist));
  } else if (OB_FAIL(check_mc_allowed_by_server_lease(is_mc_allowed))) {
    STORAGE_LOG(WARN, "fail to check mc condition by server lease", K(ret));
  } else if (is_mc_allowed) {
    if (OB_FAIL(retry_send_add_replica_mc_msg(leader, arg))) {
      STORAGE_LOG(WARN, "send add replica fail", K(leader), K(arg), K(ret));
    } else {
      STORAGE_LOG(INFO, "send add replica mc msg successfully", K(leader), K(arg));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    STORAGE_LOG(WARN,
        "member change is not allowed "
        "since server lease is not enough",
        K(ret));
  }
  return ret;
}

int ObPartitionService::add_replica(const obrpc::ObAddReplicaArg& rpc_arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;

  ObReplicaOpArg arg;
  arg.key_ = rpc_arg.key_;
  arg.dst_ = rpc_arg.dst_;
  arg.src_ = rpc_arg.src_;
  arg.data_src_ = rpc_arg.src_;
  arg.type_ = ADD_REPLICA_OP;
  arg.quorum_ = rpc_arg.quorum_;
  arg.priority_ = rpc_arg.priority_;
  arg.cluster_id_ = rpc_arg.cluster_id_;
  arg.change_member_option_ = rpc_arg.skip_change_member_list_ ? SKIP_CHANGE_MEMBER_LIST : NORMAL_CHANGE_MEMBER_LIST;
  arg.switch_epoch_ = rpc_arg.switch_epoch_;
  STORAGE_LOG(INFO, "begin add_replica", K(arg), K(rpc_arg), K(task_id));
  SERVER_EVENT_ADD("storage", "add_replica begin", "partition", arg.key_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init.", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(arg.key_), K(ret));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rpc_arg), K(ret));
  } else if (OB_FAIL(check_add_or_migrate_replica_arg(arg.key_, arg.dst_, arg.data_src_, arg.data_src_, arg.quorum_))) {
    STORAGE_LOG(WARN, "failed to check_add_or_migrate_replica_arg", K(ret), K(arg));
  } else if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
    STORAGE_LOG(WARN, "fail to schedule migrate task", K(ret), K(task_id), K(arg));
  } else {
    STORAGE_LOG(INFO, "succeed to schedule add replica task", K(arg), K(task_id));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rs_cb_->submit_pt_update_task(arg.key_))) {
      STORAGE_LOG(WARN, "notify partition table udpate failed", K(arg.key_), K(tmp_ret));
    }
    SERVER_EVENT_ADD("storage", "add_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::batch_add_replica(const obrpc::ObAddReplicaBatchArg& rpc_arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObReplicaOpArg> task_list;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(ret));
  } else if (!rpc_arg.is_valid() || rpc_arg.arg_array_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret), K(rpc_arg));
  } else if (OB_FAIL(task_list.reserve(rpc_arg.arg_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve task list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rpc_arg.arg_array_.count(); ++i) {
      const obrpc::ObAddReplicaArg& single_arg = rpc_arg.arg_array_.at(i);
      ObReplicaOpArg tmp_arg;
      tmp_arg.key_ = single_arg.key_;
      tmp_arg.dst_ = single_arg.dst_;
      tmp_arg.src_ = single_arg.src_;
      tmp_arg.data_src_ = single_arg.src_;
      tmp_arg.quorum_ = single_arg.quorum_;
      tmp_arg.priority_ = single_arg.priority_;
      tmp_arg.cluster_id_ = single_arg.cluster_id_;
      tmp_arg.change_member_option_ =
          single_arg.skip_change_member_list_ ? SKIP_CHANGE_MEMBER_LIST : NORMAL_CHANGE_MEMBER_LIST;
      tmp_arg.switch_epoch_ = single_arg.switch_epoch_;
      tmp_arg.type_ = ADD_REPLICA_OP;

      if (OB_FAIL(check_add_or_migrate_replica_arg(
              tmp_arg.key_, tmp_arg.dst_, tmp_arg.data_src_, tmp_arg.data_src_, tmp_arg.quorum_))) {
        STORAGE_LOG(WARN, "failed to check_add_or_migrate_replica_arg", K(ret), K(tmp_arg));
      } else if (OB_FAIL(task_list.push_back(tmp_arg))) {
        STORAGE_LOG(WARN, "failed to add replica task list", K(ret), K(tmp_arg));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(task_list, task_id))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task.", K(rpc_arg), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < rpc_arg.arg_array_.count(); ++i) {
      const common::ObPartitionKey& pkey = rpc_arg.arg_array_.at(i).key_;
      if (OB_SUCCESS != (tmp_ret = rs_cb_->submit_pt_update_task(pkey))) {
        STORAGE_LOG(WARN, "notify partition table udpate failed", K(pkey), K(ret));
      }
      SERVER_EVENT_ADD("storage", "add_replica failed", "partition", pkey);
    }
  }
  return ret;
}

int ObPartitionService::rebuild_replica(const obrpc::ObRebuildReplicaArg& rpc_arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg arg;
  arg.key_ = rpc_arg.key_;
  arg.dst_ = rpc_arg.dst_;
  arg.src_ = rpc_arg.src_;
  arg.data_src_ = rpc_arg.src_;
  arg.type_ = REBUILD_REPLICA_OP;
  arg.priority_ = rpc_arg.priority_;
  arg.change_member_option_ = SKIP_CHANGE_MEMBER_LIST;
  arg.switch_epoch_ = rpc_arg.switch_epoch_;
  // does not need to check quorum

  STORAGE_LOG(INFO, "begin rebuild_replica", K(arg));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init.", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(arg.key_), K(ret));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rpc_arg), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObIPartitionGroupGuard guard;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS == (tmp_ret = get_partition(arg.key_, guard))) {
      STORAGE_LOG(INFO, "local partition exists", K(arg.key_));
      if (OB_ISNULL(guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
      } else if (guard.get_partition_group()->get_replica_type() != arg.dst_.get_replica_type()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN,
            "replica type is not allowed to change while rebuild",
            K(arg.key_),
            "old_replica_type",
            guard.get_partition_group()->get_replica_type(),
            "new_replica_type",
            arg.dst_.get_replica_type(),
            K(arg.dst_),
            K(ret));
      }
    } else {
      STORAGE_LOG(INFO, "local partition does not exist", K(arg.key_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task", K(arg), K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed to schedule rebuild replica task", K(arg));
      SERVER_EVENT_ADD("storage", "rebuild_replica begin", "partition", arg.key_);
    }
  }

  if (OB_FAIL(ret) && OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT != ret) {
    SERVER_EVENT_ADD("storage", "rebuild_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::schedule_standby_restore_task(
    const obrpc::ObRebuildReplicaArg& rpc_arg, const int64_t cluster_id, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg arg;
  arg.key_ = rpc_arg.key_;
  arg.dst_ = rpc_arg.dst_;
  arg.src_ = rpc_arg.src_;
  arg.cluster_id_ = cluster_id;
  arg.data_src_ = rpc_arg.src_;
  arg.type_ = RESTORE_STANDBY_OP;
  arg.priority_ = rpc_arg.priority_;
  arg.change_member_option_ = SKIP_CHANGE_MEMBER_LIST;
  arg.switch_epoch_ = rpc_arg.switch_epoch_;

  STORAGE_LOG(INFO, "begin schedule_standby_restore_task", K(arg));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init.", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(arg.key_), K(ret));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rpc_arg), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task", K(arg), K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed to schedule standby restore task", K(arg));
      SERVER_EVENT_ADD("storage", "restore_standby_replica begin", "partition", arg.key_);
    }
  }

  if (OB_FAIL(ret) && OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT != ret) {
    SERVER_EVENT_ADD("storage", "restore_standby_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::push_into_migrate_retry_queue(const common::ObPartitionKey& pkey, const int32_t task_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else {
    ObMigrateRetryTask task;
    task.pkey_ = pkey;
    task.task_type_ = task_type;
    if (OB_FAIL(migrate_retry_queue_thread_.push(&task))) {
      STORAGE_LOG(WARN, "migrate_retry_queue_thread push task failed", K(task), K(ret));
    }
  }
  return ret;
}

int ObPartitionService::handle_rebuild_result_(
    const common::ObPartitionKey pkey, const common::ObReplicaType replica_type, const int ret_val)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionLogService* pls = NULL;
  bool is_clog_offline = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(replica_type));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (pls = partition->get_log_service()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition_log_service failed", K(pkey), K(ret));
  } else if (OB_FAIL(pls->is_offline(is_clog_offline))) {
    STORAGE_LOG(WARN, "pls->is_offline failed", K(ret), K(pkey), K(ret_val));
  } else {
    if (OB_SUCCESS != ret_val) {
      if (is_clog_offline) {
        // rebuild failed && clog offline, retry rebuild
        (void)partition->set_need_rebuild();
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = push_into_migrate_retry_queue(pkey, RETRY_REBUILD))) {
          STORAGE_LOG(WARN, "push_into_migrate_retry_queue failed", K(pkey), K(tmp_ret));
        }
        STORAGE_LOG(
            WARN, "rebuild failed and clog is offline, set need_rebuild flag", K(pkey), K(replica_type), K(ret_val));
      } else {
        // rebuild failed && clog online, clog will retry rebuild, clean flag
        (void)partition->reset_migrate_retry_flag();
        STORAGE_LOG(INFO, "clog is online, reset need_rebuild flag", K(pkey), K(replica_type));
        // clean mark in meta table
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = rs_cb_->report_rebuild_replica(pkey, self_addr_, OB_REBUILD_OFF))) {
          STORAGE_LOG(WARN, "report finish rebuild replica failed", K(tmp_ret), K(pkey));
        }
      }
    } else {
      (void)partition->reset_migrate_retry_flag();
      STORAGE_LOG(INFO, "reset need_rebuild flag", K(pkey), K(replica_type), K(ret_val), K(is_clog_offline));
      // clean mark in meta table after rebuild
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = rs_cb_->report_rebuild_replica(pkey, self_addr_, OB_REBUILD_OFF))) {
        STORAGE_LOG(WARN, "report finish rebuild replica failed", K(tmp_ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionService::copy_global_index(const obrpc::ObCopySSTableArg& rpc_arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg arg;
  arg.key_ = rpc_arg.key_;
  arg.dst_ = rpc_arg.dst_;
  arg.src_ = rpc_arg.src_;
  arg.data_src_ = rpc_arg.src_;
  arg.type_ = COPY_GLOBAL_INDEX_OP;
  arg.priority_ = rpc_arg.priority_;
  arg.cluster_id_ = rpc_arg.cluster_id_;
  arg.switch_epoch_ = rpc_arg.switch_epoch_;
  // does not need to check quorum

  STORAGE_LOG(INFO, "begin copy global index", K(arg));
  SERVER_EVENT_ADD("storage", "copy global index begin", "partition", arg.key_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init.", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(arg), K(ret));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rpc_arg), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(arg.key_, guard))) {
      STORAGE_LOG(WARN, "failed to get local partition", K(ret), K(arg.key_));
    } else {
      STORAGE_LOG(INFO, "local partition exists", K(arg.key_));
      if (OB_ISNULL(guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
      } else if (guard.get_partition_group()->get_replica_type() != arg.dst_.get_replica_type()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN,
            "replica type is not allowed to change while copy global index",
            K(arg.key_),
            "old_replica_type",
            guard.get_partition_group()->get_replica_type(),
            "new_replica_type",
            arg.dst_.get_replica_type(),
            K(arg.dst_),
            K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task", K(arg), K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed to schedule rebuild replica task", K(arg));
    }
  }

  if (OB_FAIL(ret)) {
    submit_pt_update_task_(arg.key_);
    SERVER_EVENT_ADD("storage", "copy global index failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::copy_local_index(const obrpc::ObCopySSTableArg& rpc_arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg arg;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  arg.key_ = rpc_arg.key_;
  arg.index_id_ = rpc_arg.index_table_id_;
  arg.dst_ = rpc_arg.dst_;
  arg.src_ = rpc_arg.src_;
  arg.data_src_ = rpc_arg.src_;
  arg.type_ = COPY_LOCAL_INDEX_OP;
  arg.priority_ = rpc_arg.priority_;
  arg.cluster_id_ = rpc_arg.cluster_id_;
  arg.switch_epoch_ = rpc_arg.switch_epoch_;

  STORAGE_LOG(INFO, "begin copy local index", K(arg));
  SERVER_EVENT_ADD("storage", "copy local index begin", "partition", arg.key_, "index_id", arg.index_id_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "it is rebooting now, can not do replica operation", K(ret), K(arg));
  } else if (OB_FAIL(get_partition(arg.key_, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(arg.key_));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret), K(arg.key_));
  } else if (OB_UNLIKELY(partition->get_replica_type() != arg.dst_.get_replica_type())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "replica type is not allowed to change",
        K(ret),
        K(arg.key_),
        "old replica type",
        partition->get_replica_type(),
        "new replica type",
        arg.dst_.get_replica_type(),
        K(arg.dst_));
  } else if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
    STORAGE_LOG(WARN, "fail to schedule partition migrate task", K(ret), K(arg));
  } else {
    STORAGE_LOG(INFO, "succeed to schedule copy local index task", K(arg));
  }
  return ret;
}

int ObPartitionService::change_replica(const obrpc::ObChangeReplicaArg& rpc_arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg arg;
  arg.key_ = rpc_arg.key_;
  arg.dst_ = rpc_arg.dst_;
  arg.src_ = rpc_arg.src_;
  arg.data_src_ = rpc_arg.src_;
  arg.type_ = CHANGE_REPLICA_OP;
  arg.quorum_ = rpc_arg.quorum_;
  arg.priority_ = rpc_arg.priority_;
  arg.switch_epoch_ = rpc_arg.switch_epoch_;

  STORAGE_LOG(INFO, "begin change_replica", K(arg));
  SERVER_EVENT_ADD("storage", "change_replica begin", "partition", arg.key_);

  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init.", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(arg.key_), K(ret));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rpc_arg), K(ret));
  } else if (!is_working_partition(arg.key_)) {
    ret = OB_WORKING_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "can not change replica type, it is not normal replica", K(arg.key_), K(ret));
  } else if (OB_FAIL(get_partition(arg.key_, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
  } else if (ObReplicaTypeCheck::is_paxos_replica(guard.get_partition_group()->get_replica_type())) {
    common::ObAddr leader;
    common::ObMemberList mlist;

    if (OB_FAIL(location_cache_->get_leader_by_election(arg.key_, leader, true))) {
      STORAGE_LOG(WARN, "get leader address failed", K(arg.key_), K(ret));
    } else if (OB_FAIL(retry_get_active_member(leader, arg.key_, mlist))) {
      STORAGE_LOG(WARN, "get member list failed", K(leader), K(arg.key_), K(ret));
    } else if (OB_FAIL(mlist.get_member_by_addr(self_addr_, arg.src_))) {
      STORAGE_LOG(WARN, "get memberself failed", K(arg.key_), K(self_addr_), K(mlist), K(ret));
    } else if (OB_FAIL(arg.src_.set_replica_type(guard.get_partition_group()->get_replica_type()))) {
      STORAGE_LOG(WARN,
          "set replica type to ObMember failed",
          K(arg.key_),
          K(guard.get_partition_group()->get_replica_type()),
          K(ret));
    }
  } else {  // is no paxos replica
    arg.src_ = ObReplicaMember(self_addr_,
        ObTimeUtility::current_time(),  // of no use
        guard.get_partition_group()->get_replica_type());
  }

  // remove from member list if need
  if (OB_FAIL(ret)) {
  } else if (arg.dst_.get_replica_type() == guard.get_partition_group()->get_replica_type()) {
    ret = OB_ALREADY_DONE;
    STORAGE_LOG(WARN,
        "no change of replica type",
        "dst replica type",
        arg.dst_.get_replica_type(),
        "local replica type",
        guard.get_partition_group()->get_replica_type(),
        K(ret));
    // } else if (OB_FAIL(check_replica_type_change_allowed(
    //         arg.src_.get_replica_type(),
    //         dst.get_replica_type()))) {
    //   STORAGE_LOG(WARN, "can not change to dst replica type from current replica type",
    //               K(arg.src_.get_replica_type()), K(dst), K(ret));
  } else if (!ObReplicaTypeCheck::change_replica_op_allow(arg.src_.get_replica_type(), arg.dst_.get_replica_type())) {
    ret = OB_OP_NOT_ALLOW;
    STORAGE_LOG(WARN,
        "change replica op not allow",
        "source type",
        arg.src_.get_replica_type(),
        "target type",
        arg.dst_.get_replica_type(),
        K(arg.key_),
        K(ret));
  }

  // add change replica task
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task", K(arg), K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed to schedule change replica task", K(arg));
    }
  }

  if (OB_FAIL(ret)) {
    submit_pt_update_task_(arg.key_);
    SERVER_EVENT_ADD("storage", "change_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::batch_change_replica(const obrpc::ObChangeReplicaBatchArg& arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObReplicaOpArg> arg_list;
  ObReplicaOpArg op_arg;
  ObIPartitionGroupGuard guard;
  common::ObMemberList member_list;

  STORAGE_LOG(INFO, "begin change_replica", K(arg));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init.", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(ret));
  } else if (arg.arg_array_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch change replica get invlaid argument", K(ret), K(arg));
  } else {
    const bool skip_change_member_list = arg.arg_array_.at(0).skip_change_member_list_;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
      const ObChangeReplicaArg& rpc_arg = arg.arg_array_.at(i);
      op_arg.key_ = rpc_arg.key_;
      op_arg.dst_ = rpc_arg.dst_;
      op_arg.data_src_ = rpc_arg.src_;
      op_arg.type_ = CHANGE_REPLICA_OP;
      op_arg.quorum_ = rpc_arg.quorum_;
      op_arg.priority_ = rpc_arg.priority_;
      op_arg.change_member_option_ =
          rpc_arg.skip_change_member_list_ ? SKIP_CHANGE_MEMBER_LIST : NORMAL_CHANGE_MEMBER_LIST;
      op_arg.switch_epoch_ = rpc_arg.switch_epoch_;
      SERVER_EVENT_ADD("storage", "batch change_replica begin", "partition", rpc_arg.key_);
      if (OB_FAIL(get_partition(rpc_arg.key_, guard))) {
        STORAGE_LOG(WARN, "get partition failed", K(ret), "pkey", rpc_arg.key_);
      } else if (OB_ISNULL(guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition failed", K(ret), "pkey", rpc_arg.key_);
      } else {
        ObReplicaType src_type = guard.get_partition_group()->get_replica_type();
        if (ObReplicaTypeCheck::is_paxos_replica(src_type) && !skip_change_member_list) {
          common::ObMember member;
          if (OB_FAIL(guard.get_partition_group()->get_curr_member_list(member_list))) {
            LOG_WARN("failed to get curr member list", K(ret));
          } else if (OB_FAIL(member_list.get_member_by_addr(self_addr_, member))) {
            LOG_WARN("failed to get self member", K(ret));
          } else if (OB_FAIL(op_arg.src_.set_member(member))) {
            LOG_WARN("failed to set member", K(ret), K(member));
          } else if (OB_FAIL(op_arg.src_.set_replica_type(src_type))) {
            LOG_WARN("failed to set replica type", K(ret));
          }
        } else {
          // not paxos replica
          op_arg.src_ = ObReplicaMember(self_addr_,
              ObTimeUtility::current_time(),  // of no use
              src_type);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(arg_list.push_back(op_arg))) {
          LOG_WARN("failed to add op_arg", K(ret), "pkey", rpc_arg.key_);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg_list, arg.task_id_))) {
      LOG_WARN("failed to schedule batch change replica", K(ret));
    } else {
      LOG_INFO("succeed to schedule change replica task", K(arg));
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < arg.arg_array_.count(); ++i) {
      SERVER_EVENT_ADD("storage", "batch change_replica failed", "partition", arg.arg_array_.at(i).key_);
    }
  }
  return ret;
}

int ObPartitionService::migrate_replica_batch(const obrpc::ObMigrateReplicaBatchArg& arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObReplicaOpArg> task_list;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret), K(arg));
  } else if (OB_FAIL(task_list.reserve(arg.arg_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve task list", K(ret));
  } else {
    ObReplicaOpType type = MIGRATE_REPLICA_OP;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
      const obrpc::ObMigrateReplicaArg& single_arg = arg.arg_array_.at(i);
      if (OB_FAIL(push_replica_task(type, single_arg, task_list))) {
        STORAGE_LOG(WARN, "failed to add replica task list", K(ret), K(single_arg));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(task_list, arg.task_id_))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < arg.arg_array_.count(); ++i) {
      const common::ObPartitionKey& pkey = arg.arg_array_.at(i).key_;
      submit_pt_update_task_(pkey);
      SERVER_EVENT_ADD("storage", "migrate_replica failed", "partition", pkey);
    }
  }
  return ret;
}

int ObPartitionService::restore_replica(const obrpc::ObRestoreReplicaArg& arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObReplicaOpArg> task_list;
  const ObReplicaOpType type = RESTORE_REPLICA_OP;
  ObReplicaOpArg tmp_arg;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret), K(arg));
  } else {
    tmp_arg.key_ = arg.key_;
    tmp_arg.dst_ = arg.dst_;
    tmp_arg.type_ = type;
    tmp_arg.restore_arg_ = arg.src_;
    tmp_arg.priority_ = arg.priority_;
    tmp_arg.switch_epoch_ = arg.switch_epoch_;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(tmp_arg, task_id))) {
        STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      submit_pt_update_task_(arg.key_);
      SERVER_EVENT_ADD("storage", "restore_replica failed", "partition", arg.key_);
    }
  }
  return ret;
}

int ObPartitionService::physical_restore_replica(
    const obrpc::ObPhyRestoreReplicaArg& arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  const ObReplicaOpType type = RESTORE_REPLICA_OP;
  ObReplicaOpArg tmp_arg;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The ObPartitionService is not running, ", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "physical restore arg is invalid", K(ret), K(arg));
  } else {
    tmp_arg.key_ = arg.key_;
    tmp_arg.dst_ = arg.dst_;
    tmp_arg.type_ = type;
    tmp_arg.restore_version_ = ObReplicaOpArg::RESTORE_VERSION_1;
    tmp_arg.priority_ = arg.priority_;
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* pg = NULL;
    int16_t restore_flag;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_partition(arg.key_, guard))) {
        STORAGE_LOG(WARN, "failed to get local pg partition", K(ret), K(arg.key_));
      } else if (OB_ISNULL(pg = guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
      } else if (FALSE_IT(restore_flag = pg->get_pg_storage().get_restore_state())) {
      } else if (ObReplicaRestoreStatus::REPLICA_RESTORE_DATA != restore_flag &&
                 ObReplicaRestoreStatus::REPLICA_RESTORE_ARCHIVE_DATA != restore_flag &&
                 ObReplicaRestoreStatus::REPLICA_RESTORE_CUT_DATA != restore_flag) {
        ret = OB_RESTORE_PARTITION_IS_COMPELETE;
        STORAGE_LOG(INFO, "physical restore replica task is already finished", K(ret), K(arg));
      } else if (OB_FAIL(tmp_arg.phy_restore_arg_.assign(arg.src_))) {
        STORAGE_LOG(WARN, "fail to assign restore arg", K(ret), "arg", arg.src_);
      } else if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(tmp_arg, task_id))) {
        STORAGE_LOG(WARN, "fail to schedule restore task.", K(arg), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      submit_pt_update_task_(arg.key_);
      SERVER_EVENT_ADD("storage", "restore_replica failed", "partition", arg.key_);
    }
  }
  return ret;
}

int ObPartitionService::get_tenant_log_archive_status_v2(
    const ObGetTenantLogArchiveStatusArg& arg, ObServerTenantLogArchiveStatusWrapper& result)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* partition_iter = NULL;
  ObIPartitionGroup* partition = NULL;
  hash::ObHashMap<uint64_t, ObServerTenantLogArchiveStatus> status_map;
  bool is_bad_backup_dest = false;
  result.result_code_ = OB_SUCCESS;
  int64_t rs_piece_id = arg.backup_piece_id_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(arg), K(ret));
  } else if (OB_FAIL(ObBackupDestDetector::get_instance().get_is_backup_dest_bad(arg.round_, is_bad_backup_dest))) {
    LOG_WARN("failed to get_is_backup_dest_bad", K(ret), K(arg));
  } else if (is_bad_backup_dest) {
    result.result_code_ = OB_LOG_ARCHIVE_INTERRUPTED;
    LOG_WARN("backup dest is bad, cannot report log archive status", K(ret), K(result));
    if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {  // per 600s
      SERVER_EVENT_ADD("archive_log", "backup dest is bad", "round", arg.round_);
    }
  } else if (OB_FAIL(status_map.create(OB_MAX_SERVER_TENANT_CNT, ObModIds::OB_LOG_ARCHIVE_SCHEDULER))) {
    LOG_WARN("failed to create status map", K(ret));
  } else if (OB_UNLIKELY(NULL == (partition_iter = alloc_pg_iter()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to alloc partition iter", K(arg), K(ret));
  } else {
    const bool is_backup_stop = clog_mgr_->is_server_archive_stop(arg.incarnation_, arg.round_);
    ObPGLogArchiveStatus pg_status;
    ObServerTenantLogArchiveStatus tenant_status;
    while (OB_SUCC(ret)) {
      pg_status.reset();
      tenant_status.reset();
      bool need_update = false;
      bool is_in_member_list = false;
      if (OB_FAIL(partition_iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to get next partition", K(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The partition is NULL", K(ret));
      } else if (OB_FAIL(partition->check_is_in_member_list(is_in_member_list))) {
        STORAGE_LOG(WARN, "failed to check_is_in_member_list", K(partition), K(ret));
      } else if (OB_SYS_TENANT_ID == (partition->get_partition_key().get_tenant_id())) {
        // skip sys tenant
      } else if (partition->is_removed() || !is_in_member_list) {
        // skip removed partitions
      } else if (OB_FAIL(partition->get_log_archive_status(pg_status))) {
        STORAGE_LOG(WARN, "failed to get_log_archive_status", KR(ret), "pkey", partition->get_partition_key());
      } else if (pg_status.log_archive_round_ > arg.round_ || pg_status.archive_incarnation_ > arg.incarnation_ ||
                 ((pg_status.log_archive_round_ == arg.round_) &&
                     (pg_status.archive_incarnation_ == arg.incarnation_) &&
                     ((pg_status.cur_piece_id_ >= 0 && pg_status.cur_piece_id_ < rs_piece_id - 1) ||
                         (pg_status.cur_piece_id_ > rs_piece_id + 1)))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
            "invalid archive_round, incarnation or piece",
            "pkey",
            partition->get_partition_key(),
            K(pg_status),
            K(arg),
            K(rs_piece_id),
            KR(ret));
      } else if (OB_FAIL(status_map.get_refactored(partition->get_partition_key().get_tenant_id(), tenant_status))) {
        if (OB_HASH_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "failed to get_refactored of hashmap", "pkey", partition->get_partition_key(), K(ret));
        } else {
          ret = OB_SUCCESS;
          need_update = true;
          // set tenant_status
          tenant_status.tenant_id_ = partition->get_partition_key().get_tenant_id();
          tenant_status.round_ = arg.round_;
          tenant_status.incarnation_ = arg.incarnation_;
          if (is_backup_stop && share::ObLogArchiveStatus::INTERRUPTED != pg_status.status_) {
            tenant_status.status_ = share::ObLogArchiveStatus::STOP;
          } else if (pg_status.log_archive_round_ < arg.round_ || pg_status.archive_incarnation_ < arg.incarnation_ ||
                     share::ObLogArchiveStatus::INVALID == pg_status.status_) {
            if (share::ObLogArchiveStatus::MIXED != tenant_status.status_) {
              tenant_status.status_ = share::ObLogArchiveStatus::MIXED;
              LOG_INFO("set tenant satus mix", K(ret), K(arg), K(pg_status));
            }
          } else {
            tenant_status.start_ts_ = pg_status.round_start_ts_;
            tenant_status.checkpoint_ts_ = pg_status.last_archived_checkpoint_ts_;
            tenant_status.status_ = pg_status.status_;
            tenant_status.min_backup_piece_id_ = pg_status.cur_piece_id_;
            tenant_status.max_log_ts_ = pg_status.last_archived_log_submit_ts_;
          }
        }
      } else if (share::ObLogArchiveStatus::INTERRUPTED == tenant_status.status_) {
        // just skip
        need_update = false;
      } else if (share::ObLogArchiveStatus::INTERRUPTED == pg_status.status_) {
        need_update = true;
        tenant_status.status_ = pg_status.status_;
      } else if (share::ObLogArchiveStatus::STOP == tenant_status.status_) {
        need_update = false;
      } else if (pg_status.log_archive_round_ < arg.round_ || pg_status.archive_incarnation_ < arg.incarnation_ ||
                 tenant_status.status_ != pg_status.status_ ||
                 share::ObLogArchiveStatus::INVALID == pg_status.status_) {
        need_update = true;
        if (share::ObLogArchiveStatus::MIXED != tenant_status.status_) {
          tenant_status.status_ = share::ObLogArchiveStatus::MIXED;
          LOG_INFO("set tenant satus mix", K(ret), K(arg), K(pg_status));
        }
      } else {
        need_update = true;
        if (tenant_status.start_ts_ < pg_status.round_start_ts_) {
          tenant_status.start_ts_ = pg_status.round_start_ts_;
        }

        if (tenant_status.checkpoint_ts_ > pg_status.last_archived_checkpoint_ts_) {
          tenant_status.checkpoint_ts_ = pg_status.last_archived_checkpoint_ts_;
        }

        if (tenant_status.min_backup_piece_id_ > pg_status.cur_piece_id_) {
          tenant_status.min_backup_piece_id_ = pg_status.cur_piece_id_;
        }

        if (tenant_status.max_log_ts_ < pg_status.last_archived_log_submit_ts_) {
          tenant_status.max_log_ts_ = pg_status.last_archived_log_submit_ts_;
        }
      }

      if (OB_SUCC(ret) && need_update) {
        int flag = 1;  // overwrite
        if (OB_FAIL(status_map.set_refactored(tenant_status.tenant_id_, tenant_status, flag))) {
          STORAGE_LOG(WARN, "failed to set_refactored of hashmap", K(arg), K(tenant_status), KR(ret));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      ObHashMap<uint64_t, ObServerTenantLogArchiveStatus>::const_iterator iter = status_map.begin();
      for (; OB_SUCC(ret) && iter != status_map.end(); ++iter) {
        if (OB_FAIL(result.status_array_.push_back(iter->second))) {
          STORAGE_LOG(WARN, "failed push back tenant_status into result", K(arg), KR(ret));
        } else {
          STORAGE_LOG(INFO, "success to push back tenant_status into result", "tenant_status", iter->second, KR(ret));
        }
      }
    }
  }

  if (NULL != partition_iter) {
    revert_pg_iter(partition_iter);
  }
  return ret;
}

int ObPartitionService::get_tenant_log_archive_status(
    const share::ObGetTenantLogArchiveStatusArg& arg, share::ObTenantLogArchiveStatusWrapper& result)
{
  int ret = OB_SUCCESS;
  share::ObServerTenantLogArchiveStatusWrapper tmp_result;
  share::ObTenantLogArchiveStatus compat_status;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(get_tenant_log_archive_status_v2(arg, tmp_result))) {
    LOG_WARN("failed to get_tenant_log_archive_status_v2", K(ret));
  } else {
    result.result_code_ = tmp_result.result_code_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_result.status_array_.count(); ++i) {
      const share::ObServerTenantLogArchiveStatus& status = tmp_result.status_array_.at(i);
      compat_status.reset();
      if (OB_FAIL(status.get_compat_status(compat_status))) {
        LOG_WARN("Failed to fill status", K(ret), K(status));
      } else if (OB_FAIL(result.status_array_.push_back(compat_status))) {
        LOG_WARN("failed to add compat status", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionService::get_archive_pg_map(archive::PGArchiveMap*& map)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(clog_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "clog_mgr_ is NULL", K(ret));
  } else if (OB_FAIL(clog_mgr_->get_archive_pg_map(map))) {
    STORAGE_LOG(WARN, "get_archive_pg_map fail", K(ret));
  }
  return ret;
}

int ObPartitionService::restore_follower_replica(const obrpc::ObCopySSTableBatchArg& rpc_arg)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg arg;
  share::ObTaskId task_id;
  // does not need to check quorum

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init.", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(arg), K(ret));
  } else if (rpc_arg.arg_array_.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "restore follower replica do not support batch now", K(ret), K(rpc_arg.arg_array_.count()));
  } else if (!rpc_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rpc_arg), K(ret));
  }
  if (OB_SUCC(ret)) {
    const obrpc::ObCopySSTableArg& restore_arg = rpc_arg.arg_array_.at(0);
    STORAGE_LOG(INFO, "begin restore follower replica", K(restore_arg));
    SERVER_EVENT_ADD("storage", "restore follower replica begin", "partition", restore_arg.key_);

    arg.key_ = restore_arg.key_;
    arg.dst_ = restore_arg.dst_;
    arg.src_ = restore_arg.src_;
    arg.data_src_ = restore_arg.src_;
    arg.priority_ = restore_arg.priority_;
    arg.type_ = RESTORE_FOLLOWER_REPLICA_OP;
    arg.switch_epoch_ = restore_arg.switch_epoch_;
    task_id = rpc_arg.task_id_;
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* pg = NULL;
    int16_t restore_flag;
    if (OB_FAIL(get_partition(arg.key_, guard))) {
      STORAGE_LOG(WARN, "failed to get local partition", K(ret), K(arg.key_));
    } else if (OB_ISNULL(pg = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
    } else if (pg->get_replica_type() != arg.dst_.get_replica_type()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN,
          "replica type is not allowed to change while restore follower replica",
          K(arg.key_),
          "old_replica_type",
          pg->get_replica_type(),
          "new_replica_type",
          arg.dst_.get_replica_type(),
          K(arg.dst_),
          K(ret));
    } else if (FALSE_IT(restore_flag = pg->get_pg_storage().get_restore_state())) {
    } else if (ObReplicaRestoreStatus::REPLICA_LOGICAL_RESTORE_DATA != restore_flag) {
      arg.restore_version_ = ObReplicaOpArg::RESTORE_VERSION_1;
      if (REPLICA_RESTORE_DATA != restore_flag && REPLICA_RESTORE_ARCHIVE_DATA != restore_flag) {
        ret = OB_RESTORE_PARTITION_IS_COMPELETE;
        STORAGE_LOG(INFO, "restore follower replica task is already finished", K(ret), K(arg));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task", K(arg), K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed to schedule restore follower replica task", K(arg));
    }
  }

  if (OB_FAIL(ret)) {
    submit_pt_update_task_(arg.key_);
    SERVER_EVENT_ADD("storage", "restore follower replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::backup_replica_batch(const obrpc::ObBackupBatchArg& arg)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "backup replica batch", K(arg));

  ObArray<ObReplicaOpArg> task_list;
  const ObReplicaOpType type = BACKUP_REPLICA_OP;
  int64_t backup_schema_version = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(ret));
  } else if (!arg.is_valid() || arg.arg_array_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret), K(arg));
  } else if (OB_FAIL(task_list.reserve(arg.arg_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve task list", K(ret));
  } else if (FALSE_IT(backup_schema_version = arg.arg_array_.at(0).physical_backup_arg_.backup_schema_version_)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
      const obrpc::ObBackupArg& single_arg = arg.arg_array_.at(i);
      ObReplicaOpArg replica_op_arg;
      replica_op_arg.backup_arg_ = single_arg.physical_backup_arg_;
      replica_op_arg.data_src_ = single_arg.src_;
      replica_op_arg.dst_ = single_arg.dst_;
      replica_op_arg.key_ = single_arg.key_;
      replica_op_arg.priority_ = single_arg.priority_;
      replica_op_arg.cluster_id_ = GCONF.cluster_id;
      replica_op_arg.type_ = type;
      replica_op_arg.switch_epoch_ = single_arg.switch_epoch_;
      if (OB_FAIL(task_list.push_back(replica_op_arg))) {
        LOG_WARN("failed to push replica op arg into array", K(ret), K(replica_op_arg));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(task_list, arg.task_id_))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
    }
  }
  return ret;
}

int ObPartitionService::validate_backup_batch(const obrpc::ObValidateBatchArg& arg)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "validate backup batch", " batch count:", arg.arg_array_.count(), K(arg));

  ObArray<ObReplicaOpArg> task_list;
  const ObReplicaOpType type = VALIDATE_BACKUP_OP;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been init, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The Service is not running, ", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret));
  } else if (OB_FAIL(task_list.reserve(arg.arg_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve task list", K(ret));
    ;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
      const obrpc::ObValidateArg& single_arg = arg.arg_array_.at(i);
      ObReplicaOpArg replica_op_arg;
      // construct argument here
      replica_op_arg.key_ = single_arg.physical_validate_arg_.pg_key_;
      replica_op_arg.dst_ = single_arg.dst_;
      replica_op_arg.type_ = type;
      replica_op_arg.validate_arg_ = single_arg.physical_validate_arg_;
      replica_op_arg.priority_ = ObReplicaOpPriority::PRIO_LOW;
      replica_op_arg.cluster_id_ = single_arg.physical_validate_arg_.cluster_id_;
      if (OB_FAIL(task_list.push_back(replica_op_arg))) {
        LOG_WARN("failed to push replica op arg into array", K(ret), K(replica_op_arg));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(task_list, arg.task_id_))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
    }
  }
  return ret;
}

int ObPartitionService::backup_backupset_batch(const obrpc::ObBackupBackupsetBatchArg& arg)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "backup backupset batch", K(arg));

  ObArray<ObReplicaOpArg> task_list;
  const ObReplicaOpType type = BACKUP_BACKUPSET_OP;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret), K(arg));
  } else if (OB_FAIL(task_list.reserve(arg.arg_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve task list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
      const obrpc::ObMigrateBackupsetArg& single_arg = arg.arg_array_.at(i);
      SMART_VAR(ObReplicaOpArg, replica_op_arg)
      {
        replica_op_arg.key_ = single_arg.backup_backupset_arg_.pg_key_;
        replica_op_arg.type_ = type;
        replica_op_arg.priority_ = ObReplicaOpPriority::PRIO_LOW;
        replica_op_arg.backup_backupset_arg_ = single_arg.backup_backupset_arg_;
        replica_op_arg.backup_backupset_arg_.tenant_dropped_ = arg.tenant_dropped_;
        if (OB_FAIL(task_list.push_back(replica_op_arg))) {
          LOG_WARN("failed to push replica op arg into array", K(ret), K(replica_op_arg));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(task_list, arg.task_id_))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
    }
  }

  return ret;
}

int ObPartitionService::backup_archive_log(const obrpc::ObBackupArchiveLogBatchArg& arg)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "backup archive log batch", K(arg));

  ObArray<ObReplicaOpArg> task_list;
  const ObReplicaOpType type = BACKUP_ARCHIVELOG_OP;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret), K(arg));
  } else if (OB_FAIL(task_list.reserve(arg.arg_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve task list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
      const obrpc::ObPGBackupArchiveLogArg& single_arg = arg.arg_array_.at(i);
      SMART_VAR(ObReplicaOpArg, replica_op_arg)
      {
        replica_op_arg.key_ = single_arg.pg_key_;
        replica_op_arg.type_ = type;
        replica_op_arg.priority_ = ObReplicaOpPriority::PRIO_LOW;
        share::ObBackupArchiveLogArg& share_arg = replica_op_arg.backup_archive_log_arg_;
        share_arg.pg_key_ = single_arg.pg_key_;
        share_arg.log_archive_round_ = arg.archive_round_;
        share_arg.piece_id_ = arg.piece_id_;
        share_arg.create_date_ = arg.create_date_;
        share_arg.job_id_ = arg.job_id_;
        share_arg.rs_checkpoint_ts_ = arg.checkpoint_ts_;
        if (OB_FAIL(databuff_printf(share_arg.src_backup_dest_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.src_root_path_))) {
          LOG_WARN("failed to databuff printf", KR(ret), K(arg));
        } else if (OB_FAIL(databuff_printf(
                       share_arg.src_storage_info_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.src_storage_info_))) {
          LOG_WARN("failed to databuff printf", KR(ret), K(arg));
        } else if (OB_FAIL(databuff_printf(
                       share_arg.dst_backup_dest_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.dst_root_path_))) {
          LOG_WARN("failed to databuff printf", KR(ret), K(arg));
        } else if (OB_FAIL(databuff_printf(
                       share_arg.dst_storage_info_, OB_MAX_BACKUP_PATH_LENGTH, "%s", arg.dst_storage_info_))) {
          LOG_WARN("failed to databuff printf", KR(ret), K(arg));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(task_list.push_back(replica_op_arg))) {
            LOG_WARN("failed to push replica op arg into array", K(ret), K(replica_op_arg));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(task_list, arg.task_id_))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
    }
  }

  return ret;
}

int ObPartitionService::push_replica_task(
    const ObReplicaOpType& type, const obrpc::ObMigrateReplicaArg& migrate_arg, ObIArray<ObReplicaOpArg>& task_list)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg tmp_arg;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (type != MIGRATE_REPLICA_OP && type != FAST_MIGRATE_REPLICA_OP) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "only batch migrate replica task is supported now", K(ret), K(type));
  } else if (migrate_arg.src_.get_replica_type() != migrate_arg.dst_.get_replica_type()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "replica type not match", K(ret), K(migrate_arg));
  } else if (OB_FAIL(check_add_or_migrate_replica_arg(migrate_arg.key_,
                 migrate_arg.dst_,
                 migrate_arg.src_,
                 migrate_arg.data_source_,
                 migrate_arg.quorum_))) {
    STORAGE_LOG(WARN, "failed to check_add_or_migrate_replica_arg", K(ret), K(migrate_arg));
  } else {
    tmp_arg.type_ = type;
    tmp_arg.key_ = migrate_arg.key_;
    tmp_arg.dst_ = migrate_arg.dst_;
    tmp_arg.src_ = migrate_arg.src_;
    tmp_arg.data_src_ = migrate_arg.data_source_;
    tmp_arg.quorum_ = migrate_arg.quorum_;
    tmp_arg.priority_ = migrate_arg.priority_;
    tmp_arg.change_member_option_ =
        migrate_arg.skip_change_member_list_ ? SKIP_CHANGE_MEMBER_LIST : NORMAL_CHANGE_MEMBER_LIST;
    tmp_arg.switch_epoch_ = migrate_arg.switch_epoch_;
    if (OB_FAIL(task_list.push_back(tmp_arg))) {
      STORAGE_LOG(WARN, "failed to add task_list", K(ret), K(tmp_arg));
    } else {
      STORAGE_LOG(INFO, "succeed to add migrate task", K(tmp_arg));
    }
  }
  return ret;
}

int ObPartitionService::check_add_or_migrate_replica_arg(const common::ObPartitionKey& pkey,
    const common::ObReplicaMember& dst, /* new replica */
    const common::ObReplicaMember& src,
    const common::ObReplicaMember& data_src, /* data source, use leader instead when invalid */
    const int64_t quorum)
{
  int ret = OB_SUCCESS;

  int64_t tenant_id = extract_tenant_id(pkey.get_table_id());
  int64_t table_id = pkey.get_table_id();
  bool has_partition = true;
  bool is_standby_cluster = GCTX.is_standby_cluster();
  bool need_check_quorum =
      ObMultiClusterUtil::need_create_partition(tenant_id, table_id, has_partition, is_standby_cluster);
  if (need_check_quorum && 0 == quorum) {  // maybe restore
    int tmp_ret = OB_SUCCESS;
    ObPhysicalRestoreInfo info;
    if (OB_SUCCESS == (tmp_ret = ObBackupInfoMgr::get_instance().get_restore_info(tenant_id, info))) {
      need_check_quorum = false;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (!pkey.is_valid() || !dst.is_valid() || !src.is_valid() || !data_src.is_valid() ||
             (need_check_quorum && quorum <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(dst), K(data_src), K(quorum));
  } else if (dst.get_server() == src.get_server() || dst.get_server() == data_src.get_server()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(dst), K(src), K(data_src), K(quorum));
  }
  return ret;
}

int ObPartitionService::is_log_sync(const common::ObPartitionKey& key, bool& is_sync, uint64_t& max_confirmed_log_id)
{
  int ret = OB_SUCCESS;
  is_sync = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else {
    ObIPartitionGroupGuard guard;
    clog::ObIPartitionLogService* pls = NULL;
    if (OB_FAIL(get_partition(key, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(key), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(key), K(ret));
    } else if (OB_ISNULL(pls = guard.get_partition_group()->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "partition log service is NULL", K(key), K(ret));
    } else if (OB_FAIL(pls->is_log_sync_with_leader(is_sync))) {
      STORAGE_LOG(WARN, "wait fetch log failed.", K(key), K(ret));
    } else {
      max_confirmed_log_id = pls->get_max_confirmed_log_id();
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_IS_LOG_SYNC) OB_SUCCESS;

    if (OB_TIMEOUT == ret) {
      ret = OB_SUCCESS;
      is_sync = false;
    }
  }
#endif
  return ret;
}

int ObPartitionService::remove_replica(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObReplicaOpArg op_arg;
  ObPartGroupTask* group_task = NULL;
  ObCurTraceId::TraceId task_id;

  op_arg.key_ = pkey;
  op_arg.dst_ = dst;
  op_arg.type_ = REMOVE_REPLICA_OP;
  op_arg.priority_ = ObReplicaOpPriority::PRIO_HIGH;
  task_id.init(self_addr_);
  bool in_member_list = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(pkey), K(ret));
  } else if (!pkey.is_valid() || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(dst), K(ret));
  } else if (OB_FAIL(ObPartGroupMigrator::get_instance().mark(op_arg, task_id, group_task))) {
    STORAGE_LOG(WARN, "fail to schedule remove replica task.", K(ret), K(op_arg));
  } else {
    if (OB_FAIL(check_self_in_member_list(pkey, in_member_list))) {
      STORAGE_LOG(WARN, "check whether myself is in member list failed", K(pkey), K(dst), K(ret));
    } else if (in_member_list) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "can not remove replica which is in member list", K(pkey), K(dst), K(ret));
    } else if (OB_FAIL(do_remove_replica(pkey, dst))) {
      STORAGE_LOG(WARN, "failed to do remove replica", K(ret), K(pkey), K(dst));
    }

    if (NULL != group_task) {
      if (OB_SUCCESS != (tmp_ret = ObPartGroupMigrator::get_instance().remove_finish_task(group_task))) {
        STORAGE_LOG(ERROR, "failed to remove_finish_task", K(tmp_ret), K(pkey), KP(group_task));
      }
      group_task = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "remove_replica successfully", K(pkey), K(dst));
    SERVER_EVENT_ADD("storage", "remove_replica success", "partition", pkey);
  } else {
    STORAGE_LOG(INFO, "remove_replica failed", K(pkey), K(dst), K(ret));
    SERVER_EVENT_ADD("storage", "remove_replica failed", "partition", pkey);
  }
  return ret;
}

int ObPartitionService::do_remove_replica(const ObPartitionKey& pkey, const ObReplicaMember& dst)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  STORAGE_LOG(INFO, "begin remove_replica", K(pkey), K(dst));
  SERVER_EVENT_ADD("storage", "remove_replica begin", "partition", pkey);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(pkey), K(ret));
  } else if (!pkey.is_valid() || !dst.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(dst), K(ret));
  } else if (!clog_mgr_->is_scan_finished()) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "can not remove replica while scan disk not finished", K(pkey), K(dst), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed, ", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the partition is NULL, ", K(pkey), K(ret));
  } else if (partition->is_pg() && OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), "pg_key", partition->get_partition_key());
  } else if (OB_FAIL(remove_partition(pkey))) {
    STORAGE_LOG(WARN, "remove partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(rs_cb_->pt_sync_update(pkey))) {
    STORAGE_LOG(WARN, "sync update partition table", K(ret), K(pkey));
  }

  if (OB_FAIL(ret)) {
    rs_cb_->submit_pt_update_task(pkey);
    rs_cb_->submit_pg_pt_update_task(pkeys);
  }

  return ret;
}

int ObPartitionService::batch_remove_replica(const obrpc::ObRemoveReplicaArgs& args)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartGroupTask* group_task = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "remove replica args is invalid", K(ret), K(args));
  } else {
    for (int64_t i = 0; i < args.arg_array_.count(); ++i) {
      const obrpc::ObRemoveReplicaArg& arg = args.arg_array_.at(i);
      ObReplicaOpArg op_arg;
      ObCurTraceId::TraceId task_id;
      op_arg.key_ = arg.pkey_;
      op_arg.dst_ = arg.replica_member_;
      op_arg.type_ = REMOVE_REPLICA_OP;
      op_arg.priority_ = ObReplicaOpPriority::PRIO_HIGH;
      task_id.init(self_addr_);
      ObIPartitionGroup* partition = NULL;
      ObIPartitionGroupGuard guard;
      bool need_add_server_event = true;
      bool is_success = false;
      if (!arg.is_valid()) {
        tmp_ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "remove replica arg is invalid", K(tmp_ret), K(arg.pkey_), K(arg.replica_member_));
      } else if (OB_SUCCESS != (tmp_ret = get_partition(arg.pkey_, guard))) {
        STORAGE_LOG(WARN, "get partiton failed", K(tmp_ret), K(arg.pkey_));
      } else if (OB_ISNULL(partition = guard.get_partition_group())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition should not be NULL", K(tmp_ret));
      } else if (partition->get_replica_type() != arg.replica_member_.get_replica_type()) {
        tmp_ret = OB_STATE_NOT_MATCH;
        STORAGE_LOG(WARN,
            "replica partition state do not match",
            K(tmp_ret),
            K(partition->get_replica_type()),
            K(arg.replica_member_.get_replica_type()));
      } else if (OB_SUCCESS != (tmp_ret = ObPartGroupMigrator::get_instance().mark(op_arg, task_id, group_task))) {
        STORAGE_LOG(
            WARN, "fail to schedule remove replica task.", K(tmp_ret), "pkey", arg.pkey_, "op_arg.type", op_arg.type_);
      } else {
        if (REPLICA_TYPE_READONLY != arg.replica_member_.get_replica_type()) {
          need_add_server_event = false;
          STORAGE_LOG(INFO, "replica remove relay on gc", K(arg.pkey_), K(arg.replica_member_));
        }
        if (OB_SUCCESS != (tmp_ret = do_remove_replica(arg.pkey_, arg.replica_member_))) {
          STORAGE_LOG(WARN, "failed to remove readonly_replica", K(tmp_ret), K(arg.pkey_), K(arg.replica_member_));
        } else {
          is_success = true;
        }

        if (NULL != group_task) {
          if (OB_SUCCESS != (tmp_ret = ObPartGroupMigrator::get_instance().remove_finish_task(group_task))) {
            STORAGE_LOG(ERROR, "failed to remove_finish_task", K(tmp_ret), K(arg.pkey_), KP(group_task));
          }
          group_task = NULL;
        }
      }

      if (need_add_server_event) {
        if (is_success) {
          STORAGE_LOG(INFO, "remove_replica successfully", K(arg.pkey_), K(arg.replica_member_));
          SERVER_EVENT_ADD("storage", "remove_replica success", "partition", arg.pkey_);
        } else {
          STORAGE_LOG(INFO, "remove_replica failed", K(arg.pkey_), K(arg.replica_member_));
          SERVER_EVENT_ADD("storage", "remove_replica failed", "partition", arg.pkey_);
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::batch_remove_non_paxos_replica(
    const obrpc::ObRemoveNonPaxosReplicaBatchArg& args, obrpc::ObRemoveNonPaxosReplicaBatchResult& results)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "remove replica args is invalid", K(ret), K(args));
  } else {
    results.return_array_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < args.arg_array_.count(); i++) {
      if (OB_FAIL(results.return_array_.push_back(OB_ERROR))) {
        STORAGE_LOG(WARN, "fail to push_back ret", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < args.arg_array_.count(); ++i) {
      const obrpc::ObRemoveNonPaxosReplicaArg& arg = args.arg_array_.at(i);
      int tmp_ret = OB_SUCCESS;
      ObReplicaOpArg op_arg;
      op_arg.key_ = arg.key_;
      op_arg.dst_ = arg.dst_;
      op_arg.type_ = REMOVE_REPLICA_OP;
      op_arg.priority_ = ObReplicaOpPriority::PRIO_HIGH;
      op_arg.switch_epoch_ = arg.switch_epoch_;
      ObIPartitionGroup* partition = NULL;
      ObIPartitionGroupGuard guard;
      if (!arg.is_valid()) {
        tmp_ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "remove replica arg is invalid", K(tmp_ret), K(arg.key_), K(arg.dst_));
      } else if (OB_SUCCESS != (tmp_ret = get_partition(arg.key_, guard))) {
        STORAGE_LOG(WARN, "get partiton failed", K(tmp_ret), K(arg.key_));
      } else if (OB_ISNULL(partition = guard.get_partition_group())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition should not be NULL", K(tmp_ret));
      } else if (partition->get_replica_type() != arg.dst_.get_replica_type()) {
        tmp_ret = OB_STATE_NOT_MATCH;
        STORAGE_LOG(WARN,
            "replica partition state do not match",
            K(tmp_ret),
            K(partition->get_replica_type()),
            K(arg.dst_.get_replica_type()));
        //      } else if (REPLICA_TYPE_READONLY != arg.dst_.get_replica_type()) {
        //        tmp_ret = OB_INVALID_ARGUMENT;
        //        STORAGE_LOG(INFO, "replica type is invalid", K(tmp_ret), K(arg.key_), K(arg.dst_));
      } else if (OB_SUCCESS != (tmp_ret = do_remove_replica(arg.key_, arg.dst_))) {
        STORAGE_LOG(WARN, "failed to remove readonly_replica", K(tmp_ret), K(arg.key_), K(arg.dst_));
      }

      if (i >= results.return_array_.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "results count not match", K(ret), K(i), "results_count", results.return_array_.count());
      } else {
        results.return_array_.at(i) = tmp_ret;
      }

      STORAGE_LOG(INFO, "remove_non_paxos_replica", K(tmp_ret), K(arg.key_), K(arg.dst_));
      SERVER_EVENT_ADD("storage", "remove_non_paxos_replica", "partition", arg.key_, "ret", tmp_ret);
    }
  }
  return ret;
}

int ObPartitionService::remove_replica_mc(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& rpc_mc_log_info)
{
  int ret = OB_SUCCESS;
  obrpc::ObMCLogInfo mc_log_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(ret));
  } else {
    rpc_mc_log_info.reset();
    rpc_mc_log_info.key_ = arg.key_;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(arg.key_, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->get_log_service()->remove_member(
                   arg.member_, arg.quorum_, mc_log_info))) {
      STORAGE_LOG(WARN, "fail to remove dst", K(arg), K(ret));
    } else {
      rpc_mc_log_info.log_id_ = mc_log_info.log_id_;
      rpc_mc_log_info.timestamp_ = mc_log_info.timestamp_;
    }
  }
  STORAGE_LOG(INFO, "remove member finish", K(arg), K(ret));
  return ret;
}

int ObPartitionService::batch_remove_replica_mc(
    const obrpc::ObMemberChangeBatchArg& arg, obrpc::ObMemberChangeBatchResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", "arg_cnt", arg.arg_array_.count(), K(ret));
  } else {
    common::ObPartitionArray partition_array;
    common::ObMemberArray member_array;
    common::ObQuorumArray quorum_array;
    common::ObReturnArray ret_array;
    ObMCLogInfoArray log_info_array;
    ObSArray<int64_t> index_array;

    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); i++) {
      const obrpc::ObMemberChangeArg& mc_arg = arg.arg_array_.at(i);
      ObIPartitionGroupGuard guard;
      int64_t replica_num = OB_INVALID_COUNT;
      // insert the placeholder ret first
      if (OB_FAIL(result.return_array_.push_back(OB_ERROR))) {
        STORAGE_LOG(WARN, "fail to push_back ret", K(ret));
        break;
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = get_partition(mc_arg.key_, guard))) {
        STORAGE_LOG(WARN, "get partition failed", K(mc_arg.key_), K(tmp_ret));
      } else if (OB_ISNULL(guard.get_partition_group())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get partition", K(mc_arg), K(tmp_ret));
      } else if (OB_SUCCESS !=
                 (tmp_ret = guard.get_partition_group()->get_log_service()->get_replica_num(replica_num))) {
        STORAGE_LOG(WARN, "fail to get replica num", K(tmp_ret));
      } else if (OB_INVALID_COUNT == replica_num || mc_arg.orig_quorum_ != replica_num) {
        // Check whether the quorum seen by RS is same with the quorum of the underlying storage
        tmp_ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "replica_num not match", K(mc_arg), K(replica_num), K(tmp_ret));
      }

      if (OB_FAIL(tmp_ret)) {
        result.return_array_.at(i) = tmp_ret;
      } else {
        if (OB_FAIL(partition_array.push_back(mc_arg.key_))) {
          STORAGE_LOG(WARN, "partition_array push_back failed", K(ret));
        } else if (OB_FAIL(member_array.push_back(mc_arg.member_))) {
          STORAGE_LOG(WARN, "member_array push_back failed", K(ret));
        } else if (OB_FAIL(quorum_array.push_back(mc_arg.quorum_))) {
          STORAGE_LOG(WARN, "quorum_array push_back failed", K(ret));
        } else if (OB_FAIL(index_array.push_back(i))) {
          STORAGE_LOG(WARN, "index_array push_back failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t partition_cnt = partition_array.count();
      const int64_t member_cnt = member_array.count();
      const int64_t quorum_cnt = quorum_array.count();
      const int64_t index_cnt = index_array.count();
      if (result.return_array_.count() != arg.arg_array_.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "count not matched",
            K(ret),
            "arg_cnt",
            arg.arg_array_.count(),
            "result_cnt",
            result.return_array_.count());
      } else if (partition_cnt != member_cnt || partition_cnt != quorum_cnt || partition_cnt != index_cnt) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "count not matched", K(ret), K(partition_cnt), K(quorum_cnt), K(member_cnt), K(index_cnt));
      } else if (OB_ISNULL(clog_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "clog_mgr_ is null", K(ret));
      } else if (partition_cnt > 0) {

        const int64_t start = ObTimeUtility::current_time();
        if (OB_FAIL(clog_mgr_->batch_remove_member(
                partition_array, member_array, quorum_array, ret_array, log_info_array))) {
          STORAGE_LOG(WARN, "batch_remove_member failed", K(ret));
        }
        STORAGE_LOG(INFO,
            "batch_remove_member",
            K(ret),
            "partition_cnt",
            partition_array.count(),
            "cost",
            ObTimeUtility::current_time() - start);

        if (OB_FAIL(ret)) {
          // skip
        } else if (ret_array.count() != partition_cnt) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "ret_array count not match with partition_array", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < ret_array.count(); i++) {
            const int64_t index = index_array.at(i);
            if (index >= result.return_array_.count()) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "invalid index", K(ret), "result_cnt", result.return_array_.count(), K(i), K(index));
            } else if (OB_ERROR != result.return_array_.at(index)) {
              // continue
              STORAGE_LOG(
                  WARN, "already has return code", "ret", OB_ERROR, "return_ret", result.return_array_.at(index));
            } else {
              result.return_array_.at(i) = ret_array.at(i);
            }
          }
        }
      }
    }
  }
  return ret;
}

// modify quorum value, write member change log
int ObPartitionService::change_quorum_mc(const obrpc::ObModifyQuorumArg& arg, obrpc::ObMCLogRpcInfo& rpc_mc_log_info)
{
  int ret = OB_SUCCESS;
  ObMCLogInfo mc_log_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(ret));
  } else {
    rpc_mc_log_info.reset();
    rpc_mc_log_info.key_ = arg.key_;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(arg.key_, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->get_log_service()->change_quorum(
                   arg.member_list_, arg.orig_quorum_, arg.quorum_, mc_log_info))) {
      STORAGE_LOG(WARN, "fail to modify quorum", K(arg), K(ret));
    } else {
      rpc_mc_log_info.log_id_ = mc_log_info.log_id_;
      rpc_mc_log_info.timestamp_ = mc_log_info.timestamp_;
    }
  }
  STORAGE_LOG(INFO, "modify quorum finish", K(arg), K(ret));
  return ret;
}

int ObPartitionService::migrate_replica(const obrpc::ObMigrateReplicaArg& rpc_arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObReplicaOpArg arg;
  arg.key_ = rpc_arg.key_;
  arg.src_ = rpc_arg.src_;
  arg.dst_ = rpc_arg.dst_;
  arg.data_src_ = rpc_arg.data_source_;
  arg.type_ = MIGRATE_REPLICA_OP;
  arg.quorum_ = rpc_arg.quorum_;
  arg.change_member_option_ = rpc_arg.skip_change_member_list_ ? SKIP_CHANGE_MEMBER_LIST : NORMAL_CHANGE_MEMBER_LIST;
  arg.switch_epoch_ = rpc_arg.switch_epoch_;

  STORAGE_LOG(INFO, "begin migrate_replica", K(arg));
  SERVER_EVENT_ADD("storage", "migrate_replica begin", "partition", arg.key_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(arg.key_), K(ret));
  } else if (OB_FAIL(check_add_or_migrate_replica_arg(arg.key_, arg.dst_, arg.src_, arg.data_src_, arg.quorum_))) {
    STORAGE_LOG(WARN, "failed to check_add_or_migrate_replica_arg", K(ret), K(arg));
  } else if (arg.src_.get_replica_type() != arg.dst_.get_replica_type()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "replica type not match", K(arg), K(ret));
  } else if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(arg, task_id))) {
    STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
  }

  if (OB_FAIL(ret)) {
    submit_pt_update_task_(arg.key_);
    SERVER_EVENT_ADD("storage", "migrate_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::is_member_change_done(
    const common::ObPartitionKey& key, const uint64_t log_id, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!key.is_valid() || timestamp < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(key), K(key), K(timestamp));
  } else {
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(key, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(key), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(key), K(ret));
    } else if (NULL == guard.get_partition_group()->get_log_service()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "log service must not null", K(ret), K(key));
    } else {
      obrpc::ObMCLogInfo log_info;
      log_info.log_id_ = log_id;
      log_info.timestamp_ = timestamp;
      if (OB_FAIL(guard.get_partition_group()->get_log_service()->is_member_change_done(log_info))) {
        if (OB_EAGAIN != ret && OB_MEMBER_CHANGE_FAILED != ret) {
          STORAGE_LOG(WARN, "failed to do is_member_change_done", K(ret), K(key), K(log_info));
        }
      }
    }
  }
  STORAGE_LOG(INFO, "is_member_change_done", K(ret), K(key), K(log_id), K(timestamp));
  return ret;
}

int ObPartitionService::retry_send_add_replica_mc_msg(common::ObAddr& leader, const obrpc::ObMemberChangeArg& arg)
{
  int ret = OB_SUCCESS;
  obrpc::ObMCLogRpcInfo mc_log_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_paxos_replica(arg.member_.get_replica_type())) {
    STORAGE_LOG(INFO, "is not paxos member not need add to member list", K(arg), K(ret));
  } else if (OB_FAIL(retry_post_add_replica_mc_msg(leader, arg, mc_log_info))) {
    STORAGE_LOG(WARN, "failed to retry_post_add_replica_mc_msg", K(arg), K(ret));
  } else if (arg.key_ != mc_log_info.key_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "key must be same", K(ret), K(arg), K(mc_log_info));
  } else if (OB_FAIL(retry_get_is_member_change_done(leader, mc_log_info))) {
    STORAGE_LOG(WARN, "failed to retry_get_is_member_change_done", K(arg), K(arg));
  }

  return ret;
}

int ObPartitionService::retry_send_remove_replica_mc_msg(common::ObAddr& leader, const obrpc::ObMemberChangeArg& arg)
{
  int ret = OB_SUCCESS;
  obrpc::ObMCLogRpcInfo mc_log_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_paxos_replica(arg.member_.get_replica_type())) {
    STORAGE_LOG(INFO, "is not paxos member not need remove from member list", K(arg), K(ret));
  } else if (OB_FAIL(retry_post_remove_replica_mc_msg(leader, arg, mc_log_info))) {
    STORAGE_LOG(WARN, "failed to retry_post_remove_replica_mc_msg", K(leader), K(arg), K(ret));
  } else if (arg.key_ != mc_log_info.key_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "key must be same", K(arg), K(mc_log_info), K(ret));
  } else if (OB_FAIL(retry_get_is_member_change_done(leader, mc_log_info))) {
    STORAGE_LOG(WARN, "failed to retry_get_is_member_change_done", K(leader), K(mc_log_info));
  }

  return ret;
}

int ObPartitionService::retry_post_add_replica_mc_msg(
    common::ObAddr& leader, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t stop_time = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!leader.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(leader), K(arg), K(ret));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_POST_ADD_REPILICA_MC) OB_SUCCESS;
  }
#endif

  while (OB_SUCC(ret)) {
    if (OB_SUCC(pts_rpc_.post_add_replica_mc_msg(leader, arg, mc_log_info))) {
      STORAGE_LOG(INFO, "post post add replica member change msg succeed.", K(leader), K(arg), K(mc_log_info));
      break;
    } else {
      stop_time = ObTimeUtility::current_time();
      if (OB_NOT_SUPPORTED == ret) {
        STORAGE_LOG(WARN, "post add replica member change system error.", K(leader), K(arg), K(ret));
        break;
      } else if (stop_time - start_time > MC_WAIT_TIMEOUT) {
        STORAGE_LOG(WARN, "retry add replica timeout", K(leader), K(arg), K(ret), K(stop_time - start_time));
        break;
      } else {
        usleep(MC_SLEEP_TIME);
        ret = OB_SUCCESS;  // mute the coverity checking

        if (OB_FAIL(location_cache_->get_leader_by_election(arg.key_, leader, true))) {
          if (OB_LOCATION_NOT_EXIST != ret && OB_LOCATION_LEADER_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "get leader addr fail", K(arg.key_), K(ret));
          } else {
            STORAGE_LOG(WARN, "leader not exist. sleep and retry.", K(leader), K(arg.key_), K(ret));
            ret = OB_SUCCESS;
          }
        } else {
          STORAGE_LOG(WARN,
              "leader changing...retry add replica with new leader",
              K(leader),
              K(arg),
              K(ret),
              K(stop_time - start_time));
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::retry_get_is_member_change_done(common::ObAddr& leader, obrpc::ObMCLogRpcInfo& mc_log_info)
{
  int ret = OB_SUCCESS;
  common::ObAddr tmp_leader;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t stop_time = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!leader.is_valid() || !mc_log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(leader), K(mc_log_info));
  }
  while (OB_SUCC(ret)) {
    ret = pts_rpc_.is_member_change_done(leader, mc_log_info);
    stop_time = ObTimeUtility::current_time();

    if (OB_SUCCESS == ret) {
      STORAGE_LOG(
          INFO, "is_member_change_done succeed.", K(leader), K(mc_log_info), "cost_time", stop_time - start_time);
      break;
    } else if (OB_MEMBER_CHANGE_FAILED == ret || OB_NOT_SUPPORTED == ret) {
      STORAGE_LOG(WARN, "retry_get_is_member_change_done failed.", K(ret), K(leader), K(mc_log_info));
      break;
    } else {
      STORAGE_LOG(INFO, "change member is not done", K(ret), K(leader), K(mc_log_info));

      if (stop_time - start_time > MC_TASK_TIMEOUT) {
        STORAGE_LOG(WARN,
            "retry_get_is_member_change_done timeout",
            K(ret),
            K(leader),
            K(mc_log_info),
            K(stop_time - start_time));
        break;
      } else {
        usleep(MC_SLEEP_TIME);
        ret = OB_SUCCESS;  // mute the coverity checking

        if (OB_FAIL(location_cache_->get_leader_by_election(mc_log_info.key_, tmp_leader, true /*force renew*/))) {
          if (OB_LOCATION_NOT_EXIST != ret && OB_LOCATION_LEADER_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "get leader addr fail", K(ret), K(mc_log_info));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (leader != tmp_leader) {
          STORAGE_LOG(INFO, "leader is changed, use new leader", "old_leader", leader, "new_leader", tmp_leader);
          leader = tmp_leader;
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::retry_post_remove_replica_mc_msg(
    common::ObAddr& leader, const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& mc_log_info)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t stop_time = 0;
  mc_log_info.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!leader.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(leader), K(arg), K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_SUCC(pts_rpc_.post_remove_replica_mc_msg(leader, arg, mc_log_info))) {
      STORAGE_LOG(INFO, "post remove replica member change msg succeed.", K(leader), K(arg));
      break;
    } else {
      stop_time = ObTimeUtility::current_time();
      if (OB_NOT_SUPPORTED == ret) {
        STORAGE_LOG(WARN, "post remove member change system error.", K(leader), K(arg), K(ret));
        break;
      } else if (stop_time - start_time > MC_WAIT_TIMEOUT) {
        STORAGE_LOG(WARN, "retry remove replica timeout", K(leader), K(arg), K(ret), K(stop_time - start_time));
        break;
      } else if (OB_STATE_NOT_MATCH == ret) {
        usleep(MC_SLEEP_TIME);
        ret = OB_SUCCESS;  // mute the coverity checking

        if (OB_FAIL(location_cache_->get_leader_by_election(arg.key_, leader, true))) {
          if (OB_LOCATION_NOT_EXIST != ret && OB_LOCATION_LEADER_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "get leader addr fail", K(arg.key_), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          STORAGE_LOG(WARN,
              "leader changing...retry remove replica with new leader",
              K(leader),
              K(arg),
              K(ret),
              K(stop_time - start_time));
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::retry_get_active_member(
    common::ObAddr& leader, const common::ObPartitionKey& key, common::ObMemberList& mlist)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t stop_time = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!leader.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(leader), K(key), K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_SUCC(pts_rpc_.post_get_member_list_msg(leader, key, mlist))) {
      STORAGE_LOG(INFO, "post post_get_active member_list_msg msg succeed.", K(leader), K(key), K(mlist));
      break;
    } else {
      stop_time = ObTimeUtility::current_time();
      if (stop_time - start_time > MC_WAIT_TIMEOUT) {
        STORAGE_LOG(WARN, "retry get member list timeout", K(leader), K(key), K(ret), K(stop_time - start_time));
        break;
      } else {
        usleep(MC_SLEEP_TIME);
        ret = OB_SUCCESS;  // mute the coverity checking

        if (OB_FAIL(location_cache_->get_leader_by_election(key, leader, true))) {
          if (OB_LOCATION_NOT_EXIST != ret && OB_LOCATION_LEADER_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "get leader addr fail", K(key), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          STORAGE_LOG(WARN,
              "leader changing...retry get active member list with new leader",
              K(leader),
              K(key),
              K(ret),
              K(stop_time - start_time));
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::check_self_in_member_list(const common::ObPartitionKey& key, bool& in_member_list)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  ObMemberList member_list;
  ObAddr self = pts_rpc_.get_self();
  in_member_list = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(key), K(ret));
  } else if (OB_FAIL(location_cache_->get_leader_by_election(key, leader, true))) {
    STORAGE_LOG(WARN, "fail to get standby leader from cache", K(key), K(ret));
  } else if (OB_FAIL(retry_get_active_member(leader, key, member_list))) {
    STORAGE_LOG(WARN, "fail to get leader from cache", K(key), K(ret));
  } else {
    in_member_list = member_list.contains(self);
  }
  return ret;
}

template <typename ResultT>
int ObPartitionService::get_operate_replica_res(const ObReplicaOpArg& arg, const int result, ResultT& res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(result), K(ret));
  } else {
    res.key_ = arg.key_;
    res.src_ = arg.src_;
    res.dst_ = arg.dst_;
    res.data_src_ = arg.data_src_;
    res.result_ = result;
    switch (arg.type_) {
      case ADD_REPLICA_OP:  // get through
      case CHANGE_REPLICA_OP:
        // FIXME, no quorum now
        // res.quorum_ = arg.quorum_;
        break;
      case FAST_MIGRATE_REPLICA_OP:
      case MIGRATE_REPLICA_OP:
      case REBUILD_REPLICA_OP:
      case COPY_GLOBAL_INDEX_OP:
      case COPY_LOCAL_INDEX_OP:
      case RESTORE_FOLLOWER_REPLICA_OP:
      case BACKUP_REPLICA_OP:
      case RESTORE_STANDBY_OP:
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unknown ObReplicaOpType", K(arg), K(ret));
        break;
    }
  }
  return ret;
}

int ObPartitionService::retry_post_operate_replica_res(const ObReplicaOpArg& arg, const int result)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  ObAddr rs_addr;

  STORAGE_LOG(INFO, "retry_post_operate_replica_res", K(arg), K(result));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized. ", K(ret));
  } else {
    if (REBUILD_REPLICA_OP == arg.type_) {
      // update need_rebuild after rebuild finish
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = handle_rebuild_result_(arg.key_, arg.dst_.get_replica_type(), result))) {
        STORAGE_LOG(WARN, "failed to handle_rebuild_result_", K(tmp_ret), K(arg), K(result));
      }
    } else if (ADD_REPLICA_OP == arg.type_) {
      if (GCTX.is_standby_cluster() && !ObMultiClusterUtil::is_cluster_private_table(arg.key_.get_table_id()) &&
          OB_SUCCESS == result) {
        // It is possible that is_restore=100 when adding a replica of a non-private table in the
        // standby cluster, and clog is offline and cannot automatically trigger the restore.
        // You need to explicitly join the restore retry queue, provided that the result of
        // add_replica is SUCCESS (otherwise the add replica task will be retryed), try to trigger
        // the restore here to avoid being affected by the failure of following logic.
        int tmp_ret = OB_SUCCESS;
        const common::ObPartitionKey& pkey = arg.key_;
        bool is_clog_offline = false;
        ObIPartitionGroupGuard guard;
        ObIPartitionGroup* partition = NULL;
        ObIPartitionLogService* pls = NULL;
        if (OB_SUCCESS != (tmp_ret = get_partition(pkey, guard))) {
          STORAGE_LOG(WARN, "get partition failed", K(pkey), K(tmp_ret));
        } else if (NULL == (partition = guard.get_partition_group())) {
          tmp_ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get partition failed", K(pkey), K(tmp_ret));
        } else if (NULL == (pls = partition->get_log_service())) {
          tmp_ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get_log_service failed", K(pkey), K(tmp_ret));
        } else if (!pls->is_standby_restore_state()) {
          // no need to restore, skip
        } else if (OB_SUCCESS != (tmp_ret = pls->is_offline(is_clog_offline))) {
          STORAGE_LOG(WARN, "is_offline failed", K(pkey), K(tmp_ret));
        } else if (is_clog_offline) {
          (void)partition->set_need_standby_restore();
          if (OB_SUCCESS != (tmp_ret = push_into_migrate_retry_queue(pkey, RETRY_STANDBY_RESTORE))) {
            STORAGE_LOG(WARN, "push_into_migrate_retry_queue failed", K(pkey), K(tmp_ret));
          } else {
            STORAGE_LOG(INFO, "push_into_migrate_retry_queue success", K(pkey), K(tmp_ret));
          }
        } else {
        }
      }
    }

    while (retry_times++ < MAX_RETRY_TIMES) {
      if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
        STORAGE_LOG(WARN, "get master root service failed", K(ret));
      } else {
        switch (arg.type_) {
          case ADD_REPLICA_OP: {
            obrpc::ObAddReplicaRes res;
            if (OB_FAIL(get_operate_replica_res(arg, result, res))) {
              STORAGE_LOG(WARN, "get operate replica result failed", K(arg), K(result), K(ret));
            } else if (OB_FAIL(pts_rpc_.post_add_replica_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post add replica result failed", K(res), K(rs_addr), K(ret));
            } else {
            }
            break;
          }
          case FAST_MIGRATE_REPLICA_OP:
          case MIGRATE_REPLICA_OP: {
            obrpc::ObMigrateReplicaRes res;
            if (OB_FAIL(get_operate_replica_res(arg, result, res))) {
              STORAGE_LOG(WARN, "get operate replica result failed", K(arg), K(result), K(ret));
            } else if (OB_FAIL(pts_rpc_.post_migrate_replica_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post migrate replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case REBUILD_REPLICA_OP: {
            // do nothing
            break;
          }
          case COPY_GLOBAL_INDEX_OP: {
            obrpc::ObCopySSTableRes res;
            obrpc::ObCopySSTableBatchRes results;
            res.type_ = OB_COPY_SSTABLE_TYPE_GLOBAL_INDEX;
            results.type_ = OB_COPY_SSTABLE_TYPE_GLOBAL_INDEX;
            if (OB_FAIL(get_operate_replica_res(arg, result, res))) {
              STORAGE_LOG(WARN, "get operate replica result failed", K(arg), K(result), K(ret));
            } else if (OB_FAIL(results.res_array_.push_back(res))) {
              STORAGE_LOG(WARN, "failed to pus res to array", K(ret));
            } else if (OB_FAIL(pts_rpc_.post_batch_copy_sstable_res(rs_addr, results))) {
              STORAGE_LOG(WARN, "post copy global index result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case COPY_LOCAL_INDEX_OP: {
            obrpc::ObCopySSTableRes res;
            obrpc::ObCopySSTableBatchRes results;
            res.type_ = OB_COPY_SSTABLE_TYPE_LOCAL_INDEX;
            res.index_table_id_ = arg.index_id_;
            results.type_ = OB_COPY_SSTABLE_TYPE_LOCAL_INDEX;
            if (OB_FAIL(get_operate_replica_res(arg, result, res))) {
              STORAGE_LOG(WARN, "get operate replica result failed", K(arg), K(result), K(ret));
            } else if (OB_FAIL(results.res_array_.push_back(res))) {
              STORAGE_LOG(WARN, "failed to pus res to array", K(ret));
            } else if (OB_FAIL(pts_rpc_.post_batch_copy_sstable_res(rs_addr, results))) {
              STORAGE_LOG(WARN, "post copy local index replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case CHANGE_REPLICA_OP: {
            obrpc::ObChangeReplicaRes res;
            if (OB_FAIL(get_operate_replica_res(arg, result, res))) {
              STORAGE_LOG(WARN, "get operate replica result failed", K(arg), K(result), K(ret));
            } else if (OB_FAIL(pts_rpc_.post_change_replica_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post change replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case RESTORE_REPLICA_OP: {
            if (arg.is_physical_restore_leader()) {
              obrpc::ObPhyRestoreReplicaRes res;
              res.key_ = arg.key_;
              res.src_ = arg.phy_restore_arg_;
              res.dst_ = arg.dst_;
              res.result_ = result;
              if (OB_FAIL(pts_rpc_.post_phy_restore_replica_res(rs_addr, res))) {
                STORAGE_LOG(WARN, "post physical restore replica result failed", K(res), K(rs_addr), K(ret));
              } else {
                STORAGE_LOG(INFO, "post physical restore replica res succ", K(ret), K(arg), K(res));
              }
            } else {
              obrpc::ObRestoreReplicaRes res;
              res.key_ = arg.key_;
              res.src_ = arg.restore_arg_;
              res.dst_ = arg.dst_;
              res.result_ = result;
              if (OB_FAIL(pts_rpc_.post_restore_replica_res(rs_addr, res))) {
                STORAGE_LOG(WARN, "post restore replica result failed", K(res), K(rs_addr), K(ret));
              }
            }
            break;
          }
          case RESTORE_FOLLOWER_REPLICA_OP: {
            obrpc::ObCopySSTableRes res;
            obrpc::ObCopySSTableBatchRes results;
            res.type_ = ObCopySSTableType::OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER;
            results.type_ = ObCopySSTableType::OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER;
            if (OB_FAIL(get_operate_replica_res(arg, result, res))) {
              STORAGE_LOG(WARN, "get operate replica result failed", K(arg), K(result), K(ret));
            } else if (OB_FAIL(results.res_array_.push_back(res))) {
              STORAGE_LOG(WARN, "failed to pus res to array", K(ret));
            } else if (OB_FAIL(pts_rpc_.post_batch_copy_sstable_res(rs_addr, results))) {
              STORAGE_LOG(WARN, "post copy global index result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case LINK_SHARE_MAJOR_OP:
          case BACKUP_REPLICA_OP: {
            // do nothing
            break;
          }
          case RESTORE_STANDBY_OP: {
            if (OB_SUCCESS != result) {
              // restore task failed in standby cluster, retry
              const ObPartitionKey& pkey = arg.key_;
              ObIPartitionGroupGuard guard;
              ObIPartitionGroup* partition = NULL;
              if (OB_FAIL(get_partition(pkey, guard))) {
                STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
              } else if (NULL == (partition = guard.get_partition_group())) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
              } else {
                // mark the partition when standby_leader fails and wait for retry
                STORAGE_LOG(INFO, "restore fail, set flag for retry", K(arg), K(result));
                (void)partition->set_need_standby_restore();
                int tmp_ret = OB_SUCCESS;
                if (OB_SUCCESS != (tmp_ret = push_into_migrate_retry_queue(pkey, RETRY_STANDBY_RESTORE))) {
                  STORAGE_LOG(WARN, "push_into_migrate_retry_queue failed", K(pkey), K(tmp_ret));
                }
              }
            }
            break;
          }
          case VALIDATE_BACKUP_OP: {
            obrpc::ObValidateRes res;
            res.key_ = arg.key_;
            res.dst_ = arg.dst_;
            res.validate_arg_ = arg.validate_arg_;
            res.result_ = result;
            if (OB_FAIL(pts_rpc_.post_validate_backup_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post validate backup result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          default:
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unknown operate replica type", K(arg), K(ret));
            break;
        }
      }
      if (OB_RS_NOT_MASTER != ret) {
        if (OB_SUCC(ret)) {
          STORAGE_LOG(INFO, "post replica res successfully", K(arg), K(rs_addr));
        }
        break;
      } else if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {
        STORAGE_LOG(WARN, "renew master root service failed", K(ret));
      } else {
        // retry
      }
    }
  }
  return ret;
}

int ObPartitionService::retry_post_batch_migrate_replica_res(
    const ObReplicaOpType& type, const ObArray<ObPartMigrationRes>& report_res_list)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  ObAddr rs_addr;

  STORAGE_LOG(INFO, "retry_post_batch_operate_replica_res", K(type), K(report_res_list));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized. ", K(ret));
  } else {
    if (REBUILD_REPLICA_OP == type) {
      // update need_rebuild after rebuild finish
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < report_res_list.count(); ++i) {
        ObPartMigrationRes tmp_res = report_res_list.at(i);
        if (OB_SUCCESS !=
            (tmp_ret = handle_rebuild_result_(tmp_res.key_, tmp_res.dst_.get_replica_type(), tmp_res.result_))) {
          STORAGE_LOG(WARN, "failed to handle_rebuild_result", K(tmp_ret), K(tmp_res));
        }
      }
    }
    while (retry_times++ < MAX_RETRY_TIMES) {
      if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
        STORAGE_LOG(WARN, "get master root service failed", K(ret));
      } else {
        switch (type) {
          case FAST_MIGRATE_REPLICA_OP:
          case MIGRATE_REPLICA_OP: {
            obrpc::ObMigrateReplicaBatchRes res;
            if (OB_FAIL(build_migrate_replica_batch_res(report_res_list, res))) {
              STORAGE_LOG(WARN, "failed to build_migrate_replica_batch_res", K(ret), K(type), K(report_res_list));
            } else if (OB_FAIL(pts_rpc_.post_batch_migrate_replica_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post batch migrate replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case CHANGE_REPLICA_OP: {
            ObChangeReplicaBatchRes res;
            if (OB_FAIL(build_change_replica_batch_res(report_res_list, res))) {
              STORAGE_LOG(WARN, "failed to build_change_replica_batch_res", K(ret), K(type), K(report_res_list));
            } else if (OB_FAIL(pts_rpc_.post_batch_change_replica_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post batch migrate replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case BACKUP_REPLICA_OP: {
            obrpc::ObBackupBatchRes res;
            if (OB_FAIL(build_backup_replica_batch_res(report_res_list, res))) {
              STORAGE_LOG(WARN, "failed to build_change_replica_batch_res", K(ret), K(type), K(report_res_list));
            } else if (OB_FAIL(pts_rpc_.post_batch_backup_replica_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post migrate replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case ADD_REPLICA_OP: {
            ObAddReplicaBatchRes res;
            if (OB_FAIL(build_add_replica_batch_res(report_res_list, res))) {
              STORAGE_LOG(WARN, "failed to build_add_replica_batch_res", K(ret), K(type), K(report_res_list));
            } else if (OB_FAIL(pts_rpc_.post_batch_add_replica_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post batch migrate replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case VALIDATE_BACKUP_OP: {
            obrpc::ObValidateBatchRes res;
            if (OB_FAIL(build_validate_backup_batch_res(report_res_list, res))) {
              STORAGE_LOG(WARN, "failed to build_validate_backup_batch_res", K(ret), K(report_res_list));
            } else if (OB_FAIL(pts_rpc_.post_batch_validate_backup_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post batch validate backup result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case BACKUP_BACKUPSET_OP: {
            obrpc::ObBackupBackupsetBatchRes res;
            if (OB_FAIL(build_backup_backupset_batch_res(report_res_list, res))) {
              STORAGE_LOG(WARN, "failed to build_backup_backupset_batch_res", K(ret), K(type), K(report_res_list));
            } else if (OB_FAIL(report_pg_backup_backupset_task(report_res_list))) {
              LOG_WARN("failed to report pg backup task", K(ret), K(type), K(report_res_list));
            } else if (OB_FAIL(pts_rpc_.post_batch_backup_backupset_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "post migrate replica result failed", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          case BACKUP_ARCHIVELOG_OP: {
            obrpc::ObBackupArchiveLogBatchRes res;
            if (OB_FAIL(build_backup_archivelog_batch_res(report_res_list, res))) {
              STORAGE_LOG(WARN, "failed to build_backup_archivelog_batch_res", K(ret), K(type), K(report_res_list));
            } else if (OB_FAIL(pts_rpc_.post_batch_backup_archivelog_res(rs_addr, res))) {
              STORAGE_LOG(WARN, "failed to post batch backup archivelog res", K(res), K(rs_addr), K(ret));
            }
            break;
          }
          default:
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "not supported operate replica type", K(type), K(report_res_list));
            break;
        }
      }
      if (OB_RS_NOT_MASTER != ret) {
        if (OB_SUCC(ret)) {
          STORAGE_LOG(INFO, "post batch replica res successfully", K(rs_addr), K(type), K(report_res_list));
        }
        break;
      } else if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {
        STORAGE_LOG(WARN, "renew master root service failed", K(ret));
      }
    }
  }
  return ret;
}

// static func
int ObPartitionService::build_migrate_replica_batch_res(
    const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObMigrateReplicaBatchRes& res)
{
  int ret = OB_SUCCESS;
  obrpc::ObMigrateReplicaRes tmp_res;
  res.res_array_.reset();

  if (OB_FAIL(res.res_array_.reserve(report_res_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve res array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      tmp_res.key_ = report_res_list.at(i).key_;
      tmp_res.src_ = report_res_list.at(i).src_;
      tmp_res.dst_ = report_res_list.at(i).dst_;
      tmp_res.data_src_ = report_res_list.at(i).data_src_;
      tmp_res.result_ = report_res_list.at(i).result_;
      if (OB_FAIL(res.res_array_.push_back(tmp_res))) {
        STORAGE_LOG(WARN, "failed to add res", K(ret));
      }
    }
  }
  return ret;
}
// static func
int ObPartitionService::build_change_replica_batch_res(
    const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObChangeReplicaBatchRes& res)
{
  int ret = OB_SUCCESS;
  obrpc::ObChangeReplicaRes tmp_res;
  res.res_array_.reset();

  if (OB_FAIL(res.res_array_.reserve(report_res_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve res array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      tmp_res.key_ = report_res_list.at(i).key_;
      tmp_res.src_ = report_res_list.at(i).src_;
      tmp_res.dst_ = report_res_list.at(i).dst_;
      tmp_res.data_src_ = report_res_list.at(i).data_src_;
      tmp_res.result_ = report_res_list.at(i).result_;
      if (OB_FAIL(res.res_array_.push_back(tmp_res))) {
        STORAGE_LOG(WARN, "failed to add res", K(ret));
      }
    }
  }
  return ret;
}

// static func
int ObPartitionService::build_add_replica_batch_res(
    const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObAddReplicaBatchRes& res)
{
  int ret = OB_SUCCESS;
  obrpc::ObAddReplicaRes tmp_res;
  res.res_array_.reset();

  if (OB_FAIL(res.res_array_.reserve(report_res_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve res array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      tmp_res.key_ = report_res_list.at(i).key_;
      tmp_res.src_ = report_res_list.at(i).src_;
      tmp_res.dst_ = report_res_list.at(i).dst_;
      tmp_res.data_src_ = report_res_list.at(i).data_src_;
      tmp_res.result_ = report_res_list.at(i).result_;
      if (OB_FAIL(res.res_array_.push_back(tmp_res))) {
        STORAGE_LOG(WARN, "failed to add res", K(ret));
      }
    }
  }
  return ret;
}

// static func
int ObPartitionService::build_backup_replica_batch_res(
    const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObBackupBatchRes& res)
{
  int ret = OB_SUCCESS;
  obrpc::ObBackupRes tmp_res;
  res.res_array_.reset();

  if (OB_FAIL(res.res_array_.reserve(report_res_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve res array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      tmp_res.key_ = report_res_list.at(i).key_;
      tmp_res.src_ = report_res_list.at(i).data_src_;
      tmp_res.dst_ = report_res_list.at(i).dst_;
      tmp_res.data_src_ = report_res_list.at(i).data_src_;
      tmp_res.physical_backup_arg_ = report_res_list.at(i).backup_arg_;
      tmp_res.result_ = report_res_list.at(i).result_;
      if (OB_FAIL(res.res_array_.push_back(tmp_res))) {
        STORAGE_LOG(WARN, "failed to add res", K(ret));
      }
    }
  }
  return ret;
}

// static func
int ObPartitionService::build_validate_backup_batch_res(
    const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObValidateBatchRes& res)
{
  int ret = OB_SUCCESS;
  obrpc::ObValidateRes tmp_res;
  res.res_array_.reset();

  if (OB_FAIL(res.res_array_.reserve(report_res_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve res array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      tmp_res.key_ = report_res_list.at(i).key_;
      tmp_res.dst_ = report_res_list.at(i).dst_;
      tmp_res.validate_arg_ = report_res_list.at(i).validate_arg_;
      tmp_res.result_ = report_res_list.at(i).result_;
      if (OB_FAIL(res.res_array_.push_back(tmp_res))) {
        STORAGE_LOG(WARN, "failed to add res", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionService::build_backup_backupset_batch_res(
    const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObBackupBackupsetBatchRes& res)
{
  int ret = OB_SUCCESS;
  obrpc::ObBackupBackupsetReplicaRes tmp_res;
  res.res_array_.reset();

  if (OB_FAIL(res.res_array_.reserve(report_res_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve res array", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      const common::ObAddr& server = report_res_list.at(i).backup_backupset_arg_.server_;
      tmp_res.key_ = report_res_list.at(i).key_;
      tmp_res.dst_ = ObReplicaMember(server, ObTimeUtility::current_time(), REPLICA_TYPE_FULL, 0);
      tmp_res.arg_ = report_res_list.at(i).backup_backupset_arg_;
      tmp_res.result_ = report_res_list.at(i).result_;
      if (OB_FAIL(res.res_array_.push_back(tmp_res))) {
        STORAGE_LOG(WARN, "failed to add res", KR(ret));
      }
    }
  }
  return ret;
}

int ObPartitionService::build_backup_archivelog_batch_res(
    const ObArray<ObPartMigrationRes>& report_res_list, obrpc::ObBackupArchiveLogBatchRes& res)
{
  int ret = OB_SUCCESS;
  res.res_array_.reset();

  if (OB_FAIL(res.res_array_.reserve(report_res_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve res array", KR(ret));
  } else if (report_res_list.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      obrpc::ObPGBackupArchiveLogRes tmp_res;
      const share::ObBackupArchiveLogArg& arg = report_res_list.at(i).backup_archivelog_arg_;
      tmp_res.result_ = report_res_list.at(i).result_;
      tmp_res.pg_key_ = arg.pg_key_;
      res.tenant_id_ = arg.pg_key_.get_tenant_id();
      res.archive_round_ = arg.log_archive_round_;
      res.piece_id_ = arg.piece_id_;
      res.job_id_ = arg.job_id_;
      res.checkpoint_ts_ = arg.rs_checkpoint_ts_;
      res.server_ = GCTX.self_addr_;
      if (OB_FAIL(res.res_array_.push_back(tmp_res))) {
        STORAGE_LOG(WARN, "failed to add res", KR(ret));
      }
    }
  }
  return ret;
}

int ObPartitionService::check_mc_allowed_by_server_lease(bool& is_mc_allowed)
{
  int ret = OB_SUCCESS;
  observer::ObService* ob_service = GCTX.ob_service_;
  int64_t lease_expire_time = OB_INVALID_TIMESTAMP;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(NULL == ob_service)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ob_service shouldn't be null", K(ret), KP(ob_service));
  } else if (OB_SUCCESS != (ret = ob_service->get_server_heartbeat_expire_time(lease_expire_time))) {
    STORAGE_LOG(WARN, "fail to get lease expire time", K(ret));
  } else {
    int64_t now = ObTimeUtility::current_time();
    is_mc_allowed = (now + MC_INTERVAL_BEFORE_LEASE_EXPIRE < lease_expire_time);
    if (!is_mc_allowed) {
      STORAGE_LOG(INFO, "lease is not enough to finish a member change", K(now), K(lease_expire_time));
    }
  }
  return ret;
}

int ObPartitionService::handle_add_replica_callback(const ObReplicaOpArg& arg, const int result)
{
  int ret = OB_SUCCESS;
  obrpc::ObAddReplicaRes res;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(result), K(ret));
  } else if (OB_SUCCESS != result) {
    STORAGE_LOG(INFO, "add replica failed, no need to do member change", K(arg), K(result));
  } else if (ObReplicaTypeCheck::is_paxos_replica(arg.dst_.get_replica_type())) {
    obrpc::ObMemberChangeArg add_arg;
    int64_t dummy_orig_quorum = OB_INVALID_COUNT;  // only used in ObPartitionService::batch_remove_replica_mc
    if (OB_FAIL(add_arg.init(arg.key_, arg.dst_, false, arg.quorum_, dummy_orig_quorum))) {
      STORAGE_LOG(WARN, "init ObMemberChangeArg failed", K(arg), K(ret));
    } else if (OB_FAIL(wait_fetch_log(arg.key_))) {
      STORAGE_LOG(WARN, "fetch log fail", K(arg.key_), K(ret));
    } else if (OB_FAIL(try_add_to_member_list(add_arg))) {
      STORAGE_LOG(WARN, "add replica to member list failed", K(add_arg), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage", "add_replica success", "partition", arg.key_);
  } else {
    SERVER_EVENT_ADD("storage", "add_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::handle_migrate_replica_callback(const ObReplicaOpArg& arg, const int result, bool& could_retry)
{
  int ret = OB_SUCCESS;
  could_retry = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(result), K(ret));
  } else if (OB_SUCCESS != result) {
    STORAGE_LOG(INFO, "migrate replica data failed, no need to do member change", K(arg), K(result));
  } else if (ObReplicaTypeCheck::is_paxos_replica(arg.dst_.get_replica_type())) {
    // don't modify quorum when migrate partition, use WITHOUT_MODIFY_QUORUM in type
    share::ObTaskId dummy_id;
    dummy_id.init(self_addr_);
    int64_t dummy_orig_quorum = OB_INVALID_COUNT;  // only used in ObPartitionService::batch_remove_replica_mc
    obrpc::ObMemberChangeArg remove_member_arg = {
        arg.key_, arg.src_, false, arg.quorum_, dummy_orig_quorum, WITHOUT_MODIFY_QUORUM, dummy_id};
    obrpc::ObMemberChangeArg add_member_arg = {
        arg.key_, arg.dst_, false, arg.quorum_, dummy_orig_quorum, WITHOUT_MODIFY_QUORUM, dummy_id};
    if (!ObReplicaTypeCheck::is_paxos_replica(arg.src_.get_replica_type())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "wrong replica type of migrating replica", K(arg), K(ret));
    } else if (OB_FAIL(wait_fetch_log(arg.key_))) {
      STORAGE_LOG(WARN, "fetch log fail", K(arg.key_), K(ret));
    } else if (OB_FAIL(try_remove_from_member_list(remove_member_arg))) {
      could_retry = true;
      STORAGE_LOG(WARN, "remove replica from member list failed", K(ret), K(could_retry), K(remove_member_arg));
    } else if (OB_FAIL(try_add_to_member_list(add_member_arg))) {
      STORAGE_LOG(WARN, "add replica to member list failed", K(ret), K(add_member_arg));
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    obrpc::ObRemoveReplicaArg remove_replica_arg = {arg.key_, arg.src_};
    if (OB_SUCCESS != (tmp_ret = pts_rpc_.post_remove_replica(arg.src_.get_server(), remove_replica_arg))) {
      STORAGE_LOG(WARN, "remove replica from source observer failed", K(remove_replica_arg), K(tmp_ret));
    } else {
      STORAGE_LOG(INFO, "remove source replica successfully", K(arg.src_), K(remove_replica_arg));
    }
  }

  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage", "migrate_replica success", "partition", arg.key_);
  } else {
    SERVER_EVENT_ADD("storage", "migrate_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::handle_rebuild_replica_callback(const ObReplicaOpArg& arg, const int result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_SUCCESS != result) {
    STORAGE_LOG(WARN, "cannot reset rebuild flag when rebuild failed", K(ret), "pkey", arg.key_);
  } else if (OB_FAIL(rs_cb_->report_rebuild_replica(arg.key_, arg.dst_.get_server(), OB_REBUILD_OFF))) {
    STORAGE_LOG(WARN, "report finish rebuild replica failed", K(arg), K(ret));
  }
  if (OB_SUCC(ret) && OB_SUCCESS == result) {
    SERVER_EVENT_ADD("storage", "rebuild_replica success", "partition", arg.key_);
  } else {
    SERVER_EVENT_ADD("storage", "reubild_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::turn_off_rebuild_flag(const ObReplicaOpArg& arg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(rs_cb_->report_rebuild_replica(arg.key_, arg.dst_.get_server(), OB_REBUILD_OFF))) {
    STORAGE_LOG(WARN, "report finish rebuild replica failed", K(arg), K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to turn_off_rebuild_flag", K(arg));
    SERVER_EVENT_ADD("storage", "log is new enough, turn off rebuild flag", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::handle_change_replica_callback(const ObReplicaOpArg& arg, const int result)
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(DELAY_CHANGE_REPLICA_CALLBACK);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(result), K(ret));
  } else if (arg.src_.get_server() != arg.dst_.get_server() ||
             arg.src_.get_replica_type() == arg.dst_.get_replica_type()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "wrong ObReplicaTypeTranformArg", K(arg), K(result), K(ret));
  } else if (OB_FAIL(result)) {
    STORAGE_LOG(INFO, "change replica failed, no need to do member change", K(arg), K(result));
  } else if (!ObReplicaTypeCheck::is_paxos_replica(arg.src_.get_replica_type()) &&
             ObReplicaTypeCheck::is_paxos_replica(arg.dst_.get_replica_type())) {
    share::ObTaskId dummy_id;
    dummy_id.init(self_addr_);
    int64_t dummy_orig_quorum = OB_INVALID_COUNT;  // only used in ObPartitionService::batch_remove_replica_mc
    obrpc::ObMemberChangeArg add_arg = {
        arg.key_, arg.dst_, false, arg.quorum_, dummy_orig_quorum, WITHOUT_MODIFY_QUORUM, dummy_id};
    if (OB_FAIL(try_add_to_member_list(add_arg))) {
      STORAGE_LOG(WARN, "change replica to member list failed", K(add_arg), K(ret));
    } else {
      STORAGE_LOG(INFO, "change replica to member list successfully", K(arg));
    }
  }

  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage", "change_replica success", "partition", arg.key_);
  } else {
    SERVER_EVENT_ADD("storage", "change_replica failed", "partition", arg.key_);
  }
  return ret;
}

int ObPartitionService::wait_clog_replay_over()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 100 * 1000;
  do {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      STORAGE_LOG(INFO, "still waiting for scan log");
    }
    if (observer::SS_STOPPING == GCTX.status_) {
      ret = OB_SERVER_IS_STOPPING;
      STORAGE_LOG(WARN, "observer is stopped!", K(ret));
    } else if (!clog_mgr_->is_scan_finished()) {
      ret = OB_EAGAIN;
      usleep(SLEEP_US);
    } else {
      ret = OB_SUCCESS;
      break;
    }
  } while (is_running_ && OB_EAGAIN == ret);
  return ret;
}

int ObPartitionService::wait_fetch_log(const common::ObPartitionKey& key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else {
    int64_t start = ObTimeUtility::current_time();
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!is_running_)) {
        ret = OB_CANCELED;
        STORAGE_LOG(WARN, "The service is not running, ", K(ret));
      } else {
        ObIPartitionGroupGuard guard;
        if (OB_FAIL(get_partition(key, guard))) {
          STORAGE_LOG(WARN, "get partition failed", K(key), K(ret));
        } else if (OB_ISNULL(guard.get_partition_group())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get partition failed", K(key), K(ret));
        } else {
          bool is_sync = false;
          if (OB_SUCC(guard.get_partition_group()->get_log_service()->is_log_sync_with_leader(is_sync))) {
            if (!is_sync) {
              usleep(MC_WAIT_INTERVAL);
              int64_t end = ObTimeUtility::current_time();
              if (end - start > FETCH_LOG_TIMEOUT) {
                ret = OB_TIMEOUT;
                STORAGE_LOG(WARN, "wait fetch log process timeout", "time", (end - start));
              }
            } else {
              STORAGE_LOG(INFO, "wait fetch log succeed.", K(key), K(ret));
              break;
            }
          } else {
            STORAGE_LOG(WARN, "wait fetch log failed.", K(key), K(ret));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::handle_member_change_callback(const ObReplicaOpArg& arg, const int result, bool& could_retry)
{
  int ret = OB_SUCCESS;
  could_retry = false;

  STORAGE_LOG(INFO, "handle_replica_callback", K(arg), K(result));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg));
  } else if (OB_SUCCESS != result) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "only success migrate can change member list", K(ret), K(result), K(arg));
  } else {
    switch (arg.type_) {
      case ADD_REPLICA_OP:
        // determine whether to add to the member list according to the replica_type of add replica
        if (OB_FAIL(handle_add_replica_callback(arg, result))) {
          STORAGE_LOG(WARN, "handle add replica call back fail", K(arg), K(result), K(ret));
        }
        break;
      case FAST_MIGRATE_REPLICA_OP:
      case MIGRATE_REPLICA_OP:
        // require src/dst to both be paxos member, or not
        // so we can skip check quorom
        if (OB_FAIL(handle_migrate_replica_callback(arg, result, could_retry))) {
          STORAGE_LOG(WARN, "handle migrate replica call back fail", K(arg), K(result), K(ret), K(could_retry));
        }
        break;
      case REBUILD_REPLICA_OP:
      case COPY_GLOBAL_INDEX_OP:
      case COPY_LOCAL_INDEX_OP:
      case RESTORE_FOLLOWER_REPLICA_OP:
      case RESTORE_STANDBY_OP:
        // replica_type won't change
        if (OB_FAIL(handle_rebuild_replica_callback(arg, result))) {
          STORAGE_LOG(WARN, "handle rebuild replica call back fail", K(arg), K(result), K(ret));
        }
        break;
      case CHANGE_REPLICA_OP:
        // requires that the replica_type of src and dst is different
        if (OB_FAIL(handle_change_replica_callback(arg, result))) {
          STORAGE_LOG(WARN, "handle change replica call back fail", K(arg), K(result), K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid operation type", K(arg), K(result), K(ret));
        break;
    }
  }

  return ret;
}

int ObPartitionService::report_migrate_in_indexes(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* ptt = NULL;
    ObTablesHandle tables_handle;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* data_table_schema = NULL;
    const bool with_global_index = false;
    ObArray<ObIndexTableStat> index_status;
    const uint64_t fetch_tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
    } else if (NULL == (ptt = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get partition group", K(ret), K(pkey));
    } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ptt->get_replica_type())) {
      STORAGE_LOG(INFO, "do not need to report migrate in index", K(pkey), "replica_type", ptt->get_replica_type());
    } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(fetch_tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(pkey.get_table_id(), data_table_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "table has been deleted", K(pkey));
    } else if (OB_FAIL(schema_guard.get_index_status(pkey.get_table_id(), with_global_index, index_status))) {
      STORAGE_LOG(WARN, "fail to get index status", K(ret), "table_id", pkey.get_table_id());
    } else {
      const ObTableSchema* table_schema = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_status.count(); ++i) {
        if (OB_FAIL(schema_guard.get_table_schema(index_status.at(i).index_id_, table_schema))) {
          STORAGE_LOG(WARN, "fail to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          // do nothing
        } else if (table_schema->is_storage_local_index_table() &&
                   ObIndexStatus::INDEX_STATUS_UNAVAILABLE == table_schema->get_index_status()) {
          ObBuildIndexScheduleTask task;
          const int64_t schema_version =
              std::max(data_table_schema->get_schema_version(), table_schema->get_schema_version());
          if (OB_FAIL(ObTenantDDLCheckSchemaTask::generate_schedule_index_task(
                  pkey, index_status.at(i).index_id_, schema_version, table_schema->is_unique_index()))) {
            STORAGE_LOG(ERROR, "fail to generate schedule index task", K(ret), K(pkey), K(index_status));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::handle_report_meta_table_callback(
    const ObPartitionKey& pkey, const int result, const bool need_report_checksum)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_merged = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else {
    if (OB_SUCCESS != (tmp_ret = ObPartitionScheduler::get_instance().schedule_merge(pkey, is_merged))) {
      STORAGE_LOG(WARN, "schedule merge failed", K(tmp_ret), K(pkey), K(result));
    }
    // update __all_meta_table regardless of success or failuer.
    submit_pt_update_task_(pkey, need_report_checksum);
  }
  return ret;
}

int ObPartitionService::add_replica_mc(const obrpc::ObMemberChangeArg& arg, obrpc::ObMCLogRpcInfo& rpc_mc_log_info)
{
  int ret = OB_SUCCESS;
  obrpc::ObMCLogInfo mc_log_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(arg), K(ret));
  } else if (!ObReplicaTypeCheck::is_paxos_replica(arg.member_.get_replica_type())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "member of this replica type can not be added to paxos", K(arg), K(ret));
  } else {
    rpc_mc_log_info.reset();
    rpc_mc_log_info.key_ = arg.key_;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(arg.key_, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(arg.key_), K(ret));
    } else if (OB_FAIL(
                   guard.get_partition_group()->get_log_service()->add_member(arg.member_, arg.quorum_, mc_log_info))) {
      STORAGE_LOG(WARN, "fail to add member", K(arg), K(ret));
    } else {
      rpc_mc_log_info.log_id_ = mc_log_info.log_id_;
      rpc_mc_log_info.timestamp_ = mc_log_info.timestamp_;
    }
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(INFO, "add member failed", K(arg), K(ret));
  } else {
    STORAGE_LOG(INFO, "add member successfully", K(arg));
  }
  return ret;
}

int ObPartitionService::process_ms_info_task(ObMsInfoTask& task)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("process_ms_info_task", 50L * 1000L);
  const int64_t now = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (now - task.get_gen_ts() >= ObSlogWriterQueueThread::SLOG_FLUSH_TASK_TIMEOUT_THRESHOLD) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      STORAGE_LOG(WARN, "task has been timeout, drop it", K(ret), K(task));
    }
  } else if (OB_FAIL(on_member_change_success(task.get_pkey(),
                 task.get_log_type(),
                 task.get_ms_log_id(),
                 task.get_mc_timestamp(),
                 task.get_replica_num(),
                 task.get_prev_member_list(),
                 task.get_curr_member_list(),
                 task.get_ms_proposal_id()))) {
    STORAGE_LOG(WARN, "on_member_change_success failed, need retry", K(ret), K(task));
  } else {
    timeguard.click();
    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    if (OB_FAIL(get_partition(task.get_pkey(), guard)) || NULL == guard.get_partition_group() ||
        NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(task));
    } else if (!(guard.get_partition_group()->is_valid())) {
      ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(task), K(ret));
    } else if (OB_FAIL(log_service->renew_ms_log_flush_cb(task))) {
      CLOG_LOG(WARN, "renew_ms_log_flush_cb failed", K(task), K(ret));
    } else {
      // do nothing
    }
    timeguard.click();
  }
  STORAGE_LOG(INFO, "process_ms_info_task finised", K(ret), K(task), K(timeguard));

  return ret;
}

int ObPartitionService::on_member_change_success(const common::ObPartitionKey& partition_key,
    const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
    const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
    const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("ObMemberChangeCallbackTask", 20 * 1000);
  CLOG_LOG(INFO,
      "on member change success start",
      K(partition_key),
      K(ms_log_id),
      K(mc_timestamp),
      K(replica_num),
      K(prev_member_list),
      K(curr_member_list));

  bool self_in_prev_list = prev_member_list.contains(self_addr_);
  bool self_in_curr_list = curr_member_list.contains(self_addr_);

  ObPartitionArray pkeys;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!partition_key.is_valid() || mc_timestamp <= 0 || OB_INVALID_ID == ms_log_id || replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(partition_key),
        K(mc_timestamp),
        K(ms_log_id),
        K(replica_num),
        K(prev_member_list),
        K(curr_member_list),
        K(ret));
  } else if (OB_FAIL(get_partition(partition_key, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(partition_key), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(partition_key), K(ret));
  } else if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(partition_key));
  } else if (OB_FAIL(partition->unblock_partition_split_by_mc())) {
    STORAGE_LOG(WARN, "unlocked partition split by mc failed", K(ret), K(partition_key));
  } else {
    if (!self_in_prev_list && !self_in_curr_list) {
      STORAGE_LOG(INFO,
          "server is neither in previous or current member list, do nothing.",
          K(self_addr_),
          K(prev_member_list),
          K(curr_member_list));
    } else if (self_in_prev_list && (!self_in_curr_list)) {
      // do nothing
    } else if (!self_in_prev_list && self_in_curr_list) {
      if (OFFLINE == partition->get_partition_state() && OB_FAIL(partition->switch_partition_state(F_WORKING))) {
        STORAGE_LOG(WARN, "switch partition state to F_WORKING failed", K(partition_key), K(ret));
      }
    } else {
      // self_in_prev_list && self_in_curr_list, do nothing
    }

    timeguard.click();
    if (self_in_prev_list || self_in_curr_list) {
      // if any member_list contains itself, try to update to slog
      if (OB_FAIL(partition->try_update_clog_member_list(
              ms_log_id, mc_timestamp, replica_num, curr_member_list, ms_proposal_id))) {
        STORAGE_LOG(WARN, "try_update_clog_member_list failed", K(partition_key), K(ret));
      } else {
        STORAGE_LOG(INFO,
            "try_update_clog_member_list success",
            K(partition_key),
            K(mc_timestamp),
            K(ms_log_id),
            K(replica_num),
            K(curr_member_list),
            K(ms_proposal_id));
      }
    }

    timeguard.click();
    int tmp_ret = OB_SUCCESS;
    const bool need_report_checksum = false;
    if (log_type != OB_LOG_START_MEMBERSHIP) {
      if (OB_SUCCESS != (tmp_ret = rs_cb_->submit_pt_update_task(partition_key, need_report_checksum))) {
        STORAGE_LOG(WARN,
            "submit_pt_update_task failed",
            K(partition_key),
            K(mc_timestamp),
            K(prev_member_list),
            K(curr_member_list),
            K_(self_addr),
            K(tmp_ret));
      }
    }
    rs_cb_->submit_pg_pt_update_task(pkeys);
  }

  if (OB_SUCC(ret) && NULL != partition) {
    STORAGE_LOG(INFO, "on member change success finish", K(ret), K(partition_key), K(timeguard));
  } else {
    STORAGE_LOG(WARN, "on member change success failed", K(ret), K(partition_key), K(timeguard));
  }
  return ret;
}

int ObPartitionService::nonblock_renew_loc_cache(const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  const int64_t expire_renew_time = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else {
    int64_t cluster_id = OB_INVALID_ID;
    if (!GCTX.is_standby_cluster() || ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
      cluster_id = GCONF.cluster_id;
    } else {
      cluster_id = location_cache_->get_primary_cluster_id();
    }
    if (OB_FAIL(location_cache_->nonblock_renew(pkey, expire_renew_time, cluster_id))) {
      STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(ret));
    } else {
      // do  nothing
    }
  }
  return ret;
}

int ObPartitionService::nonblock_get_strong_leader_from_loc_cache(
    const common::ObPartitionKey& pkey, ObAddr& leader, int64_t& cluster_id, const bool is_need_renew)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else {
    if (!GCTX.is_standby_cluster() || ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
      cluster_id = GCONF.cluster_id;
    } else {
      cluster_id = location_cache_->get_primary_cluster_id();
    }
    if (is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time, cluster_id))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret), K(cluster_id));
      } else {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          STORAGE_LOG(INFO, "nonblock_renew success", K(pkey), K(cluster_id));
        }
      }
    }
    if (OB_FAIL(location_cache_->nonblock_get_strong_leader_without_renew(pkey, leader, cluster_id))) {
      STORAGE_LOG(DEBUG, "nonblock_get_strong_leader_without_renew failed", K(pkey), K(cluster_id), K(ret));
    } else {
      STORAGE_LOG(DEBUG, "nonblock_get_strong_leader_without_renew success", K(pkey), K(cluster_id), K(ret));
    }
  }
  return ret;
}

int ObPartitionService::nonblock_get_leader_by_election_from_loc_cache(
    const common::ObPartitionKey& pkey, int64_t cluster_id, ObAddr& leader, const bool is_need_renew)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || cluster_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(cluster_id), K(ret));
  } else {
    if (is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time, cluster_id))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret));
      } else {
        STORAGE_LOG(DEBUG, "nonblock_renew success", K(pkey));
      }
    }
    if (OB_FAIL(location_cache_->nonblock_get_leader_by_election_without_renew(pkey, leader, cluster_id))) {
      STORAGE_LOG(DEBUG, "nonblock_get_leader_by_election_without_renew failed", K(pkey), K(ret));
    } else {
      STORAGE_LOG(DEBUG, "nonblock_get_leader_by_election_without_renew success", K(pkey), K(ret));
    }
  }
  return ret;
}

// used to obtain restore_leader from the location cache during recovery
int ObPartitionService::get_restore_leader_from_loc_cache(
    const common::ObPartitionKey& pkey, ObAddr& restore_leader, const bool is_need_renew)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else {
    if (is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret));
      } else {
        STORAGE_LOG(DEBUG, "nonblock_renew success", K(pkey));
      }
    }
    if (OB_FAIL(location_cache_->nonblock_get_restore_leader(pkey, restore_leader))) {
      STORAGE_LOG(DEBUG, "nonblock_get_restore_leader failed", K(pkey), K(ret));
    } else {
      STORAGE_LOG(DEBUG, "nonblock_get_restore_leader success", K(pkey), K(ret));
    }
    if (OB_FAIL(ret) && !is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret));
      } else {
        STORAGE_LOG(DEBUG, "nonblock_renew success", K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionService::nonblock_get_leader_by_election_from_loc_cache(
    const common::ObPartitionKey& pkey, ObAddr& leader, const bool is_need_renew)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else {
    if (is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret));
      } else {
        STORAGE_LOG(DEBUG, "nonblock_renew success", K(pkey));
      }
    }
    if (OB_FAIL(location_cache_->nonblock_get_leader_by_election(pkey, leader))) {
      STORAGE_LOG(DEBUG, "nonblock_get_leader_by_election failed", K(pkey), K(ret));
    } else {
      STORAGE_LOG(DEBUG, "nonblock_get_leader_by_election success", K(pkey), K(ret));
    }
    if (OB_FAIL(ret) && !is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret));
      } else {
        STORAGE_LOG(DEBUG, "nonblock_renew success", K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionService::nonblock_get_strong_leader_from_loc_cache(
    const common::ObPartitionKey& pkey, ObAddr& leader, const bool is_need_renew)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else {
    if (is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret));
      } else {
        STORAGE_LOG(DEBUG, "nonblock_renew success", K(pkey));
      }
    }
    if (OB_FAIL(location_cache_->nonblock_get_strong_leader(pkey, leader))) {
      STORAGE_LOG(DEBUG, "nonblock_get_strong_leader failed", K(pkey), K(ret));
    } else {
      STORAGE_LOG(DEBUG, "nonblock_get_strong_leader success", K(pkey), K(ret));
    }
    if (OB_FAIL(ret) && !is_need_renew) {
      int tmp_ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, expire_renew_time))) {
        STORAGE_LOG(WARN, "nonblock_renew failed", K(pkey), K(tmp_ret));
      } else {
        STORAGE_LOG(DEBUG, "nonblock_renew success", K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionService::check_partition_exist(const common::ObPartitionKey& pkey, bool& exist)
{
  int ret = OB_SUCCESS;
  bool check_dropped_partition = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(schema_service_->check_partition_exist(
                 pkey.get_table_id(), pkey.get_partition_id(), check_dropped_partition, exist))) {
    STORAGE_LOG(WARN, "check_table_exist failed", K(ret), K(pkey), K(exist));
  } else if (!exist) {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "failed to get partition", K(ret), K(pkey));
    } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
    } else if (partition->is_split_source_partition()) {
      exist = true;
    }
  }
  return ret;
}

int ObPartitionService::gen_rebuild_arg_(
    const common::ObPartitionKey& pkey, const common::ObReplicaType replica_type, obrpc::ObRebuildReplicaArg& arg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!pkey.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(replica_type));
  } else {
    ObReplicaMember dst(self_addr_, OB_INVALID_TIMESTAMP);
    dst.set_replica_type(replica_type);
    arg.key_ = pkey;
    arg.dst_ = dst;
    arg.src_ = dst;
    arg.priority_ = ObReplicaOpPriority::PRIO_HIGH;
    arg.switch_epoch_ = GCTX.get_switch_epoch2();
  }

  return ret;
}

int ObPartitionService::gen_standby_restore_arg_(const common::ObPartitionKey& pkey,
    const common::ObReplicaType replica_type, const common::ObAddr& src_server, obrpc::ObRebuildReplicaArg& arg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!pkey.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(replica_type));
  } else {
    ObReplicaMember dst(self_addr_, OB_INVALID_TIMESTAMP);
    ObReplicaMember src(src_server, OB_INVALID_TIMESTAMP);
    dst.set_replica_type(replica_type);
    arg.key_ = pkey;
    arg.dst_ = dst;
    arg.src_ = src;
    arg.priority_ = ObReplicaOpPriority::PRIO_HIGH;
    arg.switch_epoch_ = GCTX.get_switch_epoch2();
  }

  return ret;
}

int ObPartitionService::restore_standby_replica(
    const common::ObPartitionKey& pkey, const common::ObAddr& server, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(server), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else {
    share::ObTaskId dummy_id;
    dummy_id.init(self_addr_);
    obrpc::ObRebuildReplicaArg arg;
    if (OB_FAIL(gen_standby_restore_arg_(pkey, partition->get_replica_type(), server, arg))) {
      STORAGE_LOG(WARN, "gen_standby_restore_arg_ failed", K(pkey), K(ret));
    } else if (OB_FAIL(schedule_standby_restore_task(arg, cluster_id, dummy_id))) {
      STORAGE_LOG(WARN, "schedule_standby_restore_task failed", K(pkey), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

/* this is a callback from partition log service, which is used to handle some certain
 * situation when a replica fetch logs from the leader, however the leader doesn't have
 * the logs the replica wants. this function can only be invoked by the leader.
 */
int ObPartitionService::handle_log_missing(const common::ObPartitionKey& pkey, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(server), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else {
    share::ObTaskId dummy_id;
    dummy_id.init(self_addr_);
    obrpc::ObRebuildReplicaArg arg;
    if (OB_FAIL(gen_rebuild_arg_(pkey, partition->get_replica_type(), arg))) {
      STORAGE_LOG(WARN, "gen_rebuild_arg_ failed", K(pkey), K(ret));
    } else if (OB_FAIL(rebuild_replica(arg, dummy_id))) {
      STORAGE_LOG(WARN, "schedule rebuild sys table replica failed", K(pkey), K(ret));
    } else if (OB_FAIL(rs_cb_->report_rebuild_replica_async(pkey, server, OB_REBUILD_ON))) {
      STORAGE_LOG(WARN, "report rebuild replica failed", K(ret), K(pkey), K(server));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartitionService::process_migrate_retry_task(const ObMigrateRetryTask& task)
{
  int ret = OB_SUCCESS;
  const common::ObPartitionKey pkey = task.pkey_;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionLogService* pls = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (NULL == (pls = partition->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get_log_service failed", K(pkey), K(ret));
  } else {
    ObReplicaType tmp_replica_type = ObReplicaType::REPLICA_TYPE_MAX;
    if (partition->is_need_gc()) {
      STORAGE_LOG(INFO, "this partition need gc, cannot retry rebuild or restore", K(pkey));
    } else if (RETRY_REBUILD == task.task_type_) {
      // rebuild retry task
      // pls is offline && need_rebuild
      tmp_replica_type = partition->get_replica_type();
      share::ObTaskId dummy_id;
      dummy_id.init(self_addr_);
      obrpc::ObRebuildReplicaArg arg;
      if (OB_SUCCESS != (ret = gen_rebuild_arg_(pkey, tmp_replica_type, arg))) {
        STORAGE_LOG(WARN, "gen_rebuild_arg_ failed", K(ret), K(pkey));
      } else if (OB_SUCCESS != (ret = rebuild_replica(arg, dummy_id))) {
        STORAGE_LOG(WARN, "schedule rebuild replica failed", K(ret), K(pkey));
      } else if (OB_SUCCESS != (ret = rs_cb_->report_rebuild_replica_async(pkey, self_addr_, OB_REBUILD_ON))) {
        // the meta table still needs to be updated when rebuild is triggered
        // because RS needs to skip the replica of rebuild when doing major freeze
        STORAGE_LOG(WARN, "report rebuild replica failed", K(ret), K(pkey), K(self_addr_));
      } else {
        // clean retry flag
        (void)partition->reset_migrate_retry_flag();
        STORAGE_LOG(INFO, "schedule rebuild replica success", K(pkey));
      }
    } else if (RETRY_STANDBY_RESTORE == task.task_type_) {
      bool need_exec_restore = true;
      const ObReplicaType replica_type = partition->get_replica_type();
      const bool is_paxos_replica = ObReplicaTypeCheck::is_paxos_replica(replica_type);
      if (!pls->is_standby_restore_state()) {
        need_exec_restore = false;
      } else {
        bool is_valid_member = false;
        const int64_t fake_cluster_id = OB_INVALID_CLUSTER_ID;
        if (is_paxos_replica) {
          obrpc::ObQueryIsValidMemberRequest request;
          obrpc::ObQueryIsValidMemberResponse response;
          const int64_t TIMEOUT = 1000 * 1000;  // 1s
          request.self_addr_ = self_addr_;
          // query leader before triggering restore
          // only the replicas in the leader's member list need to execute restore
          common::ObAddr leader;
          if (OB_FAIL(request.partition_array_.push_back(pkey))) {
            STORAGE_LOG(WARN, "request partition_array push_back failed", K(ret), K(pkey));
          } else if (OB_FAIL(pls->get_leader(leader))) {
            STORAGE_LOG(WARN, "get_leader failed", K(ret), K(pkey));
          } else if (!leader.is_valid()) {
            ret = OB_EAGAIN;
            if (REACH_TIME_INTERVAL(100 * 1000)) {
              STORAGE_LOG(INFO, "leader is invalid, need retry", K(ret), K(pkey));
            }
          } else if (OB_FAIL(
                         GCTX.srv_rpc_proxy_->to(leader).timeout(TIMEOUT).query_is_valid_member(request, response)) ||
                     (OB_SUCCESS != (ret = response.ret_value_))) {
            STORAGE_LOG(WARN, "rpc query_is_valid_member failed", K(ret), K(leader), K(pkey));
          } else if (response.candidates_status_.count() < 1) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "query_is_valid_member return value unexpected", K(ret), K(pkey));
          } else {
            is_valid_member = response.candidates_status_[0];
          }
        }

        if (OB_SUCC(ret)) {
          if (!is_paxos_replica || is_valid_member) {
            // only non-paxos replica or replica in leader's member list can trigger restore
            if (OB_FAIL(restore_standby_replica(pkey, self_addr_, fake_cluster_id))) {
              STORAGE_LOG(WARN, "retry restore_standby_replica failed", K(pkey), K(ret));
            } else {
              // clean flag after restore triggered
              (void)partition->reset_migrate_retry_flag();
            }
          } else {
            need_exec_restore = false;
            STORAGE_LOG(INFO,
                "this replica cannot trigger standby_restore",
                K(pkey),
                K(ret),
                K(replica_type),
                K(is_valid_member));
          }
        }
      }
      if (!need_exec_restore) {
        // no need to restore, reset flag
        (void)partition->reset_migrate_retry_flag();
      }
    } else {
      // do nothing
    }

    if (OB_FAIL(ret)) {
      // push retry task in queue
      if (OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT == ret) {
        usleep(100 * 1000L);
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = push_into_migrate_retry_queue(task.pkey_, task.task_type_))) {
        STORAGE_LOG(WARN, "push_into_migrate_retry_queue failed", K(task), K(ret), K(tmp_ret));
      }
    }
  }

  if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    STORAGE_LOG(INFO, "process_migrate_retry_task finished", K(ret), K(task));
  }

  return ret;
}

int ObPartitionService::check_has_need_offline_replica(
    const obrpc::ObTenantSchemaVersions& arg, obrpc::ObGetPartitionCountResult& result)
{
  int ret = OB_SUCCESS;
  result.partition_count_ = 0;
  storage::ObIPartitionGroupIterator* partition_iter = NULL;
  ObSchemaGetterGuard schema_guard;
  hash::ObHashMap<uint64_t, int64_t> tenant_schema_versions;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(partition_iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "alloc_pg_iter failed", KR(ret));
  } else if (0 >= arg.tenant_schema_versions_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get tenant schema", KR(ret), K(arg));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    STORAGE_LOG(WARN, "fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    STORAGE_LOG(WARN, "schema_guard is not formal", KR(ret));
  } else if (OB_FAIL(tenant_schema_versions.create(
                 hash::cal_next_prime(arg.tenant_schema_versions_.count()), "CheckOffline", "CheckOffline"))) {
    LOG_WARN("failed to create hashmap", KR(ret), K(arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tenant_schema_versions_.count(); ++i) {
      if (OB_FAIL(tenant_schema_versions.set_refactored(
              arg.tenant_schema_versions_.at(i).tenant_id_, arg.tenant_schema_versions_.at(i).schema_version_))) {
        LOG_WARN("failed to set refactored", KR(ret), K(i), K(arg));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const bool check_delay_dropped_schema = true;
    ObPartitionKey partition_key;
    uint64_t fetch_tenant_id = OB_INVALID_TENANT_ID;
    bool is_exist = false;
    // the reason for removing the checking of create_schema_version is:
    // the procedure where create_schema_version is obtained needs to be locked
    // which may cause a lock conflict and cause this checking to fail
    // moreover, it is a check that occupies the rs's DDL thread
    // which can ensure that the latest schema has been obtained
    // int64_t create_schema_version = INT64_MAX;
    int64_t local_schema_version = OB_INVALID_VERSION;
    int64_t newest_schema_version = OB_INVALID_VERSION;
    while (OB_SUCC(ret)) {
      storage::ObIPartitionGroup* partition = NULL;
      if (OB_FAIL(partition_iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "partition_iter->get_next failed", KR(ret));
        } else {
          // do nothing
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected error, partition is NULL", KR(ret));
      } else if (FALSE_IT(partition_key = partition->get_partition_key())) {
      } else {
        fetch_tenant_id =
            is_inner_table(partition_key.get_table_id()) ? OB_SYS_TENANT_ID : partition_key.get_tenant_id();
      }

      if (OB_FAIL(ret)) {
      } else if (partition->check_pg_partition_offline(partition_key)) {
      } else if (OB_FAIL(schema_guard.get_schema_version(fetch_tenant_id, local_schema_version))) {
        STORAGE_LOG(WARN, "fail to get schema guard version", KR(ret), K(partition_key), K(fetch_tenant_id));
      } else if (OB_FAIL(tenant_schema_versions.get_refactored(fetch_tenant_id, newest_schema_version))) {
        STORAGE_LOG(WARN,
            "tenant does not exist in newest schema, tenant may be dropped",
            KR(ret),
            K(fetch_tenant_id),
            K(partition_key),
            K(local_schema_version),
            K(arg));
      } else if (newest_schema_version > local_schema_version ||
                 !share::schema::ObSchemaService::is_formal_version(local_schema_version)) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO,
            "new pg partition, schema is not flushed, skip it",
            K(partition_key),
            K(fetch_tenant_id),
            K(local_schema_version),
            K(newest_schema_version));
      } else if (OB_FAIL(schema_guard.check_partition_exist(partition_key.get_table_id(),
                     partition_key.get_partition_id(),
                     check_delay_dropped_schema,
                     is_exist))) {
        STORAGE_LOG(WARN, "fail to check partition exist", KR(ret), K(partition_key));
      } else if (!is_exist) {
        result.partition_count_ = 1;
        LOG_WARN("partition not exist in schema, need offline", KR(ret), K(partition_key));
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (NULL != partition_iter) {
    revert_pg_iter(partition_iter);
  }
  return ret;
}

int ObPartitionService::remove_partition_from_pg(
    const bool for_replay, const ObPartitionKey& pg_key, const ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  bool can_replay = true;
  const bool write_slog_trans = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartititonService not init", K(ret));
  } else if (!pg_key.is_valid() || !pkey.is_valid() || !pg_key.is_pg()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(pkey));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running.", K(ret), K(is_running_));
  } else if (OB_FAIL(get_partition(pg_key, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(pg_key), K(pkey));
  } else if (OB_UNLIKELY(NULL == guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pg_key), K(pkey));
    // filter out clog corresponded to already replayed slog
  } else if (for_replay &&
             OB_FAIL(guard.get_partition_group()->get_pg_storage().check_can_replay_remove_partition_from_pg_log(
                 pkey, log_id, can_replay))) {
    STORAGE_LOG(WARN, "check can replay remove partition from pg log error", K(ret), K(pg_key), K(pkey));
  } else if (!can_replay) {
    STORAGE_LOG(INFO, "no need to replay current clog, maybe restart scenario", K(for_replay), K(pg_key), K(pkey));
  } else if (OB_FAIL(
                 guard.get_partition_group()->remove_partition_from_pg(for_replay, pkey, write_slog_trans, log_id))) {
    STORAGE_LOG(WARN, "remove partition from pg error", K(ret), K(pg_key), K(pkey));
  } else {
    try_inc_total_partition_cnt(-1, false /*need check*/);
    // remove pg partition from all_tenant_pg_meta_table
    const bool need_report_checksum = false;
    if (OB_SUCCESS != (tmp_ret = rs_cb_->submit_pt_update_task(pkey, need_report_checksum))) {
      STORAGE_LOG(WARN, "notify pg partition table update failed", K(pkey), K(tmp_ret));
    }
  }
  STORAGE_LOG(INFO, "remove partition from pg", K(ret), K(can_replay), K(pg_key), K(pkey));

  return ret;
}

// called by replay offline partition log
int ObPartitionService::schema_drop_partition(const ObCLogCallbackAsyncTask& offline_task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const ObPartitionKey& pkey = offline_task.pg_key_;
  const uint64_t log_id = offline_task.log_id_;
  const bool is_physical_drop = offline_task.is_physical_drop_;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartititonService not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_SUCCESS != (ret = get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_FAIL(partition->schema_drop(false, log_id, is_physical_drop))) {
    STORAGE_LOG(WARN, "fail to schema drop partition", K(ret), K(pkey));
  }

  if (OB_EAGAIN == ret) {
    if (OB_SUCCESS != (tmp_ret = cb_async_worker_.push_task(offline_task))) {
      STORAGE_LOG(ERROR, "fail to push offline partition task", K(tmp_ret), K(pkey));
    }
  }

  return ret;
}

int ObPartitionService::replay_offline_partition_log(
    const ObPartitionKey& pkey, const uint64_t log_id, const bool is_physical_drop)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObPartititonService not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_FAIL(partition->schema_drop(true, log_id, is_physical_drop))) {
    STORAGE_LOG(WARN, "fail to schema drop partition", K(ret), K(pkey));
  }

  return ret;
}

int ObPartitionService::activate_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "partition not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(tenant_id), K(ret));
  } else {
    STORAGE_LOG(INFO, "activate tenant", K(tenant_id));
    int64_t word_offset = (int64_t)(tenant_id >> BITSETWORD_SHIFT_NUM);
    int64_t bit_offset = (int64_t)(tenant_id & BITSETWORD_OFFSET_MASK);
    if (word_offset >= BITSET_WORDS_NUM) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid tenant id", K(ret), K(tenant_id));
    } else {
      BITSET_TYPE* target_ptr = tenant_bit_set_ + word_offset;
      while (true) {
        BITSET_TYPE target_word = ATOMIC_LOAD(target_ptr);
        if (ATOMIC_BCAS(target_ptr, target_word, (target_word) | (0x01 << bit_offset))) {
          break;
        }
      }
      (void)OB_TS_MGR.is_external_consistent(tenant_id);
    }
  }
  return ret;
}

int ObPartitionService::inactivate_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "partition not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(txs_->inactive_tenant(tenant_id))) {
    TRANS_LOG(WARN, "transaction service inactive tenant error", K(ret), K(tenant_id));
  } else {
    STORAGE_LOG(INFO, "inactivate tenant", K(tenant_id));
    int64_t word_offset = (int64_t)(tenant_id >> BITSETWORD_SHIFT_NUM);
    int64_t bit_offset = (int64_t)(tenant_id & BITSETWORD_OFFSET_MASK);
    if (word_offset >= BITSET_WORDS_NUM) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid tenant id", K(ret), K(tenant_id));
    } else {
      BITSET_TYPE* target_ptr = tenant_bit_set_ + word_offset;
      while (true) {
        BITSET_TYPE target_word = ATOMIC_LOAD(target_ptr);
        if (ATOMIC_BCAS(target_ptr, target_word, (target_word) & ~(0x01 << bit_offset))) {
          break;
        }
      }
    }
  }
  return ret;
}

bool ObPartitionService::is_tenant_active_(const uint64_t tenant_id) const
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;

  if (0 == tenant_id) {
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(tenant_id), K(tmp_ret));
  } else {
    int64_t word_offset = (int64_t)(tenant_id >> BITSETWORD_SHIFT_NUM);
    int64_t bit_offset = (int64_t)(tenant_id & BITSETWORD_OFFSET_MASK);
    if (word_offset >= BITSET_WORDS_NUM) {
      tmp_ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid tenant id", K(tmp_ret), K(tenant_id));
    } else {
      BITSET_TYPE target_word = ATOMIC_LOAD(tenant_bit_set_ + word_offset);
      target_word &= 0x01 << bit_offset;
      if (0 != target_word) {
        bool_ret = true;
      }
    }
  }

  return bool_ret;
}

bool ObPartitionService::is_tenant_active(const uint64_t tenant_id) const
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    tmp_ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "partition not init", K(tmp_ret));
  } else if (0 == tenant_id) {
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(tenant_id), K(tmp_ret));
  } else {
    bool_ret = is_tenant_active_(tenant_id);
  }
  return bool_ret;
}

bool ObPartitionService::is_tenant_active(const common::ObPartitionKey& pkey) const
{
  bool bool_ret = false;
  int err = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    err = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "partition not init", K(err));
  } else if (!pkey.is_valid()) {
    err = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(err));
  } else {
    uint64_t tenant_id = extract_tenant_id(pkey.get_table_id());
    bool_ret = is_tenant_active_(tenant_id);
  }
  return bool_ret;
}

int ObPartitionService::get_server_locality_array(
    ObIArray<ObServerLocality>& server_locality_array, bool& has_readonly_zone) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(locality_manager_.get_server_locality_array(server_locality_array, has_readonly_zone))) {
    LOG_WARN("fail to get server locality array", K(ret));
  }
  return ret;
}

int ObPartitionService::get_server_region_across_cluster(const common::ObAddr& server, common::ObRegion& region) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(locality_manager_.get_server_region_across_cluster(server, region))) {
    LOG_WARN("fail to get server region", K(ret), K(server));
  }
  return ret;
}

int ObPartitionService::get_server_region(const common::ObAddr& server, common::ObRegion& region) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(locality_manager_.get_server_region(server, region))) {
    LOG_WARN("fail to get server region", K(ret), K(server));
  }
  return ret;
}

int ObPartitionService::record_server_region(const common::ObAddr& server, const common::ObRegion& region)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(region));
  } else if (OB_FAIL(locality_manager_.record_server_region(server, region))) {
    LOG_WARN("fail to record server region", K(ret), K(server), K(region));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_server_idc(const common::ObAddr& server, common::ObIDC& idc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(locality_manager_.get_server_idc(server, idc))) {
    LOG_WARN("fail to get server idc", K(ret), K(server));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::record_server_idc(const common::ObAddr& server, const common::ObIDC& idc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || idc.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(idc));
  } else if (OB_FAIL(locality_manager_.record_server_idc(server, idc))) {
    LOG_WARN("fail to record server idc", K(ret), K(server), K(idc));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_server_cluster_id(const common::ObAddr& server, int64_t& cluster_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(locality_manager_.get_server_cluster_id(server, cluster_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get server cluster_id", K(ret), K(server));
    }
  }
  return ret;
}

int ObPartitionService::record_server_cluster_id(const common::ObAddr& server, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !is_valid_cluster_id(cluster_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(cluster_id));
  } else if (OB_FAIL(locality_manager_.record_server_cluster_id(server, cluster_id))) {
    LOG_WARN("fail to record server cluster_id", K(ret), K(server), K(cluster_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::force_refresh_locality_info()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(locality_manager_.load_region())) {
    STORAGE_LOG(WARN, "fail to load locality", K(ret));
  } else if (OB_FAIL(enable_backup_white_list())) {
    STORAGE_LOG(WARN, "fail to load locality", K(ret));
  } else {
    STORAGE_LOG(INFO, "force to load locality", K(ret));
  }
  return ret;
}

int ObPartitionService::add_refresh_locality_task()
{
  int ret = OB_SUCCESS;
  ObRefreshLocalityTask task(this);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(refresh_locality_task_queue_.add_task(task))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "add refresh locality task failed", K(ret));
    }
  }
  return ret;
}

void ObPartitionService::init_tenant_bit_set()
{
  for (int64_t i = 0; i < BITSET_WORDS_NUM; ++i) {
    tenant_bit_set_[i] = 0x0;
  }
}

obrpc::ObCommonRpcProxy& ObPartitionService::get_rs_rpc_proxy()
{
  return *rs_rpc_proxy_;
}

/********************************************************************
 * transaction
 *******************************************************************
 */
// in the index builder process, the clog of schema version changes needs to be persisted
int ObPartitionService::write_partition_schema_version_change_clog_(const common::ObPGKey& pg_key,
    const common::ObPartitionKey& pkey, const int64_t schema_version, const uint64_t index_id, const int64_t timeout,
    uint64_t& log_id, int64_t& log_ts)
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_PG_PARTITION_CHANGE_LOG_US = 1000;
  ObSchemaChangeClogCb* cb = NULL;

  if (!pg_key.is_valid() || !pkey.is_valid() || schema_version < 0 || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(pkey), K(schema_version), K(timeout));
  } else {
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(get_partition(pg_key, guard)) || OB_ISNULL(guard.get_partition_group())) {
      STORAGE_LOG(WARN, "get partition error", K(ret), K(pg_key));
    } else if (OB_FAIL(guard.get_partition_group()->submit_partition_schema_change_log(
                   pkey, schema_version, index_id, this, log_id, log_ts, cb))) {
      STORAGE_LOG(
          WARN, "submit partition schema version change log error", K(ret), K(pg_key), K(pkey), K(schema_version));
    } else if (log_ts <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected log ts", K(ret), K(log_ts));
    } else {
      // do nothing
    }
    // check whether the clogs of all partitions are successfully written, timeout in 10s
    int64_t succ_cnt = 0;
    int64_t begin_us = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && is_running_ && succ_cnt == 0) {
      int64_t loop_start_us = ObTimeUtility::current_time();
      if (CB_SUCCESS == cb->get_write_clog_state()) {
        succ_cnt++;
      } else if (CB_FAIL == cb->get_write_clog_state()) {
        ret = OB_ERR_SYS;
      } else {
        // do nothing
      }
      // wait until all clog sync success
      if (OB_SUCC(ret) && succ_cnt == 0) {
        int64_t loop_end_us = ObTimeUtility::current_time();
        if (loop_end_us - begin_us > timeout) {
          ret = OB_TIMEOUT;
          STORAGE_LOG(
              WARN, "write partition schema version change log timeout", K(ret), K(pg_key), K(pkey), K(succ_cnt));
        } else if (loop_end_us - loop_start_us < CHECK_PG_PARTITION_CHANGE_LOG_US) {
          usleep((int)(CHECK_PG_PARTITION_CHANGE_LOG_US - (loop_end_us - loop_start_us)));
        } else {
          PAUSE();
        }
      }
    }
    // release completed callback, mark others as CB_END
    int64_t tmp_ret = OB_SUCCESS;
    bool can_release = false;
    if (NULL != cb) {
      if (OB_SUCCESS != (tmp_ret = cb->check_can_release(can_release))) {
        STORAGE_LOG(WARN, "check can release error", K(tmp_ret), KP(cb));
      } else if (can_release) {
        op_free(cb);
        cb = NULL;
        STORAGE_LOG(INFO, "release current cb success", K(pg_key), K(pkey), K(schema_version));
      } else {
        STORAGE_LOG(INFO, "current cb can not be release right now", K(pg_key), K(pkey), K(schema_version));
      }
    }
  }
  return ret;
}
int ObPartitionService::check_schema_version_elapsed(const ObPartitionKey& target_pkey, const int64_t schema_version,
    const uint64_t index_id, int64_t& max_commit_version)
{
  int ret = common::OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  int64_t latest_schema_version = 0;

  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running", K(ret));
  } else if (OB_UNLIKELY(!target_pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(target_pkey));
  } else if (OB_FAIL(get_partition(target_pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(target_pkey));
  } else if (OB_UNLIKELY(NULL == guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(target_pkey));
  } else {
    // 1. check whether observer has been refreshed to the target_pkey schema
    ObPGPartitionGuard pg_partition_guard;
    ObPGPartition* pg_partition = nullptr;
    const ObPartitionKey& pg_key = guard.get_partition_group()->get_partition_key();
    int64_t refreshed_schema_version = 0;
    int64_t refreshed_schema_ts = 0;
    uint64_t log_id = 0;
    int64_t log_ts = 0;
    if (OB_FAIL(guard.get_partition_group()->get_pg_partition(target_pkey, pg_partition_guard))) {
      LOG_WARN("failed to get pg partition", K(target_pkey), K(ret));
    } else if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg partition is null", K(ret), K(target_pkey));
    } else if (OB_FAIL(pg_partition->get_refreshed_schema_info(
                   refreshed_schema_version, refreshed_schema_ts, log_id, log_ts))) {
      LOG_WARN("get refreshed schema info error", K(ret), K(target_pkey));
      // 1.1 check whether the partition has recorded the successful refresh
    } else if (refreshed_schema_version >= schema_version) {
      if (log_ts <= 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
            "unexpected log ts",
            K(ret),
            K(pg_key),
            K(target_pkey),
            K(refreshed_schema_version),
            K(refreshed_schema_ts),
            K(log_ts));
      }
    } else if (OB_FAIL(guard.get_partition_group()->get_latest_schema_version(
                   schema_service_, pg_key, latest_schema_version))) {
      STORAGE_LOG(WARN, "get latest schema version error", K(ret), K(target_pkey), K(pg_key), K(schema_version));
      // 1.2 check whether the tenant has been refreshed to the table's schema
      // if it has been refreshed, record it on the partition
    } else if (latest_schema_version < schema_version) {
      ret = OB_EAGAIN;
      STORAGE_LOG(WARN,
          "current schema version not latest, need retry",
          K(ret),
          K(target_pkey),
          K(pg_key),
          K(latest_schema_version),
          K(schema_version));
    } else {
      const int64_t timeout = 10 * 1000 * 1000;
      if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_partition_schema_version_change_log(
              schema_version))) {
        LOG_WARN("failed to set partition max schema version", K(ret), K(pg_key), K(target_pkey), K(schema_version));
      } else if (0 < index_id && OB_FAIL(guard.get_partition_group()->get_pg_storage().create_index_table_store(
                                     target_pkey, index_id, schema_version))) {
        LOG_WARN("failed to create index table store", K(ret), K(target_pkey), K(index_id));
      } else if (OB_FAIL(write_partition_schema_version_change_clog_(
                     pg_key, target_pkey, schema_version, index_id, timeout, log_id, log_ts))) {
        STORAGE_LOG(WARN,
            "write partition schema version change clog error",
            K(ret),
            K(pg_key),
            K(target_pkey),
            K(schema_version));
        // override ret
        ret = OB_EAGAIN;
      } else if (OB_FAIL(pg_partition->update_build_index_schema_info(
                     schema_version, log_id, log_ts, refreshed_schema_ts))) {
        STORAGE_LOG(WARN, "update build index schema info error", K(ret), K(target_pkey), K(pg_key), K(schema_version));
      } else {
        STORAGE_LOG(INFO,
            "update build index schema info success",
            K(target_pkey),
            K(pg_key),
            K(schema_version),
            K(refreshed_schema_ts),
            K(log_id),
            K(log_ts));
      }
    }
    // 2. check whether the transaction before refreshed_schema_ts is over
    if (OB_SUCC(ret)) {
      int64_t tmp_max_commit_version = 0;
      if (OB_FAIL(txs_->check_schema_version_elapsed(
              pg_key, schema_version, refreshed_schema_ts, tmp_max_commit_version))) {
        if (OB_EAGAIN != ret) {
          STORAGE_LOG(WARN, "check schema version error", K(ret), K(target_pkey), K(pg_key), K(schema_version));
        }
      } else {
        max_commit_version = max(tmp_max_commit_version, log_ts - 1);
        STORAGE_LOG(INFO,
            "check target schema version transaction elapsed success",
            K(target_pkey),
            K(pg_key),
            K(schema_version),
            K(refreshed_schema_ts));
      }
    }
  }

  return ret;
}

int ObPartitionService::check_ctx_create_timestamp_elapsed(const ObPartitionKey& target_pkey, const int64_t ts)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;

  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running", K(ret));
  } else if (!target_pkey.is_valid() || 0 >= ts) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(target_pkey), K(ts));
  } else if (OB_FAIL(pg_index_.get_pg_key(target_pkey, pkey))) {
    STORAGE_LOG(WARN, "get pg key", K(ret), K(pkey), K(pkey));
  } else if (OB_FAIL(txs_->check_ctx_create_timestamp_elapsed(pkey, ts))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "check ctx create timestamp failed", K(ret), K(pkey), K(ts));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::start_trans(const uint64_t tenant_id, const uint64_t cluster_id,
    const transaction::ObStartTransParam& req, const int64_t expired_time, const uint32_t session_id,
    const uint64_t proxy_session_id, transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running", K(ret));
  } else if (OB_FAIL(txs_->start_trans(
                 tenant_id, cluster_id, req, expired_time, session_id, proxy_session_id, trans_desc))) {
    STORAGE_LOG(WARN, "start transaction failed", K(tenant_id), K(cluster_id), K(req), K(expired_time), K(ret));
  } else {
    // STORAGE_LOG(DEBUG, "start transaction success", K(tenant_id), K(cluster_id),
    //            K(req), K(expired_time), K(ret));
  }
  return ret;
}

// caution: Trigger callback if an error occurred before the end_trans of trx_ was called
// This procedure cannot determine whether ObSharedEndTransCallback should be disconnected.
// If you need to determine it, write additional logic to determine whether to disconnect.
int ObPartitionService::end_trans(bool is_rollback, transaction::ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb,
    const int64_t stmt_expired_time)
{
  int ret = common::OB_SUCCESS;
  bool need_cb = true;
  sql::ObExclusiveEndTransCallback* exclusive_cb = NULL;
#ifdef TRANS_MODULE_TEST
  {
    const int64_t tenant_id = trans_desc.get_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    const bool enable_fake_commit = tenant_config->module_test_trx_fake_commit;
    if (OB_UNLIKELY(enable_fake_commit) && !trans_desc.is_inner_trans()) {
      is_rollback = true;
    }
  }
#endif
  if (!cb.is_shared()) {
    exclusive_cb = static_cast<sql::ObExclusiveEndTransCallback*>(&cb);
    exclusive_cb->set_is_txs_end_trans_called(false);
  } else {
    // non-shared callbacks are stateless, do nothing
  }
  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else {
    need_cb = false;
    ret = txs_->end_trans(is_rollback, trans_desc, cb, stmt_expired_time);
  }

  if (need_cb) {
    if (NULL != exclusive_cb) {
      if (OB_UNLIKELY(exclusive_cb->is_txs_end_trans_called())) {
        LOG_ERROR("txs end trans is called, but callback here", K(ret));
      }
    }
    cb.callback(ret);
  } else {
    if (NULL != exclusive_cb) {
      if (OB_UNLIKELY(!exclusive_cb->is_txs_end_trans_called())) {
        LOG_ERROR("txs end trans is not called, but do not callback here", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionService::savepoint(ObTransDesc& trans_desc, const ObString& sp_name)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(txs_->savepoint(trans_desc, sp_name))) {
    LOG_WARN("transaction create savepoint error", K(ret), K(trans_desc), K(sp_name));
  } else {
    LOG_DEBUG("create savepoint", K(trans_desc), K(sp_name));
  }
  return ret;
}

int ObPartitionService::rollback_savepoint(
    ObTransDesc& trans_desc, const ObString& sp_name, const ObStmtParam& stmt_param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(txs_->rollback_savepoint(trans_desc, sp_name, stmt_param))) {
    LOG_WARN("transaction rollback savepoint error", K(ret), K(trans_desc), K(sp_name));
  } else {
    LOG_DEBUG("create savepoint", K(trans_desc), K(sp_name));
  }

  return ret;
}

int ObPartitionService::release_savepoint(ObTransDesc& trans_desc, const ObString& sp_name)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(txs_->release_savepoint(trans_desc, sp_name))) {
    LOG_WARN("transaction rollback savepoint error", K(ret), K(sp_name));
  } else {
    LOG_DEBUG("create savepoint", K(trans_desc), K(sp_name));
  }

  return ret;
}

int ObPartitionService::mark_trans_forbidden_sql_no(
    const ObTransID& trans_id, const ObPartitionArray& partitions, const int64_t sql_no, bool& forbid_succ)
{
  int ret = OB_SUCCESS;
  forbid_succ = true;
  for (int64_t i = 0; OB_SUCC(ret) && forbid_succ && i < partitions.count(); i++) {
    if (OB_FAIL(txs_->mark_trans_forbidden_sql_no(trans_id, partitions.at(i), sql_no, forbid_succ))) {
      LOG_WARN("mark trans forbidden error", K(ret), K(trans_id), K(partitions.at(i)), K(sql_no));
    } else {
      LOG_DEBUG("mark trans forbidden", K(trans_id), K(partitions.at(i)), K(sql_no));
    }
  }
  return ret;
}

int ObPartitionService::is_trans_forbidden_sql_no(
    const ObTransID& trans_id, const ObPartitionArray& partitions, const int64_t sql_no, bool& is_forbidden)
{
  int ret = OB_SUCCESS;
  is_forbidden = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_forbidden && i < partitions.count(); i++) {
    if (OB_FAIL(txs_->is_trans_forbidden_sql_no(trans_id, partitions.at(i), sql_no, is_forbidden))) {
      LOG_WARN("get trans forbidden error", K(ret), K(trans_id), K(partitions.at(i)), K(sql_no));
    } else {
      LOG_DEBUG("get trans forbidden sql no", K(trans_id), K(partitions.at(i)), K(sql_no));
    }
  }
  return ret;
}

int ObPartitionService::xa_rollback_all_changes(ObTransDesc& trans_desc, const ObStmtParam& stmt_param)
{
  int ret = OB_SUCCESS;
  if (!trans_desc.is_valid() || !trans_desc.is_xa_local_trans()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(trans_desc));
  } else if (OB_FAIL(txs_->xa_rollback_all_changes(trans_desc, stmt_param))) {
    LOG_WARN("xa rollback all changes error", K(ret), K(trans_desc));
  }
  return ret;
}

int ObPartitionService::get_pg_key(const ObPartitionKey& pkey, ObPGKey& pg_key) const
{
  return get_pg_key_(pkey, pg_key);
}

int ObPartitionService::get_pg_key_(const ObPartitionKey& pkey, ObPGKey& pg_key) const
{
  int ret = OB_SUCCESS;
  if (pkey.is_trans_table()) {
    // trans_table_id is generated from pg key by setting 38th and 39th bit to 1
    // so two possibles for pg key table_id, 0 for standalone partition, 1 for binding pg
    const uint64_t table_id = pkey.get_table_id() & ~OB_TRANS_TABLE_MASK;
    const int64_t partition_id = pkey.get_partition_id();
    const int64_t partition_cnt = pkey.get_partition_cnt();
    const ObPartitionKey tmp_key1(table_id, partition_id, partition_cnt);
    if (OB_FAIL(get_pg_key_from_index_schema_(tmp_key1, pg_key))) {
      if (OB_SCHEMA_ERROR == ret) {
        const uint64_t table_id = pkey.get_table_id() & ~OB_LINK_TABLE_MASK;
        const ObPartitionKey tmp_key2(table_id, partition_id, partition_cnt);
        pg_key = tmp_key2;
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to get pg key", K(ret), K(pkey), K(tmp_key1));
      }
    }
  } else if (pkey.is_pg()) {
    pg_key = pkey;
  } else if (OB_FAIL(get_pg_key_from_index_schema_(pkey, pg_key))) {
    STORAGE_LOG(WARN, "failed to get pg key", K(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionService::get_pg_key_from_index_schema_(const ObPartitionKey& pkey, ObPGKey& pg_key) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_index_.get_pg_key(pkey, pg_key))) {
    const uint64_t table_id = pkey.get_table_id();
    const uint64_t fetch_tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
    ObSchemaGetterGuard schema_guard;
    const ObSimpleTableSchemaV2* t_schema = NULL;

    if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard)) ||
        OB_FAIL(schema_guard.get_table_schema(table_id, t_schema)) || OB_ISNULL(t_schema)) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "failed to get schema", K(ret), KPC(t_schema), K(table_id));
    } else if (!t_schema->is_binding_table()) {
      pg_key = pkey;
    } else if (OB_FAIL(t_schema->get_pg_key(pkey, pg_key))) {
      STORAGE_LOG(WARN, "get pg key error", K(ret), K(pkey), K(pg_key));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartitionService::generate_task_pg_info_(
    const ObPartitionArray& participants, ObIPartitionArrayGuard& out_pg_guard_arr)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); ++i) {
    const ObPGKey& pg_key = participants.at(i);
    ObIPartitionGroupGuard pkey_guard;
    if (OB_FAIL(get_partition(pg_key, pkey_guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(pg_key), K(ret));
    } else if (NULL == pkey_guard.get_partition_group()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pg_key), K(ret));
    } else if (OB_FAIL(out_pg_guard_arr.push_back(pkey_guard.get_partition_group()))) {
      STORAGE_LOG(WARN, "pkey guard push back error", K(ret), K(i), K(participants));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartitionService::start_participant(transaction::ObTransDesc& trans_desc,
    const common::ObPartitionArray& participants, ObPartitionEpochArray& partition_epoch_arr)
{
  ObIPartitionArrayGuard pkey_guard_arr;
  pkey_guard_arr.set_pg_mgr(this->pg_mgr_);
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(participants.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(trans_desc), "participants count", participants.count(), K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition service is not running", K(ret));
  } else if (OB_FAIL(generate_task_pg_info_(participants, pkey_guard_arr))) {
    STORAGE_LOG(WARN, "generate task pg info error", K(ret), K(trans_desc), K(participants));
  } else if (OB_FAIL(txs_->start_participant(trans_desc, participants, partition_epoch_arr, pkey_guard_arr))) {
    STORAGE_LOG(WARN, "txs fail to start participant", K(ret), K(participants));
  } else if (OB_FAIL(check_schema_version(pkey_guard_arr))) {
    int tmp_ret = OB_SUCCESS;
    bool is_rollback = true;
    if (OB_SUCCESS != (tmp_ret = txs_->end_participant(is_rollback, trans_desc, participants))) {
      STORAGE_LOG(WARN, "txs fail to end_participant", K(tmp_ret));
    }
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(DEBUG, "start participant failed", K(trans_desc), K(participants), K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = warm_up_service_->register_warm_up_ctx(trans_desc))) {
      STORAGE_LOG(WARN, "failed to regist warm up ctx", K(tmp_ret));
    }
    STORAGE_LOG(DEBUG, "start participant success", K(trans_desc), K(participants), K(ret));
  }
  return ret;
}

int ObPartitionService::end_participant(
    bool is_rollback, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants)
{
  int ret = common::OB_SUCCESS;

  if (OB_SUCC(check_init(txs_, "transaction service"))) {
    ret = txs_->end_participant(is_rollback, trans_desc, participants);

    if (NULL != trans_desc.get_warm_up_ctx()) {
      // MUST invoke deregister no matter the value of ret is OB_SUCCESS or not
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = warm_up_service_->deregister_warm_up_ctx(trans_desc))) {
        STORAGE_LOG(WARN, "failed to deregister warm up ctx", K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObPartitionService::start_stmt(const transaction::ObStmtParam& stmt_param, transaction::ObTransDesc& trans_desc,
    const common::ObPartitionLeaderArray& pla, common::ObPartitionArray& unreachable_participants)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(check_init(txs_, "transaction service"))) {
    if (OB_FAIL(txs_->start_stmt(stmt_param, trans_desc, pla, unreachable_participants))) {
      TRANS_LOG(WARN, "ps start stmt error", K(ret), K(stmt_param), K(trans_desc), K(pla), K(unreachable_participants));
    }
  }

  return ret;
}

int ObPartitionService::end_stmt(bool is_rollback, bool is_incomplete,
    const ObPartitionArray& cur_stmt_all_participants, const transaction::ObPartitionEpochArray& epoch_arr,
    const ObPartitionArray& discard_participants, const ObPartitionLeaderArray& pla,
    transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(check_init(txs_, "transaction service"))) {
    ret = txs_->end_stmt(
        is_rollback, is_incomplete, cur_stmt_all_participants, epoch_arr, discard_participants, pla, trans_desc);
  }
  return ret;
}

int ObPartitionService::start_nested_stmt(transaction::ObTransDesc& trans_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(check_init(txs_, "transaction service"))) {
    if (OB_FAIL(txs_->start_nested_stmt(trans_desc))) {
      TRANS_LOG(WARN, "ps start nested stmt error", K(ret), K(trans_desc));
    }
  }
  return ret;
}

int ObPartitionService::end_nested_stmt(
    transaction::ObTransDesc& trans_desc, const ObPartitionArray& participants, const bool is_rollback)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(check_init(txs_, "transaction service"))) {
    if (OB_FAIL(txs_->end_nested_stmt(trans_desc, participants, is_rollback))) {
      TRANS_LOG(WARN, "ps end nested stmt error", K(ret), K(trans_desc), K(participants), K(is_rollback));
    }
  }
  return ret;
}

int ObPartitionService::half_stmt_commit(
    const transaction::ObTransDesc& trans_desc, const common::ObPartitionKey& partition)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(check_init(txs_, "transaction service"))) {
    ret = txs_->half_stmt_commit(trans_desc, partition);
  }
  return ret;
}

int ObPartitionService::replay(const ObPartitionKey& partition, const char* log, const int64_t size,
    const uint64_t log_id, const int64_t log_ts, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObStorageLogType log_type;
  const ObPartitionKey& pkey = partition;
  ObIPartitionGroupGuard guard;
  ObDataStorageInfo data_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "partition service is not initialized");
  } else if (OB_ISNULL(log) || size <= 0 || !is_valid_log_id(log_id) || 0 >= log_ts) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(log), K(size), K(log_id), K(log_ts), K(ret));
  } else if (OB_ISNULL(txs_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "transaction service can not be NULL", K(pkey), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(decode_log_type(log, size, pos, log_type))) {
    STORAGE_LOG(WARN, "deserialize log_type error", K(size), K(pos), K(ret));
  } else if (OB_LOG_MAJOR_FREEZE == log_type) {
    STORAGE_LOG(INFO, "skip freeze log");
  } else if (OB_FAIL(guard.get_partition_group()->get_saved_data_info(data_info))) {
    STORAGE_LOG(WARN, "get saved data info error", K(ret), K(partition));
  } else if (log_id < data_info.get_last_replay_log_id()) {
    STORAGE_LOG(DEBUG, "filter log id for replay", K(ret), K(log_id), K(data_info));
  } else if (OB_LOG_SPLIT_SOURCE_PARTITION == log_type) {
    ObPartitionSplitSourceLog source_log;
    if (OB_FAIL(source_log.deserialize(log, size, pos))) {
      STORAGE_LOG(WARN, "deserialize partition split source log failed", K(ret), K(pkey));
    } else if (OB_FAIL(source_log.replace_tenant_id(pkey.get_tenant_id()))) {
      STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(pkey));
    } else if (OB_FAIL(guard.get_partition_group()->replay_split_source_log(source_log, log_id, log_ts))) {
      if (OB_CS_OUTOF_DISK_SPACE == ret || OB_SLOG_REACH_MAX_CONCURRENCY == ret) {
        ret = OB_EAGAIN;
      }
      STORAGE_LOG(WARN, "replay split source log failed", K(ret), K(pkey), K(source_log));
    } else {
      STORAGE_LOG(INFO, "replay split source log success", K(pkey), K(source_log));
    }
  } else if (OB_LOG_SPLIT_DEST_PARTITION == log_type) {
    ObPartitionSplitDestLog dest_log;
    if (OB_FAIL(dest_log.deserialize(log, size, pos))) {
      STORAGE_LOG(WARN, "deserialize partition split dest log failed", K(ret), K(pkey));
    } else if (OB_FAIL(dest_log.replace_tenant_id(pkey.get_tenant_id()))) {
      STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(pkey));
    } else if (OB_FAIL(guard.get_partition_group()->replay_split_dest_log(dest_log))) {
      if (OB_CS_OUTOF_DISK_SPACE == ret || OB_SLOG_REACH_MAX_CONCURRENCY == ret) {
        ret = OB_EAGAIN;
      }
      STORAGE_LOG(WARN, "replay split dest log failed", K(ret), K(pkey), K(dest_log));
    } else {
      STORAGE_LOG(INFO, "replay split dest log success", K(pkey), K(dest_log));
    }
  } else if (ObStorageLogTypeChecker::is_partition_meta_log(log_type)) {
    if (OB_FAIL(guard.get_partition_group()->replay_partition_meta_log(log_type, log_id, log + pos, size - pos))) {
      STORAGE_LOG(WARN, "replay partition meta log failed", K(ret), K(pkey));
    } else {
      STORAGE_LOG(INFO, "replay partition meta log success", K(ret), K(pkey));
    }
  } else if (OB_LOG_OFFLINE_PARTITION == log_type) {
    const bool is_physical_drop = false;
    STORAGE_LOG(INFO, "start to replay offline partition log succ", K(ret), K(pkey), K(log_id), K(log_ts));
    if (OB_FAIL(replay_offline_partition_log(pkey, log_id, is_physical_drop))) {
      STORAGE_LOG(WARN, "replay offline partition log failed", K(ret), K(pkey), K(log_id), K(log_ts));
    } else {
      STORAGE_LOG(INFO, "replay offline partition log succ", K(ret), K(pkey), K(log_id), K(log_ts));
    }
  } else if (OB_LOG_OFFLINE_PARTITION_V2 == log_type) {
    STORAGE_LOG(INFO, "start to replay offline partition log v2", K(ret), K(pkey), K(log_id), K(log_ts));
    ObOfflinePartitionLog offline_log;
    const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    if (OB_FAIL(offline_log.deserialize(log, size, pos))) {
      STORAGE_LOG(WARN, "deserialize partition split dest log failed", K(ret), K(pkey));
    } else if (common::OB_INVALID_CLUSTER_ID != offline_log.get_cluster_id() &&
               self_cluster_id != offline_log.get_cluster_id()) {
      // cluster_id valid and not match with self, skip reolay offline_log
      STORAGE_LOG(INFO,
          "offline_log's cluster_id is not match with self, skip replay",
          K(ret),
          K(pkey),
          K(self_cluster_id),
          K(offline_log));
    } else if (OB_FAIL(offline_log.replace_tenant_id(pkey.get_tenant_id()))) {
      STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(pkey));
    } else if (OB_UNLIKELY(!offline_log.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "offline partition log is not valid", K(ret), K(pkey), K(log_id), K(log_ts), K(offline_log));
    } else if (OB_FAIL(replay_offline_partition_log(pkey, log_id, offline_log.is_physical_drop()))) {
      STORAGE_LOG(
          WARN, "replay offline partition log v2 failed", K(ret), K(pkey), K(log_id), K(log_ts), K(offline_log));
    } else {
      STORAGE_LOG(INFO, "replay offline partition log v2 succ", K(ret), K(pkey), K(log_id), K(log_ts), K(offline_log));
    }
  } else if (OB_LOG_ADD_PARTITION_TO_PG == log_type) {
    ObAddPartitionToPGLog pg_log;
    ObCreatePartitionParam create_param;
    const ObReplicaRestoreStatus is_restore = guard.get_partition_group()->get_pg_storage().get_restore_status();
    const int64_t create_frozen_version = guard.get_partition_group()->get_pg_storage().get_create_frozen_version();
    STORAGE_LOG(INFO,
        "start to replay add partition to pg log succ",
        K(ret),
        K(pkey),
        K(log_id),
        K(log_ts),
        K(is_restore),
        K(create_frozen_version));
    if (OB_FAIL(pg_log.deserialize(log, size, pos))) {
      STORAGE_LOG(WARN, "deserialize add partition to pg log failed", K(ret), K(pkey));
    } else if (OB_FAIL(create_param.extract_from(pg_log.get_arg()))) {
      STORAGE_LOG(WARN, "failed to extract create_param", K(ret), K(pkey));
    } else if (OB_FAIL(replace_restore_info_(pkey.get_tenant_id(), is_restore, create_frozen_version, create_param))) {
      STORAGE_LOG(WARN, "failed to replace restore version", K(ret), K(create_param));
    } else if (OB_FAIL(replay_add_partition_to_pg_clog(create_param, log_id, log_ts, schema_version))) {
      if (OB_CS_OUTOF_DISK_SPACE == ret || OB_SLOG_REACH_MAX_CONCURRENCY == ret) {
        ret = OB_EAGAIN;
      }
      STORAGE_LOG(WARN, "replay add partition to pg log failed", K(ret), K(pg_log));
    } else {
      STORAGE_LOG(INFO,
          "replay add partition to pg log success",
          "pg_partition_key",
          pg_log.get_arg().partition_key_,
          "replica_type",
          pg_log.get_arg().replica_type_,
          "frozen_timestmap",
          pg_log.get_arg().frozen_timestamp_,
          "last_replay_log_id",
          pg_log.get_arg().last_replay_log_id_,
          "pg_key",
          pg_log.get_arg().pg_key_,
          "replica_property",
          pg_log.get_arg().replica_property_,
          "split_info",
          pg_log.get_arg().split_info_,
          "can_repeat_create_tenant",
          pg_log.get_arg().ignore_member_list_);
    }
  } else if (OB_LOG_REMOVE_PARTITION_FROM_PG == log_type) {
    // Since 2.2.7, in order to adapt standby failover, REMOVE_PARTITION_FROM_PG log won't replay.
    STORAGE_LOG(INFO, "start to replay remove partition from pg log", K(ret), K(pkey), K(log_id), K(log_ts));
    ObRemovePartitionFromPGLog pg_log;
    if (OB_FAIL(pg_log.deserialize(log, size, pos))) {
      STORAGE_LOG(WARN, "deserialize add partition to pg log failed", K(ret), K(pkey));
    } else if (OB_FAIL(pg_log.replace_tenant_id(pkey.get_tenant_id()))) {
      STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(pkey));
    } else {
      STORAGE_LOG(INFO, "remove partition from pg log no need replay anymore", K(ret), K(pg_log));
    }
  } else if (OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG == log_type) {
    STORAGE_LOG(INFO, "start to replay partition schema version change succ", K(pkey), K(log_id), K(log_ts));
    ObPGSchemaChangeLog schema_version_change_log;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(schema_version_change_log.deserialize(log, size, pos))) {
      STORAGE_LOG(WARN, "deserialize schema version change log failed", K(ret), K(pkey));
    } else if (OB_FAIL(schema_version_change_log.replace_tenant_id(pkey.get_tenant_id()))) {
      STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(pkey));
    } else if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
    } else if (OB_UNLIKELY(NULL == guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
    } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().replay_partition_schema_version_change_log(
                   schema_version_change_log.get_schema_version()))) {
      schema_version = schema_version_change_log.get_schema_version();
      STORAGE_LOG(INFO,
          "replay partition schema version change failed",
          K(ret),
          K(schema_version_change_log),
          K(log_id),
          K(log_ts));
    } else if (schema_version_change_log.get_index_id() > 0 &&
               OB_FAIL(guard.get_partition_group()->get_pg_storage().create_index_table_store(
                   schema_version_change_log.get_pkey(),
                   schema_version_change_log.get_index_id(),
                   schema_version_change_log.get_schema_version()))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        FLOG_INFO("pg partition is already removed before replay create index table store clog",
            K(schema_version_change_log));
      } else {
        LOG_WARN("failed to create index table store when replaying partition_schema_change_log",
            K(ret),
            K(schema_version_change_log));
      }
    } else {
      STORAGE_LOG(
          INFO, "replay partition schema version change success", K(schema_version_change_log), K(log_id), K(log_ts));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "unknown log type", K(log_type), K(ret));
  }
  return ret;
}

int ObPartitionService::replace_restore_info_(const uint64_t cur_tenant_id, const ObReplicaRestoreStatus is_restore,
    const int64_t create_frozen_version, ObCreatePartitionParam& create_param)
{
  int ret = OB_SUCCESS;
  const uint64_t saved_tenant_id = create_param.partition_key_.get_tenant_id();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (saved_tenant_id == cur_tenant_id) {
    // do nothing
  } else if (OB_FAIL(create_param.replace_tenant_id(cur_tenant_id))) {
    STORAGE_LOG(WARN, "failed to replace_tenant_id", K(ret), K(saved_tenant_id));
  }

  if (OB_SUCC(ret) && is_restore != REPLICA_NOT_RESTORE) {  // for restore
    if (is_restore != REPLICA_LOGICAL_RESTORE_DATA && is_restore != REPLICA_RESTORE_WAIT_ALL_DUMPED &&
        is_restore != REPLICA_RESTORE_MEMBER_LIST) {
      FLOG_INFO("replace restore version", K(is_restore), K(create_frozen_version), K(cur_tenant_id), K(create_param));
      create_param.memstore_version_ = create_frozen_version + 1;
    } else {
      FLOG_INFO("no need to replace restore version",
          K(is_restore),
          K(create_frozen_version),
          K(cur_tenant_id),
          K(create_param));
    }
  }

  return ret;
}
/********************************************************************
 * refresh locality
 *******************************************************************
 */

ObPartitionService::ObRefreshLocalityTask::ObRefreshLocalityTask(ObPartitionService* partition_service)
    : IObDedupTask(T_REFRESH_LOCALITY), partition_service_(partition_service)
{}

ObPartitionService::ObRefreshLocalityTask::~ObRefreshLocalityTask()
{}

int64_t ObPartitionService::ObRefreshLocalityTask::hash() const
{
  uint64_t hash_val = 0;
  return static_cast<int64_t>(hash_val);
}

bool ObPartitionService::ObRefreshLocalityTask::operator==(const IObDedupTask& other) const
{
  UNUSED(other);
  bool b_ret = true;
  return b_ret;
}

int64_t ObPartitionService::ObRefreshLocalityTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

IObDedupTask* ObPartitionService::ObRefreshLocalityTask::deep_copy(char* buffer, const int64_t buf_size) const
{
  ObRefreshLocalityTask* task = NULL;
  if (OB_UNLIKELY(OB_ISNULL(buffer)) || OB_UNLIKELY(buf_size < get_deep_copy_size())) {
    STORAGE_LOG(WARN, "invalid argument", KP(buffer), K(buf_size));
  } else {
    task = new (buffer) ObRefreshLocalityTask(partition_service_);
  }
  return task;
}

int ObPartitionService::ObRefreshLocalityTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition service is null", K(ret));
  } else if (OB_FAIL(partition_service_->force_refresh_locality_info())) {
    STORAGE_LOG(WARN, "process refresh locality task fail", K(ret));
  }
  return ret;
}

/********************************************************************
 * leader change
 *******************************************************************
 */

bool ObPartitionService::dispatch_task(const ObCbTask& cb_task, ObIPartitionGroup* partition)
{
  bool dispatched = false;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!cb_task.is_valid() || OB_ISNULL(partition))) {
  } else if (LEADER_TAKEOVER_TASK == cb_task.task_type_) {
    int memstore_percent = partition->get_replica_property().get_memstore_percent();
    if ((!cb_task.large_cb_ && 0 == memstore_percent) || (cb_task.large_cb_ && 0 != memstore_percent)) {
      ObCbTask task;
      task = cb_task;
      task.large_cb_ = !cb_task.large_cb_;
      if (OB_FAIL(push_callback_task(task))) {
        STORAGE_LOG(WARN, "push callback task failed", K(task), K(ret));
      } else {
        dispatched = true;
      }
    }
  }

  return dispatched;
}

int ObPartitionService::async_leader_revoke(const common::ObPartitionKey& pkey, const uint32_t revoke_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else if (OB_UNLIKELY(OB_FAIL(election_mgr_->leader_revoke(pkey, revoke_type)))) {
    STORAGE_LOG(ERROR, "leader_revoke failed", K(ret), K(pkey), K(revoke_type));
  } else {
    FLOG_INFO("async leader revoke success", K(pkey), K(revoke_type));
  }
  return ret;
}

int ObPartitionService::submit_ms_info_task(const common::ObPartitionKey& pkey, const common::ObAddr& server,
    const int64_t cluster_id, const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp,
    const int64_t replica_num, const common::ObMemberList& prev_member_list,
    const common::ObMemberList& curr_member_list, const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid() || !server.is_valid() || OB_INVALID_CLUSTER_ID == cluster_id ||
             OB_INVALID_ID == ms_log_id || OB_INVALID_TIMESTAMP == mc_timestamp || replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(pkey),
        K(ms_log_id),
        K(mc_timestamp),
        K(replica_num),
        K(prev_member_list),
        K(curr_member_list));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObMsInfoTask task(pkey, server, cluster_id, log_type, ms_log_id, mc_timestamp, replica_num, ms_proposal_id, now);
    if (OB_FAIL(task.update_prev_member_list(prev_member_list))) {
      STORAGE_LOG(WARN, "update_prev_member_list failed", K(task), K(ret));
    } else if (OB_FAIL(task.update_curr_member_list(curr_member_list))) {
      STORAGE_LOG(WARN, "update_curr_member_list failed", K(task), K(ret));
    } else if (OB_FAIL(slog_writer_thread_pool_.push(&task))) {
      STORAGE_LOG(WARN, "push task failed", K(task), K(ret));
    } else {
      STORAGE_LOG(INFO, "submit_ms_info_task success", K(task));
    }
  }
  return ret;
}

int ObPartitionService::on_leader_revoke(const common::ObPartitionKey& pkey, const common::ObRole& role)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else if (LEADER == role) {
    // only LEADER need exec
    ObCbTask task;
    task.task_type_ = LEADER_REVOKE_TASK;
    task.pkey_ = pkey;
    task.role_ = role;
    // following fields are of no use
    task.succeed_ = true;
    task.retry_cnt_ = 0;
    task.ret_code_ = OB_SUCCESS;
    task.leader_active_arg_.is_elected_by_changing_leader_ = false;
    if (OB_FAIL(push_callback_task(task))) {
      STORAGE_LOG(WARN, "push callback task failed", K(task), K(ret));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::on_leader_takeover(
    const common::ObPartitionKey& pkey, const common::ObRole& role, const bool is_elected_by_changing_leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else if (LEADER == role) {
    // only LEADER need exec this
    ObCbTask task;
    task.task_type_ = LEADER_TAKEOVER_TASK;
    task.pkey_ = pkey;
    task.role_ = role;
    task.succeed_ = true;
    task.retry_cnt_ = 0;
    task.ret_code_ = OB_SUCCESS;
    task.leader_active_arg_.is_elected_by_changing_leader_ = is_elected_by_changing_leader;
    if (OB_FAIL(push_callback_task(task))) {
      STORAGE_LOG(WARN, "push callback task failed", K(task), K(ret));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::internal_leader_revoke(const ObCbTask& revoke_task)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = revoke_task.pkey_;
  ObIPartitionGroupGuard guard;
  STORAGE_LOG(INFO, "begin internal_leader_revoke", K(pkey), K(revoke_task));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret), K(pkey));
  } else if (!revoke_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(revoke_task), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (LEADER != revoke_task.role_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected role, internal_leader_revoke failed", K(pkey), K(ret), K(revoke_task));
  } else if (OB_FAIL(guard.get_partition_group()->try_switch_partition_state(L_REVOKE))) {
    if (OB_EAGAIN == ret) {
      // can not switch partition state now, retry later
      ret = OB_SUCCESS;
      ObCbTask task;
      task = revoke_task;
      task.retry_cnt_++;
      task.ret_code_ = ret;
      task.leader_active_arg_.is_elected_by_changing_leader_ = false;
      // overwrite retcode
      if (OB_FAIL(push_callback_task(task))) {
        STORAGE_LOG(WARN, "push callback task failed", K(task), K(ret));
      } else {
        STORAGE_LOG(INFO, "push back leader_revoke task successfully", K(task));
      }
    } else {
      STORAGE_LOG(WARN, "partition can not leader revoke", K(pkey), K(ret));
    }
  } else if (OB_FAIL(txs_->leader_revoke(pkey))) {
    if (OB_NOT_RUNNING != ret) {
      STORAGE_LOG(ERROR, "transaction leader revoke failed", K(ret), K(pkey));
    } else {
      STORAGE_LOG(WARN, "transaction leader revoke failed", K(ret), K(pkey));
    }
  } else if (OB_FAIL(guard.get_partition_group()->leader_revoke())) {
    STORAGE_LOG(WARN, "partition leader revoke failed", K(ret), K(pkey));
  } else if (OB_FAIL(rs_cb_->submit_pt_update_role_task(pkey))) {
    STORAGE_LOG(WARN, "on_leader_revoke call back failed");
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    guard.get_partition_group()->replay_status_revoke();
    if (OB_FAIL(guard.get_partition_group()->switch_partition_state(F_WORKING))) {
      STORAGE_LOG(WARN, "switch partition state to L_REVOKE failed", K(pkey), K(ret));
    } else {
      STORAGE_LOG(INFO, "on_leader_revoke callback succeed", K(pkey));
    }
    const bool is_normal_pg = !(guard.get_partition_group()->get_pg_storage().is_restore());
    if ((OB_SYS_TENANT_ID != pkey.get_tenant_id()) && is_normal_pg) {
      (void)clog_mgr_->delete_pg_archive_task(guard.get_partition_group());
    }
  }
  return ret;
}

int ObPartitionService::internal_leader_takeover(const ObCbTask& takeover_task)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = takeover_task.pkey_;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObPartitionState state = INVALID_STATE;
  bool need_retry = false;

  STORAGE_LOG(INFO, "leader_takeover", K(pkey), K(takeover_task));
  STORAGE_LOG(INFO, "begin internal_leader_takeover", K(pkey));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret), K(pkey));
  } else if (!takeover_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(takeover_task), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (LEADER != takeover_task.role_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected role, internal_leader_takeover failed", K(pkey), K(ret), K(takeover_task));
  } else if (FALSE_IT(state = partition->get_partition_state())) {
  } else if (L_TAKEOVER != state && !is_follower_state(state)) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "partition can not leader takeover", K(pkey), K(state), K(ret));
  } else if (L_TAKEOVER != state && OB_FAIL(partition->try_switch_partition_state(L_TAKEOVER))) {
    // can not switch partition state now, retry later
    need_retry = true;
  } else if (dispatch_task(takeover_task, partition)) {
    STORAGE_LOG(INFO, "callback task dispatched", K(pkey));
  } else if (OB_FAIL(partition->leader_takeover())) {
    need_retry = true;
  } else {
    ObCbTask task;
    task = takeover_task;
    task.task_type_ = LEADER_TAKEOVER_BOTTOM_TASK;
    task.ret_code_ = OB_SUCCESS;
    task.large_cb_ = false;
    // overwrite retcode
    if (OB_FAIL(push_callback_task(task))) {
      STORAGE_LOG(WARN, "push callback task failed", K(task), K(ret));
    } else {
      STORAGE_LOG(INFO, "push back leader_takeover_bottom_task", K(pkey));
    }
  }

  if (need_retry) {
    ObCbTask task;
    task = takeover_task;
    task.retry_cnt_++;
    task.ret_code_ = ret;
    // overwrite retcode
    if (OB_FAIL(push_callback_task(task))) {
      STORAGE_LOG(WARN, "push callback task failed", K(task), K(ret));
    } else {
      STORAGE_LOG(INFO, "push back leader_takeover task successfully", K(task));
    }
  }

  return ret;
}

int ObPartitionService::internal_leader_takeover_bottom_half(const ObCbTask& takeover_task)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = takeover_task.pkey_;
  ObIPartitionGroupGuard guard;
  ObPartitionState state = INVALID_STATE;
  int64_t checkpoint = 0;

  STORAGE_LOG(INFO, "leader_takeover_bottom_half", K(pkey));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated");
  } else if (!takeover_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(takeover_task), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (FALSE_IT(state = guard.get_partition_group()->get_partition_state())) {
  } else if (L_TAKEOVER != state) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "partition can not leader takeover bottom half", K(ret), K(pkey), K(state));
  } else if (LEADER == takeover_task.role_) {
    if (OB_FAIL(guard.get_partition_group()->get_replay_checkpoint(checkpoint))) {
      TRANS_LOG(WARN, "get save safe slave read timestamp error", K(ret), K(checkpoint), K(pkey));
    } else if (OB_FAIL(txs_->leader_takeover(pkey, takeover_task.leader_active_arg_, checkpoint))) {
      STORAGE_LOG(ERROR, "leader takeover failed", K(checkpoint), K(ret), K(pkey));
    } else {
      // do nothing
    }
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    if (guard.get_partition_group()->get_pg_storage().has_memstore() && OB_FAIL(save_base_schema_version(guard))) {
      STORAGE_LOG(WARN, "save_base_schema_version failed", K(pkey), K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->switch_partition_state(L_TAKEOVERED))) {
      STORAGE_LOG(WARN, "switch partition state to L_TAKEOVERED failed", K(pkey), K(ret));
    } else {
      STORAGE_LOG(INFO, "on_leader_takeover callback succeed", K(pkey));
    }
  }

  return ret;
}

int ObPartitionService::on_leader_active(
    const common::ObPartitionKey& pkey, const common::ObRole& role, const bool is_elected_by_changing_leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated");
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(ret));
  } else if (LEADER == role) {
    ObCbTask task;
    task.task_type_ = LEADER_ACTIVE_TASK;
    task.pkey_ = pkey;
    task.role_ = role;
    task.succeed_ = true;
    task.retry_cnt_ = 0;
    task.ret_code_ = OB_SUCCESS;
    task.leader_active_arg_.is_elected_by_changing_leader_ = is_elected_by_changing_leader;
    if (OB_FAIL(push_callback_task(task))) {
      STORAGE_LOG(WARN, "push callback task failed", K(task), K(ret));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::internal_leader_active(const ObCbTask& active_task)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = active_task.pkey_;

  STORAGE_LOG(INFO, "begin internal_leader_active", K(pkey), K(active_task));

  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionLogService* pls = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret), K(pkey));
  } else if (!active_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(active_task), K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(rs_cb_->submit_pt_update_role_task(pkey))) {
    STORAGE_LOG(WARN, "internal_leader_active callback failed", K(pkey), K(ret));
  } else if (OB_FAIL(submit_pg_pt_update_task_(pkey))) {
    STORAGE_LOG(WARN, "submit_pg_pt_update_task_ failed", K(pkey), K(ret));
  } else if (LEADER != active_task.role_) {
    // only LEADER need exec
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected role, internal_leader_active failed", K(pkey), K(ret), K(active_task));
  } else {
    if (OB_FAIL(txs_->leader_active(pkey, active_task.leader_active_arg_))) {
      STORAGE_LOG(ERROR, "transaction leader active failed", K(pkey), K(ret));
    } else if (OB_FAIL(partition->leader_active())) {
      STORAGE_LOG(WARN, "partition leader active failed", K(ret), K(pkey));
    } else if (OB_FAIL(partition->switch_partition_state(L_WORKING))) {
      // switch_partition_state needs to be executed after txs_,
      // otherwise the state may be confused and stuck in the concurrent execution of role switching tasks
      STORAGE_LOG(WARN, "switch partition state to L_WORKING failed", K(pkey), K(ret));
    } else {
    }

    if (OB_SUCC(ret)) {
      // the partitions of the sys tenant and the partition during the recovery process are not archived
      const bool is_normal_pg = !(guard.get_partition_group()->get_pg_storage().is_restore());
      if ((OB_SYS_TENANT_ID != pkey.get_tenant_id()) && is_normal_pg) {
        (void)clog_mgr_->add_pg_archive_task(partition);
        observer::ObTTLManager::get_instance().on_leader_active(pkey);
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(partition)) {
    int tmp_ret = OB_SUCCESS;
    const uint32_t revoke_type = ObElection::RevokeType::PS_LEADER_ACTIVE_FAIL;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = election_mgr_->leader_revoke(pkey, revoke_type)))) {
      STORAGE_LOG(ERROR, "leader_revoke failed", K(pkey), K(tmp_ret), K(revoke_type));
    }
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = partition->switch_partition_state(L_CANCELED)))) {
      STORAGE_LOG(ERROR, "switch L_CANCELED failed", "state", partition->get_partition_state(), K(pkey), K(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = partition->leader_revoke())) {
      STORAGE_LOG(WARN, "partition leader revoke failed", K(tmp_ret), K(pkey));
    }
  }

  return ret;
}

int ObPartitionService::do_warm_up_request(const obrpc::ObWarmUpRequestArg& arg, const int64_t recieve_ts)
{
  int ret = OB_SUCCESS;
  const int64_t cur_time = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (cur_time > recieve_ts + 1000 * 1000) {
    // do nothing
    EVENT_INC(WARM_UP_REQUEST_IN_DROP_COUNT);
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {  // print drop log every 1s
      LOG_INFO("drop warm up request delayed in queue too long time", K(recieve_ts));
    }
  } else {
    const ObWarmUpRequestList& requests = arg.wrapper_.get_requests();
    const ObIWarmUpRequest* request = NULL;
    for (ObWarmUpRequestList::const_iterator iter = requests.begin(); OB_SUCC(ret) && iter != requests.end(); ++iter) {
      if (!is_running_) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "partition service is not running", K(ret));
      } else if (NULL == (request = *iter)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "request node and request must not null", K(ret), KP(request));
      } else {
        ObIPartitionGroupGuard guard;
        if (OB_FAIL(get_partition(request->get_pkey(), guard))) {
          STORAGE_LOG(WARN, "get partition failed", K(ret), K(request->get_pkey()));
        } else if (OB_ISNULL(guard.get_partition_group())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "get partition failed", K(ret), K(request->get_pkey()));
        } else if (OB_FAIL(guard.get_partition_group()->do_warm_up_request(request))) {
          STORAGE_LOG(WARN, "failed to do warm up request", K(ret), K(request->get_pkey()));
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::check_can_start_service(
    bool& can_start_service, int64_t& safe_weak_read_snapshot, ObPartitionKey& min_slave_read_ts_pkey)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;
  ObPartitionState partition_state;
  ObIPartitionLogService* pls = NULL;
  int64_t tmp_safe_weak_read_snapshot = INT64_MAX;
  ObPartitionKey tmp_key;
  int64_t timestamp = 0;
  bool is_all_unreachable_partition = true;
  can_start_service = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized, ", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "partition service not running", K(ret));
  } else if (is_service_started()) {
    // already start service
    can_start_service = true;
  } else if (is_empty()) {
    // observer is starting...
    can_start_service = true;
    is_all_unreachable_partition = true;
  } else if (!clog_mgr_->is_scan_finished()) {
    can_start_service = false;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      TRANS_LOG(INFO, "scan disk not finished, need wait");
    }
  } else if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "scan next partition failed", K(ret));
        }
        break;
      } else if (NULL == partition) {
        ret = OB_PARTITION_NOT_EXIST;
        STORAGE_LOG(WARN, "get partition failed", K(ret));
      } else if (OFFLINE == (partition_state = partition->get_partition_state()) || OFFLINING == partition_state ||
                 REMOVE == partition_state || INIT == partition_state || INVALID_STATE == partition_state ||
                 !ObReplicaTypeCheck::can_slave_read_replica(partition->get_replica_type()) ||
                 partition->is_replica_using_remote_memstore()) {
        ret = OB_STATE_NOT_MATCH;
      } else if (NULL != (pls = partition->get_log_service()) && pls->need_skip_when_check_start_service()) {
        // skip the paxos replica that is not in the leader's member list
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(partition->get_weak_read_timestamp(timestamp))) {
        STORAGE_LOG(WARN, "get weak read timestmap fail", K(ret), "pkey", partition->get_partition_key());
      } else {
        is_all_unreachable_partition = false;
        // if the partition is more than 10s behind, the server cannot provide
        // external services during the restart process
        if (timestamp <= ObTimeUtility::current_time() - 30 * 1000 * 1000) {
          can_start_service = false;
        }
        if (timestamp < tmp_safe_weak_read_snapshot) {
          tmp_safe_weak_read_snapshot = timestamp;
          tmp_key = partition->get_partition_key();
        }
      }
      if (OB_FAIL(ret)) {
        if (OB_STATE_NOT_MATCH == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    // If all partitions are all offline, it is equivalent to the observer
    // just boostrap, and the observer can start_service.
    if (is_all_unreachable_partition) {
      can_start_service = true;
    }
    if (OB_SUCC(ret) && INT64_MAX != tmp_safe_weak_read_snapshot) {
      safe_weak_read_snapshot = tmp_safe_weak_read_snapshot;
      min_slave_read_ts_pkey = tmp_key;
    }

    if (NULL != iter) {
      revert_pg_iter(iter);
    }
  }

  return ret;
}

int ObPartitionService::do_partition_loop_work(ObIPartitionGroup& partition)
{
  int ret = OB_SUCCESS;
  if (!partition.can_weak_read()) {
    // If the partition cannot provide weak read services, there is no need to perform loop work.
  } else {
    (void)partition.do_partition_loop_work();
  }
  return ret;
}

int ObPartitionService::generate_partition_weak_read_snapshot_version(ObIPartitionGroup& partition, bool& need_skip,
    bool& is_user_partition, int64_t& wrs_version, const int64_t max_stale_time)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = INT64_MAX;
  const ObPartitionKey& pkey = partition.get_partition_key();
  need_skip = false;

  if (!partition.can_weak_read()) {
    need_skip = true;
  } else if (OB_FAIL(partition.generate_weak_read_timestamp(max_stale_time, timestamp))) {
    STORAGE_LOG(DEBUG, "fail to generate weak read timestamp", KR(ret), K(pkey), K(max_stale_time));
    need_skip = true;
    ret = OB_SUCCESS;
  } else if (true == partition.get_migrating_flag()) {
    // check the weak read timestamp of the migrated partition
    if (timestamp > ObTimeUtility::current_time() - 500 * 1000) {
      STORAGE_LOG(INFO, "partition received the latest log", K(pkey), K(timestamp));
      // clog chases within 500ms, then clear the mark
      (void)partition.set_migrating_flag(false);
      need_skip = false;
    } else {
      need_skip = true;
    }
  } else {
    int64_t snapshot_version_barrier = ObTimeUtility::current_time() - max_stale_time;
    if (timestamp <= snapshot_version_barrier) {
      // rule out these partition to avoid too old weak read timestamp
      need_skip = true;
      if (!partition.is_replica_using_remote_memstore()) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          TRANS_LOG(
              INFO, "slave read timestamp is too old, need skip", K(timestamp), K(snapshot_version_barrier), K(pkey));
        }
      }
    } else {
      need_skip = false;
    }
  }

  // check replica type
  if (OB_SUCC(ret) && false == need_skip) {
    // only process inner table with partition
    if (is_inner_table_with_partition(pkey.get_table_id())) {
      is_user_partition = false;
    } else if (!is_inner_table(pkey.get_table_id())) {
      is_user_partition = true;
    } else {
      need_skip = true;
    }
  }

  // update weak read timestamp
  if (OB_SUCC(ret) && !need_skip) {
    wrs_version = timestamp;
  }

  return ret;
}

int ObPartitionService::halt_all_prewarming(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    ObIPartitionGroup* partition = NULL;

    STORAGE_LOG(INFO, "halt all prewarming", K(tenant_id));
    SERVER_EVENT_ADD("warmup", "halt", "tenant_id", tenant_id);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "scan next partition failed", K(ret));
        }
        break;
      } else if (NULL == partition) {
        ret = OB_PARTITION_NOT_EXIST;
        STORAGE_LOG(WARN, "get partition failed", K(ret));
      } else if (tenant_id != OB_INVALID_TENANT_ID && partition->get_partition_key().get_tenant_id() != tenant_id) {
        // skip
      } else if (OB_FAIL(partition->get_pg_storage().halt_prewarm())) {
        STORAGE_LOG(WARN, "fail to halt prewarm", K(ret), "pkey", partition->get_partition_key());
      }
    }

    revert_pg_iter(iter);
    STORAGE_LOG(INFO, "finish halt prewarm", K(ret));
  }
  return ret;
}

int ObPartitionService::get_all_partition_status(int64_t& inactive_num, int64_t& total_num) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(election_mgr_->get_all_partition_status(inactive_num, total_num))) {
    LOG_WARN("fail to get all partition status", K(ret));
  }
  return ret;
}

/********************************************************************
 * minor freeze
 *******************************************************************
 */

int ObPartitionService::minor_freeze(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;
  bool upgrade_mode = static_cast<bool>(GCONF.in_major_version_upgrade_mode());
  ObHashMap<uint64_t, int64_t> version_map;
  STORAGE_LOG(INFO, "minor freeze", K(tenant_id));
  SERVER_EVENT_ADD("freeze", "do minor freeze", "tenant_id", tenant_id);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret));
  } else if (upgrade_mode) {
    ret = OB_OP_NOT_ALLOW;
    STORAGE_LOG(WARN, "minor freeze is not allowed while upgrading", K(tenant_id), K(ret));
  } else if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc scan iter", K(ret));
  } else if (OB_FAIL(version_map.create(128, ObModIds::OB_PARTITION_SERVICE))) {
    STORAGE_LOG(WARN, "fail to create hash bucket", K(ret));
  } else {
    ObVersion version;
    int tmp_ret = OB_SUCCESS;
    int ret_code = OB_SUCCESS;
    int64_t t_ret = OB_SUCCESS;
    freeze_async_worker_.before_minor_freeze();

    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "scan next partition failed.", K(ret));
        }
      } else {
        const ObPartitionKey& pkey = partition->get_partition_key();
        const uint64_t p_tenant_id = extract_tenant_id(pkey.table_id_);
        if (OB_INVALID_TENANT_ID == tenant_id || tenant_id == p_tenant_id) {
          int64_t freeze_snapshot = -1;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = freeze_partition(pkey,
                                             false,  // emergency
                                             false,  // force
                                             freeze_snapshot)))) {
            LOG_WARN("failed to freeze partition", K(ret), K(pkey));
          }

          if (-1 != freeze_snapshot) {
            int64_t tmp_freeze_version = -1;
            if (OB_UNLIKELY(OB_SUCCESS != (t_ret = version_map.get_refactored(p_tenant_id, tmp_freeze_version)))) {
              if (OB_HASH_NOT_EXIST != t_ret) {
                LOG_ERROR("failed to get from version map", K(t_ret));
              }
            } else if (freeze_snapshot > tmp_freeze_version &&
                       OB_UNLIKELY(OB_SUCCESS != (t_ret = version_map.get_refactored(
                                                      p_tenant_id, MAX(freeze_snapshot, tmp_freeze_version))))) {
              LOG_ERROR("failed to set version map", K(t_ret));
            }
          }
        }

        if (OB_SUCCESS == ret_code && OB_SUCCESS != tmp_ret) {
          // record the first error
          ret_code = tmp_ret;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && OB_SUCCESS != ret_code) {
      ret = ret_code;
    }
    if (NULL != iter) {
      revert_pg_iter(iter);
    }

    ObHashMap<uint64_t, int64_t>::iterator iter;
    for (iter = version_map.begin(); iter != version_map.end(); ++iter) {
      uint64_t tenant_id = iter->first;
      int64_t max_freeze_snapshot = iter->second;
      if (-1 != max_freeze_snapshot) {
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ObPartitionScheduler::get_instance().notify_minor_merge_start(
                                           tenant_id, max_freeze_snapshot)))) {
          STORAGE_LOG(WARN, "failed to notify minor merge start", K(tmp_ret), K(tenant_id), K(max_freeze_snapshot));
        }
      }
    }

    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "minor freeze successfully", K(tenant_id));
      SERVER_EVENT_ADD("freeze", "do minor freeze success", "tenant_id", tenant_id);
    } else {
      STORAGE_LOG(WARN, "minor freeze all/partial failed", K(tenant_id), K(ret));
      SERVER_EVENT_ADD("freeze", "do minor freeze fail", "tenant_id", tenant_id, "ret", ret);
    }

    freeze_async_worker_.after_minor_freeze();
  }
  return ret;
}

int ObPartitionService::minor_freeze(const common::ObPartitionKey& pkey, const bool emergency, const bool force)
{
  int ret = OB_SUCCESS;
  int64_t freeze_snapshot = -1;
  bool upgrade_mode = static_cast<bool>(GCONF.in_major_version_upgrade_mode());

  STORAGE_LOG(INFO, "minor freeze", K(pkey));

  freeze_async_worker_.before_minor_freeze();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret));
  } else if (upgrade_mode) {
    ret = OB_OP_NOT_ALLOW;
    STORAGE_LOG(WARN, "minor freeze is not allowed while upgrading", K(pkey), K(ret));
  } else if (OB_FAIL(freeze_partition(pkey, emergency, force, freeze_snapshot))) {
    LOG_WARN("failed to freeze partition", K(ret), K(pkey));
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "minor freeze successfully", K(pkey));
  } else {
    STORAGE_LOG(WARN, "minor freeze failed", K(pkey), K(ret));
  }

  freeze_async_worker_.after_minor_freeze();

  return ret;
}

int ObPartitionService::freeze_partition(
    const ObPartitionKey& pkey, const bool emergency, const bool force, int64_t& freeze_snapshot)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  STORAGE_LOG(DEBUG, "freeze partition", K(pkey));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition is not running now", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
  } else if (OB_FAIL(partition->freeze(emergency, force, freeze_snapshot))) {
    STORAGE_LOG(WARN, "freeze partition failed", K(ret), K(pkey));
  } else {
    STORAGE_LOG(DEBUG, "freeze partition successfully", K(pkey));
  }

  return ret;
}

int ObPartitionService::query_log_info_with_log_id(const ObPartitionKey& pkey, const uint64_t log_id,
    const int64_t timeout, int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& epoch_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition is not running now", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(log_id));
  } else {
    int64_t retry_cnt = 0;
    int64_t start_ts = ObTimeUtility::current_time();

    do {
      retry_cnt++;
      ret = OB_SUCCESS;
      if (OB_FAIL(clog_mgr_->query_log_info_with_log_id(pkey, log_id, accum_checksum, submit_timestamp, epoch_id))) {
        if (OB_EAGAIN != ret) {
          STORAGE_LOG(WARN, "fail to query log info", K(ret), K(pkey));
        } else {
          if (timeout > 0 && ObTimeUtility::current_time() > start_ts + timeout) {
            ret = OB_EAGAIN;
            break;
          }
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
            STORAGE_LOG(INFO, "retry to query log info", K(ret), K(pkey), K(log_id), K(retry_cnt));
          }
          usleep(10L * 1000L);
        }
      }
    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObPartitionService::query_range_to_macros(ObIAllocator& allocator, const common::ObPartitionKey& pkey,
    const ObIArray<common::ObStoreRange>& ranges, const int64_t type, uint64_t* macros_count,
    const int64_t* total_task_count, ObIArray<common::ObStoreRange>* splitted_ranges, ObIArray<int64_t>* split_index)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = nullptr;
  ObPGPartitionGuard pg_partition_guard;
  ObPGPartition* pg_partition = nullptr;
  STORAGE_LOG(DEBUG, "split_macros");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(pg->get_pg_partition(pkey, pg_partition_guard))) {
    LOG_WARN("failed to get pg partition", K(pkey), K(ret));
  } else if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg partition is null", K(ret), K(pkey));
  } else if (OB_ISNULL(pg_partition->get_storage())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition storage is null", K(ret), K(pkey));
  } else if (OB_FAIL(pg_partition->get_storage()->query_range_to_macros(
                 allocator, ranges, type, macros_count, total_task_count, splitted_ranges, split_index))) {
    STORAGE_LOG(WARN, "get scan cost failed", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::get_multi_ranges_cost(
    const common::ObPartitionKey& pkey, const common::ObIArray<common::ObStoreRange>& ranges, int64_t& total_size)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = nullptr;
  ObPGPartitionGuard pg_partition_guard;
  ObPGPartition* pg_partition = nullptr;
  STORAGE_LOG(DEBUG, "split_macros");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(pg->get_pg_partition(pkey, pg_partition_guard))) {
    LOG_WARN("failed to get pg partition", K(pkey), K(ret));
  } else if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg partition is null", K(ret), K(pkey));
  } else if (OB_ISNULL(pg_partition->get_storage())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition storage is null", K(ret), K(pkey));
  } else if (OB_FAIL(pg_partition->get_storage()->get_multi_ranges_cost(ranges, total_size))) {
    STORAGE_LOG(WARN, "Failed to get multi range cost", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::split_multi_ranges(const ObPartitionKey& pkey, const ObIArray<ObStoreRange>& ranges,
    const int64_t expected_task_count, ObIAllocator& allocator, ObArrayArray<ObStoreRange>& multi_range_split_array)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = nullptr;
  ObPGPartitionGuard pg_partition_guard;
  ObPGPartition* pg_partition = nullptr;
  STORAGE_LOG(DEBUG, "split_macros");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_FAIL(pg->get_pg_partition(pkey, pg_partition_guard))) {
    LOG_WARN("failed to get pg partition", K(pkey), K(ret));
  } else if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg partition is null", K(ret), K(pkey));
  } else if (OB_ISNULL(pg_partition->get_storage())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition storage is null", K(ret), K(pkey));
  } else if (OB_FAIL(pg_partition->get_storage()->split_multi_ranges(
                 ranges, expected_task_count, allocator, multi_range_split_array))) {
    STORAGE_LOG(WARN, "Failed to split multi ranges", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

bool ObPartitionService::is_working_partition(const common::ObPartitionKey& pkey)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;

  if (OB_FAIL(get_partition(pkey, guard))) {
    // partition does not exist
    // b_ret = false;
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (is_working_state(guard.get_partition_group()->get_partition_state())) {
    b_ret = true;
  }
  return b_ret;
}

bool ObPartitionService::is_service_started() const
{
  return GCTX.start_service_time_ != 0;
}

int ObPartitionService::get_global_max_decided_trans_version(int64_t& max_decided_trans_version) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else {
    ObSpinLockGuard guard(trans_version_lock_);
    max_decided_trans_version = global_max_decided_trans_version_;
  }

  return ret;
}

int ObPartitionService::handle_split_dest_partition_request(
    const ObSplitDestPartitionRequestArg& arg, ObSplitDestPartitionResult& result)
{
  int ret = OB_SUCCESS;
  enum ObSplitProgress progress = UNKNOWN_SPLIT_PROGRESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(split_dest_partition_(arg.dest_pkey_, arg.split_info_, progress))) {
    STORAGE_LOG(WARN, "split dest partition failed", K(ret), K(arg));
  } else {
  }
  result.status_ = ret;
  result.progress_ = progress;
  result.schema_version_ = arg.split_info_.get_schema_version();
  result.src_pkey_ = arg.split_info_.get_src_partition();
  result.dest_pkey_ = arg.dest_pkey_;
  // rewrite ret
  ret = OB_SUCCESS;
  return ret;
}

int ObPartitionService::handle_split_dest_partition_result(const ObSplitDestPartitionResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!result.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(result));
  } else if (OB_SUCCESS != result.status_) {
    STORAGE_LOG(WARN, "split dest partition failed", K(result));
    if (result.dest_pkey_.is_valid()) {
      const int64_t expire_renew_time = 0;
      (void)location_cache_->nonblock_renew(result.dest_pkey_, expire_renew_time);
    }
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(get_partition(result.src_pkey_, guard))) {
      STORAGE_LOG(WARN, "fail to get partition", K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
    } else if (OB_FAIL(partition->set_dest_partition_split_progress(
                   result.schema_version_, result.dest_pkey_, result.progress_))) {
      STORAGE_LOG(WARN, "fail to set split dest partition success", K(ret));
    } else {
      // do nothing
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "handle split dest partition result failed", K(ret), K(result));
  } else {
    STORAGE_LOG(INFO, "handle split dest partition result success", K(result));
  }
  return ret;
}

int ObPartitionService::handle_replica_split_progress_request(
    const ObReplicaSplitProgressRequest& arg, ObReplicaSplitProgressResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    int progress = UNKNOWN_SPLIT_PROGRESS;
    if (OB_FAIL(get_partition(arg.pkey_, guard))) {
      STORAGE_LOG(WARN, "fail to get partition", K(ret), K(arg));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
    } else if (OB_FAIL(partition->get_split_progress(arg.schema_version_, progress))) {
      STORAGE_LOG(WARN, "fail to get split progress", K(ret), K(arg));
    } else {
      result.pkey_ = arg.pkey_;
      result.addr_ = arg.addr_;
      result.progress_ = progress;
    }
  }
  return ret;
}

int ObPartitionService::handle_replica_split_progress_result(const ObReplicaSplitProgressResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!result.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(result));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(get_partition(result.pkey_, guard))) {
      STORAGE_LOG(WARN, "fail to get partition", K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
    } else if (OB_FAIL(partition->set_split_progress(result.addr_, result.progress_))) {
      STORAGE_LOG(WARN, "fail to set split progress", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartitionService::set_global_max_decided_trans_version(const int64_t trans_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else {
    ObSpinLockGuard guard(trans_version_lock_);
    if (trans_version > global_max_decided_trans_version_) {
      global_max_decided_trans_version_ = trans_version;
    }
  }

  return ret;
}

int ObPartitionService::append_local_sort_data(const common::ObPartitionKey& pkey,
    const share::ObBuildIndexAppendLocalDataParam& param, common::ObNewRowIterator& iter)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(param));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->append_local_sort_data(pkey, param, iter))) {
    STORAGE_LOG(WARN, "fail to append local sort data", K(ret), K(pkey), K(param));
  }
  return ret;
}

int ObPartitionService::append_sstable(const common::ObPartitionKey& pkey,
    const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(param));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->append_sstable(pkey, param, iter))) {
    STORAGE_LOG(WARN, "fail to append local sort data", K(ret), K(pkey), K(param));
  } else if (OB_FAIL(ObCompactToLatestTask::wait_compact_to_latest(pkey, param.index_id_))) {
    STORAGE_LOG(WARN, "fail to wait compact to latest", K(ret));
  } else {
    submit_pt_update_task_(pkey);
  }
  return ret;
}

bool ObPartitionService::is_election_candidate(const common::ObPartitionKey& pkey)
{
  bool is_candidate = true;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObMigrateStatus migrate_status = OB_MIGRATE_STATUS_MAX;

  if (OB_UNLIKELY(!is_inited_)) {
    tmp_ret = OB_NOT_INIT;
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(tmp_ret));
    }
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(tmp_ret), K(pkey));
  } else if (OB_SUCCESS != (tmp_ret = get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(tmp_ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    tmp_ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(tmp_ret), K(pkey));
  } else if (OB_SUCCESS != (tmp_ret = partition->get_pg_storage().get_pg_migrate_status(migrate_status))) {
    LOG_WARN("failed to get_migrate_status", K(tmp_ret), K(pkey));
  } else {
    // do nothing
  }
  if (OB_SUCCESS == tmp_ret) {
    if (ObMigrateStatusHelper::check_can_election(migrate_status)) {
      // do nothing
    } else {
      is_candidate = false;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("cur migrate status cannot as election candidate", K(pkey), K(migrate_status));
      } else {
        LOG_DEBUG("cur migrate status cannot as election candidate", K(pkey), K(migrate_status));
      }
    }
  }

  if (OB_SUCCESS != tmp_ret) {
    is_candidate = false;
    LOG_WARN("failed to check is_election_candidate", K(tmp_ret), K(pkey));
  }
  return is_candidate;
}

int ObPartitionService::split_partition(const ObSplitPartition& split_info, ObIArray<ObPartitionSplitProgress>& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (!split_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(result.reserve(split_info.get_spp_array().count()))) {
    STORAGE_LOG(ERROR, "reserve memory for split result failed", K(ret));
  } else {
    result.reset();
    const int64_t schema_version = split_info.get_schema_version();
    const ObIArray<ObSplitPartitionPair>& spp_array = split_info.get_spp_array();
    for (int64_t i = 0; OB_SUCCESS == ret && i < spp_array.count(); i++) {
      enum ObSplitProgress partition_progress = UNKNOWN_SPLIT_PROGRESS;
      const ObSplitPartitionPair& spp = spp_array.at(i);
      const ObPartitionKey& src_pkey = spp.get_source_pkey();
      if (OB_FAIL(split_source_partition_(src_pkey, schema_version, spp, partition_progress))) {
        STORAGE_LOG(WARN, "split source partition failed", K(ret), K(src_pkey));
      } else if (OB_FAIL(result.push_back(ObPartitionSplitProgress(src_pkey, partition_progress)))) {
        STORAGE_LOG(WARN, "push back result failed", K(ret), K(src_pkey));
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "split partition failed", K(ret), K(split_info));
  } else {
    STORAGE_LOG(INFO, "receive split partition request", K(split_info), K(result));
  }
  return ret;
}

int ObPartitionService::sync_split_source_log_success(
    const ObPartitionKey& pkey, const int64_t source_log_id, const int64_t source_log_ts)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (!pkey.is_valid() || !is_valid_log_id(source_log_id) || 0 >= source_log_ts) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(source_log_id), K(source_log_ts));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else {
    ret = partition->sync_split_source_log_success(source_log_id, source_log_ts);
  }
  if (OB_SUCCESS == ret) {
    STORAGE_LOG(INFO, "sync split source log callback success", K(pkey), K(source_log_id));
  } else {
    STORAGE_LOG(WARN, "sync split source log callback failed", K(ret), K(pkey), K(source_log_id));
  }
  return ret;
}

int ObPartitionService::sync_split_dest_log_success(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else {
    ret = partition->sync_split_dest_log_success();
  }
  if (OB_SUCCESS == ret) {
    STORAGE_LOG(INFO, "sync split dest log callback success", K(pkey));
  } else {
    STORAGE_LOG(WARN, "sync split dest log callback failed", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::split_dest_partition(
    const ObPartitionKey& pkey, const ObPartitionSplitInfo& split_info, enum ObSplitProgress& progress)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (!pkey.is_valid() || 0 >= split_info.get_split_version() || 0 > split_info.get_schema_version() ||
             !is_valid_log_id(split_info.get_source_log_id()) || !split_info.get_spp().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(split_info));
  } else {
    ret = split_dest_partition_(pkey, split_info, progress);
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "split dest partition failed", K(ret), K(pkey), K(split_info));
  } else if (PHYSICAL_SPLIT_FINISH != progress) {
    STORAGE_LOG(DEBUG, "receive split dest partition request", K(pkey), K(split_info));
  } else {
    STORAGE_LOG(INFO, "split dest partition success", K(pkey), K(split_info));
  }
  return ret;
}

int ObPartitionService::decode_log_type(const char* log, const int64_t size, int64_t& pos, ObStorageLogType& log_type)
{
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  if (OB_FAIL(serialization::decode_i64(log, size, pos, &tmp))) {
    STORAGE_LOG(WARN, "deserialize log_type failed", K(size), K(pos), K(ret));
  } else {
    log_type = static_cast<ObStorageLogType>(tmp);
  }
  return ret;
}

int ObPartitionService::split_source_partition_(const ObPartitionKey& pkey, const int64_t schema_version,
    const ObSplitPartitionPair& spp, enum ObSplitProgress& progress)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else if (OB_FAIL(E(EventTable::EN_BLOCK_SPLIT_SOURCE_PARTITION) OB_SUCCESS)) {
    STORAGE_LOG(WARN, "ERRSIM: EN_BLOCK_SPLIT_SOURCE_PARTITION", K(ret), K(pkey));
  } else {
    ret = partition->split_source_partition(schema_version, spp, progress);
  }
  return ret;
}

int ObPartitionService::split_dest_partition_(
    const ObPartitionKey& pkey, const ObPartitionSplitInfo& split_info, enum ObSplitProgress& progress)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else {
    ret = partition->split_dest_partition(split_info, progress);
  }
  return ret;
}

int ObPartitionService::calc_column_checksum(const common::ObPartitionKey& pkey, const uint64_t index_id,
    const int64_t schema_version, const uint64_t execution_id, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObRole role = common::INVALID_ROLE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == index_id || schema_version <= 0 ||
                         OB_INVALID_ID == execution_id || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(pkey),
        K(index_id),
        K(schema_version),
        K(execution_id),
        K(snapshot_version));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_role(role))) {
    STORAGE_LOG(WARN, "fail to get role", K(ret));
  } else if (!is_strong_leader(role)) {
    ret = OB_NOT_MASTER;
  } else {
    // schedule unique checking task
    ObGlobalUniqueIndexCallback* callback = NULL;
    ObUniqueCheckingDag* dag = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
      STORAGE_LOG(WARN, "fail to alloc dag", K(ret));
    } else if (OB_FAIL(
                   dag->init(pkey, this, schema_service_, index_id, schema_version, execution_id, snapshot_version))) {
      STORAGE_LOG(WARN, "fail to init ObUniqueCheckingDag", K(ret));
    } else if (OB_FAIL(dag->alloc_global_index_task_callback(pkey, index_id, callback))) {
      STORAGE_LOG(WARN, "fail to alloc global index task callback", K(ret));
    } else if (OB_FAIL(dag->alloc_unique_checking_prepare_task(callback))) {
      STORAGE_LOG(WARN, "fail to alloc unique checking prepare task", K(ret));
    } else if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        STORAGE_LOG(WARN, "fail to add dag to queue", K(ret));
      } else {
        ret = OB_EAGAIN;
      }
    }
    if (OB_FAIL(ret) && NULL != dag) {
      ObDagScheduler::get_instance().free_dag(*dag);
      dag = NULL;
    }
  }
  return ret;
}

int ObPartitionService::check_single_replica_major_sstable_exist(
    const ObPartitionKey& pkey, const uint64_t index_table_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == index_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(index_table_id));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->check_single_replica_major_sstable_exist(pkey, index_table_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(
          WARN, "fail to check self major sstable exist", K(ret), K(pkey), K(index_table_id), K(GCTX.self_addr_));
    }
  }
  return ret;
}

int ObPartitionService::check_single_replica_major_sstable_exist(
    const ObPartitionKey& pkey, const uint64_t index_table_id, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  timestamp = 0;
  if (OB_FAIL(check_single_replica_major_sstable_exist(pkey, index_table_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to check self major sstable exist", K(ret), K(pkey), K(index_table_id));
    }
  } else {
    timestamp = ObTimeUtility::current_time();
  }
  return ret;
}

int ObPartitionService::check_all_replica_major_sstable_exist(const ObPartitionKey& pkey, const uint64_t index_table_id)
{
  int ret = OB_SUCCESS;
  int64_t max_timestamp = 0;
  if (OB_FAIL(check_all_replica_major_sstable_exist(pkey, index_table_id, max_timestamp))) {
    STORAGE_LOG(WARN, "fail to check all replica major sstable exist", K(ret));
  }
  return ret;
}

int ObPartitionService::check_all_replica_major_sstable_exist(
    const ObPartitionKey& pkey, const uint64_t index_table_id, int64_t& max_timestamp)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObRole role = common::INVALID_ROLE;
  max_timestamp = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == index_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(index_table_id));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_role(role))) {
    STORAGE_LOG(WARN, "fail to get role", K(ret), K(pkey));
  } else if (!is_leader_by_election(role)) {
    ret = OB_NOT_MASTER;
  } else {
    // TODO(): hold check lock
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* table_schema = NULL;
    const ObTableSchema* index_schema = NULL;
    ObPartitionInfo partition_info;
    ObReplicaFilterHolder filter;
    ObArenaAllocator allocator(ObModIds::OB_BUILD_INDEX_SCHEDULER);
    int build_index_ret = OB_SUCCESS;
    ObArray<ObAddr> server_list;
    partition_info.set_allocator(&allocator);
    common::ObMemberList member_list;
    const uint64_t fetch_tenant_id =
        is_inner_table(index_table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_table_id);
    const bool need_fail_list = false;
    const int64_t cluster_id = OB_INVALID_ID;  // local cluster
    const bool filter_flag_replica = false;
    if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(fetch_tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey), K(index_table_id));
    } else if (OB_ISNULL(index_schema)) {
      // the index table is deleted, and all replicas are considered complete
      ret = OB_SUCCESS;
    } else if (OB_FAIL(schema_guard.get_table_schema(index_schema->get_data_table_id(), table_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), "data_table_id", index_schema->get_data_table_id());
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      STORAGE_LOG(WARN, "main table schema not exist while index table schema exists", K(ret));
    } else if (OB_FAIL(filter.filter_delete_server(ObAllServerTracer::get_instance()))) {
      STORAGE_LOG(WARN, "fail to set server trace", K(ret));
    } else if (OB_FAIL(filter.set_persistent_replica_status_not_equal(REPLICA_STATUS_OFFLINE))) {
      STORAGE_LOG(WARN, "fail to set replica status", K(ret));
    } else if (OB_FAIL(filter.set_filter_log_replica())) {
      STORAGE_LOG(WARN, "fail to set filter log replica", K(ret));
    } else if (OB_ISNULL(GCTX.pt_operator_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, pt operator must not be NULL", K(ret));
    } else if (OB_FAIL(GCTX.pt_operator_->get(pkey.get_table_id(),
                   pkey.get_partition_id(),
                   partition_info,
                   need_fail_list,
                   cluster_id,
                   filter_flag_replica))) {
      STORAGE_LOG(WARN, "fail to get partition info", K(ret), K(pkey));
    } else if (OB_FAIL(partition_info.filter(filter))) {
      STORAGE_LOG(WARN, "fail to filter partition", K(ret));
    } else if (partition_info.get_replicas_v2().empty()) {
      ret = OB_EAGAIN;
    } else if (OB_FAIL(get_curr_member_list(pkey, member_list))) {
      STORAGE_LOG(WARN, "fail to get curr member list", K(ret));
    } else {
      ObIArray<ObPartitionReplica>& replicas = partition_info.get_replicas_v2();
      for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
        ObPartitionReplica& replica = replicas.at(i);
        if (replicas.at(i).is_paxos_candidate()) {
          if (member_list.contains(replica.server_)) {
            if (OB_FAIL(server_list.push_back(replica.server_))) {
              STORAGE_LOG(WARN, "fail to push back server", K(ret));
            }
          }
        } else if (OB_FAIL(server_list.push_back(replica.server_))) {
          STORAGE_LOG(WARN, "fail to push back server", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
        obrpc::ObCheckSingleReplicaMajorSSTableExistArg arg;
        obrpc::ObCheckSingleReplicaMajorSSTableExistResult res;
        arg.pkey_ = pkey;
        arg.index_id_ = index_table_id;
        build_index_ret =
            GCTX.srv_rpc_proxy_->to(server_list.at(i)).check_single_replica_major_sstable_exist_with_time(arg, res);
        if (OB_TIMEOUT == build_index_ret) {
          ret = OB_EAGAIN;
        } else if (OB_SUCCESS != build_index_ret) {
          ret = build_index_ret;
          STORAGE_LOG(INFO,
              "partition build index failed",
              K(ret),
              K(index_table_id),
              "partition_id",
              partition_info.get_partition_id(),
              "current_index_status",
              index_schema->get_index_status(),
              "server",
              server_list.at(i),
              K(build_index_ret));
        }
        if (OB_SUCC(ret)) {
          max_timestamp = std::max(res.timestamp_, max_timestamp);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition->get_role(role))) {
        STORAGE_LOG(WARN, "fail to get role", K(ret), K(pkey));
      } else if (!is_leader_by_election(role)) {
        ret = OB_NOT_MASTER;
      }
    }
  }
  return ret;
}

int ObPartitionService::check_member_pg_major_sstable_enough(
    const common::ObPGKey& pg_key, const common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObPGStorage* pg_storage = NULL;
  ObRole role;
  ObTablesHandle tables_handle;
  hash::ObHashSet<uint64_t> major_table_id_set;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pg_key), K(table_ids));
  } else if (OB_FAIL(get_partition(pg_key, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pg_key));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret), K(pg_key));
  } else if (OB_FAIL(partition->get_role(role))) {
    STORAGE_LOG(WARN, "fail to get role", K(ret), K(pg_key));
  } else if (!is_leader_by_election(role)) {
    ret = OB_NOT_MASTER;
  } else if (OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "pg storage should not be NULL", K(ret), KP(pg_storage));
  } else if (OB_FAIL(pg_storage->get_last_all_major_sstable(tables_handle))) {
    STORAGE_LOG(WARN, "failed to get last all major sstable", K(ret), K(pg_key));
  } else if (tables_handle.empty() && table_ids.empty()) {
    // leader and follower both are empty pg, return success
  } else {
    const int64_t MAX_BUCKET = std::max(tables_handle.get_count(), table_ids.count());
    int hash_ret = OB_SUCCESS;
    if (OB_FAIL(major_table_id_set.create(MAX_BUCKET))) {
      STORAGE_LOG(WARN, "failed to create major table id set", K(ret), K(pg_key));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      const uint64_t table_id = table_ids.at(i);
      if (OB_FAIL(major_table_id_set.set_refactored(table_id))) {
        STORAGE_LOG(WARN, "failed to set table id into set", K(ret), K(table_id));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObITable* table = tables_handle.get_tables().at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, table must not be NULL", K(ret));
      } else {
        hash_ret = major_table_id_set.exist_refactored(table->get_key().table_id_);
        if (OB_HASH_EXIST == hash_ret) {
          // do noting
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          ret = OB_MAJOR_SSTABLE_NOT_EXIST;
          STORAGE_LOG(WARN, "major sstable not exist", K(major_table_id_set), K(table_ids));
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail to check exist table id from hash set", K(ret), K(table_ids.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::check_split_dest_partition_can_remove(
    const common::ObPartitionKey& key, const common::ObPartitionKey& pg_key, bool& can_remove)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard pg_guard;
  can_remove = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionService has not been inited", K(ret));
  } else if (!pg_key.is_pg() || key.is_pg() || !key.is_valid() || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argrument", K(ret), K(key), K(pg_key));
  } else if (OB_FAIL(get_partition(pg_key, pg_guard)) || OB_ISNULL(pg_guard.get_partition_group())) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_key));
  } else {
    const ObPartitionSplitInfo& split_info = pg_guard.get_partition_group()->get_split_info();
    if (!split_info.is_valid()) {
      // no split info
      can_remove = true;
    } else if (pg_key == split_info.get_src_partition()) {
      // source partition
      can_remove = true;
    } else if (OB_FAIL(check_split_source_partition_exist(split_info.get_src_partition(), key.get_table_id()))) {
      if (OB_PG_PARTITION_NOT_EXIST == ret || OB_PARTITION_NOT_EXIST == ret) {
        can_remove = true;
        LOG_INFO("pg or partition not exist, can remove dest partition", K(ret), K(split_info), K(key));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to check pg partition exist", K(ret), K(pg_key), K(key));
      }
    } else {
      can_remove = false;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO(
            "source partition already exist, not allow to gc dest partition", K(ret), K(pg_key), K(key), K(split_info));
      }
    }
  }
  return ret;
}

int ObPartitionService::check_all_partition_sync_state(const int64_t switchover_epoch)
{
  int ret = OB_SUCCESS;
  int64_t fail_count = 0;
  ObIPartitionGroupIterator* iter = NULL;

  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService not inited", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret));
  } else if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionLogService* pls = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "scan next partition failed", K(ret));
        }
      } else if (OB_UNLIKELY(NULL == partition)) {
        STORAGE_LOG(WARN, "get partition return NULL");
      } else if (NULL == (pls = partition->get_log_service())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get_log_service return NULL");
      } else {
        int64_t local_schema_version = OB_INVALID_VERSION;
        int64_t pg_create_schema_version = OB_INVALID_VERSION;
        const common::ObPartitionKey pkey = partition->get_partition_key();
        const uint64_t tenant_id_for_get_schema =
            is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();
        int tmp_ret = OB_SUCCESS;
        bool is_sync = false;

        if (OB_FAIL(partition->get_pg_storage().get_create_schema_version(pg_create_schema_version))) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            STORAGE_LOG(WARN, "fail to get create schema version for pg", K(ret), K(pkey));
          }
        } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_for_get_schema, local_schema_version))) {
          STORAGE_LOG(WARN, "fail to get schema version", K(ret), K(pkey), K(tenant_id_for_get_schema));
        } else if (pg_create_schema_version > local_schema_version ||
                   !share::schema::ObSchemaService::is_formal_version(local_schema_version)) {
          STORAGE_LOG(INFO,
              "new partition group, schema is not flushed",
              K(pkey),
              K(local_schema_version),
              K(pg_create_schema_version));
        } else if (!ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
          // The replica to be synchronized across clusters must also check switchover_epoch
          if (OB_SUCCESS != (tmp_ret = pls->is_log_sync_with_primary(switchover_epoch, is_sync))) {
            STORAGE_LOG(WARN, "is_log_sync_with_primary failed", K(tmp_ret), K(pkey));
          }
        } else {
          // The replica that does not need to be synchronized across clusters
          is_sync = true;
        }

        if (OB_SUCC(ret)) {
          if (!is_sync) {
            STORAGE_LOG(INFO, "this partition is not sync with leader, need check schema", K(pkey), K(ret));
          }

          bool is_dropped = false;
          bool check_dropped_partition = true;
          if (!is_sync) {
            // check whether the partition has been dropped during unsync
            ObPartitionArray pkeys;
            if (OB_SUCCESS != (tmp_ret = partition->get_all_pg_partition_keys(pkeys))) {
              STORAGE_LOG(WARN, "get all pg partition keys error", K(tmp_ret), K(pkey));
            } else if (!partition->is_pg()) {
              // dealing with stand alone partition
              if (OB_SUCCESS !=
                  (tmp_ret = schema_guard.check_partition_can_remove(
                       pkey.get_table_id(), pkey.get_partition_id(), check_dropped_partition, is_dropped))) {
                STORAGE_LOG(WARN, "fail to check partition exist", K(tmp_ret), K(pkey));
              }
            } else {
              // The deletion of PG will only be judged after all the partitions in PG are completed by gc
              if (OB_SUCCESS !=
                  (tmp_ret = schema_guard.check_partition_can_remove(
                       pkey.get_tablegroup_id(), pkey.get_partition_group_id(), check_dropped_partition, is_dropped))) {
                STORAGE_LOG(WARN, "fail to check partition group exist", K(tmp_ret), K(pkey));
              }
            }
          }

          if (!is_sync) {
            if (is_dropped) {
              STORAGE_LOG(INFO, "this unsync partition has been dropped, ignore", K(pkey));
            } else {
              fail_count++;
            }
          }
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        fail_count++;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  if (OB_SUCC(ret)) {
    if (fail_count > 0) {
      ret = OB_PARTIAL_FAILED;
    }
  }
  STORAGE_LOG(INFO, "check_all_partition_sync_state finished", K(ret), K(fail_count), K(switchover_epoch));
  return ret;
}

int ObPartitionService::send_leader_max_log_info()
{
  int ret = OB_SUCCESS;
  int64_t fail_count = 0;
  ObIPartitionGroupIterator* iter = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService not inited", K(ret));
  } else if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionLogService* pls = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "scan next partition failed", K(ret));
        }
      } else if (OB_UNLIKELY(NULL == partition)) {
        STORAGE_LOG(WARN, "get partition return NULL");
      } else if (NULL == (pls = partition->get_log_service())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get_log_service return NULL");
      } else {
        int tmp_ret = OB_SUCCESS;
        const common::ObPartitionKey pkey = partition->get_partition_key();
        if (!ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
          // only replica that are synchronized across clusters need to be executed
          if (OB_SUCCESS != (tmp_ret = pls->leader_send_max_log_info())) {
            STORAGE_LOG(WARN, "leader_send_max_log_info failed", K(tmp_ret), K(pkey));
          }
        }
        if (OB_SUCCESS != tmp_ret) {
          fail_count++;
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        fail_count++;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  if (OB_SUCC(ret)) {
    if (fail_count > 0) {
      ret = OB_PARTIAL_FAILED;
    }
  }
  STORAGE_LOG(INFO, "send_leader_max_log_info finished", K(ret), K(fail_count));
  return ret;
}

ObPartitionMigrationDataStatics::ObPartitionMigrationDataStatics()
    : total_macro_block_(0),
      ready_macro_block_(0),
      major_count_(0),
      mini_minor_count_(0),
      normal_minor_count_(0),
      buf_minor_count_(0),
      reuse_count_(0),
      partition_count_(0),
      finish_partition_count_(0),
      input_bytes_(0),
      output_bytes_(0)
{}

void ObPartitionMigrationDataStatics::reset()
{
  total_macro_block_ = 0;
  ready_macro_block_ = 0;
  major_count_ = 0;
  mini_minor_count_ = 0;
  normal_minor_count_ = 0;
  buf_minor_count_ = 0;
  reuse_count_ = 0;
  partition_count_ = 0;
  finish_partition_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
}

ObRestoreInfo::ObRestoreInfo()
    : is_inited_(false), arg_(), backup_sstable_info_map_(), allocator_(ObModIds::OB_PARTITION_MIGRATOR)
{}

ObRestoreInfo::~ObRestoreInfo()
{
  if (backup_sstable_info_map_.size() > 0) {
    for (SSTableInfoMap::iterator iter = backup_sstable_info_map_.begin(); iter != backup_sstable_info_map_.end();
         ++iter) {
      common::ObArray<blocksstable::ObSSTablePair>* info = iter->second;
      if (NULL != info) {
        info->~ObArray();
      }
    }
  }
  is_inited_ = false;
}

int ObRestoreInfo::init(const share::ObRestoreArgs& restore_args)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (!restore_args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(restore_args));
  } else if (OB_FAIL(backup_sstable_info_map_.create(2 * OB_MAX_INDEX_PER_TABLE, ObModIds::OB_PARTITION_MIGRATOR))) {
    STORAGE_LOG(WARN, "failed to create sstable info map", K(ret));
  } else {
    is_inited_ = true;
    arg_ = restore_args;
    // TODO()
    if (OB_FAIL(arg_.schema_id_list_.push_back(arg_.schema_id_pair_))) {
      STORAGE_LOG(WARN, "failed to push schema id pair into array", K(ret), K(arg_));
    }
  }
  return ret;
}

int ObRestoreInfo::add_sstable_info(const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& block_list)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObArray<blocksstable::ObSSTablePair>* new_block_list = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (NULL != backup_sstable_info_map_.get(index_id)) {
    ret = OB_ENTRY_EXIST;
    STORAGE_LOG(WARN, "sstable info has exist", K(ret), K(index_id));
  } else if (NULL == (buf = allocator_.alloc(sizeof(ObArray<blocksstable::ObSSTablePair>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
  } else if (NULL == (new_block_list = new (buf) ObArray<blocksstable::ObSSTablePair>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to new block list", K(ret));
  } else if (OB_FAIL(new_block_list->assign(block_list))) {
    STORAGE_LOG(WARN, "failed to copy block list", K(ret));
  } else if (OB_FAIL(backup_sstable_info_map_.set_refactored(index_id, new_block_list))) {
    STORAGE_LOG(WARN, "failed to set block list", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to add sstable info", K(index_id));
    new_block_list = NULL;
  }

  if (NULL != new_block_list) {
    new_block_list->~ObArray();
  }
  return ret;
}

int ObRestoreInfo::get_backup_block_info(const uint64_t index_id, const int64_t macro_idx, uint64_t& backup_index_id,
    blocksstable::ObSSTablePair& backup_block_pair) const
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::ObSSTablePair>* backup_block_list = NULL;
  // block_pair.data_seq_ is the index in block_list

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(arg_.trans_to_backup_schema_id(index_id, backup_index_id))) {
    STORAGE_LOG(WARN, "failed to trans_to_backup_index_id", K(ret), K(index_id));
  } else if (OB_FAIL(backup_sstable_info_map_.get_refactored(backup_index_id, backup_block_list))) {
    STORAGE_LOG(WARN, "failed to get index id", K(ret), K(backup_index_id));
  } else if (NULL == backup_block_list) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "block list must not null", K(ret));
  } else if (macro_idx < 0 || macro_idx >= backup_block_list->count()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid backup block pair", K(ret), K(macro_idx), K(backup_block_list->count()));
  } else {
    backup_block_pair = backup_block_list->at(macro_idx);
  }
  return ret;
}

void ObReplicaOpArg::reset()
{
  key_.reset();
  src_.reset();
  dst_.reset();
  data_src_.reset();
  type_ = UNKNOWN_REPLICA_OP;
  quorum_ = 0;
  base_version_.reset();
  restore_arg_.reset();
  index_id_ = OB_INVALID_ID;
  priority_ = ObReplicaOpPriority::PRIO_INVALID;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  change_member_option_ = NORMAL_CHANGE_MEMBER_LIST;
  switch_epoch_ = OB_INVALID_VERSION;
}

bool ObReplicaOpArg::is_physical_restore() const
{
  return RESTORE_VERSION_1 == restore_version_;
}

bool ObReplicaOpArg::is_physical_restore_leader() const
{
  return RESTORE_REPLICA_OP == type_ && RESTORE_VERSION_1 == restore_version_;
}

bool ObReplicaOpArg::is_physical_restore_follower() const
{
  return RESTORE_FOLLOWER_REPLICA_OP == type_ && RESTORE_VERSION_1 == restore_version_;
}

bool ObReplicaOpArg::is_FtoL() const
{
  return CHANGE_REPLICA_OP == type_ && ObReplicaType::REPLICA_TYPE_FULL == src_.get_replica_type() &&
         ObReplicaType::REPLICA_TYPE_LOGONLY == dst_.get_replica_type();
}

bool ObReplicaOpArg::is_standby_restore() const
{
  return RESTORE_STANDBY_OP == type_;
}

int ObPartitionService::handle_ha_gts_ping_request(
    const obrpc::ObHaGtsPingRequest& request, obrpc::ObHaGtsPingResponse& response)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(gts_mgr_.handle_ping_request(request, response))) {
    STORAGE_LOG(WARN, "handle_ping_request failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::handle_ha_gts_get_request(const obrpc::ObHaGtsGetRequest& request)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(gts_mgr_.handle_get_request(request))) {
    STORAGE_LOG(WARN, "handle_get_request failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::handle_ha_gts_get_response(const obrpc::ObHaGtsGetResponse& response)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!response.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(response));
  } else if (OB_FAIL(gts_source_.handle_get_response(response))) {
    STORAGE_LOG(WARN, "handle_get_response failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::handle_ha_gts_heartbeat(const obrpc::ObHaGtsHeartbeat& heartbeat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!heartbeat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(heartbeat));
  } else if (OB_FAIL(gts_mgr_.handle_heartbeat(heartbeat))) {
    STORAGE_LOG(WARN, "handle_heartbeat failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::handle_ha_gts_update_meta(
    const obrpc::ObHaGtsUpdateMetaRequest& request, obrpc::ObHaGtsUpdateMetaResponse& response)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(gts_mgr_.handle_update_meta(request, response))) {
    STORAGE_LOG(WARN, "handle_update_meta failed", K(ret), K(request));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::handle_ha_gts_change_member(
    const obrpc::ObHaGtsChangeMemberRequest& request, obrpc::ObHaGtsChangeMemberResponse& response)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(gts_mgr_.handle_change_member(request, response))) {
    STORAGE_LOG(WARN, "handle_change_member failed", K(ret), K(request));
  } else {
    // do nothing
  }
  response.set(ret);
  return ret;
}

int ObPartitionService::send_ha_gts_get_request(const common::ObAddr& server, const obrpc::ObHaGtsGetRequest& request)
{
  return srv_rpc_proxy_->to(server)
      .by(OB_SERVER_TENANT_ID)
      .timeout(gts::ObHaGts::HA_GTS_RPC_TIMEOUT)
      .ha_gts_get_request(request, NULL);
}

int ObPartitionService::get_gts(const uint64_t tenant_id, const MonotonicTs stc, int64_t& gts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (!is_valid_no_sys_tenant_id(tenant_id) || !stc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(stc));
  } else if (OB_FAIL(gts_source_.get_gts(tenant_id, stc, gts)) && OB_EAGAIN != ret && OB_NOT_SUPPORTED != ret) {
    STORAGE_LOG(WARN, "gts_source_ get_gts failed", K(ret), K(tenant_id), K(stc));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionService::trigger_gts()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", K(ret));
  } else if (GCONF._enable_ha_gts_full_service && OB_FAIL(gts_source_.trigger_gts())) {
    STORAGE_LOG(WARN, "gts_source_ trigger_gts failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

// flag: ObReplicaRestoreStatus
int ObPartitionService::set_restore_flag(const ObPartitionKey& pkey, const int16_t flag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_merged = false;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObPhysicalRestoreInfo restore_info;
  int64_t restore_snapshot_version = OB_INVALID_TIMESTAMP;
  int64_t restore_schema_version = OB_INVALID_TIMESTAMP;
  const bool need_valid_restore_schema_version = !(is_inner_table(pkey.get_table_id()));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "Fail to get partition, ", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group()) || (!partition->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition is invalid", K(ret), K(pkey));
  } else if (REPLICA_NOT_RESTORE == flag || REPLICA_LOGICAL_RESTORE_DATA == flag) {
    //  skip set restore snapshot version for not physical restore
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_info(
                 need_valid_restore_schema_version, pkey.get_tenant_id(), restore_info))) {
    LOG_WARN("failed to get restore info", K(ret), K(pkey));
  } else {
    restore_snapshot_version = restore_info.restore_snapshot_version_;
    restore_schema_version = restore_info.restore_schema_version_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_condition_before_set_restore_flag_(pkey, flag, restore_snapshot_version))) {
    LOG_WARN("failed to check_condition_before_set_restore_flag_", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_pg_storage().set_restore_flag(
                 flag, restore_snapshot_version, restore_schema_version))) {
    STORAGE_LOG(WARN, "pg storage set restore fail", K(ret), K(pkey), K(restore_snapshot_version));
  } else {
    if (share::REPLICA_RESTORE_DUMP_MEMTABLE == flag) {
      if (OB_SUCCESS != (tmp_ret = minor_freeze(pkey))) {
        STORAGE_LOG(WARN, "fail to freeze partition after restore", K(tmp_ret), K(pkey));
      } else if (OB_SUCCESS != (tmp_ret = ObPartitionScheduler::get_instance().schedule_merge(pkey, is_merged))) {
        STORAGE_LOG(WARN, "schedule merge failed", K(tmp_ret), K(pkey));
      } else if (is_merged && OB_SUCCESS != (tmp_ret = partition->get_pg_storage().check_release_memtable(*rs_cb_))) {
        STORAGE_LOG(WARN, "failed to check release memtable", K(tmp_ret), K(pkey));
      }
    }
    submit_pt_update_task_(pkey);  // update all_root_table:is_restore
    STORAGE_LOG(INFO, "set restore flag finish", K(ret), K(pkey), K(flag));
  }
  return ret;
}

int ObPartitionService::get_restore_replay_info(const ObPartitionKey& pkey, uint64_t& last_restore_log_id,
    int64_t& last_restore_log_ts, int64_t& restore_snapshot_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_merged = false;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "Fail to get partition, ", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group()) || (!partition->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition is invalid", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_pg_storage().get_restore_replay_info(
                 last_restore_log_id, last_restore_log_ts, restore_snapshot_version))) {
    STORAGE_LOG(WARN, "failed to get_restore_replay_info", K(ret), K(pkey));
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionService::set_restore_snapshot_version_for_trans(
    const ObPartitionKey& pkey, const int64_t restore_snapshot_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret), K(pkey));
  } else if (OB_FAIL(txs_->set_restore_snapshot_version(pkey, restore_snapshot_version))) {
    STORAGE_LOG(WARN, "failed to set_restore_snapshot_version", KR(ret), K(pkey), K(restore_snapshot_version));
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionService::get_offline_log_id(const ObPartitionKey& pkey, uint64_t offline_log_id) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "Fail to get partition, ", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = guard.get_partition_group()) || (!partition->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition is invalid", K(ret), K(pkey));
  } else {
    offline_log_id = partition->get_offline_log_id();
  }
  return ret;
}

// used to set member_list for the private table in the process of repeating the tenant building of the standby database
int ObPartitionService::set_member_list(
    const obrpc::ObSetMemberListBatchArg& arg, obrpc::ObCreatePartitionBatchRes& result)
{
  int ret = OB_SUCCESS;
  common::ObSArray<int>& ret_array = result.ret_list_;
  result.timestamp_ = arg.timestamp_;
  const common::ObSArray<ObSetMemberListArg>& batch_arg = arg.args_;
  LOG_INFO("start set_member_list", K(ret), "partition_count", batch_arg.count());
  ret_array.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (batch_arg.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), "count", batch_arg.count());
  } else if (!clog_mgr_->is_scan_finished()) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "scanning disk log has not been finished, cannot set_member_list", K(ret));
  } else if (OB_FAIL(ret_array.reserve(batch_arg.count()))) {
    STORAGE_LOG(WARN, "reserver res array failed, ", K(ret));
  } else {
    int64_t fail_count = 0;
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    ObIPartitionLogService* pls = NULL;
    int tmp_ret = OB_SUCCESS;
    const int64_t initial_ms_log_id = 1;     // set to 1 for private table in standby cluster
    const int64_t initial_mc_timestamp = 1;  // set to 1 for private table in standby cluster
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.count(); ++i) {
      const ObSetMemberListArg& cur_arg = batch_arg.at(i);
      const ObPartitionKey pkey = cur_arg.key_;
      const ObMemberList& member_list = cur_arg.member_list_;
      const int64_t replica_num = cur_arg.quorum_;
      const common::ObAddr leader = cur_arg.leader_;
      const int64_t lease_start = cur_arg.lease_start_;
      common::ObProposalID fake_ms_proposal_id;

      if (OB_SUCCESS != (tmp_ret = get_partition(pkey, guard))) {
        STORAGE_LOG(WARN, "get partition failed, ", K(pkey), K(tmp_ret));
      } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected error, the partition is NULL, ", K(pkey), K(tmp_ret));
      } else if (NULL == (pls = partition->get_log_service())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get_log_service failed", K(tmp_ret));
      } else if (OB_SUCCESS !=
                 (tmp_ret = partition->try_update_clog_member_list(
                      initial_ms_log_id, initial_mc_timestamp, replica_num, member_list, fake_ms_proposal_id))) {
        // Persist member_list to slog for private table in standby cluster
        STORAGE_LOG(WARN, "try_update_clog_member_list failed", K(tmp_ret), K(pkey));
      } else if (OB_SUCCESS != (tmp_ret = pls->set_member_list(member_list, replica_num, leader, lease_start))) {
        STORAGE_LOG(WARN, "set_member_list failed", K(tmp_ret), K(pkey), K(cur_arg));
      } else if (pls->is_archive_restoring() && OB_FAIL(set_restore_flag(pkey, share::REPLICA_RESTORE_MEMBER_LIST))) {
        STORAGE_LOG(WARN, "set_restore_flag failed", K(ret), K(pkey));
      } else {
        // do nothing
      }
      if (OB_FAIL(ret_array.push_back(tmp_ret))) {
        LOG_WARN("failed to push back ret", K(tmp_ret));
      }
      if (OB_SUCCESS != tmp_ret) {
        fail_count++;
      }
    }

    if (OB_SUCC(ret)) {
      if (fail_count > 0) {
        ret = OB_PARTIAL_FAILED;
      }
    }
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_SET_MEMBER_LIST_FAIL) OB_SUCCESS;
    }
    STORAGE_LOG(INFO, "set_member_list finished", K(ret), K(batch_arg), K(ret_array));
  }

  return ret;
}

int ObPartitionService::get_role_and_leader_epoch(
    const common::ObPartitionKey& pkey, common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time) const
{
  int ret = OB_SUCCESS;
  role = INVALID_ROLE;
  leader_epoch = OB_INVALID_TIMESTAMP;
  takeover_time = OB_INVALID_TIMESTAMP;

  ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition log service failed", K(pkey), K(ret));
  } else if (OB_FAIL(log_service->get_role_and_leader_epoch(role, leader_epoch, takeover_time))) {
    STORAGE_LOG(WARN, "failed to get role and leader epoch", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::check_physical_flashback_succ(
    const obrpc::ObCheckPhysicalFlashbackArg& arg, obrpc::ObPhysicalFlashbackResultArg& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_ERROR;
    LOG_WARN("partition service is not running", K(ret));
  } else {
    ObIPartitionGroupIterator* iter = NULL;
    if (OB_ISNULL(iter = ObPartitionService::get_instance().alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
    } else {
      ObIPartitionGroup* partition = NULL;
      result.enable_result_ = true;
      result.min_version_ = INT64_MAX;
      result.max_version_ = INT64_MIN;
      while (OB_SUCC(ret)) {
        obrpc::ObPhysicalFlashbackResultArg tmp_result;
        tmp_result.max_version_ = INT64_MIN;
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "scan next partition failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
          break;
        } else if (OB_ISNULL(partition)) {
          ret = OB_PARTITION_NOT_EXIST;
          STORAGE_LOG(WARN, "get partition failed", K(ret));
        } else if (OB_FAIL(partition->check_physical_flashback_succ(arg, result.max_version_, tmp_result))) {
          STORAGE_LOG(WARN, "failed to physical flashback succ", K(ret), K(arg));
        } else {
          result.enable_result_ = result.enable_result_ && tmp_result.enable_result_;
          result.min_version_ = min(result.min_version_, tmp_result.min_version_);
          result.max_version_ = max(result.max_version_, tmp_result.max_version_);
        }
      }
      ObPartitionService::get_instance().revert_pg_iter(iter);
    }
  }
  STORAGE_LOG(INFO, "check_physical_flashback", K(ret), K(arg), K(result));
  return ret;
}

int ObPartitionService::try_freeze_aggre_buffer(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionLogService* pls = NULL;
  if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    STORAGE_LOG(WARN, "Unexpected error, the partition is NULL", K(pkey), K(ret));
  } else if (OB_ISNULL(pls = partition->get_log_service())) {
    STORAGE_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_FAIL(pls->try_freeze_aggre_buffer())) {
    STORAGE_LOG(WARN, "try freeze aggre buffer failed", K(ret), K(pkey));
  }
  return ret;
}

// During the indexing process, check whether the pkey is in the PG, if not, need to retry the indexing and wait
int ObPartitionService::check_pg_partition_exist(const ObPGKey& pg_key, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPGKey tmp_pg_key;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (!pg_key.is_valid() || !pg_key.is_pg() || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(pkey));
  } else {
    ObIPartitionGroupGuard guard;
    ObPGPartitionGuard pg_partition_guard;
    if (OB_FAIL(get_partition(pg_key, guard))) {
      if (OB_PARTITION_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "get partition failed", K(ret), K(pg_key), K(pkey));
      }
    } else if (OB_UNLIKELY(NULL == guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pg_key), K(pkey));
    } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(pkey, pg_partition_guard))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K(pg_key), K(pkey));
      } else {
        ret = OB_PG_PARTITION_NOT_EXIST;
      }
    } else if (OB_FAIL(pg_index_.get_pg_key(pkey, tmp_pg_key))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "get pg key error", K(ret), K(pkey), K(lbt()));
      } else {
        ret = OB_PG_PARTITION_NOT_EXIST;
      }
    }
  }

  return ret;
}

// target partition must be droped after source partition in a split
int ObPartitionService::check_split_source_partition_exist(const ObPGKey& pg_key, const int64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else if (!pg_key.is_valid() || !pg_key.is_pg() || is_tablegroup_id(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(table_id));
  } else {
    ObIPartitionGroupGuard guard;
    ObPartitionArray pkeys;
    if (OB_FAIL(get_partition(pg_key, guard))) {
      if (OB_PARTITION_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "get partition failed", K(ret), K(pg_key));
      }
    } else if (OB_UNLIKELY(NULL == guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pg_key), K(table_id));
    } else if (OB_FAIL(guard.get_partition_group()->get_all_pg_partition_keys(pkeys))) {
      STORAGE_LOG(WARN, "fail to get all pg partition", K(ret), K(pg_key));
    } else {
      // do nothing
      ret = OB_PG_PARTITION_NOT_EXIST;
      for (int64_t i = 0; OB_PG_PARTITION_NOT_EXIST && i < pkeys.count(); ++i) {
        if (table_id == pkeys.at(i).get_table_id()) {
          ret = OB_SUCCESS;
          break;
        }
      }
    }
  }

  return ret;
}

int ObPartitionService::try_inc_total_partition_cnt(const int64_t new_partition_cnt, const bool need_check)
{
  int ret = OB_SUCCESS;
  if (new_partition_cnt != 0) {
    int64_t partition_cnt = ATOMIC_AAF(&total_partition_cnt_, new_partition_cnt);
    if (need_check && GCONF._max_partition_cnt_per_server < partition_cnt) {
      ret = OB_TOO_MANY_PARTITIONS_ERROR;
      LOG_WARN("too many partition count on this server");
      ATOMIC_SAF(&total_partition_cnt_, new_partition_cnt);
    }
  }
  return ret;
}

int ObPartitionService::check_tenant_pg_exist(const uint64_t tenant_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the partition service has not been inited", K(ret));
  } else {
    ObIPartitionGroupIterator* iter = NULL;
    if (NULL == (iter = alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
    } else {
      ObIPartitionGroup* partition = NULL;

      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "scan next partition failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
          break;
        } else if (OB_UNLIKELY(NULL == partition)) {
          ret = OB_PARTITION_NOT_EXIST;
          STORAGE_LOG(WARN, "get partition failed", K(ret));
        } else if (partition->get_partition_key().get_tenant_id() == tenant_id) {
          is_exist = true;
          break;
        }
      }

      revert_pg_iter(iter);
    }

    if (OB_SUCC(ret) && !is_exist) {
      if (OB_FAIL(ObPGMemoryGarbageCollector::get_instance().check_tenant_pg_exist(tenant_id, is_exist))) {
        STORAGE_LOG(WARN, "fail to check tenant pg exist", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObPartitionService::acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(table_key));
  } else if (OB_FAIL(get_partition(table_key.pkey_, guard))) {
    STORAGE_LOG(WARN, "fail to get partition group", K(ret), K(table_key));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "error sys, partition group must not be null", K(ret));
  } else if (OB_FAIL(pg->acquire_sstable(table_key, table_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to acquire sstable", K(ret), K(table_key));
    }
  }
  return ret;
}

int ObPartitionService::check_dirty_txn(
    const ObPartitionKey& pkey, const int64_t min_log_ts, const int64_t max_log_ts, int64_t& freeze_ts, bool& is_dirty)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "partition is not running now", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", K(ret), K(pkey));
  } else {
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
    } else if (OB_FAIL(partition->check_dirty_txn(min_log_ts, max_log_ts, freeze_ts, is_dirty))) {
      STORAGE_LOG(WARN, "check dirty txn failed", K(ret), K(pkey), K(min_log_ts), K(max_log_ts));
    }
  }

  return ret;
}

int ObPartitionService::get_create_pg_param(const obrpc::ObCreatePartitionArg& arg, const ObSavedStorageInfoV2& info,
    const int64_t data_version, const bool write_pg_slog, const ObPartitionSplitInfo& split_info,
    const int64_t split_state, ObStorageFileHandle* file_handle, ObBaseFileMgr* file_mgr, ObCreatePGParam& param)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid() || !info.is_valid() || OB_ISNULL(file_mgr) || OB_ISNULL(file_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get create pg param get invalid argument", K(ret), K(arg), K(info), KP(file_handle), KP(file_mgr));
  } else if (OB_FAIL(param.set_storage_info(info))) {
    LOG_WARN("failed to set storage info", K(ret), K(info));
  } else if (OB_FAIL(param.set_split_info(split_info))) {
    LOG_WARN("failed to set split info", K(ret), K(split_info));
  } else {
    param.create_timestamp_ = arg.lease_start_;
    param.data_version_ = data_version;
    param.is_restore_ = arg.restore_;
    param.replica_property_ = arg.replica_property_;
    param.replica_type_ = arg.replica_type_;
    param.split_state_ = split_state;
    param.write_slog_ = write_pg_slog;
    param.file_handle_ = file_handle;
    param.file_mgr_ = file_mgr;
    param.create_frozen_version_ = data_version;
  }
  return ret;
}

int ObPartitionService::clean_all_clog_files_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(clog_mgr_->delete_all_log_files())) {
    STORAGE_LOG(ERROR, "[PHY_FLASHBACK]delete_all_log_files failed", K(ret));
  } else {
    STORAGE_LOG(INFO, "[PHY_FLASHBACK]delete_all_log_files success");
  }
  return ret;
}

int ObPartitionService::start_physical_flashback()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "[PHY_FLASHBACK]start physical flashback");
  const bool is_physical_flashback = true;
  if (!GCTX.is_in_phy_fb_mode() && GCTX.flashback_scn_ <= 0) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN,
        "[PHY_FLASHBACK]observer do not in physic flashback mode, can not flashback",
        K(ret),
        K(GCTX.is_in_phy_fb_mode()));
  } else if (OB_FAIL(OB_STORE_FILE.open(is_physical_flashback))) {
    STORAGE_LOG(ERROR, "[PHY_FLASHBACK]fail to open store file, ", K(ret));
  } else if (OB_FAIL(check_can_physical_flashback_())) {
    STORAGE_LOG(ERROR, "[PHY_FLASHBACK]failed to check_can_physical_flashback_", K(ret));
  } else if (OB_FAIL(physical_flashback())) {
    STORAGE_LOG(ERROR, "[PHY_FLASHBACK]failed to phsical flashback", K(ret));
  } else if (OB_FAIL(clean_all_clog_files_())) {
    STORAGE_LOG(ERROR, "[PHY_FLASHBACK]failed to clean_all_clog_files_", K(ret));
  } else {
    // do nothing
  }

  STORAGE_LOG(INFO, "[PHY_FLASHBACK]finish physical flashback", K(ret));
  return ret;
}

int ObPartitionService::physical_flashback()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;

  if (!GCTX.is_in_phy_fb_mode()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(
        WARN, "observer do not in physical flashback mode, can not flashback", K(ret), K(GCTX.is_in_phy_fb_mode()));
  } else if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "cannot allocate scan iterator.", K(ret));
  } else {
    // flash backup
    while (OB_SUCC(ret)) {
      bool need_remove_pg = false;
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "get next partition failed.", K(ret));
        }
      } else if (NULL == partition) {
        ret = OB_PARTITION_NOT_EXIST;
        STORAGE_LOG(WARN, "get partition failed", K(ret));
      } else if (OB_FAIL(check_flashback_need_remove_pg(GCTX.flashback_scn_, partition, need_remove_pg))) {
        LOG_WARN("failed to check flashback need remove pg", K(ret), K(GCTX.flashback_scn_));
      } else if (need_remove_pg) {
        if (OB_FAIL(remove_flashback_unneed_pg(partition))) {
          LOG_WARN("failed to remove flashback unneed pg", K(ret));
        }
      } else if (OB_FAIL(partition->physical_flashback(GCTX.flashback_scn_))) {
        STORAGE_LOG(WARN,
            "[PHY_FLASHBACK]failed to physical_flashback",
            K(ret),
            "pkey",
            partition->get_partition_key(),
            K(GCTX.flashback_scn_));
      } else {
        STORAGE_LOG(INFO,
            "[PHY_FLASHBACK]partition physical_flashback success",
            "pkey",
            partition->get_partition_key(),
            K(GCTX.flashback_scn_));
      }
    }
    // write checkpoint
    if (OB_SUCC(ret)) {
      common::ObLogCursor cur_cursor;
      if (OB_FAIL(ObServerCheckpointWriter::get_instance().write_checkpoint(cur_cursor))) {
        ObTaskController::get().allow_next_syslog();
        LOG_ERROR("Fail to write checkpoint", K(ret));
      } else {
        LOG_INFO("[PHY_FLASHBACK]write checkpoint success", K(ret));
      }
    }
  }

  if (NULL != iter) {
    revert_pg_iter(iter);
  }
  return ret;
}

int ObPartitionService::report_pg_backup_task(const ObIArray<ObPartMigrationRes>& report_res_list)
{
  int ret = OB_SUCCESS;
  bool is_skip_report = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service do not init", K(ret));
  } else {
    ObArray<ObPGBackupTaskInfo> pg_task_info_array;
    ObPGBackupTaskInfo pg_task_info;
    const ObPGBackupTaskInfo::BackupStatus status = ObPGBackupTaskInfo::FINISH;

    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      const ObPartMigrationRes& migrate_res = report_res_list.at(i);
      int32_t result = migrate_res.result_;
      const ObPartitionKey& pkey = migrate_res.key_;
      pg_task_info.tenant_id_ = pkey.get_tenant_id();
      pg_task_info.table_id_ = pkey.get_table_id();
      pg_task_info.partition_id_ = pkey.get_partition_id();
      pg_task_info.incarnation_ = migrate_res.backup_arg_.incarnation_;
      pg_task_info.backup_set_id_ = migrate_res.backup_arg_.backup_set_id_;
      pg_task_info.backup_type_.type_ = migrate_res.backup_arg_.backup_type_;
      pg_task_info.result_ = result;
      pg_task_info.status_ = status;
      pg_task_info.end_time_ = ObTimeUtil::current_time();
      pg_task_info.finish_macro_block_count_ = migrate_res.data_statics_.ready_macro_block_;
      pg_task_info.finish_partition_count_ = migrate_res.data_statics_.finish_partition_count_;
      pg_task_info.input_bytes_ = migrate_res.data_statics_.input_bytes_;
      pg_task_info.macro_block_count_ = migrate_res.data_statics_.major_count_;
      pg_task_info.output_bytes_ = migrate_res.data_statics_.output_bytes_;
      pg_task_info.partition_count_ = migrate_res.data_statics_.partition_count_;

#ifdef ERRSIM
      const uint64_t skip_table_id = GCONF.skip_report_pg_backup_task_table_id;
      ret = E(EventTable::EN_BACKUP_REPORT_RESULT_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        result = ret;
        pg_task_info.result_ = result;
        LOG_INFO("errsim set backup result", K(result), K(ret));
      } else if (pkey.get_table_id() == skip_table_id) {
        is_skip_report = true;
      }
#endif
      if (is_skip_report) {
        LOG_INFO("skip report backup task", K(pg_task_info));
      } else if (OB_FAIL(pg_task_info_array.push_back(pg_task_info))) {
        LOG_WARN("failed to push pg task info into array", K(ret), K(pg_task_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rs_cb_->update_pg_backup_task_info(pg_task_info_array))) {
        LOG_WARN("failed to update pg backup task info", K(ret), K(pg_task_info_array));
      }
    }
  }
  return ret;
}

int ObPartitionService::enable_backup_white_list()
{
  int ret = OB_SUCCESS;
  ObString backup_zone_str = GCONF.backup_zone.get_value_string();
  ObString backup_region_str = GCONF.backup_region.get_value_string();

  bool prohibited = true;
  LOG_INFO("backup zone and region", K(backup_zone_str), K(backup_region_str));

  if (backup_zone_str.empty() && backup_region_str.empty()) {
    // Both backup region and zone are not set, IO operation is allowed.
    prohibited = false;
  } else if (!backup_zone_str.empty() && !backup_region_str.empty()) {
    // Both backup region and zone exist, not allowed, something wrong unexpected.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "backup region and zone both are set, which are unexpected.", K(ret), K(backup_zone_str), K(backup_region_str));
  } else if (!backup_zone_str.empty()) {
    // Backup zone is set.
    ObArray<ObBackupZone> backup_zone;
    if (OB_FAIL(ObBackupUtils::parse_backup_format_input(backup_zone_str, MAX_ZONE_LENGTH, backup_zone))) {
      LOG_WARN("failed to parse backup zone format", K(ret), K(backup_zone_str));
    } else if (backup_zone.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup zone is empty", K(ret), K(backup_zone_str));
    } else {
      const ObZone zone = GCONF.zone.str();
      LOG_INFO("set backup zone", K(zone), K(backup_zone_str), K(backup_zone));
      for (int64_t i = 0; i < backup_zone.count(); i++) {
        // I am in white list, IO operations are allowed.
        if (zone == backup_zone[i].zone_) {
          prohibited = false;
          break;
        }
      }
    }
  } else {
    // Backup region is set.
    ObArray<ObBackupRegion> backup_region;
    ObRegion region;
    if (OB_FAIL(ObBackupUtils::parse_backup_format_input(backup_region_str, MAX_REGION_LENGTH, backup_region))) {
      LOG_WARN("failed to parse backup region format", K(ret), K(backup_region_str));
    } else if (backup_region.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup region is empty", K(ret), K(backup_region_str));
    } else if (OB_FAIL(locality_manager_.get_local_region(region))) {
      LOG_WARN("get local region failed", K(ret), K(backup_region_str));
    } else {
      LOG_INFO("set backup region", K(region), K(backup_region_str), K(backup_region));
      for (int64_t i = 0; i < backup_region.count(); i++) {
        // I am in white list, IO operations are allowed.
        if (region == backup_region[i].region_) {
          prohibited = false;
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObStorageGlobalIns::get_instance().set_io_prohibited(prohibited);
    FLOG_WARN("backup set_io_prohibited", K(prohibited));
  }

  return ret;
}

int ObPartitionService::report_pg_backup_backupset_task(const ObIArray<ObPartMigrationRes>& report_res_list)
{
  int ret = OB_SUCCESS;
  bool is_skip_report = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service do not init", K(ret));
  } else {
    ObArray<share::ObBackupBackupsetArg> args;
    ObArray<int32_t> results;
    for (int64_t i = 0; OB_SUCC(ret) && i < report_res_list.count(); ++i) {
      const ObPartMigrationRes& migrate_res = report_res_list.at(i);
      int32_t result = migrate_res.result_;
      const share::ObBackupBackupsetArg& arg = migrate_res.backup_backupset_arg_;
#ifdef ERRSIM
      const uint64_t skip_table_id = GCONF.skip_report_pg_backup_task_table_id;
      ret = E(EventTable::EN_BACKUP_REPORT_RESULT_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        result = ret;
        LOG_INFO("errsim set backup result", K(result), K(ret));
      } else if (arg.pg_key_.get_table_id() == skip_table_id) {
        is_skip_report = true;
      }
#endif
      if (is_skip_report) {
        LOG_INFO("skip report backup task", K(arg.pg_key_), K(result));
      } else if (OB_FAIL(args.push_back(arg))) {
        LOG_WARN("failed to push pkey into array", K(ret), K(migrate_res));
      } else if (OB_FAIL(results.push_back(result))) {
        LOG_WARN("failed to push result into array", K(ret), K(result), K(migrate_res));
      }
    }

    if (OB_SUCC(ret)) {
      const ObPGBackupBackupsetTaskInfo::TaskStatus status = ObPGBackupBackupsetTaskInfo::FINISH;
      if (OB_FAIL(rs_cb_->report_pg_backup_backupset_task(args, results, status))) {
        LOG_WARN("failed to update pg backup backupset task", KR(ret), K(status));
      }
    }
  }
  return ret;
}

int ObPartitionService::check_flashback_need_remove_pg(
    const int64_t flashback_scn, ObIPartitionGroup* partition, bool& need_remove)
{
  int ret = OB_SUCCESS;
  int64_t create_timestamp = OB_INVALID_TIMESTAMP;
  ObMigrateStatus migrate_status = OB_MIGRATE_STATUS_MAX;
  need_remove = false;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition should not be NULL", K(ret), KP(partition));
  } else if (OB_FAIL(partition->get_create_ts(create_timestamp))) {
    LOG_WARN("failed to get pg meta", K(ret), KPC(partition));
  } else if (OB_FAIL(partition->get_pg_storage().get_pg_migrate_status(migrate_status))) {
    LOG_WARN("failed to get_migrate_status", K(ret), "pkey", partition->get_partition_key());
  } else if (create_timestamp > flashback_scn || ObMigrateStatus::OB_MIGRATE_STATUS_NONE != migrate_status) {
    need_remove = true;
    FLOG_INFO("pg create after flashback scn or migrate status is not none, need remove",
        K(create_timestamp),
        K(flashback_scn),
        K(migrate_status));
  }
  return ret;
}

int ObPartitionService::remove_flashback_unneed_pg(ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition should not be NULL", K(ret), KP(partition));
  } else {
    const ObPartitionKey& pkey = partition->get_partition_key();
    if (OB_FAIL(inner_del_partition(pkey))) {
      // should not happen
      STORAGE_LOG(ERROR, "Fail to inner del partition, ", K(ret), K(pkey));
      ob_abort();
    } else {
      FLOG_INFO("flashback remove unneed pg", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionService::wait_schema_version(const int64_t tenant_id, int64_t schema_version, int64_t query_end_time)
{
  int ret = OB_SUCCESS;
  int64_t local_version = 0;
  ObSchemaGetterGuard schema_guard;
  if (tenant_id < 0 || schema_version < 0 || query_end_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(schema_version), K(query_end_time));
  } else {
    ret = OB_EAGAIN;
    while (OB_EAGAIN == ret || OB_TENANT_SCHEMA_NOT_FULL == ret) {
      if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("failed to get_tenant_full_schema_guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_version))) {
        LOG_WARN("failed to get schema version", K(ret), K(tenant_id));
      } else if (!share::schema::ObSchemaService::is_formal_version(local_version)) {
        ret = OB_EAGAIN;
      } else if (local_version < schema_version) {
        ret = OB_EAGAIN;
      }

      if (OB_EAGAIN == ret) {
        int64_t loop_end_us = ObTimeUtility::current_time();
        if (loop_end_us > query_end_time) {
          ret = OB_TIMEOUT;
          STORAGE_LOG(WARN, "wait_schema_version timeout", K(ret), K(schema_version), K(query_end_time));
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::mark_log_archive_encount_fatal_error(
    const common::ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionService is not inited", KR(ret), K(incarnation), K(archive_round));
  } else if (OB_ISNULL(clog_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "clog_mgr is null", K(ret), KR(ret), K(incarnation), K(archive_round));
  } else if (OB_FAIL(clog_mgr_->mark_log_archive_encount_fatal_err(pkey, incarnation, archive_round))) {
    STORAGE_LOG(
        WARN, "failed to mark_log_archive_encount_fatal_err", KR(ret), K(pkey), K(incarnation), K(archive_round));
  } else { /*do nothing*/
  }

  return ret;
}

int ObPartitionService::inc_pending_batch_commit_count(
    const ObPartitionKey& pkey, memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts)
{
  int ret = common::OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else {
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->inc_pending_batch_commit_count(mt_ctx, log_ts))) {
      STORAGE_LOG(WARN, "failed to inc_pending_batch_commit_count", K(ret), K(pkey));
    }
  }

  return ret;
}

int ObPartitionService::inc_pending_elr_count(
    const ObPartitionKey& pkey, memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts)
{
  int ret = common::OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initiated", K(ret));
  } else {
    if (OB_FAIL(get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->inc_pending_elr_count(mt_ctx, log_ts))) {
      STORAGE_LOG(WARN, "failed to inc_pending_elr_count", K(ret), K(pkey));
    }
  }

  return ret;
}

int ObPartitionService::check_can_physical_flashback()
{
  int ret = OB_SUCCESS;
  const bool is_physical_flashback = true;

  if (!GCTX.is_in_phy_fb_verify_mode()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "observer do not in physical flashback or verify mode", K(ret), K(GCTX.mode_));
  } else if (OB_FAIL(OB_STORE_FILE.open(is_physical_flashback))) {
    STORAGE_LOG(ERROR, "[PHY_FLASHBACK]fail to open store file, ", K(ret));
  } else if (OB_FAIL(check_can_physical_flashback_())) {
    STORAGE_LOG(ERROR, "[PHY_FLASHBACK]failed to check_can_physical_flashback_", K(ret));
  } else {
    STORAGE_LOG(INFO, "[PHY_FLASHBACK]check_can_physical_flashback success", K(ret), K(GCTX.flashback_scn_));
  }

  return ret;
}

int ObPartitionService::check_can_physical_flashback_()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;

  if (!GCTX.is_in_phy_fb_mode() && !GCTX.is_in_phy_fb_verify_mode()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "observer do not in physical flashback or verify mode, can not flashback", K(ret), K(GCTX.mode_));
  } else if (NULL == (iter = alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "cannot allocate scan iterator.", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      bool need_remove_pg = false;
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "get next partition failed.", K(ret));
        }
      } else if (NULL == partition) {
        ret = OB_PARTITION_NOT_EXIST;
        STORAGE_LOG(WARN, "get partition failed", K(ret));
      } else if (OB_FAIL(check_flashback_need_remove_pg(GCTX.flashback_scn_, partition, need_remove_pg))) {
        LOG_WARN("failed to check flashback need remove pg", K(ret), K(GCTX.flashback_scn_));
      } else if (need_remove_pg) {
        LOG_WARN("[PHY_FLASHBACK]this pg need remove at flashback scn",
            K(ret),
            K(GCTX.flashback_scn_),
            "pkey",
            partition->get_partition_key());
      } else if (OB_FAIL(partition->check_can_physical_flashback(GCTX.flashback_scn_))) {
        STORAGE_LOG(WARN,
            "[PHY_FLASHBACK]check_can_physical_flashback failed",
            K(ret),
            "pkey",
            partition->get_partition_key(),
            K(GCTX.flashback_scn_));
      } else {
        STORAGE_LOG(INFO,
            "[PHY_FLASHBACK]check_can_physical_flashback success",
            "pkey",
            partition->get_partition_key(),
            K(GCTX.flashback_scn_));
      }
    }
  }

  if (NULL != iter) {
    revert_pg_iter(iter);
  }

  STORAGE_LOG(INFO, "[PHY_FLASHBACK]check_can_physical_flashback finished", K(ret), K(GCTX.flashback_scn_));

  return ret;
}

int ObPartitionService::check_condition_before_set_restore_flag_(
    const ObPartitionKey& pkey, const int16_t flag, const int64_t restore_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
          flag >= ObReplicaRestoreStatus::REPLICA_RESTORE_LOG && OB_INVALID_TIMESTAMP == restore_snapshot_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "invalid restore_state or restore_snapshot_version", KR(ret), K(pkey), K(flag), K(restore_snapshot_version));
  } else if (ObReplicaRestoreStatus::REPLICA_RESTORE_LOG == flag) {
    if (OB_FAIL(set_restore_snapshot_version_for_trans(pkey, restore_snapshot_version))) {
      LOG_WARN(
          "failed to set_restore_snapshot_version_for_trans", KR(ret), K(pkey), K(flag), K(restore_snapshot_version));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionService::wait_all_trans_clear(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret), K(pkey));
  } else if (OB_FAIL(txs_->wait_all_trans_clear(pkey))) {
    STORAGE_LOG(WARN, "failed to wait_all_trans_clear", KR(ret), K(pkey));
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionService::check_all_trans_in_trans_table_state(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(txs_, "transaction service"))) {
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret), K(pkey));
  } else if (OB_FAIL(txs_->check_all_trans_in_trans_table_state(pkey))) {
    STORAGE_LOG(WARN, "failed to check_all_trans_in_trans_table_state", KR(ret), K(pkey));
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionService::get_migrate_leader_and_parent(
    const common::ObPartitionKey& pkey, common::ObIArray<ObMigrateSrcInfo>& addr_array)
{
  int ret = OB_SUCCESS;
  ObHashSet<ObMigrateSrcInfo> addr_set;
  const int64_t MAX_BUCKET = 10;
  const bool is_standby_cluster = GCTX.is_standby_cluster();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service do not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get migrate src get invalid argument", K(ret));
  } else if (OB_FAIL(addr_set.create(MAX_BUCKET))) {
    LOG_WARN("failed to create addr set", K(ret));
  } else if (is_standby_cluster) {
    if (OB_FAIL(get_standby_cluster_migrate_src(pkey, addr_set, addr_array))) {
      LOG_WARN("failed to get standby cluster migrate src", K(ret), K(pkey));
    }
  } else {
    // primary cluster
    if (OB_FAIL(get_primary_cluster_migrate_src(pkey, addr_set, addr_array))) {
      LOG_WARN("failed to get primary cluster migrate src", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionService::get_primary_cluster_migrate_src(const common::ObPartitionKey& pkey,
    hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  share::ObIPartitionLocationCache* location_cache = NULL;
  const int64_t self_cluster_id = GCONF.cluster_id;
  const ObIPartitionLogService* log_service = NULL;
  const bool force_renew = false;
  ObMigrateSrcInfo src_info;
  bool is_restore = false;

  if (OB_FAIL(get_partition(pkey, guard))) {
    LOG_WARN("get partition failed", K(pkey), K(ret));
  } else if (NULL == (location_cache = get_location_cache())) {
    ret = OB_ERR_SYS;
    LOG_WARN("location cache must not null", K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    // partition not exist, get leader
    if (NULL == (location_cache = get_location_cache())) {
      ret = OB_ERR_SYS;
      LOG_WARN("location cache must not null", K(ret));
    } else if (OB_FAIL(location_cache->get_strong_leader(pkey, src_info.src_addr_, force_renew))) {
      LOG_WARN("get leader address failed", K(ret), K(pkey));
    } else {
      src_info.cluster_id_ = self_cluster_id;
      if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
        LOG_WARN("failed to add migrate src", K(ret), K(src_info), K(pkey));
      } else {
        src_info.reset();
      }
    }
  } else {
    // get clog parent
    if (OB_UNLIKELY(NULL == (log_service = partition->get_log_service()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log service should not be NULL", K(ret), K(pkey));
    } else if (FALSE_IT(is_restore = partition->get_pg_storage().is_restore())) {
    } else if (OB_FAIL(log_service->get_clog_parent_for_migration(src_info.src_addr_, src_info.cluster_id_))) {
      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get clog parent", K(ret), K(pkey));
      }
    } else if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
      LOG_WARN("failed to add migrate src", K(ret), K(pkey), K(src_info));
    } else {
      src_info.reset();
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_ADD_REBUILD_PARENT_SRC) OB_SUCCESS;
    }
#endif

    if (OB_SUCC(ret)) {
      if (is_restore) {
        if (OB_FAIL(location_cache->nonblock_get_restore_leader(pkey, src_info.src_addr_))) {
          LOG_WARN("failed to get restore leader", K(ret), K(pkey));
        }
      } else {
        if (OB_FAIL(location_cache->get_strong_leader(pkey, src_info.src_addr_, force_renew))) {
          LOG_WARN("get leader address failed", K(ret), K(pkey));
        }
      }

      if (OB_SUCC(ret)) {
        src_info.cluster_id_ = self_cluster_id;
        if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
          LOG_WARN("failed to add migrate src", K(ret), K(src_info), K(pkey));
        } else {
          src_info.reset();
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::get_standby_cluster_migrate_src(const common::ObPartitionKey& pkey,
    hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  share::ObIPartitionLocationCache* location_cache = NULL;
  const int64_t self_cluster_id = GCONF.cluster_id;
  const ObIPartitionLogService* log_service = NULL;
  const bool force_renew = false;
  ObRole role;
  ObMigrateSrcInfo src_info;
  if (OB_FAIL(get_partition(pkey, guard))) {
    LOG_WARN("get partition failed", K(pkey), K(ret));
  } else if (NULL == (location_cache = get_location_cache())) {
    ret = OB_ERR_SYS;
    LOG_WARN("location cache must not null", K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    // partition not exist, get leader
    if (OB_FAIL(location_cache->get_leader_by_election(pkey, src_info.src_addr_, force_renew))) {
      LOG_WARN("get standby leader address failed", K(ret), K(pkey));
    } else if (FALSE_IT(src_info.cluster_id_ = self_cluster_id)) {
    } else if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
      LOG_WARN("failed to add migrate src", K(ret), K(src_info));
    } else {
      src_info.reset();
    }
  } else {
    // get clog parent
    if (OB_FAIL(partition->get_role(role))) {
      LOG_WARN("failed to get role", K(ret), K(pkey));
    } else if (OB_UNLIKELY(NULL == (log_service = partition->get_log_service()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log service should not be NULL", K(ret), K(pkey));
    } else if (ObRole::STANDBY_LEADER == role) {
      if (OB_FAIL(log_service->get_clog_parent_for_migration(src_info.src_addr_, src_info.cluster_id_))) {
        if (OB_NEED_RETRY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get clog parent", K(ret), K(pkey));
        }
      } else if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
        LOG_WARN("failed to add migrate src", K(ret), K(role), K(src_info), K(pkey));
      } else {
        LOG_INFO("get_clog_parent_for_migration", K(src_info));
        src_info.reset();
      }
    }

    // get leader
    if (OB_SUCC(ret)) {
      if (ObRole::FOLLOWER == role) {
        if (OB_FAIL(location_cache->get_leader_by_election(pkey, src_info.src_addr_, force_renew))) {
          LOG_WARN("get standby leader address failed", K(ret), K(pkey));
        } else if (FALSE_IT(src_info.cluster_id_ = self_cluster_id)) {
        } else if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
          LOG_WARN("failed to add migrate src", K(ret), K(src_info));
        }
      } else if (ObRole::STANDBY_LEADER == role) {
        src_info.cluster_id_ = OB_INVALID_ID;
        if (!GCTX.is_standby_cluster() || ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
          src_info.cluster_id_ = GCONF.cluster_id;
        } else {
          src_info.cluster_id_ = location_cache_->get_primary_cluster_id();
        }
        if (OB_FAIL(location_cache->get_leader_across_cluster(
                pkey, src_info.src_addr_, src_info.cluster_id_, force_renew))) {
          LOG_WARN("get standby leader address failed", K(ret), K(pkey), "cluster_id", src_info.cluster_id_);
          if (OB_LOCATION_LEADER_NOT_EXIST != ret) {
            // do nothing
          } else {
            // rewrite ret
            ObPartitionLocation location;
            ObReplicaLocation replica_location;
            int64_t expire_renew_time = 0;  // INT64_MAX will force renew location
            if (OB_FAIL(location_cache->get_across_cluster(pkey, expire_renew_time, location, src_info.cluster_id_))) {
              LOG_WARN("failed to get across cluster", K(ret), K(pkey), K(src_info));
            } else {
              const ObIArray<ObReplicaLocation>& replica_locations = location.get_replica_locations();
              const ObReplicaLocation* follower = NULL;
              for (int64_t i = 0; OB_SUCC(ret) && i < replica_locations.count(); ++i) {
                const ObReplicaLocation& replica_location = replica_locations.at(i);
                if (replica_location.is_follower()) {
                  follower = &replica_location;
                  src_info.src_addr_ = follower->server_;
                  if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
                    LOG_WARN("failed to add migrate src", K(ret), K(src_info));
                  }
                }
              }
            }
          }
        } else if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
          LOG_WARN("failed to add migrate src", K(ret), K(src_info));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("standby cluster role is unexpected", K(ret), K(pkey), K(role));
      }
    }

    LOG_INFO("get_standby restore migrate src", K(src_array));
  }
  return ret;
}

int ObPartitionService::get_rebuild_src(const common::ObPartitionKey& pkey, const ObReplicaType& replica_type,
    common::ObIArray<ObMigrateSrcInfo>& addr_array)
{
  int ret = OB_SUCCESS;
  ObHashSet<ObMigrateSrcInfo> addr_set;
  const int64_t MAX_BUCKET = 1024;
  const bool is_standby_cluster = GCTX.is_standby_cluster();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service do not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get migrate src get invalid argument", K(ret));
  } else if (OB_FAIL(addr_set.create(MAX_BUCKET))) {
    LOG_WARN("failed to create addr set", K(ret));
  } else if (is_standby_cluster) {
    if (OB_FAIL(get_standby_cluster_migrate_member_src(pkey, replica_type, addr_set, addr_array))) {
      LOG_WARN("failed to get standby cluster migrate src", K(ret), K(pkey));
    }
  } else {
    // primary cluster
    if (OB_FAIL(get_primary_cluster_migrate_member_src(pkey, replica_type, addr_set, addr_array))) {
      LOG_WARN("failed to get primary cluster migrate src", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionService::get_primary_cluster_migrate_member_src(const common::ObPartitionKey& pkey,
    const ObReplicaType& replica_type, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
    common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  const bool is_paxos_replica = ObReplicaTypeCheck::is_paxos_replica(replica_type);
  if (!is_paxos_replica) {
    LOG_INFO("no paxos replica, no need get member list", K(replica_type));
  } else if (OB_FAIL(add_current_migrate_member_list(pkey, src_set, src_array))) {
    LOG_WARN("failed to add current member list", K(ret), K(pkey));
  } else if (OB_FAIL(add_meta_table_migrate_src(pkey, src_set, src_array))) {
    LOG_WARN("failed to add meta table", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::get_standby_cluster_migrate_member_src(const common::ObPartitionKey& pkey,
    const ObReplicaType& replica_type, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
    common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  const bool is_paxos_replica = ObReplicaTypeCheck::is_paxos_replica(replica_type);
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObRole role;

  if (!is_paxos_replica) {
    LOG_INFO("no paxos replica, no need get member list", K(replica_type));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    LOG_WARN("get partition failed", K(pkey), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition should not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_role(role))) {
    LOG_WARN("failed to get real role", K(ret), K(pkey));
  } else if (OB_FAIL(add_current_migrate_member_list(pkey, src_set, src_array))) {
    LOG_WARN("failed to add current member list", K(ret), K(pkey));
  } else if (OB_FAIL(add_meta_table_migrate_src(pkey, src_set, src_array))) {
    LOG_WARN("failed to add meta table", K(ret), K(pkey));
  } else {
    if (ObRole::STANDBY_LEADER == role) {
      if (OB_FAIL(add_primary_meta_table_migrate_src(pkey, src_set, src_array))) {
        LOG_WARN("failed to add primay meta table migrate src", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionService::add_current_migrate_member_list(const common::ObPartitionKey& pkey,
    hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  // get local memtbale list
  const int64_t self_cluster_id = GCONF.cluster_id;
  common::ObMemberList member_list;
  ObMigrateSrcInfo src_info;
  ObAddr server;
  if (OB_FAIL(get_curr_member_list(pkey, member_list))) {
    LOG_WARN("failed to get_curr_member_list", K(ret), K(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        LOG_WARN("failed to get server by index", K(ret), K(i));
      } else {
        src_info.src_addr_ = server;
        src_info.cluster_id_ = self_cluster_id;
        if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
          LOG_WARN("failed to add migrate src", K(ret), K(src_info));
        } else {
          src_info.reset();
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::add_meta_table_migrate_src(const common::ObPartitionKey& pkey,
    hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  share::ObPartitionTableOperator* pt_operator = NULL;
  ModulePageAllocator allocator(ObNewModIds::OB_PARTITION_MIGRATE);
  share::ObPartitionInfo partition_info;
  partition_info.set_allocator(&allocator);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL == (pt_operator = GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pt_operator must not NULL.", K(ret));
  } else if (OB_SUCCESS != (ret = pt_operator->get(pkey.get_table_id(), pkey.get_partition_id(), partition_info))) {
    STORAGE_LOG(WARN, "fail to get partition info.", K(ret), K(pkey));
  } else if (OB_FAIL(inner_add_meta_table_migrate_src(
                 partition_info.get_replicas_v2(), GCONF.cluster_id, src_set, src_array))) {
    LOG_WARN("failed to inner add meta table migrate src", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::add_migrate_src(const ObMigrateSrcInfo& src_info, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
    common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (!src_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add migrate src get invalid argument", K(ret), K(src_info));
  } else {
    hash_ret = src_set.exist_refactored(src_info);
    if (OB_HASH_EXIST == hash_ret) {
      // do nothing
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      if (OB_FAIL(src_array.push_back(src_info))) {
        LOG_WARN("failed to push src info into array", K(ret), K(src_info));
      } else if (OB_FAIL(src_set.set_refactored(src_info))) {
        LOG_WARN("failed to set src info into set", K(ret), K(src_info));
      }
    } else {
      ret = hash_ret != OB_SUCCESS ? hash_ret : OB_ERR_UNEXPECTED;
      LOG_WARN("failed to check hash set exsit", K(ret), K(hash_ret), K(src_info));
    }
  }
  return ret;
}

int ObPartitionService::check_restore_point_complete(
    const ObPartitionKey& pkey, const int64_t snapshot_version, bool& is_complete)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  is_complete = false;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionGroupGuard guard;

  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(WARN, "partition service not initialized, cannot get partitions.");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "failed to get partition", K(pkey));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else {
    ObRecoveryDataMgr& mgr = guard.get_partition_group()->get_pg_storage().get_recovery_data_mgr();
    if (OB_FAIL(mgr.check_restore_point_exist(snapshot_version, is_exist))) {
      STORAGE_LOG(WARN, "failed to check restore point exist", K(ret), K(pkey), K(snapshot_version));
    } else {
      is_complete = is_exist;
    }
  }
  return ret;
}

int ObPartitionService::try_advance_restoring_clog()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ret = clog_mgr_->try_advance_restoring_clog();
  }
  return ret;
}

int ObPartitionService::get_partition_saved_last_log_info(
    const ObPartitionKey& pkey, uint64_t& last_replay_log_id, int64_t& last_replay_log_ts)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObIPartitionGroupGuard guard;

  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(WARN, "partition service not initialized, cannot get partitions.");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "failed to get partition", K(pkey));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_saved_last_log_info(last_replay_log_id, last_replay_log_ts))) {
    STORAGE_LOG(ERROR, "get_saved_last_log_info failed", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::add_primary_meta_table_migrate_src(const common::ObPartitionKey& pkey,
    hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  share::ObRemotePartitionTableOperator* remote_pt_operator = NULL;
  ModulePageAllocator allocator(ObNewModIds::OB_PARTITION_MIGRATE);
  share::ObPartitionInfo partition_info;
  partition_info.set_allocator(&allocator);
  const int64_t primary_cluster_id = location_cache_->get_primary_cluster_id();
  const bool need_fetch_failed_list = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
    // do nothing
  } else if (NULL == (remote_pt_operator = GCTX.remote_pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pt_operator must not NULL.", K(ret));
  } else if (OB_SUCCESS != (ret = remote_pt_operator->get(pkey.get_table_id(),
                                pkey.get_partition_id(),
                                partition_info,
                                need_fetch_failed_list,
                                primary_cluster_id))) {
    STORAGE_LOG(WARN, "fail to get partition info.", K(ret), K(pkey));
  } else if (OB_FAIL(inner_add_meta_table_migrate_src(
                 partition_info.get_replicas_v2(), primary_cluster_id, src_set, src_array))) {
    LOG_WARN("failed to inner add meta table migrate src", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionService::inner_add_meta_table_migrate_src(const common::ObIArray<share::ObPartitionReplica>& replicas,
    const int64_t cluster_id, hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      const share::ObPartitionReplica& replica = replicas.at(i);
      if (ObReplicaTypeCheck::is_replica_with_ssstore(replica.replica_type_) && 0 == replica.is_restore_ &&
          replica.in_member_list_) {
        ObMigrateSrcInfo src_info;
        src_info.src_addr_ = replica.server_;
        src_info.cluster_id_ = cluster_id;
        if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
          LOG_WARN("failed to add migrate src", K(ret), K(src_info));
        } else {
          src_info.reset();
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::get_migrate_member_list_src(
    const common::ObPartitionKey& pkey, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<ObMigrateSrcInfo> src_set;
  const int64_t MAX_BUCKET = 1024;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObRole role;
  const bool is_standby_cluster = GCTX.is_standby_cluster();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get migrate meta table src get invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(src_set.create(MAX_BUCKET))) {
    LOG_WARN("failed to create hash set", K(ret));
  } else if (OB_FAIL(get_partition(pkey, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      role = ObRole::INVALID_ROLE;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get partition failed", K(pkey), K(ret));
    }
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition should not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_role(role))) {
    LOG_WARN("failed to get real role", K(ret), K(pkey));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_migrate_member_list(pkey, role, is_standby_cluster, src_set, src_array))) {
    LOG_WARN("failed to add migrate member list", K(ret), K(pkey), K(role));
  }
  return ret;
}

int ObPartitionService::inner_add_location_migrate_src(const common::ObIArray<share::ObReplicaLocation>& locations,
    const int64_t cluster_id, hash::ObHashSet<ObMigrateSrcInfo>& src_set, common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); ++i) {
      const share::ObReplicaLocation& location = locations.at(i);
      if (ObReplicaTypeCheck::is_can_elected_replica(location.replica_type_) && 0 == location.restore_status_) {
        // The replicas in recovery process will be filtered out, the purpose is to prevent the
        // wrong selection, and the disaster recovery scenarios of the replicas in recovery will
        // use get_migrate_leader_and_parent
        ObMigrateSrcInfo src_info;
        src_info.src_addr_ = location.server_;
        src_info.cluster_id_ = cluster_id;
        if (OB_FAIL(add_migrate_src(src_info, src_set, src_array))) {
          LOG_WARN("failed to add migrate src", K(ret), K(src_info));
        } else {
          src_info.reset();
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::add_location_cache_migrate_src(const common::ObPartitionKey& pkey, const ObRole& role,
    const bool is_standby_cluster, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
    common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;
  share::ObPartitionLocation location;
  const int64_t expire_renew_time = INT64_MAX;  // force renew
  bool is_cache_hit = false;
  ObIPartitionGroupGuard guard;
  ObArray<share::ObReplicaLocation> replica_locations;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location cache should not be NULL", K(ret), KP(location_cache_));
  } else if (ObRole::STANDBY_LEADER == role && is_standby_cluster) {
    const int64_t primary_cluster_id = location_cache_->get_primary_cluster_id();
    if (ObMultiClusterUtil::is_cluster_private_table(pkey.get_table_id())) {
      // do nothing
    } else if (OB_FAIL(location_cache_->get_across_cluster(pkey, expire_renew_time, location))) {
      LOG_WARN("failed to get across cluster locations", K(ret), K(pkey), K(role));
    } else if (OB_FAIL(inner_add_location_migrate_src(
                   location.get_replica_locations(), primary_cluster_id, src_set, src_array))) {
      LOG_WARN("failed to inner add location migrate src", K(ret), K(pkey));
    }
  } else {
    if (OB_FAIL(location_cache_->get(pkey, location, expire_renew_time, is_cache_hit))) {
      LOG_WARN("failed to get partition location", K(ret), K(pkey));
    } else if (OB_FAIL(inner_add_location_migrate_src(
                   location.get_replica_locations(), GCONF.cluster_id, src_set, src_array))) {
      LOG_WARN("failed to inner add location migrate src", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionService::add_migrate_member_list(const common::ObPartitionKey& pkey, const ObRole& role,
    const bool is_standby_cluster, hash::ObHashSet<ObMigrateSrcInfo>& src_set,
    common::ObIArray<ObMigrateSrcInfo>& src_array)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (OB_FAIL(add_location_cache_migrate_src(pkey, role, is_standby_cluster, src_set, src_array))) {
      LOG_WARN("failed to add location cache migrate src", K(ret), K(pkey));
    }

    if (OB_FAIL(ret)) {
      // overwrite ret
      if (ObRole::STANDBY_LEADER == role && is_standby_cluster) {
        if (OB_FAIL(add_primary_meta_table_migrate_src(pkey, src_set, src_array))) {
          LOG_WARN("failed to add primay meta table migrate src", K(ret), K(pkey));
        }
      } else {
        // for rebuild without leader
        if (OB_FAIL(add_meta_table_migrate_src(pkey, src_set, src_array))) {
          LOG_WARN("failed to add meta table", K(ret), K(pkey));
        }
      }
    }
  }
  return ret;
}

int ObPartitionService::extract_pkeys(
    const common::ObIArray<obrpc::ObCreatePartitionArg>& batch_arg, common::ObIArray<ObPartitionKey>& pkey_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.count(); ++i) {
    const ObPartitionKey& pkey = batch_arg.at(i).pg_key_;
    if (OB_UNLIKELY(!pkey.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
    } else if (OB_FAIL(pkey_array.push_back(pkey))) {
      STORAGE_LOG(WARN, "push back pkey failed.", K(i), K(batch_arg));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartitionService::mark_pg_creating(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(create_pg_checker_.mark_pg_creating(pkey))) {
    STORAGE_LOG(WARN, "fail to mark partition group creating", K(ret));
  }
  return ret;
}

int ObPartitionService::mark_pg_created(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(create_pg_checker_.mark_pg_created(pkey))) {
    STORAGE_LOG(WARN, "fail to mark partition group creating", K(ret));
  }
  return ret;
}

int ObPartitionService::check_standby_cluster_schema_condition(const ObPartitionKey& pkey, const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (GCTX.is_standby_cluster()) {
    // only stand_by cluster need to be check
    uint64_t tenant_id = extract_tenant_id(pkey.get_table_id());
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid tenant_id", K(ret), K(tenant_id), K(pkey), K(schema_version));
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      // sys tenant do not need check
    } else {
      // user tables of normal tenants(not sys tenant) need to be checked by schema version of
      // itself;
      // sys tables of normal tenants(not sys tenant) need to be checked by schema version of sys tenent;
      uint64_t referred_tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : tenant_id;
      int64_t tenant_schema_version = 0;
#ifdef ERRSIM
      if (!is_inner_table(pkey.get_table_id())) {
        ret = E(EventTable::EN_CHECK_STANDBY_CLUSTER_SCHEMA_CONDITION) OB_SUCCESS;
      }
#endif
      if (OB_FAIL(ret)) {
        // errsim
      } else if (OB_FAIL(
                     GSCHEMASERVICE.get_tenant_refreshed_schema_version(referred_tenant_id, tenant_schema_version))) {
        TRANS_LOG(WARN,
            "get_tenant_schema_version failed",
            K(ret),
            K(referred_tenant_id),
            K(pkey),
            K(tenant_id),
            K(schema_version));
        if (OB_ENTRY_NOT_EXIST == ret) {
          // In scenarios such as restarting, the tenant schema has not been updated yet.
          // rewrite OB_ENTRY_NOT_EXIST with OB_TRANS_WAIT_SCHEMA_REFRESH
          ret = OB_TRANS_WAIT_SCHEMA_REFRESH;
          TRANS_LOG(WARN,
              "tenant schema has not been update yet",
              K(ret),
              K(pkey),
              K(tenant_id),
              K(referred_tenant_id),
              K(schema_version),
              K(tenant_schema_version));
        }
      } else if (schema_version > tenant_schema_version) {
        // The table version of the data to be replayed is greater than the schema version of the reference tenant,
        // and replaying is not allowed
        ret = OB_TRANS_WAIT_SCHEMA_REFRESH;
        TRANS_LOG(WARN,
            "local table schema version is too small, cannot replay",
            K(ret),
            K(pkey),
            K(tenant_id),
            K(referred_tenant_id),
            K(schema_version),
            K(tenant_schema_version));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObPartitionService::standby_cut_data_batch(const obrpc::ObStandbyCutDataBatchTaskArg& arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObReplicaOpArg> task_list;
  const ObReplicaOpType type = RESTORE_STANDBY_OP;
  int64_t flashback_ts = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObPartitionService has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "The service is not running, ", K(ret));
  } else if (OB_UNLIKELY(!is_scan_disk_finished())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "rebooting, replica op not allow", K(ret));
  } else if (!arg.is_valid() || arg.arg_array_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed args", K(ret), K(arg));
  } else if (OB_FAIL(task_list.reserve(arg.arg_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve task list", K(ret));
  } else if (FALSE_IT(flashback_ts = arg.flashback_ts_)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
      const obrpc::ObStandbyCutDataTaskArg& single_arg = arg.arg_array_.at(i);
      ObReplicaOpArg replica_op_arg;
      replica_op_arg.data_src_ = single_arg.dst_;
      replica_op_arg.dst_ = single_arg.dst_;
      replica_op_arg.key_ = single_arg.pkey_;
      replica_op_arg.priority_ = ObReplicaOpPriority::PRIO_LOW;
      replica_op_arg.cluster_id_ = GCONF.cluster_id;
      replica_op_arg.type_ = type;
      replica_op_arg.switch_epoch_ = GCTX.get_switch_epoch2();
      replica_op_arg.phy_restore_arg_.restore_info_.restore_snapshot_version_ = flashback_ts;
      ObIPartitionGroupGuard guard;
      ObIPartitionGroup* partition = NULL;
      ObReplicaRestoreStatus restore_status;
      if (OB_FAIL(get_partition(single_arg.pkey_, guard))) {
        STORAGE_LOG(WARN, "get partition failed", K(single_arg), K(ret));
      } else if (OB_ISNULL(guard.get_partition_group())) {
        ret = OB_ENTRY_NOT_EXIST;
        STORAGE_LOG(WARN, "partition not exist, maybe migrate out", K(single_arg), K(ret));
      } else if (OB_ISNULL(partition = guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition should not be NULL", K(ret), K(single_arg));
      } else if (FALSE_IT(restore_status = partition->get_pg_storage().get_restore_status())) {
      } else if (ObReplicaRestoreStatus::REPLICA_RESTORE_STANDBY_CUT != restore_status) {
        LOG_INFO("partition restore status is not cut data, skip it", K(ret), K(single_arg));
      } else if (OB_FAIL(task_list.push_back(replica_op_arg))) {
        LOG_WARN("failed to push replica op arg into array", K(ret), K(replica_op_arg));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (task_list.empty()) {
      LOG_INFO("has no replica need cut data, skip", K(ret), K(arg));
    } else if (OB_FAIL(ObPartGroupMigrator::get_instance().schedule(task_list, arg.trace_id_))) {
      STORAGE_LOG(WARN, "fail to schedule migrate task.", K(arg), K(ret));
    }
  }
  return ret;
}

}  // end of namespace storage
}  // end of namespace oceanbase
