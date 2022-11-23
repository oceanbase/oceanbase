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
#include "ob_storage_struct.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_rpc_struct.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/ob_storage_schema.h"

using namespace oceanbase;
using namespace storage;
using namespace common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;


OB_SERIALIZE_MEMBER(ObTabletReportStatus,
    merge_snapshot_version_,
    cur_report_version_,
    data_checksum_,
    row_count_);

OB_SERIALIZE_MEMBER(ObReportStatus,
    data_version_,
    row_count_,
    row_checksum_,
    data_checksum_,
    data_size_,
    required_size_,
    snapshot_version_);

OB_SERIALIZE_MEMBER(ObPGReportStatus,
    data_version_,
    data_size_,
    required_size_,
    snapshot_version_);

ObPartitionBarrierLogState::ObPartitionBarrierLogState()
  : state_(BARRIER_LOG_INIT), log_id_(0), log_ts_(0), schema_version_(0)
{
}

ObPartitionBarrierLogStateEnum ObPartitionBarrierLogState::to_persistent_state() const
{
  ObPartitionBarrierLogStateEnum persistent_state = BARRIER_LOG_INIT;
  switch (state_) {
    case BARRIER_LOG_INIT:
      // fall through
    case BARRIER_LOG_WRITTING:
      persistent_state = BARRIER_LOG_INIT;
      break;
    case BARRIER_SOURCE_LOG_WRITTEN:
      persistent_state = BARRIER_SOURCE_LOG_WRITTEN;
      break;
    case BARRIER_DEST_LOG_WRITTEN:
      persistent_state = BARRIER_DEST_LOG_WRITTEN;
      break;
  }
  return persistent_state;
}

void ObPartitionBarrierLogState::set_log_info(const ObPartitionBarrierLogStateEnum state, const int64_t log_id, const int64_t log_ts, const int64_t schema_version)
{
  state_ = state;
  log_id_ = log_id;
  log_ts_ = log_ts;
  schema_version_ = schema_version;
}

int ObPartitionBarrierLogState::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const ObPartitionBarrierLogStateEnum persistent_state = to_persistent_state();
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, persistent_state))) {
    LOG_WARN("fail to encode state", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, log_id_))) {
    LOG_WARN("encode log id failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, log_ts_))) {
    LOG_WARN("encode log ts failed", K(ret));
  }
  return ret;
}

int ObPartitionBarrierLogState::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_state = 0;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tmp_state))) {
    LOG_WARN("fail to decode state", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &log_id_))) {
    LOG_WARN("decode log id failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &log_ts_))) {
    LOG_WARN("decode log ts failed", K(ret));
  } else {
    state_ = static_cast<ObPartitionBarrierLogStateEnum>(tmp_state);
  }
  return ret;
}

int64_t ObPartitionBarrierLogState::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(to_persistent_state());
  len += serialization::encoded_length_i64(log_id_);
  len += serialization::encoded_length_i64(log_ts_);
  return len;
}


ObGetMergeTablesParam::ObGetMergeTablesParam()
  : merge_type_(INVALID_MERGE_TYPE),
    merge_version_()
{
}

bool ObGetMergeTablesParam::is_valid() const
{
  return (merge_type_ > INVALID_MERGE_TYPE && merge_type_ < MERGE_TYPE_MAX)
    && (!is_major_merge() || merge_version_ > 0);
}

ObGetMergeTablesResult::ObGetMergeTablesResult()
  : version_range_(),
    handle_(),
    merge_version_(),
    base_schema_version_(INVALID_INT_VALUE),
    schema_version_(INVALID_INT_VALUE),
    create_snapshot_version_(INVALID_INT_VALUE),
    checksum_method_(INVALID_INT_VALUE),
    suggest_merge_type_(INVALID_MERGE_TYPE),
    update_tablet_directly_(false),
    schedule_major_(false),
    log_ts_range_(),
    dump_memtable_timestamp_(0),
    read_base_version_(0)
{
}

bool ObGetMergeTablesResult::is_valid() const
{
  return log_ts_range_.is_valid()
      && handle_.get_count() >= 1
      && merge_version_ >= 0
      && base_schema_version_ >= 0
      && schema_version_ >= 0
      && create_snapshot_version_ >= 0
      && dump_memtable_timestamp_ >= 0
      && (suggest_merge_type_ > INVALID_MERGE_TYPE && suggest_merge_type_ < MERGE_TYPE_MAX);
}

void ObGetMergeTablesResult::reset_handle_and_range()
{
  handle_.reset();
  version_range_.reset();
  log_ts_range_.reset();
}

void ObGetMergeTablesResult::reset()
{
  version_range_.reset();
  handle_.reset();
  merge_version_ = ObVersionRange::MIN_VERSION;
  base_schema_version_ = INVALID_INT_VALUE;
  schema_version_ = INVALID_INT_VALUE;
  create_snapshot_version_ = 0;
  suggest_merge_type_ = INVALID_MERGE_TYPE;
  schedule_major_ = false;
  checksum_method_ = INVALID_INT_VALUE;
  log_ts_range_.reset();
  dump_memtable_timestamp_ = 0;
  read_base_version_ = 0;
}

int ObGetMergeTablesResult::deep_copy(const ObGetMergeTablesResult &src)
{
  int ret = OB_SUCCESS;
  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else if (OB_FAIL(handle_.assign(src.handle_))) {
    LOG_WARN("failed to copy handle", K(ret));
  } else {
    version_range_ = src.version_range_;
    merge_version_ = src.merge_version_;
    base_schema_version_ = src.base_schema_version_;
    schema_version_ = src.schema_version_;
    create_snapshot_version_ = src.create_snapshot_version_;
    checksum_method_ = src.checksum_method_;
    suggest_merge_type_ = src.suggest_merge_type_;
    schedule_major_ = src.schedule_major_;
    log_ts_range_ = src.log_ts_range_;
    dump_memtable_timestamp_ = src.dump_memtable_timestamp_;
  }
  return ret;
}

ObUpdateTableStoreParam::ObUpdateTableStoreParam(
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq)
  : table_handle_(),
    snapshot_version_(snapshot_version),
    clog_checkpoint_ts_(0),
    multi_version_start_(multi_version_start),
    keep_old_ddl_sstable_(true),
    need_report_(false),
    storage_schema_(storage_schema),
    rebuild_seq_(rebuild_seq),
    update_with_major_flag_(false),
    need_check_sstable_(false),
    ddl_checkpoint_ts_(0),
    ddl_start_log_ts_(0),
    ddl_snapshot_version_(0),
    ddl_execution_id_(0),
    ddl_cluster_version_(0),
    tx_data_(),
    binding_info_(),
    auto_inc_seq_()
{
}

ObUpdateTableStoreParam::ObUpdateTableStoreParam(
    const ObTableHandleV2 &table_handle,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq,
    const bool need_report,
    const int64_t clog_checkpoint_ts,
    const bool need_check_sstable)
  : table_handle_(table_handle),
    snapshot_version_(snapshot_version),
    clog_checkpoint_ts_(clog_checkpoint_ts),
    multi_version_start_(multi_version_start),
    keep_old_ddl_sstable_(true),
    need_report_(need_report),
    storage_schema_(storage_schema),
    rebuild_seq_(rebuild_seq),
    update_with_major_flag_(false),
    need_check_sstable_(need_check_sstable),
    ddl_checkpoint_ts_(0),
    ddl_start_log_ts_(0),
    ddl_snapshot_version_(0),
    ddl_execution_id_(0),
    ddl_cluster_version_(0),
    tx_data_(),
    binding_info_(),
    auto_inc_seq_()
{
}

ObUpdateTableStoreParam::ObUpdateTableStoreParam(
    const ObTableHandleV2 &table_handle,
    const int64_t snapshot_version,
    const bool keep_old_ddl_sstable,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq,
    const bool update_with_major_flag,
    const bool need_report)
  : table_handle_(table_handle),
    snapshot_version_(snapshot_version),
    clog_checkpoint_ts_(0),
    multi_version_start_(0),
    keep_old_ddl_sstable_(keep_old_ddl_sstable),
    need_report_(need_report),
    storage_schema_(storage_schema),
    rebuild_seq_(rebuild_seq),
    update_with_major_flag_(update_with_major_flag),
    need_check_sstable_(false),
    ddl_checkpoint_ts_(0),
    ddl_start_log_ts_(0),
    ddl_snapshot_version_(0),
    ddl_execution_id_(0),
    ddl_cluster_version_(0),
    tx_data_(),
    binding_info_(),
    auto_inc_seq_()
{
}

bool ObUpdateTableStoreParam::is_valid() const
{
  return multi_version_start_ >= ObVersionRange::MIN_VERSION
      && snapshot_version_ >= ObVersionRange::MIN_VERSION
      && nullptr != storage_schema_
      && storage_schema_->is_valid()
      && rebuild_seq_ >= 0;
}


ObBatchUpdateTableStoreParam::ObBatchUpdateTableStoreParam()
  : tables_handle_(),
    snapshot_version_(0),
    multi_version_start_(0),
    need_report_(false),
    rebuild_seq_(OB_INVALID_VERSION),
    update_logical_minor_sstable_(false),
    start_scn_(0),
    tablet_meta_(nullptr)
{
}

void ObBatchUpdateTableStoreParam::reset()
{
  tables_handle_.reset();
  multi_version_start_ = 0;
  need_report_ = false;
  rebuild_seq_ = OB_INVALID_VERSION;
  update_logical_minor_sstable_ = false;
  start_scn_ = 0;
  tablet_meta_ = nullptr;
}

bool ObBatchUpdateTableStoreParam::is_valid() const
{
  return snapshot_version_ >= 0
      && multi_version_start_ >= 0
      && rebuild_seq_ > OB_INVALID_VERSION
      && (!update_logical_minor_sstable_
          || (update_logical_minor_sstable_ && start_scn_ > 0 && OB_ISNULL(tablet_meta_)));
}

int ObBatchUpdateTableStoreParam::assign(
    const ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign batch update tablet store param get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(tables_handle_.assign(param.tables_handle_))) {
    LOG_WARN("failed to assign tables handle", K(ret), K(param));
  } else {
    multi_version_start_ = param.multi_version_start_;
    need_report_ = param.need_report_;
    rebuild_seq_ = param.rebuild_seq_;
    update_logical_minor_sstable_ = param.update_logical_minor_sstable_;
    start_scn_ = param.start_scn_;
    tablet_meta_ = param.tablet_meta_;
  }
  return ret;
}

int ObBatchUpdateTableStoreParam::get_max_clog_checkpoint_ts(int64_t &clog_checkpoint_ts) const
{
  int ret = OB_SUCCESS;
  clog_checkpoint_ts = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch update table store param is invalid", K(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle_.get_count(); ++i) {
      const ObITable *table = tables_handle_.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table));
      } else if (!table->is_multi_version_minor_sstable()) {
        //do nothing
      } else {
        clog_checkpoint_ts = std::max(clog_checkpoint_ts, table->get_end_log_ts());
      }
    }
  }
  return ret;
}

ObPartitionReadableInfo::ObPartitionReadableInfo()
  : min_log_service_ts_(0),
    min_trans_service_ts_(0),
    min_replay_engine_ts_(0),
    generated_ts_(0),
    max_readable_ts_(OB_INVALID_TIMESTAMP),
    force_(false)
{
}

ObPartitionReadableInfo::~ObPartitionReadableInfo()
{
}

bool ObPartitionReadableInfo::is_valid() const
{
  return min_replay_engine_ts_ > 0
    && min_trans_service_ts_ > 0
    && min_log_service_ts_ > 0
    && max_readable_ts_ > 0;
}

void ObPartitionReadableInfo::calc_readable_ts()
{
  // ignore current transaction by subtract 1
  max_readable_ts_ = MIN(MIN(min_log_service_ts_, min_replay_engine_ts_), min_trans_service_ts_) - 1;
  generated_ts_ = ObTimeUtility::current_time();
}

void ObPartitionReadableInfo::reset()
{
  min_log_service_ts_ = 0;
  min_trans_service_ts_ = 0;
  min_replay_engine_ts_ = 0;
  generated_ts_ = 0;
  max_readable_ts_ = OB_INVALID_TIMESTAMP;
  force_ = false;
}


int ObMigrateStatusHelper::trans_replica_op(const ObReplicaOpType &op_type, ObMigrateStatus &migrate_status)
{
  int ret = OB_SUCCESS;
  migrate_status = OB_MIGRATE_STATUS_MAX;

  if (!is_replica_op_valid(op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(op_type));
  } else {
    switch (op_type) {
    case ADD_REPLICA_OP: {
      migrate_status = OB_MIGRATE_STATUS_ADD;
      break;
    }
    case FAST_MIGRATE_REPLICA_OP:
    case MIGRATE_REPLICA_OP: {
      migrate_status = OB_MIGRATE_STATUS_MIGRATE;
      break;
    }
    case REBUILD_REPLICA_OP: {
      migrate_status = OB_MIGRATE_STATUS_REBUILD;
      break;
    }
    case CHANGE_REPLICA_OP: {
      migrate_status = OB_MIGRATE_STATUS_CHANGE;
      break;
    }
    case RESTORE_REPLICA_OP: {
      migrate_status = OB_MIGRATE_STATUS_RESTORE;
      break;
    }
    case COPY_GLOBAL_INDEX_OP: {
      migrate_status = OB_MIGRATE_STATUS_COPY_GLOBAL_INDEX;
      break;
    }
    case COPY_LOCAL_INDEX_OP: {
      migrate_status = OB_MIGRATE_STATUS_COPY_LOCAL_INDEX;
      break;
    }
    case RESTORE_FOLLOWER_REPLICA_OP: {
      migrate_status = OB_MIGRATE_STATUS_RESTORE_FOLLOWER;
      break;
    }
    case RESTORE_STANDBY_OP: {
      migrate_status = OB_MIGRATE_STATUS_RESTORE_STANDBY;
      break;
    }
    case LINK_SHARE_MAJOR_OP: {
      migrate_status = OB_MIGRATE_STATUS_LINK_MAJOR;
      break;
    }

    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("unknown op type", K(ret), K(op_type));
    }
    }
  }

  return ret;
}

int ObMigrateStatusHelper::trans_fail_status(const ObMigrateStatus &cur_status, ObMigrateStatus &fail_status)
{
  int ret = OB_SUCCESS;
  fail_status = OB_MIGRATE_STATUS_MAX;

  if (cur_status < OB_MIGRATE_STATUS_NONE || cur_status >= OB_MIGRATE_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_status));
  } else {
    switch (cur_status) {
    case OB_MIGRATE_STATUS_ADD: {
      fail_status = OB_MIGRATE_STATUS_ADD_FAIL;
      break;
    }
    case OB_MIGRATE_STATUS_MIGRATE: {
      fail_status = OB_MIGRATE_STATUS_MIGRATE_FAIL;
      break;
    }
    case OB_MIGRATE_STATUS_REBUILD: {
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_CHANGE: {
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_RESTORE: {
      //allow observer self reentry
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_COPY_GLOBAL_INDEX: {
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_COPY_LOCAL_INDEX: {
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_HOLD: {
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_RESTORE_FOLLOWER : {
      //allow observer self reentry
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_RESTORE_STANDBY : {
      //allow observer self reentry
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_LINK_MAJOR : {
      fail_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status));
    }
    }
  }
  return ret;
}

int ObMigrateStatusHelper::trans_reboot_status(const ObMigrateStatus &cur_status, ObMigrateStatus &reboot_status)
{
  int ret = OB_SUCCESS;
  reboot_status = OB_MIGRATE_STATUS_MAX;

  if (cur_status < OB_MIGRATE_STATUS_NONE || cur_status >= OB_MIGRATE_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_status));
  } else {
    switch (cur_status) {
    case OB_MIGRATE_STATUS_NONE: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_ADD:
    case OB_MIGRATE_STATUS_ADD_FAIL: {
      reboot_status = OB_MIGRATE_STATUS_ADD_FAIL;
      break;
    }
    case OB_MIGRATE_STATUS_MIGRATE:
    case OB_MIGRATE_STATUS_MIGRATE_FAIL: {
      reboot_status = OB_MIGRATE_STATUS_MIGRATE_FAIL;
      break;
    }
    case OB_MIGRATE_STATUS_REBUILD: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_CHANGE: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_RESTORE:
    case OB_MIGRATE_STATUS_RESTORE_FOLLOWER:
    case OB_MIGRATE_STATUS_RESTORE_STANDBY: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_RESTORE_FAIL: {
      reboot_status = OB_MIGRATE_STATUS_RESTORE_FAIL;
      break;
    }
    case OB_MIGRATE_STATUS_COPY_GLOBAL_INDEX: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_COPY_LOCAL_INDEX: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_HOLD: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    case OB_MIGRATE_STATUS_LINK_MAJOR: {
      reboot_status = OB_MIGRATE_STATUS_NONE;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status));
    }
    }
  }
  return ret;
}

int ObCreateSSTableParamExtraInfo::assign(const ObCreateSSTableParamExtraInfo &other)
{
  int ret = OB_SUCCESS;
  column_default_checksum_ = other.column_default_checksum_;
  column_cnt_ = other.column_cnt_;
  return ret;
}

ObRebuildListener::ObRebuildListener(transaction::ObLSTxCtxMgr &mgr)
  : ls_tx_ctx_mgr_(mgr)
{
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_.lock_minor_merge_lock())) {
    STORAGE_LOG(ERROR, "lock minor merge lock failed, we need retry forever", K(tmp_ret));
  }
}

ObRebuildListener::~ObRebuildListener()
{
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_.unlock_minor_merge_lock())) {
    STORAGE_LOG(ERROR, "unlock minor merge lock failed, we need retry forever", K(tmp_ret));
  }
}

bool ObRebuildListener::on_partition_rebuild()
{
  bool ret = false;

  if (ls_tx_ctx_mgr_.is_stopped()) {
    STORAGE_LOG(INFO, "rebuild listener find rebuild is on doing");
    ret = true;
  }

  return ret;
}

/***********************ObBackupRestoreTableSchemaChecker***************************/
int ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(
    const share::schema::ObTableSchema *table_schema,
    bool &need_skip,
    const bool is_restore_point)
{
  int ret = OB_SUCCESS;
  ObIndexStatus index_status;
  need_skip = true;
  int64_t table_id = 0;

  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check backup restore need skip table get invalid argument", K(ret), KP(table_schema));
  } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
  } else if (FALSE_IT(index_status = table_schema->get_index_status())) {
  } else if (table_schema->is_index_table()
             && (is_restore_point ?
                 !is_final_index_status(index_status) :
                 is_error_index_status(index_status))) {
    STORAGE_LOG(INFO, "restore table index is not expected status, skip it",
                K(is_restore_point), K(index_status), K(*table_schema));
  } else {
    need_skip = false;
  }
  return ret;
}

int ObRestoreFakeMemberListHelper::fake_restore_member_list(
    const int64_t replica_cnt,
    common::ObMemberList &fake_member_list)
{
  int ret = OB_SUCCESS;
  fake_member_list.reset();

  const char *fake_ip = "127.0.0.1";
  int32_t fake_port = 10000;

  if (replica_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fake restore member list get invalid argument", K(ret), K(replica_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_cnt; ++i) {
      fake_port  = fake_port + i;
      ObAddr fake_addr(ObAddr::IPV4, fake_ip, fake_port);
      ObMember fake_member(fake_addr, 0);
      if (!fake_member.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fake member is not valid", K(ret), K(fake_member));
      } else if (OB_FAIL(fake_member_list.add_member(fake_member))) {
        STORAGE_LOG(WARN, "failed to fake member list", K(ret), K(fake_member));
      }
    }
  }
  return ret;
}

