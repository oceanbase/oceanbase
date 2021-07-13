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
#include "share/ob_rpc_struct.h"
#include "ob_partition_split.h"
#include "storage/ob_tenant_file_struct.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"

using namespace oceanbase;
using namespace storage;
using namespace common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;

OB_SERIALIZE_MEMBER(ObReportStatus, data_version_, row_count_, row_checksum_, data_checksum_, data_size_,
    required_size_, snapshot_version_);

OB_SERIALIZE_MEMBER(ObPGReportStatus, data_version_, data_size_, required_size_, snapshot_version_);

OB_SERIALIZE_MEMBER(ObPartitionStoreMeta, pkey_, is_restore_, replica_type_, saved_split_state_, migrate_status_,
    migrate_timestamp_, storage_info_, report_status_, multi_version_start_, data_table_id_, split_info_,
    replica_property_, create_timestamp_);

ObPartitionStoreMeta::ObPartitionStoreMeta()
{
  reset();
}

ObPartitionStoreMeta::~ObPartitionStoreMeta()
{
  reset();
}

bool ObPartitionStoreMeta::is_valid() const
{
  bool is_valid = true;
  if (!pkey_.is_valid()) {
    is_valid = false;
  } else {
    if (pkey_.is_pg()) {
      is_valid = multi_version_start_ > 0 ? true : false;
    } else {
      is_valid = inner_is_valid();
    }
  }
  return is_valid;
}

bool ObPartitionStoreMeta::inner_is_valid() const
{
  return (pkey_.is_valid() && is_restore_ >= 0 && ObReplicaTypeCheck::is_replica_type_valid(replica_type_) &&
          storage_info_.is_valid() && multi_version_start_ > 0 && replica_property_.is_valid() &&
          create_timestamp_ >= 0);
}

void ObPartitionStoreMeta::reset()
{
  pkey_.reset();
  is_restore_ = 0;
  replica_type_ = REPLICA_TYPE_MAX;
  saved_split_state_ = static_cast<int64_t>(UNKNOWN_SPLIT_STATE);
  storage_info_.reset();

  report_status_.reset();

  migrate_status_ = OB_MIGRATE_STATUS_NONE;
  migrate_timestamp_ = 0;
  multi_version_start_ = 0;
  data_table_id_ = 0;
  split_info_.reset();
  replica_property_.reset();
  create_timestamp_ = 0;
}

int ObPartitionStoreMeta::deep_copy(const ObPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta is valid, cannot overwrite", K(ret), K(*this));
  } else if (OB_FAIL(storage_info_.deep_copy(meta.storage_info_))) {
    LOG_WARN("failed to copy saved storage info", K(ret));
  } else if (OB_FAIL(split_info_.assign(meta.split_info_))) {
    STORAGE_LOG(WARN, "failed to assign split info", K(ret), K(meta.split_info_));
  } else {
    pkey_ = meta.pkey_;
    is_restore_ = meta.is_restore_;
    migrate_status_ = meta.migrate_status_;
    migrate_timestamp_ = meta.migrate_timestamp_;
    replica_type_ = meta.replica_type_;
    saved_split_state_ = meta.saved_split_state_;
    report_status_ = meta.report_status_;
    multi_version_start_ = meta.multi_version_start_;
    data_table_id_ = meta.data_table_id_;
    replica_property_ = meta.replica_property_;
    create_timestamp_ = meta.create_timestamp_;
  }

  return ret;
}

ObPGPartitionStoreMeta::ObPGPartitionStoreMeta()
{
  reset();
}

ObPGPartitionStoreMeta::~ObPGPartitionStoreMeta()
{
  reset();
}

void ObPGPartitionStoreMeta::reset()
{
  pkey_.reset();
  report_status_.reset();
  multi_version_start_ = 0;
  data_table_id_ = 0;
  create_schema_version_ = 0;
  create_timestamp_ = 0;
  replica_type_ = ObReplicaType::REPLICA_TYPE_MAX;
}

bool ObPGPartitionStoreMeta::is_valid() const
{
  return pkey_.is_valid() && multi_version_start_ > 0 && create_schema_version_ >= 0 && create_timestamp_ >= 0;
}

int ObPGPartitionStoreMeta::deep_copy(const ObPGPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta is valid, cannot overwrite", K(ret), K(*this));
  } else {
    pkey_ = meta.pkey_;
    report_status_ = meta.report_status_;
    multi_version_start_ = meta.multi_version_start_;
    data_table_id_ = meta.data_table_id_;
    create_schema_version_ = meta.create_schema_version_;
    replica_type_ = meta.replica_type_;
    create_timestamp_ = meta.create_timestamp_;
  }
  return ret;
}

int ObPGPartitionStoreMeta::copy_from_old_meta(const ObPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta is valid, cannot overwrite", K(ret), K(*this));
  } else {
    pkey_ = meta.pkey_;
    report_status_ = meta.report_status_;
    multi_version_start_ = meta.multi_version_start_;
    data_table_id_ = meta.data_table_id_;
    replica_type_ = meta.replica_type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPGPartitionStoreMeta, pkey_, report_status_, multi_version_start_, data_table_id_,
    create_schema_version_, create_timestamp_);

OB_SERIALIZE_MEMBER(ObPartitionGroupMeta, pg_key_, is_restore_, replica_type_, replica_property_, saved_split_state_,
    migrate_status_, migrate_timestamp_, storage_info_, split_info_, partitions_, report_status_,
    create_schema_version_, ddl_seq_num_, create_timestamp_, create_frozen_version_, last_restore_log_id_,
    restore_snapshot_version_);

ObPartitionGroupMeta::ObPartitionGroupMeta()
{
  reset();
}

ObPartitionGroupMeta::~ObPartitionGroupMeta()
{
  reset();
}

bool ObPartitionGroupMeta::is_valid() const
{
  return pg_key_.is_valid() && (is_restore_ >= 0) && ObReplicaTypeCheck::is_replica_type_valid(replica_type_) &&
         replica_property_.is_valid() && storage_info_.is_valid() && create_schema_version_ >= 0 && ddl_seq_num_ >= 0 &&
         create_frozen_version_ >= 0 && restore_snapshot_version_ >= OB_INVALID_TIMESTAMP &&
         (is_restore_ != REPLICA_RESTORE_LOG || restore_snapshot_version_ > 0);
}

void ObPartitionGroupMeta::reset()
{
  pg_key_.reset();
  is_restore_ = 0;
  replica_type_ = REPLICA_TYPE_MAX;
  replica_property_.reset();
  saved_split_state_ = static_cast<int64_t>(UNKNOWN_SPLIT_STATE);
  storage_info_.reset();
  migrate_status_ = OB_MIGRATE_STATUS_NONE;
  migrate_timestamp_ = 0;
  split_info_.reset();
  partitions_.reset();
  create_schema_version_ = 0;
  ddl_seq_num_ = 0;
  create_timestamp_ = 0;
  create_frozen_version_ = 0;
  last_restore_log_id_ = OB_INVALID_ID;
  restore_snapshot_version_ = OB_INVALID_TIMESTAMP;
}

int ObPartitionGroupMeta::deep_copy(const ObPartitionGroupMeta& meta)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta is valid, cannot overwrite", K(ret), K(*this));
  } else if (this == &meta) {
    LOG_WARN("deep copy the same meta, no need to copy", K(meta), K(*this));
  } else if (OB_FAIL(storage_info_.deep_copy(meta.storage_info_))) {
    LOG_WARN("failed to copy saved storage info", K(ret), K(meta));
  } else if (OB_FAIL(partitions_.assign(meta.partitions_))) {
    LOG_WARN("failed to assign partitions", K(ret), K(meta));
  } else if (OB_FAIL(split_info_.assign(meta.split_info_))) {
    LOG_WARN("failed to assign split info", K(ret), K(meta));
  } else {
    pg_key_ = meta.pg_key_;
    is_restore_ = meta.is_restore_;
    migrate_status_ = meta.migrate_status_;
    migrate_timestamp_ = meta.migrate_timestamp_;
    replica_type_ = meta.replica_type_;
    replica_property_ = meta.replica_property_;
    saved_split_state_ = meta.saved_split_state_;
    report_status_ = meta.report_status_;
    create_schema_version_ = meta.create_schema_version_;
    ddl_seq_num_ = meta.ddl_seq_num_;
    create_timestamp_ = meta.create_timestamp_;
    create_frozen_version_ = meta.create_frozen_version_;
    last_restore_log_id_ = meta.last_restore_log_id_;
    restore_snapshot_version_ = meta.restore_snapshot_version_;
  }

  return ret;
}

int ObPartitionGroupMeta::copy_from_store_meta(const ObPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta is valid, cannot overwrite", K(ret), K(*this));
  } else if (OB_FAIL(storage_info_.deep_copy(meta.storage_info_))) {
    LOG_WARN("failed to copy storage info", K(ret), K(meta));
  } else if (OB_FAIL(split_info_.assign(meta.split_info_))) {
    LOG_WARN("failed to assign split info", K(ret), K(meta));
  } else {
    pg_key_ = meta.pkey_;
    is_restore_ = meta.is_restore_;
    migrate_status_ = meta.migrate_status_;
    migrate_timestamp_ = meta.migrate_timestamp_;
    replica_type_ = meta.replica_type_;
    saved_split_state_ = meta.saved_split_state_;
    report_status_.data_size_ = meta.report_status_.data_size_;
    report_status_.data_version_ = meta.report_status_.data_version_;
    report_status_.required_size_ = meta.report_status_.required_size_;
    replica_property_ = meta.replica_property_;
    create_schema_version_ = meta.storage_info_.get_data_info().get_schema_version();
    create_timestamp_ = meta.create_timestamp_;
  }
  return ret;
}

int ObPartitionGroupMeta::get_recover_info_for_flashback(const int64_t, ObRecoverPoint&)
{
  return OB_NOT_SUPPORTED;
}

int ObPartitionGroupMeta::clear_recover_points_for_physical_flashback(const int64_t)
{
  return OB_NOT_SUPPORTED;
}

int64_t ObPartitionGroupMeta::get_migrate_replay_log_ts() const
{
  int64_t replay_log_ts = 0;
  if (0 == partitions_.count() || !ObReplicaTypeCheck::is_replica_with_ssstore(replica_type_)) {
    replay_log_ts = storage_info_.get_clog_info().get_submit_timestamp();
  } else {
    replay_log_ts = storage_info_.get_data_info().get_last_replay_log_ts();
  }
  return replay_log_ts;
}

ObGetMergeTablesParam::ObGetMergeTablesParam()
    : merge_type_(INVALID_MERGE_TYPE),
      index_id_(OB_INVALID_ID),
      merge_version_(),
      trans_table_end_log_ts_(-1),
      trans_table_timestamp_(-1)
{}

bool ObGetMergeTablesParam::is_valid() const
{
  return OB_INVALID_ID != index_id_ && (merge_type_ > INVALID_MERGE_TYPE && merge_type_ < MERGE_TYPE_MAX) &&
         (!is_major_merge() || merge_version_.is_valid());
}

ObGetMergeTablesResult::ObGetMergeTablesResult()
    : version_range_(),
      handle_(),
      merge_version_(),
      base_schema_version_(-1),
      schema_version_(-1),
      create_snapshot_version_(-1),
      checksum_method_(-1),
      suggest_merge_type_(INVALID_MERGE_TYPE),
      create_sstable_for_large_snapshot_(false),
      log_ts_range_(),
      dump_memtable_timestamp_(0),
      read_base_version_(0)
{}

bool ObGetMergeTablesResult::is_valid() const
{
  return log_ts_range_.is_valid() && handle_.get_count() >= 1 && (merge_version_.is_valid() || 0 == merge_version_) &&
         base_schema_version_ >= 0 && schema_version_ >= 0 && create_snapshot_version_ >= 0 &&
         dump_memtable_timestamp_ >= 0 &&
         (suggest_merge_type_ > INVALID_MERGE_TYPE && suggest_merge_type_ < MERGE_TYPE_MAX);
}

void ObGetMergeTablesResult::reset()
{
  version_range_.reset();
  handle_.reset();
  base_handle_.reset();
  merge_version_.reset();
  base_schema_version_ = -1;
  schema_version_ = -1;
  create_snapshot_version_ = 0;
  suggest_merge_type_ = INVALID_MERGE_TYPE;
  create_sstable_for_large_snapshot_ = false;
  checksum_method_ = -1;
  log_ts_range_.reset();
  dump_memtable_timestamp_ = 0;
  read_base_version_ = 0;
}

int ObGetMergeTablesResult::deep_copy(const ObGetMergeTablesResult& src)
{
  int ret = OB_SUCCESS;
  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else if (OB_FAIL(handle_.assign(src.handle_))) {
    LOG_WARN("failed to copy handle", K(ret));
  } else if (OB_FAIL(base_handle_.assign(src.base_handle_))) {
    LOG_WARN("failed to copy base handle", K(ret));
  } else {
    version_range_ = src.version_range_;
    merge_version_ = src.merge_version_;
    base_schema_version_ = src.base_schema_version_;
    schema_version_ = src.schema_version_;
    create_snapshot_version_ = src.create_snapshot_version_;
    checksum_method_ = src.checksum_method_;
    suggest_merge_type_ = src.suggest_merge_type_;
    create_sstable_for_large_snapshot_ = src.create_sstable_for_large_snapshot_;
    log_ts_range_ = src.log_ts_range_;
    dump_memtable_timestamp_ = src.dump_memtable_timestamp_;
  }
  return ret;
}

AddTableParam::AddTableParam()
    : table_(NULL),
      max_kept_major_version_number_(-1),
      multi_version_start_(-1),
      in_slog_trans_(false),
      need_prewarm_(false),
      is_daily_merge_(false),
      complement_minor_sstable_(nullptr),
      backup_snapshot_version_(0),
      schema_version_(0)
{}

bool AddTableParam::is_valid() const
{
  return (!is_daily_merge_ || (OB_NOT_NULL(table_) || OB_NOT_NULL(complement_minor_sstable_))) &&
         max_kept_major_version_number_ >= 0 && multi_version_start_ > ObVersionRange::MIN_VERSION &&
         backup_snapshot_version_ >= 0;
}

ObPartitionReadableInfo::ObPartitionReadableInfo()
    : min_log_service_ts_(0),
      min_trans_service_ts_(0),
      min_replay_engine_ts_(0),
      generated_ts_(0),
      max_readable_ts_(OB_INVALID_TIMESTAMP),
      force_(false)
{}

ObPartitionReadableInfo::~ObPartitionReadableInfo()
{}

bool ObPartitionReadableInfo::is_valid() const
{
  return min_replay_engine_ts_ > 0 && min_trans_service_ts_ > 0 && min_log_service_ts_ > 0 && max_readable_ts_ > 0;
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

int ObMigrateStatusHelper::trans_replica_op(const ObReplicaOpType& op_type, ObMigrateStatus& migrate_status)
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

int ObMigrateStatusHelper::trans_fail_status(const ObMigrateStatus& cur_status, ObMigrateStatus& fail_status)
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
        fail_status = OB_MIGRATE_STATUS_RESTORE_FAIL;
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
      case OB_MIGRATE_STATUS_RESTORE_FOLLOWER: {
        fail_status = OB_MIGRATE_STATUS_RESTORE_FAIL;
        break;
      }
      case OB_MIGRATE_STATUS_RESTORE_STANDBY: {
        fail_status = OB_MIGRATE_STATUS_RESTORE_FAIL;
        break;
      }
      case OB_MIGRATE_STATUS_LINK_MAJOR: {
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

int ObMigrateStatusHelper::trans_reboot_status(const ObMigrateStatus& cur_status, ObMigrateStatus& reboot_status)
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
      case OB_MIGRATE_STATUS_RESTORE_STANDBY:
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

ObCreatePGParam::ObCreatePGParam()
    : info_(),
      is_restore_(0),
      replica_type_(ObReplicaType::REPLICA_TYPE_MAX),
      replica_property_(),
      data_version_(0),
      write_slog_(false),
      split_info_(),
      split_state_(0),
      create_timestamp_(0),
      file_handle_(nullptr),
      file_mgr_(nullptr),
      create_frozen_version_(0),
      last_restore_log_id_(OB_INVALID_ID),
      restore_snapshot_version_(OB_INVALID_TIMESTAMP),
      migrate_status_(ObMigrateStatus::OB_MIGRATE_STATUS_NONE)
{}

void ObCreatePGParam::reset()
{
  info_.reset();
  is_restore_ = 0;
  replica_type_ = ObReplicaType::REPLICA_TYPE_MAX;
  replica_property_.reset();
  data_version_ = 0;
  write_slog_ = false;
  split_info_.reset();
  split_state_ = 0;
  create_timestamp_ = 0;
  file_handle_ = nullptr;
  file_mgr_ = nullptr;
  create_frozen_version_ = 0;
  last_restore_log_id_ = OB_INVALID_ID;
  restore_snapshot_version_ = OB_INVALID_TIMESTAMP;
  migrate_status_ = ObMigrateStatus::OB_MIGRATE_STATUS_NONE;
}

bool ObCreatePGParam::is_valid() const
{
  return info_.is_valid() && ObReplicaTypeCheck::is_replica_type_valid(replica_type_) && replica_property_.is_valid() &&
         (is_restore_ >= 0) && create_timestamp_ >= 0 && nullptr != file_handle_ && nullptr != file_mgr_ &&
         create_frozen_version_ >= 0 && restore_snapshot_version_ >= OB_INVALID_TIMESTAMP &&
         migrate_status_ < ObMigrateStatus::OB_MIGRATE_STATUS_MAX;
}

int ObCreatePGParam::assign(const ObCreatePGParam& param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create pg param is invalid argument", K(ret), K(param));
  } else if (OB_FAIL(info_.deep_copy(param.info_))) {
    LOG_WARN("failed to copy saved storage info", K(ret), K(param));
  } else if (OB_FAIL(split_info_.assign(param.split_info_))) {
    LOG_WARN("failed to assign split info", K(ret), K(param));
  } else {
    is_restore_ = param.is_restore_;
    replica_type_ = param.replica_type_;
    replica_property_ = param.replica_property_;
    data_version_ = param.data_version_;
    write_slog_ = param.write_slog_;
    split_state_ = param.split_state_;
    create_timestamp_ = param.create_timestamp_;
    file_handle_ = param.file_handle_;
    file_mgr_ = param.file_mgr_;
    create_frozen_version_ = param.create_frozen_version_;
    last_restore_log_id_ = param.last_restore_log_id_;
    restore_snapshot_version_ = param.restore_snapshot_version_;
    migrate_status_ = param.migrate_status_;
  }
  return ret;
}

int ObCreatePGParam::set_storage_info(const ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(info_.deep_copy(info))) {
    LOG_WARN("failed to deep copy saved storage info", K(ret), K(info));
  }
  return ret;
}

int ObCreatePGParam::set_split_info(const ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(split_info_.assign(split_info))) {
    LOG_WARN("failed to assign split info", K(ret), K(split_info));
  }
  return ret;
}
void ObCreatePartitionMeta::reset()
{
  is_valid_ = false;
  table_id_ = OB_INVALID_ID;
  data_table_id_ = OB_INVALID_ID;
  progressive_merge_round_ = 0;
  is_global_index_table_ = false;
  table_type_ = ObTableType::USER_TABLE;
  table_mode_.reset();
  index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  rowkey_column_num_ = 0;
  column_ids_.reset();
  id_hash_array_ = NULL;
}

int ObCreatePartitionMeta::extract_from(const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_schema));
  } else if (OB_FAIL(table_schema.get_store_column_ids(column_ids_))) {
    LOG_WARN("failed to get_store_column_ids", K(ret), K(table_schema));
  } else {
    table_id_ = table_schema.get_table_id();
    data_table_id_ = table_schema.get_data_table_id();
    progressive_merge_round_ = table_schema.get_progressive_merge_round();
    is_global_index_table_ = table_schema.is_global_index_table();
    table_type_ = table_schema.get_table_type();
    table_mode_ = table_schema.get_table_mode_struct();
    index_type_ = table_schema.get_index_type();
    rowkey_column_num_ = table_schema.get_rowkey_column_num();
    id_hash_array_ = table_schema.get_id_hash_array();
    is_valid_ = true;
  }
  return ret;
}

const ObColumnSchemaV2* ObCreatePartitionMeta::get_column_schema(const uint64_t column_id) const
{
  ObColumnSchemaV2* column = NULL;
  if (NULL != id_hash_array_) {
    if (OB_SUCCESS != id_hash_array_->get_refactored(ObColumnIdKey(column_id), column)) {
      column = NULL;
    }
  }
  return column;
}

int ObCreatePartitionMeta::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", KR(ret), K(new_tenant_id), K(*this));
  } else if (new_tenant_id == extract_tenant_id(table_id_)) {
    // no need to replace
  } else {
    table_id_ = combine_id(new_tenant_id, table_id_);
    data_table_id_ = combine_id(new_tenant_id, data_table_id_);
  }
  return ret;
}

int ObCreatePartitionMeta::assign(const ObCreatePartitionMeta& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_ids_.assign(other.get_store_column_ids()))) {
    STORAGE_LOG(WARN, "failed to assign column_ids", KR(ret), K(other), K(*this));
  } else {
    is_valid_ = other.is_valid();
    table_id_ = other.get_table_id();
    data_table_id_ = other.get_data_table_id();
    progressive_merge_round_ = other.get_progressive_merge_round();
    is_global_index_table_ = other.is_global_index_table();
    table_type_ = other.get_table_type();
    table_mode_ = other.get_table_mode();
    index_type_ = other.get_index_type();
    rowkey_column_num_ = other.get_rowkey_column_num();
    id_hash_array_ = other.get_id_hash_array();
  }
  return ret;
}

void ObCreatePartitionParam::reset()
{
  is_valid_ = false;
  member_list_.reset();
  partition_key_.reset();
  need_create_sstable_ = false;
  ;
  schema_version_ = 0;
  memstore_version_ = 0;
  lease_start_ = 0;
  replica_type_ = common::REPLICA_TYPE_MAX;
  restore_ = share::REPLICA_NOT_RESTORE;
  frozen_timestamp_ = 0;
  pg_key_.reset();
  schemas_.reset();
}

int ObCreatePartitionParam::extract_from(const obrpc::ObCreatePartitionArg& arg)
{
  int ret = OB_SUCCESS;
  const int64_t table_cnt = arg.table_schemas_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_cnt; ++i) {
    const ObTableSchema& table_schema = arg.table_schemas_.at(i);
    ObCreatePartitionMeta partition_schema;
    if (OB_FAIL(partition_schema.extract_from(table_schema))) {
      STORAGE_LOG(WARN, "failed to extract partition_schema", KR(ret), K(table_schema));
    } else if (OB_FAIL(schemas_.push_back(partition_schema))) {
      STORAGE_LOG(WARN, "member list deep copy error", KR(ret), K(partition_schema));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(member_list_.deep_copy(arg.member_list_))) {
      STORAGE_LOG(WARN, "member list deep copy error", K(arg.member_list_));
    } else if (OB_FAIL(arg.check_need_create_sstable(need_create_sstable_))) {
      STORAGE_LOG(WARN, "failed to check_nened_create_sstable", K(arg));
    } else {
      partition_key_ = arg.partition_key_;
      schema_version_ = arg.schema_version_;
      memstore_version_ = arg.memstore_version_;
      lease_start_ = arg.lease_start_;
      replica_type_ = arg.replica_type_;
      restore_ = arg.restore_;
      frozen_timestamp_ = arg.frozen_timestamp_;
      pg_key_ = arg.pg_key_;
      is_valid_ = true;
    }
  }
  return ret;
}

int ObCreatePartitionParam::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", KR(ret), K(new_tenant_id), K(*this));
  } else if (new_tenant_id == partition_key_.get_tenant_id()) {
    // no need to replace
  } else if (OB_FAIL(partition_key_.replace_tenant_id(new_tenant_id))) {
    STORAGE_LOG(WARN, "failed to replace_tenant_id of partition_key_", KR(ret), K(new_tenant_id), K(*this));
  } else if (OB_FAIL(pg_key_.replace_tenant_id(new_tenant_id))) {
    STORAGE_LOG(WARN, "failed to replace_tenant_id of pg_key_", KR(ret), K(new_tenant_id), K(*this));
  } else {
    const int64_t cnt = schemas_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      ObCreatePartitionMeta& partition_schema = schemas_.at(i);
      if (OB_FAIL(partition_schema.replace_tenant_id(new_tenant_id))) {
        STORAGE_LOG(WARN, "failed to replace_tenant_id of partition_scheam", KR(ret), K(*this));
      }
    }
  }
  return ret;
}

/**********************ObRecoveryPointSchemaFilter***********************/
ObRecoveryPointSchemaFilter::ObRecoveryPointSchemaFilter()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      tenant_recovery_point_schema_version_(OB_INVALID_VERSION),
      tenant_current_schema_version_(OB_INVALID_VERSION),
      recovery_point_schema_guard_(),
      current_schema_guard_(),
      is_schema_version_same_(false)
{}

ObRecoveryPointSchemaFilter::~ObRecoveryPointSchemaFilter()
{}

bool ObRecoveryPointSchemaFilter::is_inited() const
{
  return is_inited_;
}

int ObRecoveryPointSchemaFilter::init(const int64_t tenant_id, const int64_t tenant_recovery_point_schema_version,
    const int64_t tenant_current_schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "appender already inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id || tenant_recovery_point_schema_version < OB_INVALID_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "init backup schema checker get invalid argument",
        K(ret),
        K(tenant_id),
        K(tenant_recovery_point_schema_version),
        K(tenant_current_schema_version));
  } else {
    is_schema_version_same_ = (tenant_recovery_point_schema_version == tenant_current_schema_version);
    ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();
    if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
            tenant_id, schema_service, tenant_recovery_point_schema_version, recovery_point_schema_guard_))) {
      STORAGE_LOG(WARN,
          "failed to get tenant backup schema guard",
          K(ret),
          K(tenant_id),
          K(tenant_recovery_point_schema_version));
    } else if (!is_schema_version_same_ &&
               OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                   tenant_id, schema_service, tenant_current_schema_version, current_schema_guard_))) {
      STORAGE_LOG(WARN, "failed to get tenant current schema guard", K(ret), K(tenant_current_schema_version));
    } else {
      tenant_id_ = tenant_id;
      tenant_recovery_point_schema_version_ = tenant_recovery_point_schema_version;
      tenant_current_schema_version_ = tenant_current_schema_version;
      is_inited_ = true;
      STORAGE_LOG(INFO,
          "init backup schema filter succ",
          K(tenant_id),
          K(tenant_recovery_point_schema_version),
          K(tenant_current_schema_version));
    }
  }

  return ret;
}

int ObRecoveryPointSchemaFilter::check_partition_exist(const ObPartitionKey pkey, bool& is_exist)
{
  int ret = OB_SUCCESS;
  bool check_dropped_partition = false;
  is_exist = false;
  ObSchemaGetterGuard* schema_guard = NULL;
  bool is_exist_in_backup_schema = false;
  bool is_exist_in_delay_delete = false;
  bool is_exist_in_current_schema = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup schema filter is not init", K(ret), K(pkey));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "check partition exist get invalid argument", K(ret), K(pkey));
  } else if (FALSE_IT(schema_guard = &recovery_point_schema_guard_)) {
  } else if (OB_FAIL(check_partition_exist_(pkey, check_dropped_partition, *schema_guard, is_exist_in_backup_schema))) {
    STORAGE_LOG(WARN, "failed to check partition exist", K(ret), K(pkey));
  } else if (FALSE_IT(check_dropped_partition = true)) {
  } else if (OB_FAIL(check_partition_exist_(pkey, check_dropped_partition, *schema_guard, is_exist_in_delay_delete))) {
    STORAGE_LOG(WARN, "failed to check partition exist", K(ret), K(pkey));
  } else if (is_schema_version_same_) {
    is_exist = is_exist_in_backup_schema;
  } else if (FALSE_IT(schema_guard = &current_schema_guard_)) {
  } else if (OB_FAIL(
                 check_partition_exist_(pkey, check_dropped_partition, *schema_guard, is_exist_in_current_schema))) {
    STORAGE_LOG(WARN, "failed to check partition exist", K(ret), K(pkey));
  } else if (is_exist_in_backup_schema && !is_exist_in_current_schema) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "table exist in backup schema, but not exist in current schema",
        K(ret),
        K(tenant_recovery_point_schema_version_),
        K(tenant_current_schema_version_));
  } else if (!is_exist_in_backup_schema && is_exist_in_delay_delete) {
    is_exist = false;
  } else {
    is_exist = is_exist_in_backup_schema || is_exist_in_current_schema;
  }
  return ret;
}

int ObRecoveryPointSchemaFilter::check_partition_exist_(
    const ObPartitionKey pkey, const bool check_dropped_partition, ObSchemaGetterGuard& schema_guard, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup schema filter is not init", K(ret), K(pkey));
  } else if (pkey.is_trans_table()) {
    is_exist = true;
  } else if (OB_FAIL(schema_guard.check_partition_exist(
                 pkey.get_table_id(), pkey.get_partition_id(), check_dropped_partition, is_exist))) {
    STORAGE_LOG(WARN, "schema guard check partition fail", K(ret), K(pkey));
  }
  return ret;
}

int ObRecoveryPointSchemaFilter::check_table_exist(const uint64_t table_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObSchemaGetterGuard* schema_guard = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup schema filter is not init", K(ret), K(table_id));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "check table exist get invalid argument", K(ret), K(table_id));
  } else if (FALSE_IT(schema_guard = &recovery_point_schema_guard_)) {
  } else if (OB_FAIL(check_table_exist_(table_id, *schema_guard, is_exist))) {
    STORAGE_LOG(WARN, "failed to check table exist", K(ret), K(table_id));
  } else if (is_exist) {
    // do nothing
  } else if (is_schema_version_same_) {
    // do nothing
  } else if (FALSE_IT(schema_guard = &current_schema_guard_)) {
  } else if (OB_FAIL(check_table_exist_(table_id, *schema_guard, is_exist))) {
    STORAGE_LOG(WARN, "failed to check table exist", K(ret), K(table_id));
  }
  return ret;
}

int ObRecoveryPointSchemaFilter::check_table_exist_(
    const uint64_t table_id, ObSchemaGetterGuard& schema_guard, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  const ObTableSchema* table_schema = NULL;
  bool need_skip = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup schema filter is not init", K(ret), K(table_id));
  } else if (is_trans_table_id(table_id)) {
    is_exist = true;
  } else if (OB_FAIL(schema_guard.check_table_exist(table_id, is_exist))) {
    STORAGE_LOG(WARN, "schema guard check table fail", K(ret), K(table_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(table_id));
  } else if (OB_FAIL(
                 ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(table_schema, need_skip))) {
    LOG_WARN("failed to check backup restore need skip table", K(ret), K(table_id));
  } else if (!need_skip) {
    // do nothing
  } else {
    is_exist = false;
  }
  return ret;
}

int ObRecoveryPointSchemaFilter::do_filter_tables(common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> backup_tables;
  bool filtered = false;

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "filter not inited", K(ret));
  } else {
    bool is_exist = false;
    for (int i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      uint64_t table_id = table_ids.at(i);
      if (OB_FAIL(check_table_exist(table_id, is_exist))) {
        STORAGE_LOG(WARN, "backup filter check table failed", K(ret), K(table_id));
      } else if (is_exist) {
        if (OB_FAIL(backup_tables.push_back(table_id))) {
          STORAGE_LOG(WARN, "push back table id failed", K(ret), K(table_id));
        }
      } else {
        filtered = true;
        FLOG_INFO("backup table is not exist, no need backup", K(table_id));

#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          ret = E(EventTable::EN_BACKUP_FILTER_TABLE_BY_SCHEMA) OB_SUCCESS;
        }
#endif
      }
    }
  }

  if (OB_SUCC(ret) && filtered) {
    table_ids.reuse();
    if (OB_FAIL(table_ids.assign(backup_tables))) {
      STORAGE_LOG(WARN, "assign table id failed", K(ret), K(backup_tables));
    }
  }
  return ret;
}

int ObRecoveryPointSchemaFilter::do_filter_pg_partitions(
    const ObPartitionKey& pg_key, common::ObPartitionArray& partitions)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "filter not inited", K(ret));
  } else {
    bool is_exist = false;
    bool filtered = false;
    ObPartitionArray exist_partitions;
    for (int i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      const ObPartitionKey& pkey = partitions.at(i);
      if (OB_FAIL(check_partition_exist(pkey, is_exist))) {
        STORAGE_LOG(WARN, "backup filter check partition failed", K(ret), K(pg_key), K(pkey));
      } else if (is_exist) {
        if (OB_FAIL(exist_partitions.push_back(pkey))) {
          STORAGE_LOG(WARN, "pusk back partition failed", K(ret), K(pkey));
        }
      } else {
        filtered = true;
        STORAGE_LOG(INFO, "backup partition is filtered", K(pkey));
      }
    }
    if (OB_SUCC(ret) && filtered) {
      partitions.reuse();
      if (OB_FAIL(partitions.assign(exist_partitions))) {
        STORAGE_LOG(WARN, "assign pg partitions failed", K(ret), K(exist_partitions));
      }
    }
  }

  return ret;
}

int ObRecoveryPointSchemaFilter::check_if_table_miss_by_schema(
    const ObPartitionKey& pgkey, const hash::ObHashSet<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObArray<uint64_t> schema_tables;
  ObSchemaGetterGuard* schema_guard = NULL;

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "filter not inited", K(ret));
  } else if (FALSE_IT(schema_guard = &recovery_point_schema_guard_)) {
  } else if (OB_FAIL(check_if_table_miss_by_schema_(pgkey, table_ids, *schema_guard))) {
    STORAGE_LOG(WARN, "failed to check if table miss by schema", K(ret), K(pgkey));
  } else if (is_schema_version_same_) {
    // do nothing
  } else if (FALSE_IT(schema_guard = &current_schema_guard_)) {
  } else if (OB_FAIL(check_if_table_miss_by_schema_(pgkey, table_ids, *schema_guard))) {
    STORAGE_LOG(WARN, "failed to check if table miss by schema", K(ret), K(pgkey));
  }
  return ret;
}

int ObRecoveryPointSchemaFilter::check_if_table_miss_by_schema_(const ObPartitionKey& pgkey,
    const common::hash::ObHashSet<uint64_t>& table_ids, share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObArray<uint64_t> schema_tables;

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "filter not inited", K(ret));
  } else if (OB_FAIL(get_table_ids_in_pg_(pgkey, schema_tables, schema_guard))) {
    STORAGE_LOG(WARN, "get pg tables fail", K(ret), K(pgkey));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < schema_tables.count(); ++i) {
      hash_ret = table_ids.exist_refactored(schema_tables.at(i));
      if (OB_HASH_EXIST == hash_ret) {
        // do nothing
      } else {
        ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
        STORAGE_LOG(
            WARN, "backup table is miss", K(ret), K(hash_ret), K(pgkey), K(table_ids), K(i), K(schema_tables.at(i)));
      }
    }
  }
  return ret;
}

int ObRecoveryPointSchemaFilter::get_table_ids_in_pg_(const ObPartitionKey& pgkey,
    common::ObIArray<uint64_t>& table_ids, share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> data_table_ids;
  common::ObArray<uint64_t> local_index_ids;
  ObArray<ObIndexTableStat> index_stats;
  table_ids.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup schema filter is not init", K(ret), K(pgkey));
  } else if (pgkey.is_pg()) {
    if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(
            pgkey.get_tenant_id(), pgkey.get_tablegroup_id(), data_table_ids))) {
      STORAGE_LOG(WARN, "get pg table ids fail", K(ret), K(pgkey));
    }
  } else if (OB_FAIL(data_table_ids.push_back(pgkey.get_table_id()))) {
    STORAGE_LOG(WARN, "push back data table id fail", K(ret), K(pgkey));
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_table_ids.count(); ++i) {
      const uint64_t data_table_id = data_table_ids.at(i);
      const ObTableSchema* table_schema = NULL;

      if (OB_FAIL(schema_guard.get_table_schema(data_table_id, table_schema))) {
        STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(data_table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "table schema should not be NULL", K(ret), KP(table_schema));
      } else if (table_schema->is_dropped_schema()) {
        // do nothing
        FLOG_INFO("table is dropped schema, skip check", K(data_table_id));
      } else if (OB_FAIL(table_ids.push_back(data_table_id))) {
        STORAGE_LOG(WARN, "failed to push data table id into array", K(ret), K(data_table_id));
      } else if (OB_FAIL(schema_guard.get_index_status(data_table_id, false /*with global index*/, index_stats))) {
        STORAGE_LOG(WARN, "get local index status fail", K(ret), K(pgkey));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < index_stats.count(); ++j) {
          const ObTableSchema* index_table_schema = NULL;
          const ObIndexTableStat& index_table_stat = index_stats.at(j);
          if (INDEX_STATUS_AVAILABLE != index_table_stat.index_status_) {
            // filter unavailable index
          } else if (OB_FAIL(schema_guard.get_table_schema(index_table_stat.index_id_, index_table_schema))) {
            STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(index_table_stat));
          } else if (OB_ISNULL(index_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "index table schema should not be NULL", K(ret), KP(index_table_schema));
          } else if (index_table_schema->is_dropped_schema()) {
            // do nothing
            FLOG_INFO("index table is dropped schema, skip check", K(data_table_id), K(index_table_stat));
          } else if (OB_FAIL(table_ids.push_back(index_stats.at(j).index_id_))) {
            STORAGE_LOG(WARN, "push back index id fail", K(ret), K(pgkey));
          }
        }
      }
    }
  }
  return ret;
}

/***********************ObBackupRestoreTableSchemaChecker***************************/
int ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(
    const share::schema::ObTableSchema* table_schema, bool& need_skip)
{
  int ret = OB_SUCCESS;
  ObIndexStatus index_status;
  need_skip = true;
  int64_t table_id = 0;

  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check backup restore need skip table get invalid argument", K(ret), KP(table_schema));
  } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
  } else if (table_schema->is_dropped_schema()) {
    STORAGE_LOG(INFO, "table is dropped, skip it", K(table_id));
  } else if (FALSE_IT(index_status = table_schema->get_index_status())) {
  } else if (table_schema->is_index_table() && ObIndexStatus::INDEX_STATUS_AVAILABLE != index_status) {
    STORAGE_LOG(INFO, "restore table is not available index, skip it", K(index_status), K(*table_schema));
  } else {
    need_skip = false;
  }
  return ret;
}

ObRebuildListener::ObRebuildListener(transaction::ObPartitionTransCtxMgr& mgr) : ob_partition_ctx_mgr_(mgr)
{
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS != (tmp_ret = ob_partition_ctx_mgr_.lock_minor_merge_lock())) {
    STORAGE_LOG(ERROR, "lock minor merge lock failed, we need retry forever", K(tmp_ret));
  }
}

ObRebuildListener::~ObRebuildListener()
{
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS != (tmp_ret = ob_partition_ctx_mgr_.unlock_minor_merge_lock())) {
    STORAGE_LOG(ERROR, "unlock minor merge lock failed, we need retry forever", K(tmp_ret));
  }
}

bool ObRebuildListener::on_partition_rebuild()
{
  bool ret = false;

  if (ob_partition_ctx_mgr_.is_partition_stopped()) {
    STORAGE_LOG(INFO, "rebuild listener find rebuild is on doing");
    ret = true;
  }

  return ret;
}

int ObRestoreFakeMemberListHelper::fake_restore_member_list(
    const int64_t replica_cnt, common::ObMemberList& fake_member_list)
{
  int ret = OB_SUCCESS;
  fake_member_list.reset();

  const char* fake_ip = "127.0.0.1";
  int32_t fake_port = 10000;

  if (replica_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fake restore member list get invalid argument", K(ret), K(replica_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_cnt; ++i) {
      fake_port = fake_port + i;
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
