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
using namespace compaction;
using namespace common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;


#ifdef ERRSIM
static const char *OB_ERRSIM_POINT_TYPES[] = {
    "POINT_NONE",
    "START_BACKFILL_BEFORE",
    "REPLACE_SWAP_BEFORE",
    "REPLACE_AFTER",
};

void ObErrsimBackfillPointType::reset()
{
  type_ = ObErrsimBackfillPointType::ERRSIM_POINT_NONE;
}

bool ObErrsimBackfillPointType::is_valid() const
{
  return true;
}

bool ObErrsimBackfillPointType::operator == (const ObErrsimBackfillPointType &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else {
    is_same = type_ == other.type_;
  }
  return is_same;
}

int64_t ObErrsimBackfillPointType::hash() const
{
  int64_t hash_value = 0;
  hash_value = common::murmurhash(
      &type_, sizeof(type_), hash_value);
  return hash_value;
}

int ObErrsimBackfillPointType::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObErrsimBackfillPointType, type_);

ObErrsimTransferBackfillPoint::ObErrsimTransferBackfillPoint()
  : point_type_(ObErrsimBackfillPointType::ERRSIM_MODULE_MAX),
    point_start_time_(0)
{
}

ObErrsimTransferBackfillPoint::~ObErrsimTransferBackfillPoint()
{
}

bool ObErrsimTransferBackfillPoint::is_valid() const
{
  return point_type_.is_valid() && point_start_time_ > 0;
}

int ObErrsimTransferBackfillPoint::set_point_type(const ObErrsimBackfillPointType &point_type)
{
  int ret = OB_SUCCESS;
  if (!point_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("point type is invalid", K(ret), K(point_type));
  } else if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The point type is in effect, reset is not allowed", K(ret), K(point_type_), K(point_type));
  } else {
    point_type_ = point_type;
  }

  return ret;
}
int ObErrsimTransferBackfillPoint::set_point_start_time(int64_t start_time)
{
  int ret = OB_SUCCESS;
  if (start_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("point type is invalid", K(ret), K(start_time));
  } else if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The point type is in effect, reset is not allowed", K(ret), K(point_start_time_), K(start_time));
  } else {
    point_start_time_ = start_time;
  }

  return ret;
}

void ObErrsimTransferBackfillPoint::reset()
{
  point_type_.type_ = ObErrsimBackfillPointType::ERRSIM_MODULE_MAX;
  point_start_time_ = 0;
}

bool ObErrsimTransferBackfillPoint::is_errsim_point(const ObErrsimBackfillPointType &point_type) const
{
  bool is_point = false;
  if (!is_valid()) {
    is_point = false;
  } else if (point_type.type_ == point_type_.type_) {
    is_point = true;
  } else {
    is_point = false;
  }
  return is_point;
}

#endif

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
  : state_(BARRIER_LOG_INIT), log_id_(0), scn_(), schema_version_(0)
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

void ObPartitionBarrierLogState::set_log_info(const ObPartitionBarrierLogStateEnum state, const int64_t log_id, const SCN &scn, const int64_t schema_version)
{
  state_ = state;
  log_id_ = log_id;
  scn_ = scn;
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
  } else if (OB_FAIL(scn_.fixed_serialize(buf, buf_len, pos))) {
    LOG_WARN("fix serialized failed", K(ret));
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
  } else if (OB_FAIL(scn_.fixed_deserialize(buf, data_len, pos))) {
    LOG_WARN("fixed deserialize failed", K(ret));
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
  len += scn_.get_fixed_serialize_size();
  return len;
}


ObGetMergeTablesParam::ObGetMergeTablesParam()
  : merge_type_(INVALID_MERGE_TYPE),
    merge_version_(0)
{
}

bool ObGetMergeTablesParam::is_valid() const
{
  return is_valid_merge_type(merge_type_)
    && (!compaction::is_major_merge_type(merge_type_) || merge_version_ > 0);
}

ObGetMergeTablesResult::ObGetMergeTablesResult()
  : version_range_(),
    handle_(),
    merge_version_(),
    update_tablet_directly_(false),
    schedule_major_(false),
    is_simplified_(false),
    scn_range_(),
    error_location_(nullptr),
    snapshot_info_(),
    is_backfill_(false),
    backfill_scn_()
{
}

bool ObGetMergeTablesResult::is_valid() const
{
  return scn_range_.is_valid()
      && (is_simplified_ || handle_.get_count() >= 1)
      && merge_version_ >= 0
      && (!is_backfill_ || backfill_scn_.is_valid());
}

void ObGetMergeTablesResult::reset_handle_and_range()
{
  handle_.reset();
  version_range_.reset();
  scn_range_.reset();
}

void ObGetMergeTablesResult::simplify_handle()
{
  handle_.reset();
  is_simplified_ = true;
}

void ObGetMergeTablesResult::reset()
{
  version_range_.reset();
  handle_.reset();
  merge_version_ = ObVersionRange::MIN_VERSION;
  schedule_major_ = false;
  scn_range_.reset();
  error_location_ = nullptr;
  is_simplified_ = false;
  snapshot_info_.reset();
  is_backfill_ = false;
  backfill_scn_.reset();
}

int ObGetMergeTablesResult::copy_basic_info(const ObGetMergeTablesResult &src)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else {
    version_range_ = src.version_range_;
    merge_version_ = src.merge_version_;
    schedule_major_ = src.schedule_major_;
    scn_range_ = src.scn_range_;
    error_location_ = src.error_location_;
    is_simplified_ = src.is_simplified_;
    is_backfill_ = src.is_backfill_;
    backfill_scn_ = src.backfill_scn_;
  }
  return ret;
}

int ObGetMergeTablesResult::assign(const ObGetMergeTablesResult &src)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else if (OB_FAIL(handle_.assign(src.handle_))) {
    LOG_WARN("failed to assign table handle", K(ret), K(src));
  } else if (OB_FAIL(copy_basic_info(src))) {
    LOG_WARN("failed to copy basic info", K(ret), K(src));
  }
  return ret;
}

share::SCN ObGetMergeTablesResult::get_merge_scn() const
{
  return is_backfill_ ? backfill_scn_ : scn_range_.end_scn_;
}

ObDDLTableStoreParam::ObDDLTableStoreParam()
  : keep_old_ddl_sstable_(true),
    ddl_start_scn_(SCN::min_scn()),
    ddl_commit_scn_(SCN::min_scn()),
    ddl_checkpoint_scn_(SCN::min_scn()),
    ddl_snapshot_version_(0),
    ddl_execution_id_(-1),
    data_format_version_(0)
{

}

bool ObDDLTableStoreParam::is_valid() const
{
  return ddl_start_scn_.is_valid()
    && ddl_commit_scn_.is_valid()
    && ddl_checkpoint_scn_.is_valid()
    && ddl_snapshot_version_ >= 0
    && ddl_execution_id_ >= 0
    && data_format_version_ >= 0;
}

UpdateUpperTransParam::UpdateUpperTransParam()
  : new_upper_trans_(nullptr),
    last_minor_end_scn_()
{
  last_minor_end_scn_.set_min();
}

UpdateUpperTransParam::~UpdateUpperTransParam()
{
  reset();
}

void UpdateUpperTransParam::reset()
{
  new_upper_trans_ = nullptr;
  last_minor_end_scn_.set_min();
}

ObUpdateTableStoreParam::ObUpdateTableStoreParam(
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq)
  : sstable_(nullptr),
    snapshot_version_(snapshot_version),
    clog_checkpoint_scn_(),
    multi_version_start_(multi_version_start),
    need_report_(false),
    storage_schema_(storage_schema),
    rebuild_seq_(rebuild_seq),
    update_with_major_flag_(false),
    need_check_sstable_(false),
    ddl_info_(),
    allow_duplicate_sstable_(false),
    need_check_transfer_seq_(false),
    transfer_seq_(-1),
    merge_type_(MERGE_TYPE_MAX),
    upper_trans_param_()
{
  clog_checkpoint_scn_.set_min();
}

ObUpdateTableStoreParam::ObUpdateTableStoreParam(
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq,
    const UpdateUpperTransParam upper_trans_param)
  : sstable_(nullptr),
    snapshot_version_(snapshot_version),
    clog_checkpoint_scn_(),
    multi_version_start_(multi_version_start),
    need_report_(false),
    storage_schema_(storage_schema),
    rebuild_seq_(rebuild_seq),
    update_with_major_flag_(false),
    need_check_sstable_(false),
    ddl_info_(),
    allow_duplicate_sstable_(false),
    need_check_transfer_seq_(false),
    transfer_seq_(-1),
    merge_type_(MERGE_TYPE_MAX),
    upper_trans_param_(upper_trans_param)
{
  clog_checkpoint_scn_.set_min();
}

ObUpdateTableStoreParam::ObUpdateTableStoreParam(
    const blocksstable::ObSSTable *sstable,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq,
    const bool need_check_transfer_seq,
    const int64_t transfer_seq,
    const bool need_report,
    const SCN clog_checkpoint_scn,
    const bool need_check_sstable,
    const bool allow_duplicate_sstable,
    const ObMergeType merge_type)
  : sstable_(sstable),
    snapshot_version_(snapshot_version),
    clog_checkpoint_scn_(),
    multi_version_start_(multi_version_start),
    need_report_(need_report),
    storage_schema_(storage_schema),
    rebuild_seq_(rebuild_seq),
    update_with_major_flag_(false),
    need_check_sstable_(need_check_sstable),
    ddl_info_(),
    allow_duplicate_sstable_(allow_duplicate_sstable),
    need_check_transfer_seq_(need_check_transfer_seq),
    transfer_seq_(transfer_seq),
    merge_type_(merge_type),
    upper_trans_param_()
{
  clog_checkpoint_scn_ = clog_checkpoint_scn;
}

ObUpdateTableStoreParam::ObUpdateTableStoreParam(
    const blocksstable::ObSSTable *sstable,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const int64_t rebuild_seq,
    const ObStorageSchema *storage_schema,
    const bool update_with_major_flag,
    const ObMergeType merge_type,
    const bool need_report)
  : sstable_(sstable),
    snapshot_version_(snapshot_version),
    clog_checkpoint_scn_(),
    multi_version_start_(multi_version_start),
    need_report_(need_report),
    storage_schema_(storage_schema),
    rebuild_seq_(rebuild_seq),
    update_with_major_flag_(update_with_major_flag),
    need_check_sstable_(false),
    ddl_info_(),
    allow_duplicate_sstable_(false),
    need_check_transfer_seq_(false),
    transfer_seq_(-1),
    merge_type_(merge_type),
    upper_trans_param_()
{
  clog_checkpoint_scn_.set_min();
}

bool ObUpdateTableStoreParam::is_valid() const
{
  bool bret = false;
  bret = multi_version_start_ >= ObVersionRange::MIN_VERSION
      && snapshot_version_ >= ObVersionRange::MIN_VERSION
      && clog_checkpoint_scn_.is_valid()
      && nullptr != storage_schema_
      && storage_schema_->is_valid()
      && rebuild_seq_ >= 0;
  if (need_check_transfer_seq_) {
    bret = bret && transfer_seq_ >= 0;
  }
  return bret;
}


ObBatchUpdateTableStoreParam::ObBatchUpdateTableStoreParam()
  : tables_handle_(),
#ifdef ERRSIM
    errsim_point_info_(),
#endif
    rebuild_seq_(OB_INVALID_VERSION),
    is_transfer_replace_(false),
    start_scn_(SCN::min_scn()),
    tablet_meta_(nullptr),
    restore_status_(ObTabletRestoreStatus::FULL)
{
}

void ObBatchUpdateTableStoreParam::reset()
{
  tables_handle_.reset();
  rebuild_seq_ = OB_INVALID_VERSION;
  is_transfer_replace_ = false;
  start_scn_.set_min();
  tablet_meta_ = nullptr;
  restore_status_ = ObTabletRestoreStatus::FULL;
}

bool ObBatchUpdateTableStoreParam::is_valid() const
{
  return rebuild_seq_ > OB_INVALID_VERSION
      && ObTabletRestoreStatus::is_valid(restore_status_);
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
    rebuild_seq_ = param.rebuild_seq_;
    is_transfer_replace_ = param.is_transfer_replace_;
    start_scn_ = param.start_scn_;
    tablet_meta_ = param.tablet_meta_;
    restore_status_ = param.restore_status_;
#ifdef ERRSIM
    errsim_point_info_ = param.errsim_point_info_;
#endif
  }
  return ret;
}

int ObBatchUpdateTableStoreParam::get_max_clog_checkpoint_scn(SCN &clog_checkpoint_scn) const
{
  int ret = OB_SUCCESS;
  clog_checkpoint_scn.set_min();
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
        clog_checkpoint_scn = std::max(clog_checkpoint_scn, table->get_end_scn());
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
    STORAGE_LOG_RET(ERROR, tmp_ret, "lock minor merge lock failed, we need retry forever", K(tmp_ret));
  }
}

ObRebuildListener::~ObRebuildListener()
{
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS != (tmp_ret = ls_tx_ctx_mgr_.unlock_minor_merge_lock())) {
    STORAGE_LOG_RET(ERROR, tmp_ret, "unlock minor merge lock failed, we need retry forever", K(tmp_ret));
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

