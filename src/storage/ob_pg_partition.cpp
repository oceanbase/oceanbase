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

#include "storage/ob_pg_partition.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_partition_key.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_saved_storage_info_v2.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_partition_log.h"
#include "share/ob_partition_modify.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
using namespace common;
using namespace memtable;
using namespace transaction;
using namespace share::schema;
using namespace share;
namespace storage {

ObPGPartition::ObPGPartition()
    : is_inited_(false),
      pkey_(),
      cp_fty_(NULL),
      storage_(NULL),
      schema_recorder_(),
      pg_(NULL),
      merge_successed_(false),
      merge_timestamp_(0),
      merge_failed_cnt_(0),
      gc_start_ts_(0),
      build_index_schema_version_(0),
      build_index_schema_version_refreshed_ts_(INT64_MAX),
      schema_version_change_log_id_(0),
      schema_version_change_log_ts_(0)
{}

ObPGPartition::~ObPGPartition()
{
  destroy();
}

void ObPGPartition::destroy()
{
  pkey_.reset();
  if (NULL != cp_fty_) {
    if (NULL != storage_) {
      cp_fty_->free(storage_);
      storage_ = NULL;
    }
  }
  cp_fty_ = NULL;
  is_inited_ = false;
}

bool ObPGPartition::is_inited() const
{
  bool bool_ret = true;

  // No lock operation, the caller needs to handle the logic by himself
  if (!is_inited_) {
    bool_ret = false;
  } else if (OB_ISNULL(storage_) || !storage_->is_inited()) {
    bool_ret = false;
  } else {
    // do nothing
  }
  return bool_ret;
}

int ObPGPartition::check_init(void* cp, const char* cp_name) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == cp || NULL == cp_name) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "component does not exist", "component name", cp_name);
  }

  return ret;
}

int ObPGPartition::init(const common::ObPartitionKey& pkey, ObIPartitionComponentFactory* cp_fty,
    share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs, ObIPartitionGroup* pg,
    ObPGMemtableMgr& pg_memtable_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ((pkey.get_tenant_id() > 0) ? pkey.get_tenant_id() : OB_SERVER_TENANT_ID);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPGPartition init twice", K(pkey), KP(cp_fty), KP(schema_service), KP(txs));
  } else if (!pkey.is_valid() || OB_ISNULL(cp_fty) || OB_ISNULL(schema_service) || OB_ISNULL(pg)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), KP(cp_fty), KP(schema_service));
  } else if (NULL == (storage_ = cp_fty->get_partition_storage(tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "create partition storage failed.", K(ret));
  } else if (OB_FAIL(storage_->init(pkey, cp_fty, schema_service, txs, pg_memtable_mgr))) {
    STORAGE_LOG(WARN, "ObPartitionStorage init error", K(ret));
  } else {
    is_inited_ = true;
    cp_fty_ = cp_fty;
    pkey_ = pkey;
    pg_ = pg;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

int ObPGPartition::serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == (ret = check_init(storage_, "partition storage"))) {
    if (OB_FAIL(storage_->serialize(allocator, new_buf, serialize_size))) {
      STORAGE_LOG(WARN, "Fail to serialize storage, ", K(ret));
    }
  }
  return ret;
}

int ObPGPartition::deserialize(const ObReplicaType replica_type, const char* buf, const int64_t buf_len,
    ObIPartitionGroup* pg, int64_t& pos, bool& is_old_meta, ObPartitionStoreMeta& old_meta)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (NULL == buf || buf_len <= 0 || tmp_pos < 0 || tmp_pos >= buf_len || OB_ISNULL(pg)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf), K(buf_len), K(pos), KP(pg));
  } else if (OB_FAIL(check_init(storage_, "partition storage"))) {
    // do nothing
  } else if (OB_FAIL(storage_->deserialize(replica_type, buf, buf_len, pg, tmp_pos, is_old_meta, old_meta))) {
    STORAGE_LOG(WARN, "Fail to deserialize partition storage, ", K(ret));
  } else {
    pos = tmp_pos;
    pkey_ = storage_->get_partition_key();
  }

  STORAGE_LOG(INFO, "deserialize partition", K(ret), K_(pkey));
  return ret;
}

ObIPartitionStorage* ObPGPartition::get_storage()
{
  return storage_;
}

const ObPartitionSplitInfo& ObPGPartition::get_split_info()
{
  return pg_->get_split_info();
}

void ObPGPartition::set_merge_status(bool merge_success)
{
  ATOMIC_STORE(&merge_successed_, merge_success);
  ATOMIC_STORE(&merge_timestamp_, ObTimeUtility::current_time());
  if (merge_success) {
    ATOMIC_STORE(&merge_failed_cnt_, 0);
  } else {
    ATOMIC_AAF(&merge_failed_cnt_, 1);
  }
}

bool ObPGPartition::can_schedule_merge()
{
  bool bool_ret = true;
  int64_t delay_duration = merge_failed_cnt_ * DELAY_SCHEDULE_TIME_UNIT;
  if (delay_duration > MAX_DELAY_SCHEDULE_TIME_UNIT) {
    delay_duration = MAX_DELAY_SCHEDULE_TIME_UNIT;
  }
  if (!merge_successed_ && (ObTimeUtility::current_time() < (merge_timestamp_ + delay_duration))) {
    bool_ret = false;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /*10s*/)) {
      STORAGE_LOG(
          INFO, "cannot schedule merge now", K(pkey_), K(merge_timestamp_), K(merge_successed_), K(merge_failed_cnt_));
    }
  }

  return bool_ret;
}

int ObPGPartition::get_refreshed_schema_info(int64_t& schema_version, int64_t& refreshed_schema_ts,
    uint64_t& schema_version_change_log_id, int64_t& schema_version_change_log_ts)
{
  // Ensure to get the schema version first, and then get ts
  ATOMIC_STORE(&schema_version, build_index_schema_version_);
  refreshed_schema_ts = build_index_schema_version_refreshed_ts_;
  schema_version_change_log_id = schema_version_change_log_id_;
  schema_version_change_log_ts = schema_version_change_log_ts_;
  return OB_SUCCESS;
}

// There will be no concurrency here, so there is no need to lock
int ObPGPartition::update_build_index_schema_info(
    const int64_t schema_version, const int64_t schema_refreshed_ts, const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  if (schema_version < 0 || schema_refreshed_ts < 0 || log_id <= 0 || log_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(schema_version), K(schema_refreshed_ts), K(log_id), K(log_ts));
  } else if (build_index_schema_version_ < schema_version) {
    // First record
    if (INT64_MAX == build_index_schema_version_refreshed_ts_) {
      // Ensure that the schema version is updated at the end
      build_index_schema_version_refreshed_ts_ = schema_refreshed_ts;
      schema_version_change_log_id_ = log_id;
      schema_version_change_log_ts_ = log_ts;
      ATOMIC_STORE(&build_index_schema_version_, schema_version);
    } else if (build_index_schema_version_refreshed_ts_ > schema_refreshed_ts) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "unexpected schema version refreshed ts",
          K(ret),
          K_(pkey),
          K_(build_index_schema_version_refreshed_ts),
          K(schema_version),
          K(schema_refreshed_ts));
    } else {
      // Ensure that the schema version is updated at the end
      build_index_schema_version_refreshed_ts_ = schema_refreshed_ts;
      schema_version_change_log_id_ = log_id;
      schema_version_change_log_ts_ = log_ts;
      ATOMIC_STORE(&build_index_schema_version_, schema_version);
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPGPartition::replay_schema_log(const char* buf, const int64_t size, const int64_t log_id)
{
  return schema_recorder_.replay_schema_log(buf, size, log_id);
}

bool ObPGPartition::is_gc_starting_() const
{
  // no need to use atmoic_load
  return gc_start_ts_ > 0;
}

int ObPGPartition::get_gc_start_ts(int64_t& gc_start_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else {
    // no need to use atmoic_load
    gc_start_ts = gc_start_ts_;
  }
  return ret;
}

// The call of this method, only gc thread operation,
//  there will be no multi-threaded concurrency
int ObPGPartition::set_gc_starting()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (gc_start_ts_ <= 0) {
    /* Problem Description:
     * Here it is only necessary to preset gc_start_ts to a value greater than 0,
     * in order to solve the concurrency in the following scenarios:
     *  Thread 1: Get the local system time (a), then update gc_start_ts(b);
     *  Thread 2: Generate ctx_create_timestamp(c), access gc_start_ts is greater than 0(d)
     *  Possible problems: If the execution order becomes abcd,
     *    the gc thread may miss the transaction operated by thread 2
     *
     * Solution:
     *   (1) Set the bool variable, dml interface all checks whether the variable is greater than 0;
     *   (2) Combine gc_start_ts_ with the bool variable in (1)
     *
     * So ensure that gc_start_ts is greater than all transaction contexts
     * operated by this pg_partition ctx_create_timestamp
     */
    ATOMIC_STORE(&gc_start_ts_, 1);
    // To be safe, reduce the impact of clock rollback and other effects,
    // increase the value of gc_start_ts by 10s
    ATOMIC_STORE(&gc_start_ts_, ObTimeUtility::current_time() + 10 * 1000 * 1000);
  } else {
    // do nothing
  }
  return ret;
}

int ObPGPartition::delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->delete_rows(ctx, dml_param, column_ids, row_iter, affected_rows);
  }

  return ret;
}

int ObPGPartition::delete_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->delete_row(ctx, dml_param, column_ids, row);
  }

  return ret;
}

int ObPGPartition::put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->put_rows(ctx, dml_param, column_ids, row_iter, affected_rows);
  }
  return ret;
}

int ObPGPartition::insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->insert_rows(ctx, dml_param, column_ids, row_iter, affected_rows);
  }

  return ret;
}

int ObPGPartition::insert_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->insert_row(ctx, dml_param, column_ids, row);
  }

  return ret;
}

int ObPGPartition::insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& duplicated_column_ids,
    const common::ObNewRow& row, const ObInsertFlag flag, int64_t& affected_rows,
    common::ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->insert_row(
        ctx, dml_param, column_ids, duplicated_column_ids, row, flag, affected_rows, duplicated_rows);
  }

  return ret;
}

int ObPGPartition::fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& in_column_ids, const ObIArray<uint64_t>& out_column_ids, ObNewRowIterator& check_row_iter,
    ObIArray<ObNewRowIterator*>& dup_row_iters)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->fetch_conflict_rows(ctx, dml_param, in_column_ids, out_column_ids, check_row_iter, dup_row_iters);
  }

  return ret;
}

int ObPGPartition::update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, ObNewRowIterator* row_iter,
    int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->update_rows(ctx, dml_param, column_ids, updated_column_ids, row_iter, affected_rows);
  }

  return ret;
}

int ObPGPartition::update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, const ObNewRow& old_row,
    const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->update_row(ctx, dml_param, column_ids, updated_column_ids, old_row, new_row);
  }
  return ret;
}

int ObPGPartition::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
    ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->lock_rows(ctx, dml_param, abs_lock_timeout, row_iter, lock_flag, affected_rows);
  }
  return ret;
}

int ObPGPartition::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
    const ObNewRow& row, const ObLockFlag lock_flag)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(ctx), K(dml_param));
  } else {
    ret = storage_->lock_rows(ctx, dml_param, abs_lock_timeout, row, lock_flag);
  }
  return ret;
}

int ObPGPartition::table_scan(ObTableScanParam& param, const int64_t data_max_schema_version, ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(param));
  } else {
    ret = storage_->table_scan(param, data_max_schema_version, result);
  }
  return ret;
}

int ObPGPartition::table_scan(
    ObTableScanParam& param, const int64_t data_max_schema_version, ObNewIterIterator*& result)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(param));
  } else {
    ret = storage_->table_scan(param, data_max_schema_version, result);
  }
  return ret;
}

int ObPGPartition::join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
    const int64_t left_data_max_schema_version, const int64_t right_data_max_schema_version, ObPGPartition& right_part,
    common::ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (OB_ISNULL(right_part.get_storage())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(right_part.get_storage()));
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(left_param), K(right_param));
  } else {
    ret = storage_->join_mv_scan(left_param,
        right_param,
        left_data_max_schema_version,
        right_data_max_schema_version,
        *(right_part.get_storage()),
        result);
  }
  return ret;
}

int ObPGPartition::feedback_scan_access_stat(const ObTableScanParam& param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_init(storage_, "partition storage"))) {
  } else if (is_gc_starting_()) {
    ret = OB_PARTITION_NOT_EXIST;
    STORAGE_LOG(WARN, "partition is garbage cleaning", K(ret), K(param));
  } else {
    ret = storage_->feedback_scan_access_stat(param);
  }
  return ret;
}

ObPGPartitionArrayGuard::~ObPGPartitionArrayGuard()
{
  ObPGPartition* partition = nullptr;
  ObIPartitionGroup* pg = nullptr;
  for (int64_t i = 0; i < partitions_.count(); ++i) {
    if (OB_LIKELY(nullptr != (partition = partitions_.at(i)))) {
      if (OB_LIKELY(nullptr != (pg = partition->get_pg()))) {
        pg_partition_map_.revert(partition);
      }
    }
  }
}

int ObPGPartitionArrayGuard::push_back(ObPGPartition* pg_partition)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partitions_.push_back(pg_partition))) {
    STORAGE_LOG(WARN, "failed to push back pg partition", K(ret));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
