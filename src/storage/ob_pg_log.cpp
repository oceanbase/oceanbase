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

#include "ob_pg_log.h"
#include "ob_storage_log_type.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace clog;

namespace storage {
OB_SERIALIZE_MEMBER(ObOfflinePartitionLog, log_type_, is_physical_drop_, cluster_id_);
OB_SERIALIZE_MEMBER(ObAddPartitionToPGLog, log_type_, arg_);
OB_SERIALIZE_MEMBER(ObRemovePartitionFromPGLog, log_type_, pg_key_, partition_key_);
OB_SERIALIZE_MEMBER(ObPGSchemaChangeLog, log_type_, pg_key_, pkey_, schema_version_, index_id_);

int ObOfflinePartitionLog::init(const int64_t log_type, const bool is_physical_drop)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObOfflinePartitionLog init twice", K(log_type), K(is_physical_drop));
  } else if (!ObStorageLogTypeChecker::is_offline_partition_log_new(log_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(log_type), KR(ret));
  } else {
    log_type_ = log_type;
    is_physical_drop_ = is_physical_drop;
    cluster_id_ = obrpc::ObRpcNetHandler::CLUSTER_ID;
    is_inited_ = true;
  }

  return ret;
}

void ObOfflinePartitionLog::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  is_physical_drop_ = false;
  cluster_id_ = common::OB_INVALID_CLUSTER_ID;
  is_inited_ = false;
}

bool ObOfflinePartitionLog::is_valid() const
{
  return ObStorageLogTypeChecker::is_offline_partition_log_new(log_type_);
}

int ObAddPartitionToPGLog::init(const int64_t log_type, const obrpc::ObCreatePartitionArg& arg)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAddPartitionToPGLog init twice", K(log_type), K(arg));
  } else if (!ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type) || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(log_type), K(arg));
  } else if (OB_FAIL(arg_.deep_copy(arg))) {
    STORAGE_LOG(WARN, " deep copy error", K(ret), K(log_type), K(arg));
  } else {
    log_type_ = log_type;
    is_inited_ = true;
  }

  return ret;
}

void ObAddPartitionToPGLog::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  arg_.reset();
  is_inited_ = false;
}

bool ObAddPartitionToPGLog::is_valid() const
{
  return ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type_) && arg_.is_valid();
}

int ObPGSchemaChangeLog::init(const int64_t log_type, const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey,
    const int64_t schema_version, const uint64_t index_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGSchemaChangeLog init twice", K(ret), K(log_type), K(pg_key), K(pkey), K(schema_version));
  } else if (!ObStorageLogTypeChecker::is_schema_version_change_log(log_type) || !pg_key.is_valid() ||
             !pkey.is_valid() || schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_type), K(pg_key), K(pkey), K(schema_version), K(index_id));
  } else {
    log_type_ = log_type;
    pg_key_ = pg_key;
    pkey_ = pkey;
    schema_version_ = schema_version;
    index_id_ = index_id;
    is_inited_ = true;
  }

  return ret;
}

void ObPGSchemaChangeLog::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  pg_key_.reset();
  pkey_.reset();
  schema_version_ = -1;
  index_id_ = 0;
  is_inited_ = false;
}

bool ObPGSchemaChangeLog::is_valid() const
{
  return ObStorageLogTypeChecker::is_schema_version_change_log(log_type_) && pg_key_.is_valid() && pkey_.is_valid() &&
         schema_version_ >= 0;
}

int ObPGSchemaChangeLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id));
  } else if (new_tenant_id == pg_key_.get_tenant_id()) {
    // no need replace
  } else {
    ObPartitionKey new_pg_key;
    ObPartitionKey new_pkey;
    if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(pg_key_, new_tenant_id, new_pg_key))) {
      STORAGE_LOG(WARN, "replace_pkey_tenant_id failed", K(ret), K(new_tenant_id));
    } else if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(pkey_, new_tenant_id, new_pkey))) {
      STORAGE_LOG(WARN, "replace_pkey_tenant_id failed", K(ret), K(new_tenant_id));
    } else if (0 == index_id_) {
      // "0 == index_id" indicates unique index and no need to replace tenant_id when log is replaying
    } else {
      const uint64_t pure_id = extract_pure_id(index_id_);
      index_id_ = combine_id(new_tenant_id, pure_id);
      pg_key_ = new_pg_key;
      pkey_ = new_pkey;
    }
  }
  return ret;
}

int ObRemovePartitionFromPGLog::init(const int64_t log_type, const ObPGKey& pg_key, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObRemovePartitionFromPGLog init twice", K(ret), K(log_type), K(pg_key), K(pkey));
  } else if (!ObStorageLogTypeChecker::is_remove_partition_from_pg_log(log_type) || !pg_key.is_valid() ||
             !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(log_type), K(pg_key), K(pkey));
  } else {
    log_type_ = log_type;
    pg_key_ = pg_key;
    partition_key_ = pkey;
    is_inited_ = true;
  }

  return ret;
}

void ObRemovePartitionFromPGLog::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  pg_key_.reset();
  partition_key_.reset();
  is_inited_ = false;
}

bool ObRemovePartitionFromPGLog::is_valid() const
{
  return ObStorageLogTypeChecker::is_remove_partition_from_pg_log(log_type_) && pg_key_.is_valid() &&
         partition_key_.is_valid();
}

int ObRemovePartitionFromPGLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id));
  } else if (new_tenant_id == pg_key_.get_tenant_id()) {
    // no need replace
  } else {
    ObPartitionKey new_pg_key;
    ObPartitionKey new_pkey;
    if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(pg_key_, new_tenant_id, new_pg_key))) {
      STORAGE_LOG(WARN, "replace_pkey_tenant_id failed", K(ret), K(new_tenant_id));
    } else if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(partition_key_, new_tenant_id, new_pkey))) {
      STORAGE_LOG(WARN, "replace_pkey_tenant_id failed", K(ret), K(new_tenant_id));
    } else {
      pg_key_ = new_pg_key;
      partition_key_ = new_pkey;
    }
  }
  return ret;
}

int ObAddPartitionToPGLogCb::init(
    const int64_t log_type, const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObAddPartitionToPGLog init twice", K(log_type), K(pg_key), K(pkey));
  } else if (!ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type) || !pg_key.is_valid() || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(log_type), K(pg_key), K(pkey));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, guard_))) {
    STORAGE_LOG(WARN, "get partition group error", K(ret), K(pg_key), K(pkey));
  } else {
    log_type_ = log_type;
    pg_key_ = pg_key;
    partition_key_ = pkey;
    is_inited_ = true;
  }

  return ret;
}

void ObAddPartitionToPGLogCb::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  pg_key_.reset();
  partition_key_.reset();
  // revert partition group
  guard_.reset();
  write_clog_state_ = CB_INIT;
  is_locking_ = false;
  is_inited_ = false;
}

bool ObAddPartitionToPGLogCb::is_valid() const
{
  return is_inited_ && ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type_) && pg_key_.is_valid() &&
         partition_key_.is_valid();
}

int ObAddPartitionToPGLogCb::on_success(const common::ObPGKey& pg_key, const clog::ObLogType log_type,
    const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  UNUSED(pg_key);
  UNUSED(log_type);
  UNUSED(log_id);
  UNUSED(version);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(pg_key), K(log_id), K(version), K(batch_committed), K(batch_last_succeed));
  } else if (!pg_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pg_key), K(log_id), K(version));
  } else {
    STORAGE_LOG(INFO, "ObAddPartitionToPGLogCb sync callback success", K(pg_key), K(log_id));
    // lock
    bool need_release = false;
    while (true) {
      if (false == ATOMIC_TAS(&is_locking_, true)) {
        if (CB_END != write_clog_state_) {
          write_clog_state_ = CB_SUCCESS;
        } else {
          need_release = true;
        }
        ATOMIC_STORE(&is_locking_, false);
        break;
      }
    }
    if (need_release) {
      op_free(this);
    }
  }

  return ret;
}

int ObAddPartitionToPGLogCb::on_finished(const common::ObPGKey& pg_key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(pg_key), K(log_id));
  } else if (!pg_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pg_key), K(log_id));
  } else {
    STORAGE_LOG(INFO, "ObAddPartitionToPGLogCb sync callback finished", K(pg_key), K(log_id));
    // lock
    bool need_release = false;
    while (true) {
      if (false == ATOMIC_TAS(&is_locking_, true)) {
        if (CB_END != write_clog_state_) {
          write_clog_state_ = CB_FAIL;
        } else {
          need_release = true;
        }
        ATOMIC_STORE(&is_locking_, false);
        break;
      }
    }
    if (need_release) {
      op_free(this);
    }
  }

  return ret;
}

int ObAddPartitionToPGLogCb::check_can_release(bool& can_release)
{
  int ret = OB_SUCCESS;
  // lock
  while (true) {
    if (false == ATOMIC_TAS(&is_locking_, true)) {
      // clog callback not finish
      if (CB_FAIL != write_clog_state_ && CB_SUCCESS != write_clog_state_) {
        write_clog_state_ = CB_END;
        can_release = false;
      } else {
        can_release = true;
      }
      ATOMIC_STORE(&is_locking_, false);
      break;
    }
  }

  return ret;
}

int ObRemovePartitionFromPGLogCb::init(const int64_t log_type, const common::ObPGKey& pg_key,
    const common::ObPartitionKey& pkey, ObCLogCallbackAsyncWorker* cb_async_worker)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObAddPartitionToPGLog init twice", K(log_type), K(pg_key), K(pkey));
  } else if (!ObStorageLogTypeChecker::is_remove_partition_from_pg_log(log_type) || !pg_key.is_valid() ||
             !pkey.is_valid() || OB_ISNULL(cb_async_worker)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(log_type), K(pg_key), K(pkey), KP(cb_async_worker));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, guard_))) {
    STORAGE_LOG(WARN, "get partition group error", K(ret), K(pg_key), K(pkey));
  } else {
    log_type_ = log_type;
    pg_key_ = pg_key;
    partition_key_ = pkey;
    cb_async_worker_ = cb_async_worker;
    is_inited_ = true;
  }

  return ret;
}

void ObRemovePartitionFromPGLogCb::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  pg_key_.reset();
  partition_key_.reset();
  // revert partition group
  guard_.reset();
  cb_async_worker_ = nullptr;
  is_inited_ = false;
}

bool ObRemovePartitionFromPGLogCb::is_valid() const
{
  return is_inited_ && ObStorageLogTypeChecker::is_remove_partition_from_pg_log(log_type_) && pg_key_.is_valid() &&
         partition_key_.is_valid();
}

int ObRemovePartitionFromPGLogCb::on_success(const common::ObPGKey& pg_key, const clog::ObLogType log_type,
    const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  UNUSED(log_type);
  UNUSED(version);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(pg_key), K(log_id));
  } else if (!pg_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pg_key), K(log_id));
  } else {
    ObCLogCallbackAsyncTask task;
    task.pg_key_ = pg_key;
    task.partition_key_ = partition_key_;
    task.log_id_ = log_id;
    task.log_type_ = log_type_;
    if (OB_FAIL(cb_async_worker_->push_task(task))) {
      STORAGE_LOG(WARN, "fail to push task to worker", K(ret), K(task));
    } else {
      STORAGE_LOG(INFO, "ObRemovePartitionFromPGLogCb sync callback success", K(partition_key_), K(pg_key), K(log_id));
      op_free(this);
    }
  }

  return ret;
}

int ObRemovePartitionFromPGLogCb::on_finished(const common::ObPGKey& pg_key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(pg_key), K(log_id));
  } else if (!pg_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pg_key), K(log_id));
  } else {
    STORAGE_LOG(INFO, "ObRemovePartitionFromPGLogCb sync callback finished", K(pg_key), K(log_id));
    op_free(this);
  }

  return ret;
}

int ObSchemaChangeClogCb::init(
    const int64_t log_type, const common::ObPGKey& pg_key, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSchemaChangeClogCb init twice", K(ret), K(log_type), K(pg_key), K(pkey));
  } else if (!ObStorageLogTypeChecker::is_schema_version_change_log(log_type) || !pg_key.is_valid() ||
             !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log_type), K(pg_key), K(pkey));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, guard_))) {
    STORAGE_LOG(WARN, "get partition group error", K(ret), K(pg_key), K(pkey));
  } else {
    log_type_ = log_type;
    pg_key_ = pg_key;
    partition_key_ = pkey;
    is_inited_ = true;
  }

  return ret;
}

void ObSchemaChangeClogCb::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  pg_key_.reset();
  partition_key_.reset();
  // revert partition group
  guard_.reset();
  write_clog_state_ = CB_INIT;
  is_locking_ = false;
  is_inited_ = false;
}

bool ObSchemaChangeClogCb::is_valid() const
{
  return is_inited_ && ObStorageLogTypeChecker::is_schema_version_change_log(log_type_) && pg_key_.is_valid() &&
         partition_key_.is_valid();
}

int ObSchemaChangeClogCb::on_success(const common::ObPGKey& pg_key, const clog::ObLogType log_type,
    const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  UNUSED(log_type);
  UNUSED(pg_key);
  UNUSED(log_id);
  UNUSED(version);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), K(pg_key), K(log_id), K(version), K(batch_committed), K(batch_last_succeed));
  } else if (!pg_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(log_id), K(version));
  } else {
    STORAGE_LOG(INFO, "ObSchemaChangeClogCb sync callback success", K(pg_key), K(log_id));
    // lock
    bool need_release = false;
    while (true) {
      if (false == ATOMIC_TAS(&is_locking_, true)) {
        if (CB_END != write_clog_state_) {
          write_clog_state_ = CB_SUCCESS;
        } else {
          // index_builder thread already exit, so it can release memory
          need_release = true;
        }
        ATOMIC_STORE(&is_locking_, false);
        break;
      }
    }
    if (need_release) {
      op_free(this);
    }
  }

  return ret;
}

int ObSchemaChangeClogCb::on_finished(const common::ObPGKey& pg_key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(pg_key), K(log_id));
  } else if (!pg_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pg_key), K(log_id));
  } else {
    STORAGE_LOG(INFO, "ObSchemaChangeClogCb sync callback finished", K(pg_key), K(log_id));
    // lock
    bool need_release = false;
    while (true) {
      if (false == ATOMIC_TAS(&is_locking_, true)) {
        if (CB_END != write_clog_state_) {
          write_clog_state_ = CB_FAIL;
        } else {
          // index_builder thread already exit, so it can release memory
          need_release = true;
        }
        ATOMIC_STORE(&is_locking_, false);
        break;
      }
    }
    if (need_release) {
      op_free(this);
    }
  }

  return ret;
}

int ObSchemaChangeClogCb::check_can_release(bool& can_release)
{
  int ret = OB_SUCCESS;
  // lock
  while (true) {
    if (false == ATOMIC_TAS(&is_locking_, true)) {
      // clog callback not finish
      if (CB_FAIL != write_clog_state_ && CB_SUCCESS != write_clog_state_) {
        write_clog_state_ = CB_END;
        can_release = false;
      } else {
        can_release = true;
      }
      ATOMIC_STORE(&is_locking_, false);
      break;
    }
  }

  return ret;
}

int ObOfflinePartitionCb::init(ObCLogCallbackAsyncWorker* cb_async_worker, const bool is_physical_drop)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(cb_async_worker)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(cb_async_worker));
  } else {
    cb_async_worker_ = cb_async_worker;
    is_physical_drop_ = is_physical_drop;
    is_inited_ = true;
  }
  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObOfflinePartitionCb::reset()
{
  cb_async_worker_ = nullptr;
  is_inited_ = false;
  is_physical_drop_ = false;
}

void ObOfflinePartitionCb::destroy()
{
  reset();
}

int ObOfflinePartitionCb::on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type,
    const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  UNUSED(log_type);
  UNUSED(version);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not init", K(partition_key), K(log_id), KR(ret));
  } else if (OB_UNLIKELY((!partition_key.is_valid()) || log_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", K(ret), K(partition_key), K(log_id));
  } else {
    ObCLogCallbackAsyncTask task;
    task.pg_key_ = partition_key;
    task.partition_key_ = partition_key;
    task.log_type_ = OB_LOG_OFFLINE_PARTITION_V2;
    task.log_id_ = log_id;
    task.is_physical_drop_ = is_physical_drop_;
    if (OB_FAIL(cb_async_worker_->push_task(task))) {
      STORAGE_LOG(WARN, "fail to push task to worker", K(ret), K(task), K(partition_key), K(log_id));
    } else {
      STORAGE_LOG(
          INFO, "ObOfflinePartitionCb sync callback success", K(partition_key), K(log_id), K(is_physical_drop_));
      op_free(this);
    }
  }
  return ret;
}

int ObOfflinePartitionCb::on_finished(const common::ObPGKey& pg_key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not init", K(pg_key), K(log_id));
  } else if (!pg_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", K(pg_key), K(log_id), KR(ret));
  } else {
    STORAGE_LOG(INFO, "ObOfflinePartitionCb::sync callback finished", K(pg_key), K(log_id));
    op_free(this);
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
