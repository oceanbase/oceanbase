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
#include "ob_storage_schema_recorder.h"

#include "lib/utility/ob_tracepoint.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_handler.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx_storage/ob_ls_handle.h"//ObLSHandle
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{

using namespace common;
using namespace clog;
using namespace share::schema;

namespace storage
{

void ObStorageSchemaRecorder::ObStorageSchemaLogCb::set_table_version(const int64_t table_version)
{
  ATOMIC_SET(&table_version_, table_version);
}

int ObStorageSchemaRecorder::ObStorageSchemaLogCb::on_success()
{
  int ret = OB_SUCCESS;
  int64_t table_version = table_version_;
  bool finish_flag = false;

  if (OB_UNLIKELY(OB_INVALID_VERSION == table_version)) {
    LOG_ERROR("table version is invalid", K(table_version));
  } else {
    // clear table_version whether success or failed, make sure next time can update
    ATOMIC_SET(&table_version_, OB_INVALID_VERSION);
    recorder_.update_table_schema_succ(table_version, finish_flag);
    if (!finish_flag) {
      LOG_WARN("update table schema failed", K(table_version), K(finish_flag));
      recorder_.update_table_schema_fail();
    }
  }

  return ret;
}

int ObStorageSchemaRecorder::ObStorageSchemaLogCb::on_failure()
{
  int ret = OB_SUCCESS;
  LOG_INFO("schema log failure callback", K(table_version_));
  ATOMIC_SET(&table_version_, OB_INVALID_VERSION);
  recorder_.update_table_schema_fail();
  return ret;
}


void ObStorageSchemaRecorder::ObStorageSchemaLogCb::clear()
{
  ATOMIC_SET(&table_version_, OB_INVALID_VERSION);
}

ObStorageSchemaRecorder::ObStorageSchemaRecorder()
  : is_inited_(false),
    lock_(false),
    logcb_finish_flag_(true),
    logcb_ptr_(nullptr),
    max_saved_table_version_(OB_INVALID_VERSION),
    clog_buf_(nullptr),
    clog_len_(0),
    clog_ts_(-1),
    schema_guard_(nullptr),
    storage_schema_(nullptr),
    allocator_(nullptr),
    log_handler_(nullptr),
    ls_id_(),
    tablet_id_(),
    tablet_handle_()
{
  STATIC_ASSERT(sizeof(ObStorageSchemaRecorder) <= 136, "size of schema recorder is oversize");
}

ObStorageSchemaRecorder::~ObStorageSchemaRecorder()
{
  reset();
}

void ObStorageSchemaRecorder::reset()
{
  if (is_inited_) {
    wait_to_lock(OB_INVALID_VERSION); // lock
    max_saved_table_version_ = 0;
    ATOMIC_STORE(&lock_, false); // unlock
  }
}

void ObStorageSchemaRecorder::destroy()
{
  is_inited_ = false;
  max_saved_table_version_ = OB_INVALID_VERSION;
  lock_ = false;
  logcb_finish_flag_ = true;
  free_allocated_info();
  log_handler_ = NULL;
  ls_id_.reset();
  tablet_id_.reset();
  tablet_handle_.reset();
  clog_ts_ = -1;
  clog_len_ = 0;
}

int ObStorageSchemaRecorder::init(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t saved_schema_version,
    logservice::ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(saved_schema_version < 0 || nullptr == log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(saved_schema_version), KP(log_handler));
  } else {
    max_saved_table_version_ = saved_schema_version;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    log_handler_ = log_handler;
    WEAK_BARRIER();
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

// schema log is barrier, there is no concurrency problem, no need to lock
int ObStorageSchemaRecorder::replay_schema_log(
    const int64_t log_ts,
    const char *buf,
    const int64_t size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema recorder not inited", K(ret));
  } else {
    int64_t table_version = OB_INVALID_VERSION;
    ObArenaAllocator tmp_allocator;
    ObStorageSchema replay_storage_schema;
    if (tablet_id_.is_special_merge_tablet()) {
      // do nothing
    } else if (OB_FAIL(serialization::decode_i64(buf, size, pos, &table_version))) {
      // table_version
      LOG_WARN("fail to deserialize table_version", K(ret), K_(tablet_id));
    } else if (table_version <= ATOMIC_LOAD(&max_saved_table_version_)) {
      LOG_INFO("skip schema log with smaller table version", K_(tablet_id), K(table_version),
          K(max_saved_table_version_));
    } else if (OB_FAIL(replay_get_tablet_handle(log_ts, tablet_handle_))) {
      LOG_WARN("failed to get tablet handle", K(ret), K_(tablet_id), K(log_ts));
    } else if (OB_FAIL(replay_storage_schema.deserialize(tmp_allocator, buf, size, pos))) {
      LOG_WARN("fail to deserialize storage schema", K(ret), K_(tablet_id));
    } else if (FALSE_IT(replay_storage_schema.set_sync_finish(true))) {
    } else if (OB_FAIL(tablet_handle_.get_obj()->save_multi_source_data_unit(&replay_storage_schema, log_ts,
        true/*for_replay*/, memtable::MemtableRefOp::NONE))) {
      LOG_WARN("failed to save storage schema on memtable", K(ret), K_(tablet_id), K(replay_storage_schema));
    } else {
      ATOMIC_SET(&max_saved_table_version_, table_version);
      LOG_INFO("success to replay schema log", K(ret), K_(tablet_id), K(max_saved_table_version_));
      replay_storage_schema.reset();
    }
    tablet_handle_.reset();
  }

  return ret;
}

OB_INLINE void ObStorageSchemaRecorder::wait_to_lock(const int64_t table_version)
{
  while (true) {
    while (true == ATOMIC_LOAD(&lock_)) {
      ob_usleep(100);
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        LOG_DEBUG("waiting to update schema", K_(tablet_id), K(table_version), K(max_saved_table_version_));
      }
      WEAK_BARRIER();
    }

    if (ATOMIC_BCAS(&lock_, false, true)) {
      break;
    }
  } // end of while

}
OB_INLINE void ObStorageSchemaRecorder::wait_for_logcb(const int64_t table_version)
{
  while (false == ATOMIC_LOAD(&logcb_finish_flag_)) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_DEBUG("waiting for clog callback", K_(tablet_id), K(table_version), K(max_saved_table_version_));
    }
    ob_usleep(100);
    WEAK_BARRIER();
  }
}

int ObStorageSchemaRecorder::try_update_with_lock(
    const int64_t table_id,
    const int64_t table_version,
    const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  while ((OB_SUCC(ret) || OB_BLOCK_FROZEN == ret)
      && table_version > ATOMIC_LOAD(&max_saved_table_version_)) {
    if (OB_FAIL(submit_schema_log(table_id))) {
      if (OB_BLOCK_FROZEN != ret) {
        LOG_WARN("fail to save table schema", K(ret), K_(tablet_id), K(table_version), K(max_saved_table_version_));
      } else if (ObTimeUtility::fast_current_time() >= expire_ts) {
        ret = OB_EAGAIN;
        LOG_WARN("failed to sync table schema", K_(tablet_id), K(table_version),
            K(max_saved_table_version_), K(expire_ts));
      }
    } else {
      wait_for_logcb(table_version);  // wait clog callback
    }
    WEAK_BARRIER();
  } // end of while

  return ret;
}

int ObStorageSchemaRecorder::try_update_storage_schema(
    const int64_t table_id,
    const int64_t table_version,
    ObIAllocator &allocator,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema recorder not inited", K(ret));
  } else if (table_version < 0) {
    LOG_WARN("input schema version is invalid", K(ret), K(table_id), K(table_version));
  } else if (tablet_id_.is_special_merge_tablet()) {
    // do nothing
  } else if (table_version > ATOMIC_LOAD(&max_saved_table_version_)) {

    wait_to_lock(table_version); // lock
    allocator_ = &allocator;
    if (table_version > ATOMIC_LOAD(&max_saved_table_version_)) {
      LOG_INFO("save table schema", K_(ls_id), K_(tablet_id), K(table_version), K(max_saved_table_version_));
      int64_t sync_table_version = table_version;
      if (OB_FAIL(get_tablet_handle(tablet_handle_))) {
        LOG_WARN("failed to get tablet handle", K(ret), K_(ls_id), K_(tablet_id));
      } else if (OB_FAIL(prepare_schema(table_id, sync_table_version))) {
        LOG_WARN("fail to save table schema", K(ret), K_(ls_id), K_(tablet_id), K(sync_table_version));
      } else if (OB_FAIL(try_update_with_lock(table_id, sync_table_version, timeout))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("try update failed", K(ret), K_(ls_id), K_(tablet_id), K(table_version));
        }
      } else { // sync schema clog success
        FLOG_INFO("finish save table schema", K_(ls_id), K_(tablet_id), K(sync_table_version),
            "schema_version", storage_schema_->get_schema_version(), K_(clog_ts), K(timeout));
      }
    }

    // clear state no matter success or failed

    ATOMIC_STORE(&logcb_finish_flag_, true);
    free_allocated_info();
    tablet_handle_.reset();
    WEAK_BARRIER();
    ATOMIC_STORE(&lock_, false); // unlock
  }
  if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObStorageSchemaRecorder::get_tablet_handle(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K_(ls_id), K_(tablet_id));
  }
  return ret;
}

int ObStorageSchemaRecorder::replay_get_tablet_handle(const int64_t log_ts, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->replay_get_tablet(tablet_id_, log_ts, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K_(ls_id), K_(tablet_id), K(log_ts));
  }
  return ret;
}

void ObStorageSchemaRecorder::update_table_schema_fail()
{
  dec_ref_on_memtable(false);
  ATOMIC_STORE(&logcb_finish_flag_, true);
}

void ObStorageSchemaRecorder::update_table_schema_succ(
    const int64_t table_version,
    bool &finish_flag)
{
  int ret = OB_SUCCESS;
  finish_flag = false;
  if (table_version <= ATOMIC_LOAD(&max_saved_table_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("schema log with smaller table version", K(ret), K_(tablet_id),
        K(table_version), K(max_saved_table_version_));
  } else if (OB_UNLIKELY(clog_ts_ <= 0 || nullptr == storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    // clog_ts_ may be invalid because of concurrency in rare situation
    LOG_WARN("clog ts or storage schema is invalid", K(ret), K_(ls_id), K_(tablet_id), 
        K_(clog_ts), KP_(storage_schema));
  } else if (storage_schema_->get_schema_version() != table_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema version not match", K(storage_schema_), K(table_version));
  }
  if (OB_SUCC(ret)) {
    finish_flag = true;
    if (OB_FAIL(dec_ref_on_memtable(true))) {
      LOG_WARN("failed to save storage schema", K_(tablet_id), K(storage_schema_));
    } else {
      FLOG_INFO("update table schema success", K(ret), K_(ls_id), K_(tablet_id), K(table_version), 
          "schema_version", table_version);
      ATOMIC_SET(&max_saved_table_version_, table_version);
    }
  }
  ATOMIC_STORE(&logcb_finish_flag_, true);
}

int ObStorageSchemaRecorder::dec_ref_on_memtable(const bool sync_finish)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == storage_schema_ || !tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema or tablet handle is unexpected null", K(ret), K_(ls_id), K_(tablet_id),
        KP_(storage_schema), K_(tablet_handle));
  } else {
    storage_schema_->set_sync_finish(sync_finish);
    if (OB_FAIL(tablet_handle_.get_obj()->save_multi_source_data_unit(storage_schema_, clog_ts_,
        false/*for_replay*/, memtable::MemtableRefOp::DEC_REF, true/*is_callback*/))) {
      LOG_WARN("failed to save storage schema", K(ret), K_(tablet_id), K(storage_schema_));
    }
  }
  return ret;
}

int ObStorageSchemaRecorder::prepare_schema(
    const int64_t table_id,
    int64_t &table_version)
{
  int ret = OB_SUCCESS;
  const int64_t alloc_size = sizeof(ObSchemaGetterGuard) + sizeof(ObStorageSchema);
  void *buf = nullptr;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is unexpected null", K(ret), KP(allocator_));
  } else if (OB_ISNULL(buf = allocator_->alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate schema guard", K(ret), K_(tablet_id));
  } else if (FALSE_IT(schema_guard_ = new (buf) ObSchemaGetterGuard(share::schema::ObSchemaMgrItem::MOD_SCHEMA_RECORDER))) {
  } else if (FALSE_IT(storage_schema_ = new (static_cast<char *>(buf) + sizeof(ObSchemaGetterGuard)) ObStorageSchema())) {
  } else if (OB_FAIL(get_expected_schema_guard(table_id, table_version))) {
    LOG_WARN("fail to get expected schema", K(ret), K_(tablet_id), K(table_version));
  } else if (OB_FAIL(generate_clog())) {
    LOG_WARN("failed to generate clog", K(ret), K_(tablet_id));
  }
  return ret;
}

void ObStorageSchemaRecorder::free_allocated_info()
{
  if (nullptr != allocator_) {
    if (nullptr != schema_guard_) {
      schema_guard_->~ObSchemaGetterGuard();
      storage_schema_->~ObStorageSchema();
      allocator_->free(schema_guard_);
      schema_guard_ = nullptr;
      storage_schema_ = nullptr;
    }

    if (nullptr != clog_buf_) {
      allocator_->free(clog_buf_);
      clog_buf_ = nullptr;
      clog_len_ = 0;
    }

    if (nullptr != logcb_ptr_) {
      allocator_->free(logcb_ptr_);
      logcb_ptr_ = nullptr;
    }

    allocator_ = nullptr;
  }
}

int ObStorageSchemaRecorder::get_expected_schema_guard(
    const int64_t table_id,
    int64_t &table_version)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *t_schema = NULL;

  if (table_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(tablet_id), K(table_version));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", K(ret), K_(tablet_id), K(table_version));
  } else if (OB_FAIL(MTL(ObTenantSchemaService*)->get_schema_service()->get_tenant_schema_guard(MTL_ID(), *schema_guard_))
             || OB_FAIL(schema_guard_->get_table_schema(MTL_ID(), table_id, t_schema))
             || NULL == t_schema
             || table_version > t_schema->get_schema_version()) {
    // The version is checked here, so there is no need to check whether it is full
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get schema",  K(ret), K_(tablet_id), K(table_version), KPC(t_schema));
    if (NULL != t_schema) {
      LOG_WARN("current schema version", K(t_schema->get_schema_version()));
    }
  } else {
    table_version = t_schema->get_schema_version();
    if (OB_FAIL(storage_schema_->init(*allocator_, *t_schema, lib::get_compat_mode()))) {
      LOG_WARN("failed to init storage schema", K(ret), K(t_schema));
    }
  }

  return ret;
}

int64_t ObStorageSchemaRecorder::calc_schema_log_size() const
{
  const int64_t size = tablet_id_.get_serialize_size()
      + serialization::encoded_length_i64(storage_schema_->get_schema_version()) // tablet_id + schema_version
      + storage_schema_->get_serialize_size();
  return size;
}

int ObStorageSchemaRecorder::submit_schema_log(const int64_t table_id)
{
  int ret = OB_SUCCESS;
  const bool need_nonblock = false;
  const int64_t ref_ts_ns = 0;
  palf::LSN lsn;
  clog_ts_ = -1;

  if (OB_UNLIKELY(nullptr == log_handler_ || nullptr == storage_schema_
      || !tablet_handle_.is_valid()
      || nullptr == clog_buf_
      || clog_len_ <= 0
      || nullptr == allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler or storage_schema is null", K(ret), KP(log_handler_), KP(storage_schema_),
        KP(clog_buf_), K(clog_len_), K(tablet_handle_), KP_(allocator));
  } else if (OB_ISNULL(logcb_ptr_)) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObStorageSchemaLogCb)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      logcb_ptr_ = new(buf) ObStorageSchemaLogCb(*this);
    }
  }
  if (OB_SUCC(ret)) {
    logcb_ptr_->set_table_version(storage_schema_->get_schema_version());
    ATOMIC_STORE(&logcb_finish_flag_, false);
    storage_schema_->set_sync_finish(false);
    if (OB_FAIL(tablet_handle_.get_obj()->save_multi_source_data_unit(storage_schema_,
        ObLogTsRange::MAX_TS, false/*for_replay*/, memtable::MemtableRefOp::INC_REF))) {
      if (OB_BLOCK_FROZEN != ret) {
        LOG_WARN("failed to inc ref for storage schema", K(ret), K_(tablet_id), K(storage_schema_));
      }
    } else if (OB_FAIL(log_handler_->append(clog_buf_, clog_len_, ref_ts_ns, need_nonblock, logcb_ptr_, lsn, clog_ts_))) {
      LOG_WARN("fail to submit log", K(ret), K_(tablet_id));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(dec_ref_on_memtable(false))) {
        LOG_ERROR("failed to dec ref on memtable", K(tmp_ret), K_(ls_id), K_(tablet_id));
      }
    } else {
      LOG_INFO("submit schema log succeed", K(ret), K_(ls_id), K_(tablet_id), K_(clog_ts), K_(clog_len),
          "schema_version", storage_schema_->get_schema_version());
    }
  }

  return ret;
}

int ObStorageSchemaRecorder::generate_clog()
{
  int ret = OB_SUCCESS;

  // tablet_id, schema_version, storage_schema
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  const logservice::ObLogBaseHeader log_header(
      logservice::ObLogBaseType::STORAGE_SCHEMA_LOG_BASE_TYPE,
      logservice::ObReplayBarrierType::STRICT_BARRIER/*need_replay_barrier*/);

  // log_header + tablet_id + schema_version + storage_schema
  if (OB_UNLIKELY(nullptr == storage_schema_ || nullptr == allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_schema is null", K(ret), KP(storage_schema_));
  } else if (OB_UNLIKELY(!storage_schema_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data storage schema is invalid", K(ret), K_(tablet_id), K(storage_schema_));
  } else if (FALSE_IT(buf_len = log_header.get_serialize_size() + calc_schema_log_size())) {
  } else if (buf_len >= common::OB_MAX_LOG_ALLOWED_SIZE) { // need be separated into several clogs
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("schema log too long", K(buf_len), LITERAL_K(common::OB_MAX_LOG_ALLOWED_SIZE));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K_(tablet_id));
  } else if (OB_FAIL(log_header.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize log header", K(ret));
  } else if (OB_FAIL(tablet_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize tablet_id", K(ret), K_(tablet_id));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, storage_schema_->get_schema_version()))) {
    LOG_WARN("fail to serialize table_version", K(ret), K_(tablet_id));
  } else if (OB_FAIL(storage_schema_->serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize data_table_schema", K(ret), K_(tablet_id));
  }

  if (OB_SUCC(ret)) {
    clog_buf_ = buf;
    clog_len_ = pos;
  } else if (nullptr != buf && nullptr != allocator_) {
    allocator_->free(buf);
    buf = nullptr;
  }
  return ret;
}

} // storage
} // oceanbase
