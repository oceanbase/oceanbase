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

#include "ob_trans_audit_record_mgr.h"
#include "ob_trans_ctx.h"
#include "ob_trans_part_ctx.h"
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/rc/ob_rc.h"

namespace oceanbase {
namespace transaction {
using namespace common;

int ObTransAuditRecord::init(ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (NULL == ctx) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid ctx", K(ctx));
  } else if (lock_.try_wrlock()) {
    reset();
    trans_info_.ctx_addr_ = (int64_t)(ctx);
    is_valid_ = true;
    ctx_ = ctx;
    lock_.unlock();
  } else {
    ret = OB_INIT_FAIL;
  }

  return ret;
}

void ObTransAuditRecord::reset()
{
  common_info_.reset();

  trans_info_.reset();
  trace_log_.reset();

  for (int32_t i = 0; i < STMT_INFO_COUNT; i++) {
    stmt_info_[i].reset();
  }

  is_valid_ = false;
  ctx_ = NULL;
}

int ObTransAuditRecord::get_trans_audit_data(
    ObTransAuditCommonInfo& common_info, ObTransAuditInfo& trans_info, char* trace_log_buffer, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);

  if (!is_valid_) {
    ret = OB_INVALID_DATA;
    TRANS_LOG(DEBUG, "Trans audit record invalid", KR(ret));
  } else if (NULL != ctx_) {
    common_info.tenant_id_ = ctx_->get_tenant_id();
    common_info.server_addr_ = ctx_->get_addr();
    common_info.trans_id_ = ctx_->get_trans_id();
    common_info.partition_key_ = ctx_->get_partition();
    trans_info.session_id_ = ctx_->get_session_id();
    trans_info.proxy_session_id_ = ctx_->get_proxy_session_id();
    trans_info.trans_type_ = ctx_->get_trans_type();
    trans_info.ctx_create_time_ = ctx_->get_ctx_create_time();
    trans_info.expired_time_ = ctx_->get_trans_expired_time();
    trans_info.trans_param_ = ctx_->get_trans_param();
    trans_info.trans_ctx_type_ = ctx_->get_type();
    trans_info.total_sql_no_ = trans_info_.total_sql_no_;
    trans_info.ctx_refer_ = ctx_->get_uref();
    trans_info.ctx_addr_ = (int64_t)(ctx_);
    trans_info.status_ = ctx_->get_status();
    trans_info.for_replay_ = ctx_->is_for_replay();
    if (ObTransCtxType::PARTICIPANT == ctx_->get_type()) {
      ObPartTransCtx* part_ctx = reinterpret_cast<ObPartTransCtx*>(ctx_);
      ObElrTransArrGuard prev_trans_guard;
      ObElrTransArrGuard next_trans_guard;
      (void)part_ctx->get_ctx_dependency_wrap().get_prev_trans_arr_guard(prev_trans_guard);
      (void)part_ctx->get_ctx_dependency_wrap().get_next_trans_arr_guard(next_trans_guard);
      (void)trans_info.prev_trans_arr_.assign(prev_trans_guard.get_elr_trans_arr());
      (void)trans_info.next_trans_arr_.assign(next_trans_guard.get_elr_trans_arr());
    }
    (void)trace_log_.to_string(trace_log_buffer, buf_len);
  } else {
    common_info = common_info_;
    trans_info = trans_info_;
    (void)trace_log_.to_string(trace_log_buffer, buf_len);
  }
  return ret;
}

int ObTransAuditRecord::get_trans_sql_audit_data(
    ObTransAuditCommonInfo& common_info, ObTransAuditStmtInfoIterator& stmt_info_iter)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);

  if (!is_valid_) {
    ret = OB_INVALID_DATA;
    TRANS_LOG(WARN, "Trans audit record invalid", KR(ret));
  } else {
    common_info = common_info_;
    for (int64_t i = 0; i < STMT_INFO_COUNT; i++) {
      if (0 == stmt_info_[i].sql_no_) {
        // Invalid sql record
        continue;
      } else if (OB_FAIL(stmt_info_iter.push(stmt_info_[i]))) {
        TRANS_LOG(WARN, "iterate sql audit failed", KR(ret));
        break;
      }
    }
  }
  return ret;
}

int ObTransAuditRecord::set_trans_dep_arr(
    const ObElrTransInfoArray& prev_trans_arr, const ObElrTransInfoArray& next_trans_arr)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (!is_valid_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Trans audit record invalid", KR(ret));
  } else if (OB_FAIL(trans_info_.prev_trans_arr_.assign(prev_trans_arr))) {
    TRANS_LOG(WARN, "Assign trans audit record prev_trans_arr failed", KR(ret));
  } else if (OB_FAIL(trans_info_.next_trans_arr_.assign(next_trans_arr))) {
    TRANS_LOG(WARN, "Assign trans audit record next_trans_arr failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObTransAuditRecord::set_trans_audit_data(int64_t tenant_id, const common::ObAddr& addr, const ObTransID& trans_id,
    const common::ObPartitionKey& pkey, uint64_t session_id, uint64_t proxy_session_id, int32_t trans_type,
    int32_t ctx_refer, uint64_t ctx_create_time, uint64_t expired_time, const ObStartTransParam& trans_param,
    int64_t trans_ctx_type, int status, bool for_replay)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (!is_valid_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Trans audit record invalid", KR(ret));
  } else {
    common_info_.tenant_id_ = tenant_id;
    common_info_.server_addr_ = addr;
    common_info_.trans_id_ = trans_id;
    common_info_.partition_key_ = pkey;
    trans_info_.session_id_ = session_id;
    trans_info_.proxy_session_id_ = proxy_session_id;
    trans_info_.trans_type_ = trans_type;
    trans_info_.ctx_create_time_ = ctx_create_time;
    trans_info_.expired_time_ = expired_time;
    trans_info_.trans_param_ = trans_param;
    trans_info_.trans_ctx_type_ = trans_ctx_type;
    trans_info_.ctx_refer_ = ctx_refer;
    trans_info_.ctx_addr_ = (int64_t)(ctx_);
    trans_info_.status_ = status;
    trans_info_.for_replay_ = for_replay;

    ctx_ = NULL;  // The transaction context is released,
                  // the audit buffer needs to be accessed in the future,
                  // and the pointer cannot be accessed.
  }
  return ret;
}

int ObTransAuditRecord::set_start_stmt_info(int64_t sql_no, sql::ObPhyPlanType phy_plan_type,
    const ObTraceIdAdaptor& trace_id, int64_t proxy_receive_us, int64_t server_receive_us, int64_t trans_receive_us)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (!is_valid_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Trans audit record invalid", KR(ret));
  } else if (sql_no <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid sql no", K(sql_no));
  } else if (sql_no > STMT_INFO_COUNT) {
    // Ignore
  } else {
    TRANS_LOG(DEBUG, "New start stmt record", K(sql_no), K(trace_id));
    ObTransAuditStmtInfo& info = stmt_info_[sql_no - 1];
    info.sql_no_ = sql_no;
    info.phy_plan_type_ = phy_plan_type;
    info.trace_id_ = trace_id;
    info.ctx_addr_ = trans_info_.ctx_addr_;
    info.proxy_receive_us_ = proxy_receive_us;
    info.server_receive_us_ = server_receive_us;
    info.trans_receive_us_ = trans_receive_us;
  }
  return ret;
}

int ObTransAuditRecord::set_end_stmt_info(int64_t sql_no, int64_t trans_execute_us, int64_t lock_for_read_retry_count)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (!is_valid_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Trans audit record invalid", KR(ret));
  } else if (sql_no <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid sql no", K(sql_no));
  } else {
    trans_info_.total_sql_no_++;
    if (sql_no <= STMT_INFO_COUNT) {
      TRANS_LOG(DEBUG, "New end stmt record", K(sql_no));
      ObTransAuditStmtInfo& info = stmt_info_[sql_no - 1];
      info.trans_execute_us_ = trans_execute_us;
      info.lock_for_read_retry_count_ = lock_for_read_retry_count;
    }
  }
  return ret;
}

bool ObTransAuditRecord::is_valid()
{
  SpinRLockGuard guard(lock_);
  return is_valid_;
}

int ObTransAuditRecordMgr::init(const int32_t mem_size, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  const int32_t record_size = sizeof(ObTransAuditRecord);
  int32_t record_count = mem_size / record_size;
  ObMemAttr memattr(tenant_id, "TransAudit");

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObTransAuditRecordMgr inited twice");
  } else if (mem_size <= 0 || record_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid memory size", K(mem_size));
  } else if (NULL ==
             (records_ = reinterpret_cast<ObTransAuditRecord*>(ob_malloc(record_count * record_size, memattr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(ERROR, "alloc trans audit recodes failed", KR(ret));
  } else if (OB_FAIL(free_addrs_.init(record_count))) {
    TRANS_LOG(WARN, "fixed queue free_addrs init failed", K_(record_count), KR(ret));
  } else {
    for (int32_t i = 0; i < record_count; i++) {
      ObTransAuditRecord* rec = records_ + i;
      if (NULL == (rec = new (rec) ObTransAuditRecord)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "construct ObTransAuditRecord failed", KR(ret));
        break;
      } else if (OB_FAIL(free_addrs_.push(rec))) {
        TRANS_LOG(WARN, "free_addrs_ push failed", KR(ret));
        break;
      } else {
        rec->reset();
      }
    }
    record_count_ = record_count;
    is_inited_ = OB_SUCC(ret);
  }

  return ret;
}

void ObTransAuditRecordMgr::destroy()
{
  if (is_inited_) {
    free_addrs_.destroy();
    for (int64_t i = 0; i < record_count_; i++) {
      ObTransAuditRecord* rec = records_ + i;
      rec->~ObTransAuditRecord();
    }
    ob_free(records_);
    record_count_ = 0;
    is_inited_ = false;
  }
}

int ObTransAuditRecordMgr::mtl_init(ObTransAuditRecordMgr*& rec_mgr)
{
  int ret = OB_SUCCESS;
  rec_mgr = OB_NEW(ObTransAuditRecordMgr, ObModIds::OB_TRANS_AUDIT_RECORD);
  if (nullptr == rec_mgr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "failed to alloc memory for ObTransAuditRecordMgr", KR(ret));
  } else {
    if (OB_FAIL(
            rec_mgr->init(!lib::is_mini_mode() ? MAX_TRANS_RECORD_MEMORY_SIZE : MINI_MODE_MAX_TRANS_RECORD_MEMORY_SIZE,
                lib::current_tenant_id()))) {
      TRANS_LOG(WARN, "failed to init trans audit record manager", KR(ret));
    }
  }
  if (OB_FAIL(ret) && rec_mgr != nullptr) {
    // cleanup
    common::ob_delete(rec_mgr);
    rec_mgr = nullptr;
  }
  return ret;
}

void ObTransAuditRecordMgr::mtl_destroy(ObTransAuditRecordMgr*& rec_mgr)
{
  common::ob_delete(rec_mgr);
  rec_mgr = nullptr;
}

int ObTransAuditRecordMgr::get_empty_record(ObTransAuditRecord*& record)
{
  int ret = OB_SUCCESS;
  ObTransAuditRecord* rec = NULL;
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransAuditRecordMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(free_addrs_.pop(rec))) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(WARN, "pop from free_addrs_ failed", KR(ret));
    }
  } else {
    record = rec;
  }
  return ret;
}

int ObTransAuditRecordMgr::revert_record(ObTransAuditRecord* record)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransAuditRecordMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(free_addrs_.push(record))) {
    TRANS_LOG(WARN, "push into free_addrs_ failed", KR(ret));
  }
  return ret;
}

int ObTransAuditRecordMgr::get_record(const int32_t index, ObTransAuditRecord*& record)
{
  int ret = OB_SUCCESS;
  ObTransAuditRecord* tmp_rec = records_ + index;
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransAuditRecordMgr not inited");
    ret = OB_NOT_INIT;
  } else if (index < 0 || index >= record_count_) {
    TRANS_LOG(WARN, "ObTransAuditRecordMgr get_record out of range", K(index));
    ret = OB_ERROR_OUT_OF_RANGE;
  } else if (tmp_rec->is_valid()) {
    record = tmp_rec;
  } else {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

int ObTransAuditRecordIterator::init(ObTransAuditRecordMgr* mgr)
{
  int ret = OB_SUCCESS;
  if (nullptr == mgr) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    record_mgr_ = mgr;
    record_index_ = 0;
    last_ret_ = OB_SUCCESS;
  }
  return ret;
}

bool ObTransAuditRecordIterator::is_vaild()
{
  bool bool_ret = false;
  if (nullptr == record_mgr_) {
    bool_ret = false;
  } else if (record_index_ < 0 || record_index_ >= record_mgr_->get_record_count()) {
    bool_ret = false;
  } else if (OB_SUCCESS != last_ret_) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int ObTransAuditRecordIterator::get_next(ObTransAuditRecord*& record)
{
  int ret = OB_INVALID_DATA;
  ObTransAuditRecord* rec = NULL;
  // Find a record that contains valid data
  while (OB_INVALID_DATA == ret) {
    if (record_index_ >= record_mgr_->get_record_count()) {
      ret = OB_ITER_END;
    } else if (OB_SUCC(record_mgr_->get_record(record_index_++, rec))) {
      record = rec;
    }
  }
  last_ret_ = ret;
  return ret;
}

int ObTransAuditDataIterator::get_next(
    ObTransAuditCommonInfo& common_info, ObTransAuditInfo& trans_info, char* trace_log_buffer, int64_t buf_len)
{
  int ret = OB_INVALID_DATA;
  ObTransAuditRecord* rec = NULL;
  if (OB_FAIL(rec_iter_.get_next(rec))) {
    TRANS_LOG(WARN, "ObTransAuditDataIterator get next record failed", KR(ret));
  } else if (OB_FAIL(rec->get_trans_audit_data(common_info, trans_info, trace_log_buffer, buf_len))) {
    TRANS_LOG(WARN, "ObTransAuditDataIterator get trans audit failed", KR(ret));
  } else {
  }
  return ret;
}

int ObTransSQLAuditDataIterator::init(ObTransAuditRecordMgr* mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stmt_info_iter_.set_ready())) {
    TRANS_LOG(WARN, "ObTransSQLAuditDataIterator stmt_info_iter set_ready failed", KR(ret));
  } else if (OB_FAIL(rec_iter_.init(mgr))) {
    TRANS_LOG(WARN, "ObTransSQLAuditDataIterator rec_iter_ init failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObTransSQLAuditDataIterator::get_next(ObTransAuditCommonInfo& common_info, ObTransAuditStmtInfo& stmt_info)
{
  int ret = OB_SUCCESS;
  ObTransAuditRecord* rec = NULL;
  ObTransAuditStmtInfo tmp_stmt_info;
  while (OB_SUCC(ret)) {
    if (OB_ITER_END == (ret = stmt_info_iter_.get_next(tmp_stmt_info))) {
      if (OB_FAIL(rec_iter_.get_next(rec))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "ObTransSQLAuditDataIterator get next record failed", KR(ret));
        }
      } else {
        common_info_.reset();
        stmt_info_iter_.reset();
        if (OB_FAIL(rec->get_trans_sql_audit_data(common_info_, stmt_info_iter_))) {
          TRANS_LOG(WARN, "ObTransSQLAuditDataIterator get sql audit failed", KR(ret));
        } else if (OB_FAIL(stmt_info_iter_.set_ready())) {
          TRANS_LOG(WARN, "ObTransSQLAuditDataIterator stmt_info_iter set_ready failed", KR(ret));
        }
      }
    } else {
      break;
    }
  }

  if (OB_SUCC(ret)) {
    common_info = common_info_;
    stmt_info = tmp_stmt_info;
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
