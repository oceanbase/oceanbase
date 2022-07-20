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

#include "ob_trans_define.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "ob_trans_sche_ctx.h"
#include "ob_trans_part_ctx.h"
#include "storage/ob_partition_service.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "ob_trans_service.h"
#include "observer/ob_server.h"
#include "lib/profile/ob_trace_id.h"
#include "sql/session/ob_sql_session_info.h"
#include "clog/ob_max_log_meta_info.h"
#include "clog/ob_clog_config.h"
namespace oceanbase {
using namespace common;
using namespace sql;
using namespace storage;
using namespace memtable;
using namespace observer;
using namespace clog;

namespace transaction {
OB_SERIALIZE_MEMBER(ObTransID, hv_, server_, inc_, timestamp_);
// no need to serialize and deserialize autocommit
OB_SERIALIZE_MEMBER(ObStartTransParam, access_mode_, type_, isolation_, consistency_type_, cluster_version_,
    is_inner_trans_, read_snapshot_type_);
OB_SERIALIZE_MEMBER(ObTransSnapInfo, snapshot_version_, sql_no_, trans_id_, is_cursor_or_nested_, read_sql_no_);
OB_SERIALIZE_MEMBER(ObStmtDesc, phy_plan_type_, stmt_type_, consistency_level_, execution_id_, sql_id_, trace_id_,
    inner_sql_, app_trace_id_str_, nested_sql_, trace_id_adaptor_, stmt_tenant_id_, is_sfu_, is_contain_inner_table_,
    trx_lock_timeout_);
OB_SERIALIZE_MEMBER(ObPartitionLeaderInfo, partition_, addr_);
OB_SERIALIZE_MEMBER(ObPartitionLogInfo, partition_, log_id_, log_timestamp_);
OB_SERIALIZE_MEMBER(ObStmtInfo, nested_sql_, start_stmt_cnt_, end_stmt_cnt_);
OB_SERIALIZE_MEMBER(ObPartitionEpochInfo, partition_, epoch_);
OB_SERIALIZE_MEMBER(ObTaskInfo, sql_no_, active_task_cnt_, snapshot_version_);
OB_SERIALIZE_MEMBER(ObTransTaskInfo, tasks_);
OB_SERIALIZE_MEMBER(ObTransStmtInfo, sql_no_, start_task_cnt_, end_task_cnt_, need_rollback_, task_info_);
OB_SERIALIZE_MEMBER(ObElrTransInfo, trans_id_, commit_version_, result_);
OB_SERIALIZE_MEMBER(ObStmtPair, from_, to_);
OB_SERIALIZE_MEMBER(ObStmtRollbackInfo, stmt_pair_array_);
OB_SERIALIZE_MEMBER(MonotonicTs, mts_);
OB_SERIALIZE_MEMBER(ObTransSSTableDurableCtxInfo, trans_table_info_, partition_, trans_param_, tenant_id_,
    trans_expired_time_, cluster_id_, scheduler_, coordinator_, participants_, prepare_status_, prev_redo_log_ids_,
    app_trace_id_str_, partition_log_info_arr_, prev_trans_arr_, can_elr_, max_durable_log_ts_, global_trans_version_,
    commit_log_checksum_, state_, prepare_version_, max_durable_sql_no_, trans_type_, elr_prepared_state_,
    is_dup_table_trans_, redo_log_no_, mutator_log_no_, stmt_info_, min_log_ts_, sp_user_request_, need_checksum_,
    prepare_log_id_, prepare_log_timestamp_, clear_log_base_ts_, min_log_id_);
OB_SERIALIZE_MEMBER(ObXATransID, gtrid_str_, bqual_str_, format_id_);

int64_t ObTransID::s_inc_num = 1;
int64_t ObTransID::s_last_sample_ts = 0;

ObTransID::ObTransID(const ObTransID& trans_id) : hv_(0), inc_(0), timestamp_(0)
{
  server_ = trans_id.server_;
  inc_ = trans_id.inc_;
  timestamp_ = trans_id.timestamp_;
  hv_ = trans_id.hv_;
}

ObTransID::ObTransID(const ObAddr& server) : hv_(0), inc_(0), timestamp_(0)
{
  server_ = server;
  inc_ = ATOMIC_FAA(&s_inc_num, 1);
  timestamp_ = ObClockGenerator::getRealClock();
  if (OB_UNLIKELY(timestamp_ - s_last_sample_ts > SAMPLE_INTERVAL)) {
    s_last_sample_ts = timestamp_;
    timestamp_ = timestamp_ - (timestamp_ % 1000);
  } else if (OB_UNLIKELY(0 == timestamp_ % 1000)) {
    timestamp_++;
  } else {
    // do nothing
  }
  hv_ = hash_();
}

ObTransID::ObTransID(const common::ObAddr& server, const int64_t inc, const int64_t timestamp)
    : server_(server), inc_(inc), timestamp_(timestamp)
{
  hv_ = hash_();
}

int ObTransID::parse(const char* trans_id)
{
  int ret = OB_SUCCESS;
  char server_buf[128];
  if (NULL == trans_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (4 !=
             sscanf(trans_id, "{hash:%lu, inc:%ld, addr:\"%[^\"]\", t:%ld}", &hv_, &inc_, server_buf, &timestamp_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(server_.parse_from_cstring(server_buf))) {
  } else {
    hv_ = hash_();
  }
  return ret;
}

ObTransID& ObTransID::operator=(const ObTransID& trans_id)
{
  if (this != &trans_id) {
    server_ = trans_id.server_;
    inc_ = trans_id.inc_;
    timestamp_ = trans_id.timestamp_;
    hv_ = trans_id.hv_;
  }

  return *this;
}

int ObTransID::compare(const ObTransID& other) const
{
  int compare_ret = 0;
  if (this == &other) {
    compare_ret = 0;
  } else if (hv_ != other.hv_) {
    // iterate transaction ctx sequentially
    compare_ret = hv_ > other.hv_ ? 1 : -1;
  } else if (other.server_ != server_) {
    compare_ret = other.server_ < server_ ? 1 : -1;
  } else if (inc_ != other.get_inc_num()) {
    compare_ret = inc_ > other.get_inc_num() ? 1 : -1;
  } else if (timestamp_ != other.timestamp_) {
    compare_ret = timestamp_ > other.timestamp_ ? 1 : -1;
  } else {
    compare_ret = 0;
  }
  return compare_ret;
}

bool ObTransID::operator==(const ObTransID& trans_id) const
{
  return server_ == trans_id.server_ && inc_ == trans_id.inc_ && timestamp_ == trans_id.timestamp_;
}

bool ObTransID::operator!=(const ObTransID& trans_id) const
{
  return server_ != trans_id.server_ || inc_ != trans_id.inc_ || timestamp_ != trans_id.timestamp_;
}

uint64_t ObTransID::hash() const
{
  return hv_;
}

inline uint64_t ObTransID::hash_() const
{
  uint64_t hv = 0;

  hv = server_.hash();
  hv = murmurhash(&inc_, sizeof(inc_), hv);
  hv = murmurhash(&timestamp_, sizeof(timestamp_), hv);

  return hv;
}

void ObTransID::reset()
{
  hv_ = 0;
  server_.reset();
  inc_ = 0;
  timestamp_ = 0;
}

DEFINE_TO_STRING_AND_YSON(ObTransID, OB_ID(hash), hv_, OB_ID(inc), inc_, OB_ID(addr), server_, OB_ID(t), timestamp_)

void ObStmtParam::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  stmt_expired_time_ = 0;
  is_retry_sql_ = false;
  trx_elr_ = false;
  safe_weak_read_snapshot_ = 0;
  weak_read_snapshot_source_ = 0;
}

int ObStmtParam::init(const uint64_t tenant_id, const int64_t stmt_expired_time, const bool is_retry_sql)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(tenant_id) || stmt_expired_time <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(stmt_expired_time), K(is_retry_sql));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
    stmt_expired_time_ = stmt_expired_time;
    is_retry_sql_ = is_retry_sql;
  }

  return ret;
}

int ObStmtParam::init(const uint64_t tenant_id, const int64_t stmt_expired_time, const bool is_retry_sql,
    const int64_t safe_weak_read_snapshot, const int64_t weak_read_snapshot_source, const bool trx_elr)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(tenant_id) || stmt_expired_time <= 0 || safe_weak_read_snapshot < 0) {
    TRANS_LOG(
        WARN, "invalid argument", K(tenant_id), K(stmt_expired_time), K(is_retry_sql), K(safe_weak_read_snapshot));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
    stmt_expired_time_ = stmt_expired_time;
    is_retry_sql_ = is_retry_sql;
    safe_weak_read_snapshot_ = safe_weak_read_snapshot;
    weak_read_snapshot_source_ = weak_read_snapshot_source;
    trx_elr_ = trx_elr;
  }

  return ret;
}

int64_t ObStmtParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf,
      buf_len,
      pos,
      "[tenant_id=%lu, stmt_expired_time=%ld, is_retry_sql=%d, "
      "safe_weak_read_snapshot=%ld, "
      "weak_read_snapshot_source=%ld, weak_read_snapshot_source_str=%s, is_trx_elr=%d]",
      tenant_id_,
      stmt_expired_time_,
      is_retry_sql_,
      safe_weak_read_snapshot_,
      weak_read_snapshot_source_,
      ObSQLSessionInfo::source_to_string(weak_read_snapshot_source_),
      trx_elr_);
  return pos;
}

bool ObStmtParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && stmt_expired_time_ > 0 && safe_weak_read_snapshot_ >= 0;
}

// class ObStartTransParam
void ObStartTransParam::reset()
{
  access_mode_ = ObTransAccessMode::UNKNOWN;
  type_ = ObTransType::UNKNOWN;
  isolation_ = ObTransIsolation::UNKNOWN;
  magic_ = 0xF0F0F0F0F0F0F0F0;
  autocommit_ = false;
  consistency_type_ = ObTransConsistencyType::CURRENT_READ;
  read_snapshot_type_ = ObTransReadSnapshotType::STATEMENT_SNAPSHOT;
  cluster_version_ = ObStartTransParam::INVALID_CLUSTER_VERSION;
  is_inner_trans_ = false;
}

bool ObStartTransParam::is_valid() const
{
  return ObTransAccessMode::is_valid(access_mode_) && ObTransType::is_valid(type_) &&
         ObTransIsolation::is_valid(isolation_) && ObTransConsistencyType::is_valid(consistency_type_) &&
         ObTransReadSnapshotType::is_valid(read_snapshot_type_);
}

int ObStartTransParam::set_access_mode(const int32_t access_mode)
{
  int ret = OB_SUCCESS;

  if (!ObTransAccessMode::is_valid(access_mode)) {
    TRANS_LOG(WARN, "invalid argument", K(access_mode));
    ret = OB_INVALID_ARGUMENT;
  } else if (MAGIC_NUM != magic_) {
    TRANS_LOG(ERROR, "magic number error", K_(magic));
    ret = OB_ERR_UNEXPECTED;
  } else {
    access_mode_ = access_mode;
  }

  return ret;
}

int ObStartTransParam::set_type(const int32_t type)
{
  int ret = OB_SUCCESS;

  if (!ObTransType::is_valid(type)) {
    TRANS_LOG(WARN, "invalid argument", K(type));
    ret = OB_INVALID_ARGUMENT;
  } else if (MAGIC_NUM != magic_) {
    TRANS_LOG(ERROR, "magic number error", K_(magic));
    ret = OB_ERR_UNEXPECTED;
  } else {
    type_ = type;
  }

  return ret;
}

int ObStartTransParam::set_isolation(const int32_t isolation)
{
  int ret = OB_SUCCESS;

  if (!ObTransIsolation::is_valid(isolation)) {
    TRANS_LOG(WARN, "invalid argument", K(isolation));
    ret = OB_INVALID_ARGUMENT;
  } else if (MAGIC_NUM != magic_) {
    TRANS_LOG(ERROR, "magic number error", K_(magic));
    ret = OB_ERR_UNEXPECTED;
  } else {
    isolation_ = isolation;
  }

  return ret;
}

bool ObStartTransParam::is_serializable_isolation() const
{
  return ObTransIsolation::SERIALIZABLE == isolation_ || ObTransIsolation::REPEATABLE_READ == isolation_;
}

int64_t ObStartTransParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf,
      buf_len,
      pos,
      "[access_mode=%d, type=%d, isolation=%d, magic=%lu, autocommit=%d, "
      "consistency_type=%d(%s), read_snapshot_type=%d(%s), cluster_version=%lu, "
      "is_inner_trans=%d]",
      access_mode_,
      type_,
      isolation_,
      magic_,
      autocommit_,
      consistency_type_,
      ObTransConsistencyType::cstr(consistency_type_),
      read_snapshot_type_,
      ObTransReadSnapshotType::cstr(read_snapshot_type_),
      cluster_version_,
      is_inner_trans_);
  return pos;
}

int ObStartTransParam::reset_read_snapshot_type_for_isolation()
{
  int ret = OB_SUCCESS;
  if (is_serializable_isolation()) {
    read_snapshot_type_ = ObTransReadSnapshotType::TRANSACTION_SNAPSHOT;
  } else {
    read_snapshot_type_ = ObTransReadSnapshotType::STATEMENT_SNAPSHOT;
  }
  return ret;
}

bool ObTransSnapInfo::is_valid() const
{
  bool ret = false;

  if (is_cursor_or_nested_) {
    ret = snapshot_version_ > 0 && sql_no_ > 0 && trans_id_.is_valid();
  } else {
    ret = snapshot_version_ > 0;
  }

  return ret;
}

int ObTraceInfo::init()
{
  int ret = OB_SUCCESS;

  if (!app_trace_info_.empty() || !app_trace_id_.empty()) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObTraceInfo init twice", K(ret), K_(app_trace_info));
  } else {
    app_trace_info_.reset();
    (void)app_trace_info_.assign_buffer(app_trace_info_buffer_, sizeof(app_trace_info_buffer_));
    app_trace_id_.reset();
    (void)app_trace_id_.assign_buffer(app_trace_id_buffer_, sizeof(app_trace_id_buffer_));
  }
  return ret;
}

// for trans_desc::reset
void ObTraceInfo::reuse()
{
  reset();
  (void)app_trace_info_.assign_buffer(app_trace_info_buffer_, sizeof(app_trace_info_buffer_));
  (void)app_trace_id_.assign_buffer(app_trace_id_buffer_, sizeof(app_trace_id_buffer_));
}

void ObTraceInfo::reset()
{
  if (app_trace_info_.length() >= MAX_TRACE_INFO_BUFFER) {
    void* buf = app_trace_info_.ptr();
    if (NULL != buf && buf != &app_trace_info_) {
      ob_free(buf);
      buf = NULL;
    }
  }
  app_trace_info_.reset();
  app_trace_id_.reset();
}

int ObTraceInfo::set_app_trace_info(const ObString& app_trace_info)
{
  const int64_t len = app_trace_info.length();
  int ret = OB_SUCCESS;

  if (len < 0 || len > OB_MAX_TRACE_INFO_BUFFER_SIZE) {
    TRANS_LOG(WARN, "unexpected trace info str", K(app_trace_info));
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != app_trace_info_.length()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "different app trace info", K(ret), K(app_trace_info_), K(app_trace_info));
  } else if (len < MAX_TRACE_INFO_BUFFER) {
    (void)app_trace_info_.write(app_trace_info.ptr(), len);
    app_trace_info_buffer_[len] = '\0';
  } else {
    char* buf = NULL;
    if (NULL == (buf = (char*)ob_malloc(len + 1, "AppTraceInfo"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory for app trace info failed", K(ret), K(app_trace_info));
    } else {
      app_trace_info_.reset();
      (void)app_trace_info_.assign_buffer(buf, len + 1);
      (void)app_trace_info_.write(app_trace_info.ptr(), len);
      buf[len] = '\0';
    }
  }

  return ret;
}

int ObTraceInfo::set_app_trace_id(const ObString& app_trace_id)
{
  int ret = OB_SUCCESS;
  const int64_t len = app_trace_id.length();

  if (len < 0 || len > OB_MAX_TRACE_ID_BUFFER_SIZE) {
    TRANS_LOG(WARN, "unexpected trace id str", K(app_trace_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != app_trace_id_.length()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "different app trace id", K(ret), K(app_trace_id_), K(app_trace_id));
  } else {
    (void)app_trace_id_.write(app_trace_id.ptr(), len);
    app_trace_id_buffer_[len] = '\0';
  }

  return ret;
}

ObStmtDesc& ObStmtDesc::operator=(const ObStmtDesc& stmt_desc)
{
  if (this != &stmt_desc) {
    phy_plan_type_ = stmt_desc.phy_plan_type_;
    stmt_type_ = stmt_desc.stmt_type_;
    consistency_level_ = stmt_desc.consistency_level_;
    execution_id_ = stmt_desc.execution_id_;
    sql_id_ = stmt_desc.sql_id_;
    inner_sql_ = stmt_desc.inner_sql_;
    trace_id_adaptor_ = stmt_desc.trace_id_adaptor_;
    app_trace_id_str_.reset();
    (void)app_trace_id_str_.assign_buffer(buffer_, sizeof(buffer_));
    (void)app_trace_id_str_.write(stmt_desc.app_trace_id_str_.ptr(), stmt_desc.app_trace_id_str_.length());
    cur_stmt_specified_snapshot_version_ = stmt_desc.cur_stmt_specified_snapshot_version_;
    nested_sql_ = stmt_desc.nested_sql_;
    cur_query_start_time_ = stmt_desc.cur_query_start_time_;
    stmt_tenant_id_ = stmt_desc.stmt_tenant_id_;
    is_sfu_ = stmt_desc.is_sfu_;
    is_contain_inner_table_ = stmt_desc.is_contain_inner_table_;
  }
  return *this;
}

// class ObStmtDesc
void ObStmtDesc::reset()
{
  phy_plan_type_ = OB_PHY_PLAN_UNINITIALIZED;
  stmt_type_ = stmt::T_NONE;
  consistency_level_ = ObTransConsistencyLevel::UNKNOWN;
  execution_id_ = 0;
  sql_id_.reset();
  trace_id_.reset();
  trace_id_adaptor_.reset();
  inner_sql_ = false;
  buffer_[0] = '\0';
  app_trace_id_str_.reset();
  (void)app_trace_id_str_.assign_buffer(buffer_, sizeof(buffer_));
  cur_stmt_specified_snapshot_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  nested_sql_ = false;
  cur_query_start_time_ = 0;
  stmt_tenant_id_ = OB_INVALID_TENANT_ID;
  is_sfu_ = false;
  is_contain_inner_table_ = false;
  trx_lock_timeout_ = -1;
}

int64_t ObStmtDesc::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  char trace_id[TRACE_ID_LENGTH];
  trace_id_adaptor_.to_string(trace_id, TRACE_ID_LENGTH);
  databuff_printf(buf,
      buf_len,
      pos,
      "[stmt_tenant_id=%ld, phy_plan_type=%d, stmt_type=%d, consistency_level=%ld, "
      "execution_id=%ld, sql_id=%s, trace_id=%s, is_inner_sql=%d, app_trace_id_str=%s, "
      "cur_stmt_specified_snapshot_version=%ld, cur_query_start_time=%ld, is_sfu=%d, "
      "is_contain_inner_table=%d, trx_lock_timeout=%ld]",
      stmt_tenant_id_,
      phy_plan_type_,
      stmt_type_,
      consistency_level_,
      execution_id_,
      sql_id_.ptr(),
      trace_id,
      inner_sql_,
      to_cstring(app_trace_id_str_),
      cur_stmt_specified_snapshot_version_,
      cur_query_start_time_,
      is_sfu_,
      is_contain_inner_table_,
      trx_lock_timeout_);
  return pos;
}

void ObStmtInfo::reset()
{
  nested_sql_ = false;
  start_stmt_cnt_ = 0;
  end_stmt_cnt_ = 0;
}

void ObStmtInfo::reset_stmt_info()
{
  if (start_stmt_cnt_ == end_stmt_cnt_) {
    nested_sql_ = false;
    start_stmt_cnt_ = 0;
    end_stmt_cnt_ = 0;
  }
}

void ObSavepointInfo::reset()
{
  sql_no_ = -1;
  id_.reset();
  id_len_ = 0;
}

int ObSavepointInfo::init(const common::ObString& id, const int64_t sql_no)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(id.empty() || sql_no < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(id), K(sql_no));
  } else if (OB_FAIL(id_.assign(id))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      // rewrite ret
      ret = OB_ERR_TOO_LONG_IDENT;
    }
    TRANS_LOG(WARN, "assign savepoint id error", K(ret), K(id), K(sql_no));
  } else {
    sql_no_ = sql_no;
    id_len_ = id.length();
  }

  return ret;
}

int ObSavepointPartitionInfo::init(const ObPartitionKey& partition, const int64_t max_sql_no)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid() || max_sql_no < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(max_sql_no));
  } else {
    partition_ = partition;
    max_sql_no_ = max_sql_no;
  }

  return ret;
}

int ObSavepointPartitionInfo::update_max_sql_no(const int64_t sql_no)
{
  int ret = OB_SUCCESS;

  if (sql_no <= 0 || sql_no <= max_sql_no_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    max_sql_no_ = sql_no;
  }

  return ret;
}

int ObSavepointMgr::deep_copy(const ObSavepointMgr& sp_mgr)
{
  int ret = OB_SUCCESS;
  if (this != &sp_mgr) {
    if (OB_FAIL(savepoint_arr_.assign(sp_mgr.savepoint_arr_))) {
      TRANS_LOG(WARN, "assign savepoint arr failed", K(ret), K(sp_mgr.savepoint_arr_));
    } else if (OB_FAIL(savepoint_partition_arr_.assign(sp_mgr.savepoint_partition_arr_))) {
      TRANS_LOG(WARN, "assign savepoint partition arr failed", K(ret), K(sp_mgr.savepoint_arr_));
    }
  }
  return ret;
}

int ObSavepointMgr::push_savepoint(const ObString& id, const int64_t sql_no)
{
  int ret = OB_SUCCESS;
  ObSavepointInfo savepoint_info;
  if (OB_FAIL(del_savepoint(id))) {
    if (OB_SAVEPOINT_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "del savepoint failed", KR(ret), K(id));
    } else {
      // rewrite ret
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(savepoint_info.init(id, sql_no))) {
      TRANS_LOG(WARN, "savepoint info init", KR(ret), K(id), K(sql_no));
    } else if (OB_FAIL(savepoint_arr_.push_back(savepoint_info))) {
      TRANS_LOG(WARN, "push back savepoint error", KR(ret), K(savepoint_info));
    } else {
      // do nothing
    }
  }

  return ret;
}

int64_t ObSavepointMgr::find_savepoint_location_(const ObString& id)
{
  int64_t index = -1;

  for (int64_t i = 0; i < savepoint_arr_.count(); i++) {
    ObSavepointInfo& info = savepoint_arr_.at(i);
    if (info.get_id_len() == id.length() && (0 == STRNCMP(info.get_id().ptr(), id.ptr(), info.get_id_len()))) {
      index = i;
      break;
    }
  }

  return index;
}

int ObSavepointMgr::get_savepoint_rollback_partitions_(const int64_t sql_no, ObPartitionArray& partition_arr)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < savepoint_partition_arr_.count(); i++) {
    if (savepoint_partition_arr_.at(i).get_max_sql_no() > sql_no) {
      if (OB_FAIL(partition_arr.push_back(savepoint_partition_arr_.at(i).get_partition()))) {
        TRANS_LOG(WARN, "push back partition error", KR(ret), K(sql_no));
      }
    }
  }

  return ret;
}

int ObSavepointMgr::truncate_savepoint(const ObString& id)
{
  int ret = OB_SUCCESS;
  const int64_t index = find_savepoint_location_(id);

  if (index < 0) {
    ret = OB_SAVEPOINT_NOT_EXIST;
    TRANS_LOG(WARN, "truncate savepoint error", K(ret), K(id), K_(savepoint_arr));
  } else {
    while (savepoint_arr_.count() > index + 1) {
      savepoint_arr_.pop_back();
    }
  }

  return ret;
}

int ObSavepointMgr::del_savepoint(const ObString& id)
{
  int ret = OB_SUCCESS;
  const int64_t index = find_savepoint_location_(id);

  if (OB_UNLIKELY(index < 0)) {
    ret = OB_SAVEPOINT_NOT_EXIST;
  } else if (OB_FAIL(savepoint_arr_.remove(index))) {
    TRANS_LOG(WARN, "savepoint remove error", KR(ret), K(index), K(id));
  } else {
    TRANS_LOG(DEBUG, "del savepoint success", K_(savepoint_arr));
  }

  return ret;
}

int64_t ObSavepointMgr::find_savepoint_partition_location_(const ObPartitionKey& partition)
{
  int64_t index = -1;

  for (int64_t i = 0; i < savepoint_partition_arr_.count(); i++) {
    if (savepoint_partition_arr_.at(i).get_partition() == partition) {
      index = i;
      break;
    }
  }

  return index;
}

int ObSavepointMgr::update_savepoint_partition_info(const ObPartitionArray& participants, const int64_t sql_no)
{
  int ret = OB_SUCCESS;

  if (savepoint_arr_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); i++) {
      const int64_t index = find_savepoint_partition_location_(participants.at(i));
      if (index >= 0) {
        if (OB_FAIL(savepoint_partition_arr_.at(index).update_max_sql_no(sql_no))) {
          TRANS_LOG(
              WARN, "update max sql no error", KR(ret), K(participants), K(sql_no), K(savepoint_partition_arr_.at(i)));
        }
      } else {
        ObSavepointPartitionInfo savepoint_partition_info;
        if (OB_FAIL(savepoint_partition_info.init(participants.at(i), sql_no))) {
          TRANS_LOG(WARN, "savepoint partition info init error", KR(ret), K(participants), K(sql_no));
        } else if (OB_FAIL(savepoint_partition_arr_.push_back(savepoint_partition_info))) {
          TRANS_LOG(WARN, "push back error", KR(ret), K(savepoint_partition_arr_));
        } else {
          // do nothing
        }
      }
    }
  }

  return ret;
}

int ObSavepointMgr::get_savepoint_rollback_info(const ObString& id, int64_t& sql_no, ObPartitionArray& partition_arr)
{
  int ret = OB_SUCCESS;
  const int64_t index = find_savepoint_location_(id);

  if (index < 0) {
    ret = OB_SAVEPOINT_NOT_EXIST;
    TRANS_LOG(WARN, "truncate savepoint error", K(ret), K(id), K_(savepoint_arr));
  } else {
    sql_no = savepoint_arr_.at(index).get_sql_no();
    if (OB_FAIL(get_savepoint_rollback_partitions_(sql_no, partition_arr))) {
      TRANS_LOG(WARN, "find savepoint rollback partitions error", K(ret), K(id), K_(savepoint_arr));
    }
  }

  return ret;
}
void ObStmtRollbackInfo::reset()
{
  stmt_pair_array_.reset();
}

int ObStmtRollbackInfo::push(const ObStmtPair& stmt_pair)
{
  int ret = OB_SUCCESS;
  const int64_t count = stmt_pair_array_.count();
  if (!stmt_pair.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(stmt_pair));
  } else if (count > 0) {
    ObStmtPair& last = stmt_pair_array_.at(count - 1);
    if (stmt_pair.get_from() <= last.get_from()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "illegal sql sequence", K(ret), K(stmt_pair), K_(stmt_pair_array));
    } else if (stmt_pair.get_to() == last.get_from()) {
      last.get_from() = stmt_pair.get_from();
    } else {
      if (OB_FAIL(stmt_pair_array_.push_back(stmt_pair))) {
        TRANS_LOG(WARN, "push back stmt pair failed", K(ret), K(stmt_pair));
      }
    }
  } else {
    if (OB_FAIL(stmt_pair_array_.push_back(stmt_pair))) {
      TRANS_LOG(WARN, "push back stmt pair failed", K(ret), K(stmt_pair));
    }
  }
  return ret;
}

int ObStmtRollbackInfo::search(const int64_t sql_no, ObStmtPair& stmt_pair) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = stmt_pair_array_.count() - 1; i >= 0; i--) {
    const ObStmtPair& tmp_stmt_pair = stmt_pair_array_.at(i);
    if (tmp_stmt_pair.get_from() < sql_no) {
      break;
    } else if (tmp_stmt_pair.get_from() >= sql_no) {
      if (tmp_stmt_pair.get_to() < sql_no) {
        stmt_pair = tmp_stmt_pair;
        ret = OB_SUCCESS;
        break;
      } else {
        // do nothing
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObStmtRollbackInfo::assign(const ObStmtRollbackInfo& other)
{
  return stmt_pair_array_.assign(other.stmt_pair_array_);
}

ObXATransID::ObXATransID(const ObXATransID& xid)
{
  format_id_ = xid.format_id_;
  gtrid_str_.reset();
  bqual_str_.reset();
  gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
  bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
  gtrid_str_.write(xid.gtrid_str_.ptr(), xid.gtrid_str_.length());
  bqual_str_.write(xid.bqual_str_.ptr(), xid.bqual_str_.length());
}

void ObXATransID::reset()
{
  gtrid_str_.reset();
  (void)gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
  bqual_str_.reset();
  (void)bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
  format_id_ = 1;
}

int ObXATransID::set(const ObString& gtrid_str, const ObString& bqual_str, const int64_t format_id)
{
  int ret = OB_SUCCESS;
  if (0 > gtrid_str.length() || 0 > bqual_str.length() || MAX_GTRID_LENGTH < gtrid_str.length() ||
      MAX_BQUAL_LENGTH < bqual_str.length()) {
    TRANS_LOG(WARN, "invalid arguments", K(gtrid_str), K(bqual_str), K(format_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    format_id_ = format_id;
    gtrid_str_.reset();
    bqual_str_.reset();
    gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
    bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
    gtrid_str_.write(gtrid_str.ptr(), gtrid_str.length());
    bqual_str_.write(bqual_str.ptr(), bqual_str.length());
  }
  return ret;
}

int ObXATransID::set(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  if (!xid.is_valid()) {
    TRANS_LOG(WARN, "invalid xid", K(xid));
    ret = OB_INVALID_ARGUMENT;
  } else {
    format_id_ = xid.format_id_;
    gtrid_str_.reset();
    bqual_str_.reset();
    gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
    bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
    gtrid_str_.write(xid.gtrid_str_.ptr(), xid.gtrid_str_.length());
    bqual_str_.write(xid.bqual_str_.ptr(), xid.bqual_str_.length());
  }
  return ret;
}

bool ObXATransID::empty() const
{
  return gtrid_str_.empty();
}

bool ObXATransID::is_valid() const
{
  return 0 <= gtrid_str_.length() && 0 <= bqual_str_.length() && MAX_GTRID_LENGTH >= gtrid_str_.length() &&
         MAX_BQUAL_LENGTH >= bqual_str_.length();
}

ObXATransID& ObXATransID::operator=(const ObXATransID& xid)
{
  if (this != &xid) {
    format_id_ = xid.format_id_;
    gtrid_str_.reset();
    bqual_str_.reset();
    gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
    bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
    gtrid_str_.write(xid.gtrid_str_.ptr(), xid.gtrid_str_.length());
    bqual_str_.write(xid.bqual_str_.ptr(), xid.bqual_str_.length());
  }
  return *this;
}

bool ObXATransID::operator==(const ObXATransID& xid) const
{
  return gtrid_str_ == xid.gtrid_str_ && bqual_str_ == xid.bqual_str_ && format_id_ == xid.format_id_;
}

bool ObXATransID::operator!=(const ObXATransID& xid) const
{
  return gtrid_str_ != xid.gtrid_str_;
  //|| bqual_str_ != xid.bqual_str_
  //|| format_id_ != xid.format_id_;
}

int32_t ObXATransID::to_full_xid_string(char* buffer, const int64_t capacity) const
{
  int64_t count = 0;
  if (NULL == buffer || 0 >= capacity) {
    TRANS_LOG(WARN, "invalid augumnets", KP(buffer), K(capacity));
  } else if (gtrid_str_.empty()) {
    TRANS_LOG(WARN, "xid is empty");
  } else if ((count = snprintf(buffer,
                  capacity,
                  "%.*s%.*s",
                  gtrid_str_.length(),
                  gtrid_str_.ptr(),
                  bqual_str_.length(),
                  bqual_str_.ptr())) <= 0 ||
             count >= capacity) {
    TRANS_LOG(WARN, "buffer is not enough", K(count), K(capacity), K(gtrid_str_.length()), K(bqual_str_.length()));
  } else {
    TRANS_LOG(DEBUG, "generate buffer success", K(count), K(capacity), K(gtrid_str_.length()), K(bqual_str_.length()));
  }
  return count;
}

bool ObXATransID::all_equal_to(const ObXATransID& other) const
{
  return gtrid_str_ == other.gtrid_str_ && bqual_str_ == other.bqual_str_ && format_id_ == other.format_id_;
}

ObTransDesc::ObTransDesc()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      snapshot_version_(ObTransVersion::INVALID_TRANS_VERSION),
      trans_snapshot_version_(ObTransVersion::INVALID_TRANS_VERSION),
      sql_no_(0),
      need_rollback_(false),
      trans_expired_time_(0),
      cur_stmt_expired_time_(0),
      cluster_version_(0),
      cluster_id_(0),
      snapshot_gene_type_(ObTransSnapshotGeneType::UNKNOWN),
      session_id_(0),
      proxy_session_id_(0),
      can_elr_(false),
      is_dup_table_trans_(false),
      is_local_trans_(true),
      stmt_rollback_info_(),
      trace_info_(),
      is_fast_select_(false),
      xa_end_timeout_seconds_(-1),
      is_nested_stmt_(false),
      max_sql_no_(0),
      stmt_min_sql_no_(0),
      is_tightly_coupled_(true),
      part_ctx_(NULL),
      sche_ctx_(NULL),
      trans_type_(TransType::DIST_TRANS),
      warm_up_ctx_(NULL),
      is_all_select_stmt_(true),
      local_consistency_type_(ObTransConsistencyType::UNKNOWN),
      is_sp_trans_exiting_(false),
      trans_end_(false),
      session_(NULL),
      need_print_trace_log_(false),
      tenant_config_refresh_time_(0),
      start_participant_ts_(0),
      standalone_stmt_desc_(),
      need_check_at_end_participant_(false),
      trx_level_temporary_table_involved_(false),
      audit_record_(NULL),
      trx_idle_timeout_(false)
{
  // if (GCONF._xa_gc_timeout > 86400000000) {
  //  is_tightly_coupled_ = false;
  //} else {
  //  is_tightly_coupled_ = true;
  //}
  xid_.reset();
}

// for test
int ObTransDesc::test_init()
{
  return OB_SUCCESS;
}

int ObTransDesc::init(sql::ObBasicSessionInfo* session)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(session)) {
    TRANS_LOG(WARN, "invalid argument", KP(session));
    ret = OB_INVALID_ARGUMENT;
  } else {
    session_ = session;
  }

  return ret;
}

int ObTransDesc::set_trans_app_trace_id_str(const ObString& app_trace_id_str)
{
  int ret = OB_SUCCESS;

  if (app_trace_id_str.length() < 0 || app_trace_id_str.length() > OB_MAX_TRACE_ID_BUFFER_SIZE) {
    TRANS_LOG(WARN, "unexpected trace id str", K(app_trace_id_str), "stmt_desc", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (app_trace_id_confirmed_) {
    TRANS_LOG(INFO, "app trace id already confirmed", K(app_trace_id_str), K_(trans_id));
  } else if (OB_FAIL(trace_info_.set_app_trace_id(app_trace_id_str))) {
    TRANS_LOG(WARN, "set app trace id error", K(app_trace_id_str), K_(trans_id));
  } else {
    app_trace_id_confirmed_ = true;
  }

  return ret;
}

void ObTransDesc::destroy()
{
  reset();
}

int ObTransDesc::set_session_id(const uint32_t session_id)
{
  int ret = OB_SUCCESS;

  if (UINT32_MAX == session_id) {
    TRANS_LOG(ERROR, "invalid session id, unexpected error", K(session_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    session_id_ = session_id;
  }

  return ret;
}

int ObTransDesc::set_proxy_session_id(const uint64_t proxy_session_id)
{
  int ret = OB_SUCCESS;

  if (UINT64_MAX == proxy_session_id) {
    TRANS_LOG(ERROR, "invalid proxy session id, unexpected error", K(proxy_session_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    proxy_session_id_ = proxy_session_id;
  }

  return ret;
}

void ObTransDesc::consistency_wait()
{
  while (trans_need_wait_wrap_.need_wait()) {
    usleep(trans_need_wait_wrap_.get_remaining_wait_interval_us());
  }
}

void ObTransDesc::reset()
{
  if (need_print_trace_log_) {
    FORCE_PRINT_TRACE(&tlog_, "[trans error] ");
  }
  tenant_id_ = OB_INVALID_TENANT_ID;
  trans_id_.reset();
  snapshot_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  trans_snapshot_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  commit_version_ = 0;
  trans_param_.reset();
  scheduler_.reset();
  participants_.reset();
  gc_participants_.reset();
  participants_pla_.reset();
  stmt_participants_.reset();
  stmt_participants_pla_.reset();
  sql_no_ = 0;
  cur_stmt_desc_.reset();
  need_rollback_ = false;
  trans_expired_time_ = 0;
  cur_stmt_expired_time_ = 0;
  sche_ctx_ = NULL;
  part_ctx_ = NULL;
  cluster_version_ = 0;
  cluster_id_ = 0;
  warm_up_ctx_ = NULL;
  is_all_select_stmt_ = true;
  trans_type_ = TransType::DIST_TRANS;
  local_consistency_type_ = ObTransConsistencyType::UNKNOWN;
  is_sp_trans_exiting_ = false;
  trans_end_ = false;
  session_ = NULL;
  snapshot_gene_type_ = ObTransSnapshotGeneType::UNKNOWN;
  session_id_ = 0;
  proxy_session_id_ = 0;
  app_trace_id_confirmed_ = false;
  last_end_stmt_ts_ = 0;
  tlog_.reset();
  need_print_trace_log_ = false;
  nested_stmt_info_.reset();
  is_dup_table_trans_ = false;
  is_local_trans_ = true;
  stmt_rollback_info_.reset();
  trans_need_wait_wrap_.reset();
  ObSavepointMgr::reset();
  stmt_snapshot_info_.reset();
  is_fast_select_ = false;
  is_nested_stmt_ = false;
  max_sql_no_ = 0;
  stmt_min_sql_no_ = 0;
  trace_info_.reuse();
  need_check_at_end_participant_ = false;
  xid_.reset();
  // can_elr_ = false;
  // need_record_rollback_trans_log_ = false;
  // tenant_config_refresh_time_ = 0;
  start_participant_ts_ = 0;
  standalone_stmt_desc_.reset();
  xa_end_timeout_seconds_ = -1;
  is_nested_stmt_ = false;
  max_sql_no_ = 0;
  stmt_min_sql_no_ = 0;
  trx_level_temporary_table_involved_ = false;
  trx_idle_timeout_ = false;
}

int ObTransDesc::set_trans_consistency_type(const int32_t trans_consistency_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ObTransConsistencyType::is_valid(trans_consistency_type))) {
    TRANS_LOG(WARN, "invalid argument", K(trans_consistency_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    local_consistency_type_ = trans_consistency_type;
  }

  return ret;
}

int ObTransDesc::set_trans_id(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (!trans_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_id_ = trans_id;
  }

  return ret;
}

int ObTransDesc::set_snapshot_version(const int64_t version)
{
  int ret = OB_SUCCESS;

  if (version < 0) {
    TRANS_LOG(WARN, "invalid argument", K(version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    snapshot_version_ = version;
  }

  return ret;
}

int ObTransDesc::set_trans_param(const ObStartTransParam& trans_param)
{
  int ret = OB_SUCCESS;

  if (!trans_param.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(trans_param));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_param_ = trans_param;
  }

  return ret;
}

int ObTransDesc::set_scheduler(const ObAddr& scheduler)
{
  int ret = OB_SUCCESS;

  if (!scheduler.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(scheduler));
    ret = OB_INVALID_ARGUMENT;
  } else {
    scheduler_ = scheduler;
  }

  return ret;
}

int ObTransDesc::set_orig_scheduler(const ObAddr& orig_scheduler)
{
  int ret = OB_SUCCESS;

  if (!orig_scheduler.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(orig_scheduler));
    ret = OB_INVALID_ARGUMENT;
  } else {
    orig_scheduler_ = orig_scheduler;
  }

  return ret;
}

int ObTransDesc::set_stmt_participants_pla(const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;

  if (pla.count() < 0) {
    TRANS_LOG(WARN, "invalid argument", K(pla));
    ret = OB_INVALID_ARGUMENT;
  } else {
    stmt_participants_pla_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); ++i) {
      if (OB_FAIL(stmt_participants_pla_.push(
              pla.get_partitions().at(i), pla.get_leaders().at(i), pla.get_types().at(i)))) {
        TRANS_LOG(WARN, "stmt participants pla push error", KR(ret), K(pla));
      }
    }
  }

  return ret;
}

int ObTransDesc::merge_stmt_participants_pla(const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  bool is_new = true;

  if (pla.count() <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(pla));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); ++i) {
      is_new = true;
      for (int64_t j = 0; j < stmt_participants_pla_.count(); ++j) {
        if (stmt_participants_pla_.get_partitions().at(j) == pla.get_partitions().at(i) &&
            stmt_participants_pla_.get_leaders().at(j) == pla.get_leaders().at(i)) {
          is_new = false;
          break;
        }
      }
      if (is_new && OB_FAIL(stmt_participants_pla_.push(
                        pla.get_partitions().at(i), pla.get_leaders().at(i), pla.get_types().at(i)))) {
        TRANS_LOG(WARN, "extend stmt participant pla push error", KR(ret), K(pla));
      }
    }
  }

  return ret;
}

int ObTransDesc::set_stmt_participants(const ObPartitionArray& stmt_participants)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(stmt_participants_.assign(stmt_participants))) {
    TRANS_LOG(WARN, "assign stmt participants error", KR(ret), K(stmt_participants));
  } else {
    TRANS_LOG(DEBUG, "assign stmt participants success", K(stmt_participants));
  }

  return ret;
}

int ObTransDesc::merge_stmt_participants(const ObPartitionArray& extend_participants)
{
  int ret = OB_SUCCESS;

  if (extend_participants.count() <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(extend_participants));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extend_participants.count(); ++i) {
      if (!contain_(stmt_participants_, extend_participants.at(i))) {
        if (OB_FAIL(stmt_participants_.push_back(extend_participants.at(i)))) {
          TRANS_LOG(WARN, "push back error", KR(ret), "partition", extend_participants.at(i));
        }
      }
    }
  }

  return ret;
}

int ObTransDesc::set_gc_participants(const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(gc_participants_.assign(participants))) {
    TRANS_LOG(WARN, "assign stmt participants error", KR(ret), K(participants));
  } else {
    TRANS_LOG(DEBUG, "assign stmt participants success", K(participants));
  }

  return ret;
}

int ObTransDesc::merge_gc_participants(const ObPartitionArray& extend_participants)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < extend_participants.count(); ++i) {
    if (!contain_(gc_participants_, extend_participants.at(i))) {
      if (OB_FAIL(gc_participants_.push_back(extend_participants.at(i)))) {
        TRANS_LOG(WARN, "push back error", KR(ret), "partition", extend_participants.at(i));
      }
    }
  }

  return ret;
}

int ObTransDesc::merge_participants_pla()
{
  int ret = OB_SUCCESS;
  bool is_new = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_participants_pla_.count(); ++i) {
    is_new = true;
    for (int64_t index = 0; index < participants_pla_.count(); ++index) {
      if (participants_pla_.get_partitions().at(index) == stmt_participants_pla_.get_partitions().at(i) &&
          participants_pla_.get_leaders().at(index) == stmt_participants_pla_.get_leaders().at(i)) {
        is_new = false;
        break;
      }
    }
    if (is_new && OB_FAIL(participants_pla_.push(stmt_participants_pla_.get_partitions().at(i),
                      stmt_participants_pla_.get_leaders().at(i),
                      stmt_participants_pla_.get_types().at(i)))) {
      TRANS_LOG(WARN, "push back error", KR(ret), "index", i, K_(stmt_participants_pla), K_(participants_pla));
    }
  }
  if (OB_SUCC(ret)) {
    stmt_participants_pla_.reset();
  }

  return ret;
}

int ObTransDesc::merge_participants_pla(const common::ObPartitionLeaderArray& participant_pla)
{
  int ret = OB_SUCCESS;
  bool is_new = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < participant_pla.count(); ++i) {
    is_new = true;
    for (int64_t index = 0; index < participants_pla_.count(); ++index) {
      if (participants_pla_.get_partitions().at(index) == participant_pla.get_partitions().at(i) &&
          participants_pla_.get_leaders().at(index) == participant_pla.get_leaders().at(i)) {
        is_new = false;
        break;
      }
    }
    if (is_new && OB_FAIL(participants_pla_.push(participant_pla.get_partitions().at(i),
                      participant_pla.get_leaders().at(i),
                      participant_pla.get_types().at(i)))) {
      TRANS_LOG(WARN, "push back error", K(ret), "index", i, K(participant_pla), K_(participants_pla));
    }
  }

  return ret;
}

int ObTransDesc::merge_participants()
{
  int ret = OB_SUCCESS;

  for (ObPartitionArray::iterator it = stmt_participants_.begin(); it != stmt_participants_.end() && OB_SUCCESS == ret;
       it++) {
    if (!contain_(participants_, *it)) {
      if (OB_FAIL(participants_.push_back(*it))) {
        TRANS_LOG(WARN, "push back error", KR(ret), "partition", *it);
      }
    }
  }
  if (OB_SUCC(ret)) {
    stmt_participants_.reset();
  }

  return ret;
}

int ObTransDesc::merge_participants(const common::ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); ++i) {
    const ObPartitionKey& pkey = participants.at(i);
    if (!contain_(participants_, pkey)) {
      if (OB_FAIL(participants_.push_back(pkey))) {
        TRANS_LOG(WARN, "push back error", K(ret), "partition", pkey);
      }
    }
  }

  return ret;
}

#ifdef ERRSIM
#define INJECT_PARTICIPANTS_OVERFLOW_ERRSIM                                                                        \
  do {                                                                                                             \
    if (OB_FAIL(E(EventTable::EN_PARTICIPANTS_SIZE_OVERFLOW) OB_SUCCESS)) {                                        \
      OB_MAX_TRANS_SERIALIZE_SIZE = 1500;                                                                          \
      OB_MIN_REDO_LOG_SERIALIZE_SIZE = 500;                                                                        \
    } else if (OB_FAIL(E(EventTable::EN_PART_PLUS_UNDO_OVERFLOW) OB_SUCCESS)) {                                    \
      OB_MAX_TRANS_SERIALIZE_SIZE = 7500;                                                                          \
      OB_MIN_REDO_LOG_SERIALIZE_SIZE = 500;                                                                        \
    }                                                                                                              \
                                                                                                                   \
    transaction::OB_MAX_UNDO_ACTION_SERIALIZE_SIZE = OB_MAX_TRANS_SERIALIZE_SIZE - OB_MIN_REDO_LOG_SERIALIZE_SIZE; \
                                                                                                                   \
    TRANS_LOG(INFO,                                                                                                \
        "ERRSIM modify trans ctx serialize size ",                                                                 \
        K(OB_MAX_TRANS_SERIALIZE_SIZE),                                                                            \
        K(OB_MIN_REDO_LOG_SERIALIZE_SIZE),                                                                         \
        K(transaction::OB_MAX_UNDO_ACTION_SERIALIZE_SIZE));                                                        \
                                                                                                                   \
    ret = OB_SUCCESS;                                                                                              \
  } while (false);
#else
#define INJECT_PARTICIPANTS_OVERFLOW_ERRSIM
#endif

int ObTransDesc::check_participants_size()
{
  int ret = OB_SUCCESS;

  INJECT_PARTICIPANTS_OVERFLOW_ERRSIM

  int64_t participants_size = participants_.get_serialize_size();
  if (participants_size > OB_MAX_TRANS_SERIALIZE_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    TRANS_LOG(WARN,
        "Participants are too large which may make dump trans state table failed.",
        KR(ret),
        K(participants_size),
        K(participants_.count()),
        K(OB_MAX_TRANS_SERIALIZE_SIZE),
        K(participants_));
  }
  return ret;
}

int ObTransDesc::set_cur_stmt_desc(const ObStmtDesc& stmt_desc)
{
  int ret = OB_SUCCESS;

  if (!stmt_desc.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(stmt_desc));
    ret = OB_INVALID_ARGUMENT;
  } else {
    cur_stmt_desc_ = stmt_desc;
  }

  return ret;
}

int ObTransDesc::set_trans_expired_time(const int64_t expired_time)
{
  int ret = OB_SUCCESS;

  if (expired_time <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(expired_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_expired_time_ = expired_time;
  }

  return ret;
}

int ObTransDesc::set_cur_stmt_expired_time(const int64_t expired_time)
{
  int ret = OB_SUCCESS;

  if (expired_time <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(expired_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    cur_stmt_expired_time_ = expired_time;
  }

  return ret;
}

bool ObTransDesc::is_valid() const
{
  return trans_id_.is_valid() && trans_param_.is_valid() && sql_no_ >= 0;
}

bool ObTransDesc::is_valid_or_standalone_stmt() const
{
  return (trans_id_.is_valid() && trans_param_.is_valid() && sql_no_ >= 0) || standalone_stmt_desc_.is_valid();
}

bool ObTransDesc::has_create_ctx(const ObPartitionKey& pkey) const
{
  bool bool_ret = false;
  if (!pkey.is_valid()) {
    TRANS_LOG(WARN, "INVALID_ARGUMENT", K(pkey));
  } else {
    bool_ret = contain_(participants_, pkey);
  }
  return bool_ret;
}

bool ObTransDesc::has_create_ctx(const ObPartitionKey& pkey, const ObAddr& addr) const
{
  bool bool_ret = false;
  if (!pkey.is_valid() || !addr.is_valid()) {
    TRANS_LOG(WARN, "INVALID_ARGUMENT", K(pkey), K(addr));
  } else {
    for (int64_t i = 0; i < participants_pla_.count(); ++i) {
      if (pkey == participants_pla_.get_partitions().at(i) && addr == participants_pla_.get_leaders().at(i)) {
        bool_ret = true;
      }
    }
  }

  return bool_ret;
}

int ObTransDesc::set_tenant_id(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(tenant_id)) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
  }

  return ret;
}

int ObTransDesc::set_sche_ctx(ObScheTransCtx* sche_ctx)
{
  sche_ctx_ = sche_ctx;
  return OB_SUCCESS;
}

int ObTransDesc::set_cluster_id(const uint64_t cluster_id)
{
  cluster_id_ = cluster_id;
  return OB_SUCCESS;
}

int ObTransDesc::set_cluster_version(const uint64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (0 >= cluster_version) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    cluster_version_ = cluster_version;
  }
  return ret;
}

int ObTransDesc::set_last_end_stmt_ts(const uint64_t last_end_stmt_ts)
{
  int ret = OB_SUCCESS;

  if (last_end_stmt_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "set_last_end_stmt_ts fail", KR(ret), K(last_end_stmt_ts));
  } else {
    last_end_stmt_ts_ = last_end_stmt_ts;
  }

  return ret;
}

bool ObTransDesc::is_all_ro_participants(const ObPartitionArray& participants, const int64_t participant_cnt) const
{
  int bool_ret = true;

  for (int64_t i = 0; i < participants.count() && i < participant_cnt; i++) {
    if (!is_not_create_ctx_participant(participants.at(i))) {
      bool_ret = false;
      break;
    }
  }

  return bool_ret;
}

int ObTransDesc::find_first_not_ro_participant(
    const ObPartitionArray& participants, ObPartitionKey*& ret_pkey, int64_t& pkey_index) const
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; i < participants.count(); i++) {
    if (!is_not_create_ctx_participant(participants.at(i))) {
      ret_pkey = const_cast<ObPartitionKey*>(&participants.at(i));
      pkey_index = i;
      break;
    }
  }
  if (participants.count() == i) {
    // not found
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

bool ObTransDesc::is_not_create_ctx_participant(
    const ObPartitionKey& pkey, const int64_t user_specified_snapshot /* = -1 */) const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  if (user_specified_snapshot > 0) {
    bool_ret = true;
  } else if (is_bounded_staleness_read()) {
    bool_ret = true;
  } else if (maybe_violate_snapshot_consistency()) {
    bool_ret = false;
  } else if (!cur_stmt_desc_.is_readonly_stmt()) {
    bool is_leader = true;
    if (OB_FAIL(is_leader_stmt_participant(pkey, is_leader))) {}
    if (!is_leader && !is_contain(participants_, pkey)) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  } else if (is_contain(participants_, pkey)) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

int ObTransDesc::is_leader_stmt_participant(const ObPartitionKey& pkey, bool& is_leader) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;

  for (int64_t i = 0; i < stmt_participants_pla_.count(); i++) {
    const ObPartitionKey& cur_pkey = stmt_participants_pla_.get_partitions().at(i);
    const ObPartitionType& cur_type = stmt_participants_pla_.get_types().at(i);
    if (cur_pkey == pkey) {
      is_found = true;
      if (ObPartitionType::NORMAL_PARTITION == cur_type || ObPartitionType::DUPLICATE_LEADER_PARTITION == cur_type) {
        is_leader = true;
      } else {
        is_leader = false;
      }
      break;
    }
  }

  if (!is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObStandaloneStmtDesc::stmt_deep_copy(const ObStandaloneStmtDesc& desc)
{
  int ret = OB_SUCCESS;
  magic_ = desc.magic_;
  trans_id_ = desc.trans_id_;
  tenant_id_ = desc.tenant_id_;
  stmt_expired_time_ = desc.stmt_expired_time_;
  trx_lock_timeout_ = desc.trx_lock_timeout_;
  consistency_type_ = desc.consistency_type_;
  read_snapshot_type_ = desc.read_snapshot_type_;
  snapshot_version_ = desc.snapshot_version_;
  is_local_single_partition_ = desc.is_local_single_partition_;
  is_standalone_stmt_end_ = desc.is_standalone_stmt_end_;
  first_pkey_ = desc.first_pkey_;
  if (OB_FAIL(pla_.assign(desc.pla_))) {
    TRANS_LOG(WARN, "assign pla failed", K(ret), K(desc));
  }
  return ret;
}

int ObTransDesc::stmt_deep_copy(const ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;

  if (this != &trans_desc) {
    tenant_id_ = trans_desc.tenant_id_;
    trans_id_ = trans_desc.trans_id_;
    snapshot_version_ = trans_desc.snapshot_version_;
    trans_snapshot_version_ = trans_desc.trans_snapshot_version_;
    commit_version_ = trans_desc.commit_version_;
    trans_param_ = trans_desc.trans_param_;
    scheduler_ = trans_desc.scheduler_;
    sql_no_ = trans_desc.sql_no_;
    cur_stmt_desc_ = trans_desc.cur_stmt_desc_;
    need_rollback_ = trans_desc.need_rollback_;
    trans_expired_time_ = trans_desc.trans_expired_time_;
    cur_stmt_expired_time_ = trans_desc.cur_stmt_expired_time_;
    cluster_version_ = trans_desc.cluster_version_;
    cluster_id_ = trans_desc.cluster_id_;
    snapshot_gene_type_ = trans_desc.snapshot_gene_type_;
    session_id_ = trans_desc.session_id_;
    proxy_session_id_ = trans_desc.proxy_session_id_;
    app_trace_id_confirmed_ = trans_desc.app_trace_id_confirmed_;
    nested_stmt_info_ = trans_desc.nested_stmt_info_;
    is_dup_table_trans_ = trans_desc.is_dup_table_trans_;
    is_local_trans_ = trans_desc.is_local_trans_;
    stmt_snapshot_info_ = trans_desc.stmt_snapshot_info_;
    is_fast_select_ = trans_desc.is_fast_select_;
    // for nested sql
    sche_ctx_ = trans_desc.sche_ctx_;
    // no need to serialize and deserialize
    trans_type_ = trans_desc.trans_type_;
    is_all_select_stmt_ = trans_desc.is_all_select_stmt_;
    local_consistency_type_ = trans_desc.local_consistency_type_;
    is_sp_trans_exiting_ = trans_desc.is_sp_trans_exiting_;
    trans_end_ = trans_desc.trans_end_;
    can_elr_ = trans_desc.can_elr_;
    start_participant_ts_ = trans_desc.start_participant_ts_;
    trace_info_.reuse();
    need_check_at_end_participant_ = trans_desc.need_check_at_end_participant_;
    stmt_min_sql_no_ = trans_desc.stmt_min_sql_no_;
    xa_end_timeout_seconds_ = trans_desc.xa_end_timeout_seconds_;
    xid_ = trans_desc.xid_;
    is_nested_stmt_ = trans_desc.is_nested_stmt_;
    stmt_min_sql_no_ = trans_desc.stmt_min_sql_no_;
    is_tightly_coupled_ = trans_desc.is_tightly_coupled_;

    if (OB_FAIL(trace_info_.set_app_trace_id(trans_desc.get_trace_info().get_app_trace_id()))) {
      TRANS_LOG(WARN, "set app trace id error", K(ret), K(trans_desc));
    } else if (OB_FAIL(trace_info_.set_app_trace_info(trans_desc.get_trace_info().get_app_trace_info()))) {
      TRANS_LOG(WARN, "set app trace info error", K(ret), K(trans_desc));
    } else if (OB_FAIL(participants_.assign(trans_desc.participants_))) {
      TRANS_LOG(WARN, "participants assign error", K(trans_desc));
    } else if (OB_FAIL(stmt_participants_.assign(trans_desc.stmt_participants_))) {
      TRANS_LOG(WARN, "stmt participants assign error", K(trans_desc));
    } else if (OB_FAIL(stmt_participants_pla_.assign(trans_desc.stmt_participants_pla_))) {
      TRANS_LOG(WARN, "stmt participants pla assign error", KR(ret), K(trans_desc));
    } else if (OB_FAIL(participants_pla_.assign(trans_desc.participants_pla_))) {
      TRANS_LOG(WARN, "participants pla assign error", K(ret), K(trans_desc));
    } else if (OB_FAIL(stmt_rollback_info_.assign(trans_desc.stmt_rollback_info_))) {
      TRANS_LOG(WARN, "stmt rollback info assign error", K(ret), K(trans_desc));
    } else if (OB_FAIL(standalone_stmt_desc_.stmt_deep_copy(trans_desc.standalone_stmt_desc_))) {
      TRANS_LOG(WARN, "stmt standalone stmt desc error", K(ret), K(trans_desc));
    }
  }
  return ret;
}

int ObTransDesc::trans_deep_copy(const ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  if (this != &trans_desc) {
    if (OB_FAIL(ObSavepointMgr::deep_copy(trans_desc))) {
      TRANS_LOG(WARN, "deep copy savepoint mgr error", K(ret), K(trans_desc));
    } else if (OB_FAIL(stmt_deep_copy(trans_desc))) {
      TRANS_LOG(WARN, "deep copy trans desc error", K(ret), K(trans_desc));
    } else {
      max_sql_no_ = trans_desc.max_sql_no_;
      part_ctx_ = trans_desc.part_ctx_;
      //    memstore_version_ = trans_desc.memstore_version_;
    }
  }
  return ret;
}

int ObTransDesc::set_xid(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  if (!xid.is_valid()) {
    TRANS_LOG(WARN, "invalid xid", K(xid));
    ret = OB_INVALID_ARGUMENT;
  } else {
    xid_ = xid;
  }
  return ret;
}

bool ObTransDesc::contain_(const ObPartitionArray& participants, const ObPartitionKey& participant) const
{
  bool bool_ret = false;

  ObPartitionArray& pptr = const_cast<ObPartitionArray&>(participants);
  for (ObPartitionArray::iterator it = pptr.begin(); (it != pptr.end()) && (!bool_ret); it++) {
    if (*it == participant) {
      bool_ret = true;
    }
  }

  return bool_ret;
}

int ObTransDesc::set_start_participant_ts(const int64_t start_participant_ts)
{
  int ret = OB_SUCCESS;
  if (start_participant_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(start_participant_ts));
  } else {
    start_participant_ts_ = start_participant_ts;
  }
  return ret;
}

int64_t ObTransDesc::inc_savepoint()
{
  if (!cluster_version_before_2271()) {
    inc_sub_sql_no();
  }
  return sql_no_;
}

const ObString ObTransIsolation::LEVEL_NAME[ObTransIsolation::MAX_LEVEL] = {
    "READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"};

int32_t ObTransIsolation::get_level(const ObString& level_name)
{
  int32_t level = UNKNOWN;
  for (int32_t i = 0; i < MAX_LEVEL; i++) {
    if (0 == LEVEL_NAME[i].case_compare(level_name)) {
      level = i;
    }
  }
  return level;
}

const ObString& ObTransIsolation::get_name(int32_t level)
{
  static const ObString EMPTY_NAME;
  const ObString* level_name = &EMPTY_NAME;
  if (ObTransIsolation::UNKNOWN < level && level < ObTransIsolation::MAX_LEVEL) {
    level_name = &LEVEL_NAME[level];
  }
  return *level_name;
}

bool ObXATransState::can_convert(const int32_t src_state, const int32_t dst_state)
{
  bool ret_bool = true;
  if (src_state != dst_state) {
    switch (dst_state) {
      case NON_EXISTING:
        break;
      case ACTIVE: {
        if (NON_EXISTING != src_state && IDLE != src_state) {
          ret_bool = false;
        }
        break;
      }
      case IDLE: {
        if (ACTIVE != src_state) {
          ret_bool = false;
        }
        break;
      }
      case PREPARED: {
        if (IDLE != src_state) {
          ret_bool = false;
        }
        break;
      }
      case COMMITTED: {
        if (IDLE != src_state && PREPARED != src_state) {
          ret_bool = false;
        }
        break;
      }
      case ROLLBACKED: {
        if (IDLE != src_state && PREPARED != src_state) {
          ret_bool = false;
        }
        break;
      }
      default: {
        ret_bool = false;
      }
    }
  }
  return ret_bool;
}

bool ObXAFlag::is_valid(const int64_t flag, const int64_t xa_req_type)
{
  bool ret_bool = true;

  switch (xa_req_type) {
    case ObXAReqType::XA_START: {
      const bool is_resumejoin = flag & (TMRESUME | TMJOIN);
      if (!is_resumejoin) {
        const int64_t mask = LOOSELY | TMREADONLY | TMSERIALIZABLE;
        if (mask != (flag | mask)) {
          ret_bool = false;
        } else {
          ret_bool = true;
        }
      } else {
        if ((flag & TMJOIN) && (flag & TMRESUME)) {
          ret_bool = false;
        } else {
          ret_bool = true;
        }
      }
      break;
    }
    case ObXAReqType::XA_END: {
      if (flag != TMSUSPEND && flag != TMSUCCESS) {
        ret_bool = false;
      } else {
        ret_bool = true;
      }
      break;
    }
    case ObXAReqType::XA_PREPARE: {
      ret_bool = true;
      TRANS_LOG(INFO, "no need to check flag for xa prepare", K(xa_req_type), K(flag));
      break;
    }
    case ObXAReqType::XA_COMMIT: {
      if (flag != TMNOFLAGS && flag != TMONEPHASE) {
        ret_bool = false;
      } else {
        ret_bool = true;
      }
      break;
    }
    case ObXAReqType::XA_ROLLBACK: {
      ret_bool = true;
      TRANS_LOG(INFO, "no need to check flag for xa rollback", K(xa_req_type), K(flag));
      break;
    }
    default: {
      ret_bool = false;
      TRANS_LOG(WARN, "invalid xa request type", K(xa_req_type), K(flag));
    }
  }
  TRANS_LOG(INFO, "check xa flag", K(ret_bool), K(xa_req_type), KPHEX(&flag, sizeof(int64_t)));

  return ret_bool;
}

bool ObXAFlag::is_valid_inner_flag(const int64_t flag)
{
  bool ret_bool = true;
  if ((flag & TMSUSPEND) && (flag & TMSUCCESS)) {
    ret_bool = false;
  } else if (!(flag & TMSUSPEND) && !(flag & TMSUCCESS)) {
    ret_bool = false;
  } else {
    ret_bool = true;
  }
  TRANS_LOG(INFO, "check xa inner flag", K(ret_bool), KPHEX(&flag, sizeof(int64_t)));
  return ret_bool;
}

bool ObXAFlag::is_tmnoflags(const int64_t flag, const int64_t xa_req_type)
{
  bool ret_bool = true;
  if (ObXAReqType::XA_START == xa_req_type) {
    const int64_t mask = LOOSELY | TMREADONLY | TMSERIALIZABLE;
    ret_bool = ((mask | flag) == mask);
  } else {
    ret_bool = (TMNOFLAGS == flag);
  }
  TRANS_LOG(INFO, "check tmnoflags", K(ret_bool), K(xa_req_type), KPHEX(&flag, sizeof(int64_t)));
  return ret_bool;
}

int ObPartitionLeaderInfo::set_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_ = partition;
  }

  return ret;
}

int ObPartitionLeaderInfo::set_addr(const ObAddr& addr)
{
  int ret = OB_SUCCESS;

  if (!addr.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(addr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    addr_ = addr;
  }

  return ret;
}

void ObPartitionEpochInfo::reset()
{
  partition_.reset();
  epoch_ = 0;
}

int ObPartitionEpochInfo::generate(const ObPartitionKey& partition, const int64_t epoch)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || epoch < 0) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(epoch));
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_ = partition;
    epoch_ = epoch;
  }

  return ret;
}

void ObPartitionLeaderEpochInfo::reset()
{
  is_inited_ = false;
  partition_.reset();
  epoch_arr_.reset();
}

int ObPartitionLeaderEpochInfo::init(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObPartitionLeaderEpochInfo init twice", K(partition));
    ret = OB_INIT_TWICE;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_ = partition;
  }
  return ret;
}

int ObPartitionLeaderEpochInfo::push(const int64_t leader_epoch)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObPartitionLeaderEpochInfo not init", K(leader_epoch));
    ret = OB_NOT_INIT;
  } else if (leader_epoch <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(leader_epoch));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(epoch_arr_.push_back(leader_epoch))) {
    TRANS_LOG(WARN, "leader epoch push error", K_(partition), K(leader_epoch));
  } else {
    // do nothing
  }

  return ret;
}

bool ObPartitionLeaderEpochInfo::is_all_identical_epoch() const
{
  bool is_identical_epoch = true;

  if (epoch_arr_.count() > 0) {
    for (int64_t i = 1; i < epoch_arr_.count(); ++i) {
      if (epoch_arr_.at(0) != epoch_arr_.at(i)) {
        is_identical_epoch = false;
        break;
      }
    }
  }

  return is_identical_epoch;
}

int ObPartitionSchemaInfo::init(const ObPartitionKey& pkey, const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (!pkey.is_valid() || schema_version < 0) {
    TRANS_LOG(WARN, "invalid argument", K(pkey), K(schema_version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    pkey_ = pkey;
    schema_version_ = schema_version;
  }

  return ret;
}

void ObPartitionSchemaInfo::reset()
{
  pkey_.reset();
  schema_version_ = 0;
}

int ObMemtableKeyInfo::init(const uint64_t table_id, const uint64_t hash_val)
{
  int ret = OB_SUCCESS;

  if (table_id == 0 || hash_val == 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable key info init fail", KR(ret), K(table_id), K(hash_val));
  } else {
    table_id_ = table_id;
    hash_val_ = hash_val;
  }

  return ret;
}

void ObMemtableKeyInfo::reset()
{
  table_id_ = 0;
  hash_val_ = 0;
  row_lock_ = NULL;
  buf_[0] = '\0';
}

bool ObMemtableKeyInfo::operator==(const ObMemtableKeyInfo& other) const
{
  return (table_id_ == other.get_table_id()) && (hash_val_ == other.get_hash_val());
}

int ObTransTaskInfo::start_stmt(const int32_t sql_no)
{
  int ret = OB_SUCCESS;

  if (!is_task_match(0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "previous stmt's task not match", K(ret), K(sql_no), K(*this));
  } else {
    tasks_.reuse();
  }

  return ret;
}

int ObTransTaskInfo::start_task(const int32_t sql_no, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;

  if (tasks_.empty()) {
    if (OB_FAIL(tasks_.push_back(ObTaskInfo(sql_no, snapshot_version)))) {
      TRANS_LOG(WARN, "fail to push back task", K(ret), K(sql_no));
    }
  } else {
    ObTaskInfo& top_task = tasks_[tasks_.count() - 1];
    if (sql_no == top_task.sql_no_) {
      top_task.active_task_cnt_++;
    } else if (sql_no > top_task.sql_no_) {
      if (top_task.is_task_match()) {
        tasks_.pop_back();
      }
      if (OB_FAIL(tasks_.push_back(ObTaskInfo(sql_no, snapshot_version)))) {
        TRANS_LOG(WARN, "fail to push back task", K(ret), K(sql_no));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "sql no illegal", K(ret), K(sql_no), K(*this));
    }
  }

  return ret;
}

void ObTransTaskInfo::end_task()
{
  if (!tasks_.empty()) {
    ObTaskInfo& top_task = tasks_[tasks_.count() - 1];
    if (!top_task.is_task_match()) {
      top_task.active_task_cnt_--;
    } else {
      tasks_.pop_back();
      end_task();
    }
  } else {
    TRANS_LOG(WARN, "unexpected end task", K(*this));
  }
}

bool ObTransTaskInfo::is_task_match(const int32_t sql_no) const
{
  bool bool_ret = true;

  for (int64_t i = 0; i < tasks_.count(); ++i) {
    if (tasks_[i].sql_no_ >= sql_no && !tasks_[i].is_task_match()) {
      bool_ret = false;
      break;
    }
  }

  return bool_ret;
}

void ObTransTaskInfo::remove_task_after(const int32_t sql_no)
{
  while (!tasks_.empty()) {
    ObTaskInfo& top_task = tasks_[tasks_.count() - 1];
    if (top_task.sql_no_ >= sql_no) {
      tasks_.pop_back();
    } else {
      break;
    }
  }
}

int64_t ObTransTaskInfo::get_top_snapshot_version() const
{
  int64_t snapshot_version = OB_INVALID_VERSION;

  for (int64_t i = tasks_.count() - 1; i >= 0; --i) {
    const ObTaskInfo& task = tasks_[i];
    if (!task.is_task_match()) {
      snapshot_version = task.snapshot_version_;
      break;
    }
  }

  return snapshot_version;
}

void ObTransStmtInfo::reset()
{
  sql_no_ = 0;
  start_task_cnt_ = 0;
  end_task_cnt_ = 0;
  need_rollback_ = false;
  task_info_.reset();
}

void ObElrTransInfo::reset()
{
  trans_id_.reset();
  commit_version_ = -1;
  result_ = ObTransResultState::UNKNOWN;
  ctx_id_ = 0;
}

int ObElrTransInfo::init(const ObTransID& trans_id, uint32_t ctx_id, const int64_t commit_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!trans_id.is_valid()) || ctx_id <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(trans_id), K(ctx_id), K(commit_version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_id_ = trans_id;
    commit_version_ = commit_version;
    ctx_id_ = ctx_id;
  }

  return ret;
}

void ObTransTask::reset()
{
  retry_interval_us_ = 0;
  next_handle_ts_ = 0;
  task_type_ = ObTransRetryTaskType::UNKNOWN;
}

int ObTransTask::make(const int64_t task_type)
{
  int ret = OB_SUCCESS;

  if (!ObTransRetryTaskType::is_valid(task_type)) {
    TRANS_LOG(WARN, "invalid argument", K(task_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    task_type_ = task_type;
  }

  return ret;
}

int ObTransTask::set_retry_interval_us(const int64_t start_interval_us, const int64_t retry_interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(retry_interval_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(retry_interval_us));
  } else {
    retry_interval_us_ = retry_interval_us;
    next_handle_ts_ = ObTimeUtility::current_time() + start_interval_us;
  }

  return ret;
}

bool ObTransTask::ready_to_handle()
{
  bool boot_ret = false;
  ;
  int64_t current_ts = ObTimeUtility::current_time();

  if (current_ts >= next_handle_ts_) {
    boot_ret = true;
    next_handle_ts_ = current_ts + retry_interval_us_;
  } else {
    int64_t left_time = next_handle_ts_ - current_ts;
    if (left_time > RETRY_SLEEP_TIME_US) {
      usleep(RETRY_SLEEP_TIME_US);
      boot_ret = false;
    } else {
      usleep(left_time);
      boot_ret = true;
      next_handle_ts_ += retry_interval_us_;
    }
  }

  return boot_ret;
}

ObPartitionAuditInfo& ObPartitionAuditInfo::operator=(const ObPartitionAuditInfo& other)
{
  if (this != &other) {
    this->base_row_count_ = other.base_row_count_;
    this->insert_row_count_ = other.insert_row_count_;
    this->delete_row_count_ = other.delete_row_count_;
    this->update_row_count_ = other.update_row_count_;
    /*
    this->query_row_count_ = other.query_row_count_;
    this->insert_sql_count_ = other.insert_sql_count_;
    this->delete_sql_count_ = other.delete_sql_count_;
    this->update_sql_count_ = other.update_sql_count_;
    this->query_sql_count_ = other.query_sql_count_;
    this->trans_count_ = other.trans_count_;
    this->sql_count_ = other.sql_count_;
    this->rollback_insert_row_count_ = other.rollback_insert_row_count_;
    this->rollback_delete_row_count_ = other.rollback_delete_row_count_;
    this->rollback_update_row_count_ = other.rollback_update_row_count_;
    this->rollback_insert_sql_count_ = other.rollback_insert_sql_count_;
    this->rollback_delete_sql_count_ = other.rollback_delete_sql_count_;
    this->rollback_update_sql_count_ = other.rollback_update_sql_count_;
    this->rollback_trans_count_ = other.rollback_trans_count_;
    this->rollback_sql_count_ = other.rollback_sql_count_;
    */
  }
  return *this;
}

ObPartitionAuditInfo& ObPartitionAuditInfo::operator+=(const ObPartitionAuditInfo& other)
{
  this->base_row_count_ += other.base_row_count_;
  this->insert_row_count_ += other.insert_row_count_;
  this->delete_row_count_ += other.delete_row_count_;
  this->update_row_count_ += other.update_row_count_;
  /*
  this->query_row_count_ += other.query_row_count_;
  this->insert_sql_count_ += other.insert_sql_count_;
  this->delete_sql_count_ += other.delete_sql_count_;
  this->update_sql_count_ += other.update_sql_count_;
  this->query_sql_count_ += other.query_sql_count_;
  this->trans_count_ += other.trans_count_;
  this->sql_count_ += other.sql_count_;
  this->rollback_insert_row_count_ += other.rollback_insert_row_count_;
  this->rollback_delete_row_count_ += other.rollback_delete_row_count_;
  this->rollback_update_row_count_ += other.rollback_update_row_count_;
  this->rollback_insert_sql_count_ += other.rollback_insert_sql_count_;
  this->rollback_delete_sql_count_ += other.rollback_delete_sql_count_;
  this->rollback_update_sql_count_ += other.rollback_update_sql_count_;
  this->rollback_trans_count_ += other.rollback_trans_count_;
  this->rollback_sql_count_ += other.rollback_sql_count_;
  */
  return *this;
}

int ObPartitionAuditInfo::update_audit_info(const ObPartitionAuditInfoCache& cache, const bool commit)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  /*
  query_row_count_ += cache.query_row_count_;
  insert_sql_count_ += cache.insert_sql_count_;
  delete_sql_count_ += cache.delete_sql_count_;
  update_sql_count_ += cache.update_sql_count_;
  query_sql_count_ += cache.query_sql_count_;
  sql_count_ += cache.sql_count_;
  */
  if (commit) {
    insert_row_count_ += cache.insert_row_count_;
    delete_row_count_ += cache.delete_row_count_;
    update_row_count_ += cache.update_row_count_;
    /*
    rollback_insert_row_count_ += cache.rollback_insert_row_count_;
    rollback_delete_row_count_ += cache.rollback_delete_row_count_;
    rollback_update_row_count_ += cache.rollback_update_row_count_;
    trans_count_ += 1;
    */
  } else {
    /*
    rollback_insert_row_count_ += (cache.insert_row_count_ + cache.rollback_insert_row_count_);
    rollback_delete_row_count_ += (cache.delete_row_count_ + cache.rollback_delete_row_count_);
    rollback_update_row_count_ += (cache.update_row_count_ + cache.rollback_update_row_count_);
    rollback_trans_count_ += 1;
    */
  }
  /*
  rollback_insert_sql_count_ += cache.rollback_insert_sql_count_;
  rollback_delete_sql_count_ += cache.rollback_delete_sql_count_;
  rollback_update_sql_count_ += cache.rollback_update_sql_count_;
  rollback_sql_count_ += cache.rollback_sql_count_;
  */
  return ret;
}

int ObPartitionAuditInfoCache::update_audit_info(const enum ObPartitionAuditOperator op, const int32_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op >= PART_AUDIT_OP_MAX || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(op), K(count));
  } else {
    switch (op) {
      case PART_AUDIT_SET_BASE_ROW_COUNT: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_INSERT_ROW: {
        cur_insert_row_count_ += count;
        break;
      }
      case PART_AUDIT_DELETE_ROW: {
        cur_delete_row_count_ += count;
        break;
      }
      case PART_AUDIT_UPDATE_ROW: {
        cur_update_row_count_ += count;
        break;
      }
      /*
      case PART_AUDIT_QUERY_ROW: {
        query_row_count_ += count;
        break;
      }
      case PART_AUDIT_INSERT_SQL: {
        insert_sql_count_ += count;
        break;
      }
      case PART_AUDIT_DELETE_SQL: {
        delete_sql_count_ += count;
        break;
      }
      case PART_AUDIT_UPDATE_SQL: {
        update_sql_count_ += count;
        break;
      }
      case PART_AUDIT_QUERY_SQL: {
        query_sql_count_ += count;
        break;
      }
      case PART_AUDIT_TRANS: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_SQL: {
        sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_INSERT_ROW: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_DELETE_ROW: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_UPDATE_ROW: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_INSERT_SQL: {
        rollback_insert_sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_DELETE_SQL: {
        rollback_delete_sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_UPDATE_SQL: {
        rollback_update_sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_TRANS: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_SQL: {
        rollback_sql_count_ += count;
        break;
      }
      */
      default: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
    }
  }
  return ret;
}

int ObPartitionAuditInfoCache::stmt_end_update_audit_info(bool commit)
{
  int ret = OB_SUCCESS;

  if (commit) {
    insert_row_count_ += cur_insert_row_count_;
    delete_row_count_ += cur_delete_row_count_;
    update_row_count_ += cur_update_row_count_;
  } else {
    /*
    rollback_insert_row_count_ += cur_insert_row_count_;
    rollback_delete_row_count_ += cur_delete_row_count_;
    rollback_update_row_count_ += cur_update_row_count_;
    */
  }
  cur_insert_row_count_ = 0;
  cur_delete_row_count_ = 0;
  cur_update_row_count_ = 0;

  return ret;
}

void ObCoreLocalPartitionAuditInfo::reset()
{
  if (NULL != val_array_) {
    for (int i = 0; i < array_len_; i++) {
      ObPartitionAuditInfoFactory::release(VAL_ARRAY_AT(ObPartitionAuditInfo*, i));
    }
    ob_free(val_array_);
    val_array_ = NULL;
  }
  core_num_ = 0;
  array_len_ = 0;
  is_inited_ = false;
}

int ObCoreLocalPartitionAuditInfo::init(int64_t array_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObCoreLocalStorage<ObPartitionAuditInfo*>::init(array_len))) {
    TRANS_LOG(WARN, "ObCoreLocalStorage init error", KR(ret), K(array_len));
  } else {
    int alloc_succ_pos = 0;
    ObPartitionAuditInfo* info = NULL;
    for (int i = 0; OB_SUCC(ret) && i < array_len; i++) {
      if (OB_ISNULL(info = ObPartitionAuditInfoFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc ObPartitionAuditInfo error", KR(ret));
      } else {
        VAL_ARRAY_AT(ObPartitionAuditInfo*, i) = info;
        alloc_succ_pos = i + 1;
      }
    }
    if (OB_FAIL(ret)) {
      for (int i = 0; i < alloc_succ_pos; i++) {
        ObPartitionAuditInfoFactory::release(VAL_ARRAY_AT(ObPartitionAuditInfo*, i));
      }
    }
  }

  return ret;
}

void ObAddrLogId::reset()
{
  addr_.reset();
  log_id_ = 0;
}

bool ObAddrLogId::operator==(const ObAddrLogId& other) const
{
  return (addr_ == other.addr_) && (log_id_ == other.log_id_);
}

int64_t ObTransNeedWaitWrap::get_remaining_wait_interval_us() const
{
  int64_t ret_val = 0;

  if (receive_gts_ts_ <= MonotonicTs(0)) {
    ret_val = 0;
  } else if (need_wait_interval_us_ <= 0) {
    ret_val = 0;
  } else {
    MonotonicTs tmp_ts = MonotonicTs(need_wait_interval_us_) - (MonotonicTs::current_time() - receive_gts_ts_);
    ret_val = tmp_ts.mts_;
    ret_val = ret_val > 0 ? ret_val : 0;
  }

  return ret_val;
}

void ObTransNeedWaitWrap::set_trans_need_wait_wrap(
    const MonotonicTs receive_gts_ts, const int64_t need_wait_interval_us)
{
  if (need_wait_interval_us > 0) {
    receive_gts_ts_ = receive_gts_ts;
    need_wait_interval_us_ = need_wait_interval_us;
  }
}

OB_SERIALIZE_MEMBER(ObUndoAction, undo_from_, undo_to_);

int ObUndoAction::merge(const ObUndoAction& other)
{
  int ret = OB_SUCCESS;

  if (!is_conjoint(other)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(*this), K(other));
  } else {
    undo_from_ = MAX(undo_from_, other.undo_from_);
    undo_to_ = MIN(undo_to_, other.undo_to_);
  }

  return ret;
}

int ObTransUndoStatus::set(const ObTransUndoStatus& undo_status)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(latch_);

  if (OB_FAIL(undo_action_arr_.assign(undo_status.undo_action_arr_))) {
    TRANS_LOG(WARN, "assign undo action arr error", K(ret), K(*this));
  }

  return ret;
}

int ObTransUndoStatus::undo(const int64_t undo_to, const int64_t undo_from)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(latch_);

  int64_t cur_count = undo_action_arr_.count();
  ObUndoAction new_undo_action(undo_from, undo_to);

  if (cur_count > 0 && undo_from < undo_action_arr_.at(cur_count - 1).get_undo_from()) {
    // do nothing
  } else {
    int64_t i = cur_count - 1;
    for (; i >= 0; i--) {
      if (new_undo_action.is_contain(undo_action_arr_.at(i)) || new_undo_action.is_conjoint(undo_action_arr_.at(i)) ||
          undo_action_arr_.at(i).is_contain(new_undo_action)) {
        new_undo_action.merge(undo_action_arr_.at(i));
        undo_action_arr_.remove(i);
      } else {
        break;
      }
    }
    if (OB_FAIL(undo_action_arr_.push_back(new_undo_action))) {
      TRANS_LOG(WARN, "undo action arr push item error", K(ret), K(new_undo_action), K(*this));
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObTransUndoStatus, undo_action_arr_);

bool ObTransUndoStatus::is_contain(const int64_t sql_no) const
{
  bool ret = false;
  ObLockGuard<ObSpinLock> guard(latch_);

  int64_t count = undo_action_arr_.count();
  if (count > 0) {
    int64_t left = 0;
    int64_t right = count - 1;
    do {
      int64_t mid = (right + left) / 2;
      if (undo_action_arr_.at(mid).is_contain(sql_no)) {
        ret = true;
        break;
      } else if (undo_action_arr_.at(mid).is_less_than(sql_no)) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    } while (left <= right);
  }

  return ret;
}

int ObTransUndoStatus::deep_copy(ObTransUndoStatus& status)
{
  int ret = false;
  ObLockGuard<ObSpinLock> guard(latch_);

  if (OB_FAIL(status.undo_action_arr_.assign(undo_action_arr_))) {
    TRANS_LOG(WARN, "deep copy undo action arr error", K(ret), K(*this), K(status));
  }

  return ret;
}

OB_SERIALIZE_MEMBER(
    ObTransTableStatusInfo, status_, trans_version_, undo_status_, terminate_log_ts_, checksum_, checksum_log_ts_);

void ObTransTableStatusInfo::reset()
{
  status_ = ObTransTableStatusType::RUNNING;
  trans_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  undo_status_.reset();
  terminate_log_ts_ = INT64_MAX;
  checksum_ = 0;
  checksum_log_ts_ = 0;
}

int ObTransTableStatusInfo::set(const ObTransTableStatusType& status, const int64_t trans_version,
    const ObTransUndoStatus& undo_status, const int64_t terminate_log_ts, const uint64_t& checksum,
    const int64_t checksum_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(undo_status_.set(undo_status))) {
    TRANS_LOG(WARN, "ObTransUndoStatus set error", K(ret), K(*this));
  } else {
    status_ = status;
    trans_version_ = trans_version;
    terminate_log_ts_ = terminate_log_ts;
    checksum_ = checksum;
    checksum_log_ts_ = checksum_log_ts;
  }

  return ret;
}

int ObTransTableStatusInfo::set(const ObTransTableStatusType& status, const int64_t trans_version,
    const ObTransUndoStatus& undo_status, const int64_t terminate_log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(undo_status_.set(undo_status))) {
    TRANS_LOG(WARN, "ObTransUndoStatus set error", K(ret), K(*this));
  } else {
    status_ = status;
    trans_version_ = trans_version;
    terminate_log_ts_ = terminate_log_ts;
  }

  return ret;
}

bool is_single_leader(const ObTransLocationCache& trans_location_cache)
{
  bool bool_ret = true;
  for (int64_t i = 1; i < trans_location_cache.count(); ++i) {
    if ((trans_location_cache.at(0).get_addr() != trans_location_cache.at(i).get_addr()) ||
        (trans_location_cache.at(0).get_partition().get_tenant_id() !=
            trans_location_cache.at(i).get_partition().get_tenant_id())) {
      bool_ret = false;
      break;
    }
  }
  return bool_ret;
}

int ObStandaloneStmtDesc::init(const common::ObAddr& addr, const uint64_t tenant_id, const int64_t stmt_expired_time,
    const int64_t trx_lock_timeout, const bool is_local_single_partition, const int32_t consistency_type,
    const int32_t read_snapshot_type, const common::ObPartitionKey& first_pkey)
{
  int ret = OB_SUCCESS;

  // no need to check first_pkey, maybe invalid
  if (OB_UNLIKELY(!addr.is_valid() || tenant_id <= 0 || stmt_expired_time <= 0 ||
                  consistency_type == ObTransConsistencyType::UNKNOWN ||
                  read_snapshot_type == ObTransReadSnapshotType::UNKNOWN)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN,
        "invalid argument",
        K(ret),
        K(addr),
        K(tenant_id),
        K(stmt_expired_time),
        K(consistency_type),
        K(read_snapshot_type),
        K(first_pkey));
  } else {
    ObTransID trans_id(addr);
    trans_id_ = trans_id;
    tenant_id_ = tenant_id;
    stmt_expired_time_ = stmt_expired_time;
    trx_lock_timeout_ = trx_lock_timeout;
    consistency_type_ = consistency_type;
    read_snapshot_type_ = read_snapshot_type;
    is_local_single_partition_ = is_local_single_partition;
    first_pkey_ = first_pkey;
    is_standalone_stmt_end_ = false;
  }

  return ret;
}

int ObSameLeaderPartitionArr::init(const common::ObAddr& leader, const common::ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!leader.is_valid() || !partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partition_arr_.push_back(partition))) {
    TRANS_LOG(WARN, "partition arr assign error", K(ret), K(leader), K(partition));
  } else {
    leader_ = leader;
  }

  return ret;
}

bool ObStandaloneStmtDesc::is_valid() const
{
  return trans_id_.is_valid() && tenant_id_ > 0 && stmt_expired_time_ > 0 &&
         consistency_type_ != ObTransConsistencyType::UNKNOWN &&
         read_snapshot_type_ != ObTransReadSnapshotType::UNKNOWN && false == is_standalone_stmt_end_;
}

void ObStandaloneStmtDesc::reset()
{
  magic_ = UINT64_MAX;
  trans_id_.reset();
  tenant_id_ = 0;
  stmt_expired_time_ = -1;
  trx_lock_timeout_ = -1;
  pla_.reset();
  consistency_type_ = ObTransConsistencyType::UNKNOWN;
  read_snapshot_type_ = ObTransReadSnapshotType::UNKNOWN;
  snapshot_version_ = OB_INVALID_TIMESTAMP;
  is_local_single_partition_ = false;
  first_pkey_.reset();
  is_standalone_stmt_end_ = true;
}

void ObSameLeaderPartitionArrMgr::reset()
{
  is_ready_ = false;
  for (int i = 0; i < array_.count(); i++) {
    array_.at(i).reset();
  }
  array_.reset();
}

int ObSameLeaderPartitionArrMgr::push(const common::ObAddr& leader, const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;

  for (; OB_SUCC(ret) && index < array_.count(); index++) {
    if (array_.at(index).get_leader() == leader) {
      if (OB_FAIL(array_.at(index).get_partition_arr().push_back(partition))) {
        TRANS_LOG(WARN, "psuh back error", K(ret), K(leader), K(partition));
      }
      break;
    }
  }

  if (array_.count() == index) {
    ObSameLeaderPartitionArr tmp;
    if (OB_FAIL(tmp.init(leader, partition))) {
      TRANS_LOG(WARN, "ObSameLeaderPartitionArr init error", K(ret), K(leader), K(partition));
    } else if (OB_FAIL(array_.push_back(tmp))) {
      TRANS_LOG(WARN, "push back error", K(ret), K(leader), K(partition));
    } else {
      // do nothing
    }
  }

  return ret;
}

bool ObPartTransSameLeaderBatchRpcItem::allow_to_preempt_(const ObTransID& trans_id)
{
  return ObTimeUtility::current_time() - last_add_ts_ >= ALLOW_PREEMPT_INTERVAL_US || !trans_id_.is_valid() ||
         (trans_id_ == trans_id);
}

int ObPartTransSameLeaderBatchRpcItem::try_preempt(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::TRANS_BATCH_RPC_LOCK);
  if (allow_to_preempt_(trans_id)) {
    reset();
    trans_id_ = trans_id;
    last_add_ts_ = ObTimeUtility::current_time();
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObPartTransSameLeaderBatchRpcItem::add_batch_rpc(const ObTransID& trans_id, const int64_t msg_type,
    const ObPartitionKey& partition, const int64_t base_ts, const int64_t batch_count, bool& need_response,
    ObPartitionArray& batch_partitions, int64_t& same_leader_batch_base_ts)
{
  int ret = OB_SUCCESS;
  need_response = false;
  ObLatchWGuard guard(lock_, ObLatchIds::TRANS_BATCH_RPC_LOCK);

  if (trans_id_ != trans_id || msg_type_ != msg_type) {
    if (allow_to_preempt_(trans_id)) {
      msg_type_ = msg_type;
      trans_id_ = trans_id;
      participants_.reset();
      if (OB_FAIL(participants_.push_back(partition))) {
        TRANS_LOG(WARN, "push back error", K(ret), K(trans_id));
      } else {
        if (base_ts > base_timestamp_) {
          base_timestamp_ = base_ts;
        }
        if (participants_.count() == batch_count) {
          need_response = true;
        } else {
          last_add_ts_ = ObTimeUtility::current_time();
        }
      }
    } else {
      ret = OB_EAGAIN;
    }
  } else {
    if (is_contain(participants_, partition)) {
      need_response = true;
    } else if (OB_FAIL(participants_.push_back(partition))) {
      TRANS_LOG(WARN, "push back error", K(ret), K(*this));
    } else {
      if (base_ts > base_timestamp_) {
        base_timestamp_ = base_ts;
      }
      if (participants_.count() >= batch_count) {
        need_response = true;
      } else {
        last_add_ts_ = ObTimeUtility::current_time();
      }
    }
  }

  if (OB_SUCC(ret) && need_response) {
    if (OB_FAIL(batch_partitions.assign(participants_))) {
      TRANS_LOG(WARN, "assign batch partitions error", K(ret), K(*this));
    } else {
      same_leader_batch_base_ts = base_timestamp_;
    }
    reset();
  }

  return ret;
}

void ObPartTransSameLeaderBatchRpcMgr::reset()
{
  for (int64_t i = 0; i < SAME_LEADER_BATCH_RPC_ITEM_ARR_SIZE; i++) {
    batch_rpc_item_arr_[i].reset();
  }
}

int ObPartTransSameLeaderBatchRpcMgr::add_batch_rpc(const ObTransID& trans_id, const int64_t msg_type,
    const ObPartitionKey& partition, const int64_t base_ts, const int64_t batch_count, bool& need_response,
    ObPartitionArray& batch_partitions, int64_t& same_leader_batch_base_ts)
{
  int64_t index = trans_id.hash() & (SAME_LEADER_BATCH_RPC_ITEM_ARR_SIZE - 1);
  return batch_rpc_item_arr_[index].add_batch_rpc(
      trans_id, msg_type, partition, base_ts, batch_count, need_response, batch_partitions, same_leader_batch_base_ts);
}

int ObPartTransSameLeaderBatchRpcMgr::try_preempt(const ObTransID& trans_id)
{
  const int64_t index = trans_id.hash() & (SAME_LEADER_BATCH_RPC_ITEM_ARR_SIZE - 1);
  return batch_rpc_item_arr_[index].try_preempt(trans_id);
}

int ObTransLogBufferAggreContainer::init(const common::ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(partition.get_tenant_id(), ObModIds::OB_LOG_AGGRE_BUFFER_ARRAY);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(partition.get_tenant_id()));
  int64_t aggre_buffer_cnt = tenant_config->_clog_aggregation_buffer_amount;
  void* ptr = NULL;

  if (aggre_buffer_cnt <= 0) {
  } else if (OB_ISNULL(ptr = ob_malloc(sizeof(ObAggreBuffer), mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(aggre_buffer_ = new (ptr) ObAggreBuffer())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(ptr = ob_malloc(sizeof(AggreLogTask), mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(task_ = new (ptr) AggreLogTask())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(aggre_buffer_->init(1, partition.get_tenant_id()))) {
    TRANS_LOG(WARN, "aggre buffer init error", K(ret), K(partition));
  } else if (OB_FAIL(task_->make(partition, this))) {
    TRANS_LOG(WARN, "aggre log task make error", K(ret), K(partition));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObTransLogBufferAggreContainer::destroy()
{
  if (NULL != aggre_buffer_) {
    aggre_buffer_->~ObAggreBuffer();
    ob_free(aggre_buffer_);
    aggre_buffer_ = NULL;
  }
  if (NULL != task_) {
    task_->~AggreLogTask();
    ob_free(task_);
    task_ = NULL;
  }
  offset_ = 0;
  last_flush_ts_ = 0;
  is_inited_ = false;
}

void ObTransLogBufferAggreContainer::reuse()
{
  if (is_inited_) {
    aggre_buffer_->reuse(1);
    ATOMIC_STORE(&offset_, 0);
  }
}

int ObTransLogBufferAggreContainer::leader_revoke()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    if (task_->get_in_queue()) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "can't leader revoke until AggreBuffer flush", K(ret));
    } else {
      reuse();
    }
  }
  return ret;
}

void ObTransLogBufferAggreContainer::flush_(bool& need_submit_log)
{
  int32_t cur_offset = ATOMIC_LOAD(&offset_);
  need_submit_log = false;
  if (cur_offset > 0) {
    if (ATOMIC_BCAS(&offset_, cur_offset, -1)) {
      if (0 == aggre_buffer_->ref(-1 * cur_offset)) {
        need_submit_log = true;
      }
    }
  }
}

int ObTransLogBufferAggreContainer::fill(
    const char* buf, const int64_t size, ObITransSubmitLogCb* cb, const int64_t base_ts, bool& need_submit_log)
{
  int ret = OB_SUCCESS;
  need_submit_log = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (base_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(base_ts));
  } else {
    (void)inc_update(&base_timestamp_, base_ts);
    while (OB_SUCCESS == ret) {
      int32_t cur_offset = ATOMIC_LOAD(&offset_);
      if (cur_offset >= 0 && cur_offset + size + AGGRE_LOG_RESERVED_SIZE < AGGRE_BUFFER_LIMIT) {
        if (ATOMIC_BCAS(&offset_,
                cur_offset,
                static_cast<int32_t>((cur_offset + size + AGGRE_LOG_RESERVED_SIZE) & 0xffffffff))) {
          aggre_buffer_->fill(cur_offset, buf, size, TRANS_AGGRE_LOG_TIMESTAMP, cb);
          if (0 == aggre_buffer_->ref(size + AGGRE_LOG_RESERVED_SIZE)) {
            need_submit_log = true;
          } else if (ATOMIC_LOAD(&offset_) >= AGGRE_BUFFER_LIMIT / 2) {
            // flush log
            flush_(need_submit_log);
          } else {
            // do nothing
          }
          break;
        }
      } else {
        ret = OB_EAGAIN;
      }
    }  // while
  }

  last_flush_ts_ = ObTimeUtility::current_time();
  return ret;
}

int ObTransLogBufferAggreContainer::flush(bool& need_submit_log)
{
  int ret = OB_SUCCESS;
  int64_t cur_ts = ObTimeUtility::current_time();
  need_submit_log = false;

  if (is_inited_ && cur_ts - last_flush_ts_ > FLUSH_INTERVAL_US) {
    flush_(need_submit_log);
    last_flush_ts_ = ObTimeUtility::current_time();
  }

  return ret;
}

OB_DEF_SERIALIZE(ObTransDesc)
{
  int ret = OB_SUCCESS;

  if (is_standalone_stmt_desc()) {
    LST_DO_CODE(OB_UNIS_ENCODE,
        standalone_stmt_desc_.magic_,
        standalone_stmt_desc_.trans_id_,
        standalone_stmt_desc_.tenant_id_,
        standalone_stmt_desc_.stmt_expired_time_,
        standalone_stmt_desc_.trx_lock_timeout_,
        standalone_stmt_desc_.pla_,
        standalone_stmt_desc_.consistency_type_,
        standalone_stmt_desc_.read_snapshot_type_,
        standalone_stmt_desc_.snapshot_version_,
        standalone_stmt_desc_.is_standalone_stmt_end_);
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        tenant_id_,
        trans_id_,
        snapshot_version_,
        trans_param_,
        scheduler_,
        participants_,
        stmt_participants_,
        sql_no_,
        cur_stmt_desc_,
        need_rollback_,
        trans_expired_time_,
        cur_stmt_expired_time_,
        memstore_version_,
        cluster_version_,
        cluster_id_,
        participants_pla_,
        stmt_participants_pla_,
        snapshot_gene_type_,
        session_id_,
        proxy_session_id_,
        trace_info_.get_app_trace_id(),
        app_trace_id_confirmed_,
        nested_stmt_info_,
        can_elr_,
        is_dup_table_trans_,
        trans_snapshot_version_,
        is_local_trans_,
        stmt_snapshot_info_,
        is_fast_select_,
        stmt_rollback_info_,
        commit_version_,
        xid_,
        xa_end_timeout_seconds_,
        is_nested_stmt_,
        max_sql_no_,
        stmt_min_sql_no_,
        is_tightly_coupled_);
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObTransDesc)
{
  int ret = OB_SUCCESS;
  int64_t prev_pos = pos;
  uint64_t magic = 0;
  OB_UNIS_DECODE(magic);
  pos = prev_pos;

  if (UINT64_MAX == magic) {
    LST_DO_CODE(OB_UNIS_DECODE,
        standalone_stmt_desc_.magic_,
        standalone_stmt_desc_.trans_id_,
        standalone_stmt_desc_.tenant_id_,
        standalone_stmt_desc_.stmt_expired_time_,
        standalone_stmt_desc_.trx_lock_timeout_,
        standalone_stmt_desc_.pla_,
        standalone_stmt_desc_.consistency_type_,
        standalone_stmt_desc_.read_snapshot_type_,
        standalone_stmt_desc_.snapshot_version_,
        standalone_stmt_desc_.is_standalone_stmt_end_);
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
        tenant_id_,
        trans_id_,
        snapshot_version_,
        trans_param_,
        scheduler_,
        participants_,
        stmt_participants_,
        sql_no_,
        cur_stmt_desc_,
        need_rollback_,
        trans_expired_time_,
        cur_stmt_expired_time_,
        memstore_version_,
        cluster_version_,
        cluster_id_,
        participants_pla_,
        stmt_participants_pla_,
        snapshot_gene_type_,
        session_id_,
        proxy_session_id_,
        trace_info_.get_app_trace_id(),
        app_trace_id_confirmed_,
        nested_stmt_info_,
        can_elr_,
        is_dup_table_trans_,
        trans_snapshot_version_,
        is_local_trans_,
        stmt_snapshot_info_,
        is_fast_select_,
        stmt_rollback_info_,
        commit_version_,
        xid_,
        xa_end_timeout_seconds_,
        is_nested_stmt_,
        max_sql_no_,
        stmt_min_sql_no_,
        is_tightly_coupled_);
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTransDesc)
{
  int64_t len = 0;

  if (is_standalone_stmt_desc()) {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
        standalone_stmt_desc_.magic_,
        standalone_stmt_desc_.trans_id_,
        standalone_stmt_desc_.tenant_id_,
        standalone_stmt_desc_.stmt_expired_time_,
        standalone_stmt_desc_.trx_lock_timeout_,
        standalone_stmt_desc_.pla_,
        standalone_stmt_desc_.consistency_type_,
        standalone_stmt_desc_.read_snapshot_type_,
        standalone_stmt_desc_.snapshot_version_,
        standalone_stmt_desc_.is_standalone_stmt_end_);
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
        tenant_id_,
        trans_id_,
        snapshot_version_,
        trans_param_,
        scheduler_,
        participants_,
        stmt_participants_,
        sql_no_,
        cur_stmt_desc_,
        need_rollback_,
        trans_expired_time_,
        cur_stmt_expired_time_,
        memstore_version_,
        cluster_version_,
        cluster_id_,
        participants_pla_,
        stmt_participants_pla_,
        snapshot_gene_type_,
        session_id_,
        proxy_session_id_,
        trace_info_.get_app_trace_id(),
        app_trace_id_confirmed_,
        nested_stmt_info_,
        can_elr_,
        is_dup_table_trans_,
        trans_snapshot_version_,
        is_local_trans_,
        stmt_snapshot_info_,
        is_fast_select_,
        stmt_rollback_info_,
        commit_version_,
        xid_,
        xa_end_timeout_seconds_,
        is_nested_stmt_,
        max_sql_no_,
        stmt_min_sql_no_,
        is_tightly_coupled_);
  }

  return len;
}

void ObLightTransCtxItem::reset()
{
  trans_ctx_ = NULL;
}

bool ObLightTransCtxItem::get_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx)
{
  bool ret_bool = true;
  ObLatchWGuard guard(lock_, ObLatchIds::DEFAULT_MUTEX);
  if (OB_ISNULL(trans_ctx_)) {
    ret_bool = false;
  } else {
    if (partition != trans_ctx_->get_partition() || trans_id != trans_ctx_->get_trans_id()) {
      ret_bool = false;
    } else {
      ctx = trans_ctx_;
    }
  }
  return ret_bool;
}

bool ObLightTransCtxItem::get_and_remove_trans_ctx(
    const ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx)
{
  bool ret_bool = true;
  ObLatchWGuard guard(lock_, ObLatchIds::DEFAULT_MUTEX);
  if (OB_ISNULL(trans_ctx_)) {
    ret_bool = false;
  } else {
    if (partition != trans_ctx_->get_partition() || trans_id != trans_ctx_->get_trans_id()) {
      ret_bool = false;
    } else {
      ctx = trans_ctx_;
      trans_ctx_ = NULL;
    }
  }
  return ret_bool;
}

bool ObLightTransCtxItem::remove_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id)
{
  bool ret_bool = true;
  ObLatchWGuard guard(lock_, ObLatchIds::DEFAULT_MUTEX);
  if (OB_ISNULL(trans_ctx_)) {
    ret_bool = false;
  } else {
    if (partition != trans_ctx_->get_partition() || trans_id != trans_ctx_->get_trans_id()) {
      ret_bool = false;
    } else {
      trans_ctx_ = NULL;
    }
  }
  return ret_bool;
}

bool ObLightTransCtxItem::add_trans_ctx(ObTransCtx* ctx)
{
  bool ret_bool = true;

  ObLatchWGuard guard(lock_, ObLatchIds::DEFAULT_MUTEX);
  if (OB_ISNULL(trans_ctx_)) {
    trans_ctx_ = ctx;
  } else {
    ret_bool = false;
  }

  return ret_bool;
}

void ObLightTransCtxMgr::reset()
{
  for (int i = 0; i < LIGHT_TRANS_CTX_MGR_TABLE_SIZE; i++) {
    item_array_[i].reset();
  }
  part_trans_ctx_mgr_ = NULL;
}

int ObLightTransCtxMgr::init(ObPartTransCtxMgr* part_trans_ctx_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_trans_ctx_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(part_trans_ctx_mgr));
  } else {
    part_trans_ctx_mgr_ = part_trans_ctx_mgr;
  }
  return ret;
}

int ObLightTransCtxMgr::leader_takeover(const common::ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_trans_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
  } else {
    for (int i = 0; i < LIGHT_TRANS_CTX_MGR_TABLE_SIZE; i++) {
      ObTransCtx* ctx = item_array_[i].get_trans_ctx();
      if (NULL != ctx && partition == ctx->get_partition()) {
        (void)part_trans_ctx_mgr_->revert_trans_ctx(ctx);
        item_array_[i].reset();
      }
    }
  }
  return ret;
}

int ObLightTransCtxMgr::remove_partition(const common::ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_trans_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
  } else {
    for (int i = 0; i < LIGHT_TRANS_CTX_MGR_TABLE_SIZE; i++) {
      ObTransCtx* ctx = item_array_[i].get_trans_ctx();
      if (NULL != ctx && partition == ctx->get_partition()) {
        (void)part_trans_ctx_mgr_->revert_trans_ctx(ctx);
        item_array_[i].reset();
      }
    }
  }
  return ret;
}

uint64_t ObLightTransCtxMgr::get_index(const ObPartitionKey& partition, const ObTransID& trans_id)
{
  uint64_t partition_hv = partition.hash();
  uint64_t trans_id_hv = trans_id.hash();
  return murmurhash(&trans_id_hv, sizeof(trans_id_hv), partition_hv) % LIGHT_TRANS_CTX_MGR_TABLE_SIZE;
}

bool ObLightTransCtxMgr::get_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx)
{
  bool ret_bool = true;
  ;
  if (!partition.is_valid() || !trans_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret_bool = false;
  } else {
    const uint64_t index = get_index(partition, trans_id);
    ret_bool = item_array_[index].get_trans_ctx(partition, trans_id, ctx);
  }
  return ret_bool;
}

bool ObLightTransCtxMgr::get_and_remove_trans_ctx(
    const ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx*& ctx)
{
  bool ret_bool = true;
  if (!partition.is_valid() || !trans_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret_bool = false;
  } else {
    const uint64_t index = get_index(partition, trans_id);
    ret_bool = item_array_[index].get_and_remove_trans_ctx(partition, trans_id, ctx);
  }
  return ret_bool;
}

bool ObLightTransCtxMgr::remove_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id)
{
  bool ret_bool = true;
  if (!partition.is_valid() || !trans_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret_bool = false;
  } else {
    const uint64_t index = get_index(partition, trans_id);
    ret_bool = item_array_[index].remove_trans_ctx(partition, trans_id);
  }
  return ret_bool;
}

bool ObLightTransCtxMgr::add_trans_ctx(ObTransCtx* ctx)
{
  bool ret_bool = true;
  if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid trans context");
    ret_bool = false;
  } else {
    const common::ObPartitionKey partition = ctx->get_partition();
    const ObTransID trans_id = ctx->get_trans_id();
    if (!partition.is_valid() || !trans_id.is_valid()) {
      TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
      ret_bool = false;
    } else {
      const uint64_t index = get_index(partition, trans_id);
      ret_bool = item_array_[index].add_trans_ctx(ctx);
    }
  }
  return ret_bool;
}

DEF_TO_STRING(ObLockForReadArg)
{
  int64_t pos = 0;
  J_KV(K(read_ctx_),
      K(snapshot_version_),
      K(read_trans_id_),
      K(data_trans_id_),
      K(read_sql_sequence_),
      K(data_sql_sequence_),
      K(read_latest_));
  return pos;
}

DEFINE_TO_STRING_AND_YSON(ObTransKey, OB_ID(hash), hash_val_, OB_ID(pkey), pkey_, OB_ID(trans_id), trans_id_);

}  // namespace transaction
}  // namespace oceanbase
