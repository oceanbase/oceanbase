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

#define USING_LOG_PREFIX SQL_SESSION

#include "sql/session/ob_basic_session_info.h"

#include "lib/string/ob_sql_string.h"
#include "lib/timezone/ob_oracle_format_models.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/ob_smart_call.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_sys_variable_mgr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "sql/ob_sql_trans_util.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/ob_physical_plan.h"
#include "storage/transaction/ob_weak_read_util.h"  //ObWeakReadUtil
#include "observer/omt/ob_tenant_timezone_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace sql {

ObBasicSessionInfo::ObBasicSessionInfo()
    : query_mutex_(),
      thread_data_mutex_(),
      is_valid_(true),
      is_deserialized_(false),
      tenant_id_(OB_INVALID_ID),
      effective_tenant_id_(OB_INVALID_ID),
      rpc_tenant_id_(0),
      is_changed_to_temp_tenant_(false),
      user_id_(OB_INVALID_ID),
      client_version_(),
      driver_version_(),
      sessid_(0),
      master_sessid_(INVALID_SESSID),
      version_(0),
      proxy_sessid_(VALID_PROXY_SESSID),
      //      in_transaction_(false),
      variables_last_modify_time_(0),
      global_vars_version_(0),
      sys_var_base_version_(OB_INVALID_VERSION),
      des_sys_var_base_version_(OB_INVALID_VERSION),
      trans_desc_(),
      trans_consistency_type_(transaction::ObTransConsistencyType::UNKNOWN),
      // minus 32 is used for alignment of ObMalloc, preventing memory allocation more than 8K.
      block_allocator_(
          SMALL_BLOCK_SIZE, common::OB_MALLOC_NORMAL_BLOCK_SIZE - 32, ObMalloc(ObModIds::OB_SQL_SESSION_SBLOCK)),
      ps_session_info_allocator_(
          sizeof(ObPsSessionInfo), common::OB_MALLOC_NORMAL_BLOCK_SIZE - 32, ObMalloc("PsSessionInfo")),
      name_pool_(ObModIds::OB_SQL_SESSION, OB_MALLOC_NORMAL_BLOCK_SIZE),
      base_sys_var_alloc_(ObModIds::OB_SQL_SESSION, OB_MALLOC_NORMAL_BLOCK_SIZE),
      trans_flags_(),
      bucket_allocator_wrapper_(&block_allocator_),
      user_var_val_map_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
      influence_plan_var_indexs_(),
      is_first_gen_(true),
      sys_var_fac_(),
      consistency_level_(INVALID_CONSISTENCY),
      tz_info_wrap_(),
      next_tx_read_only_(-1),
      next_tx_isolation_(transaction::ObTransIsolation::UNKNOWN),
      log_id_level_map_valid_(false),
      cur_phy_plan_(NULL),
      capability_(),
      proxy_capability_(),
      client_mode_(OB_MIN_CLIENT_MODE),
      changed_sys_vars_(),
      changed_user_vars_(),
      changed_var_pool_(ObModIds::OB_SQL_SESSION, OB_MALLOC_NORMAL_BLOCK_SIZE),
      is_database_changed_(false),
      feedback_manager_(),
      trans_spec_status_(TRANS_SPEC_NOT_SET),
      debug_sync_actions_(),
      partition_hit_(),
      magic_num_(0x13572468),
      current_execution_id_(-1),
      database_id_(OB_INVALID_ID),
      retry_info_(),
      last_query_trace_id_(),
      nested_count_(-1),
      safe_weak_read_snapshot_(0),
      weak_read_snapshot_source_(0),
      curr_trans_last_stmt_end_time_(0),
      read_snapshot_version_(OB_INVALID_VERSION),
      check_sys_variable_(true),
      is_foreign_key_cascade_(false),
      is_foreign_key_check_exist_(false),
      acquire_from_pool_(false),
      release_to_pool_(true),
      is_tenant_killed_(0),
      reused_count_(0),
      first_stmt_type_(stmt::T_NONE),
      exec_min_cluster_version_(GET_MIN_CLUSTER_VERSION()),
      stmt_type_(stmt::T_NONE),
      thread_id_(0),
      is_password_expired_(false)
{
  MEMSET(sys_vars_, 0, sizeof(sys_vars_));
  log_id_level_map_.reset_level();
  CHAR_CARRAY_INIT(tenant_);
  CHAR_CARRAY_INIT(effective_tenant_);
  CHAR_CARRAY_INIT(trace_id_buff_);
  ssl_cipher_buff_[0] = '\0';
}

ObBasicSessionInfo::~ObBasicSessionInfo()
{
  destroy();
}

bool ObBasicSessionInfo::is_server_status_in_transaction() const
{
  bool result = get_in_transaction() ||
                (!get_local_autocommit() && trans_desc_.get_standalone_stmt_desc().is_snapshot_version_valid());
  return result;
}

// for test
int ObBasicSessionInfo::test_init(
    uint32_t version, uint32_t sessid, uint64_t proxy_sessid, common::ObIAllocator* bucket_allocator)
{
  int ret = OB_SUCCESS;
  if (NULL != bucket_allocator) {
    bucket_allocator_wrapper_.set_alloc(bucket_allocator);
  }

  ret = user_var_val_map_.init(1024 * 1024 * 2,
      256,  // # of user variables
      (NULL == bucket_allocator ? NULL : &bucket_allocator_wrapper_));
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to init user_var_val_map", K(ret));
  } else if (OB_FAIL(debug_sync_actions_.init(SMALL_BLOCK_SIZE, bucket_allocator_wrapper_))) {
    LOG_WARN("fail to init debug sync actions", K(ret));
  } else if (OB_FAIL(set_session_state(SESSION_INIT))) {
    LOG_WARN("fail to set session stat", K(ret));
  } else if (OB_FAIL(trans_desc_.test_init())) {
    LOG_WARN("transaction desc init error", K(ret));
  } else if (OB_FAIL(set_time_zone(ObString("+8:00"), is_oracle_compatible(), true /* check_timezone_valid */))) {
    LOG_WARN("fail to set time zone", K(ret));
  } else {
    // tz_info_wrap_.set_tz_info_map(GCTX.tz_info_mgr_->get_tz_info_map());

    version_ = version;
    sessid_ = sessid;
    proxy_sessid_ = proxy_sessid;
  }
  return ret;
}

bool ObBasicSessionInfo::is_use_inner_allocator() const
{
  return bucket_allocator_wrapper_.get_alloc() == &block_allocator_;
}

int ObBasicSessionInfo::init(uint32_t version, uint32_t sessid, uint64_t proxy_sessid,
    common::ObIAllocator* bucket_allocator, const ObTZInfoMap* tz_info)
{
  int ret = OB_SUCCESS;
  ObWrapperAllocator* user_var_allocator_wrapper = NULL;
  if (is_acquire_from_pool()) {
    reused_count_++;
    if (OB_NOT_NULL(bucket_allocator) || !is_use_inner_allocator()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session from pool must use inner allocator", K(ret));
    }
  } else {
    if (NULL != bucket_allocator) {
      bucket_allocator_wrapper_.set_alloc(bucket_allocator);
      user_var_allocator_wrapper = &bucket_allocator_wrapper_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_acquire_from_pool() &&
             OB_FAIL(user_var_val_map_.init(1024 * 1024 * 2, 256, user_var_allocator_wrapper))) {
    LOG_WARN("fail to init user_var_val_map", K(ret));
  } else if (!is_acquire_from_pool() &&
             OB_FAIL(debug_sync_actions_.init(SMALL_BLOCK_SIZE, bucket_allocator_wrapper_))) {
    LOG_WARN("fail to init debug sync actions", K(ret));
  } else if (OB_FAIL(set_session_state(SESSION_INIT))) {
    LOG_WARN("fail to set session stat", K(ret));
  } else if (OB_FAIL(trans_desc_.init(this))) {
    LOG_WARN("transaction desc init error", K(ret));
  } else if (FALSE_IT(trans_result_.set_trans_desc(&trans_desc_))) {
  } else {
    version_ = version;
    sessid_ = sessid;
    proxy_sessid_ = proxy_sessid;
    uint64_t tenant_id = tenant_id_;
    if (OB_ISNULL(tz_info)) {
      ObTZMapWrap tz_map_wrap;
      if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap))) {
        LOG_WARN("get tenant timezone map failed", K(ret));
      } else {
        tz_info_wrap_.set_tz_info_map(tz_map_wrap.get_tz_map());
      }
    } else {
      tz_info_wrap_.set_tz_info_map(tz_info);
    }
  }
  return ret;
}

int ObBasicSessionInfo::update_safe_weak_read_snapshot(
    const uint64_t tenant_id, const int64_t& safe_weak_read_snapshot, const int64_t& safe_weak_read_snapshot_source)
{
  int ret = OB_SUCCESS;

  if (!ObWeakReadUtil::enable_monotonic_weak_read(tenant_id)) {
    LOG_WARN("observer not support causal order slave read", K(safe_weak_read_snapshot));
  } else if (safe_weak_read_snapshot < 0) {
    TRANS_LOG(WARN, "invalid argument", K(safe_weak_read_snapshot));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (safe_weak_read_snapshot_ < safe_weak_read_snapshot) {
      safe_weak_read_snapshot_ = safe_weak_read_snapshot;
      weak_read_snapshot_source_ = safe_weak_read_snapshot_source;
    }
  }

  return ret;
}

void ObBasicSessionInfo::destroy()
{
  if (magic_num_ != 0x13572468) {
    LOG_ERROR("ObBasicSessionInfo may be double free!!!", K(magic_num_));
  }
  trans_desc_.destroy();
  trans_result_.reset();
  trans_consistency_type_ = transaction::ObTransConsistencyType::UNKNOWN;
  magic_num_ = 0x86427531;
  if (thread_data_.cur_query_ != nullptr) {
    ob_free(thread_data_.cur_query_);
    thread_data_.cur_query_ = nullptr;
    thread_data_.cur_query_buf_len_ = 0;
  }
  total_stmt_tables_.reset();
  cur_stmt_tables_.reset();
}

void ObBasicSessionInfo::clean_status()
{
  //  in_transaction_ = false;
  trans_flags_.reset();
  trans_spec_status_ = TRANS_SPEC_NOT_SET;
  // trans_desc_.reset();
  trans_consistency_type_ = transaction::ObTransConsistencyType::UNKNOWN;
  set_valid(true);
  thread_data_.cur_query_start_time_ = 0;
  thread_data_.cur_query_len_ = 0;
  thread_data_.last_active_time_ = 0;
  reset_session_changed_info();
}

void ObBasicSessionInfo::reset(bool skip_sys_var)
{
  set_valid(false);
  is_deserialized_ = false;
  CHAR_CARRAY_INIT(tenant_);
  tenant_id_ = OB_INVALID_ID;
  CHAR_CARRAY_INIT(effective_tenant_);
  effective_tenant_id_ = OB_INVALID_ID;
  is_changed_to_temp_tenant_ = false;
  user_id_ = OB_INVALID_ID;
  client_version_.reset();
  driver_version_.reset();
  sessid_ = 0;
  master_sessid_ = INVALID_SESSID;
  version_ = 0;
  proxy_sessid_ = VALID_PROXY_SESSID;
  variables_last_modify_time_ = 0;
  global_vars_version_ = 0;
  des_sys_var_base_version_ = OB_INVALID_VERSION;

  trans_desc_.reset();
  trans_consistency_type_ = transaction::ObTransConsistencyType::UNKNOWN;
  trans_result_.reset();
  total_stmt_tables_.reset();
  cur_stmt_tables_.reset();
  // reset() of user_var_val_map_ and debug_sync_actions_ will keep some memory
  // allocated from block_allocator_ / bucket_allocator_wrapper_, so we skip
  // reset() of block_allocator_ and bucket_allocator_wrapper_.
  // block_allocator_.reset();
  ps_session_info_allocator_.reset();
  trans_flags_.reset();
  // bucket_allocator_wrapper_.reset();
  user_var_val_map_.reuse();
  if (!skip_sys_var) {
    memset(sys_vars_, 0, sizeof(sys_vars_));
    influence_plan_var_indexs_.reset();
  } else {
    const SysVarIds& all_sys_var_ids = sys_var_inc_info_.get_all_sys_var_ids();
    for (int i = 0; i < all_sys_var_ids.count(); i++) {
      int ret = OB_SUCCESS;
      int64_t store_idx = -1;
      ObSysVarClassType sys_var_id = all_sys_var_ids.at(i);
      OZ(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, store_idx));
      OV(0 <= store_idx && store_idx < ObSysVarFactory::ALL_SYS_VARS_COUNT);
      OV(OB_NOT_NULL(sys_vars_[store_idx]));
      OX(sys_vars_[store_idx]->clean_inc_value());
    }
    sys_var_inc_info_.reset();
    reset_timezone();
  }
  sys_var_in_pc_str_.reset();
  is_first_gen_ = true;
  CHAR_CARRAY_INIT(trace_id_buff_);
  // consistency_level_ = INVALID_CONSISTENCY;
  next_tx_read_only_ = -1;
  next_tx_isolation_ = transaction::ObTransIsolation::UNKNOWN;
  log_id_level_map_valid_ = false;
  log_id_level_map_.reset_level();
  cur_phy_plan_ = NULL;
  capability_.capability_ = 0;
  proxy_capability_.capability_ = 0;
  client_mode_ = OB_MIN_CLIENT_MODE;
  reset_session_changed_info();
  trans_spec_status_ = TRANS_SPEC_NOT_SET;
  debug_sync_actions_.reset();
  partition_hit_.reset();
  // magic_num_ = 0x86427531;
  current_execution_id_ = -1;
  last_trace_id_.reset();
  app_trace_id_.reset();
  database_id_ = OB_INVALID_ID;
  retry_info_.reset();
  last_query_trace_id_.reset();
  thread_data_.reset();
  nested_count_ = -1;
  if (!skip_sys_var) {
    sys_vars_cache_.reset();
  } else {
    sys_vars_cache_.clean_inc();
  }
  safe_weak_read_snapshot_ = 0;
  weak_read_snapshot_source_ = 0;
  curr_trans_last_stmt_end_time_ = 0;
  read_snapshot_version_ = OB_INVALID_VERSION;
  check_sys_variable_ = true;
  is_foreign_key_cascade_ = false;
  is_foreign_key_check_exist_ = false;
  acquire_from_pool_ = false;
  // skip release_to_pool_, see comments in .h file.
  is_tenant_killed_ = 0;
  first_stmt_type_ = stmt::T_NONE;
  exec_min_cluster_version_ = GET_MIN_CLUSTER_VERSION();
  thread_id_ = 0;
  is_password_expired_ = false;
  // release all allocators at last, otherwise some members may keep invalid pointers,
  // and for_each_session() of session mgr may core if access these invalid pointers.
  name_pool_.reset();
  if (!skip_sys_var) {
    base_sys_var_alloc_.reset();
    sys_var_fac_.destroy();
  }
}

void ObBasicSessionInfo::reset_timezone()
{
  int ret = OB_SUCCESS;
  ObObj tmp_obj1;
  if (OB_FAIL(get_sys_variable(SYS_VAR_TIME_ZONE, tmp_obj1))) {
    LOG_WARN("get sys var failed", K(ret));
  } else if (OB_FAIL(process_session_time_zone_value(tmp_obj1, false))) {
    LOG_WARN("set time zone failed", K(ret));
  }

  ObObj tmp_obj2;
  if (OB_FAIL(get_sys_variable(SYS_VAR_ERROR_ON_OVERLAP_TIME, tmp_obj2))) {
    LOG_WARN("get sys var failed", K(ret));
  } else if (OB_FAIL(process_session_overlap_time_value(tmp_obj2))) {
    LOG_WARN("process session overlap time value failed", K(ret), K(tmp_obj2));
  }
}

int ObBasicSessionInfo::init_tenant(const ObString& tenant_name, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(tenant_id), K(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant name is empty", K(tenant_name), K(ret));
  } else if (tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant name too long", K(tenant_name), K(ret));
  } else if (OB_FAIL(ob_cstrcopy(tenant_, sizeof(tenant_), tenant_name))) {
    LOG_WARN("failed to copy tenant name", K(tenant_name), K(ret));
  } else if (OB_FAIL(ob_cstrcopy(effective_tenant_, sizeof(effective_tenant_), tenant_name))) {
    LOG_WARN("failed to copy effective tenant name", K(tenant_name), K(ret));
  } else {
    ObTZMapWrap tz_map_wrap;
    if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap))) {
      LOG_WARN("get tenant timezone map failed", K(ret));
    } else {
      tz_info_wrap_.set_tz_info_map(tz_map_wrap.get_tz_map());
      tenant_id_ = tenant_id;
      effective_tenant_id_ = tenant_id;
      LOG_DEBUG("init session tenant", K(tenant_name), K(tenant_id));
    }
  }
  return ret;
}

int ObBasicSessionInfo::set_tenant(const common::ObString& tenant_name, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(tenant_id), K(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant name is empty", K(tenant_name), K(ret));
  } else if (tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant name too long", K(tenant_name), K(ret));
  } else if (OB_FAIL(ob_cstrcopy(tenant_, sizeof(tenant_), tenant_name))) {
    LOG_WARN("tenant name too long", K(tenant_name));
  } else {
    tenant_id_ = tenant_id;
    LOG_TRACE("set tenant", K(tenant_name), K(tenant_id));
  }
  return ret;
}

int ObBasicSessionInfo::set_tenant(const common::ObString& tenant_name, const uint64_t tenant_id, char* ori_tenant_name,
    const uint64_t length, uint64_t& ori_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(tenant_id), K(ret));
  } else if (OB_ISNULL(ori_tenant_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ori_tenant_name is NULL", K(ret));
  } else if (length < OB_MAX_TENANT_NAME_LENGTH + 1) {
    ret = OB_INVALID_ARGUMENT_FOR_LENGTH;
    LOG_WARN("tenant_name length is not enough ");
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant name is empty", K(tenant_name), K(ret));
  } else if (tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant name too long", K(tenant_name), K(ret));
  } else if (OB_FAIL(ob_cstrcopy(ori_tenant_name, length, tenant_, strlen(tenant_)))) {
    LOG_ERROR("tenant_name is longer than ori_tenant_name", K(length), K(strlen(tenant_)));
  } else if (OB_FAIL(ob_cstrcopy(tenant_, sizeof(tenant_), tenant_name))) {
    LOG_ERROR("tenant name too long", K(tenant_name));
  } else {
    ori_tenant_id = tenant_id_;
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObBasicSessionInfo::switch_tenant(uint64_t effective_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID != tenant_id_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support sys tenant switch tenant", K(ret), K(tenant_id_), K(effective_tenant_id_));
  } else if (effective_tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(effective_tenant_id));
  } else {
#ifndef NDEBUG
    if (effective_tenant_id_ != effective_tenant_id) {
      LOG_INFO(
          "switch tenant", K(effective_tenant_id), K(effective_tenant_id_), "priv_tenant_id", tenant_id_, K(lbt()));
    }
#endif
    effective_tenant_id_ = effective_tenant_id;
  }
  return ret;
}

const common::ObString ObBasicSessionInfo::get_tenant_name() const
{
  return ObString::make_string(tenant_);
}

const common::ObString ObBasicSessionInfo::get_effective_tenant_name() const
{
  return ObString::make_string(effective_tenant_);
}

int ObBasicSessionInfo::set_user(const ObString& user_name, const ObString& host_name, const uint64_t user_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(user_name.length() > common::OB_MAX_USER_NAME_LENGTH) ||
      OB_UNLIKELY(host_name.length() > common::OB_MAX_HOST_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name length invalid_", K(user_name), K(host_name), K(ret));
  } else {
    char tmp_buf[common::OB_MAX_USER_NAME_LENGTH + common::OB_MAX_HOST_NAME_LENGTH + 2] = {};
    snprintf(tmp_buf,
        sizeof(tmp_buf),
        "%.*s@%.*s",
        user_name.length(),
        user_name.ptr(),
        host_name.length(),
        host_name.ptr());
    ObString tmp_string(tmp_buf);
    LockGuard lock_guard(thread_data_mutex_);
    if (OB_FAIL(name_pool_.write_string(user_name, &thread_data_.user_name_))) {
      LOG_WARN("fail to write username to string_buf_", K(user_name), K(ret));
    } else if (OB_FAIL(name_pool_.write_string(host_name, &thread_data_.host_name_))) {
      LOG_WARN("fail to write hostname to string_buf_", K(host_name), K(ret));
    } else if (OB_FAIL(name_pool_.write_string(tmp_string, &thread_data_.user_at_host_name_))) {
      LOG_WARN("fail to write user_at_host_name to string_buf_", K(tmp_string), K(ret));
    } else {
      user_id_ = user_id;
    }
  }
  return ret;
}

int ObBasicSessionInfo::set_real_client_ip(const common::ObString& client_ip)
{
  int ret = OB_SUCCESS;
  char tmp_buf[common::OB_MAX_USER_NAME_LENGTH + common::OB_MAX_HOST_NAME_LENGTH + 2] = {};
  snprintf(tmp_buf,
      sizeof(tmp_buf),
      "%.*s@%.*s",
      thread_data_.user_name_.length(),
      thread_data_.user_name_.ptr(),
      client_ip.length(),
      client_ip.ptr());
  ObString tmp_string(tmp_buf);
  LockGuard lock_guard(thread_data_mutex_);
  if (OB_FAIL(name_pool_.write_string(client_ip, &thread_data_.client_ip_))) {
    LOG_WARN("fail to write client_ip to string_buf_", K(client_ip), K(ret));
  } else if (OB_FAIL(name_pool_.write_string(tmp_string, &thread_data_.user_at_client_ip_))) {
    LOG_WARN("fail to write user_at_host_name to string_buf_", K(tmp_string), K(ret));
  } else {
    thread_data_.user_client_addr_.set_ip_addr(client_ip, 0);
  }
  return ret;
}

int ObBasicSessionInfo::check_and_init_retry_info(const ObCurTraceId::TraceId& cur_trace_id, const ObString& sql)
{
  int ret = OB_SUCCESS;
  if (last_query_trace_id_.equals(cur_trace_id)) {  // is retrying.
    if (OB_UNLIKELY(!retry_info_.is_inited())) {
      LOG_ERROR("is retry packet, but retry info is not inited, will init it",
          K(last_query_trace_id_),
          K(cur_trace_id),
          K(retry_info_),
          K(get_sessid()),
          K(sql));
      if (OB_FAIL(retry_info_.init())) {
        LOG_WARN("fail to init retry info", K(ret), K(retry_info_), K(sql));
      }
    }
  } else {
    if (OB_UNLIKELY(retry_info_.is_inited())) {
      retry_info_.reset();
    }
    if (OB_FAIL(retry_info_.init())) {
      LOG_WARN("fail to init retry info", K(ret), K(retry_info_), K(sql));
    } else {
      last_query_trace_id_.set(cur_trace_id);
    }
  }
  return ret;
}

void ObBasicSessionInfo::check_and_reset_retry_info(const ObCurTraceId::TraceId& cur_trace_id, bool is_packet_retry)
{
  if (!is_packet_retry) {
    retry_info_.reset();
  }
  last_query_trace_id_.set(cur_trace_id);
}

const ObLogIdLevelMap* ObBasicSessionInfo::get_log_id_level_map() const
{
  return (log_id_level_map_valid_ ? (&log_id_level_map_) : NULL);
}

int ObBasicSessionInfo::set_client_version(const ObString& client_version)
{
  int ret = OB_SUCCESS;
  if (client_version.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("client version is empty", K(client_version), K(ret));
  } else if (OB_FAIL(name_pool_.write_string(client_version, &client_version_))) {
    LOG_WARN("failed to write client_version to string_buf_", K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::set_driver_version(const ObString& driver_version)
{
  int ret = OB_SUCCESS;
  if (driver_version.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("driver version is empty", K(driver_version), K(ret));
  } else if (OB_FAIL(name_pool_.write_string(driver_version, &driver_version_))) {
    LOG_WARN("failed to write driver_version to string_buf_", K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::set_default_database(
    const ObString& database_name, const ObCollationType coll_type /*= CS_TYPE_INVALID */)
{
  int ret = OB_SUCCESS;
  if (database_name.length() > OB_MAX_DATABASE_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT_FOR_LENGTH;
    LOG_WARN("invalid length for database_name", K(database_name), K(ret));
  } else {
    if (CS_TYPE_INVALID != coll_type) {
      const int64_t coll_val = static_cast<int64_t>(coll_type);
      if (OB_FAIL(update_sys_variable(SYS_VAR_CHARACTER_SET_DATABASE, coll_val))) {
        LOG_WARN("failed to update variable", K(ret));
      } else if (OB_FAIL(update_sys_variable(SYS_VAR_COLLATION_DATABASE, coll_val))) {
        LOG_WARN("failed to update variable", K(ret));
      } else {
      }
    }

    if (OB_SUCC(ret)) {
      LockGuard lock_guard(thread_data_mutex_);
      MEMCPY(thread_data_.database_name_, database_name.ptr(), database_name.length());
      thread_data_.database_name_[database_name.length()] = '\0';
      if (is_track_session_info()) {
        is_database_changed_ = true;
      }
    }
  }
  return ret;
}

int ObBasicSessionInfo::update_database_variables(ObSchemaGetterGuard* schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema guard is NULL", K(ret));
  } else {
    if ('\0' == thread_data_.database_name_[0]) {
      // no default database
      ObObj val;
      if (OB_FAIL(get_sys_variable(SYS_VAR_CHARACTER_SET_SERVER, val))) {
        LOG_WARN("failed to get sys variable", K(ret));
      } else if (OB_FAIL(update_sys_variable(SYS_VAR_CHARACTER_SET_DATABASE, val))) {
        LOG_WARN("failed to update sys variable", K(ret));
      } else if (OB_FAIL(get_sys_variable(SYS_VAR_COLLATION_SERVER, val))) {
        LOG_WARN("failed to get sys variable", K(ret));
      } else if (OB_FAIL(update_sys_variable(SYS_VAR_COLLATION_DATABASE, val))) {
        LOG_WARN("failed to update sys variable", K(ret));
      } else {
      }
    } else {
      const share::schema::ObDatabaseSchema* db_schema = NULL;
      ObString db_name(thread_data_.database_name_);
      if (OB_FAIL(schema_guard->get_database_schema(effective_tenant_id_, db_name, db_schema))) {
        LOG_WARN("get database schema failed", K(effective_tenant_id_), K(db_name), K(ret));
      } else if (NULL == db_schema) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("database not exist", K(effective_tenant_id_), K(db_name), K(ret));
        LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
      } else {
        const int64_t db_coll = static_cast<int64_t>(db_schema->get_collation_type());
        if (OB_FAIL(update_sys_variable(SYS_VAR_CHARACTER_SET_DATABASE, db_coll))) {
          LOG_WARN("failed to update sys variable", K(ret));
        } else if (OB_FAIL(update_sys_variable(SYS_VAR_COLLATION_DATABASE, db_coll))) {
          LOG_WARN("failed to update sys variable", K(ret));
        } else {
        }
      }
    }
  }

  return ret;
}

int ObBasicSessionInfo::update_max_packet_size()
{
  int ret = OB_SUCCESS;
  int64_t max_allowed_pkt = 0;
  int64_t net_buffer_len = 0;
  if (OB_FAIL(get_max_allowed_packet(max_allowed_pkt))) {
    LOG_WARN("fail to get_max_allowed_packet", K(ret));
  } else if (OB_FAIL(get_net_buffer_length(net_buffer_len))) {
    LOG_WARN("fail to get_net_buffer_length", K(ret));
  } else {
    thread_data_.max_packet_size_ = std::max(max_allowed_pkt, net_buffer_len);
  }
  return ret;
}

const ObString ObBasicSessionInfo::get_database_name() const
{
  ObString str_ret;
  str_ret.assign_ptr(
      const_cast<char*>(thread_data_.database_name_), static_cast<int32_t>(strlen(thread_data_.database_name_)));
  return str_ret;
}

//// FIXME:
// int ObBasicSessionInfo::get_database_id(
//    ObSchemaGetterGuard *schema_guard,
//    uint64_t &db_id) const
//{
//  int ret = OB_SUCCESS;
//  db_id = OB_INVALID_ID;
//  if (get_database_name().empty()) {
//    //do nothing
//  } else if (OB_UNLIKELY(NULL == schema_guard)) {
//    ret = OB_INVALID_ARGUMENT;
//    LOG_WARN("Schema guard should not be NULL", K(ret));
//  } else if (OB_FAIL(schema_guard->get_database_id(get_effective_tenant_id(), get_database_name(),
//                                                     db_id))) {
//    db_id = OB_INVALID_ID;
//    LOG_WARN("failed to get database id", K(db_id), K(ret));
//  } else { }//do nothing
//  return ret;
//}

////////////////////////////////////////////////////////////////
int ObBasicSessionInfo::get_global_sys_variable(
    const ObBasicSessionInfo* session, ObIAllocator& calc_buf, const ObString& var_name, ObObj& val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is NULL", K(ret), K(var_name));
  } else {
    // const ObDataTypeCastParams dtc_params(session->get_timezone_info(),
    //                                      session->get_local_nls_formats(),
    //                                      session->get_nls_collation(),
    //                                      session->get_nls_collation_nation());
    ObDataTypeCastParams dtc_params = session->get_dtc_params();
    if (OB_FAIL(get_global_sys_variable(session->get_effective_tenant_id(), calc_buf, dtc_params, var_name, val))) {
      LOG_WARN("fail to get global sys variable", K(ret), K(var_name));
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_global_sys_variable(const uint64_t actual_tenant_id,  // tenant may be switched
    ObIAllocator& calc_buf, const ObDataTypeCastParams& dtc_params, const ObString& var_name, ObObj& val)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema* tenant_schema = NULL;
  const ObSysVarSchema* sysvar_schema = NULL;
  const ObSysVariableSchema* sys_variable_schema = NULL;
  if (OB_UNLIKELY(!is_valid_tenant_id(actual_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(actual_tenant_id), K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(actual_tenant_id, schema_guard))) {
    ret = OB_SCHEMA_ERROR;
    OB_LOG(WARN, "fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(actual_tenant_id, tenant_schema))) {
    LOG_WARN("get tenant info failed", K(ret), K(actual_tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("tenant_schema is NULL", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(actual_tenant_id, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_sysvar_schema(var_name, sysvar_schema))) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_WARN("failed to get sysvar", K(ret), K(var_name));
  } else if (OB_ISNULL(sysvar_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("tenant_schema is NULL", K(ret));
  } else if (OB_FAIL(sysvar_schema->get_value(&calc_buf, dtc_params, val))) {
    LOG_WARN("failed to get value", K(ret), K(var_name));
  } else if (OB_FAIL(ObBasicSessionInfo::change_value_for_special_sys_var(var_name, val, val))) {
    LOG_ERROR("fail to change value for special sys var", K(ret), K(var_name), K(val));
  } else {
    LOG_DEBUG("get global sysvar", K(var_name), K(val));
  }
  return ret;
}

const ObBasicSysVar* ObBasicSessionInfo::get_sys_var(const int64_t idx) const
{
  const ObBasicSysVar* var = NULL;
  if (idx >= 0 && idx < ObSysVarFactory::ALL_SYS_VARS_COUNT) {
    var = sys_vars_[idx];
  }
  return var;
}

int ObBasicSessionInfo::init_system_variables(const bool print_info_log, const bool is_sys_tenant)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObObj type;
  ObObj value;
  ObObj min_val;
  ObObj max_val;
  ObObjType var_type = ObNullType;
  int64_t var_flag = ObSysVarFlag::NONE;
  int64_t var_amount = ObSysVariables::get_amount();
  ObObj casted_value;
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
  ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
  for (int64_t i = 0; OB_SUCC(ret) && i < var_amount; ++i) {
    name.assign_ptr(const_cast<char*>(ObSysVariables::get_name(i).ptr()),
        static_cast<ObString::obstr_size_t>(strlen(ObSysVariables::get_name(i).ptr())));
    bool is_exist = false;
    if (OB_FAIL(sys_variable_exists(name, is_exist))) {
      LOG_WARN("failed to check if sys variable exists", K(name), K(ret));
    } else if (!is_exist) {
      var_type = ObSysVariables::get_type(i);
      var_flag = ObSysVariables::get_flags(i);
      value.set_varchar(ObSysVariables::get_value(i));
      value.set_collation_type(ObCharset::get_system_collation());
      min_val.set_varchar(ObSysVariables::get_min(i));
      min_val.set_collation_type(ObCharset::get_system_collation());
      max_val.set_varchar(ObSysVariables::get_max(i));
      max_val.set_collation_type(ObCharset::get_system_collation());
      type.set_type(var_type);
      if (is_sys_tenant) {
        if (OB_FAIL(process_variable_for_tenant(name, value))) {
          LOG_WARN("process system variable for tenant error", K(name), K(value), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(load_sys_variable(name, type, value, min_val, max_val, var_flag))) {
          LOG_WARN("fail to load default system variable", K(name), K(ret));
        } else if (OB_NOT_NULL(sys_vars_[i]) && sys_vars_[i]->is_influence_plan() &&
                   OB_FAIL(influence_plan_var_indexs_.push_back(i))) {
          LOG_WARN("fail to add influence plan sys var", K(name), K(ret));
        } else if (print_info_log) {
          LOG_INFO("load default system variable", name.ptr(), value.get_string().ptr());
        }
      }
    }
  }  // end for

  if (OB_SUCC(ret)) {
    if (OB_FAIL(gen_sys_var_in_pc_str())) {
      LOG_INFO("fail to generate system variables in pc str");
    } else {
      global_vars_version_ = 0;
      sys_var_base_version_ = 0;
    }
  }
  return ret;
}

int ObBasicSessionInfo::update_query_sensitive_system_variable(ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const ObSysVarSchema* sysvar = NULL;
  int64_t schema_version = -1;
  const ObSimpleSysVariableSchema* sys_variable_schema = NULL;
  const uint64_t tenant_id = get_effective_tenant_id();
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  if (!check_sys_variable_) {
    // skip to avoid circular dependency.
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_CORE_SCHEMA_VERSION >= refreshed_schema_version) {
    // if in tenant creation process or the tenant creation fails or the local schema is not refreshed,
    // the system variables can not be obtained, skip this step.
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get tenant schema version failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema should not be null", K(ret), K(tenant_id));
  } else if (FALSE_IT(schema_version = sys_variable_schema->get_schema_version())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (schema_version > get_global_vars_version() && schema_version > OB_CORE_SCHEMA_VERSION) {
    const ObTenantSchema* tenant_info = NULL;
    bool need_update_version = false;
    const ObSysVariableSchema* sys_variable_schema = NULL;
    if (OB_FAIL(schema_guard.get_tenant_info(get_effective_tenant_id(), tenant_info))) {
      LOG_WARN("get tenant info from schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_sys_variable_schema(get_effective_tenant_id(), sys_variable_schema))) {
      if (OB_TENANT_NOT_EXIST == ret) {
        LOG_INFO("tenant maybe creating, just skip", K(ret), K(ret));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get sys variable schema failed", K(ret));
      }
    } else if (OB_ISNULL(sys_variable_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable schema is null", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema->get_sysvar_count(); ++i) {
        sysvar = sys_variable_schema->get_sysvar_schema(i);
        if (sysvar != NULL && sysvar->is_query_sensitive()) {
          if (OB_FAIL(update_sys_variable(sysvar->get_name(), sysvar->get_value()))) {
            if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret) {
              // this system variable may come from a higher version,
              // which is not available locally, ignore it.
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("update system variable failed", K(ret), K(*sysvar));
            }
          } else {
            need_update_version = true;
          }
        }
      }
      if (OB_SUCC(ret) && need_update_version) {
        set_global_vars_version(schema_version);
      }
    }
  }
  return ret;
}

// used for bootstarp, in which we can not get system variables from inner table.
int ObBasicSessionInfo::load_default_sys_variable(const bool print_info_log, const bool is_sys_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_system_variables(print_info_log, is_sys_tenant))) {
    LOG_WARN("Init system variables failed !", K(ret));
  } else {
    variables_last_modify_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

// used for session deserialization from lower version observer,
// and some system variables may be supported in current version.
int ObBasicSessionInfo::load_default_sys_variable(int64_t var_idx)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObObj type;
  ObObj value;
  ObObj min_val;
  ObObj max_val;
  ObObjType var_type = ObNullType;
  int64_t var_flag = ObSysVarFlag::NONE;
  if (var_idx < 0 || var_idx >= ObSysVarFactory::ALL_SYS_VARS_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the value of var_idx is unexpected", K(ret));
  } else {
    name.assign_ptr(const_cast<char*>(ObSysVariables::get_name(var_idx).ptr()),
        static_cast<ObString::obstr_size_t>(strlen(ObSysVariables::get_name(var_idx).ptr())));
    var_type = ObSysVariables::get_type(var_idx);
    var_flag = ObSysVariables::get_flags(var_idx);
    value.set_varchar(ObSysVariables::get_value(var_idx));
    value.set_collation_type(ObCharset::get_system_collation());
    min_val.set_varchar(ObSysVariables::get_min(var_idx));
    min_val.set_collation_type(ObCharset::get_system_collation());
    max_val.set_varchar(ObSysVariables::get_max(var_idx));
    max_val.set_collation_type(ObCharset::get_system_collation());
    type.set_type(var_type);
    if (OB_FAIL(load_sys_variable(name, type, value, min_val, max_val, var_flag))) {
      LOG_WARN("fail to load default system variable", K(name), K(ret));
    }
  }
  return ret;
}

int ObBasicSessionInfo::process_variable_for_tenant(const ObString& var, ObObj& val)
{
  int ret = OB_SUCCESS;
  if (0 == var.compare(OB_SV_LOWER_CASE_TABLE_NAMES)) {
    val.set_varchar("2");
    val.set_collation_type(ObCharset::get_system_collation());
  }
  return ret;
}

int ObBasicSessionInfo::create_sys_var(ObSysVarClassType sys_var_id, int64_t store_idx, ObBasicSysVar*& sys_var)
{
  int ret = OB_SUCCESS;
  OV(0 <= store_idx && store_idx < ObSysVarFactory::ALL_SYS_VARS_COUNT, OB_ERR_UNEXPECTED, sys_var_id, store_idx);
  if (OB_NOT_NULL(sys_vars_[store_idx])) {
    OV(sys_vars_[store_idx]->get_type() == sys_var_id,
        OB_ERR_UNEXPECTED,
        sys_var_id,
        store_idx,
        sys_vars_[store_idx]->get_type());
    OX(sys_var = sys_vars_[store_idx]);
  } else {
    OZ(sys_var_fac_.create_sys_var(sys_var_id, sys_var), sys_var_id);
    OV(OB_NOT_NULL(sys_var), OB_ERR_UNEXPECTED, sys_var_id, store_idx);
    OX(sys_vars_[store_idx] = sys_var);
  }
  return ret;
}

int ObBasicSessionInfo::inner_get_sys_var(const ObString& sys_var_name, ObBasicSysVar*& sys_var) const
{
  int ret = OB_SUCCESS;
  ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
  int64_t store_idx = -1;
  if (OB_UNLIKELY(SYS_VAR_INVALID == (sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(sys_var_name)))) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_WARN("fail to find sys var id by name", K(ret), K(sys_var_name), K(lbt()));
  } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_id), K(sys_var_name), K(lbt()));
  } else if (OB_UNLIKELY(store_idx < 0) || OB_UNLIKELY(store_idx >= ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("got store_idx is invalid", K(ret), K(store_idx));
  } else if (OB_ISNULL(sys_vars_[store_idx])) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("sys var is NULL", K(ret), K(store_idx), K(sys_var_name), K(lbt()));
  } else {
    sys_var = sys_vars_[store_idx];
  }
  return ret;
}

int ObBasicSessionInfo::inner_get_sys_var(const ObSysVarClassType sys_var_id, ObBasicSysVar*& sys_var) const
{
  int ret = OB_SUCCESS;
  int64_t store_idx = -1;
  if (OB_UNLIKELY(SYS_VAR_INVALID == sys_var_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid sys var id", K(ret), K(sys_var_id), K(lbt()));
  } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_id), K(lbt()));
  } else if (OB_UNLIKELY(store_idx < 0) || OB_UNLIKELY(store_idx >= ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("got store_idx is invalid", K(ret), K(store_idx));
  } else if (OB_ISNULL(sys_vars_[store_idx])) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("sys var is NULL", K(ret), K(store_idx), K(lbt()));
  } else {
    sys_var = sys_vars_[store_idx];
  }
  return ret;
}

int ObBasicSessionInfo::change_value_for_special_sys_var(
    const ObString& sys_var_name, const ObObj& ori_val, ObObj& new_val)
{
  int ret = OB_SUCCESS;
  ObSysVarClassType sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(sys_var_name);
  if (OB_UNLIKELY(SYS_VAR_INVALID == sys_var_id)) {
    LOG_WARN("fail to find sys var id by name", K(ret), K(sys_var_name));
  } else if (OB_FAIL(ObBasicSessionInfo::change_value_for_special_sys_var(sys_var_id, ori_val, new_val))) {
    LOG_WARN("fail to change value for special sys var", K(ret), K(sys_var_name), K(sys_var_id), K(ori_val));
  }
  return ret;
}

int ObBasicSessionInfo::change_value_for_special_sys_var(
    const ObSysVarClassType sys_var_id, const ObObj& ori_val, ObObj& new_val)
{
  int ret = OB_SUCCESS;
  int64_t sys_var_store_idx = -1;
  if (SYS_VAR_VERSION_COMMENT == sys_var_id || SYS_VAR_VERSION == sys_var_id ||
      SYS_VAR_SYSTEM_TIME_ZONE == sys_var_id) {
    if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, sys_var_store_idx))) {
      LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_id));
    } else {
      new_val = ObSysVariables::get_default_value(sys_var_store_idx);
    }
  } else {
    new_val = ori_val;
  }
  return ret;
}

int ObBasicSessionInfo::load_sys_variable(const ObString& name, const ObObj& type, const ObObj& value,
    const ObObj& min_val, const ObObj& max_val, const int64_t flags, bool is_from_sys_table /*= false*/)
{
  int ret = OB_SUCCESS;
  ObObj casted_cell;
  ObBasicSysVar* sys_var = NULL;
  ObSysVarClassType var_id = SYS_VAR_INVALID;
  ObObj real_val;
  ObObj val_ptr;
  ObObj min_ptr;
  ObObj max_ptr;
  ObObj val_type;
  ObObj tmp_type;
  int64_t store_idx = -1;
  if (SYS_VAR_INVALID == (var_id = ObSysVarFactory::find_sys_var_id_by_name(name, is_from_sys_table))) {
    if (is_from_sys_table) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
      LOG_ERROR("failed to find system variable", K(ret), K(name));
    }
  } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(var_id));
  } else if (OB_FAIL(cast_sys_variable(false, var_id, type, value, flags, val_type, val_ptr))) {
    LOG_WARN("fail to cast system variable", K(var_id), K(type), K(value), K(flags), K(val_ptr), K(ret));
  } else if (OB_FAIL(cast_sys_variable(true, var_id, type, min_val, flags, tmp_type, min_ptr))) {
    LOG_WARN("fail to cast system variable", K(var_id), K(type), K(min_val), K(flags), K(min_val), K(min_ptr), K(ret));
  } else if (OB_FAIL(cast_sys_variable(true, var_id, type, max_val, flags, tmp_type, max_ptr))) {
    LOG_WARN("fail to cast system variable", K(var_id), K(type), K(max_val), K(flags), K(max_ptr), K(ret));
  } else if (OB_FAIL(create_sys_var(var_id, store_idx, sys_var))) {
    LOG_WARN("fail to create sys var", K(name), K(value), K(ret));
  } else if (OB_ISNULL(sys_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sys var is NULL", K(name), K(value), K(ret));
  } else if (OB_FAIL(ObBasicSessionInfo::change_value_for_special_sys_var(var_id, val_ptr, real_val))) {
    LOG_WARN("fail to change value for special sys var", K(ret), K(var_id), K(val_ptr));
  } else if (OB_FAIL(sys_var->init(real_val, min_ptr, max_ptr, val_type.get_type(), flags))) {
    LOG_WARN("fail to init sys var", K(ret), K(sys_var->get_type()), K(real_val), K(name), K(value));
  } else if (OB_FAIL(process_session_variable(var_id, real_val, false, false /*check_timezone_valid*/))) {
    LOG_WARN("process system variable error", K(name), K(type), K(real_val), K(value), K(ret));
  } else {
    variables_last_modify_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObBasicSessionInfo::load_sys_variable(const ObString& name, const int64_t dtype, const ObString& value_str,
    const ObString& min_val_str, const ObString& max_val_str, const int64_t flags, bool is_from_sys_table /*= false*/)
{
  int ret = OB_SUCCESS;
  ObObj value, min_val, max_val;
  ObObj otype;
  value.set_varchar(value_str);
  value.set_collation_type(ObCharset::get_system_collation());
  min_val.set_varchar(min_val_str);
  min_val.set_collation_type(ObCharset::get_system_collation());
  max_val.set_varchar(max_val_str);
  max_val.set_collation_type(ObCharset::get_system_collation());
  otype.set_type(static_cast<ObObjType>(dtype));
  if (OB_FAIL(load_sys_variable(name, otype, value, min_val, max_val, flags, is_from_sys_table))) {
    LOG_WARN("fail to load system variable",
        K(name),
        K(otype),
        K(value),
        K(min_val),
        K(max_val),
        K(flags),
        K(is_from_sys_table),
        K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::cast_sys_variable(bool is_range_value, const ObSysVarClassType sys_var_id, const ObObj& type,
    const ObObj& value, int64_t flags, ObObj& out_type, ObObj& out_value)
{
  UNUSED(sys_var_id);
  int ret = OB_SUCCESS;
  ObObj casted_cell;
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
  if (ObVarcharType != value.get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(value), K(ret));
  } else {
    if (is_range_value && value.get_varchar() == ObString::make_string(ObBasicSysVar::EMPTY_STRING)) {
      out_value.set_null();
    } else if (ObBasicSysVar::is_null_value(value.get_varchar(), flags)) {
      out_value.set_null();
    } else {
      ObDataTypeCastParams dtc_params = get_dtc_params();
      ObCastCtx cast_ctx(&calc_buf, &dtc_params, CM_NONE, ObCharset::get_system_collation());
      if (OB_FAIL(
              ObObjCaster::to_type(type.get_type(), ObCharset::get_system_collation(), cast_ctx, value, casted_cell))) {
        _LOG_WARN("failed to cast object, cell=%s from_type=%s to_type=%s ret=%d ",
            to_cstring(value),
            ob_obj_type_str(value.get_type()),
            ob_obj_type_str(type.get_type()),
            ret);
      } else if (OB_FAIL(base_sys_var_alloc_.write_obj(casted_cell, &out_value))) {
        LOG_WARN("fail to store variable value", K(casted_cell), K(value), K(ret));
      } else {
        if (ob_is_string_type(out_value.get_type())) {
          out_value.set_collation_level(CS_LEVEL_SYSCONST);
          if (CS_TYPE_INVALID == out_value.get_collation_type()) {
            out_value.set_collation_type(ObCharset::get_system_collation());
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      out_type.set_type(type.get_type());
    }
  }
  return ret;
}

bool sys_var_compare(const SysVarNameVal& left, const SysVarNameVal& right)
{
  return left.name_ < right.name_;
}

/*
 *the following system variables affect plan cache:
 *read_only
 *ob_enable_transformation
 *binlog_row_image
 *collation_connection
 *sql_auto_is_null
 *div_precision_increment
 *ob_enable_aggregation_pushdown
 *ob_enable_index_direct_select
 *sql_mode
 *ob_max_parallel_degree
 *ob_route_policy
 *ob_read_consistency
 */
int ObBasicSessionInfo::get_influence_plan_sys_var(ObSysVarInPC& sys_vars) const
{
  int ret = OB_SUCCESS;
  // get influence_plan_sys_var from sys_var_val_map
  int64_t index = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_influence_plan_var_count(); ++i) {
    index = influence_plan_var_indexs_.at(i);
    if (index >= ObSysVarFactory::ALL_SYS_VARS_COUNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("influence plan system var indexs out of range", K(i), K(ret));
    } else if (NULL == sys_vars_[index]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("influence plan system var is NULL", K(i), K(ret));
    } else if (OB_FAIL(sys_vars.push_back(sys_vars_[index]->get_value()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("influence plan system variables push failed", K(ret));
    }
  }
  return ret;
}

int ObBasicSessionInfo::gen_sys_var_in_pc_str()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_SYS_VARS_STR_SIZE = 1024;
  ObSysVarInPC sys_vars;
  char* buf = NULL;
  int64_t pos = 0;
  if (is_first_gen_) {
    if (NULL == (buf = (char*)name_pool_.alloc(MAX_SYS_VARS_STR_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocator memory", K(ret), K(MAX_SYS_VARS_STR_SIZE));
    }
    is_first_gen_ = false;
  } else {
    buf = sys_var_in_pc_str_.ptr();
    MEMSET(buf, 0, sys_var_in_pc_str_.length());
    sys_var_in_pc_str_.reset();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_influence_plan_sys_var(sys_vars))) {
    LOG_WARN("fail to get influence plan system variables", K(ret));
  } else if (OB_FAIL(sys_vars.serialize_sys_vars(buf, MAX_SYS_VARS_STR_SIZE, pos))) {
    LOG_WARN("fail to serialize system vars");
  } else {
    (void)sys_var_in_pc_str_.assign(buf, int32_t(pos));
  }

  return ret;
}

int ObBasicSessionInfo::update_sys_variable_by_name(const ObString& var, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObSysVarClassType var_id = SYS_VAR_INVALID;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(val), K(ret));
  } else if (SYS_VAR_INVALID == (var_id = ObSysVarFactory::find_sys_var_id_by_name(var))) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_WARN("unknown variable", K(var), K(val), K(ret));
  } else if (OB_FAIL(update_sys_variable(var_id, val))) {
    LOG_WARN("failed to update sys variable", K(var), K(val), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::update_sys_variable_by_name(const common::ObString& var, int64_t val)
{
  ObObj obj;
  obj.set_int(val);
  return update_sys_variable_by_name(var, obj);
}

int ObBasicSessionInfo::update_sys_variable(const ObSysVarClassType sys_var_id, const ObString& val)
{
  ObObj obj;
  obj.set_varchar(val);
  obj.set_collation_type(ObCharset::get_system_collation());
  return update_sys_variable(sys_var_id, obj);
}

int ObBasicSessionInfo::update_sys_variable(const ObSysVarClassType sys_var_id, const int64_t val)
{
  ObObj obj;
  obj.set_int(val);
  return update_sys_variable(sys_var_id, obj);
}

int ObBasicSessionInfo::update_sys_variable(const ObString& var, const ObString& val)
{
  int ret = OB_SUCCESS;
  ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
  ObBasicSysVar* sys_var = NULL;
  if (OB_UNLIKELY(SYS_VAR_INVALID == (sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(var)))) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_WARN("unknown variable", K(var), K(val), K(ret));
  } else if (OB_FAIL(inner_get_sys_var(sys_var_id, sys_var))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(var), K(val));
  } else if (OB_ISNULL(sys_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(var));
  } else {
    // trim quotation marks
    ObString tmp_val;
    if (val.length() > 1 &&
        (('\'' == val[0] && '\'' == val[val.length() - 1]) || ('\"' == val[0] && '\"' == val[val.length() - 1]))) {
      tmp_val.assign_ptr(val.ptr() + 1, val.length() - 2);
    } else {
      tmp_val.assign_ptr(val.ptr(), val.length());
    }
    ObObj in_obj;
    in_obj.set_varchar(tmp_val);
    in_obj.set_collation_type(ObCharset::get_system_collation());
    const ObObj* out_obj = NULL;
    ObObj buf_obj;
    ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
    ObDataTypeCastParams dtc_params = get_dtc_params();
    ObCastCtx cast_ctx(&calc_buf, &dtc_params, CM_NONE, ObCharset::get_system_collation());
    if (OB_FAIL(ObObjCaster::to_type(sys_var->get_data_type(), cast_ctx, in_obj, buf_obj, out_obj))) {
      LOG_WARN("failed to cast obj", "expected type", sys_var->get_meta_type(), K(ret), K(var), K(in_obj));
    } else if (OB_ISNULL(out_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("casted success, but out_obj is NULL", K(in_obj), K(ret));
    } else if (OB_FAIL(update_sys_variable(sys_var_id, *out_obj))) {
      LOG_WARN("fail to update sys variable", K(ret), K(var), K(*out_obj));
    }
  }
  return ret;
}

int ObBasicSessionInfo::update_sys_variable(const ObSysVarClassType sys_var_id, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  ObBasicSysVar* sys_var = NULL;
  if (SYS_VAR_INVALID == sys_var_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sys_var_id", K(sys_var_id), K(ret));
  } else if (OB_FAIL(inner_get_sys_var(sys_var_id, sys_var))) {
    LOG_WARN("failed to inner get sys var", K(ret), K(sys_var_id), K(val));
  } else if (OB_ISNULL(sys_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to inner get sys var, sys var is null", K(ret), K(sys_var_id), K(val));
  } else if (is_track_session_info()) {
    if (OB_FAIL(track_sys_var(sys_var_id, sys_var->get_value()))) {
      LOG_WARN("failed to track sys var", K(ret), K(sys_var_id), K(val));
    } else {
      variables_last_modify_time_ = ObTimeUtility::current_time();
      LOG_DEBUG("succ to track system variable", K(ret), K(sys_var_id), K(val), K(sys_var->get_value()));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(deep_copy_sys_variable(sys_var_id, val, &obj))) {
      LOG_WARN("fail to update system variable", K(sys_var_id), K(val), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_session_variable(sys_var_id, val, true, false /*check_timezone_valid*/))) {
      LOG_WARN("process system variable error", K(sys_var_id), K(val), K(ret));
    } else if (OB_FAIL(sys_var_inc_info_.add_sys_var_id(sys_var_id))) {
      LOG_WARN("add sys var id error", K(sys_var_id), K(ret));
    } else {
      if (SYS_VAR_TIME_ZONE == sys_var_id && !tz_info_wrap_.is_position_class()) {
        const int64_t buf_len = 16;
        char tmp_buf[buf_len] = {0};
        int64_t pos = 0;
        ObObj tmp_obj = val;
        if (OB_ISNULL(tz_info_wrap_.get_time_zone_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("time zone info is null", K(ret));
        } else if (OB_FAIL(tz_info_wrap_.get_time_zone_info()->timezone_to_str(tmp_buf, buf_len, pos))) {
          LOG_WARN("timezone to str failed", K(ret));
        } else {
          tmp_obj.set_common_value(ObString(pos, tmp_buf));
          if (OB_FAIL(deep_copy_sys_variable(sys_var_id, tmp_obj, &obj))) {
            LOG_WARN("deep copy sys var failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (ob_is_string_type(obj.get_type())) {
          obj.set_collation_level(CS_LEVEL_SYSCONST);
          if (CS_TYPE_INVALID == obj.get_collation_type()) {
            obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        }
        sys_var->set_value(obj);
      }
    }
  }
  if (OB_SUCC(ret) && !is_deserialized_ && sys_var->is_influence_plan() && OB_FAIL(gen_sys_var_in_pc_str())) {
    LOG_ERROR("fail to gen sys var in pc str", K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::deep_copy_trace_id_var(const ObObj& src_val, ObObj* dest_val_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_val_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest val ptr is NULL", K(ret));
  } else {
    *dest_val_ptr = src_val;
    ObString new_str;
    ObString src_str;
    const int64_t STR_BUFF_LEN = sizeof(trace_id_buff_);
    int64_t src_str_len = 0;
    if (OB_FAIL(src_val.get_varchar(src_str))) {
      LOG_WARN("fail to get varchar", K(src_val), K(ret));
    } else if (OB_UNLIKELY((src_str_len = src_str.length()) == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid string", K(src_str), K(ret));
    } else if (STR_BUFF_LEN <= src_str_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid src str len", K(src_str_len), K(ret));
    } else {
      MEMCPY(trace_id_buff_, src_str.ptr(), src_str_len);
      trace_id_buff_[src_str_len] = '\0';
      new_str.assign_ptr(trace_id_buff_, static_cast<ObString::obstr_size_t>(src_str_len));
      dest_val_ptr->set_varchar(new_str);
    }
  }
  return ret;
}

// name_pool_ is an allocator that only alloc but not free,
// now we optimize for trace_id only, which will be accessed frequently.
int ObBasicSessionInfo::deep_copy_sys_variable(
    const ObSysVarClassType sys_var_id, const ObObj& src_val, ObObj* dest_val_ptr)
{
  int ret = OB_SUCCESS;
  if (sys_var_id == SYS_VAR_OB_STATEMENT_TRACE_ID) {
    if (OB_FAIL(deep_copy_trace_id_var(src_val, dest_val_ptr))) {
      LOG_WARN("fail to deep copy trace id", K(ret));
    }
  } else if (OB_FAIL(name_pool_.write_obj(src_val, dest_val_ptr))) {
    LOG_WARN("fail to write obj", K(src_val), K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::update_session_sys_variable(ObExecContext& ctx, const ObString& var, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* sys_var = NULL;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(ret), K(var));
  } else if (OB_FAIL(inner_get_sys_var(var, sys_var))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(var), K(val));
  } else if (OB_ISNULL(sys_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(var), K(val));
  } else {
    ObMySQLProxy sql_proxy;
    ObSetVar set_var(var, ObSetVar::SET_SCOPE_SESSION, false, OB_INVALID_ID, ctx.get_allocator(), sql_proxy);
    if (OB_FAIL(sys_var->session_update(ctx, set_var, val))) {
      LOG_WARN("session update system variable failed", K(ret), K(set_var));
    } else if (OB_FAIL(sys_var->update(ctx, set_var, val))) {
      LOG_WARN("fail to update sys var", K(ret), K(var), K(val));
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_sys_variable_by_name(const ObString& var, ObBasicSysVar*& val) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else if (OB_FAIL(inner_get_sys_var(var, val))) {
    LOG_WARN("fail to inner get sys var", K(var), K(ret));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(var));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::get_sys_variable_by_name(const ObString& var, ObObj& val) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* sys_var = NULL;
  if (OB_UNLIKELY(var.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else if (OB_FAIL(inner_get_sys_var(var, sys_var))) {
    LOG_WARN("fail to get sys var", K(ret), K(var));
  } else if (OB_ISNULL(sys_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to get sys var, but sys var is NULL", K(ret), K(var));
  } else {
    val = sys_var->get_value();
  }
  return ret;
}

int ObBasicSessionInfo::get_sys_variable_by_name(const common::ObString& var, int64_t& val) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_FAIL(get_sys_variable_by_name(var, obj))) {
  } else if (OB_FAIL(obj.get_int(val))) {
    LOG_WARN("wrong obj type for system variable", K(var), K(obj), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::get_sys_variable(const ObSysVarClassType sys_var_id, common::ObObj& val) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* var = NULL;
  if (OB_UNLIKELY(SYS_VAR_INVALID == sys_var_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sys_var_id", K(ret), K(sys_var_id));
  } else if (OB_FAIL(inner_get_sys_var(sys_var_id, var))) {
    LOG_WARN("fail to get sys var", K(ret), K(var));
  } else if (OB_ISNULL(var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to get sys var, but sys var is NULL", K(ret), K(var));
  } else {
    val = var->get_value();
  }
  return ret;
}

int ObBasicSessionInfo::get_sys_variable(const ObSysVarClassType sys_var_id, common::ObString& val) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (SYS_VAR_INVALID == sys_var_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sys_var_id", K(sys_var_id), K(ret));
  } else if (OB_FAIL(get_sys_variable(sys_var_id, obj))) {
    LOG_WARN("failed to get system variable", K(sys_var_id), K(ret));
  } else if (OB_FAIL(obj.get_varchar(val))) {
    LOG_WARN("wrong obj type for system variable", K(sys_var_id), K(obj), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::get_sys_variable(const ObSysVarClassType sys_var_id, int64_t& val) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_FAIL(get_sys_variable(sys_var_id, obj))) {
    LOG_WARN("failed to get system variable", K(sys_var_id), K(obj), K(ret));
  } else if (OB_FAIL(obj.get_int(val))) {
    LOG_WARN("wrong obj type for system variable", K(sys_var_id), K(obj), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::get_sys_variable(const ObSysVarClassType sys_var_id, uint64_t& val) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_FAIL(get_sys_variable(sys_var_id, obj))) {
    LOG_WARN("failed to get system variable", K(sys_var_id), K(obj), K(ret));
  } else if (OB_FAIL(obj.get_uint64(val))) {
    LOG_WARN("wrong obj type for system variable", K(ret), K(sys_var_id), K(obj));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::sys_variable_exists(const ObString& var, bool& is_exists) const
{
  int ret = OB_SUCCESS;
  is_exists = false;
  ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
  int64_t store_idx = -1;
  if (SYS_VAR_INVALID == (sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(var))) {
    LOG_DEBUG("sys var is not exist", K(var), K(ret));
  } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, store_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(sys_var_id), K(var), K(ret));
  } else if (store_idx < 0 || store_idx >= ObSysVarFactory::ALL_SYS_VARS_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("got store_idx is invalid", K(store_idx), K(ret));
  } else {
    is_exists = (NULL != sys_vars_[store_idx]);
  }
  return ret;
}

ObPhysicalPlan* ObBasicSessionInfo::get_cur_phy_plan() const
{
  return cur_phy_plan_;
}

int ObBasicSessionInfo::set_cur_phy_plan(ObPhysicalPlan* cur_phy_plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_phy_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("current physical plan is NULL", K(lbt()), K(ret));
  } else {
    cur_phy_plan_ = cur_phy_plan;
  }
  return ret;
}

void ObBasicSessionInfo::reset_cur_phy_plan_to_null()
{
  cur_phy_plan_ = NULL;
}

ObObjType ObBasicSessionInfo::get_sys_variable_type(const ObString& var_name) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = ObMaxType;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(var_name, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(var_name));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(var_name));
  } else {
    obj_type = val->get_data_type();
  }
  return obj_type;
}

ObObjType ObBasicSessionInfo::get_sys_variable_meta_type(const ObString& var_name) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = ObMaxType;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(var_name, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(var_name));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(var_name));
  } else {
    obj_type = val->get_meta_type();
  }
  return obj_type;
}

#define PROCESS_SESSION_INT_VARIABLE(sys_var)                       \
  do {                                                              \
    ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);           \
    int64_t val_int = 0;                                            \
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, CS_TYPE_INVALID); \
    EXPR_GET_INT64_V2(val, val_int);                                \
    if (OB_SUCC(ret)) {                                             \
      sys_var = val_int;                                            \
    }                                                               \
  } while (0)

OB_INLINE int ObBasicSessionInfo::process_session_variable(
    ObSysVarClassType var, const ObObj& val, bool is_inc, const bool check_timezone_valid /*true*/)
{
  int ret = OB_SUCCESS;
  switch (var) {
    case SYS_VAR_OB_LOG_LEVEL: {
      OZ(process_session_log_level(val), val);
      break;
    }
    case SYS_VAR_OB_COMPATIBILITY_MODE: {
      OZ(process_session_compatibility_mode_value(val, is_inc), val);
      break;
    }
    case SYS_VAR_SQL_MODE: {
      OZ(process_session_sql_mode_value(val, is_inc), val);
      break;
    }
    case SYS_VAR_TIME_ZONE: {
      OZ(process_session_time_zone_value(val, check_timezone_valid));
      break;
    }
    case SYS_VAR_ERROR_ON_OVERLAP_TIME: {
      OZ(process_session_overlap_time_value(val));
      break;
    }
    case SYS_VAR_WAIT_TIMEOUT: {
      LockGuard lock_guard(thread_data_mutex_);
      PROCESS_SESSION_INT_VARIABLE(thread_data_.wait_timeout_);
      break;
    }
    case SYS_VAR_DEBUG_SYNC: {
      const bool is_global = false;
      ret = process_session_debug_sync(val, is_global);
      break;
    }
    case SYS_VAR_OB_GLOBAL_DEBUG_SYNC: {
      const bool is_global = true;
      ret = process_session_debug_sync(val, is_global);
      break;
    }
    case SYS_VAR_OB_READ_CONSISTENCY: {
      int64_t consistency = 0;
      PROCESS_SESSION_INT_VARIABLE(consistency);
      if (OB_SUCC(ret)) {
        consistency_level_ = static_cast<ObConsistencyLevel>(consistency);
      }
      break;
    }
    case SYS_VAR_AUTO_INCREMENT_INCREMENT: {
      uint64_t uint_val = 0;
      OZ(val.get_uint64(uint_val), val);
      OX(sys_vars_cache_.set_auto_increment_increment(uint_val, is_inc));
      break;
    }
    case SYS_VAR_SQL_THROTTLE_CURRENT_PRIORITY: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_sql_throttle_current_priority(int_val, is_inc));
      break;
    }
    case SYS_VAR_OB_LAST_SCHEMA_VERSION: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_last_schema_version(int_val, is_inc));
      break;
    }
    case SYS_VAR_SQL_SELECT_LIMIT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_sql_select_limit(int_val, is_inc));
      break;
    }
    case SYS_VAR_AUTO_INCREMENT_OFFSET: {
      uint64_t uint_val = 0;
      OZ(val.get_uint64(uint_val), val);
      OX(sys_vars_cache_.set_auto_increment_offset(uint_val, is_inc));
      break;
    }
    case SYS_VAR_LAST_INSERT_ID: {
      uint64_t uint_val = 0;
      OZ(val.get_uint64(uint_val), val);
      OX(sys_vars_cache_.set_last_insert_id(uint_val, is_inc));
      break;
    }
    case SYS_VAR_BINLOG_ROW_IMAGE: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_binlog_row_image(int_val, is_inc));
      break;
    }
    case SYS_VAR_FOREIGN_KEY_CHECKS: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_foreign_key_checks(int_val, is_inc));
      break;
    }
    case SYS_VAR_TX_ISOLATION: {
      ObString str_val;
      int32_t isolation = ObTransIsolation::UNKNOWN;
      OZ(val.get_string(str_val));
      OX(isolation = ObTransIsolation::get_level(str_val));
      OV(isolation != ObTransIsolation::UNKNOWN, OB_ERR_UNEXPECTED, isolation);
      OX(sys_vars_cache_.set_tx_isolation(isolation, is_inc));
      break;
    }
    case SYS_VAR_TX_READ_ONLY: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_tx_read_only(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_OB_ENABLE_PLAN_CACHE: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_enable_plan_cache(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_optimizer_use_sql_plan_baselines(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_optimizer_capture_sql_plan_baselines(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_IS_RESULT_ACCURATE: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_is_result_accurate(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR__OB_USE_PARALLEL_EXECUTION: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set__ob_use_parallel_execution(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR__OPTIMIZER_ADAPTIVE_CURSOR_SHARING: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set__optimizer_adaptive_cursor_sharing(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_CHARACTER_SET_RESULTS: {
      int64_t coll_int64 = 0;
      if (val.is_null()) {
        OX(sys_vars_cache_.set_character_set_results(CHARSET_INVALID, is_inc));
      } else if (OB_FAIL(val.get_int(coll_int64))) {
        LOG_WARN("fail to get int from value", K(val), K(ret));
      } else if (OB_UNLIKELY(false == ObCharset::is_valid_collation(coll_int64))) {
        // do not set error code.
        LOG_WARN("invalid collation", K(coll_int64), K(val));
        OX(sys_vars_cache_.set_character_set_results(CHARSET_INVALID, is_inc));
      } else {
        OX(sys_vars_cache_.set_character_set_results(
            ObCharset::charset_type_by_coll(static_cast<ObCollationType>(coll_int64)), is_inc));
      }
      break;
    }
    case SYS_VAR_OB_ENABLE_SQL_AUDIT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_enable_sql_audit(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_NLS_LENGTH_SEMANTICS: {
      ObString str_val;
      ObLengthSemantics nls_length_semantics = LS_BYTE;
      OZ(val.get_string(str_val));
      OX(nls_length_semantics = get_length_semantics(str_val));
      OV(nls_length_semantics != LS_INVALIED, OB_ERR_UNEXPECTED, nls_length_semantics);
      OX(sys_vars_cache_.set_nls_length_semantics(nls_length_semantics, is_inc));
      break;
    }
    case SYS_VAR_AUTOCOMMIT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_autocommit(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_OB_ENABLE_TRACE_LOG: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_enable_trace_log(int_val != 0, is_inc));
      break;
    }
    case SYS_VAR_OB_ORG_CLUSTER_ID: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_org_cluster_id(int_val, is_inc));
      break;
    }
    case SYS_VAR_OB_QUERY_TIMEOUT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_query_timeout(int_val, is_inc));
      break;
    }
    case SYS_VAR_OB_PL_BLOCK_TIMEOUT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_pl_block_timeout(int_val, is_inc));
      break;
    }
    case SYS_VAR_OB_TRX_TIMEOUT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_trx_timeout(int_val, is_inc));
      break;
    }
    case SYS_VAR_OB_TRX_IDLE_TIMEOUT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_trx_idle_timeout(int_val, is_inc));
      break;
    }
    case SYS_VAR_OB_TRX_LOCK_TIMEOUT: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_ob_trx_lock_timeout(int_val, is_inc));
      break;
    }
    case SYS_VAR_COLLATION_CONNECTION: {
      int64_t int_val = 0;
      OZ(val.get_int(int_val), val);
      OX(sys_vars_cache_.set_collation_connection(int_val, is_inc));
      break;
    }
    case SYS_VAR_TIMESTAMP: {
      // the unit of set timestamp operation is second,
      // but we store it in microsecond, after a round operation.
      if (val.get_number().is_zero()) {
        sys_vars_cache_.set_timestamp(0, is_inc);
      } else {
        const ObObj* res_obj = NULL;
        ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
        number::ObNumber value;
        number::ObNumber unit;
        if (OB_FAIL(unit.from(static_cast<int64_t>(USECS_PER_SEC * 10), allocator))) {
          LOG_WARN("failed to get the number", K(ret));
        } else if (OB_FAIL(val.get_number().mul(unit, value, allocator))) {
          LOG_WARN("failed to get the result of timestamp to microsecond", K(ret));
        } else {
          ObObj param_obj;
          param_obj.set_number(value);
          ObCollationType cast_coll_type = static_cast<ObCollationType>(get_local_collation_connection());
          // const ObDataTypeCastParams dtc_params(TZ_INFO(this), GET_NLS_FORMATS(this), get_nls_collation(),
          // get_nls_collation_nation());;
          ObDataTypeCastParams dtc_params = get_dtc_params();
          ObCastCtx cast_ctx(&allocator,
              &dtc_params,
              0, /*number_int useless*/
              CM_NONE,
              cast_coll_type,
              NULL /* time zone info */);
          EXPR_CAST_OBJ_V2(ObIntType, param_obj, res_obj);
          if (OB_SUCC(ret)) {
            if (OB_ISNULL(res_obj)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result obj is null");
            } else {
              sys_vars_cache_.set_timestamp((res_obj->get_int() + 5) / 10, is_inc);
            }
          } else {
            LOG_WARN("failed to convert the number to int", K(ret));
          }
        }
      }
      break;
    }
    case SYS_VAR_NLS_DATE_FORMAT:
    case SYS_VAR_NLS_TIMESTAMP_FORMAT:
    case SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT: {
      ObString format;
      if (OB_FAIL(val.get_string(format))) {
        LOG_WARN("fail to get nls_date_format str value", K(ret), K(var));
      } else {
        int64_t nls_enum = ObNLSFormatEnum::NLS_DATE;
        ObDTMode mode = DT_TYPE_DATETIME;
        if (SYS_VAR_NLS_TIMESTAMP_FORMAT == var) {
          mode |= DT_TYPE_ORACLE;
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP;
        } else if (SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT == var) {
          mode |= DT_TYPE_ORACLE;
          mode |= DT_TYPE_TIMEZONE;
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP_TZ;
        }
        ObSEArray<ObDFMElem, ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems;
        ObBitSet<ObDFMFlag::MAX_FLAG_NUMBER> elem_flags;
        // 1. parse and check semantic of format string
        if (OB_FAIL(ObDFMUtil::parse_datetime_format_string(format, dfm_elems))) {
          LOG_WARN("fail to parse oracle datetime format string", K(ret), K(format));
        } else if (OB_FAIL(ObDFMUtil::check_semantic(dfm_elems, elem_flags, mode))) {
          LOG_WARN("check semantic of format string failed", K(ret), K(format));
        } else {
          switch (nls_enum) {
            case ObNLSFormatEnum::NLS_DATE:
              sys_vars_cache_.set_nls_date_format(format, is_inc);
              break;
            case ObNLSFormatEnum::NLS_TIMESTAMP:
              sys_vars_cache_.set_nls_timestamp_format(format, is_inc);
              break;
            case ObNLSFormatEnum::NLS_TIMESTAMP_TZ:
              sys_vars_cache_.set_nls_timestamp_tz_format(format, is_inc);
              break;
            default:
              break;
          }
        }
        LOG_DEBUG("succ to set SYS_VAR_NLS_FORMAT", K(ret), K(var), K(nls_enum), K(format));
      }
      break;
    }
    case SYS_VAR_NLS_NCHAR_CHARACTERSET:
    case SYS_VAR_NLS_CHARACTERSET: {
      ObString str_val;
      ObCharsetType charset = CHARSET_INVALID;
      ObCollationType collation = CS_TYPE_INVALID;
      OZ(val.get_string(str_val), val);
      OX(charset = ObCharset::charset_type_by_name_oracle(str_val));
      OX(collation = ObCharset::get_default_collation_oracle(charset));
      OV(ObCharset::is_valid_charset(charset) && ObCharset::is_valid_collation(collation),
          OB_ERR_INVALID_CHARACTER_STRING,
          str_val,
          charset,
          collation);
      if (var == SYS_VAR_NLS_CHARACTERSET) {
        OX(sys_vars_cache_.set_nls_collation(collation, is_inc));
      } else {
        OX(sys_vars_cache_.set_nls_nation_collation(collation, is_inc));
      }
      break;
    }
    case SYS_VAR_OB_TRACE_INFO: {
      ObString trace_info;
      if (OB_FAIL(val.get_string(trace_info))) {
        LOG_WARN("fail to get str value", K(ret), K(var));
      } else {
        sys_vars_cache_.set_ob_trace_info(trace_info, is_inc);
      }
      break;
    }
    default: {
      // do nothing
    }
  }
  return ret;
}

int ObBasicSessionInfo::process_session_variable_fast()
{
  int ret = OB_SUCCESS;
  int64_t store_idx = -1;
  // SYS_VAR_OB_LOG_LEVEL
  OZ(ObSysVarFactory::calc_sys_var_store_idx(SYS_VAR_OB_LOG_LEVEL, store_idx));
  OV(ObSysVarFactory::is_valid_sys_var_store_idx(store_idx));
  OZ(process_session_log_level(sys_vars_[store_idx]->get_value()));
  // SYS_VAR_DEBUG_SYNC
  OZ(ObSysVarFactory::calc_sys_var_store_idx(SYS_VAR_DEBUG_SYNC, store_idx));
  OV(ObSysVarFactory::is_valid_sys_var_store_idx(store_idx));
  OZ(process_session_debug_sync(sys_vars_[store_idx]->get_value(), false));
  // SYS_VAR_OB_GLOBAL_DEBUG_SYNC
  OZ(ObSysVarFactory::calc_sys_var_store_idx(SYS_VAR_OB_GLOBAL_DEBUG_SYNC, store_idx));
  OV(ObSysVarFactory::is_valid_sys_var_store_idx(store_idx));
  OZ(process_session_debug_sync(sys_vars_[store_idx]->get_value(), true));
  // SYS_VAR_OB_READ_CONSISTENCY
  OZ(ObSysVarFactory::calc_sys_var_store_idx(SYS_VAR_OB_READ_CONSISTENCY, store_idx));
  OV(ObSysVarFactory::is_valid_sys_var_store_idx(store_idx));
  if (OB_SUCC(ret)) {
    const ObObj& val = sys_vars_[store_idx]->get_value();
    int64_t consistency = 0;
    PROCESS_SESSION_INT_VARIABLE(consistency);
    OX(consistency_level_ = static_cast<ObConsistencyLevel>(consistency));
  }
  // SYS_VAR_WAIT_TIMEOUT
  {
    LockGuard lock_guard(thread_data_mutex_);
    get_int64_sys_var(SYS_VAR_WAIT_TIMEOUT, thread_data_.wait_timeout_);
  }
  return ret;
}

int ObBasicSessionInfo::process_session_sql_mode_value(const ObObj& value, bool is_inc)
{
  int ret = OB_SUCCESS;
  ObSQLMode sql_mode = 0;
  if (value.is_string_type()) {
    const ObString& sql_mode_str = value.get_string();
    if (OB_FAIL(ob_str_to_sql_mode(sql_mode_str, sql_mode))) {
      LOG_WARN("failed to get sql mode", K(sql_mode_str), K(value), K(ret));
    }
  } else if (ObUInt64Type == value.get_type()) {
    sql_mode = value.get_uint64();
  } else if (ObIntType == value.get_type()) {
    sql_mode = static_cast<ObSQLMode>(value.get_int());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql mode val type", K(value.get_type()), K(value), K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (!is_sql_mode_supported(sql_mode)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Value for sql_mode");
    LOG_WARN("invalid sql mode val", K(value.get_type()), K(value), K(ret));
  } else {
    set_sql_mode(sql_mode, is_inc);
  }
  return ret;
}

int ObBasicSessionInfo::check_compatibility_mode() const
{
  int ret = OB_SUCCESS;
  ObCompatibilityMode mode = get_compatibility_mode();
  if (ORACLE_MODE == mode && is_oracle_mode()) {
  } else if (MYSQL_MODE == mode && is_mysql_mode()) {
  } else {
    // sometimes mode is OCEANBASE_MODE because mode is not assigned, we can accept that,
    // but at least they must be consistent in oracle mode.
    if (ORACLE_MODE == mode && !is_oracle_mode()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compatibility mode is not match", K(mode), K(is_oracle_mode()), K(lbt()), K(ret));
    }
  }
  return ret;
}

int ObBasicSessionInfo::process_session_compatibility_mode_value(const ObObj& value, bool is_inc)
{
  int ret = OB_SUCCESS;
  ObCompatibilityMode comp_mode = ObCompatibilityMode::OCEANBASE_MODE;
  if (value.is_string_type()) {
    const ObString& comp_mode_str = value.get_string();
    if (comp_mode_str.case_compare("ORACLE")) {
      comp_mode = ObCompatibilityMode::ORACLE_MODE;
    } else if (comp_mode_str.case_compare("MYSQL")) {
      comp_mode = ObCompatibilityMode::MYSQL_MODE;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sql mode", K(ret), K(value), K(comp_mode_str));
    }
  } else if (ObUInt64Type == value.get_type()) {
    comp_mode = static_cast<ObCompatibilityMode>(value.get_uint64());
  } else if (ObIntType == value.get_type()) {
    comp_mode = static_cast<ObCompatibilityMode>(value.get_int());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql mode val type", K(value.get_type()), K(value), K(ret));
  }

  if (OB_SUCC(ret)) {
    set_compatibility_mode(comp_mode, is_inc);
    LOG_DEBUG("set compatibility mode", K(ret), K(comp_mode), K(value), K(get_compatibility_mode()), K(lbt()));
  }
  return ret;
}

int ObBasicSessionInfo::process_session_time_zone_value(const ObObj& value, const bool check_timezone_valid)
{
  int ret = OB_SUCCESS;
  ObString str_val;
  const bool is_oralce_mode = is_oracle_compatible();
  if (OB_FAIL(value.get_string(str_val))) {
    LOG_WARN("fail to get string value", K(value), K(ret));
  } else if (OB_FAIL(set_time_zone(str_val, is_oralce_mode, check_timezone_valid))) {
    LOG_WARN("failed to set time zone",
        K(str_val),
        K(is_oralce_mode),
        "is_oracle_compatible",
        is_oracle_compatible(),
        K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::process_session_overlap_time_value(const ObObj& value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_wrap_.set_error_on_overlap_time(value.get_bool()))) {
    LOG_WARN("fail to set error on overlap time", K(value), K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::process_session_log_level(const ObObj& val)
{
  int ret = OB_SUCCESS;
  int32_t valid_length = 0;
  ObString val_str;
  if (OB_SUCC(val.get_varchar(val_str))) {
    if (0 == val_str.case_compare("disabled")) {
      log_id_level_map_valid_ = false;
      log_id_level_map_.reset_level();
    } else if (OB_FAIL(OB_LOGGER.parse_set(val_str.ptr(), val_str.length(), valid_length, log_id_level_map_))) {
      LOG_WARN("Failed to parse set log_level", K(ret), "log_level", val_str);
    } else {
      log_id_level_map_valid_ = true;
    }
  }
  return ret;
}

int ObBasicSessionInfo::process_session_debug_sync(const ObObj& val, const bool is_global)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id_ && GCONF.is_debug_sync_enabled()) {
    ObString debug_sync;
    if (OB_FAIL(val.get_varchar(debug_sync))) {
      LOG_WARN("varchar expected", K(ret));
    } else {
      if (!debug_sync.empty()) {
        if (OB_FAIL(GDS.add_debug_sync(debug_sync, is_global, debug_sync_actions_))) {
          LOG_WARN("set debug sync string failed", K(debug_sync), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_int64_sys_var(const ObSysVarClassType sys_var_id, int64_t& int64_val) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(sys_var_id, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(sys_var_id));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(sys_var_id));
  } else {
    int64_t int_val = 0;
    if (OB_FAIL(val->get_value().get_int(int_val))) {
      LOG_WARN("fail to get int from value", K(*val), K(ret));
    } else {
      int64_val = int_val;
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_uint64_sys_var(const ObSysVarClassType sys_var_id, uint64_t& uint64_val) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(sys_var_id, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(sys_var_id));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(sys_var_id));
  } else {
    uint64_t uint_val = 0;
    if (OB_FAIL(val->get_value().get_uint64(uint_val))) {
      LOG_ERROR("fail to get uint64 from value", K(*val), K(ret));
    } else {
      uint64_val = uint_val;
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_bool_sys_var(const ObSysVarClassType sys_var_id, bool& bool_val) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(sys_var_id, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(sys_var_id));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(sys_var_id));
  } else {
    int64_t int_val = 0;
    if (OB_SUCCESS != (ret = val->get_value().get_int(int_val))) {
      LOG_ERROR("fail to get int from value", K(*val), K(ret));
    } else {
      bool_val = (0 != int_val);
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_charset_sys_var(const ObSysVarClassType sys_var_id, ObCharsetType& cs_type) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(sys_var_id, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(sys_var_id));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret), K(sys_var_id));
  } else {
    int64_t coll_int64 = 0;
    if (val->get_value().is_null()) {
      cs_type = CHARSET_INVALID;
    } else if (OB_FAIL(val->get_value().get_int(coll_int64))) {
      LOG_ERROR("fail to get int from value", K(*val), K(ret));
    } else if (OB_UNLIKELY(false == ObCharset::is_valid_collation(coll_int64))) {
      LOG_ERROR("invalid collation", K(sys_var_id), K(coll_int64), K(*val));
    } else {
      cs_type = ObCharset::charset_type_by_coll(static_cast<ObCollationType>(coll_int64));
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_collation_sys_var(ObSysVarClassType sys_var_id, ObCollationType& coll_type) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(sys_var_id, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(sys_var_id));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get sys var, but sys var is NULL", K(ret));
  } else {
    int64_t coll_int64 = 0;
    if (OB_FAIL(val->get_value().get_int(coll_int64))) {
      LOG_ERROR("fail to get int from value", K(ret), K(*val));
    } else if (OB_UNLIKELY(false == ObCharset::is_valid_collation(coll_int64))) {
      LOG_ERROR("invalid collation", K(sys_var_id), K(coll_int64), K(*val));
    } else {
      coll_type = static_cast<ObCollationType>(coll_int64);
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_string_sys_var(ObSysVarClassType sys_var_id, ObString& str) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(sys_var_id, val))) {
    LOG_WARN("fail to inner get sys var", K(ret), K(sys_var_id));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get says var, but sys var is NULL", K(ret));
  } else {
    if (OB_FAIL(val->get_value().get_string(str))) {
      LOG_ERROR("fail to get int from value", K(ret), K(*val));
    }
  }
  return ret;
}

int ObBasicSessionInfo::if_aggr_pushdown_allowed(bool& aggr_pushdown_allowed) const
{
  return get_bool_sys_var(SYS_VAR_OB_ENABLE_AGGREGATION_PUSHDOWN, aggr_pushdown_allowed);
}

int ObBasicSessionInfo::is_use_trace_log(bool& use_trace_log) const
{
  use_trace_log = sys_vars_cache_.get_ob_enable_trace_log();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::is_use_transmission_checksum(bool& use_transmission_checksum) const
{
  return get_bool_sys_var(SYS_VAR_OB_ENABLE_TRANSMISSION_CHECKSUM, use_transmission_checksum);
}

int ObBasicSessionInfo::get_name_case_mode(ObNameCaseMode& case_mode) const
{
  int ret = OB_SUCCESS;
  int64_t int64_val = -1;
  if (OB_FAIL(get_sys_variable(SYS_VAR_LOWER_CASE_TABLE_NAMES, int64_val))) {
    LOG_WARN("failed to load sys param", "var_name", OB_SV_LOWER_CASE_TABLE_NAMES, K(ret));
  } else {
    int32_t value = static_cast<int32_t>(int64_val);
    if (value <= OB_NAME_CASE_INVALID || value >= OB_NAME_CASE_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid value", "var_name", OB_SV_LOWER_CASE_TABLE_NAMES, K(value), K(ret));
    } else {
      case_mode = static_cast<ObNameCaseMode>(value);
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_init_connect(ObString& str) const
{
  return get_string_sys_var(SYS_VAR_INIT_CONNECT, str);
}

int ObBasicSessionInfo::is_transformation_enabled(bool& transformation_enabled) const
{
  return get_bool_sys_var(SYS_VAR_OB_ENABLE_TRANSFORMATION, transformation_enabled);
}

int ObBasicSessionInfo::is_select_index_enabled(bool& select_index_enabled) const
{
  return get_bool_sys_var(SYS_VAR_OB_ENABLE_INDEX_DIRECT_SELECT, select_index_enabled);
}

int ObBasicSessionInfo::is_create_table_strict_mode(bool& strict_mode) const
{
  return get_bool_sys_var(SYS_VAR_OB_CREATE_TABLE_STRICT_MODE, strict_mode);
}

void ObBasicSessionInfo::set_autocommit(bool autocommit)
{
  sys_vars_cache_.set_autocommit(autocommit, true);
}

int ObBasicSessionInfo::get_autocommit(bool& autocommit) const
{
  autocommit = sys_vars_cache_.get_autocommit();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_explicit_defaults_for_timestamp(bool& explicit_defaults_for_timestamp) const
{
  return get_bool_sys_var(SYS_VAR_EXPLICIT_DEFAULTS_FOR_TIMESTAMP, explicit_defaults_for_timestamp);
}

int ObBasicSessionInfo::get_sql_auto_is_null(bool& sql_auto_is_null) const
{
  return get_bool_sys_var(SYS_VAR_SQL_AUTO_IS_NULL, sql_auto_is_null);
}

int ObBasicSessionInfo::get_is_result_accurate(bool& is_result_accurate) const
{
  is_result_accurate = sys_vars_cache_.get_is_result_accurate();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_div_precision_increment(int64_t& div_precision_increment) const
{
  int ret = get_int64_sys_var(SYS_VAR_DIV_PRECISION_INCREMENT, div_precision_increment);
  return ret;
}

int ObBasicSessionInfo::get_tx_timeout(int64_t& tx_timeout) const
{
  tx_timeout = sys_vars_cache_.get_ob_trx_timeout();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_query_timeout(int64_t& query_timeout) const
{
  query_timeout = sys_vars_cache_.get_ob_query_timeout();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_sql_throttle_current_priority(int64_t& sql_throttle_current_priority)
{
  sql_throttle_current_priority = sys_vars_cache_.get_sql_throttle_current_priority();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_ob_last_schema_version(int64_t& ob_last_schema_version)
{
  ob_last_schema_version = sys_vars_cache_.get_ob_last_schema_version();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_pl_block_timeout(int64_t& pl_block_timeout) const
{
  pl_block_timeout = sys_vars_cache_.get_ob_pl_block_timeout();
  return OB_SUCCESS;
}

int64_t ObBasicSessionInfo::get_trx_lock_timeout() const
{
  return sys_vars_cache_.get_ob_trx_lock_timeout();
}

int ObBasicSessionInfo::get_binlog_row_image(int64_t& binlog_row_image) const
{
  binlog_row_image = sys_vars_cache_.get_binlog_row_image();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_ob_read_consistency(int64_t& ob_read_consistency) const
{
  return get_int64_sys_var(SYS_VAR_OB_READ_CONSISTENCY, ob_read_consistency);
}

int ObBasicSessionInfo::get_sql_select_limit(int64_t& sql_select_limit) const
{
  sql_select_limit = sys_vars_cache_.get_sql_select_limit();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_ob_stmt_parallel_degree(int64_t& ob_stmt_parallel_degree) const
{
  return get_int64_sys_var(SYS_VAR_OB_STMT_PARALLEL_DEGREE, ob_stmt_parallel_degree);
}

int ObBasicSessionInfo::use_parallel_execution(bool& v) const
{
  v = sys_vars_cache_.get__ob_use_parallel_execution();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_ob_stmt_parallel_degree_min_max(
    int64_t& min_ob_stmt_parallel_degree, int64_t& max_ob_stmt_parallel_degree) const
{
  int ret = OB_SUCCESS;
  ObBasicSysVar* val = NULL;
  if (OB_FAIL(inner_get_sys_var(SYS_VAR_OB_STMT_PARALLEL_DEGREE, val))) {
    LOG_WARN("fail to inner get sys var", K(ret));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("succ to inner get sys var, but sys var is NULL", K(ret));
  } else if (OB_FAIL(val->get_min_val().get_int(min_ob_stmt_parallel_degree))) {
    LOG_WARN("fail to get int from value", K(*val), K(ret));
  } else if (OB_FAIL(val->get_max_val().get_int(max_ob_stmt_parallel_degree))) {
    LOG_WARN("fail to get int from value", K(*val), K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::get_group_concat_max_len(uint64_t& group_concat_max_len) const
{
  return get_uint64_sys_var(SYS_VAR_GROUP_CONCAT_MAX_LEN, group_concat_max_len);
}

int ObBasicSessionInfo::get_ob_interm_result_mem_limit(int64_t& ob_interm_result_mem_limit) const
{
  return get_int64_sys_var(SYS_VAR_OB_INTERM_RESULT_MEM_LIMIT, ob_interm_result_mem_limit);
}

int ObBasicSessionInfo::get_ob_org_cluster_id(int64_t& ob_org_cluster_id) const
{
  ob_org_cluster_id = sys_vars_cache_.get_ob_org_cluster_id();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_max_allowed_packet(int64_t& max_allowed_pkt) const
{
  return get_int64_sys_var(SYS_VAR_MAX_ALLOWED_PACKET, max_allowed_pkt);
}

int ObBasicSessionInfo::get_net_buffer_length(int64_t& net_buffer_len) const
{
  return get_int64_sys_var(SYS_VAR_NET_BUFFER_LENGTH, net_buffer_len);
}

////////////////////////////////////////////////////////////////
int ObBasicSessionInfo::replace_user_variables(const ObSessionValMap& user_var_map)
{
  int ret = OB_SUCCESS;
  if (user_var_map.size() > 0) {
    const sql::ObSessionValMap::VarNameValMap& new_map = user_var_map.get_val_map();
    for (sql::ObSessionValMap::VarNameValMap::const_iterator iter = new_map.begin();
         OB_SUCC(ret) && iter != new_map.end();
         iter++) {
      const common::ObString& key = iter->first;
      const sql::ObSessionVariable& value = iter->second;
      if (OB_FAIL(replace_user_variable(key, value))) {
        LOG_WARN("fail to replace user var", K(ret), K(key), K(value));
      }
    }
  }
  return ret;
}

int ObBasicSessionInfo::replace_user_variable(const ObString& var, const ObSessionVariable& val)
{
  int ret = OB_SUCCESS;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid var name", K(var), K(ret));
  } else if (OB_FAIL(user_var_val_map_.set_refactored(var, val))) {
    LOG_ERROR("fail to add variable", K(var), K(ret));
  } else {
    if (is_track_session_info()) {
      if (OB_FAIL(track_user_var(var))) {
        LOG_WARN("fail to track user var", K(var), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      variables_last_modify_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObBasicSessionInfo::remove_user_variable(const ObString& var)
{
  int ret = OB_SUCCESS;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else if (OB_SUCCESS != user_var_val_map_.erase_refactored(var)) {
    ret = OB_ERR_USER_VARIABLE_UNKNOWN;
    LOG_WARN("unknown variable", K(var), K(ret));
  } else {
    variables_last_modify_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObBasicSessionInfo::get_user_variable(const ObString& var, ObSessionVariable& val) const
{
  int ret = OB_SUCCESS;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else if (OB_SUCCESS != user_var_val_map_.get_refactored(var, val)) {
    ret = OB_ERR_USER_VARIABLE_UNKNOWN;
    LOG_WARN("unknown user variable", K(var), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::get_user_variable_value(const ObString& var, common::ObObj& val) const
{
  int ret = OB_SUCCESS;
  ObSessionVariable sess_var;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else if (OB_SUCCESS != user_var_val_map_.get_refactored(var, sess_var)) {
    ret = OB_ERR_USER_VARIABLE_UNKNOWN;
    LOG_WARN("unknown user variable", K(var), K(ret));
  } else {
    val = sess_var.value_;
  }
  return ret;
}

int ObBasicSessionInfo::get_user_variable_meta(const ObString& var, ObObjMeta& meta) const
{
  int ret = OB_SUCCESS;
  ObSessionVariable val;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else if (OB_SUCCESS != user_var_val_map_.get_refactored(var, val)) {
    ret = OB_ERR_USER_VARIABLE_UNKNOWN;
    LOG_WARN("unknown user variable", K(var), K(ret));
  } else {
    meta = val.meta_;
  }
  return ret;
}

bool ObBasicSessionInfo::user_variable_exists(const ObString& var) const
{
  ObSessionVariable val;
  bool exist = false;
  int ret = OB_SUCCESS;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else {
    exist = (OB_SUCCESS == user_var_val_map_.get_refactored(var, val));
  }
  return exist;
}

const ObSessionVariable* ObBasicSessionInfo::get_user_variable(const common::ObString& var) const
{
  const ObSessionVariable* sess_var = NULL;
  int ret = OB_SUCCESS;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else {
    sess_var = user_var_val_map_.get(var);
  }
  return sess_var;
}

const common::ObObj* ObBasicSessionInfo::get_user_variable_value(const ObString& var) const
{
  const ObObj* val_obj = NULL;
  const ObSessionVariable* sess_var = NULL;
  int ret = OB_SUCCESS;
  if (var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid variable name", K(var), K(ret));
  } else if (NULL != (sess_var = user_var_val_map_.get(var))) {
    val_obj = &(sess_var->value_);
  } else {
  }  // just return NULL
  return val_obj;
}

////////////////////////////////////////////////////////////////
int64_t ObBasicSessionInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TENANT,
      get_tenant_name(),
      N_EFFECTIVE_TENANT,
      get_effective_tenant_name(),
      N_DATABASE,
      get_database_name(),
      N_USER,
      (lib::is_oracle_mode() ? get_user_name() : get_user_at_host()),
      "consistency_level",
      consistency_level_,
      "consistency_type",
      trans_consistency_type_,
      "safe_weak_snapshot_version",
      safe_weak_read_snapshot_,
      "weak_read_snapshot_source",
      weak_read_snapshot_source_,
      "weak_read_snapshot_source_str",
      source_to_string(weak_read_snapshot_source_));
  return pos;
}

int ObBasicSessionInfo::calc_need_serialize_vars(
    ObIArray<ObSysVarClassType>& sys_var_ids, ObIArray<ObString>& user_var_names, int64_t ser_version) const
{
  int ret = OB_SUCCESS;
  sys_var_ids.reset();
  user_var_names.reset();
  if (ser_version == CUR_SER_VERSION) {
    if (OB_FAIL(sys_var_ids.assign(sys_var_inc_info_.get_all_sys_var_ids()))) {
      LOG_WARN("fail to assign ser_sys_var_ids", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ObSysVariables::get_all_sys_var_count(); ++i) {
      bool can_serialize = true;
      if (exec_min_cluster_version_ < CLUSTER_VERSION_2230) {
        can_serialize = true;
      } else {
        can_serialize = !(is_mysql_mode() && (ObSysVariables::get_flags(i) & ObSysVarFlag::ORACLE_ONLY));
      }
      if (((ObSysVariables::get_flags(i) & ObSysVarFlag::NEED_SERIALIZE) ||
              (ObSysVariables::get_flags(i) & ObSysVarFlag::QUERY_SENSITIVE)) &&
          can_serialize) {
        if (OB_FAIL(sys_var_ids.push_back(ObSysVariables::get_sys_var_id(i)))) {
          LOG_WARN("fail to push back sys var id", K(i), K(ObSysVariables::get_sys_var_id(i)), K(sys_var_ids), K(ret));
        }
      }
    }
  }

  CK(OB_NOT_NULL(cur_phy_plan_));
  if (OB_SUCC(ret)) {
    // process user variables and system variables that need to be serialized for current query.
    const ObIArray<ObVarInfo>& extra_serialize_vars = cur_phy_plan_->get_vars();
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_serialize_vars.count(); ++i) {
      const ObVarInfo& var_info = extra_serialize_vars.at(i);
      if (USER_VAR == var_info.type_) {
        if (OB_FAIL(user_var_names.push_back(var_info.name_))) {
          LOG_WARN("fail to push user var name", K(var_info), K(user_var_names), K(ret));
        }
      } else if (SYS_VAR == var_info.type_) {
        ObSysVarClassType sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(var_info.name_);
        if (SYS_VAR_INVALID == sys_var_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid sys var id", K(sys_var_id), K(var_info), K(ret));
        } else {
          bool sys_var_exist = false;
          for (int64_t j = 0; OB_SUCC(ret) && !sys_var_exist && j < sys_var_ids.count(); ++j) {
            if (sys_var_id == sys_var_ids.at(j)) {
              sys_var_exist = true;
            }
          }
          if (OB_SUCCESS == ret && !sys_var_exist) {
            if (OB_FAIL(sys_var_ids.push_back(sys_var_id))) {
              LOG_WARN("fail to push back sys var id", K(sys_var_id), K(var_info), K(sys_var_ids), K(ret));
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid var info type", K(var_info.type_), K(var_info), K(ret));
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBasicSessionInfo::TableStmtType, table_id_, stmt_type_);

OB_DEF_SERIALIZE(ObBasicSessionInfo::SysVarsCacheData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      autocommit_,
      ob_enable_trace_log_,
      ob_org_cluster_id_,
      ob_query_timeout_,
      ob_trx_timeout_,
      collation_connection_,
      sql_mode_,
      ob_trx_idle_timeout_,
      nls_collation_,
      nls_nation_collation_,
      ob_enable_sql_audit_,
      nls_length_semantics_,
      nls_formats_[NLS_DATE],
      nls_formats_[NLS_TIMESTAMP],
      nls_formats_[NLS_TIMESTAMP_TZ],
      ob_trx_lock_timeout_,
      ob_trace_info_);
  return ret;
}

OB_DEF_DESERIALIZE(ObBasicSessionInfo::SysVarsCacheData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      autocommit_,
      ob_enable_trace_log_,
      ob_org_cluster_id_,
      ob_query_timeout_,
      ob_trx_timeout_,
      collation_connection_,
      sql_mode_,
      ob_trx_idle_timeout_,
      nls_collation_,
      nls_nation_collation_,
      ob_enable_sql_audit_,
      nls_length_semantics_,
      nls_formats_[NLS_DATE],
      nls_formats_[NLS_TIMESTAMP],
      nls_formats_[NLS_TIMESTAMP_TZ],
      ob_trx_lock_timeout_,
      ob_trace_info_);
  set_nls_date_format(nls_formats_[NLS_DATE]);
  set_nls_timestamp_format(nls_formats_[NLS_TIMESTAMP]);
  set_nls_timestamp_tz_format(nls_formats_[NLS_TIMESTAMP_TZ]);
  set_ob_trace_info(ob_trace_info_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasicSessionInfo::SysVarsCacheData)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      autocommit_,
      ob_enable_trace_log_,
      ob_org_cluster_id_,
      ob_query_timeout_,
      ob_trx_timeout_,
      collation_connection_,
      sql_mode_,
      ob_trx_idle_timeout_,
      nls_collation_,
      nls_nation_collation_,
      ob_enable_sql_audit_,
      nls_length_semantics_,
      nls_formats_[NLS_DATE],
      nls_formats_[NLS_TIMESTAMP],
      nls_formats_[NLS_TIMESTAMP_TZ],
      ob_trx_lock_timeout_,
      ob_trace_info_);
  return len;
}

int64_t ObBasicSessionInfo::get_ser_version(uint64_t tenant_id, uint64_t eff_tenant_id) const
{
  return (GET_UNIS_CLUSTER_VERSION() < CLUSTER_VERSION_2250 || tenant_id == OB_SYS_TENANT_ID ||
             eff_tenant_id == OB_SYS_TENANT_ID)
             ? PRE_SER_VERSION
             : CUR_SER_VERSION;
}

OB_DEF_SERIALIZE(ObBasicSessionInfo)
{
  int ret = OB_SUCCESS;
  int64_t ser_version = get_ser_version(tenant_id_, effective_tenant_id_);
  ObTimeZoneInfo tmp_tz_info;  // for compatibility with old version
  bool in_transaction = trans_flags_.is_in_transaction();
  // To be compatible with old version which store sql_mode and compatibility mode in ObSQLModeManager;
  int64_t compatibility_mode_index = 0;
  if (OB_FAIL(compatibility_mode2index(get_compatibility_mode(), compatibility_mode_index))) {
    LOG_WARN("convert compatibility mode to index failed", K(ret));
  }

  LST_DO_CODE(OB_UNIS_ENCODE,
      in_transaction,
      trans_desc_,
      consistency_level_,
      compatibility_mode_index,
      tmp_tz_info,
      // NOTE: rpc_tenant_id may cause compatability problem,
      // But only in diagnose tenant, so keep the stupid hack as it is.
      tenant_id_ | (rpc_tenant_id_ << 32),
      effective_tenant_id_,
      is_changed_to_temp_tenant_,
      user_id_,
      is_master_session() ? sessid_ : master_sessid_,
      capability_.capability_,
      thread_data_.database_name_);
  // serialize user variables and system variables.
  ObSEArray<ObSysVarClassType, 128> sys_var_ids;
  ObSEArray<ObString, 32> user_var_names;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_need_serialize_vars(sys_var_ids, user_var_names, ser_version))) {
    LOG_WARN("fail to calc need serialize vars", K(ret));
  } else {
    ObSEArray<std::pair<ObString, ObSessionVariable>, 32> actual_ser_user_vars;
    ObSessionVariable user_var_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < user_var_names.count(); ++i) {
      user_var_val.reset();
      const ObString& user_var_name = user_var_names.at(i);
      ret = user_var_val_map_.get_refactored(user_var_name, user_var_val);
      if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get user var from session user var map",
            K(i),
            K(user_var_name),
            K(user_var_val_map_.size()),
            K(ret));
      } else {
        if (OB_SUCCESS == ret && OB_FAIL(actual_ser_user_vars.push_back(std::make_pair(user_var_name, user_var_val)))) {
          LOG_WARN("fail to push back pair(user_var_name, user_var_val)",
              K(buf_len),
              K(pos),
              K(user_var_name),
              K(user_var_val),
              K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, actual_ser_user_vars.count()))) {
        LOG_WARN("fail to serialize user var count", K(ret), K(buf_len), K(pos), K(actual_ser_user_vars.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < actual_ser_user_vars.count(); ++i) {
          const ObString& user_var_name = actual_ser_user_vars.at(i).first;
          const ObSessionVariable& user_var_val = actual_ser_user_vars.at(i).second;
          if (OB_FAIL(serialization::encode(buf, buf_len, pos, user_var_name))) {
            LOG_WARN("fail to serialize user var name", K(buf_len), K(pos), K(user_var_name), K(ret));
          } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, user_var_val.meta_))) {
            LOG_WARN("fail to serialize user var val meta", K(buf_len), K(pos), K(user_var_val.meta_), K(ret));
          } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, user_var_val.value_))) {
            LOG_WARN("fail to serialize user var val value", K(buf_len), K(pos), K(user_var_val.value_), K(ret));
          } else {
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ser_version == CUR_SER_VERSION) {
      OB_UNIS_ENCODE(sys_var_base_version_);
      OB_UNIS_ENCODE(sys_var_inc_info_);
    }
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, sys_var_ids.count()))) {
      LOG_WARN("fail to serialize sys var count", K(buf_len), K(pos), K(sys_var_ids.count()), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_var_ids.count(); ++i) {
      int64_t sys_var_idx = -1;
      if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_ids.at(i), sys_var_idx))) {
        LOG_WARN("fail to calc sys var store idx", K(i), K(sys_var_idx), K(sys_var_ids.at(i)), K(ret));
      } else if (sys_var_idx < 0 || get_sys_var_count() <= sys_var_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sys var idx is invalid", K(sys_var_idx), K(get_sys_var_count()), K(ret));
      } else if (OB_ISNULL(sys_vars_[sys_var_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sys var is NULL", K(ret), K(sys_var_idx), K(get_sys_var_count()));
      } else {
        int16_t sys_var_id = static_cast<int16_t>(sys_vars_[sys_var_idx]->get_type());
        if (OB_FAIL(serialization::encode(buf, buf_len, pos, sys_var_id))) {
          LOG_ERROR("fail to serialize sys var id", K(buf_len), K(pos), K(sys_var_id), K(ret));
        } else if (OB_FAIL(sys_vars_[sys_var_idx]->serialize(buf, buf_len, pos))) {
          LOG_ERROR("fail to serialize sys var",
              K(buf_len),
              K(pos),
              K(i),
              K(sys_var_idx),
              K(*sys_vars_[sys_var_idx]),
              K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to encode session info", K(ret));
  } else {
    bool tx_read_only = get_tx_read_only();
    if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, tx_read_only))) {
      LOG_WARN("fail to encode tx_read_only", K(ret));
    }
  }

  // for compatibility with old version
  bool unused_literal_query = false;
  int64_t unused_inner_safe_weak_read_snapshot = safe_weak_read_snapshot_;

  bool need_serial_exec = trans_flags_.need_serial_exec();
  LST_DO_CODE(OB_UNIS_ENCODE,
      sys_vars_cache_.inc_data_,
      trans_consistency_type_,
      safe_weak_read_snapshot_,
      unused_inner_safe_weak_read_snapshot,
      unused_literal_query,
      tz_info_wrap_,
      app_trace_id_,
      proxy_capability_.capability_,
      client_mode_,
      proxy_sessid_,
      nested_count_,
      thread_data_.user_name_,
      next_tx_isolation_,
      read_snapshot_version_,
      check_sys_variable_,
      weak_read_snapshot_source_,
      database_id_,
      thread_data_.user_at_host_name_,
      thread_data_.user_at_client_ip_,
      current_execution_id_,
      total_stmt_tables_,
      cur_stmt_tables_,
      is_foreign_key_cascade_,
      sys_var_in_pc_str_,
      is_foreign_key_check_exist_,
      need_serial_exec);
  return ret;
}

OB_DEF_DESERIALIZE(ObBasicSessionInfo)
{
  int ret = OB_SUCCESS;
  ObTimeZoneInfo tmp_tz_info;  // for compatibility with old version
  bool in_transaction = false;
  int64_t compatibility_mode_index = 0;
  is_deserialized_ = true;
  LST_DO_CODE(OB_UNIS_DECODE,
      in_transaction,
      trans_desc_,
      consistency_level_,
      compatibility_mode_index,
      tmp_tz_info,
      tenant_id_,
      effective_tenant_id_,
      is_changed_to_temp_tenant_,
      user_id_,
      master_sessid_,
      capability_.capability_,
      thread_data_.database_name_);
  int64_t ser_version = get_ser_version(tenant_id_, effective_tenant_id_);
  trans_flags_.set_is_in_transaction(in_transaction);
  rpc_tenant_id_ = (tenant_id_ >> 32);
  tenant_id_ = (tenant_id_ << 32 >> 32);

  // deserialize user variables and system variables.
  int64_t deserialize_user_var_count = 0;
  int64_t deserialize_sys_var_count = 0;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, deserialize_user_var_count))) {
    LOG_WARN("fail to deserialize user var count", K(data_len), K(pos), K(ret));
  } else {
    ObSessionVariable user_var_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < deserialize_user_var_count; ++i) {
      ObString user_var_name;
      user_var_val.reset();
      if (OB_FAIL(serialization::decode(buf, data_len, pos, user_var_name))) {
        LOG_WARN("fail to deserialize user var name", K(i), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode(buf, data_len, pos, user_var_val.meta_))) {
        LOG_WARN("fail to deserialize user var val meta", K(i), K(data_len), K(pos), K(ret));
      } else if (OB_FAIL(serialization::decode(buf, data_len, pos, user_var_val.value_))) {
        LOG_WARN("fail to deserialize user var val value", K(i), K(data_len), K(pos), K(ret));
      } else {
        if (OB_FAIL(user_var_val_map_.set_refactored(user_var_name, user_var_val))) {
          LOG_WARN("Insert value into map failed", K(user_var_name), K(user_var_val), K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  if (ser_version == CUR_SER_VERSION) {
    OB_UNIS_DECODE(des_sys_var_base_version_);
    OB_UNIS_DECODE(sys_var_inc_info_);
    OV(des_sys_var_base_version_ > OB_INVALID_VERSION);
    if (des_sys_var_base_version_ == sys_var_base_version_) {
      // skip.
    } else if (des_sys_var_base_version_ > 0) {
      ObSchemaGetterGuard schema_guard;
      OZ(GCTX.schema_service_->get_tenant_schema_guard(effective_tenant_id_, schema_guard, des_sys_var_base_version_));
      OZ(load_all_sys_vars(schema_guard));
      if (OB_FAIL(ret) && is_schema_error(ret)) {
        ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
      }
    } else {
      OZ(load_all_sys_vars_default());
    }
  }
  if (OB_SUCC(ret)) {
    ObTZMapWrap tz_map_wrap;
    if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id_, tz_map_wrap))) {
      LOG_WARN("get tenant timezone map failed", K(ret));
    } else {
      tz_info_wrap_.set_tz_info_map(tz_map_wrap.get_tz_map());
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode(buf, data_len, pos, deserialize_sys_var_count))) {
      LOG_WARN("fail to deserialize sys var count", K(data_len), K(pos), K(ret));
    } else {
      const bool check_timezone_valid = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < deserialize_sys_var_count; ++i) {
        ObObj tmp_val;
        ObBasicSysVar* sys_var = NULL;
        ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
        int16_t tmp_sys_var_id = -1;
        int64_t store_idx = -1;
        if (OB_FAIL(serialization::decode(buf, data_len, pos, tmp_sys_var_id))) {
          LOG_WARN("fail to deserialize sys var id", K(data_len), K(pos), K(ret));
        } else if (FALSE_IT(sys_var_id = static_cast<ObSysVarClassType>(tmp_sys_var_id))) {
        } else if (ser_version == PRE_SER_VERSION && OB_FAIL(sys_var_inc_info_.add_sys_var_id(sys_var_id))) {
          LOG_WARN("fail to add sys var id", K(sys_var_id), K(ret));
        } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, store_idx))) {
          if (OB_SYS_VARS_MAYBE_DIFF_VERSION == ret) {
            // observer of higher version serialized some system variables which are
            // not supported in current version, just skip this data and continue.
            ret = OB_SUCCESS;
            int64_t sys_var_version = 0;
            int64_t sys_var_len = 0;
            OB_UNIS_DECODEx(sys_var_version);
            OB_UNIS_DECODEx(sys_var_len);
            if (OB_SUCC(ret)) {
              pos += sys_var_len;
              LOG_WARN("invalid sys var id, maybe version is different, skip it", K(sys_var_id));
            }
          } else {
            LOG_ERROR("invalid sys var id", K(sys_var_id), K(ret));
          }
        } else if (OB_FAIL(create_sys_var(sys_var_id, store_idx, sys_var))) {
          LOG_WARN("fail to create sys var", K(sys_var_id), K(ret));
        } else if (OB_ISNULL(sys_var)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create sys var is NULL", K(ret));
        } else if (OB_FAIL(sys_var->deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize sys var", K(data_len), K(pos), K(sys_var_id), K(ret));
        } else if (OB_FAIL(deep_copy_sys_variable(sys_var_id, sys_var->get_value(), &tmp_val))) {
          LOG_WARN("fail to update system variable", K(sys_var_id), K(sys_var->get_value()), K(ret));
        } else if (FALSE_IT(sys_var->set_value(tmp_val))) {
        } else if (OB_FAIL(process_session_variable(sys_var_id, sys_var->get_value(), true, check_timezone_valid))) {
          LOG_ERROR("process system variable error", K(ret), K(*sys_var));
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool tx_read_only = false;
      if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &tx_read_only))) {
        LOG_WARN("fail to decode tx_read_only", K(ret));
      } else {
        // direct assignment because in this case we won't track trans_specified_status
        next_tx_read_only_ = tx_read_only;
      }
    }
  }

  int64_t unused_inner_safe_weak_read_snapshot = 0;
  bool unused_literal_query = false;
  bool need_serial_exec = false;

  // sys_var_in_pc_str_ may be set when deserialize system variables,
  // so reset it before deserialization of itself.
  sys_var_in_pc_str_.reset();
  const ObTZInfoMap* tz_info_map = tz_info_wrap_.get_tz_info_offset().get_tz_info_map();
  LST_DO_CODE(OB_UNIS_DECODE,
      sys_vars_cache_.inc_data_,
      trans_consistency_type_,
      safe_weak_read_snapshot_,
      unused_inner_safe_weak_read_snapshot,
      unused_literal_query,
      tz_info_wrap_,
      app_trace_id_,
      proxy_capability_.capability_,
      client_mode_,
      proxy_sessid_,
      nested_count_,
      thread_data_.user_name_,
      next_tx_isolation_,
      read_snapshot_version_,
      check_sys_variable_,
      weak_read_snapshot_source_,
      database_id_,
      thread_data_.user_at_host_name_,
      thread_data_.user_at_client_ip_,
      current_execution_id_,
      total_stmt_tables_,
      cur_stmt_tables_,
      is_foreign_key_cascade_,
      sys_var_in_pc_str_,
      is_foreign_key_check_exist_,
      need_serial_exec);
  trans_flags_.set_need_serial_exec(need_serial_exec);
  is_deserialized_ = true;
  tz_info_wrap_.set_tz_info_map(tz_info_map);
  if (OB_SUCC(ret)) {
    if (unused_inner_safe_weak_read_snapshot > safe_weak_read_snapshot_) {
      LOG_INFO("ObBasicSessionInfo::deserialize: use inner safe weak read snapshot, "
               "which comes from cluster before 2.2",
          K(unused_inner_safe_weak_read_snapshot),
          K(safe_weak_read_snapshot_),
          K(*this));
      safe_weak_read_snapshot_ = unused_inner_safe_weak_read_snapshot;
    }
  }

  if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() < CLUSTER_CURRENT_VERSION) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sys_var_count(); ++i) {
      if ((ObSysVariables::get_flags(i) & ObSysVarFlag::NEED_SERIALIZE) && OB_ISNULL(sys_vars_[i])) {
        if (OB_FAIL(load_default_sys_variable(i))) {
          LOG_WARN("fail to load default sys variable", K(ret), K(i));
        }
      }
    }
  }
  release_to_pool_ = OB_SUCC(ret);
  return ret;
}

OB_DEF_SERIALIZE(ObBasicSessionInfo::SysVarIncInfo)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(all_sys_var_ids_);
  return ret;
}

OB_DEF_DESERIALIZE(ObBasicSessionInfo::SysVarIncInfo)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(all_sys_var_ids_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasicSessionInfo::SysVarIncInfo)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(all_sys_var_ids_);
  return len;
}

ObBasicSessionInfo::SysVarIncInfo::SysVarIncInfo() : all_sys_var_ids_()
{}

ObBasicSessionInfo::SysVarIncInfo::~SysVarIncInfo()
{}

int ObBasicSessionInfo::SysVarIncInfo::add_sys_var_id(ObSysVarClassType sys_var_id)
{
  int ret = OB_SUCCESS;
  OZ(add_var_to_array_no_dup(all_sys_var_ids_, sys_var_id), all_sys_var_ids_, sys_var_id);
  return ret;
}

int ObBasicSessionInfo::SysVarIncInfo::get_des_sys_var_id(int64_t idx, ObSysVarClassType& sys_var_id) const
{
  int ret = OB_SUCCESS;
  OV(0 <= idx && idx < all_sys_var_ids_.count());
  OX(sys_var_id = all_sys_var_ids_.at(idx));
  return ret;
}

bool ObBasicSessionInfo::SysVarIncInfo::all_has_sys_var_id(ObSysVarClassType sys_var_id) const
{
  return has_exist_in_array(all_sys_var_ids_, sys_var_id);
}

int64_t ObBasicSessionInfo::SysVarIncInfo::all_count() const
{
  return all_sys_var_ids_.count();
}

const ObBasicSessionInfo::SysVarIds& ObBasicSessionInfo::SysVarIncInfo::get_all_sys_var_ids() const
{
  return all_sys_var_ids_;
}

int ObBasicSessionInfo::SysVarIncInfo::assign(const SysVarIncInfo& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    OZ(all_sys_var_ids_.assign(other.all_sys_var_ids_));
  }
  return ret;
}

int ObBasicSessionInfo::SysVarIncInfo::reset()
{
  int ret = OB_SUCCESS;
  OX(all_sys_var_ids_.reset());
  return ret;
}

int ObBasicSessionInfo::load_all_sys_vars_default()
{
  int ret = OB_SUCCESS;
  OZ(clean_all_sys_vars());
  OZ(load_default_sys_variable(false, false));
  return ret;
}

int ObBasicSessionInfo::clean_all_sys_vars()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_sys_var_count(); ++i) {
    if (OB_NOT_NULL(sys_vars_[i])) {
      OX(sys_vars_[i]->clean_value());
    }
  }
  OX(base_sys_var_alloc_.reset());
  return ret;
}

int ObBasicSessionInfo::load_all_sys_vars(ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const ObSysVariableSchema* sys_var_schema = NULL;
  OZ(schema_guard.get_sys_variable_schema(effective_tenant_id_, sys_var_schema));
  OV(OB_NOT_NULL(sys_var_schema));
  OZ(load_all_sys_vars(*sys_var_schema));
  return ret;
}

int ObBasicSessionInfo::load_all_sys_vars(const ObSysVariableSchema& sys_var_schema)
{
  int ret = OB_SUCCESS;
  if (sys_var_base_version_ == sys_var_schema.get_schema_version()) {
    OZ(process_session_variable_fast());
  } else {
    OZ(clean_all_sys_vars());
    OX(influence_plan_var_indexs_.reset());
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sys_var_count(); i++) {
      ObSysVarClassType sys_var_id = ObSysVariables::get_sys_var_id(i);
      const ObSysVarSchema* sys_var = NULL;
      OZ(sys_var_schema.get_sysvar_schema(sys_var_id, sys_var));
      OV(OB_NOT_NULL(sys_var));
      OZ(load_sys_variable(sys_var->get_name(),
          sys_var->get_data_type(),
          sys_var->get_value(),
          sys_var->get_min_val(),
          sys_var->get_max_val(),
          sys_var->get_flags(),
          true));
      if (OB_NOT_NULL(sys_vars_[i]) && sys_vars_[i]->is_influence_plan()) {
        OZ(influence_plan_var_indexs_.push_back(i));
      }
    }
    OX(sys_var_base_version_ = sys_var_schema.get_schema_version());
  }
  if (!is_deserialized_) {
    OZ(gen_sys_var_in_pc_str());
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasicSessionInfo)
{
  int64_t len = 0;
  int64_t ser_version = get_ser_version(tenant_id_, effective_tenant_id_);
  ObTimeZoneInfo tmp_tz_info;  // for compatibility with old version
  int ret = OB_SUCCESS;
  bool in_transaction = trans_flags_.is_in_transaction();
  // To be compatible with old version which store sql_mode and compatibility mode in ObSQLModeManager;
  int64_t compatibility_mode_index = 0;
  if (OB_FAIL(compatibility_mode2index(get_compatibility_mode(), compatibility_mode_index))) {
    LOG_WARN("convert compatibility mode to index failed", K(ret));
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      in_transaction,
      trans_desc_,
      consistency_level_,
      compatibility_mode_index,
      tmp_tz_info,
      tenant_id_ | (rpc_tenant_id_ << 32),
      effective_tenant_id_,
      is_changed_to_temp_tenant_,
      user_id_,
      is_master_session() ? sessid_ : master_sessid_,
      capability_.capability_,
      thread_data_.database_name_);

  // calc the length of user variables and system variables.
  ObSEArray<ObSysVarClassType, 128> sys_var_ids;
  ObSEArray<ObString, 32> user_var_names;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_need_serialize_vars(sys_var_ids, user_var_names, ser_version))) {
    LOG_WARN("fail to calc need serialize vars", K(ret));
  } else {
    int64_t actual_ser_user_var_count = 0;
    ObSessionVariable user_var_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < user_var_names.count(); ++i) {
      user_var_val.reset();
      const ObString& user_var_name = user_var_names.at(i);
      ret = user_var_val_map_.get_refactored(user_var_name, user_var_val);
      if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get user var from session user var map",
            K(i),
            K(user_var_name),
            K(user_var_val_map_.size()),
            K(ret));
      } else {
        if (OB_SUCCESS == ret) {
          actual_ser_user_var_count++;
          len += serialization::encoded_length(user_var_name);
          len += serialization::encoded_length(user_var_val.meta_);
          len += serialization::encoded_length(user_var_val.value_);
        }
        ret = OB_SUCCESS;
      }
    }
    len += serialization::encoded_length(actual_ser_user_var_count);
  }
  if (OB_SUCC(ret)) {
    if (ser_version == CUR_SER_VERSION) {
      OB_UNIS_ADD_LEN(sys_var_base_version_);
      OB_UNIS_ADD_LEN(sys_var_inc_info_);
    }
    len += serialization::encoded_length(sys_var_ids.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_var_ids.count(); ++i) {
      int64_t sys_var_idx = -1;
      ObSysVarClassType& type = sys_var_ids.at(i);
      if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(type, sys_var_idx))) {
        LOG_WARN("fail to calc sys var store idx", K(i), K(sys_var_idx), K(type), K(ret));
      } else if (sys_var_idx < 0 || get_sys_var_count() <= sys_var_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sys var idx is invalid", K(sys_var_idx), K(get_sys_var_count()), K(ret));
      } else if (OB_ISNULL(sys_vars_[sys_var_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sys var is NULL", K(ret), K(sys_var_idx), K(get_sys_var_count()));
      } else {
        int16_t sys_var_id = static_cast<int16_t>(sys_vars_[sys_var_idx]->get_type());
        len += serialization::encoded_length(sys_var_id);
        len += sys_vars_[sys_var_idx]->get_serialize_size();
      }
    }
  }
  if (OB_SUCC(ret)) {
    bool tx_read_only = get_tx_read_only();
    len += serialization::encoded_length_bool(tx_read_only);
  }
  bool unused_literal_query = false;
  int64_t unused_inner_safe_weak_read_snapshot = safe_weak_read_snapshot_;
  bool need_serial_exec = trans_flags_.need_serial_exec();

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      sys_vars_cache_.inc_data_,
      trans_consistency_type_,
      safe_weak_read_snapshot_,
      unused_inner_safe_weak_read_snapshot,
      unused_literal_query,
      tz_info_wrap_,
      app_trace_id_,
      proxy_capability_.capability_,
      client_mode_,
      proxy_sessid_,
      nested_count_,
      thread_data_.user_name_,
      next_tx_isolation_,
      read_snapshot_version_,
      check_sys_variable_,
      weak_read_snapshot_source_,
      database_id_,
      thread_data_.user_at_host_name_,
      thread_data_.user_at_client_ip_,
      current_execution_id_,
      total_stmt_tables_,
      cur_stmt_tables_,
      is_foreign_key_cascade_,
      sys_var_in_pc_str_,
      is_foreign_key_check_exist_,
      need_serial_exec);
  return len;
}

////////////////////////////////////////////////////////////////
void ObBasicSessionInfo::reset_session_changed_info()
{
  changed_sys_vars_.reset();
  changed_user_vars_.reset();
  is_database_changed_ = false;
  changed_var_pool_.reset();
  feedback_manager_.reset();
}

bool ObBasicSessionInfo::is_already_tracked(
    const ObSysVarClassType& sys_var_id, const ObIArray<ChangedVar>& array) const
{
  bool found = false;
  for (int64_t i = 0; !found && i < array.count(); ++i) {
    if (array.at(i).id_ == sys_var_id) {
      found = true;
      break;
    }
  }
  return found;
}

bool ObBasicSessionInfo::is_already_tracked(const ObString& name, const ObIArray<ObString>& array) const
{
  bool found = false;
  for (int64_t i = 0; !found && i < array.count(); ++i) {
    if (array.at(i) == name) {
      found = true;
      break;
    }
  }
  return found;
}

int ObBasicSessionInfo::add_changed_sys_var(
    const ObSysVarClassType& sys_var_id, const ObObj& old_val, ObIArray<ChangedVar>& array)
{
  int ret = OB_SUCCESS;
  ObObj val;
  if (SYS_VAR_INVALID == sys_var_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(sys_var_id), K(old_val), K(ret));
  } else if (OB_FAIL(deep_copy_obj(changed_var_pool_, old_val, val))) {
    LOG_WARN("failed to deep copy system var old value", K(ret), K(old_val));
  } else if (OB_FAIL(array.push_back(ChangedVar(sys_var_id, val)))) {
    LOG_WARN("fail to push back", K(sys_var_id), K(old_val), K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::add_changed_user_var(const ObString& name, common::ObIArray<common::ObString>& array)
{
  return array.push_back(name);
}

int ObBasicSessionInfo::track_sys_var(const ObSysVarClassType& sys_var_id, const ObObj& old_val)
{
  int ret = OB_SUCCESS;
  if (SYS_VAR_INVALID == sys_var_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(sys_var_id), K(ret));
  } else if (!is_already_tracked(sys_var_id, changed_sys_vars_)) {
    if (OB_FAIL(add_changed_sys_var(sys_var_id, old_val, changed_sys_vars_))) {
      LOG_WARN("fail to add changed system var", K(sys_var_id), K(old_val), K(ret));
    } else {
      LOG_DEBUG("add changed var success", K(sys_var_id), K(old_val), K(changed_sys_vars_));
    }
  }
  return ret;
}

int ObBasicSessionInfo::track_user_var(const common::ObString& user_var)
{
  int ret = OB_SUCCESS;
  if (user_var.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(user_var), K(ret));
  } else if (!is_already_tracked(user_var, changed_user_vars_)) {
    ObString name;
    if (OB_FAIL(ob_write_string(changed_var_pool_, user_var, name))) {
      LOG_WARN("fail to write string", K(user_var), K(ret));
    } else if (OB_FAIL(add_changed_user_var(name, changed_user_vars_))) {
      LOG_WARN("fail to add changed user var", K(name), K(ret));
    }
  }
  return ret;
}

int ObBasicSessionInfo::is_sys_var_actully_changed(
    const ObSysVarClassType& sys_var_id, const ObObj& old_val, ObObj& new_val, bool& changed)
{
  int ret = OB_SUCCESS;
  changed = true;
  OZ(get_sys_variable(sys_var_id, new_val));
  if (OB_SUCC(ret)) {
    switch (sys_var_id) {
      case SYS_VAR_AUTO_INCREMENT_INCREMENT:
      case SYS_VAR_AUTO_INCREMENT_OFFSET:
      case SYS_VAR_LAST_INSERT_ID:
      case SYS_VAR_TX_READ_ONLY:
      case SYS_VAR_OB_ENABLE_PLAN_CACHE:
      case SYS_VAR_OB_ENABLE_SQL_AUDIT:
      case SYS_VAR_AUTOCOMMIT:
      case SYS_VAR_OB_ENABLE_TRACE_LOG:
      case SYS_VAR_OB_ORG_CLUSTER_ID:
      case SYS_VAR_OB_QUERY_TIMEOUT:
      case SYS_VAR_OB_TRX_TIMEOUT:
      case SYS_VAR_OB_TRX_IDLE_TIMEOUT:
      case SYS_VAR_COLLATION_CONNECTION:
      case SYS_VAR_OB_PL_BLOCK_TIMEOUT:
      case SYS_VAR_OB_COMPATIBILITY_MODE: {
        changed = old_val.get_meta() == new_val.get_meta() ? old_val != new_val : true;
      } break;
      default: {
        break;
      }
    }
  }
  LOG_DEBUG("is_sys_var_actully_changed", K(sys_var_id), K(changed), K(old_val), K(new_val));
  return ret;
}

int ObBasicSessionInfo::set_partition_hit(const bool is_hit)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_bool(is_hit);
  if (OB_FAIL(update_sys_variable(SYS_VAR_OB_PROXY_PARTITION_HIT, obj))) {
    LOG_WARN("fail to update_system_variable", K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::set_proxy_user_privilege(const int64_t user_priv_set)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_int(user_priv_set);
  if (OB_FAIL(update_sys_variable(SYS_VAR_OB_PROXY_USER_PRIVILEGE, obj))) {
    LOG_WARN("fail to update_system_variable", K(SYS_VAR_OB_PROXY_USER_PRIVILEGE), K(user_priv_set), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::update_last_trace_id(const ObCurTraceId::TraceId& trace_id)
{
  int ret = OB_SUCCESS;
  const int64_t TEMP_STR_BUF_LEN = 128;
  // 128 is enough to tracd_id, or will be truncated
  char tmp_str_buf[TEMP_STR_BUF_LEN];  // no need init
  int64_t pos = trace_id.to_string(tmp_str_buf, TEMP_STR_BUF_LEN);
  ObString trace_id_str(static_cast<ObString::obstr_size_t>(pos), tmp_str_buf);

  ObObj obj;
  obj.set_string(ObVarcharType, trace_id_str);
  if (OB_FAIL(update_sys_variable(SYS_VAR_OB_STATEMENT_TRACE_ID, obj))) {
    LOG_WARN("fail to update_system_variable", K(SYS_VAR_OB_STATEMENT_TRACE_ID), K(trace_id_str), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::set_proxy_capability(const uint64_t cap)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_int(cap);
  if (OB_FAIL(update_sys_variable(SYS_VAR_OB_CAPABILITY_FLAG, obj))) {
    LOG_WARN("fail to update_system_variable", K(SYS_VAR_OB_CAPABILITY_FLAG), K(cap), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::set_trans_specified(const bool is_spec)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_bool(is_spec);
  if (OB_FAIL(update_sys_variable(SYS_VAR_OB_PROXY_SET_TRX_EXECUTED, obj))) {
    LOG_WARN("fail to update_system_variable", K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::set_safe_weak_read_snapshot_variable(const int64_t safe_snapshot)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_int(safe_snapshot);
  if (OB_FAIL(update_sys_variable(SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT, obj))) {
    LOG_WARN("fail to update_system_variable", K(SYS_VAR_OB_SAFE_WEAK_READ_SNAPSHOT), K(safe_snapshot), K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::set_init_connect(const ObString& init_sql)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_string(ObVarcharType, init_sql);
  if (OB_FAIL(update_sys_variable(SYS_VAR_INIT_CONNECT, obj))) {
    LOG_WARN("fail to update_system_variable", K(ret));
  } else {
  }
  return ret;
}

int ObBasicSessionInfo::save_trans_status()
{
  int ret = OB_SUCCESS;
  switch (trans_spec_status_) {
    case TRANS_SPEC_SET:
      ret = set_trans_specified(true);
      break;
    case TRANS_SPEC_COMMIT:
      ret = set_trans_specified(false);
      trans_spec_status_ = TRANS_SPEC_NOT_SET;
      break;
    case TRANS_SPEC_NOT_SET:
      // do nothing
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unknown trans specified status", K(trans_spec_status_), K(ret));
      break;
  }
  return ret;
}

int ObBasicSessionInfo::get_character_set_client(ObCharsetType& character_set_client) const
{
  return get_charset_sys_var(SYS_VAR_CHARACTER_SET_CLIENT, character_set_client);
}

int ObBasicSessionInfo::get_character_set_connection(ObCharsetType& character_set_connection) const
{
  return get_charset_sys_var(SYS_VAR_CHARACTER_SET_CONNECTION, character_set_connection);
}

int ObBasicSessionInfo::get_character_set_database(ObCharsetType& character_set_database) const
{
  return get_charset_sys_var(SYS_VAR_CHARACTER_SET_DATABASE, character_set_database);
}

int ObBasicSessionInfo::get_character_set_results(ObCharsetType& character_set_results) const
{
  if (CHARSET_INVALID == (character_set_results = sys_vars_cache_.get_character_set_results())) {
    get_charset_sys_var(SYS_VAR_CHARACTER_SET_RESULTS, character_set_results);
  }
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_character_set_server(ObCharsetType& character_set_server) const
{
  return get_charset_sys_var(SYS_VAR_CHARACTER_SET_SERVER, character_set_server);
}

int ObBasicSessionInfo::get_character_set_system(ObCharsetType& character_set_system) const
{
  return get_charset_sys_var(SYS_VAR_CHARACTER_SET_SYSTEM, character_set_system);
}

int ObBasicSessionInfo::get_character_set_filesystem(ObCharsetType& character_set_filesystem) const
{
  return get_charset_sys_var(SYS_VAR_CHARACTER_SET_FILESYSTEM, character_set_filesystem);
}

int ObBasicSessionInfo::get_collation_database(ObCollationType& collation_database) const
{
  return get_collation_sys_var(SYS_VAR_COLLATION_DATABASE, collation_database);
}

int ObBasicSessionInfo::get_collation_server(ObCollationType& collation_server) const
{
  return get_collation_sys_var(SYS_VAR_COLLATION_SERVER, collation_server);
}

int ObBasicSessionInfo::get_foreign_key_checks(int64_t& foreign_key_checks) const
{
  foreign_key_checks = sys_vars_cache_.get_foreign_key_checks();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_capture_plan_baseline(bool& v) const
{
  v = sys_vars_cache_.get_optimizer_capture_sql_plan_baselines();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_use_plan_baseline(bool& v) const
{
  v = sys_vars_cache_.get_optimizer_use_sql_plan_baselines();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_adaptive_cursor_sharing(bool& acs) const
{
  acs = sys_vars_cache_.get__optimizer_adaptive_cursor_sharing();
  return OB_SUCCESS;
}

int ObBasicSessionInfo::get_nlj_batching_enabled(bool& v) const
{
  return get_bool_sys_var(SYS_VAR__NLJ_BATCHING_ENABLED, v);
}

int ObBasicSessionInfo::get_auto_increment_cache_size(int64_t& auto_increment_cache_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_sys_variable(SYS_VAR_AUTO_INCREMENT_CACHE_SIZE, auto_increment_cache_size))) {
    LOG_WARN("fail to get variables", K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::get_enable_parallel_dml(bool& v) const
{
  return get_bool_sys_var(SYS_VAR__ENABLE_PARALLEL_DML, v);
}

int ObBasicSessionInfo::get_enable_parallel_query(bool& v) const
{
  return get_bool_sys_var(SYS_VAR__ENABLE_PARALLEL_QUERY, v);
}

int ObBasicSessionInfo::get_force_parallel_query_dop(uint64_t& v) const
{
  int ret = OB_SUCCESS;
  if (exec_min_cluster_version_ >= CLUSTER_VERSION_3100) {
    ret = get_uint64_sys_var(SYS_VAR__FORCE_PARALLEL_QUERY_DOP, v);
  } else {
    v = 1;
  }
  return ret;
}

int ObBasicSessionInfo::get_force_parallel_dml_dop(uint64_t& v) const
{
  int ret = OB_SUCCESS;
  if (exec_min_cluster_version_ >= CLUSTER_VERSION_3100) {
    ret = get_uint64_sys_var(SYS_VAR__FORCE_PARALLEL_DML_DOP, v);
  } else {
    v = 1;
  }
  return ret;
}

int ObBasicSessionInfo::get_secure_file_priv(ObString& v) const
{
  return get_string_sys_var(SYS_VAR_SECURE_FILE_PRIV, v);
}

int ObBasicSessionInfo::get_sql_safe_updates(bool& v) const
{
  return get_bool_sys_var(SYS_VAR_SQL_SAFE_UPDATES, v);
}

void ObBasicSessionInfo::reset_tx_variable()
{
  // this function will be called in end_trans phase, and must be idempotent.
  // trans_desc will be reset in start_trans phase, not here, because end_trans may use it.
  reset_tx_isolation();
  reset_tx_read_only();
  reset_trans_flags();
  clear_trans_consistency_type();
  clear_app_trace_id();
}

int32_t ObBasicSessionInfo::get_tx_isolation() const
{
  int32_t isolation = -1;
  const int32_t TX_NOT_SET = -1;
  if (next_tx_isolation_ != TX_NOT_SET) {
    isolation = next_tx_isolation_;
  } else {
    isolation = sys_vars_cache_.get_tx_isolation();
  }
  return isolation;
}

bool ObBasicSessionInfo::is_isolation_serializable() const
{
  return ObTransIsolation::SERIALIZABLE == get_tx_isolation();
}

void ObBasicSessionInfo::set_tx_isolation(int32_t isolation)
{
  next_tx_isolation_ = isolation;
  trans_spec_status_ = TRANS_SPEC_SET;
}

void ObBasicSessionInfo::reset_tx_isolation()
{
  next_tx_isolation_ = transaction::ObTransIsolation::UNKNOWN;
  if (TRANS_SPEC_SET == trans_spec_status_) {
    trans_spec_status_ = TRANS_SPEC_COMMIT;
  }
}

void ObBasicSessionInfo::set_tx_read_only(bool read_only)
{
  next_tx_read_only_ = read_only;
  trans_spec_status_ = TRANS_SPEC_SET;
}

void ObBasicSessionInfo::reset_tx_read_only()
{
  next_tx_read_only_ = -1;
  if (TRANS_SPEC_SET == trans_spec_status_) {
    trans_spec_status_ = TRANS_SPEC_COMMIT;
  }
}

int ObBasicSessionInfo::check_tx_read_only_privilege(const ObSqlTraits& sql_traits)
{
  int ret = OB_SUCCESS;
  if (sql_traits.is_cause_implicit_commit_) {
    reset_tx_read_only();
  } else {
  }
  if (get_tx_read_only() && !sql_traits.is_readonly_stmt_) {
    ret = OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION;

  } else {
  }
  return ret;
}

int ObBasicSessionInfo::reset_tx_variable_if_remote_trans(const ObPhyPlanType& type)
{
  int ret = OB_SUCCESS;
  if (ObSqlTransUtil::is_remote_trans(get_local_autocommit(), trans_flags_.is_in_transaction(), type)) {
    reset_tx_variable();
  } else {
    // nothing to do
  }
  return ret;
}

bool ObBasicSessionInfo::get_tx_read_only() const
{
  bool tx_read_only = false;
  const int32_t TX_NOT_SET = -1;
  if (next_tx_read_only_ != TX_NOT_SET) {
    tx_read_only = next_tx_read_only_;
  } else {
    tx_read_only = sys_vars_cache_.get_tx_read_only();
  }
  return tx_read_only;
}

int ObBasicSessionInfo::store_query_string(const ObString& stmt)
{
  LockGuard lock_guard(thread_data_mutex_);
  return store_query_string_(stmt);
}

int ObBasicSessionInfo::store_query_string_(const ObString& stmt)
{
  int ret = OB_SUCCESS;
  int64_t truncated_len = std::min(MAX_CUR_QUERY_LEN - 1, static_cast<int64_t>(stmt.length()));
  if (truncated_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str length", K(ret), K(truncated_len));
  } else if (thread_data_.cur_query_buf_len_ - 1 < truncated_len) {
    if (thread_data_.cur_query_ != nullptr) {
      ob_free(thread_data_.cur_query_);
      thread_data_.cur_query_ = NULL;
      thread_data_.cur_query_buf_len_ = 0;
    }
    int64_t len = MAX(MIN_CUR_QUERY_LEN, truncated_len + 1);
    char* buf = reinterpret_cast<char*>(ob_malloc(len, ObModIds::OB_SQL_SESSION_QUERY_SQL));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      thread_data_.cur_query_ = buf;
      thread_data_.cur_query_buf_len_ = len;
    }
  }
  if (OB_SUCC(ret)) {
    MEMCPY(thread_data_.cur_query_, stmt.ptr(), truncated_len);
    thread_data_.cur_query_[truncated_len] = '\0';
    thread_data_.cur_query_len_ = truncated_len;
  }
  return ret;
}

void ObBasicSessionInfo::reset_query_string()
{
  thread_data_.cur_query_[0] = '\0';
  thread_data_.cur_query_len_ = 0;
}

int ObBasicSessionInfo::update_session_timeout()
{
  int ret = OB_SUCCESS;
  ObObj val;
  if (thread_data_.is_interactive_) {
    val.set_int(thread_data_.interactive_timeout_);
    if (OB_FAIL(update_sys_variable(SYS_VAR_WAIT_TIMEOUT, val))) {
      LOG_WARN("failed to update session_timeout", K(ret));
    }
  }
  return ret;
}

const char* ObBasicSessionInfo::get_session_state_str() const
{
  const char* str_ret = NULL;
  switch (thread_data_.state_) {
    case SESSION_INIT:
      str_ret = "INIT";
      break;
    case SESSION_SLEEP:
      str_ret = "SLEEP";
      break;
    case QUERY_ACTIVE:
      str_ret = "ACTIVE";
      break;
    case QUERY_KILLED:
      str_ret = "QUERY_KILLED";
      break;
    case SESSION_KILLED:
      str_ret = "SESSION_KILLED";
      break;
    default:
      str_ret = "INVALID SESSION STATE";
      break;
  }
  return str_ret;
}

int ObBasicSessionInfo::is_timeout(bool& is_timeout)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  is_timeout = false;
  if (SESSION_SLEEP == thread_data_.state_) {
    timeout = thread_data_.is_interactive_ ? thread_data_.interactive_timeout_ : thread_data_.wait_timeout_;
    int64_t cur_time = ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("session timeout setting is invalid", K(ret), K(timeout));
    } else if (OB_UNLIKELY(timeout > INT64_MAX / 1000 / 1000)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("session timeout setting is too larger, skip check timeout", K(ret), K(timeout));
    } else if (0 == timeout) {
      // no timeout check for timeout == 0
    } else if (SESSION_SLEEP == thread_data_.state_ &&
               thread_data_.last_active_time_ + timeout * 1000 * 1000 < cur_time) {
      const ObString& user_name = get_user_name();
      char addr_buf[common::OB_IP_PORT_STR_BUFF];
      if (OB_FAIL(get_peer_addr().ip_port_to_string(addr_buf, OB_IP_PORT_STR_BUFF))) {
        LOG_WARN("failed to get peer addr string", K(ret), K(get_peer_addr()), K(addr_buf));
      } else {
        _LOG_INFO("sessionkey %u, proxy_sessid %lu: %.*s from %s timeout: last active time=%s, cur=%s, timeout=%lds",
            sessid_,
            proxy_sessid_,
            user_name.length(),
            user_name.ptr(),
            addr_buf,
            time2str(thread_data_.last_active_time_),
            time2str(cur_time),
            timeout);
      }
      is_timeout = true;
    }
  }
  return ret;
}

int ObBasicSessionInfo::is_trx_idle_timeout(bool& timeout)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ::oceanbase::common::ObTimeUtility::current_time();
  LOG_DEBUG("check trx_idle_timeout : ",
      K_(sessid),
      K(trans_flags_.is_in_transaction()),
      K(curr_trans_last_stmt_end_time_),
      K(cur_time),
      K(get_query_start_time()));
  if (get_query_start_time() < curr_trans_last_stmt_end_time_) {
    int64_t ob_trx_idle_timeout =
        sys_vars_cache_.get_ob_trx_idle_timeout() > 0 ? sys_vars_cache_.get_ob_trx_idle_timeout() : 120 * 1000 * 1000;
    if (trans_flags_.is_in_transaction() && curr_trans_last_stmt_end_time_ > 0 &&
        curr_trans_last_stmt_end_time_ + ob_trx_idle_timeout < cur_time) {
      LOG_WARN("ob_trx_idle_timeout happen", K_(sessid), K_(curr_trans_last_stmt_end_time), K(ob_trx_idle_timeout));
      curr_trans_last_stmt_end_time_ = 0;  // flip
      timeout = true;
    }
  }
  return ret;
}

int ObBasicSessionInfo::get_pc_mem_conf(ObPCMemPctConf& pc_mem_conf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_sys_variable(SYS_VAR_OB_PLAN_CACHE_PERCENTAGE, pc_mem_conf.limit_pct_))) {
    LOG_WARN("fail to get plan cache system variables", K(pc_mem_conf.limit_pct_), K(ret));
  } else if (OB_FAIL(get_sys_variable(SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE, pc_mem_conf.high_pct_))) {
    LOG_WARN("fail to get plan cache system variables", K(pc_mem_conf.high_pct_), K(ret));
  } else if (OB_FAIL(get_sys_variable(SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE, pc_mem_conf.low_pct_))) {
    LOG_WARN("fail to get plan cache system variables", K(pc_mem_conf.low_pct_), K(ret));
  }
  return ret;
}

int ObBasicSessionInfo::get_sql_audit_mem_conf(int64_t& mem_pct)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_sys_variable(SYS_VAR_OB_SQL_AUDIT_PERCENTAGE, mem_pct))) {
    LOG_WARN("failed to get memory percentage for sql audit", K(ret));
  }
  return ret;
}

void ObBasicSessionInfo::set_conn(easy_connection_t* conn)
{
  LockGuard lock_guard(thread_data_mutex_);
  thread_data_.conn_ = conn;
}

void ObBasicSessionInfo::set_session_data(ObSQLSessionState state, obmysql::ObMySQLCmd mysql_cmd, int64_t thread_id)
{
  LockGuard lock_guard(thread_data_mutex_);
  set_session_state_(state);
  thread_data_.mysql_cmd_ = mysql_cmd;
  thread_id_ = thread_id;
}

int ObBasicSessionInfo::set_session_state(ObSQLSessionState state)
{
  LockGuard lock_guard(thread_data_mutex_);
  return set_session_state_(state);
}

int ObBasicSessionInfo::set_session_state_(ObSQLSessionState state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(SESSION_KILLED == thread_data_.state_)) {
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session is killed", K(ret), K(sessid_), K(proxy_sessid_), K(version_), K(state));
  } else if (OB_UNLIKELY(SESS_NOT_IN_RETRY != thread_data_.is_in_retry_ && QUERY_KILLED == thread_data_.state_ &&
                         SESSION_KILLED != state)) {

    ret = OB_ERR_QUERY_INTERRUPTED;
    LOG_WARN("query is killed", K(ret), K(sessid_), K(proxy_sessid_), K(version_), K(state));
  } else {
    thread_data_.state_ = state;
    thread_data_.cur_state_start_time_ = ::oceanbase::common::ObClockGenerator::getClock();
  }
  return ret;
}

int ObBasicSessionInfo::set_session_active(
    const ObString& sql, const int64_t query_receive_ts, const int64_t last_active_time_ts, obmysql::ObMySQLCmd cmd)
{
  int ret = OB_SUCCESS;

  LockGuard lock_guard(thread_data_mutex_);
  if (OB_FAIL(store_query_string_(sql))) {
    LOG_WARN("store query string fail", K(ret));
  } else if (OB_FAIL(set_session_state_(QUERY_ACTIVE))) {
    LOG_WARN("fail to set session state", K(ret));
  } else {
    thread_data_.cur_query_start_time_ = query_receive_ts;
    thread_data_.mysql_cmd_ = cmd;
    thread_data_.last_active_time_ = last_active_time_ts;
  }
  return ret;
}

int ObBasicSessionInfo::base_save_session(BaseSavedValue& saved_value, bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  // OZ (saved_value.trans_desc_.stmt_deep_copy(trans_desc_));
  // OX (saved_value.in_transaction_ = trans_flags_.is_in_transaction());
  OX(saved_value.cur_phy_plan_ = cur_phy_plan_);
  OX(cur_phy_plan_ = NULL);
  int len = MIN(thread_data_.cur_query_len_, static_cast<int64_t>(sizeof(saved_value.cur_query_) - 1));
  if (thread_data_.cur_query_ != nullptr) {
    OX(MEMCPY(&saved_value.cur_query_, thread_data_.cur_query_, len));
    OX(thread_data_.cur_query_[0] = 0);
  }
  OX(saved_value.cur_query_len_ = len);
  OX(thread_data_.cur_query_len_ = 0);
  // OX (saved_value.cur_query_start_time_ = thread_data_.cur_query_start_time_);
  // OX (thread_data_.cur_query_start_time_ = 0);
  OZ(saved_value.total_stmt_tables_.assign(total_stmt_tables_));
  if (!skip_cur_stmt_tables) {
    OZ(append_array_no_dup(total_stmt_tables_, cur_stmt_tables_));
  }
  OZ(saved_value.cur_stmt_tables_.assign(cur_stmt_tables_));
  OX(cur_stmt_tables_.reset());
  OX(sys_vars_cache_.get_autocommit_info(
      saved_value.base_autocommit_, saved_value.inc_autocommit_, saved_value.is_inc_autocommit_));
  OX(sys_vars_cache_.set_autocommit_info(false, false, false));
  OX(saved_value.is_foreign_key_cascade_ = is_foreign_key_cascade_);
  OX(is_foreign_key_cascade_ = false);
  OX(saved_value.is_foreign_key_check_exist_ = is_foreign_key_check_exist_);
  OX(is_foreign_key_check_exist_ = false);
  OX(saved_value.need_serial_exec_ = trans_flags_.need_serial_exec());
  if (OB_NOT_NULL(saved_value.cur_phy_plan_) && saved_value.cur_phy_plan_->get_need_serial_exec()) {
    OX(trans_flags_.set_need_serial_exec(true));
  }
  return ret;
}

int ObBasicSessionInfo::stmt_save_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  OZ(base_save_session(saved_value, skip_cur_stmt_tables));
  OZ(saved_value.trans_desc_.stmt_deep_copy(trans_desc_));
  OX(saved_value.in_transaction_ = trans_flags_.is_in_transaction());
  OX(saved_value.cur_query_start_time_ = thread_data_.cur_query_start_time_);
  OX(thread_data_.cur_query_start_time_ = 0);
  OZ(saved_value.trans_result_.assign(trans_result_));
  OX(trans_result_.reset());
  return ret;
}

int ObBasicSessionInfo::save_basic_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables)
{
  return stmt_save_session(saved_value, skip_cur_stmt_tables);
}

int ObBasicSessionInfo::begin_nested_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  OZ(SMART_CALL(save_basic_session(saved_value, skip_cur_stmt_tables)));
  OX(trans_flags_.set_is_in_transaction(true));
  return ret;
}

int ObBasicSessionInfo::base_restore_session(BaseSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OX(trans_flags_.set_need_serial_exec(saved_value.need_serial_exec_));
  OX(is_foreign_key_check_exist_ = saved_value.is_foreign_key_check_exist_);
  OX(is_foreign_key_cascade_ = saved_value.is_foreign_key_cascade_);
  OX(sys_vars_cache_.set_autocommit_info(
      saved_value.base_autocommit_, saved_value.inc_autocommit_, saved_value.is_inc_autocommit_));
  OZ(cur_stmt_tables_.assign(saved_value.cur_stmt_tables_));
  OZ(total_stmt_tables_.assign(saved_value.total_stmt_tables_));
  // OX (thread_data_.cur_query_start_time_ = saved_value.cur_query_start_time_);
  int64_t len = MIN(saved_value.cur_query_len_, thread_data_.cur_query_buf_len_ - 1);
  OX(thread_data_.cur_query_len_ = len);
  if (thread_data_.cur_query_ != nullptr) {
    OX(MEMCPY(thread_data_.cur_query_, saved_value.cur_query_, len));
    thread_data_.cur_query_[len] = '\0';
  }
  OX(cur_phy_plan_ = saved_value.cur_phy_plan_);
  return ret;
}

int ObBasicSessionInfo::stmt_restore_session(StmtSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OZ(trans_result_.merge_result(saved_value.trans_result_));
  OX(thread_data_.cur_query_start_time_ = saved_value.cur_query_start_time_);
  OX(trans_flags_.set_is_in_transaction(saved_value.in_transaction_));
  OZ(trans_desc_.stmt_deep_copy(saved_value.trans_desc_));
  OZ(base_restore_session(saved_value));
  return ret;
}

int ObBasicSessionInfo::restore_basic_session(StmtSavedValue& saved_value)
{
  return stmt_restore_session(saved_value);
}

int ObBasicSessionInfo::end_nested_session(StmtSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  int64_t max_sql_no = MAX(trans_desc_.get_max_sql_no(), trans_result_.get_max_sql_no());
  OZ(SMART_CALL(restore_basic_session(saved_value)));
  OX(saved_value.reset());
  OX(trans_desc_.restore_max_sql_no(max_sql_no));
  OX(trans_result_.set_max_sql_no(max_sql_no));
  return ret;
}

int ObBasicSessionInfo::trans_save_session(TransSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OZ(base_save_session(saved_value, false));
  OZ(saved_value.trans_desc_.trans_deep_copy(trans_desc_));
  OX(trans_desc_.reset());
  OZ(trans_desc_.init(this));
  OX(saved_value.trans_flags_ = trans_flags_);
  OX(trans_flags_.reset());
  OZ(saved_value.trans_result_.assign(trans_result_));
  OX(trans_result_.reset());
  OX(saved_value.nested_count_ = nested_count_);
  OX(nested_count_ = -1);
  return ret;
}

int ObBasicSessionInfo::trans_restore_session(TransSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OX(nested_count_ = saved_value.nested_count_);
  OZ(trans_result_.assign(saved_value.trans_result_));
  OX(trans_flags_ = saved_value.trans_flags_);
  OZ(trans_desc_.trans_deep_copy(saved_value.trans_desc_));
  OZ(base_restore_session(saved_value));
  return ret;
}

int ObBasicSessionInfo::begin_autonomous_session(TransSavedValue& saved_value)
{
  return trans_save_session(saved_value);
}

int ObBasicSessionInfo::end_autonomous_session(TransSavedValue& saved_value)
{
  return trans_restore_session(saved_value);
}

int ObBasicSessionInfo::set_start_stmt()
{
  int ret = OB_SUCCESS;
  OV(nested_count_ == -1, OB_ERR_UNEXPECTED, nested_count_);
  OX(nested_count_ = 0);
  return ret;
}

int ObBasicSessionInfo::set_end_stmt()
{
  int ret = OB_SUCCESS;
  OV(nested_count_ == 0, OB_ERR_UNEXPECTED, nested_count_);
  OX(nested_count_ = -1);
  return ret;
}

int ObBasicSessionInfo::set_cur_stmt_tables(const ObIArray<ObPartitionKey>& stmt_partitions, stmt::StmtType stmt_type)
{
  int ret = OB_SUCCESS;
  if (!is_nested_session()) {
    // cur stmt is top stmt.
    total_stmt_tables_.reset();
    cur_stmt_tables_.reset();
  }
  if (!is_foreign_key_cascade() && !is_foreign_key_check_exist()) {
    TableStmtType table_stmt_type;
    int64_t idx = -1;
    table_stmt_type.stmt_type_ = stmt_type;
    for (int i = 0; OB_SUCC(ret) && i < stmt_partitions.count(); i++) {
      OX(table_stmt_type.table_id_ = stmt_partitions.at(i).table_id_);
      if (has_exist_in_array(total_stmt_tables_, table_stmt_type, &idx)) {
        OV(0 <= idx && idx < total_stmt_tables_.count());
        OV(table_stmt_type.is_select_stmt() && total_stmt_tables_.at(idx).is_select_stmt(),
            OB_ERR_MUTATING_TABLE_OPERATION,
            total_stmt_tables_,
            table_stmt_type,
            is_foreign_key_cascade_,
            is_foreign_key_check_exist_);
      }
      OZ(add_var_to_array_no_dup(cur_stmt_tables_, table_stmt_type), cur_stmt_tables_, table_stmt_type);
    }
  }
  return ret;
}

int ObBasicSessionInfo::set_time_zone(
    const ObString& str_val, const bool is_oralce_mode, const bool check_timezone_valid)
{
  int ret = OB_SUCCESS;
  int32_t offset = 0;
  int ret_more = OB_SUCCESS;
  if (OB_FAIL(ObTimeConverter::str_to_offset(str_val, offset, ret_more, is_oralce_mode, check_timezone_valid))) {
    if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
      LOG_WARN("fail to convert time zone", K(str_val), K(ret));
    }
  } else {
    tz_info_wrap_.set_tz_info_offset(offset);
  }

  if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
    ObTZMapWrap tz_map_wrap;
    ObTimeZoneInfoManager* tz_info_mgr = NULL;
    if (OB_FAIL(OTTZ_MGR.get_tenant_timezone(tenant_id_, tz_map_wrap, tz_info_mgr))) {
      LOG_WARN("get tenant timezone with lock failed", K(ret));
    } else if (OB_ISNULL(tz_info_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant timezone mgr is null", K(tz_info_mgr));
    } else {
      const int64_t orig_version = tz_info_wrap_.get_cur_version();
      tz_info_wrap_.set_cur_version(tz_info_mgr->get_version());
      ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
      int32_t no_sp_len =
          static_cast<int32_t>(ObCharset::strlen_byte_no_sp(coll_type, str_val.ptr(), str_val.length()));
      ObString val_no_sp(no_sp_len, str_val.ptr());
      if (is_oralce_mode) {
        val_no_sp = val_no_sp.trim();
      }
      int64_t start_service_time = GCTX.start_service_time_;
      if (OB_FAIL(tz_info_mgr->find_time_zone_info(val_no_sp, tz_info_wrap_.get_tz_info_pos()))) {
        LOG_WARN("fail to find time zone", K(str_val), K(val_no_sp), K(ret));
        tz_info_wrap_.set_cur_version(orig_version);
      } else {
        tz_info_wrap_.set_tz_info_position();
      }

      if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
        if (0 == start_service_time) {
          LOG_INFO(
              "ignore unknow time zone, perhaps in remote/distribute task processer when server start_time is zero",
              K(str_val));
          ret = OB_SUCCESS;
          if (OB_FAIL(tz_info_wrap_.get_tz_info_pos().set_tz_name(str_val.ptr(), no_sp_len))) {
            LOG_WARN("fail to set time zone info name", K(str_val), K(ret));
          } else {
            tz_info_wrap_.set_tz_info_position();
          }
        }
      }
    }
  }
  return ret;
}

int ObBasicSessionInfo::update_timezone_info()
{
  int ret = OB_SUCCESS;
  ObTZMapWrap tz_map_wrap;
  ObTimeZoneInfoManager* tz_info_mgr = NULL;
  if (OB_FAIL(OTTZ_MGR.get_tenant_timezone(tenant_id_, tz_map_wrap, tz_info_mgr))) {
    LOG_WARN("get tenant timezone with lock failed", K(ret));
  } else if (OB_ISNULL(tz_info_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant timezone mgr is null", K(tz_info_mgr));
  } else if (OB_UNLIKELY(
                 tz_info_wrap_.is_position_class() && tz_info_mgr->get_version() > tz_info_wrap_.get_cur_version())) {
    ObString tz_name;
    if (OB_UNLIKELY(!tz_info_wrap_.get_tz_info_pos().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("time zone info is invalid", K(tz_info_wrap_.get_tz_info_pos()), K(ret));
    } else if (OB_FAIL(tz_info_wrap_.get_tz_info_pos().get_tz_name(tz_name))) {
      LOG_WARN("fal to get time zone name", K(tz_info_wrap_.get_tz_info_pos()), K(ret));
    } else {
      int64_t orig_version = tz_info_wrap_.get_cur_version();
      tz_info_wrap_.set_cur_version(tz_info_mgr->get_version());
      if (OB_FAIL(tz_info_mgr->find_time_zone_info(tz_name, tz_info_wrap_.get_tz_info_pos()))) {
        LOG_WARN("fail to find time zone info", K(tz_name), K(ret));
        tz_info_wrap_.set_cur_version(orig_version);
      } else {
        tz_info_wrap_.get_tz_info_pos().set_error_on_overlap_time(tz_info_wrap_.is_error_on_overlap_time());
      }
    }
  }
  return ret;
}

constexpr ObSysVarClassType ObExecEnv::ExecEnvMap[MAX_ENV + 1];

void ObExecEnv::reset()
{
  sql_mode_ = DEFAULT_OCEANBASE_MODE;
  charset_client_ = CS_TYPE_INVALID;
  collation_connection_ = CS_TYPE_INVALID;
  collation_database_ = CS_TYPE_INVALID;
}

bool ObExecEnv::operator==(const ObExecEnv& other) const
{
  return sql_mode_ == other.sql_mode_ && charset_client_ == other.charset_client_ &&
         collation_connection_ == other.collation_connection_ && collation_database_ == other.collation_database_;
}

bool ObExecEnv::operator!=(const ObExecEnv& other) const
{
  return !(*this == other);
}

int ObExecEnv::gen_exec_env(const ObBasicSessionInfo& session, char* buf, int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObObj val;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_ENV; ++i) {
    int64_t size = 0;
    val.reset();
    if (OB_FAIL(session.get_sys_variable(ExecEnvMap[i], val))) {
      LOG_WARN("failed to get sys_variable", K(ExecEnvMap[i]), K(ret));
    } else if (OB_FAIL(val.print_plain_str_literal(buf + pos, len - pos, size))) {
      LOG_WARN("fail to encode obj", K(i), K(buf + pos), K(len), K(pos), K(val), K(ret));
    } else {
      pos += size;
      buf[pos++] = ',';
    }
  }
  return ret;
}

int ObExecEnv::get_exec_env(const ObString& exec_env, ExecEnvType type, ObString& value)
{
  int ret = OB_SUCCESS;
  if (exec_env.empty()) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid exec_env", K(exec_env), K(type), K(ret));
  } else {
    ObString result;
    ObString start = exec_env;
    const char* end = NULL;
    int64_t i = 0;
    while (!start.empty() && i++ < type) {
      start = start.after(',');
    }
    end = start.find(',');
    value.assign(start.ptr(), static_cast<ObString::obstr_size_t>(end - start.ptr()));
  }
  return ret;
}

#define SET_ENV_VALUE(env, type)                               \
  do {                                                         \
    int64_t value = 0;                                         \
    int64_t pos = 0;                                           \
    if (OB_FAIL(extract_int(value_str, 0, pos, value))) {      \
      LOG_WARN("Failed to extract int", K(value_str), K(ret)); \
    } else {                                                   \
      env = static_cast<type>(value);                          \
    }                                                          \
  } while (0)

int ObExecEnv::init(const ObString& exec_env)
{
  int ret = OB_SUCCESS;
  ObString value_str;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_ENV; ++i) {
    value_str.reset();
    if (OB_FAIL(get_exec_env(exec_env, static_cast<ExecEnvType>(i), value_str))) {
      LOG_WARN("failed to get exec_env", K(exec_env), K(i), K(ret));
    } else {
      switch (i) {
        case SQL_MODE: {
          SET_ENV_VALUE(sql_mode_, ObSQLMode);
        } break;
        case CHARSET_CLIENT: {
          SET_ENV_VALUE(charset_client_, ObCollationType);
        } break;
        case COLLATION_CONNECTION: {
          SET_ENV_VALUE(collation_connection_, ObCollationType);
        } break;
        case COLLATION_DATABASE: {
          SET_ENV_VALUE(collation_database_, ObCollationType);
        } break;
        default: {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid env type", K(exec_env), K(i), K(ret));
        } break;
      }
    }
  }
  return ret;
}

int ObExecEnv::load(ObBasicSessionInfo& session)
{
  int ret = OB_SUCCESS;
  ObObj val;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_ENV; ++i) {
    val.reset();
    if (OB_FAIL(session.get_sys_variable(ExecEnvMap[i], val))) {
      LOG_WARN("failed to get sys_variable", K(ExecEnvMap[i]), K(ret));
    } else {
      switch (i) {
        case SQL_MODE: {
          sql_mode_ = static_cast<ObSQLMode>(val.get_int());
        } break;
        case CHARSET_CLIENT: {
          charset_client_ = static_cast<ObCollationType>(val.get_int());
        } break;
        case COLLATION_CONNECTION: {
          collation_connection_ = static_cast<ObCollationType>(val.get_int());
        } break;
        case COLLATION_DATABASE: {
          collation_database_ = static_cast<ObCollationType>(val.get_int());
        } break;
        default: {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid env type", K(i), K(ret));
        } break;
      }
    }
  }
  return ret;
}

int ObExecEnv::store(ObBasicSessionInfo& session)
{
  int ret = OB_SUCCESS;
  ObObj val;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_ENV; ++i) {
    val.reset();
    switch (i) {
      case SQL_MODE: {
        val.set_uint64(sql_mode_);
      } break;
      case CHARSET_CLIENT: {
        val.set_int(charset_client_);
      } break;
      case COLLATION_CONNECTION: {
        val.set_int(collation_connection_);
      } break;
      case COLLATION_DATABASE: {
        val.set_int(collation_database_);
      } break;
      default: {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid env type", K(i), K(ret));
      } break;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(session.update_sys_variable(ExecEnvMap[i], val))) {
        LOG_WARN("failed to get sys_variable", K(ExecEnvMap[i]), K(ret));
      }
    }
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
