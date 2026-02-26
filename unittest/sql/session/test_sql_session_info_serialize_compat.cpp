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
#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/session/ob_sql_session_info.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

static const int64_t BUF_SIZE = 1024 * 1024; // 1MB buffer

void init_set(ObBasicSessionInfo &info)
{
  for (int i = 0; i < share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++i) {
    info.sys_vars_[i] = NULL;
  }
  memset(info.flt_vars_.last_flt_trace_id_buf_, 0, sizeof(info.flt_vars_.last_flt_trace_id_buf_));
  memset(info.flt_vars_.last_flt_span_id_buf_, 0, sizeof(info.flt_vars_.last_flt_span_id_buf_));
  info.flt_vars_.row_traceformat_ = false;
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  bool autocommit = false;
  bool is_valid  = false;
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
  ASSERT_EQ(OB_SUCCESS, share::ObPreProcessSysVars::init_sys_var());
  int ret = share::ObPreProcessSysVars::change_initial_value();
  if (OB_SUCCESS != ret) {
    LOG_ERROR("Change initial value failed !", K(ret));
  }
  ASSERT_EQ(OB_SUCCESS, info.load_default_sys_variable(true, true));
  {
    ObString tenant_name = ObString::make_string("yyy");
    ObString user_name = ObString::make_string("aaa");
    info.init_tenant(tenant_name, 1);
    info.set_user(user_name, OB_DEFAULT_HOST_NAME, 1);
  }
  info.tz_info_wrap_.set_tz_info_offset(123);
}

struct ObSQLSessionInfo42x : public common::ObVersionProvider, public ObBasicSessionInfo, public SessionInfoHashValue
{
  OB_UNIS_VERSION(2);
  ObSQLSessionInfo42x(const uint64_t tenant_id = OB_SERVER_TENANT_ID) :
      ObVersionProvider(),
      ObBasicSessionInfo(tenant_id),
      user_priv_set_(10),
      db_priv_set_(2),
      trans_type_(transaction::ObTxClass::USER),
      global_sessid_(1001),
      inner_flag_(false),
      is_max_availability_mode_(false),
      session_type_(ObSQLSessionInfo::USER_SESSION),
      has_temp_table_flag_(false),
      enable_early_lock_release_(false),
      enable_role_array_(false),
      in_definer_named_proc_(false),
      priv_user_id_(1001),
      xa_end_timeout_seconds_(60),
      cached_tenant_config_info_(nullptr),
      prelock_(false),
      proxy_version_(1001),
      min_proxy_version_ps_(1001),
      ddl_info_(),
      gtt_session_scope_unique_id_(1001),
      gtt_trans_scope_unique_id_(1001),
      txn_free_route_ctx_(),
      cur_exec_ctx_(nullptr),
      restore_auto_commit_(false),
      dblink_context_(reinterpret_cast<ObSQLSessionInfo*>(this)),
      sql_req_level_(0),
      gtt_session_scope_ids_(),
      gtt_trans_scope_ids_(),
      unit_gc_min_sup_proxy_version_(1001)
  {
    init_set(*this);
  }
  const common::ObVersion get_frozen_version() const
  {
    return version_provider_->get_frozen_version();
  }
  const common::ObVersion get_merged_version() const
  {
    return version_provider_->get_merged_version();
  }
  // 成员变量
  const common::ObVersionProvider *version_provider_;
  ObPrivSet user_priv_set_;
  ObPrivSet db_priv_set_;
  transaction::ObTxClass trans_type_;
  int64_t global_sessid_;
  bool inner_flag_;
  bool is_max_availability_mode_;
  ObSQLSessionInfo::SessionType session_type_;
  bool has_temp_table_flag_;
  bool enable_early_lock_release_;
  bool enable_role_array_;
  bool in_definer_named_proc_;
  uint64_t priv_user_id_;
  int64_t xa_end_timeout_seconds_;
  ObSQLSessionInfo::ObCachedTenantConfigInfo cached_tenant_config_info_;
  bool prelock_;
  uint64_t proxy_version_;
  uint64_t min_proxy_version_ps_;
  ObSessionDDLInfo ddl_info_;
  int64_t gtt_session_scope_unique_id_;
  int64_t gtt_trans_scope_unique_id_;
  transaction::ObTxnFreeRouteCtx txn_free_route_ctx_;
  ObExecContext *cur_exec_ctx_;
  bool restore_auto_commit_;
  oceanbase::sql::ObDblinkCtxInSession dblink_context_;
  int64_t sql_req_level_;
  common::ObSEArray<uint64_t, 1> gtt_session_scope_ids_;
  common::ObSEArray<uint64_t, 1> gtt_trans_scope_ids_;
  uint64_t unit_gc_min_sup_proxy_version_;
};

OB_DEF_SERIALIZE(ObSQLSessionInfo42x)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObSQLSessionInfo, ObBasicSessionInfo));
  LST_DO_CODE(OB_UNIS_ENCODE,
      thread_data_.cur_query_start_time_,
      user_priv_set_,
      db_priv_set_,
      trans_type_,
      global_sessid_,
      inner_flag_,
      is_max_availability_mode_,
      session_type_,
      has_temp_table_flag_,
      enable_early_lock_release_,
      enable_role_array_,
      in_definer_named_proc_,
      priv_user_id_,
      xa_end_timeout_seconds_,
      prelock_,
      proxy_version_,
      min_proxy_version_ps_,
      thread_data_.is_in_retry_,
      ddl_info_,
      gtt_session_scope_unique_id_,
      gtt_trans_scope_unique_id_,
      gtt_session_scope_ids_,
      gtt_trans_scope_ids_,
      unit_gc_min_sup_proxy_version_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSQLSessionInfo42x)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObSQLSessionInfo, ObBasicSessionInfo));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      thread_data_.cur_query_start_time_,
      user_priv_set_,
      db_priv_set_,
      trans_type_,
      global_sessid_,
      inner_flag_,
      is_max_availability_mode_,
      session_type_,
      has_temp_table_flag_,
      enable_early_lock_release_,
      enable_role_array_,
      in_definer_named_proc_,
      priv_user_id_,
      xa_end_timeout_seconds_,
      prelock_,
      proxy_version_,
      min_proxy_version_ps_,
      thread_data_.is_in_retry_,
      ddl_info_,
      gtt_session_scope_unique_id_,
      gtt_trans_scope_unique_id_,
      gtt_session_scope_ids_,
      gtt_trans_scope_ids_,
      unit_gc_min_sup_proxy_version_);
  return len;
}

int ObSQLSessionInfo42x::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int version = 0;
  int len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  BASE_DESER((ObSQLSessionInfo, ObBasicSessionInfo));
  LST_DO_CODE(OB_UNIS_DECODE,
      thread_data_.cur_query_start_time_,
      user_priv_set_,
      db_priv_set_,
      trans_type_,
      global_sessid_,
      inner_flag_,
      is_max_availability_mode_,
      session_type_,
      has_temp_table_flag_,
      enable_early_lock_release_,
      enable_role_array_,
      in_definer_named_proc_,
      priv_user_id_,
      xa_end_timeout_seconds_,
      prelock_,
      proxy_version_,
      min_proxy_version_ps_,
      thread_data_.is_in_retry_,
      ddl_info_,
      gtt_session_scope_unique_id_,
      gtt_trans_scope_unique_id_,
      gtt_session_scope_ids_,
      gtt_trans_scope_ids_);
  if (version == 3) {
    int64_t affect_rows = 0;
    OB_UNIS_DECODE(affect_rows);
  }
  OB_UNIS_DECODE(unit_gc_min_sup_proxy_version_);
  (void)ObSQLUtils::adjust_time_by_ntp_offset(thread_data_.cur_query_start_time_);
  return ret;
}

class TestSQLSessionInfoSerializeCompat : public ::testing::Test
{
public:
  TestSQLSessionInfoSerializeCompat() {}
  virtual ~TestSQLSessionInfoSerializeCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void fill_session_info(ObSQLSessionInfo &sess)
{
  sess.thread_data_.cur_query_start_time_ = 1234567890;
  sess.user_priv_set_ = 10;
  sess.db_priv_set_ = 2;
  sess.trans_type_ = transaction::ObTxClass::USER;
  sess.global_sessid_ = 1001;
  sess.inner_flag_ = false;
  sess.is_max_availability_mode_ = false;
  sess.session_type_ = ObSQLSessionInfo::USER_SESSION;
  sess.has_temp_table_flag_ = false;
  sess.enable_early_lock_release_ = false;
  sess.in_definer_named_proc_ = false;
  sess.priv_user_id_ = 1001;
  sess.xa_end_timeout_seconds_ = 60;
  sess.prelock_ = false;
  sess.proxy_version_ = 1001;
  sess.min_proxy_version_ps_ = 1001;
  sess.ddl_info_ = ObSessionDDLInfo();
  sess.gtt_session_scope_unique_id_ = 1001;
  sess.gtt_trans_scope_unique_id_ = 1001;
  sess.txn_free_route_ctx_ = transaction::ObTxnFreeRouteCtx();
  sess.cur_exec_ctx_ = nullptr;
  sess.restore_auto_commit_ = false;
  sess.sql_req_level_ = 0;
  sess.gtt_session_scope_ids_.reset();
  sess.gtt_trans_scope_ids_.reset();
  sess.unit_gc_min_sup_proxy_version_ = 1001;
}

void verify_basic_fields_equal(const ObSQLSessionInfo42x &sess1, const ObSQLSessionInfo &sess2)
{
  LOG_INFO("verify_basic_fields_equal", K(sess1.thread_data_.cur_query_start_time_), K(sess2.thread_data_.cur_query_start_time_));
  ASSERT_EQ(sess1.thread_data_.cur_query_start_time_, sess2.thread_data_.cur_query_start_time_);
  ASSERT_EQ(sess1.user_priv_set_, sess2.user_priv_set_);
  ASSERT_EQ(sess1.db_priv_set_, sess2.db_priv_set_);
  ASSERT_EQ(sess1.trans_type_, sess2.trans_type_);
  ASSERT_EQ(sess1.global_sessid_, sess2.global_sessid_);
  ASSERT_EQ(sess1.inner_flag_, sess2.inner_flag_);
  ASSERT_EQ(sess1.is_max_availability_mode_, sess2.is_max_availability_mode_);
  ASSERT_EQ(sess1.session_type_, sess2.session_type_);
  ASSERT_EQ(sess1.has_temp_table_flag_, sess2.has_temp_table_flag_);
  ASSERT_EQ(sess1.enable_early_lock_release_, sess2.enable_early_lock_release_);
  ASSERT_EQ(sess1.in_definer_named_proc_, sess2.in_definer_named_proc_);
  ASSERT_EQ(sess1.priv_user_id_, sess2.priv_user_id_);
  ASSERT_EQ(sess1.xa_end_timeout_seconds_, sess2.xa_end_timeout_seconds_);
  ASSERT_EQ(sess1.prelock_, sess2.prelock_);
  ASSERT_EQ(sess1.proxy_version_, sess2.proxy_version_);
  ASSERT_EQ(sess1.min_proxy_version_ps_, sess2.min_proxy_version_ps_);
  ASSERT_EQ(sess1.thread_data_.is_in_retry_, sess2.thread_data_.is_in_retry_);
  ASSERT_EQ(sess1.gtt_trans_scope_unique_id_, sess2.gtt_trans_scope_unique_id_);
  LOG_INFO("verify_basic_fields_equal", K(sess1.gtt_trans_scope_unique_id_), K(sess2.gtt_trans_scope_unique_id_));
  ASSERT_EQ(sess1.unit_gc_min_sup_proxy_version_, sess2.unit_gc_min_sup_proxy_version_);
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_42x_to_master)
{
  ObSQLSessionInfo42x sess_42x;
  ObSQLSessionInfo sess_master(500);
  init_set(sess_master);

  // 序列化42x版本
  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_42x.serialize(buf, sizeof(buf), pos));

  // 反序列化到master版本
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_master.deserialize(buf, pos, deserialize_pos));

  // 验证基本字段
  verify_basic_fields_equal(sess_42x, sess_master);
  sess_master.cached_tenant_config_info_.session_ = nullptr;
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_master_to_42x)
{
  ObSQLSessionInfo sess_master(500);
  sess_master.cached_tenant_config_info_.session_ = nullptr;
  ObSQLSessionInfo42x sess_42x(500);
  LOG_INFO("verify_basic_fields_equal", K(sess_master.thread_data_.cur_query_start_time_), K(sess_42x.thread_data_.cur_query_start_time_));
  init_set(sess_master);
  LOG_INFO("verify_basic_fields_equal", K(sess_master.thread_data_.cur_query_start_time_), K(sess_42x.thread_data_.cur_query_start_time_));
  // 序列化master版本
  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_master.serialize(buf, sizeof(buf), pos));

  // 反序列化到42x版本
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_42x.deserialize(buf, pos, deserialize_pos));

  // 验证基本字段
  verify_basic_fields_equal(sess_42x, sess_master);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}