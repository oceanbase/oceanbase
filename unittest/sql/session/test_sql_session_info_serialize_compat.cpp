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

void init_remote_extra_maps(ObSQLSessionInfo &sess)
{
  ASSERT_EQ(OB_SUCCESS,
            sess.sequence_currval_map_.create(hash::cal_next_prime(32),
                                              ObMemAttr(500, "SequenceMap")));
  ASSERT_EQ(OB_SUCCESS,
            sess.dblink_sequence_id_map_.create(hash::cal_next_prime(32),
                                                ObMemAttr(500, "SequenceIdMap")));
  ASSERT_EQ(OB_SUCCESS,
            sess.contexts_map_.create(hash::cal_next_prime(32),
                                      ObMemAttr(500, "ContextsMap")));
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

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_split_round_trip)
{
  common::ObArenaAllocator sess_src_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_src(500);
  ASSERT_EQ(OB_SUCCESS, sess_src.test_init(0, 0, 0, &sess_src_allocator));
  init_set(sess_src);
  init_remote_extra_maps(sess_src);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(sess_src);
  sess_src.thread_data_.cur_query_start_time_ = 1234567890;
  sess_src.set_cur_sql_id(const_cast<char *>("0123456789abcdef0123456789abcdef"));
  ASSERT_EQ(OB_SUCCESS, sess_src.set_module_name(ObString::make_string("das_module")));
  ASSERT_EQ(OB_SUCCESS, sess_src.set_action_name(ObString::make_string("das_action")));
  ASSERT_EQ(OB_SUCCESS, sess_src.set_client_info(ObString::make_string("das_client_info")));
  ASSERT_EQ(OB_SUCCESS, sess_src.set_client_id(ObString::make_string("das_client_identifier")));
  ASSERT_EQ(OB_SUCCESS,
            sess_src.set_context_values(ObString::make_string("DAS_CTX"),
                                        ObString::make_string("DAS_ATTR"),
                                        ObString::make_string("DAS_VALUE")));
  const uint64_t seq_id = 10001;
  const share::ObSequenceValue seq_value(20260722);
  ASSERT_EQ(OB_SUCCESS, sess_src.set_sequence_value(500, seq_id, seq_value));
  FLTControlInfo control_info;
  control_info.level_ = 1;
  control_info.sample_pct_ = 0.5;
  control_info.rp_ = FLTControlInfo::RP_ALL;
  control_info.print_sample_pct_ = 0.25;
  control_info.slow_query_thres_ = 1000;
  control_info.show_trace_enable_ = true;
  sess_src.set_flt_control_info(control_info);
  sess_src.set_coninfo_set_by_sess(true);

  // ---- reference path: legacy serialize -> legacy deserialize ----
  char *legacy_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(legacy_buf != nullptr);
  int64_t legacy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.serialize(legacy_buf, BUF_SIZE, legacy_pos));
  common::ObArenaAllocator sess_ref_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_ref(500);
  ASSERT_EQ(OB_SUCCESS, sess_ref.test_init(0, 0, 0, &sess_ref_allocator));
  init_set(sess_ref);
  init_remote_extra_maps(sess_ref);
  sess_ref.cached_tenant_config_info_.session_ = nullptr;
  int64_t legacy_des_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_ref.deserialize(legacy_buf, legacy_pos, legacy_des_pos));

  // ---- new path: split serialize -> production split reconstruction ----
  // this mirrors deser_session_with_cache: parse the [len_inv][inv][len_var][var] windows, build a
  // read-only template from the invariant bytes, apply it into the working session, then decode the
  // volatile suffix. (There is deliberately no single "deserialize the whole split blob" entry point:
  // the invariant is always reconstructed from a cached template, never re-decoded inline.)
  char *split_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(split_buf != nullptr);
  int64_t split_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split(split_buf, BUF_SIZE, split_pos));
  int64_t inv_len = 0;
  int64_t var_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split_size(inv_len, var_len));
  ASSERT_EQ(split_pos, inv_len + var_len);  // split body size matches the size twin

  // locate the invariant / volatile section windows inside the split blob
  const int64_t nb = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  int64_t tmp = 0;
  int64_t wire_inv_len = 0;
  serialization::decode_vi64(split_buf, nb, tmp, &wire_inv_len);
  const int64_t inv_begin = nb;
  const int64_t var_len_pos = inv_begin + wire_inv_len;
  tmp = 0;
  int64_t wire_var_len = 0;
  serialization::decode_vi64(split_buf + var_len_pos, nb, tmp, &wire_var_len);
  const int64_t var_begin = var_len_pos + nb;

  // build the read-only template from the invariant section, then reconstruct the working session
  ObSQLSessionInfo sess_templ(500);
  init_set(sess_templ);
  sess_templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t tpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            sess_templ.das_build_template_invariant_section(split_buf + inv_begin, wire_inv_len, tpos));
  ASSERT_EQ(tpos, wire_inv_len);

  common::ObArenaAllocator sess_dst_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_dst(500);
  ASSERT_EQ(OB_SUCCESS, sess_dst.test_init(0, 0, 0, &sess_dst_allocator));
  init_remote_extra_maps(sess_dst);
  sess_dst.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, sess_dst.das_apply_invariant_section(sess_templ));
  ASSERT_TRUE(sess_dst.is_das_borrowed_sys_vars());
  ASSERT_EQ(sess_templ.sys_var_base_version_, sess_dst.sys_var_base_version_);
  ASSERT_FALSE(ObBasicSessionInfo::CACHED_SYS_VAR_VERSION == sess_dst.sys_var_base_version_);
  for (int64_t i = 0; i < share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++i) {
    ASSERT_EQ(sess_templ.sys_vars_[i], sess_dst.sys_vars_[i]);
  }
  int64_t vpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            sess_dst.das_decode_volatile_section(split_buf + var_begin, wire_var_len, vpos));
  ASSERT_EQ(vpos, wire_var_len);
  ASSERT_EQ(var_begin + wire_var_len, split_pos);  // consumed the whole split blob
  ASSERT_EQ(sess_src.get_module_name(), sess_dst.get_module_name());
  ASSERT_EQ(sess_src.get_action_name(), sess_dst.get_action_name());
  ASSERT_EQ(sess_src.get_client_info(), sess_dst.get_client_info());
  ASSERT_EQ(sess_src.get_client_identifier(), sess_dst.get_client_identifier());
  ObString context_value;
  bool context_exists = false;
  ASSERT_EQ(OB_SUCCESS,
            sess_dst.get_context_values(ObString::make_string("DAS_CTX"),
                                        ObString::make_string("DAS_ATTR"),
                                        context_value,
                                        context_exists));
  ASSERT_TRUE(context_exists);
  ASSERT_EQ(ObString::make_string("DAS_VALUE"), context_value);
  share::ObSequenceValue decoded_seq_value;
  ASSERT_EQ(OB_SUCCESS, sess_dst.get_sequence_value(500, seq_id, decoded_seq_value));
  ASSERT_EQ(0, seq_value.val().compare(decoded_seq_value.val()));
  ASSERT_TRUE(sess_src.get_control_info() == sess_dst.get_control_info());
  ASSERT_EQ(sess_src.is_coninfo_set_by_sess(), sess_dst.is_coninfo_set_by_sess());

  // ---- compare: legacy-serialize both deserialized sessions, bytes must match ----
  char *reser_ref = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *reser_dst = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(reser_ref != nullptr && reser_dst != nullptr);
  int64_t reser_ref_len = 0;
  int64_t reser_dst_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_ref.serialize(reser_ref, BUF_SIZE, reser_ref_len));
  ASSERT_EQ(OB_SUCCESS, sess_dst.serialize(reser_dst, BUF_SIZE, reser_dst_len));

  LOG_INFO("[DAS_SPLIT_RT] lengths", K(legacy_pos), K(split_pos), K(inv_len), K(var_len),
           K(reser_ref_len), K(reser_dst_len));
  ASSERT_EQ(reser_ref_len, reser_dst_len);
  ASSERT_EQ(0, MEMCMP(reser_ref, reser_dst, reser_ref_len));

  sess_dst.das_detach_borrowed_sys_vars();
  ASSERT_FALSE(sess_dst.is_das_borrowed_sys_vars());
  for (int64_t i = 0; i < share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++i) {
    ASSERT_EQ(nullptr, sess_dst.sys_vars_[i]);
  }

  ob_free(legacy_buf);
  ob_free(split_buf);
  ob_free(reser_ref);
  ob_free(reser_dst);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  sess_ref.cached_tenant_config_info_.session_ = nullptr;
  sess_dst.cached_tenant_config_info_.session_ = nullptr;
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_apply_invariant)
{
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_src(500);
  ASSERT_EQ(OB_SUCCESS, sess_src.test_init(0, 0, 0, &allocator));
  init_set(sess_src);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(sess_src);
  sess_src.thread_data_.cur_query_start_time_ = 1234567890;
  sess_src.set_cur_sql_id(const_cast<char *>("0123456789abcdef0123456789abcdef"));
  // exercise a user variable (invariant) so apply's user-var loop is covered
  {
    ObSessionVariable uv;
    uv.meta_.set_int();
    uv.value_.set_int(42);
    ASSERT_EQ(OB_SUCCESS, sess_src.replace_user_variable(ObString::make_string("my_uvar"), uv));
  }

  // ---- reference path: legacy serialize -> legacy deserialize ----
  char *legacy_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(legacy_buf != nullptr);
  int64_t legacy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.serialize(legacy_buf, BUF_SIZE, legacy_pos));
  ObSQLSessionInfo sess_ref(500);
  init_set(sess_ref);
  sess_ref.cached_tenant_config_info_.session_ = nullptr;
  int64_t legacy_des_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_ref.deserialize(legacy_buf, legacy_pos, legacy_des_pos));

  // ---- split serialize, then parse the [len_inv][inv][len_var][var] section windows ----
  char *split_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(split_buf != nullptr);
  int64_t split_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split(split_buf, BUF_SIZE, split_pos));
  const int64_t nb = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  int64_t tmp = 0;
  int64_t inv_len = 0;
  int64_t var_len = 0;
  serialization::decode_vi64(split_buf, nb, tmp, &inv_len);
  const int64_t inv_begin = nb;
  const int64_t var_len_pos = inv_begin + inv_len;
  tmp = 0;
  serialization::decode_vi64(split_buf + var_len_pos, nb, tmp, &var_len);
  const int64_t var_begin = var_len_pos + nb;

  // ---- build the read-only template (invariant section only) ----
  ObSQLSessionInfo sess_templ(500);
  init_set(sess_templ);
  sess_templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t tpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            sess_templ.das_build_template_invariant_section(split_buf + inv_begin, inv_len, tpos));
  ASSERT_EQ(tpos, inv_len);

  // ---- apply into a working session + decode the volatile suffix ----
  common::ObArenaAllocator sess_app_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_app(500);
  ASSERT_EQ(OB_SUCCESS, sess_app.test_init(0, 0, 0, &sess_app_allocator));
  sess_app.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, sess_app.das_apply_invariant_section(sess_templ));
  ASSERT_TRUE(sess_app.is_das_borrowed_sys_vars());
  ASSERT_EQ(sess_templ.sys_var_base_version_, sess_app.sys_var_base_version_);
  ASSERT_FALSE(ObBasicSessionInfo::CACHED_SYS_VAR_VERSION == sess_app.sys_var_base_version_);
  for (int64_t i = 0; i < share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++i) {
    ASSERT_EQ(sess_templ.sys_vars_[i], sess_app.sys_vars_[i]);
  }
  int64_t vpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            sess_app.das_decode_volatile_section(split_buf + var_begin, var_len, vpos));
  ASSERT_EQ(vpos, var_len);

  // ---- compare: legacy-serialize ref and applied, bytes must match ----
  char *reser_ref = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *reser_app = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(reser_ref != nullptr && reser_app != nullptr);
  int64_t reser_ref_len = 0;
  int64_t reser_app_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_ref.serialize(reser_ref, BUF_SIZE, reser_ref_len));
  ASSERT_EQ(OB_SUCCESS, sess_app.serialize(reser_app, BUF_SIZE, reser_app_len));

  LOG_INFO("[DAS_APPLY] lengths", K(inv_len), K(var_len), K(reser_ref_len), K(reser_app_len));
  ASSERT_EQ(reser_ref_len, reser_app_len);
  ASSERT_EQ(0, MEMCMP(reser_ref, reser_app, reser_ref_len));

  ObObj new_autocommit;
  new_autocommit.set_int(0);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            sess_app.update_sys_variable(share::SYS_VAR_AUTOCOMMIT, new_autocommit));
  sess_app.das_detach_borrowed_sys_vars();
  ASSERT_FALSE(sess_app.is_das_borrowed_sys_vars());

  ob_free(legacy_buf);
  ob_free(split_buf);
  ob_free(reser_ref);
  ob_free(reser_app);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  sess_ref.cached_tenant_config_info_.session_ = nullptr;
  sess_templ.cached_tenant_config_info_.session_ = nullptr;
  sess_app.cached_tenant_config_info_.session_ = nullptr;
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
