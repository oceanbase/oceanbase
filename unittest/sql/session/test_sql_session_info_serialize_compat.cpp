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
#include "sql/engine/ob_physical_plan.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_profiler.h"
#include "pl/ob_pl_code_coverage.h"
#endif

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

struct DasSplitSections
{
  DasSplitSections()
      : inv_(nullptr),
        inv_len_(0),
        inv_basic_(nullptr),
        inv_basic_len_(0),
        inv_sql_(nullptr),
        inv_sql_len_(0),
        inv_tail_(nullptr),
        inv_tail_len_(0),
        var_(nullptr),
        var_len_(0),
        var_basic_(nullptr),
        var_basic_len_(0),
        var_sql_(nullptr),
        var_sql_len_(0),
        var_tail_(nullptr),
        var_tail_len_(0)
  {}
  const char *inv_;
  int64_t inv_len_;
  const char *inv_basic_;
  int64_t inv_basic_len_;
  const char *inv_sql_;
  int64_t inv_sql_len_;
  const char *inv_tail_;
  int64_t inv_tail_len_;
  const char *var_;
  int64_t var_len_;
  const char *var_basic_;
  int64_t var_basic_len_;
  const char *var_sql_;
  int64_t var_sql_len_;
  const char *var_tail_;
  int64_t var_tail_len_;
};

int parse_das_layered_section(const char *section,
                              const int64_t section_len,
                              const char *&basic,
                              int64_t &basic_len,
                              const char *&sql,
                              int64_t &sql_len,
                              const char *&tail,
                              int64_t &tail_len)
{
  int ret = OB_SUCCESS;
  const int64_t fixed_len = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  int64_t pos = 0;
  basic = nullptr;
  basic_len = 0;
  sql = nullptr;
  sql_len = 0;
  tail = nullptr;
  tail_len = 0;
  if (OB_ISNULL(section) || section_len < 2 * fixed_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t basic_len_begin = pos;
    if (OB_FAIL(serialization::decode_vi64(section, section_len, pos, &basic_len))) {
    } else if (OB_UNLIKELY(fixed_len != pos - basic_len_begin
                           || basic_len < 0
                           || pos > section_len
                           || basic_len > section_len - pos)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      basic = section + pos;
      pos += basic_len;
      const int64_t sql_len_begin = pos;
      if (OB_UNLIKELY(pos >= section_len)) {
        ret = OB_DESERIALIZE_ERROR;
      } else if (OB_FAIL(serialization::decode_vi64(section, section_len, pos, &sql_len))) {
      } else if (OB_UNLIKELY(fixed_len != pos - sql_len_begin
                             || sql_len < 0
                             || pos > section_len
                             || sql_len > section_len - pos)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        sql = section + pos;
        pos += sql_len;
        tail = section + pos;
        tail_len = section_len - pos;
      }
    }
  }
  return ret;
}

int parse_das_split_sections(const char *buf, const int64_t data_len, DasSplitSections &sections)
{
  int ret = OB_SUCCESS;
  const int64_t fixed_len = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  int64_t pos = 0;
  int64_t inv_len = 0;
  int64_t var_len = 0;
  if (OB_ISNULL(buf) || data_len < 2 * fixed_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t inv_len_begin = pos;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &inv_len))) {
    } else if (OB_UNLIKELY(fixed_len != pos - inv_len_begin
                           || inv_len < 0
                           || pos > data_len
                           || inv_len > data_len - pos)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    sections.inv_ = buf + pos;
    sections.inv_len_ = inv_len;
    pos += inv_len;
    const int64_t var_len_begin = pos;
    if (OB_UNLIKELY(pos >= data_len)) {
      ret = OB_DESERIALIZE_ERROR;
    } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &var_len))) {
    } else if (OB_UNLIKELY(fixed_len != pos - var_len_begin
                           || var_len < 0
                           || pos > data_len
                           || var_len != data_len - pos)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sections.var_ = buf + pos;
      sections.var_len_ = var_len;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_das_layered_section(sections.inv_,
                                               sections.inv_len_,
                                               sections.inv_basic_,
                                               sections.inv_basic_len_,
                                               sections.inv_sql_,
                                               sections.inv_sql_len_,
                                               sections.inv_tail_,
                                               sections.inv_tail_len_))) {
  } else if (OB_FAIL(parse_das_layered_section(sections.var_,
                                               sections.var_len_,
                                               sections.var_basic_,
                                               sections.var_basic_len_,
                                               sections.var_sql_,
                                               sections.var_sql_len_,
                                               sections.var_tail_,
                                               sections.var_tail_len_))) {
  }
  return ret;
}


int append_das_split_unknown_tails(const char *src,
                                   const int64_t src_len,
                                   const char *inv_tail,
                                   const int64_t inv_tail_len,
                                   const char *var_tail,
                                   const int64_t var_tail_len,
                                   char *dst,
                                   const int64_t dst_len,
                                   int64_t &dst_pos)
{
  int ret = OB_SUCCESS;
  DasSplitSections sections;
  const int64_t len_bytes = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  dst_pos = 0;
  if (OB_ISNULL(src) || OB_ISNULL(dst)
      || inv_tail_len < 0 || var_tail_len < 0
      || (inv_tail_len > 0 && OB_ISNULL(inv_tail))
      || (var_tail_len > 0 && OB_ISNULL(var_tail))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_das_split_sections(src, src_len, sections))) {
  } else {
    const int64_t max_fixed_value = static_cast<int64_t>(serialization::OB_MAX_V5B);
    if (OB_UNLIKELY(inv_tail_len > max_fixed_value - sections.inv_len_
                    || var_tail_len > max_fixed_value - sections.var_len_)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      const int64_t new_inv_len = sections.inv_len_ + inv_tail_len;
      const int64_t new_var_len = sections.var_len_ + var_tail_len;
      const int64_t required_len = 2 * len_bytes + new_inv_len + new_var_len;
      if (OB_UNLIKELY(dst_len < 0 || required_len > dst_len)) {
        ret = OB_SIZE_OVERFLOW;
      }
      int64_t tmp_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(serialization::encode_fixed_bytes_i64(
              dst + dst_pos, len_bytes, tmp_pos, new_inv_len))) {
      } else {
        dst_pos += len_bytes;
        MEMCPY(dst + dst_pos, sections.inv_, sections.inv_len_);
        dst_pos += sections.inv_len_;
        if (inv_tail_len > 0) {
          MEMCPY(dst + dst_pos, inv_tail, inv_tail_len);
          dst_pos += inv_tail_len;
        }
        tmp_pos = 0;
        if (OB_FAIL(serialization::encode_fixed_bytes_i64(
                dst + dst_pos, len_bytes, tmp_pos, new_var_len))) {
        } else {
          dst_pos += len_bytes;
          MEMCPY(dst + dst_pos, sections.var_, sections.var_len_);
          dst_pos += sections.var_len_;
          if (var_tail_len > 0) {
            MEMCPY(dst + dst_pos, var_tail, var_tail_len);
            dst_pos += var_tail_len;
          }
        }
      }
    }
  }
  return ret;
}

struct DasBlockExtensions
{
  DasBlockExtensions()
      : inv_basic_(nullptr),
        inv_basic_len_(0),
        inv_sql_(nullptr),
        inv_sql_len_(0),
        var_basic_(nullptr),
        var_basic_len_(0),
        var_sql_(nullptr),
        var_sql_len_(0)
  {}
  const char *inv_basic_;
  int64_t inv_basic_len_;
  const char *inv_sql_;
  int64_t inv_sql_len_;
  const char *var_basic_;
  int64_t var_basic_len_;
  const char *var_sql_;
  int64_t var_sql_len_;
};

int append_das_test_length_prefix(char *dst,
                                  const int64_t dst_len,
                                  int64_t &dst_pos,
                                  const int64_t value)
{
  int ret = OB_SUCCESS;
  const int64_t fixed_len = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  if (OB_ISNULL(dst)
      || dst_len < 0
      || dst_pos < 0
      || dst_pos > dst_len
      || fixed_len > dst_len - dst_pos
      || value < 0
      || value > static_cast<int64_t>(serialization::OB_MAX_V5B)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t tmp_pos = 0;
    if (OB_FAIL(serialization::encode_fixed_bytes_i64(
            dst + dst_pos, fixed_len, tmp_pos, value))) {
    } else if (fixed_len != tmp_pos) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      dst_pos += fixed_len;
    }
  }
  return ret;
}

int append_das_test_bytes(char *dst,
                          const int64_t dst_len,
                          int64_t &dst_pos,
                          const char *src,
                          const int64_t src_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dst)
      || src_len < 0
      || (src_len > 0 && OB_ISNULL(src))
      || dst_pos < 0
      || dst_pos > dst_len
      || src_len > dst_len - dst_pos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (src_len > 0) {
    MEMCPY(dst + dst_pos, src, src_len);
    dst_pos += src_len;
  }
  return ret;
}

int append_das_phase_block_extensions(const char *basic,
                                      const int64_t basic_len,
                                      const char *sql,
                                      const int64_t sql_len,
                                      const char *section_tail,
                                      const int64_t section_tail_len,
                                      const char *basic_extension,
                                      const int64_t basic_extension_len,
                                      const char *sql_extension,
                                      const int64_t sql_extension_len,
                                      char *dst,
                                      const int64_t dst_len,
                                      int64_t &dst_pos)
{
  int ret = OB_SUCCESS;
  const int64_t max_fixed_value = static_cast<int64_t>(serialization::OB_MAX_V5B);
  if (basic_len < 0
      || sql_len < 0
      || section_tail_len < 0
      || basic_extension_len < 0
      || sql_extension_len < 0
      || basic_extension_len > max_fixed_value - basic_len
      || sql_extension_len > max_fixed_value - sql_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(append_das_test_length_prefix(
                 dst, dst_len, dst_pos, basic_len + basic_extension_len))) {
  } else if (OB_FAIL(append_das_test_bytes(dst, dst_len, dst_pos, basic, basic_len))) {
  } else if (OB_FAIL(append_das_test_bytes(
                 dst, dst_len, dst_pos, basic_extension, basic_extension_len))) {
  } else if (OB_FAIL(append_das_test_length_prefix(
                 dst, dst_len, dst_pos, sql_len + sql_extension_len))) {
  } else if (OB_FAIL(append_das_test_bytes(dst, dst_len, dst_pos, sql, sql_len))) {
  } else if (OB_FAIL(append_das_test_bytes(
                 dst, dst_len, dst_pos, sql_extension, sql_extension_len))) {
  } else if (OB_FAIL(append_das_test_bytes(
                 dst, dst_len, dst_pos, section_tail, section_tail_len))) {
  }
  return ret;
}

int append_das_split_block_extensions(const char *src,
                                      const int64_t src_len,
                                      const DasBlockExtensions &extensions,
                                      char *dst,
                                      const int64_t dst_len,
                                      int64_t &dst_pos)
{
  int ret = OB_SUCCESS;
  DasSplitSections sections;
  dst_pos = 0;
  if (OB_ISNULL(src)
      || OB_ISNULL(dst)
      || extensions.inv_basic_len_ < 0
      || extensions.inv_sql_len_ < 0
      || extensions.var_basic_len_ < 0
      || extensions.var_sql_len_ < 0
      || (extensions.inv_basic_len_ > 0 && OB_ISNULL(extensions.inv_basic_))
      || (extensions.inv_sql_len_ > 0 && OB_ISNULL(extensions.inv_sql_))
      || (extensions.var_basic_len_ > 0 && OB_ISNULL(extensions.var_basic_))
      || (extensions.var_sql_len_ > 0 && OB_ISNULL(extensions.var_sql_))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_das_split_sections(src, src_len, sections))) {
  } else {
    const int64_t max_fixed_value = static_cast<int64_t>(serialization::OB_MAX_V5B);
    if (extensions.inv_basic_len_ > max_fixed_value - extensions.inv_sql_len_
        || extensions.var_basic_len_ > max_fixed_value - extensions.var_sql_len_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      const int64_t inv_extension_len = extensions.inv_basic_len_ + extensions.inv_sql_len_;
      const int64_t var_extension_len = extensions.var_basic_len_ + extensions.var_sql_len_;
      if (inv_extension_len > max_fixed_value - sections.inv_len_
          || var_extension_len > max_fixed_value - sections.var_len_) {
        ret = OB_SIZE_OVERFLOW;
      }
      const int64_t new_inv_len = sections.inv_len_ + inv_extension_len;
      const int64_t new_var_len = sections.var_len_ + var_extension_len;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(append_das_test_length_prefix(dst, dst_len, dst_pos, new_inv_len))) {
      } else if (OB_FAIL(append_das_phase_block_extensions(
                     sections.inv_basic_,
                     sections.inv_basic_len_,
                     sections.inv_sql_,
                     sections.inv_sql_len_,
                     sections.inv_tail_,
                     sections.inv_tail_len_,
                     extensions.inv_basic_,
                     extensions.inv_basic_len_,
                     extensions.inv_sql_,
                     extensions.inv_sql_len_,
                     dst,
                     dst_len,
                     dst_pos))) {
      } else if (OB_FAIL(append_das_test_length_prefix(dst, dst_len, dst_pos, new_var_len))) {
      } else if (OB_FAIL(append_das_phase_block_extensions(
                     sections.var_basic_,
                     sections.var_basic_len_,
                     sections.var_sql_,
                     sections.var_sql_len_,
                     sections.var_tail_,
                     sections.var_tail_len_,
                     extensions.var_basic_,
                     extensions.var_basic_len_,
                     extensions.var_sql_,
                     extensions.var_sql_len_,
                     dst,
                     dst_len,
                     dst_pos))) {
      }
    }
  }
  return ret;
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
  ObPhysicalPlan user_var_plan;
  ObSEArray<ObVarInfo, 1> plan_vars;
  ObVarInfo user_var_info;
  user_var_info.type_ = USER_VAR;
  user_var_info.name_ = ObString::make_string("my_uvar");
  ASSERT_EQ(OB_SUCCESS, plan_vars.push_back(user_var_info));
  ASSERT_EQ(OB_SUCCESS, user_var_plan.set_vars(plan_vars));

  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_src(500);
  ASSERT_EQ(OB_SUCCESS, sess_src.test_init(0, 0, 0, &allocator));
  init_set(sess_src);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(sess_src);
  sess_src.thread_data_.cur_query_start_time_ = 1234567890;
  sess_src.set_cur_sql_id(const_cast<char *>("0123456789abcdef0123456789abcdef"));
  ASSERT_EQ(OB_SUCCESS, sess_src.set_cur_phy_plan(&user_var_plan));
  // Exercise a plan-referenced user variable in the volatile section.
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
  common::ObArenaAllocator sess_ref_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_ref(500);
  ASSERT_EQ(OB_SUCCESS, sess_ref.test_init(0, 0, 0, &sess_ref_allocator));
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
  ASSERT_EQ(0, sess_templ.get_user_var_val_map().size());

  // ---- apply into a working session + decode the volatile suffix ----
  common::ObArenaAllocator sess_app_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_app(500);
  ASSERT_EQ(OB_SUCCESS, sess_app.test_init(0, 0, 0, &sess_app_allocator));
  sess_app.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, sess_app.das_apply_invariant_section(sess_templ));
  ASSERT_EQ(0, sess_app.get_user_var_val_map().size());
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
  ObSessionVariable decoded_uv;
  ASSERT_EQ(OB_SUCCESS,
            sess_app.get_user_variable(ObString::make_string("my_uvar"), decoded_uv));
  ASSERT_TRUE(decoded_uv.meta_.is_int());
  ASSERT_TRUE(decoded_uv.value_.is_int());
  ASSERT_EQ(42, decoded_uv.value_.get_int());

  // ---- compare: legacy-serialize ref and applied, bytes must match ----
  char *reser_ref = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *reser_app = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(reser_ref != nullptr && reser_app != nullptr);
  int64_t reser_ref_len = 0;
  int64_t reser_app_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_ref.set_cur_phy_plan(&user_var_plan));
  ASSERT_EQ(OB_SUCCESS, sess_app.set_cur_phy_plan(&user_var_plan));
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

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_ignore_unknown_section_tails)
{
  common::ObArenaAllocator src_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_src(500);
  ASSERT_EQ(OB_SUCCESS, sess_src.test_init(0, 0, 0, &src_allocator));
  init_set(sess_src);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(sess_src);
  sess_src.thread_data_.cur_query_start_time_ = 1234567890;
  sess_src.set_cur_sql_id(const_cast<char *>("0123456789abcdef0123456789abcdef"));

  char *split_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *future_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(OB_NOT_NULL(split_buf) && OB_NOT_NULL(future_buf));
  int64_t split_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split(split_buf, BUF_SIZE, split_len));

  const char inv_tail[] = {static_cast<char>(0x80), 'I', 'N', 'V', 0, static_cast<char>(0xff)};
  const char var_tail[] = {static_cast<char>(0x81), 'V', 'A', 'R', 0, static_cast<char>(0xfe), 1};
  int64_t future_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            append_das_split_unknown_tails(split_buf,
                                           split_len,
                                           inv_tail,
                                           sizeof(inv_tail),
                                           var_tail,
                                           sizeof(var_tail),
                                           future_buf,
                                           BUF_SIZE,
                                           future_len));

  DasSplitSections current_sections;
  DasSplitSections future_sections;
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(split_buf, split_len, current_sections));
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(future_buf, future_len, future_sections));
  ASSERT_EQ(0, current_sections.inv_tail_len_);
  ASSERT_EQ(0, current_sections.var_tail_len_);
  ASSERT_EQ(sizeof(inv_tail), future_sections.inv_tail_len_);
  ASSERT_EQ(sizeof(var_tail), future_sections.var_tail_len_);
  ASSERT_EQ(0, MEMCMP(inv_tail, future_sections.inv_tail_, sizeof(inv_tail)));
  ASSERT_EQ(0, MEMCMP(var_tail, future_sections.var_tail_, sizeof(var_tail)));
  ASSERT_EQ(current_sections.inv_basic_len_, future_sections.inv_basic_len_);
  ASSERT_EQ(current_sections.inv_sql_len_, future_sections.inv_sql_len_);
  ASSERT_EQ(current_sections.var_basic_len_, future_sections.var_basic_len_);
  ASSERT_EQ(current_sections.var_sql_len_, future_sections.var_sql_len_);

  ObSQLSessionInfo strict_templ(500);
  init_set(strict_templ);
  strict_templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t strict_inv_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            strict_templ.das_build_template_invariant_section(
                current_sections.inv_, current_sections.inv_len_, strict_inv_pos));
  ASSERT_EQ(current_sections.inv_len_, strict_inv_pos);

  common::ObArenaAllocator strict_working_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo strict_working(500);
  ASSERT_EQ(OB_SUCCESS, strict_working.test_init(0, 0, 0, &strict_working_allocator));
  strict_working.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, strict_working.das_apply_invariant_section(strict_templ));
  int64_t strict_var_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            strict_working.das_decode_volatile_section(
                current_sections.var_, current_sections.var_len_, strict_var_pos));
  ASSERT_EQ(current_sections.var_len_, strict_var_pos);

  ObSQLSessionInfo compat_templ(500);
  init_set(compat_templ);
  compat_templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t compat_inv_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            compat_templ.das_build_template_invariant_section(
                future_sections.inv_, future_sections.inv_len_, compat_inv_pos));
  ASSERT_EQ(future_sections.inv_len_, compat_inv_pos);

  common::ObArenaAllocator compat_working_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo compat_working(500);
  ASSERT_EQ(OB_SUCCESS, compat_working.test_init(0, 0, 0, &compat_working_allocator));
  compat_working.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, compat_working.das_apply_invariant_section(compat_templ));
  int64_t compat_var_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            compat_working.das_decode_volatile_section(
                future_sections.var_, future_sections.var_len_, compat_var_pos));
  ASSERT_EQ(future_sections.var_len_, compat_var_pos);

  ASSERT_EQ(strict_working.user_priv_set_, compat_working.user_priv_set_);
  ASSERT_EQ(strict_working.db_priv_set_, compat_working.db_priv_set_);
  ASSERT_EQ(strict_working.global_sessid_, compat_working.global_sessid_);
  ASSERT_EQ(strict_working.thread_data_.cur_query_start_time_,
            compat_working.thread_data_.cur_query_start_time_);
  ASSERT_EQ(strict_working.get_cur_sql_id(), compat_working.get_cur_sql_id());

  strict_working.das_detach_borrowed_sys_vars();
  compat_working.das_detach_borrowed_sys_vars();
  ob_free(split_buf);
  ob_free(future_buf);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  strict_templ.cached_tenant_config_info_.session_ = nullptr;
  strict_working.cached_tenant_config_info_.session_ = nullptr;
  compat_templ.cached_tenant_config_info_.session_ = nullptr;
  compat_working.cached_tenant_config_info_.session_ = nullptr;
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_basic_extension_independent_evolution)
{
  common::ObArenaAllocator current_src_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo current_src(500);
  ASSERT_EQ(OB_SUCCESS, current_src.test_init(0, 0, 0, &current_src_allocator));
  init_set(current_src);
  current_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(current_src);
  current_src.thread_data_.cur_query_start_time_ = 2233445566;
  current_src.set_cur_sql_id(const_cast<char *>("fedcba9876543210fedcba9876543210"));

  char *future_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *current_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(OB_NOT_NULL(future_buf) && OB_NOT_NULL(current_buf));
  int64_t current_len = 0;
  ASSERT_EQ(OB_SUCCESS, current_src.das_serialize_split(current_buf, BUF_SIZE, current_len));

  const int64_t future_inv = 0x123456789;
  const int64_t future_var = 0x23456789A;
  char future_inv_buf[16];
  char future_var_buf[16];
  int64_t future_inv_len = 0;
  int64_t future_var_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(
                future_inv_buf, sizeof(future_inv_buf), future_inv_len, future_inv));
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(
                future_var_buf, sizeof(future_var_buf), future_var_len, future_var));
  DasBlockExtensions extensions;
  extensions.inv_basic_ = future_inv_buf;
  extensions.inv_basic_len_ = future_inv_len;
  extensions.var_basic_ = future_var_buf;
  extensions.var_basic_len_ = future_var_len;
  int64_t future_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            append_das_split_block_extensions(
                current_buf, current_len, extensions, future_buf, BUF_SIZE, future_len));
  ASSERT_EQ(current_len + future_inv_len + future_var_len, future_len);

  DasSplitSections future_sections;
  DasSplitSections current_sections;
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(future_buf, future_len, future_sections));
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(current_buf, current_len, current_sections));
  ASSERT_EQ(0, future_sections.inv_tail_len_);
  ASSERT_EQ(0, future_sections.var_tail_len_);
  ASSERT_EQ(current_sections.inv_basic_len_ + future_inv_len,
            future_sections.inv_basic_len_);
  ASSERT_EQ(current_sections.var_basic_len_ + future_var_len,
            future_sections.var_basic_len_);
  ASSERT_EQ(0,
            MEMCMP(current_sections.inv_basic_,
                   future_sections.inv_basic_,
                   current_sections.inv_basic_len_));
  ASSERT_EQ(0,
            MEMCMP(current_sections.var_basic_,
                   future_sections.var_basic_,
                   current_sections.var_basic_len_));
  ASSERT_EQ(0,
            MEMCMP(future_inv_buf,
                   future_sections.inv_basic_ + current_sections.inv_basic_len_,
                   future_inv_len));
  ASSERT_EQ(0,
            MEMCMP(future_var_buf,
                   future_sections.var_basic_ + current_sections.var_basic_len_,
                   future_var_len));
  ASSERT_EQ(current_sections.inv_sql_len_, future_sections.inv_sql_len_);
  ASSERT_EQ(current_sections.var_sql_len_, future_sections.var_sql_len_);
  ASSERT_EQ(0,
            MEMCMP(current_sections.inv_sql_,
                   future_sections.inv_sql_,
                   current_sections.inv_sql_len_));
  ASSERT_EQ(0,
            MEMCMP(current_sections.var_sql_,
                   future_sections.var_sql_,
                   current_sections.var_sql_len_));

  ObSQLSessionInfo current_templ(500);
  init_set(current_templ);
  current_templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t current_inv_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            current_templ.das_build_template_invariant_section(
                future_sections.inv_, future_sections.inv_len_, current_inv_pos));
  ASSERT_EQ(future_sections.inv_len_, current_inv_pos);
  common::ObArenaAllocator current_working_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo current_working(500);
  ASSERT_EQ(OB_SUCCESS, current_working.test_init(0, 0, 0, &current_working_allocator));
  current_working.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, current_working.das_apply_invariant_section(current_templ));
  int64_t current_var_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            current_working.das_decode_volatile_section(
                future_sections.var_, future_sections.var_len_, current_var_pos));
  ASSERT_EQ(future_sections.var_len_, current_var_pos);
  ASSERT_EQ(current_src.user_priv_set_, current_working.user_priv_set_);
  ASSERT_EQ(current_src.global_sessid_, current_working.global_sessid_);
  ASSERT_EQ(current_src.get_cur_sql_id(), current_working.get_cur_sql_id());

  current_working.das_detach_borrowed_sys_vars();
  ob_free(future_buf);
  ob_free(current_buf);
  current_src.cached_tenant_config_info_.session_ = nullptr;
  current_templ.cached_tenant_config_info_.session_ = nullptr;
  current_working.cached_tenant_config_info_.session_ = nullptr;
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_basic_and_sql_extension_evolution)
{
  common::ObArenaAllocator current_src_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo current_src(500);
  ASSERT_EQ(OB_SUCCESS, current_src.test_init(0, 0, 0, &current_src_allocator));
  init_set(current_src);
  current_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(current_src);
  current_src.thread_data_.cur_query_start_time_ = 9988776655;
  current_src.set_cur_sql_id(const_cast<char *>("00112233445566778899aabbccddeeff"));
  ASSERT_EQ(OB_SUCCESS, current_src.enable_role_array_.push_back(1001));
  ASSERT_EQ(OB_SUCCESS, current_src.enable_role_array_.push_back(1002));

  char *future_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *current_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(OB_NOT_NULL(future_buf) && OB_NOT_NULL(current_buf));
  int64_t current_len = 0;
  ASSERT_EQ(OB_SUCCESS, current_src.das_serialize_split(current_buf, BUF_SIZE, current_len));

  const int64_t future_basic_inv = 1111;
  const int64_t future_basic_var = 2222;
  const int64_t future_sql_inv = 3333;
  const int64_t future_sql_var = 4444;
  char future_basic_inv_buf[16];
  char future_basic_var_buf[16];
  char future_sql_inv_buf[16];
  char future_sql_var_buf[16];
  int64_t future_basic_inv_len = 0;
  int64_t future_basic_var_len = 0;
  int64_t future_sql_inv_len = 0;
  int64_t future_sql_var_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(future_basic_inv_buf,
                                       sizeof(future_basic_inv_buf),
                                       future_basic_inv_len,
                                       future_basic_inv));
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(future_basic_var_buf,
                                       sizeof(future_basic_var_buf),
                                       future_basic_var_len,
                                       future_basic_var));
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(future_sql_inv_buf,
                                       sizeof(future_sql_inv_buf),
                                       future_sql_inv_len,
                                       future_sql_inv));
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(future_sql_var_buf,
                                       sizeof(future_sql_var_buf),
                                       future_sql_var_len,
                                       future_sql_var));
  DasBlockExtensions extensions;
  extensions.inv_basic_ = future_basic_inv_buf;
  extensions.inv_basic_len_ = future_basic_inv_len;
  extensions.var_basic_ = future_basic_var_buf;
  extensions.var_basic_len_ = future_basic_var_len;
  extensions.inv_sql_ = future_sql_inv_buf;
  extensions.inv_sql_len_ = future_sql_inv_len;
  extensions.var_sql_ = future_sql_var_buf;
  extensions.var_sql_len_ = future_sql_var_len;
  int64_t future_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            append_das_split_block_extensions(
                current_buf, current_len, extensions, future_buf, BUF_SIZE, future_len));
  ASSERT_EQ(current_len
                + future_basic_inv_len
                + future_basic_var_len
                + future_sql_inv_len
                + future_sql_var_len,
            future_len);

  DasSplitSections future_sections;
  DasSplitSections current_sections;
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(future_buf, future_len, future_sections));
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(current_buf, current_len, current_sections));
  ASSERT_EQ(current_sections.inv_basic_len_ + future_basic_inv_len,
            future_sections.inv_basic_len_);
  ASSERT_EQ(current_sections.var_basic_len_ + future_basic_var_len,
            future_sections.var_basic_len_);
  ASSERT_EQ(current_sections.inv_sql_len_ + future_sql_inv_len,
            future_sections.inv_sql_len_);
  ASSERT_EQ(current_sections.var_sql_len_ + future_sql_var_len,
            future_sections.var_sql_len_);
  ASSERT_EQ(0,
            MEMCMP(current_sections.inv_basic_,
                   future_sections.inv_basic_,
                   current_sections.inv_basic_len_));
  ASSERT_EQ(0,
            MEMCMP(current_sections.var_basic_,
                   future_sections.var_basic_,
                   current_sections.var_basic_len_));
  ASSERT_EQ(0,
            MEMCMP(current_sections.inv_sql_,
                   future_sections.inv_sql_,
                   current_sections.inv_sql_len_));
  ASSERT_EQ(0,
            MEMCMP(current_sections.var_sql_,
                   future_sections.var_sql_,
                   current_sections.var_sql_len_));
  ASSERT_EQ(0,
            MEMCMP(future_basic_inv_buf,
                   future_sections.inv_basic_ + current_sections.inv_basic_len_,
                   future_basic_inv_len));
  ASSERT_EQ(0,
            MEMCMP(future_basic_var_buf,
                   future_sections.var_basic_ + current_sections.var_basic_len_,
                   future_basic_var_len));
  ASSERT_EQ(0,
            MEMCMP(future_sql_inv_buf,
                   future_sections.inv_sql_ + current_sections.inv_sql_len_,
                   future_sql_inv_len));
  ASSERT_EQ(0,
            MEMCMP(future_sql_var_buf,
                   future_sections.var_sql_ + current_sections.var_sql_len_,
                   future_sql_var_len));

  ObSQLSessionInfo current_templ(500);
  init_set(current_templ);
  current_templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t current_inv_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            current_templ.das_build_template_invariant_section(
                future_sections.inv_, future_sections.inv_len_, current_inv_pos));
  ASSERT_EQ(future_sections.inv_len_, current_inv_pos);
  common::ObArenaAllocator current_working_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo current_working(500);
  ASSERT_EQ(OB_SUCCESS, current_working.test_init(0, 0, 0, &current_working_allocator));
  current_working.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, current_working.das_apply_invariant_section(current_templ));
  int64_t current_var_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            current_working.das_decode_volatile_section(
                future_sections.var_, future_sections.var_len_, current_var_pos));
  ASSERT_EQ(future_sections.var_len_, current_var_pos);
  ASSERT_EQ(current_src.user_priv_set_, current_working.user_priv_set_);
  ASSERT_EQ(current_src.enable_role_array_.count(), current_working.enable_role_array_.count());
  for (int64_t i = 0; i < current_src.enable_role_array_.count(); ++i) {
    EXPECT_EQ(current_src.enable_role_array_.at(i), current_working.enable_role_array_.at(i));
  }
  ASSERT_EQ(current_src.get_cur_sql_id(), current_working.get_cur_sql_id());
  ASSERT_EQ(current_src.thread_data_.cur_query_start_time_,
            current_working.thread_data_.cur_query_start_time_);

  current_working.das_detach_borrowed_sys_vars();
  ob_free(future_buf);
  ob_free(current_buf);
  current_src.cached_tenant_config_info_.session_ = nullptr;
  current_templ.cached_tenant_config_info_.session_ = nullptr;
  current_working.cached_tenant_config_info_.session_ = nullptr;
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_layered_block_accepts_vi64_length_prefixes)
{
  common::ObArenaAllocator src_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_src(500);
  ASSERT_EQ(OB_SUCCESS, sess_src.test_init(0, 0, 0, &src_allocator));
  init_set(sess_src);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(sess_src);

  char *split_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *compat_inv = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(OB_NOT_NULL(split_buf) && OB_NOT_NULL(compat_inv));
  int64_t split_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split(split_buf, BUF_SIZE, split_len));
  DasSplitSections sections;
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(split_buf, split_len, sections));

  int64_t compat_inv_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(
                compat_inv, BUF_SIZE, compat_inv_len, sections.inv_basic_len_));
  ASSERT_EQ(OB_SUCCESS,
            append_das_test_bytes(compat_inv,
                                  BUF_SIZE,
                                  compat_inv_len,
                                  sections.inv_basic_,
                                  sections.inv_basic_len_));
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(
                compat_inv, BUF_SIZE, compat_inv_len, sections.inv_sql_len_));
  ASSERT_EQ(OB_SUCCESS,
            append_das_test_bytes(compat_inv,
                                  BUF_SIZE,
                                  compat_inv_len,
                                  sections.inv_sql_,
                                  sections.inv_sql_len_));
  ASSERT_EQ(OB_SUCCESS,
            append_das_test_bytes(compat_inv,
                                  BUF_SIZE,
                                  compat_inv_len,
                                  sections.inv_tail_,
                                  sections.inv_tail_len_));
  ASSERT_LT(compat_inv_len, sections.inv_len_);

  ObSQLSessionInfo templ(500);
  init_set(templ);
  templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            templ.das_build_template_invariant_section(compat_inv, compat_inv_len, pos));
  ASSERT_EQ(compat_inv_len, pos);
  ASSERT_EQ(sess_src.user_priv_set_, templ.user_priv_set_);
  ASSERT_EQ(sess_src.global_sessid_, templ.global_sessid_);

  ob_free(split_buf);
  ob_free(compat_inv);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  templ.cached_tenant_config_info_.session_ = nullptr;
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_layered_block_malformed_lengths)
{
  common::ObArenaAllocator src_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_src(500);
  ASSERT_EQ(OB_SUCCESS, sess_src.test_init(0, 0, 0, &src_allocator));
  init_set(sess_src);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(sess_src);

  char *split_buf = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(OB_NOT_NULL(split_buf));
  int64_t split_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split(split_buf, BUF_SIZE, split_len));
  DasSplitSections sections;
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(split_buf, split_len, sections));
  const int64_t fixed_len = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  char *mutable_inv = const_cast<char *>(sections.inv_);
  char saved_basic_len[serialization::OB_SERIALIZE_SIZE_NEED_BYTES];
  MEMCPY(saved_basic_len, mutable_inv, fixed_len);

  auto decode_inv = [](const char *buf, int64_t data_len) {
    ObSQLSessionInfo templ(500);
    init_set(templ);
    templ.cached_tenant_config_info_.session_ = nullptr;
    int64_t pos = 0;
    const int ret = templ.das_build_template_invariant_section(buf, data_len, pos);
    templ.cached_tenant_config_info_.session_ = nullptr;
    return ret;
  };

  EXPECT_EQ(OB_DESERIALIZE_ERROR, decode_inv(sections.inv_, 0));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, decode_inv(sections.inv_, fixed_len - 1));

  int64_t tmp_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_fixed_bytes_i64(
                mutable_inv, fixed_len, tmp_pos, sections.inv_len_));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, decode_inv(sections.inv_, sections.inv_len_));
  MEMCPY(mutable_inv, saved_basic_len, fixed_len);

  const int64_t sql_len_pos = fixed_len + sections.inv_basic_len_;
  ASSERT_LE(sql_len_pos + fixed_len, sections.inv_len_);
  char saved_sql_len[serialization::OB_SERIALIZE_SIZE_NEED_BYTES];
  MEMCPY(saved_sql_len, mutable_inv + sql_len_pos, fixed_len);
  tmp_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_fixed_bytes_i64(
                mutable_inv + sql_len_pos, fixed_len, tmp_pos, sections.inv_len_));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, decode_inv(sections.inv_, sections.inv_len_));
  MEMCPY(mutable_inv + sql_len_pos, saved_sql_len, fixed_len);

  EXPECT_EQ(OB_DESERIALIZE_ERROR,
            decode_inv(sections.inv_, sql_len_pos));

  ob_free(split_buf);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
}

TEST_F(TestSQLSessionInfoSerializeCompat, test_das_user_var_in_volatile)
{
  ObPhysicalPlan user_var_plan;
  ObSEArray<ObVarInfo, 1> plan_vars;
  ObVarInfo user_var_info;
  user_var_info.type_ = USER_VAR;
  user_var_info.name_ = ObString::make_string("my_uvar");
  ASSERT_EQ(OB_SUCCESS, plan_vars.push_back(user_var_info));
  ASSERT_EQ(OB_SUCCESS, user_var_plan.set_vars(plan_vars));

  common::ObArenaAllocator src_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo sess_src(500);
  ASSERT_EQ(OB_SUCCESS, sess_src.test_init(0, 0, 0, &src_allocator));
  init_set(sess_src);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  fill_session_info(sess_src);
  ASSERT_EQ(OB_SUCCESS, sess_src.set_cur_phy_plan(&user_var_plan));

  char *split_a = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *split_b = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  char *split_unset = static_cast<char *>(ob_malloc(BUF_SIZE, "test"));
  ASSERT_TRUE(OB_NOT_NULL(split_a) && OB_NOT_NULL(split_b) && OB_NOT_NULL(split_unset));

  const ObString user_var_name = ObString::make_string("my_uvar");
  ObSessionVariable uv;
  uv.meta_.set_int();
  uv.value_.set_int(10);
  ASSERT_EQ(OB_SUCCESS, sess_src.replace_user_variable(user_var_name, uv));
  int64_t split_a_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split(split_a, BUF_SIZE, split_a_len));
  int64_t inv_a_size = 0;
  int64_t var_a_size = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split_size(inv_a_size, var_a_size));
  ASSERT_EQ(split_a_len, inv_a_size + var_a_size);

  uv.value_.set_int(20);
  ASSERT_EQ(OB_SUCCESS, sess_src.replace_user_variable(user_var_name, uv));
  int64_t split_b_len = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split(split_b, BUF_SIZE, split_b_len));
  int64_t inv_b_size = 0;
  int64_t var_b_size = 0;
  ASSERT_EQ(OB_SUCCESS, sess_src.das_serialize_split_size(inv_b_size, var_b_size));
  ASSERT_EQ(split_b_len, inv_b_size + var_b_size);

  ASSERT_EQ(OB_SUCCESS, sess_src.remove_user_variable(user_var_name));
  int64_t split_unset_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            sess_src.das_serialize_split(split_unset, BUF_SIZE, split_unset_len));
  int64_t inv_unset_size = 0;
  int64_t var_unset_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            sess_src.das_serialize_split_size(inv_unset_size, var_unset_size));
  ASSERT_EQ(split_unset_len, inv_unset_size + var_unset_size);

  DasSplitSections sections_a;
  DasSplitSections sections_b;
  DasSplitSections sections_unset;
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(split_a, split_a_len, sections_a));
  ASSERT_EQ(OB_SUCCESS, parse_das_split_sections(split_b, split_b_len, sections_b));
  ASSERT_EQ(OB_SUCCESS,
            parse_das_split_sections(split_unset, split_unset_len, sections_unset));

  ASSERT_EQ(sections_a.inv_len_, sections_b.inv_len_);
  ASSERT_EQ(sections_a.inv_len_, sections_unset.inv_len_);
  ASSERT_EQ(0, MEMCMP(sections_a.inv_, sections_b.inv_, sections_a.inv_len_));
  ASSERT_EQ(0, MEMCMP(sections_a.inv_, sections_unset.inv_, sections_a.inv_len_));
  ASSERT_EQ(sections_a.var_len_, sections_b.var_len_);
  ASSERT_NE(0, MEMCMP(sections_a.var_, sections_b.var_, sections_a.var_len_));
  ASSERT_GT(sections_a.var_len_, sections_unset.var_len_);

  ObSQLSessionInfo sess_templ(500);
  init_set(sess_templ);
  sess_templ.cached_tenant_config_info_.session_ = nullptr;
  int64_t tpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            sess_templ.das_build_template_invariant_section(
                sections_a.inv_, sections_a.inv_len_, tpos));
  ASSERT_EQ(sections_a.inv_len_, tpos);
  ASSERT_EQ(0, sess_templ.get_user_var_val_map().size());

  common::ObArenaAllocator working_allocator(ObModIds::OB_SQL_SESSION);
  ObSQLSessionInfo working(500);
  ASSERT_EQ(OB_SUCCESS, working.test_init(0, 0, 0, &working_allocator));
  working.cached_tenant_config_info_.session_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, working.das_apply_invariant_section(sess_templ));
  ASSERT_EQ(0, working.get_user_var_val_map().size());

  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_begin_pool_request());
  int64_t vpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            working.das_decode_volatile_section(
                sections_a.var_, sections_a.var_len_, vpos));
  ASSERT_EQ(sections_a.var_len_, vpos);
  ObSessionVariable decoded_uv;
  ASSERT_EQ(OB_SUCCESS, working.get_user_variable(user_var_name, decoded_uv));
  ASSERT_TRUE(decoded_uv.meta_.is_int());
  ASSERT_TRUE(decoded_uv.value_.is_int());
  ASSERT_EQ(10, decoded_uv.value_.get_int());
  bool can_return_to_pool = false;
  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_end_pool_request(can_return_to_pool));
  ASSERT_TRUE(can_return_to_pool);
  ASSERT_EQ(0, working.get_user_var_val_map().size());

  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_begin_pool_request());
  vpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            working.das_decode_volatile_section(
                sections_b.var_, sections_b.var_len_, vpos));
  ASSERT_EQ(sections_b.var_len_, vpos);
  ASSERT_EQ(OB_SUCCESS, working.get_user_variable(user_var_name, decoded_uv));
  ASSERT_EQ(20, decoded_uv.value_.get_int());
  can_return_to_pool = false;
  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_end_pool_request(can_return_to_pool));
  ASSERT_TRUE(can_return_to_pool);
  ASSERT_EQ(0, working.get_user_var_val_map().size());

  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_begin_pool_request());
  vpos = 0;
  ASSERT_EQ(OB_SUCCESS,
            working.das_decode_volatile_section(
                sections_unset.var_, sections_unset.var_len_, vpos));
  ASSERT_EQ(sections_unset.var_len_, vpos);
  ASSERT_EQ(0, working.get_user_var_val_map().size());
  ASSERT_EQ(OB_ERR_USER_VARIABLE_UNKNOWN,
            working.get_user_variable(user_var_name, decoded_uv));
  can_return_to_pool = false;
  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_end_pool_request(can_return_to_pool));
  ASSERT_TRUE(can_return_to_pool);

#ifdef OB_BUILD_ORACLE_PL
  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_begin_pool_request());
  ObSessionVariable pkg_uv;
  pkg_uv.meta_.set_int();
  pkg_uv.value_.set_int(1);
  ASSERT_EQ(OB_SUCCESS,
            working.ObBasicSessionInfo::replace_user_variable(
                ObString::make_string("pkg.pool_test"), pkg_uv, false));
  working.pl_profiler_ = OB_NEW(pl::ObPLProfiler, ObMemAttr(500, "PoolProfiler"), working);
  working.pl_code_coverage_ =
      OB_NEW(pl::ObPLCodeCoverage, ObMemAttr(500, "PoolCoverage"), working);
  ASSERT_TRUE(OB_NOT_NULL(working.pl_profiler_));
  ASSERT_TRUE(OB_NOT_NULL(working.pl_code_coverage_));
  can_return_to_pool = false;
  ASSERT_EQ(OB_SUCCESS, working.das_deser_cache_end_pool_request(can_return_to_pool));
  ASSERT_TRUE(can_return_to_pool);
  ASSERT_EQ(0, working.get_user_var_val_map().size());
  ASSERT_TRUE(OB_ISNULL(working.get_pl_profiler()));
  ASSERT_TRUE(OB_ISNULL(working.get_pl_code_coverage()));
#endif

  working.das_detach_borrowed_sys_vars();
  ob_free(split_a);
  ob_free(split_b);
  ob_free(split_unset);
  sess_src.cached_tenant_config_info_.session_ = nullptr;
  sess_templ.cached_tenant_config_info_.session_ = nullptr;
  working.cached_tenant_config_info_.session_ = nullptr;
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
