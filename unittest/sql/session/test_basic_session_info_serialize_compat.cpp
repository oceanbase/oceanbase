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
#include "sql/session/ob_basic_session_info.h"
#include "observer/ob_server.h"
#include "lib/utility/utility.h"
#include "common/ob_smart_var.h"
#include "pl/ob_pl_package_state.h"
#include "share/system_variable/ob_system_variable_factory.h"


namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

static const int64_t BUF_SIZE = 1024 * 1024; // 1MB buffer

static const int64_t ALL_SYS_VARS_COUNT = 838;

struct ObDiagnosisInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObDiagnosisInfo()
    : is_enabled_(false),
      limit_num_(0),
      log_file_(),
      bad_file_()
  {}

  void reset() {
    is_enabled_ = false;
    limit_num_ = 0;
    log_file_.reset();
    bad_file_.reset();
  }

  bool is_enabled_;
  int64_t limit_num_;
  common::ObString log_file_;
  common::ObString bad_file_;

  TO_STRING_KV(K(is_enabled_), K(limit_num_), K(log_file_), K(bad_file_));
};

struct ObBasicSessionInfoMaster
{
  OB_UNIS_VERSION(3);

  ObBasicSessionInfoMaster() :
    tenant_id_(500),
    effective_tenant_id_(500),
    rpc_tenant_id_(1001),
    is_changed_to_temp_tenant_(false),
    user_id_(1001),
    client_version_(),
    driver_version_(),
    master_sessid_(INVALID_SESSID),
    client_sessid_(12345),
    proxy_sessid_(54321),
    sys_var_base_version_(CACHED_SYS_VAR_VERSION),
    proxy_user_id_(1002),
    tx_desc_(NULL),
    tx_result_(),
    reserved_read_snapshot_version_(),
    xid_(),
    cached_tenant_config_version_(1),
    total_stmt_tables_(),
    cur_stmt_tables_(),
    sys_var_in_pc_str_(),
    config_in_pc_str_(),
    block_allocator_(ObSessionValMap::SMALL_BLOCK_SIZE, common::OB_MALLOC_NORMAL_BLOCK_SIZE - 32,
      ObMalloc(SET_IGNORE_MEM_VERSION(lib::ObMemAttr(tenant_id_, ObModIds::OB_SQL_SESSION_SBLOCK)))),
    user_var_val_map_(ObSessionValMap::SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_), tenant_id_),
    cur_phy_plan_(NULL),
    sql_id_(),
    next_tx_read_only_(1),
    sql_scope_flags_(),
    client_mode_(ObClientMode::OB_JDBC_CLIENT_MODE),
    consistency_level_(common::STRONG),
    tz_info_wrap_(),
    next_tx_isolation_(transaction::ObTxIsolationLevel::RC),
    enable_mysql_compatible_dates_(false),
    is_diagnosis_enabled_(false),
    diagnosis_limit_num_(0),
    flt_vars_(),
    capability_(),
    proxy_capability_(),
    current_execution_id_(1),
    app_trace_id_(),
    database_id_(1),
    thread_data_(),
    nested_count_(0),
    sys_vars_cache_(),
    check_sys_variable_(true),
    first_need_txn_stmt_type_(stmt::T_NONE),
    exec_min_cluster_version_(0),
    stmt_type_(stmt::T_SELECT),
    labels_(),
    process_query_time_(100),
    is_client_sessid_support_(true),
    use_rich_vector_format_(false),
    sys_var_config_hash_val_(12345)
  {
    for (int i = 0; i < ALL_SYS_VARS_COUNT; ++i) {
      sys_vars_[i] = NULL;
    }
    tz_info_wrap_.set_tz_info_offset(123);

    memset(flt_vars_.last_flt_trace_id_buf_, 0, sizeof(flt_vars_.last_flt_trace_id_buf_));
    memset(flt_vars_.last_flt_span_id_buf_, 0, sizeof(flt_vars_.last_flt_span_id_buf_));
    flt_vars_.row_traceformat_ = false;
  }
  int calc_need_serialize_vars(ObSEArray<share::ObSysVarClassType, 64> &sys_var_ids, ObSEArray<ObString, 32> &user_var_names) const;
  int set_last_flt_span_id(const common::ObString &span_id)
  {
    int ret = OB_SUCCESS;
    if (span_id.empty()) {
      flt_vars_.last_flt_span_id_.reset();
    } else {
      int64_t span_len = std::min(static_cast<int64_t>(span_id.length()), OB_MAX_UUID_STR_LENGTH);
      MEMCPY(flt_vars_.last_flt_span_id_buf_, span_id.ptr(), span_len);
      flt_vars_.last_flt_span_id_buf_[span_len] = '\0';
      flt_vars_.last_flt_span_id_.assign_ptr(flt_vars_.last_flt_span_id_buf_, span_len);
    }
    return ret;
  }

  int set_last_flt_trace_id(const common::ObString &trace_id)
  {
    int ret = OB_SUCCESS;
    if (trace_id.empty()) {
      flt_vars_.last_flt_trace_id_.reset();
    } else {
      int64_t trace_len = std::min(static_cast<int64_t>(trace_id.length()), OB_MAX_UUID_STR_LENGTH);
      MEMCPY(flt_vars_.last_flt_trace_id_buf_, trace_id.ptr(), trace_len);
      flt_vars_.last_flt_trace_id_buf_[trace_len] = '\0';
      flt_vars_.last_flt_trace_id_.assign_ptr(flt_vars_.last_flt_trace_id_buf_, trace_len);
    }
    return ret;
  }

  static const int MAX_SESS_BT_BUFF_SIZE = 1024;
  static const int64_t CACHED_SYS_VAR_VERSION = 721;
  ObBasicSessionInfo::SysVarIncInfo sys_var_inc_info_;
  uint64_t tenant_id_;
  uint64_t effective_tenant_id_;
  uint64_t rpc_tenant_id_;
  bool is_changed_to_temp_tenant_;
  uint64_t user_id_;
  common::ObString client_version_;
  common::ObString driver_version_;
  uint32_t master_sessid_;
  uint32_t client_sessid_;
  uint64_t proxy_sessid_;
  int64_t sys_var_base_version_;
  uint64_t proxy_user_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxExecResult tx_result_;
  share::SCN reserved_read_snapshot_version_;
  transaction::ObXATransID xid_;
  int64_t cached_tenant_config_version_;
  common::ObSEArray<ObBasicSessionInfo::TableStmtType, 2> total_stmt_tables_;
  common::ObSEArray<ObBasicSessionInfo::TableStmtType, 1> cur_stmt_tables_;
  share::ObBasicSysVar *sys_vars_[ALL_SYS_VARS_COUNT];
  common::ObString sys_var_in_pc_str_;
  common::ObString config_in_pc_str_;
  common::ObSmallBlockAllocator<> block_allocator_;
  ObSessionValMap user_var_val_map_;
  ObPhysicalPlan *cur_phy_plan_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  int64_t next_tx_read_only_;
  ObBasicSessionInfo::SqlScopeFlags sql_scope_flags_;
  common::ObClientMode client_mode_;


  common::ObConsistencyLevel consistency_level_;
  ObTimeZoneInfoWrap tz_info_wrap_;
  transaction::ObTxIsolationLevel next_tx_isolation_;
  bool enable_mysql_compatible_dates_;
  bool is_diagnosis_enabled_;
  int64_t diagnosis_limit_num_;

  ObFLTVars flt_vars_;
  obmysql::ObMySQLCapabilityFlags capability_;
  obmysql::ObProxyCapabilityFlags proxy_capability_;
  int64_t current_execution_id_;
  common::ObString app_trace_id_;
  uint64_t database_id_;
  ObBasicSessionInfo::MultiThreadData thread_data_;
  int64_t nested_count_;
  ObBasicSessionInfo::SysVarsCache sys_vars_cache_;

  bool check_sys_variable_;
  stmt::StmtType first_need_txn_stmt_type_;
  uint64_t exec_min_cluster_version_;
  stmt::StmtType stmt_type_;
  common::ObSEArray<share::ObLabelSeSessionLabel, 4> labels_;
  int64_t process_query_time_;
  bool is_client_sessid_support_;
  bool use_rich_vector_format_;
  common::ObSEArray<uint64_t, 4> enable_role_ids_;
  uint64_t sys_var_config_hash_val_;
  ObDiagnosisInfo diagnosis_info_;
  uint64_t client_create_time_;
};
OB_SERIALIZE_MEMBER(ObDiagnosisInfo, is_enabled_, limit_num_, log_file_, bad_file_);

int ObBasicSessionInfoMaster::calc_need_serialize_vars(ObSEArray<ObSysVarClassType, 64> &sys_var_ids, ObSEArray<ObString, 32> &user_var_names) const
{
  int ret = OB_SUCCESS;
  sys_var_ids.reset();
  user_var_names.reset();
  // 默认需要序列化的系统变量
  // 普通租户，序列化和 hardcode 不一致的变量
  const ObIArray<ObSysVarClassType> &ids = sys_var_inc_info_.get_all_sys_var_ids();
  for (int64_t i = 0; OB_SUCC(ret) && i < ids.count(); ++i) {
    int64_t sys_var_idx = -1;
    if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(ids.at(i), sys_var_idx))) {
      LOG_WARN("fail to calc sys var store idx", K(i), K(sys_var_idx), K(ids.at(i)), K(ret));
    } else if ((ObSysVariables::get_flags(sys_var_idx) &
                (ObSysVarFlag::SESSION_SCOPE | // "delta compare algorithm"
                ObSysVarFlag::NEED_SERIALIZE |
                ObSysVarFlag::QUERY_SENSITIVE))) {
      if (OB_FAIL(sys_var_ids.push_back(ids.at(i)))) {
        LOG_WARN("fail to push back sys var id", K(i), K(ids.at(i)), K(sys_var_ids), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(cur_phy_plan_) && cur_phy_plan_->contain_pl_udf_or_trigger()) {
    // 如果该语句包含PL UDF/TRIGGER, 将该Sesssion上变化的Package变量进行同步
    // TODO: 当前做的不够精细, 后续应该做到仅同步需要的变量
    ObSessionValMap::VarNameValMap::const_iterator iter = user_var_val_map_.get_val_map().begin();
    for (; OB_SUCC(ret) && iter != user_var_val_map_.get_val_map().end(); ++iter) {
      const ObString name = iter->first;
      if (name.prefix_match("pkg.")) {
        if (OB_FAIL(user_var_names.push_back(name))) {
          LOG_WARN("failed push back package var name", K(name));
        }
      }
    }
    LOG_DEBUG("sync package variables", K(user_var_names), K(cur_phy_plan_), K(lbt()));
  }

  if (OB_SUCC(ret) && cur_phy_plan_ != nullptr) {
    // 处理该语句用到的需要序列化的用户变量和系统变量
    const ObIArray<ObVarInfo> &extra_serialize_vars = cur_phy_plan_->get_vars();
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_serialize_vars.count(); ++i) {
      const ObVarInfo &var_info = extra_serialize_vars.at(i);
      if (USER_VAR == var_info.type_) {
        // 用户变量
        if (OB_FAIL(user_var_names.push_back(var_info.name_))) {
          LOG_WARN("fail to push user var name", K(var_info), K(user_var_names), K(ret));
        }
      } else if (SYS_VAR == var_info.type_) {
        // 系统变量
        ObSysVarClassType sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(var_info.name_);
        if (SYS_VAR_INVALID == sys_var_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid sys var id", K(sys_var_id), K(var_info), K(ret));
        } else {
          // 去重
          bool sys_var_exist = false;
          for (int64_t j = 0; OB_SUCC(ret) && !sys_var_exist && j < sys_var_ids.count(); ++j) {
            if (sys_var_id == sys_var_ids.at(j)) {
              sys_var_exist = true;
            }
          }
          if (OB_SUCCESS == ret && !sys_var_exist) {
            if (OB_FAIL(sys_var_ids.push_back(sys_var_id))) {
              LOG_WARN("fail to push back sys var id", K(sys_var_id), K(var_info), K(sys_var_ids),
                      K(ret));
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

OB_DEF_SERIALIZE(ObBasicSessionInfoMaster)
{
  int ret = OB_SUCCESS;
  ObTimeZoneInfo tmp_tz_info;//为了兼容老版本，创建临时time zone info 占位
  // To be compatible with old version which store sql_mode and compatibility mode in ObSQLModeManager;
  int64_t compatibility_mode_index = 0;
  if (OB_FAIL(compatibility_mode2index(get_compatibility_mode(), compatibility_mode_index))) {
    LOG_WARN("convert compatibility mode to index failed", K(ret));
  }
  bool has_tx_desc = tx_desc_ != NULL;
  OB_UNIS_ENCODE(has_tx_desc);
  if (has_tx_desc) {
    OB_UNIS_ENCODE(*tx_desc_);
    LOG_TRACE("serialize txDesc", KPC_(tx_desc));
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              consistency_level_,
              compatibility_mode_index,
              tmp_tz_info,
              // NOTE: rpc_tenant_id may cause compatability problem,
              // But only in diagnose tenant, so keep the stupid hack as it is.
              tenant_id_ | (rpc_tenant_id_ << 32),
              effective_tenant_id_,
              is_changed_to_temp_tenant_,
              user_id_,
              master_sessid_,
              // is_master_session() ? get_sid() : master_sessid_,
              capability_.capability_,
              thread_data_.database_name_);
  // 序列化需要序列化的用户变量和系统变量
  ObSEArray<ObSysVarClassType, 64> sys_var_ids;
  ObSEArray<ObString, 32> user_var_names;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_need_serialize_vars(sys_var_ids, user_var_names))) {
    LOG_WARN("fail to calc need serialize vars", K(ret));
  } else {
    ObSEArray<std::pair<ObString, ObSessionVariable>, 32> actual_ser_user_vars;
    ObSessionVariable user_var_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < user_var_names.count(); ++i) {
      user_var_val.reset();
      const ObString &user_var_name = user_var_names.at(i);
      ret = user_var_val_map_.get_refactored(user_var_name, user_var_val);
      if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get user var from session user var map", K(i), K(user_var_name),
                 K(user_var_val_map_.size()), K(ret));
      } else {
        if (OB_SUCCESS == ret
            && OB_FAIL(actual_ser_user_vars.push_back(std::make_pair(user_var_name, user_var_val)))) {
          LOG_WARN("fail to push back pair(user_var_name, user_var_val)", K(buf_len),
                   K(pos), K(user_var_name), K(user_var_val), K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, actual_ser_user_vars.count()))) {
        LOG_WARN("fail to serialize user var count", K(ret), K(buf_len), K(pos),
                 K(actual_ser_user_vars.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < actual_ser_user_vars.count(); ++i) {
          const ObString &user_var_name = actual_ser_user_vars.at(i).first;
          const ObSessionVariable &user_var_val = actual_ser_user_vars.at(i).second;
          if (OB_FAIL(serialization::encode(buf, buf_len, pos, user_var_name))) {
            LOG_WARN("fail to serialize user var name", K(buf_len), K(pos), K(user_var_name),
                     K(ret));
          } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, user_var_val.meta_))) {
            LOG_WARN("fail to serialize user var val meta", K(buf_len), K(pos),
                     K(user_var_val.meta_), K(ret));
          } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, user_var_val.value_))) {
            LOG_WARN("fail to serialize user var val value", K(buf_len), K(pos),
                     K(user_var_val.value_), K(ret));
          } else {}
        }
      }
    }
  }

  // split function, make stack checker happy
  [&](){
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, sys_var_ids.count()))) {
      LOG_WARN("fail to serialize sys var count", K(buf_len), K(pos), K(sys_var_ids.count()), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_var_ids.count(); ++i) {
      int64_t sys_var_idx = -1;
      if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_ids.at(i), sys_var_idx))) {
        LOG_WARN("fail to calc sys var store idx", K(i), K(sys_var_idx), K(sys_var_ids.at(i)), K(ret));
      } else if (sys_var_idx < 0 || share::ObSysVarFactory::ALL_SYS_VARS_COUNT <= sys_var_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sys var idx is invalid", K(sys_var_idx), K(ret));
      } else if (OB_ISNULL(sys_vars_[sys_var_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sys var is NULL", K(ret), K(sys_var_idx));
      } else {
        int16_t sys_var_id = static_cast<int16_t>(sys_vars_[sys_var_idx]->get_type());
        if (OB_FAIL(serialization::encode(buf, buf_len, pos, sys_var_id))) {
          LOG_ERROR("fail to serialize sys var id", K(buf_len), K(pos), K(sys_var_id), K(ret));
        } else if (OB_FAIL(sys_vars_[sys_var_idx]->serialize(buf, buf_len, pos))) {
          LOG_ERROR("fail to serialize sys var", K(buf_len), K(pos), K(i), K(sys_var_idx),
                    K(*sys_vars_[sys_var_idx]), K(ret));
        } else {
          LOG_DEBUG("serialize sys vars", K(sys_var_idx),
                    "name", ObSysVariables::get_name(sys_var_idx),
                    "val", sys_vars_[sys_var_idx]->get_value(),
                    "def", ObSysVariables::get_default_value(sys_var_idx));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to encode session info", K(ret));
  } else {
    bool tx_read_only = next_tx_read_only_;
    if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, tx_read_only))) {
      LOG_WARN("fail to encode tx_read_only", K(ret));
    }
  }

  // 不再有意义字段，为了兼容性考虑
  bool unused_literal_query = false;
  int64_t unused_inner_safe_weak_read_snapshot = 0;
  int64_t unused_weak_read_snapshot_source = 0;
  int64_t unused_safe_weak_read_snapshot = 0;

  bool need_serial_exec = false;
  uint64_t sql_scope_flags = sql_scope_flags_.get_flags();
  // No meaningful field for serialization compatibility
  bool is_foreign_key_cascade = false;
  bool is_foreign_key_check_exist = false;
  LST_DO_CODE(OB_UNIS_ENCODE,
              sys_vars_cache_.inc_data_,
              unused_safe_weak_read_snapshot,
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
              reserved_read_snapshot_version_,
              check_sys_variable_,
              unused_weak_read_snapshot_source,
              database_id_,
              thread_data_.user_at_host_name_,
              thread_data_.user_at_client_ip_,
              current_execution_id_,
              labels_,
              total_stmt_tables_,
              cur_stmt_tables_,
              is_foreign_key_cascade,
              sys_var_in_pc_str_,
              config_in_pc_str_,
              is_foreign_key_check_exist,
              need_serial_exec,
              sql_scope_flags,
              stmt_type_,
              thread_data_.client_addr_,
              thread_data_.user_client_addr_,
              process_query_time_,
              flt_vars_.last_flt_trace_id_,
              flt_vars_.row_traceformat_,
              flt_vars_.last_flt_span_id_,
              exec_min_cluster_version_,
              is_client_sessid_support_,
              use_rich_vector_format_);
  }();
  OB_UNIS_ENCODE(ObString(sql_id_));
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE,
                proxy_user_id_,
                thread_data_.proxy_user_name_,
                thread_data_.proxy_host_name_,
                enable_role_ids_);
  }
  OB_UNIS_ENCODE(sys_var_config_hash_val_);
  OB_UNIS_ENCODE(enable_mysql_compatible_dates_);
  OB_UNIS_ENCODE(is_diagnosis_enabled_);
  OB_UNIS_ENCODE(diagnosis_limit_num_);
  OB_UNIS_ENCODE(client_sessid_);
  OB_UNIS_ENCODE(diagnosis_info_);
  OB_UNIS_ENCODE(client_create_time_);
  return ret;
}

class TestBasicSessionInfoSerializeCompat : public ::testing::Test
{
public:
  TestBasicSessionInfoSerializeCompat() {}
  virtual ~TestBasicSessionInfoSerializeCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void verify_basic_fields_equal(const ObBasicSessionInfoMaster &info1, const ObBasicSessionInfo &info2)
{
  ASSERT_EQ(info1.tenant_id_, info2.tenant_id_);
  ASSERT_EQ(info1.effective_tenant_id_, info2.effective_tenant_id_);
  ASSERT_EQ(info1.user_id_, info2.user_id_);
  ASSERT_EQ(info1.proxy_user_id_, info2.proxy_user_id_);

  ASSERT_EQ(info1.master_sessid_, info2.master_sessid_);
  ASSERT_EQ(info1.client_sessid_, info2.client_sessid_);
  ASSERT_EQ(info1.proxy_sessid_, info2.proxy_sessid_);

  ASSERT_EQ(info1.next_tx_read_only_, info2.next_tx_read_only_);

  ASSERT_EQ(info1.is_changed_to_temp_tenant_, info2.is_changed_to_temp_tenant_);
  ASSERT_EQ(info1.enable_mysql_compatible_dates_, info2.enable_mysql_compatible_dates_);
  ASSERT_EQ(info1.consistency_level_, info2.consistency_level_);

  ASSERT_EQ(info1.current_execution_id_, info2.current_execution_id_);
  ASSERT_EQ(info1.database_id_, info2.database_id_);
  ASSERT_EQ(info1.client_mode_, info2.client_mode_);
  ASSERT_EQ(info1.nested_count_, info2.nested_count_);
  ASSERT_EQ(info1.process_query_time_, info2.process_query_time_);
  LOG_INFO("info1.sys_var_config_hash_val_", K(info1.sys_var_config_hash_val_));
  ASSERT_EQ(info1.sys_var_config_hash_val_, info2.sys_var_config_hash_val_);
}

void init_set(ObBasicSessionInfo &info)
{
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  easy_connection_t conn;
  bool autocommit = false;
  bool is_valid  = false;
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  int ret = ObPreProcessSysVars::change_initial_value();
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
}

TEST_F(TestBasicSessionInfoSerializeCompat, test_master_to_42x)
{
  ObBasicSessionInfoMaster info_master;
  ObBasicSessionInfo info_42x(OB_SERVER_TENANT_ID);
  init_set(info_42x);
  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, info_master.serialize(buf, sizeof(buf), pos));
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, info_42x.deserialize(buf, pos, deserialize_pos));
  verify_basic_fields_equal(info_master, info_42x);
}

TEST_F(TestBasicSessionInfoSerializeCompat, test_42x_to_master)
{
  // deserialize for masterversion not implemented, because need copy too much code.
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_CURRENT_VERSION;
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}