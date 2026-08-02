/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>
#define private public
#define protected public
#include "observer/ob_server.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/ob_errno.h"
#include "common/ob_smart_var.h"
#include "deps/easy/src/io/easy_io_struct.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_mod_define.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

TEST(test_basic_session_info, init_set_get)
{
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  ObBasicSessionInfo session_info(OB_SERVER_TENANT_ID);
  easy_connection_t conn;
  bool autocommit = false;
  bool is_valid  = false;
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
  ASSERT_EQ(OB_SUCCESS, session_info.test_init(0, 0, &allocator));
  {
    ObString tenant_name = ObString::make_string("yyy");
    ObString user_name = ObString::make_string("aaa");
    session_info.init_tenant(tenant_name, 1);
    session_info.set_user(user_name, OB_DEFAULT_HOST_NAME, 1);
    ObObj autocommit_obj, min_val, max_val;
    ObObj autocommit_type;
    autocommit_obj.set_varchar("0");
    min_val.set_varchar("");
    max_val.set_varchar("");
    autocommit_type.set_type(ObIntType);
    ASSERT_EQ(OB_SUCCESS, session_info.load_sys_variable(calc_buf, ObString::make_string(OB_SV_AUTOCOMMIT), autocommit_type, autocommit_obj, min_val, max_val,
                                                         ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE, false));
    //session_info.set_autocommit(autocommit);
    //session_info.set_conn(&conn);
  }
  bool ac = true;
  ASSERT_EQ(OB_SUCCESS, session_info.get_autocommit(ac));
  ObString tenant_name = ObString::make_string("yyy");
  ObString user_name = ObString::make_string("aaa");
  ASSERT_EQ(tenant_name, session_info.get_tenant_name());
  ASSERT_EQ(user_name, session_info.get_user_name());
  ASSERT_EQ(autocommit, ac);
  ASSERT_EQ(is_valid, ac);
  session_info.get_user_var_val_map();
  //session_info.get_sys_var_val_map();
  //ASSERT_EQ(&conn, session_info.get_conn());
  ASSERT_EQ(NULL, session_info.get_log_id_level_map());
  session_info.log_id_level_map_valid_ = true;
  ASSERT_EQ(&session_info.log_id_level_map_, session_info.get_log_id_level_map());
  const int64_t serialize_size = session_info.get_serialize_size();
  // dima 2026072800117796771
  session_info.set_fetch_request_info(16384, 16384);
  ASSERT_EQ(serialize_size, session_info.get_serialize_size());
  session_info.reset_fetch_request_info();
  ObBasicSessionInfo::LockGuard lock_guard(session_info.get_query_lock());
  sleep(1);
}

TEST(test_basic_session_info, fetch_request_info)
{
  // dima 2026072800117796771
  ObBasicSessionInfo session_info(OB_SERVER_TENANT_ID);
  const int64_t query_start_time = 100;
  const int64_t fetch_start_time = 200;
  const int64_t fetch_execution_id = 300;

  session_info.set_query_start_time(query_start_time);
  ASSERT_EQ(0, session_info.get_fetch_start_time());
  ASSERT_EQ(-1, session_info.get_fetch_execution_id());
  ASSERT_EQ(query_start_time, session_info.get_current_request_start_time());

  session_info.set_fetch_request_info(fetch_start_time, fetch_execution_id);
  ASSERT_EQ(fetch_start_time, session_info.get_fetch_start_time());
  ASSERT_EQ(fetch_execution_id, session_info.get_fetch_execution_id());
  ASSERT_EQ(fetch_start_time, session_info.get_current_request_start_time());
  ASSERT_EQ(query_start_time, session_info.get_query_start_time());

  session_info.reset_fetch_request_info();
  ASSERT_EQ(0, session_info.get_fetch_start_time());
  ASSERT_EQ(-1, session_info.get_fetch_execution_id());
  ASSERT_EQ(query_start_time, session_info.get_current_request_start_time());
}

TEST(test_basic_session_info, load_variables)
{
  int ret = OB_SUCCESS;
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  SMART_VAR(sql::ObSQLSessionInfo, session_info) {
    ObBasicSessionInfo::LockGuard lock_guard(session_info.get_query_lock());
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, session_info.test_init(0, 0, 0, &allocator));
    if (OB_SUCCESS != (ret = ObPreProcessSysVars::change_initial_value())){
      LOG_ERROR("Change initial value failed !", K(ret));
    }

    //test for load_system_variable
    ASSERT_EQ(OB_SUCCESS, session_info.load_default_sys_variable(true, true));
    ObString name;
    ObObj type;
    ObObj value;
    name = ObString::make_string("autocommit");
    type.set_type(ObIntType);
    value.set_int(1);
    ret = session_info.update_sys_variable_by_name(name, value);
    ASSERT_EQ(OB_SUCCESS, ret);
    //value.set_int(10);
    //ret = session_info.update_sys_variable(name, value);
    //ASSERT_EQ(OB_ERR_WRONG_VALUE_FOR_VAR, ret);

    //test replace_user_variable()
    ObSessionVariable sess_var;
    ObString empty_name = ObString::make_empty_string();
    ret = session_info.replace_user_variable(empty_name, sess_var);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    ObString user_val("user_val");
    type.set_type(ObIntType);
    sess_var.value_.set_int(7);
    ret = session_info.replace_user_variable(user_val, sess_var);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = session_info.replace_user_variable(user_val, sess_var);
    ASSERT_EQ(OB_SUCCESS, ret);

    //test remove_user_variable()
    ret = session_info.remove_user_variable(empty_name);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    ObString not_exist = ObString::make_string("not_exist");
    ret = session_info.remove_user_variable(not_exist);
    ASSERT_EQ(OB_ERR_USER_VARIABLE_UNKNOWN, ret);
    ret = session_info.remove_user_variable(user_val);
    ASSERT_EQ(OB_SUCCESS, ret);

    //test update_sys_variable()
    ret = session_info.update_sys_variable_by_name(empty_name, value);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    ret = session_info.update_sys_variable_by_name(not_exist, value);
    ASSERT_EQ(OB_ERR_SYS_VARIABLE_UNKNOWN, ret);
    ObString log_level = ObString::make_string("ob_log_level");
    ObString log_level_string = ObString::make_string("sql.*:debug,rs.*:info");
    ObObj log_level_value;
    log_level_value.set_varchar(log_level_string);
    ret = session_info.update_sys_variable_by_name(log_level, log_level_value);
    ASSERT_EQ(OB_SUCCESS, ret);
    log_level_string = ObString::make_string("disabled");
    log_level_value.set_varchar(log_level_string);
    ret = session_info.update_sys_variable_by_name(log_level, log_level_value);
    ASSERT_EQ(OB_SUCCESS, ret);
    log_level_string = ObString::make_string("wrong value");
    log_level_value.set_varchar(log_level_string);
    ret = session_info.update_sys_variable_by_name(log_level, log_level_value);
    ASSERT_EQ(OB_LOG_LEVEL_INVALID, ret);
    value.set_int(1);
    ret = session_info.update_sys_variable_by_name(name, value);
    ASSERT_EQ(OB_SUCCESS, ret);

    //test get_user_variable_value()
    ObObj result;
    ret = session_info.get_user_variable_value(not_exist, result);
    ASSERT_EQ(OB_ERR_USER_VARIABLE_UNKNOWN, ret);
    ret = session_info.replace_user_variable(user_val, sess_var);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = session_info.get_user_variable_value(user_val, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(result, sess_var.value_);

    //test get_sys_variable()
    ObBasicSysVar *val = NULL;
    ret = session_info.get_sys_variable_by_name(not_exist, val);
    ASSERT_EQ(OB_ERR_SYS_VARIABLE_UNKNOWN, ret);
    ret = session_info.get_sys_variable_by_name(name, val);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(val->get_value(), value);

    //test get_sys_variable()
    result.reset();
    ret = session_info.get_sys_variable_by_name(not_exist, result);
    ASSERT_EQ(OB_ERR_SYS_VARIABLE_UNKNOWN, ret);
    ret = session_info.get_sys_variable_by_name(name, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(result, value);
    result.reset();
    ret = session_info.get_sys_variable_by_name("not_exist", result);
    ASSERT_EQ(OB_ERR_SYS_VARIABLE_UNKNOWN, ret);
    ret = session_info.get_sys_variable_by_name("autocommit", result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(result, value);

    //test get_user_variable_value()
    const ObObj * obj_ptr = NULL;
    ASSERT_EQ(NULL, session_info.get_user_variable_value(not_exist));
    ASSERT_TRUE(NULL != (obj_ptr= session_info.get_user_variable_value(user_val)));
    ASSERT_EQ(*obj_ptr, sess_var.value_);

    //test get_user_variable_value()
    ObObj out_value;
    ASSERT_TRUE(OB_SUCCESS == session_info.get_sys_variable_by_name(name, out_value));
    ASSERT_EQ(out_value, value);

    //test variable_exist()
    ASSERT_FALSE(session_info.user_variable_exists(not_exist));
    ASSERT_TRUE(session_info.user_variable_exists(user_val));

    //test sys_variable_exist()
    bool is_exist = false;
    ASSERT_TRUE(OB_SUCCESS == session_info.sys_variable_exists(not_exist, is_exist));
    ASSERT_FALSE(is_exist);
    ASSERT_TRUE(OB_SUCCESS == session_info.sys_variable_exists(name, is_exist));
    ASSERT_TRUE(is_exist);

    //test get_sys_variable_type()
    ObObjType re_type;
    re_type = session_info.get_sys_variable_type(name);
    ASSERT_EQ(re_type, ObIntType);
    LOG_WARN("session_info:", K(session_info));
    session_info.reset(false);
  }
}

TEST(test_basic_session_info, reset_sys_vars)
{
  int ret = OB_SUCCESS;
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  SMART_VAR(sql::ObSQLSessionInfo, session_info) {
    ObBasicSessionInfo::LockGuard lock_guard(session_info.get_query_lock());
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, session_info.test_init(0, 0, 0, &allocator));
    if (OB_SUCCESS != (ret = ObPreProcessSysVars::change_initial_value())){
      LOG_ERROR("Change initial value failed !", K(ret));
    }

    // Step 1: Load default system variables
    ASSERT_EQ(OB_SUCCESS, session_info.load_default_sys_variable(true, true));

    // Step 2: Set some variables to non-default values

    ObString current_default_catalog_name = ObString::make_string("_current_default_catalog");
    ObObj current_default_catalog_value;

    current_default_catalog_value.set_uint64(50008);

    ASSERT_EQ(OB_SUCCESS, session_info.update_sys_variable_by_name(current_default_catalog_name, current_default_catalog_value));

    // Step 3: Verify variables are set to non-default values
    ObObj result;
    ASSERT_EQ(OB_SUCCESS, session_info.get_sys_variable_by_name(current_default_catalog_name, result));
    ASSERT_EQ(result.get_uint64(), current_default_catalog_value.get_uint64());

    // Step 4: Reset session with skip_sys_var = false (no session cache)
    session_info.reset(false);

    // Step 5: Load default system variables again
    ASSERT_EQ(OB_SUCCESS, session_info.load_default_sys_variable(true, true));

    // Step 6: Verify variables are back to default values
    ObObj default_current_default_catalog;
    default_current_default_catalog.set_uint64(0);

    ASSERT_EQ(OB_SUCCESS, session_info.get_sys_variable_by_name(current_default_catalog_name, result));
    ASSERT_EQ(result.get_uint64(), default_current_default_catalog.get_uint64()); // both are default value 0

    // Step 7: Test with skip_sys_var = true (preserve system variables)
    // Set variables again to non-default values
    ASSERT_EQ(OB_SUCCESS, session_info.update_sys_variable_by_name(current_default_catalog_name, current_default_catalog_value));

    // Reset with skip_sys_var = true (with session cache, only clean inc value by reset inc flags)
    session_info.reset(true);

    // Load default system variables
    ASSERT_EQ(OB_SUCCESS, session_info.load_default_sys_variable(true, true));

    // Verify variables are back to default values
    ASSERT_EQ(OB_SUCCESS, session_info.get_sys_variable_by_name(current_default_catalog_name, result));
    ASSERT_EQ(result.get_uint64(), default_current_default_catalog.get_uint64());
  }
}

TEST(test_basic_session_info, reset_cached_global_read_consistency)
{
  int ret = OB_SUCCESS;
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  common::ObArenaAllocator source_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator serialize_allocator(ObModIds::OB_SQL_SESSION);
  SMART_VAR(sql::ObSQLSessionInfo, source_session) {
    SMART_VAR(sql::ObSQLSessionInfo, cached_session) {
      ObBasicSessionInfo::LockGuard source_lock_guard(source_session.get_query_lock());
      ObBasicSessionInfo::LockGuard cached_lock_guard(cached_session.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
      ASSERT_EQ(OB_SUCCESS, source_session.test_init(0, 0, 0, &source_allocator));
      // Pool sessions must use the inner allocator; nullptr keeps block_allocator_.
      ASSERT_EQ(OB_SUCCESS, cached_session.test_init(0, 0, 0, nullptr));
      ASSERT_EQ(OB_SUCCESS, source_session.init_tenant(
          ObString::make_string("test"), OB_SYS_TENANT_ID));
      ASSERT_EQ(OB_SUCCESS, cached_session.init_tenant(
          ObString::make_string("test"), OB_SYS_TENANT_ID));
      ASSERT_EQ(OB_SUCCESS, source_session.load_essential_sys_vars_only(true, true));
      ASSERT_EQ(OB_SUCCESS, cached_session.load_essential_sys_vars_only(true, true));

      int64_t store_idx = -1;
      ASSERT_EQ(OB_SUCCESS, ObSysVarFactory::calc_sys_var_store_idx(
          SYS_VAR_OB_READ_CONSISTENCY, store_idx));
      ObBasicSysVar *read_consistency_var = cached_session.get_sys_var(store_idx);
      ASSERT_NE(nullptr, read_consistency_var);

      // Simulate a pooled session created while the global value was WEAK.
      ObObj weak_read_consistency;
      weak_read_consistency.set_int(common::WEAK);
      const ObObj min_val = read_consistency_var->get_min_val();
      const ObObj max_val = read_consistency_var->get_max_val();
      const ObObjType data_type = read_consistency_var->get_data_type();
      const int64_t flags = read_consistency_var->flags_;
      read_consistency_var->clean_base_value();
      read_consistency_var->clean_inc_value();
      ASSERT_EQ(OB_SUCCESS, read_consistency_var->init(
          weak_read_consistency, min_val, max_val, data_type, flags));
      ASSERT_EQ(OB_SUCCESS, cached_session.sys_var_inc_info_.add_sys_var_id(
          SYS_VAR_OB_READ_CONSISTENCY));
      cached_session.consistency_level_ = common::WEAK;
      cached_session.reset(true);
      cached_session.set_acquire_from_pool(true);
      const ObTZInfoMap *tz_map =
          cached_session.tz_info_wrap_.get_tz_info_offset().get_tz_info_map();
      ASSERT_NE(nullptr, tz_map);
      ASSERT_EQ(OB_SUCCESS, cached_session.ObBasicSessionInfo::init(
          0, 0, nullptr, tz_map));

      // A new source session at the hardcoded STRONG value sends no delta.
      ASSERT_FALSE(source_session.sys_var_inc_info_.all_has_sys_var_id(
          SYS_VAR_OB_READ_CONSISTENCY));
      const int64_t serialize_size =
          source_session.ObBasicSessionInfo::get_serialize_size();
      char *buf = static_cast<char *>(serialize_allocator.alloc(serialize_size));
      ASSERT_NE(nullptr, buf);
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, source_session.ObBasicSessionInfo::serialize(
          buf, serialize_size, pos));

      int64_t deserialize_pos = 0;
      ASSERT_EQ(OB_SUCCESS, cached_session.ObBasicSessionInfo::deserialize(
          buf, pos, deserialize_pos));
      ASSERT_EQ(pos, deserialize_pos);

      int64_t read_consistency = common::INVALID_CONSISTENCY;
      ASSERT_EQ(OB_SUCCESS, cached_session.get_ob_read_consistency(read_consistency));
      ASSERT_EQ(common::STRONG, read_consistency);
      ASSERT_EQ(common::STRONG, cached_session.get_consistency_level());
    }
  }
}

TEST(test_basic_session_info, reset_cached_session_read_consistency)
{
  int ret = OB_SUCCESS;
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  common::ObArenaAllocator source_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator serialize_allocator(ObModIds::OB_SQL_SESSION);
  SMART_VAR(sql::ObSQLSessionInfo, source_session) {
    SMART_VAR(sql::ObSQLSessionInfo, cached_session) {
      ObBasicSessionInfo::LockGuard source_lock_guard(source_session.get_query_lock());
      ObBasicSessionInfo::LockGuard cached_lock_guard(cached_session.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
      ASSERT_EQ(OB_SUCCESS, source_session.test_init(0, 0, 0, &source_allocator));
      // Pool sessions must use the inner allocator; nullptr keeps block_allocator_.
      ASSERT_EQ(OB_SUCCESS, cached_session.test_init(0, 0, 0, nullptr));
      ASSERT_EQ(OB_SUCCESS, source_session.init_tenant(
          ObString::make_string("test"), OB_SYS_TENANT_ID));
      ASSERT_EQ(OB_SUCCESS, cached_session.init_tenant(
          ObString::make_string("test"), OB_SYS_TENANT_ID));
      ASSERT_EQ(OB_SUCCESS, source_session.load_essential_sys_vars_only(true, true));
      ASSERT_EQ(OB_SUCCESS, cached_session.load_essential_sys_vars_only(true, true));

      int64_t store_idx = -1;
      ASSERT_EQ(OB_SUCCESS, ObSysVarFactory::calc_sys_var_store_idx(
          SYS_VAR_OB_READ_CONSISTENCY, store_idx));
      ObBasicSysVar *read_consistency_var = cached_session.get_sys_var(store_idx);
      ASSERT_NE(nullptr, read_consistency_var);

      // Simulate SET SESSION ob_read_consistency = WEAK: base stays STRONG, inc becomes WEAK.
      ASSERT_EQ(common::STRONG, read_consistency_var->get_base_value().get_int());
      ASSERT_EQ(OB_SUCCESS, cached_session.update_sys_variable(
          SYS_VAR_OB_READ_CONSISTENCY, static_cast<int64_t>(common::WEAK)));
      ASSERT_EQ(common::STRONG, read_consistency_var->get_base_value().get_int());
      ASSERT_EQ(common::WEAK, read_consistency_var->get_value().get_int());
      ASSERT_EQ(common::WEAK, cached_session.get_consistency_level());

      cached_session.reset(true);
      // reset(true) cleans inc but leaves consistency_level_ untouched.
      ASSERT_EQ(common::STRONG, read_consistency_var->get_base_value().get_int());
      ASSERT_EQ(common::STRONG, read_consistency_var->get_value().get_int());
      ASSERT_EQ(common::WEAK, cached_session.get_consistency_level());

      cached_session.set_acquire_from_pool(true);
      const ObTZInfoMap *tz_map =
          cached_session.tz_info_wrap_.get_tz_info_offset().get_tz_info_map();
      ASSERT_NE(nullptr, tz_map);
      ASSERT_EQ(OB_SUCCESS, cached_session.ObBasicSessionInfo::init(
          0, 0, nullptr, tz_map));
      ASSERT_EQ(common::STRONG, cached_session.get_consistency_level());

      // A new source session at the hardcoded STRONG value sends no delta.
      ASSERT_FALSE(source_session.sys_var_inc_info_.all_has_sys_var_id(
          SYS_VAR_OB_READ_CONSISTENCY));
      const int64_t serialize_size =
          source_session.ObBasicSessionInfo::get_serialize_size();
      char *buf = static_cast<char *>(serialize_allocator.alloc(serialize_size));
      ASSERT_NE(nullptr, buf);
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, source_session.ObBasicSessionInfo::serialize(
          buf, serialize_size, pos));

      int64_t deserialize_pos = 0;
      ASSERT_EQ(OB_SUCCESS, cached_session.ObBasicSessionInfo::deserialize(
          buf, pos, deserialize_pos));
      ASSERT_EQ(pos, deserialize_pos);

      int64_t read_consistency = common::INVALID_CONSISTENCY;
      ASSERT_EQ(OB_SUCCESS, cached_session.get_ob_read_consistency(read_consistency));
      ASSERT_EQ(common::STRONG, read_consistency);
      ASSERT_EQ(common::STRONG, cached_session.get_consistency_level());
    }
  }
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
