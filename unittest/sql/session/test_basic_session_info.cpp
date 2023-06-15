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

#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>
#define private public
#define protected public
#include "io/easy_io_struct.h"
#include "lib/oblog/ob_log.h"
#include "share/schema/ob_server_schema_service.h"
#include "sql/session/ob_basic_session_info.h"
#include "lib/allocator/ob_malloc.h"
#include "observer/ob_server.h"

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
  ObBasicSessionInfo::LockGuard lock_guard(session_info.get_query_lock());
  sleep(1);
}

TEST(test_basic_session_info, load_variables)
{
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_SESSION);
  ObBasicSessionInfo session_info(OB_SERVER_TENANT_ID);
  ObBasicSessionInfo::LockGuard lock_guard(session_info.get_query_lock());
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session_info.test_init(0, 0, &allocator));
  int ret = OB_SUCCESS;
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
  session_info.reset();
}


}
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
