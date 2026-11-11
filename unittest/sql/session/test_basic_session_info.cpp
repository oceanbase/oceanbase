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
#include "observer/ob_server.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
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

static int decode_sql_and_basic_session_versions(const char *buf,
                                                 const int64_t data_len,
                                                 int64_t &sql_version,
                                                 int64_t &basic_version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t sql_body_len = 0;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, sql_version))) {
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, sql_body_len))) {
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, basic_version))) {
  }
  return ret;
}

static int init_compact_test_session(ObSQLSessionInfo &session,
                                     common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObString tenant_name = ObString::make_string("compact_tenant");
  OZ (session.test_init(0, 0, 0, &allocator));
  OZ (session.load_default_sys_variable(true, true));
  OZ (session.init_tenant(tenant_name, 1));
  OZ (session.set_user(
      ObString::make_string("compact_user"), OB_DEFAULT_HOST_NAME, 1));
  return ret;
}

TEST(test_basic_session_info, serialize_mode_nested_restore)
{
  ObBasicSessionInfo session(OB_SERVER_TENANT_ID);
  EXPECT_FALSE(session.is_compact_session_serialize());
  EXPECT_TRUE(session.is_tx_desc_inline_serialize());
  {
    ObSessionSerializeModeGuard outer(ObSessionSerializeMode(true, false));
    EXPECT_TRUE(session.is_compact_session_serialize());
    EXPECT_FALSE(session.is_tx_desc_inline_serialize());
    {
      ObSessionSerializeModeGuard inner(ObSessionSerializeMode(false, true));
      EXPECT_FALSE(session.is_compact_session_serialize());
      EXPECT_TRUE(session.is_tx_desc_inline_serialize());
    }
    EXPECT_TRUE(session.is_compact_session_serialize());
    EXPECT_FALSE(session.is_tx_desc_inline_serialize());
  }
  EXPECT_FALSE(session.is_compact_session_serialize());
  EXPECT_TRUE(session.is_tx_desc_inline_serialize());
}

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

TEST(test_basic_session_info, normal_quit_disconnect_reason)
{
  ObBasicSessionInfo session_info(OB_SERVER_TENANT_ID);

  EXPECT_FALSE(session_info.is_normal_quit());
  EXPECT_EQ(CLIENT_FORCE_DISCONNECT, session_info.get_disconnect_state());

  session_info.set_disconnect_state(SERVER_FORCE_DISCONNECT);
  EXPECT_FALSE(session_info.is_normal_quit());
  EXPECT_EQ(SERVER_FORCE_DISCONNECT, session_info.get_disconnect_state());

  session_info.set_disconnect_state(NORMAL_QUIT);
  EXPECT_TRUE(session_info.is_normal_quit());
  session_info.set_disconnect_state(SERVER_FORCE_DISCONNECT);
  EXPECT_TRUE(session_info.is_normal_quit());
  EXPECT_EQ(NORMAL_QUIT, session_info.get_disconnect_state());

  // KILL must not discard a QUIT already accepted before LOGOFF is evaluated.
  session_info.set_disconnect_state(NORMAL_KILL_SESSION);
  EXPECT_TRUE(session_info.is_normal_quit());
  EXPECT_EQ(NORMAL_QUIT, session_info.get_disconnect_state());

  // Other server-initiated closes retain their existing classification.
  ObBasicSessionInfo other_session(OB_SERVER_TENANT_ID);
  other_session.set_disconnect_state(NORMAL_KILL_SESSION);
  EXPECT_EQ(NORMAL_KILL_SESSION, other_session.get_disconnect_state());
  other_session.set_disconnect_state(SERVER_FORCE_DISCONNECT);
  EXPECT_FALSE(other_session.is_normal_quit());
  EXPECT_EQ(SERVER_FORCE_DISCONNECT, other_session.get_disconnect_state());

  session_info.set_disconnect_state(SERVER_FORCE_DISCONNECT);
  EXPECT_TRUE(session_info.is_normal_quit());
}

TEST(test_basic_session_info, reset_normal_quit_disconnect_reason)
{
  const bool skip_sys_var[] = {false, true};
  for (int64_t i = 0; i < 2; ++i) {
    ObBasicSessionInfo session_info(OB_SERVER_TENANT_ID);
    session_info.set_disconnect_state(NORMAL_QUIT);
    EXPECT_TRUE(session_info.is_normal_quit());

    // Neither a fresh session nor a reused cached session inherits the previous QUIT.
    session_info.reset(skip_sys_var[i]);
    EXPECT_FALSE(session_info.is_normal_quit());
    EXPECT_EQ(CLIENT_FORCE_DISCONNECT, session_info.get_disconnect_state());

    session_info.set_disconnect_state(SERVER_FORCE_DISCONNECT);
    EXPECT_FALSE(session_info.is_normal_quit());
    EXPECT_EQ(SERVER_FORCE_DISCONNECT, session_info.get_disconnect_state());
  }
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

TEST(test_basic_session_info, legacy_sys_var_descriptor_roundtrip)
{
  int ret = OB_SUCCESS;
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  common::ObArenaAllocator sender_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator receiver_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator serialize_allocator(ObModIds::OB_SQL_SESSION);
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::change_initial_value());

  SMART_VAR(sql::ObSQLSessionInfo, sender) {
    SMART_VAR(sql::ObSQLSessionInfo, receiver) {
      ObBasicSessionInfo::LockGuard sender_lock(sender.get_query_lock());
      ObBasicSessionInfo::LockGuard receiver_lock(receiver.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, init_compact_test_session(sender, sender_allocator));
      ASSERT_EQ(OB_SUCCESS, init_compact_test_session(receiver, receiver_allocator));

      ObBasicSysVar *sender_enum_var = NULL;
      ObBasicSysVar *sender_plain_var = NULL;
      ObBasicSysVar *receiver_enum_var = NULL;
      ObBasicSysVar *receiver_plain_var = NULL;
      ASSERT_EQ(OB_SUCCESS, sender.inner_get_sys_var(
          SYS_VAR_BINLOG_ROW_IMAGE, sender_enum_var));
      ASSERT_EQ(OB_SUCCESS, sender.inner_get_sys_var(
          SYS_VAR_AUTOCOMMIT, sender_plain_var));
      ASSERT_EQ(OB_SUCCESS, receiver.inner_get_sys_var(
          SYS_VAR_BINLOG_ROW_IMAGE, receiver_enum_var));
      ASSERT_EQ(OB_SUCCESS, receiver.inner_get_sys_var(
          SYS_VAR_AUTOCOMMIT, receiver_plain_var));
      ASSERT_TRUE(NULL != sender_enum_var);
      ASSERT_TRUE(NULL != sender_plain_var);
      ASSERT_TRUE(NULL != receiver_enum_var);
      ASSERT_TRUE(NULL != receiver_plain_var);
      ASSERT_TRUE(sender_enum_var->is_enum_type());
      ASSERT_FALSE(sender_plain_var->is_enum_type());

      const int64_t enum_size = sender_enum_var->get_serialize_size();
      char *enum_buf = static_cast<char *>(serialize_allocator.alloc(enum_size));
      ASSERT_TRUE(NULL != enum_buf);
      int64_t enum_encode_pos = 0;
      ASSERT_EQ(OB_SUCCESS, sender_enum_var->serialize(
          enum_buf, enum_size, enum_encode_pos));
      ASSERT_EQ(enum_size, enum_encode_pos);
      int64_t enum_wire_pos = enum_encode_pos - 1;
      ASSERT_EQ(OB_SUCCESS, serialization::encode(
          enum_buf, enum_size, enum_wire_pos, false));
      receiver_enum_var->set_data_type(ObUnknownType);
      int64_t enum_decode_pos = 0;
      ASSERT_EQ(OB_SUCCESS, receiver_enum_var->deserialize(
          enum_buf, enum_size, enum_decode_pos));
      ASSERT_EQ(enum_encode_pos, enum_decode_pos);
      ASSERT_EQ(sender_enum_var->get_data_type(), receiver_enum_var->get_data_type());
      ASSERT_FALSE(receiver_enum_var->is_enum_type());

      const int64_t plain_size = sender_plain_var->get_serialize_size();
      char *plain_buf = static_cast<char *>(serialize_allocator.alloc(plain_size));
      ASSERT_TRUE(NULL != plain_buf);
      int64_t plain_encode_pos = 0;
      ASSERT_EQ(OB_SUCCESS, sender_plain_var->serialize(
          plain_buf, plain_size, plain_encode_pos));
      ASSERT_EQ(plain_size, plain_encode_pos);
      int64_t plain_wire_pos = plain_encode_pos - 1;
      ASSERT_EQ(OB_SUCCESS, serialization::encode(
          plain_buf, plain_size, plain_wire_pos, true));
      receiver_plain_var->set_data_type(ObUnknownType);
      int64_t plain_decode_pos = 0;
      ASSERT_EQ(OB_SUCCESS, receiver_plain_var->deserialize(
          plain_buf, plain_size, plain_decode_pos));
      ASSERT_EQ(plain_encode_pos, plain_decode_pos);
      ASSERT_EQ(sender_plain_var->get_data_type(), receiver_plain_var->get_data_type());
      ASSERT_TRUE(receiver_plain_var->is_enum_type());
    }
  }

}

class TestSessionCodec : public ::testing::Test
{
protected:
  void SetUp() override
  {
    OBSERVER.init_schema();
    OBSERVER.init_tz_info_mgr();
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::change_initial_value());
  }

  void serialize_session(ObSQLSessionInfo &session,
                         ObIAllocator &allocator,
                         const ObSessionSerializeMode &mode,
                         ObString &wire)
  {
    ObSessionSerializeModeGuard guard(mode);
    const int64_t size = session.get_serialize_size();
    char *buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, session.serialize(buf, size, pos));
    ASSERT_EQ(size, pos);
    wire.assign_ptr(buf, static_cast<int32_t>(size));
  }

  void deserialize_session(ObSQLSessionInfo &session, const ObString &wire)
  {
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, session.deserialize(wire.ptr(), wire.length(), pos));
    ASSERT_EQ(wire.length(), pos);
    ASSERT_FALSE(session.is_compact_session_serialize());
    ASSERT_TRUE(session.is_tx_desc_inline_serialize());
  }

  void check_compact_defaults(const ObSQLSessionInfo &session)
  {
    ASSERT_TRUE(session.get_cur_sql_id().empty());
    ASSERT_FALSE(session.is_diagnosis_enabled_);
    ASSERT_EQ(0, session.diagnosis_limit_num_);
    ASSERT_TRUE(session.total_stmt_tables_.empty());
    ASSERT_TRUE(session.cur_stmt_tables_.empty());
    ASSERT_FALSE(session.is_max_availability_mode_);
  }
};

TEST_F(TestSessionCodec, compact_roundtrip_and_legacy_copy)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator sender_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator receiver_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator reset_receiver_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator position_receiver_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator serialize_allocator(ObModIds::OB_SQL_SESSION);

  SMART_VAR(sql::ObSQLSessionInfo, sender) {
    SMART_VAR(sql::ObSQLSessionInfo, receiver) {
      ObBasicSessionInfo::LockGuard sender_lock(sender.get_query_lock());
      ObBasicSessionInfo::LockGuard receiver_lock(receiver.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, init_compact_test_session(sender, sender_allocator));
      ASSERT_EQ(OB_SUCCESS, init_compact_test_session(receiver, receiver_allocator));

      sender.is_changed_to_temp_tenant_ = true;
      sender.next_tx_read_only_ = true;
      sender.check_sys_variable_ = true;
      sender.flt_vars_.row_traceformat_ = true;
      sender.is_client_sessid_support_ = true;
      sender.use_rich_vector_format_ = true;
      sender.enable_mysql_compatible_dates_ = true;
      sender.global_rich_vector_configured_ = true;
      sender.inner_flag_ = true;
      sender.has_temp_table_flag_ = true;
      sender.enable_early_lock_release_ = true;
      sender.in_definer_named_proc_ = true;
      sender.prelock_ = true;
      sender.thread_data_.is_in_retry_ = SESS_IN_RETRY_FOR_DUP_TBL;
      sender.tz_info_wrap_.set_tz_info_offset(8 * 60 * 60);
      ASSERT_EQ(OB_SUCCESS, sender.tz_info_wrap_.set_error_on_overlap_time(true));

      char sql_id[] = "0123456789abcdef0123456789abcdef";
      sender.set_cur_sql_id(sql_id);
      sender.proxy_user_id_ = OB_INVALID_ID;
      sender.thread_data_.proxy_user_name_ = ObString::make_string("proxy_user");
      sender.thread_data_.proxy_host_name_.reset();
      sender.diagnosis_info_.is_enabled_ = false;
      sender.diagnosis_info_.limit_num_ = 17;
      sender.diagnosis_info_.log_file_ = ObString::make_string("diag.log");
      sender.diagnosis_info_.bad_file_.reset();
      InnerDDLInfo inner_ddl_info;
      inner_ddl_info.set_refreshing_mview(true);
      ASSERT_EQ(OB_SUCCESS, sender.ddl_info_.init(inner_ddl_info, 1002));
      ASSERT_EQ(OB_SUCCESS, sender.set_module_name(ObString::make_string("module")));

      ObString legacy_wire;
      ObString compact_wire;
      ASSERT_NO_FATAL_FAILURE(serialize_session(
          sender, serialize_allocator, ObSessionSerializeMode(false, true), legacy_wire));
      ASSERT_NO_FATAL_FAILURE(serialize_session(
          sender, serialize_allocator, ObSessionSerializeMode(true, false), compact_wire));
      ASSERT_LT(compact_wire.length(), legacy_wire.length());
      int64_t sql_version = 0;
      int64_t basic_version = 0;
      ASSERT_EQ(OB_SUCCESS, decode_sql_and_basic_session_versions(
          legacy_wire.ptr(), legacy_wire.length(), sql_version, basic_version));
      ASSERT_EQ(3, sql_version);
      ASSERT_EQ(3, basic_version);

      ASSERT_EQ(OB_SUCCESS, decode_sql_and_basic_session_versions(
          compact_wire.ptr(), compact_wire.length(), sql_version, basic_version));
      ASSERT_EQ(4, sql_version);
      ASSERT_EQ(4, basic_version);
      ASSERT_NO_FATAL_FAILURE(deserialize_session(receiver, compact_wire));
      ASSERT_TRUE(receiver.is_changed_to_temp_tenant_);
      ASSERT_TRUE(receiver.next_tx_read_only_);
      ASSERT_TRUE(receiver.check_sys_variable_);
      ASSERT_TRUE(receiver.flt_vars_.row_traceformat_);
      ASSERT_TRUE(receiver.is_client_sessid_support_);
      ASSERT_TRUE(receiver.use_rich_vector_format_);
      ASSERT_TRUE(receiver.enable_mysql_compatible_dates_);
      ASSERT_TRUE(receiver.global_rich_vector_configured_);
      ASSERT_TRUE(receiver.inner_flag_);
      ASSERT_TRUE(receiver.has_temp_table_flag_);
      ASSERT_TRUE(receiver.enable_early_lock_release_);
      ASSERT_TRUE(receiver.in_definer_named_proc_);
      ASSERT_TRUE(receiver.prelock_);
      ASSERT_EQ(SESS_IN_RETRY_FOR_DUP_TBL, receiver.thread_data_.is_in_retry_);
      ASSERT_TRUE(receiver.ddl_info_.is_refreshing_mview());
      ASSERT_EQ(1002, receiver.ddl_info_.get_session_id());
      ASSERT_FALSE(receiver.diagnosis_info_.is_enabled_);
      ASSERT_EQ(17, receiver.diagnosis_info_.limit_num_);
      ASSERT_EQ(OB_INVALID_ID, receiver.proxy_user_id_);
      ASSERT_EQ(ObString::make_string("proxy_user"), receiver.thread_data_.proxy_user_name_);
      ASSERT_TRUE(receiver.thread_data_.proxy_host_name_.empty());
      ASSERT_EQ(ObString::make_string("module"), receiver.get_module_name());
      ASSERT_TRUE(receiver.get_action_name().empty());
      ASSERT_TRUE(receiver.get_client_info().empty());
      ASSERT_FALSE(receiver.tz_info_wrap_.is_position_class());
      ASSERT_EQ(8 * 60 * 60, receiver.tz_info_wrap_.get_tz_info_offset().get_offset());
      ASSERT_TRUE(receiver.tz_info_wrap_.is_error_on_overlap_time());
      ASSERT_NO_FATAL_FAILURE(check_compact_defaults(receiver));
    }
    SMART_VAR(sql::ObSQLSessionInfo, copy_receiver) {
      SCOPED_TRACE("legacy TableLoad copy after compact serialization");
      ObBasicSessionInfo::LockGuard sender_lock(sender.get_query_lock());
      ObBasicSessionInfo::LockGuard receiver_lock(copy_receiver.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, init_compact_test_session(copy_receiver, receiver_allocator));
      ASSERT_FALSE(sender.is_compact_session_serialize());
      ASSERT_TRUE(sender.is_tx_desc_inline_serialize());
      ASSERT_EQ(OB_SUCCESS, observer::ObTableLoadUtils::deep_copy(
          sender, copy_receiver, serialize_allocator));
      ASSERT_FALSE(copy_receiver.is_compact_session_serialize());
      ASSERT_TRUE(copy_receiver.is_tx_desc_inline_serialize());
      ASSERT_EQ(sender.get_cur_sql_id(), copy_receiver.get_cur_sql_id());
    }
    SMART_VAR(sql::ObSQLSessionInfo, reset_receiver) {
      ObBasicSessionInfo::LockGuard sender_lock(sender.get_query_lock());
      ObBasicSessionInfo::LockGuard receiver_lock(reset_receiver.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, init_compact_test_session(reset_receiver, reset_receiver_allocator));

      sender.ddl_info_.reset();
      sender.diagnosis_info_.reset();
      sender.proxy_user_id_ = OB_INVALID_ID;
      sender.thread_data_.proxy_user_name_.reset();
      sender.thread_data_.proxy_host_name_.reset();
      sender.client_app_info_.reset();
      sender.tz_info_wrap_.set_tz_info_offset(-5 * 60 * 60);
      ASSERT_EQ(OB_SUCCESS, sender.tz_info_wrap_.set_error_on_overlap_time(false));

      InnerDDLInfo stale_ddl_info;
      stale_ddl_info.set_refreshing_mview(true);
      ASSERT_EQ(OB_SUCCESS, reset_receiver.ddl_info_.init(stale_ddl_info, 1003));
      reset_receiver.diagnosis_info_.is_enabled_ = true;
      reset_receiver.diagnosis_info_.limit_num_ = 99;
      reset_receiver.diagnosis_info_.log_file_ = ObString::make_string("stale.log");
      reset_receiver.proxy_user_id_ = 1004;
      reset_receiver.thread_data_.proxy_user_name_ = ObString::make_string("stale_proxy");
      ASSERT_EQ(OB_SUCCESS, reset_receiver.set_module_name(ObString::make_string("stale_module")));
      reset_receiver.is_diagnosis_enabled_ = true;
      reset_receiver.diagnosis_limit_num_ = 88;
      reset_receiver.is_max_availability_mode_ = true;
      ASSERT_EQ(OB_SUCCESS, reset_receiver.total_stmt_tables_.push_back(
          ObBasicSessionInfo::TableStmtType(1005)));
      ASSERT_EQ(OB_SUCCESS, reset_receiver.cur_stmt_tables_.push_back(
          ObBasicSessionInfo::TableStmtType(1006)));

      ObString reset_wire;
      ASSERT_NO_FATAL_FAILURE(serialize_session(
          sender, serialize_allocator, ObSessionSerializeMode(true, true), reset_wire));
      ASSERT_NO_FATAL_FAILURE(deserialize_session(reset_receiver, reset_wire));
      ASSERT_TRUE(reset_receiver.ddl_info_.is_ddl_info_empty());
      ASSERT_TRUE(reset_receiver.diagnosis_info_.is_empty());
      ASSERT_EQ(OB_INVALID_ID, reset_receiver.proxy_user_id_);
      ASSERT_TRUE(reset_receiver.thread_data_.proxy_user_name_.empty());
      ASSERT_TRUE(reset_receiver.thread_data_.proxy_host_name_.empty());
      ASSERT_TRUE(reset_receiver.client_app_info_.is_empty());
      ASSERT_NO_FATAL_FAILURE(check_compact_defaults(reset_receiver));
      ASSERT_EQ(-5 * 60 * 60,
          reset_receiver.tz_info_wrap_.get_tz_info_offset().get_offset());
      ASSERT_FALSE(reset_receiver.tz_info_wrap_.is_error_on_overlap_time());
    }
    SMART_VAR(sql::ObSQLSessionInfo, position_receiver) {
      ObBasicSessionInfo::LockGuard sender_lock(sender.get_query_lock());
      ObBasicSessionInfo::LockGuard receiver_lock(position_receiver.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, init_compact_test_session(position_receiver, position_receiver_allocator));

      sender.tz_info_wrap_.set_tz_info_position();
      sender.tz_info_wrap_.set_cur_version(123);
      ASSERT_EQ(OB_SUCCESS, sender.tz_info_wrap_.set_error_on_overlap_time(true));
      ObString position_wire;
      ASSERT_NO_FATAL_FAILURE(serialize_session(
          sender, serialize_allocator, ObSessionSerializeMode(true, true), position_wire));
      ASSERT_NO_FATAL_FAILURE(deserialize_session(position_receiver, position_wire));
      ASSERT_TRUE(position_receiver.tz_info_wrap_.is_position_class());
      ASSERT_EQ(123, position_receiver.tz_info_wrap_.get_cur_version());
      ASSERT_TRUE(position_receiver.tz_info_wrap_.is_error_on_overlap_time());
    }
  }
}

TEST_F(TestSessionCodec, pc_config_env_roundtrip)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator sender_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator absent_receiver_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator present_receiver_allocator(ObModIds::OB_SQL_SESSION);
  common::ObArenaAllocator serialize_allocator(ObModIds::OB_SQL_SESSION);

  ObPhysicalPlan phy_plan;
  SMART_VAR(sql::ObSQLSessionInfo, sender) {
    ObBasicSessionInfo::LockGuard sender_lock(sender.get_query_lock());
    ASSERT_EQ(OB_SUCCESS, init_compact_test_session(sender, sender_allocator));
    const ObString expected_pc_config =
        ObString::make_string("compact_pc_config_env");
    ASSERT_EQ(OB_SUCCESS, sender.sess_level_name_pool_.write_string(
        expected_pc_config, &sender.config_in_pc_str_));
    ASSERT_FALSE(sender.sys_var_in_pc_str_.empty());

    ASSERT_FALSE(sender.need_serialize_pc_config_env_(false));
    ASSERT_FALSE(sender.need_serialize_pc_config_env_(true));
    ObString absent_wire;
    ASSERT_NO_FATAL_FAILURE(serialize_session(
        sender, serialize_allocator, ObSessionSerializeMode(true, true), absent_wire));

    SMART_VAR(sql::ObSQLSessionInfo, absent_receiver) {
      ObBasicSessionInfo::LockGuard receiver_lock(
          absent_receiver.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, absent_receiver.test_init(
          0, 0, 0, &absent_receiver_allocator));
      ASSERT_EQ(OB_SUCCESS, absent_receiver.sess_level_name_pool_.write_string(
          ObString::make_string("stale_pc_config_env"),
          &absent_receiver.config_in_pc_str_));
      absent_receiver.sys_var_config_hash_val_ = 12345;
      ASSERT_NO_FATAL_FAILURE(deserialize_session(absent_receiver, absent_wire));
      ASSERT_TRUE(absent_receiver.config_in_pc_str_.empty());
      ASSERT_EQ(0, absent_receiver.sys_var_config_hash_val_);
    }

    ASSERT_EQ(OB_SUCCESS, sender.set_cur_phy_plan(&phy_plan));
    ASSERT_FALSE(sender.need_serialize_pc_config_env_(true));
    phy_plan.set_contain_pl_udf_or_trigger(true);
    ASSERT_TRUE(sender.need_serialize_pc_config_env_(true));
    ASSERT_FALSE(sender.need_serialize_pc_config_env_(false));

    ObString present_wire;
    ASSERT_NO_FATAL_FAILURE(serialize_session(
        sender, serialize_allocator, ObSessionSerializeMode(true, true), present_wire));
    ASSERT_EQ(absent_wire.length() + serialization::encoded_length(expected_pc_config),
              present_wire.length());

    SMART_VAR(sql::ObSQLSessionInfo, present_receiver) {
      ObBasicSessionInfo::LockGuard receiver_lock(
          present_receiver.get_query_lock());
      ASSERT_EQ(OB_SUCCESS, present_receiver.test_init(
          0, 0, 0, &present_receiver_allocator));
      ASSERT_TRUE(NULL == present_receiver.cur_phy_plan_);
      ASSERT_NO_FATAL_FAILURE(deserialize_session(present_receiver, present_wire));
      ASSERT_TRUE(NULL == present_receiver.cur_phy_plan_);
      ASSERT_EQ(expected_pc_config, present_receiver.config_in_pc_str_);
      uint64_t expected_receiver_hash =
          present_receiver.sys_var_in_pc_str_.hash();
      expected_receiver_hash =
          expected_pc_config.hash(expected_receiver_hash);
      ASSERT_EQ(expected_receiver_hash,
                present_receiver.sys_var_config_hash_val_);

      MEMSET(present_wire.ptr(), 0, present_wire.length());
      ASSERT_EQ(expected_pc_config, present_receiver.config_in_pc_str_);
      present_receiver.sys_var_config_hash_val_ = 0;
      present_receiver.eval_sys_var_config_hash_val();
      ASSERT_EQ(expected_receiver_hash,
                present_receiver.sys_var_config_hash_val_);
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
