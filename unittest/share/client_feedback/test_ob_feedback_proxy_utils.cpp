/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#define private public
#include "observer/mysql/ob_feedback_proxy_utils.h"
#include "src/sql/session/ob_sql_session_info.h"
#undef private
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace unittest
{

using namespace observer;
using namespace common;
using namespace std;

bool verifyHexString(const char *data, const size_t data_size, const std::string &hex_string)
{
  // data length mismatch
  if (data_size * 2 != hex_string.size()) {
    return false;
  }

  std::ostringstream stream;
  for (size_t i = 0; i < data_size; ++i) {
    stream << std::hex << std::setfill('0') << std::setw(2) << (0xFF & static_cast<unsigned char>(data[i]));
  }

  return stream.str() == hex_string;
}

void set_gtt_non_forced_routing(sql::ObSQLSessionInfo &session, const bool enabled)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  session.effective_tenant_id_ = tenant_id;
  ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id));
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  ASSERT_TRUE(tenant_config.is_valid());
  tenant_config->_enable_gtt_non_forced_routing = enabled;
}

TEST(ObFeedbackProxyInfo, test_serialize)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  bool verified = false;

  ObIsLockSessionInfo is_lock_session(ObFeedbackProxyInfoType::IS_LOCK_SESSION, '1');
  ret = is_lock_session.serialize(buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 7);

  // result in hex
  std::string hex_string = "00000100000031";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}

TEST(ObFeedbackProxyUtils, test_serialize_from_session)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  sql::ObSQLSessionInfo session;
  bool verified = false;

  session.set_is_lock_session(true);
  session.set_is_temporary_table_session(true);
  ret = ObFeedbackProxyUtils::serialize_(session, buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 14);

  // result in hex
  std::string hex_string = "0000010000003101000100000031";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}

TEST(ObFeedbackProxyUtils, test_serialize_from_kv)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  sql::ObSQLSessionInfo session;
  bool verified = false;

  ObSEArray<short, 3> test_array;
  test_array.push_back(1);
  test_array.push_back(2);
  test_array.push_back(3);
  ret = ObFeedbackProxyUtils::serialize_(ObFeedbackProxyInfoType::IS_LOCK_SESSION, test_array, buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 10);

  // result in hex
  std::string hex_string = "00000400000003010203";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}

TEST(ObFeedbackProxyUtils, feedback_when_session_tablet_state_changes)
{
  sql::ObSQLSessionInfo session;
  ObArenaAllocator allocator;
  ObSEArray<obmysql::Obp20Encoder *, 2> extra_info_ecds;
  storage::ObSessionTabletInfo tablet_info(
      ObTabletID(200001), share::ObLSID(1001), 500001,
      storage::OB_GTT_V2_SESS_TABLET_SEQUENCE, 1, 0);

  session.set_feedback_proxy_info_support(true);
  session.set_client_sessid(1);
  set_gtt_non_forced_routing(session, true);
  session.min_data_version_of_init_sess_ = MOCK_DATA_VERSION_4_4_2_1;

  // An empty map does not change the initial false state.
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_EQ(0, extra_info_ecds.count());

  ASSERT_EQ(OB_SUCCESS,
      session.get_gtt_tablet_info_map().tablet_infos_.push_back(tablet_info));

  // The first session tablet changes false to true and triggers feedback.
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_TRUE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(1, extra_info_ecds.count());
  obmysql::Obp20FeedbackProxyInfoEncoder *feedback_encoder =
      static_cast<obmysql::Obp20FeedbackProxyInfoEncoder *>(extra_info_ecds.at(0));
  ASSERT_TRUE(verifyHexString(feedback_encoder->feedback_proxy_info_.ptr(),
                             feedback_encoder->feedback_proxy_info_.length(),
                             "0000010000003001000100000031"));

  // Keeping a nonempty map does not trigger duplicate feedback.
  extra_info_ecds.reset();
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_TRUE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(0, extra_info_ecds.count());

  // Removing the last session tablet changes true to false and feeds back.
  session.get_gtt_tablet_info_map().reset();
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(1, extra_info_ecds.count());
  feedback_encoder =
      static_cast<obmysql::Obp20FeedbackProxyInfoEncoder *>(extra_info_ecds.at(0));
  ASSERT_TRUE(verifyHexString(feedback_encoder->feedback_proxy_info_.ptr(),
                             feedback_encoder->feedback_proxy_info_.length(),
                             "0000010000003001000100000030"));

  // Keeping an empty map does not trigger duplicate feedback either.
  extra_info_ecds.reset();
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(0, extra_info_ecds.count());
}

TEST(ObFeedbackProxyUtils, keep_marked_state_when_non_forced_routing_is_disabled)
{
  sql::ObSQLSessionInfo session;
  ObArenaAllocator allocator;
  ObSEArray<obmysql::Obp20Encoder *, 2> extra_info_ecds;
  storage::ObSessionTabletInfo tablet_info(
      ObTabletID(200001), share::ObLSID(1001), 500001,
      storage::OB_GTT_V2_SESS_TABLET_SEQUENCE, 1, 0);

  session.set_feedback_proxy_info_support(true);
  session.set_client_sessid(1);
  set_gtt_non_forced_routing(session, true);
  session.min_data_version_of_init_sess_ = MOCK_DATA_VERSION_4_4_2_1;
  ASSERT_EQ(OB_SUCCESS,
      session.get_gtt_tablet_info_map().tablet_infos_.push_back(tablet_info));

  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_TRUE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(1, extra_info_ecds.count());

  // Disabling non-forced routing does not change a session that has already
  // been marked while its session tablet still exists.
  extra_info_ecds.reset();
  set_gtt_non_forced_routing(session, false);
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_TRUE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(0, extra_info_ecds.count());

  // Removing the last session tablet still clears the marked state and sends
  // the false value back to the proxy.
  session.get_gtt_tablet_info_map().reset();
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(1, extra_info_ecds.count());
  obmysql::Obp20FeedbackProxyInfoEncoder *feedback_encoder =
      static_cast<obmysql::Obp20FeedbackProxyInfoEncoder *>(extra_info_ecds.at(0));
  ASSERT_TRUE(verifyHexString(feedback_encoder->feedback_proxy_info_.ptr(),
                             feedback_encoder->feedback_proxy_info_.length(),
                             "0000010000003001000100000030"));

  extra_info_ecds.reset();
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(0, extra_info_ecds.count());
}

TEST(ObFeedbackProxyUtils, strong_routing_only_gates_unmarked_session)
{
  sql::ObSQLSessionInfo session;
  ObArenaAllocator allocator;
  ObSEArray<obmysql::Obp20Encoder *, 2> extra_info_ecds;
  storage::ObSessionTabletInfo tablet_info(
      ObTabletID(200001), share::ObLSID(1001), 500001,
      storage::OB_GTT_V2_SESS_TABLET_SEQUENCE, 1, 0);

  session.set_feedback_proxy_info_support(true);
  session.set_client_sessid(1);
  set_gtt_non_forced_routing(session, false);
  session.min_data_version_of_init_sess_ = MOCK_DATA_VERSION_4_4_2_1;
  ASSERT_EQ(OB_SUCCESS,
      session.get_gtt_tablet_info_map().tablet_infos_.push_back(tablet_info));

  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(0, extra_info_ecds.count());

  // An unmarked session can be marked after non-forced routing is enabled.
  set_gtt_non_forced_routing(session, true);
  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_TRUE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(1, extra_info_ecds.count());
  obmysql::Obp20FeedbackProxyInfoEncoder *feedback_encoder =
      static_cast<obmysql::Obp20FeedbackProxyInfoEncoder *>(extra_info_ecds.at(0));
  ASSERT_TRUE(verifyHexString(feedback_encoder->feedback_proxy_info_.ptr(),
                             feedback_encoder->feedback_proxy_info_.length(),
                             "0000010000003001000100000031"));
}

TEST(ObFeedbackProxyUtils, no_session_tablet_feedback_without_client_sessid)
{
  sql::ObSQLSessionInfo session;
  ObArenaAllocator allocator;
  ObSEArray<obmysql::Obp20Encoder *, 2> extra_info_ecds;
  storage::ObSessionTabletInfo tablet_info(
      ObTabletID(200001), share::ObLSID(1001), 500001,
      storage::OB_GTT_V2_SESS_TABLET_SEQUENCE, 1, 0);

  session.set_feedback_proxy_info_support(true);
  set_gtt_non_forced_routing(session, true);
  session.min_data_version_of_init_sess_ = MOCK_DATA_VERSION_4_4_2_1;
  ASSERT_EQ(INVALID_SESSID, session.get_client_sid());
  ASSERT_EQ(OB_SUCCESS,
      session.get_gtt_tablet_info_map().tablet_infos_.push_back(tablet_info));

  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_FALSE(session.is_need_send_feedback_proxy_info());
  ASSERT_EQ(0, extra_info_ecds.count());
}

TEST(ObFeedbackProxyUtils, no_session_tablet_feedback_before_data_version_gate)
{
  sql::ObSQLSessionInfo session;
  ObArenaAllocator allocator;
  ObSEArray<obmysql::Obp20Encoder *, 2> extra_info_ecds;
  storage::ObSessionTabletInfo tablet_info(
      ObTabletID(200001), share::ObLSID(1001), 500001,
      storage::OB_GTT_V2_SESS_TABLET_SEQUENCE, 1, 0);

  session.set_feedback_proxy_info_support(true);
  session.set_client_sessid(1);
  set_gtt_non_forced_routing(session, true);
  session.min_data_version_of_init_sess_ = MOCK_DATA_VERSION_4_4_2_0;
  ASSERT_EQ(OB_SUCCESS,
      session.get_gtt_tablet_info_map().tablet_infos_.push_back(tablet_info));

  ASSERT_EQ(OB_SUCCESS, ObFeedbackProxyUtils::append_feedback_proxy_info(
      allocator, &extra_info_ecds, session));
  ASSERT_FALSE(session.is_temporary_table_session());
  ASSERT_EQ(0, extra_info_ecds.count());
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ob_feedback_proxy_utils.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_feedback_proxy_utils.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
