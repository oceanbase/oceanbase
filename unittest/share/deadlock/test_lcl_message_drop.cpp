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
#include "lib/ob_errno.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "share/deadlock/ob_deadlock_key_wrapper.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/deadlock/test/test_key.h"
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>
#include "share/deadlock/ob_lcl_scheme/ob_lcl_batch_sender_thread.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_message.h"
#include "storage/tx/ob_trans_define_v4.h"

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace share::detector;
using namespace std;


class TestLCLMsgDrop : public ::testing::Test {
public:
  TestLCLMsgDrop() {}
  ~TestLCLMsgDrop() {}
  virtual void SetUp() { share::ObTenantEnv::get_tenant_local()->id_ = 1; }
  virtual void TearDown() {}
  static ObLCLBatchSenderThread batch_sender_;
  int port;
};
ObLCLBatchSenderThread TestLCLMsgDrop::batch_sender_(nullptr);

TEST_F(TestLCLMsgDrop, always_keep) {// the first 2048 will always success
  ASSERT_EQ(OB_SUCCESS, batch_sender_.init());
  ASSERT_EQ(OB_SUCCESS, batch_sender_.start());
  ATOMIC_STORE(&batch_sender_.allow_send_, false);
  ObLCLMessage mock_message;
  ObAddr mock_addr(ObAddr::VER::IPV4, "127.0.0.1", 0);
  UserBinaryKey mock_self_key, mock_dest_key;
  ObLCLLabel mock_lcl_label(1, ObDetectorPriority(1));
  mock_lcl_label.addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1);
  ASSERT_EQ(OB_SUCCESS, mock_self_key.set_user_key(transaction::ObTransID(1)));
  ASSERT_EQ(OB_SUCCESS, mock_dest_key.set_user_key(transaction::ObTransID(1)));

  for (int i = 0; i < LCL_MSG_CACHE_LIMIT / 2; ++i) {
    mock_addr.set_port(i + 1);
    ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                                mock_dest_key,
                                                ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                                mock_self_key,
                                                0,
                                                mock_lcl_label,
                                                1));
    ObDependencyResource mock_resource(mock_addr, mock_dest_key);
    ObDependencyResource mock_resource2(mock_addr, mock_dest_key);
    OB_ASSERT(mock_resource == mock_resource2);
    ASSERT_EQ(OB_SUCCESS, batch_sender_.cache_msg(mock_resource, mock_message));
    ObLCLMessage read_message;
    ASSERT_EQ(OB_SUCCESS, batch_sender_.lcl_msg_map_.get(mock_resource, read_message));
    DETECT_LOG(INFO, "print mock resource message", K(read_message));
  }
}

TEST_F(TestLCLMsgDrop, random_drop_25_percentage) {
  int ret = OB_SUCCESS;
  ObLCLMessage mock_message;
  ObAddr mock_addr(ObAddr::VER::IPV4, "127.0.0.1", 0);
  UserBinaryKey mock_self_key, mock_dest_key;
  ObLCLLabel mock_lcl_label(1, ObDetectorPriority(1));
  mock_lcl_label.addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1);
  ASSERT_EQ(OB_SUCCESS, mock_self_key.set_user_key(transaction::ObTransID(1)));
  ASSERT_EQ(OB_SUCCESS, mock_dest_key.set_user_key(transaction::ObTransID(1)));
  for (int i = LCL_MSG_CACHE_LIMIT / 2; i < LCL_MSG_CACHE_LIMIT / 2 + LCL_MSG_CACHE_LIMIT / 8; ++i) {
    mock_addr.set_port(i + 1);
    ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                                mock_dest_key,
                                                ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                                mock_self_key,
                                                0,
                                                mock_lcl_label,
                                                1));
    ObDependencyResource mock_resource(mock_addr, mock_dest_key);
    ASSERT_EQ(OB_SUCCESS, batch_sender_.lcl_msg_map_.insert(mock_resource, mock_message));
  }

  mock_addr.set_port(65535);
  int fail_times = 0, succ_times = 0;
  for (int i = 0; i < 100000; ++i) {
    ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                                mock_dest_key,
                                                ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                                mock_self_key,
                                                0,
                                                mock_lcl_label,
                                                1));
    ObDependencyResource mock_resource(mock_addr, mock_dest_key);
    if (OB_SUCC(batch_sender_.cache_msg(mock_resource, mock_message))) {
      ++succ_times;
      batch_sender_.lcl_msg_map_.erase(mock_resource);
    } else {
      ++fail_times;
    }
  }
  int fail_percentage = fail_times * 100 / (succ_times + fail_times);
  ASSERT_GE(fail_percentage, 20);
  ASSERT_LE(fail_percentage, 30);
  DETECT_LOG(INFO, "print drop percentage", K(fail_percentage));
}

TEST_F(TestLCLMsgDrop, random_drop_75_percentage) {
  int ret = OB_SUCCESS;
  ObLCLMessage mock_message;
  ObAddr mock_addr(ObAddr::VER::IPV4, "127.0.0.1", 0);
  UserBinaryKey mock_self_key, mock_dest_key;
  ObLCLLabel mock_lcl_label(1, ObDetectorPriority(1));
  mock_lcl_label.addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1);
  ASSERT_EQ(OB_SUCCESS, mock_self_key.set_user_key(transaction::ObTransID(1)));
  ASSERT_EQ(OB_SUCCESS, mock_dest_key.set_user_key(transaction::ObTransID(1)));
  for (int i = LCL_MSG_CACHE_LIMIT / 2 + LCL_MSG_CACHE_LIMIT / 8; i < LCL_MSG_CACHE_LIMIT / 2 + LCL_MSG_CACHE_LIMIT * 3 / 8; ++i) {
    mock_addr.set_port(i + 1);
    ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                                mock_dest_key,
                                                ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                                mock_self_key,
                                                0,
                                                mock_lcl_label,
                                                1));
    ObDependencyResource mock_resource(mock_addr, mock_dest_key);
    ASSERT_EQ(OB_SUCCESS, batch_sender_.lcl_msg_map_.insert(mock_resource, mock_message));
  }

  mock_addr.set_port(65535);
  int fail_times = 0, succ_times = 0;
  for (int i = 0; i < 100000; ++i) {
    ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                                mock_dest_key,
                                                ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                                mock_self_key,
                                                0,
                                                mock_lcl_label,
                                                1));
    ObDependencyResource mock_resource(mock_addr, mock_dest_key);
    if (OB_SUCC(batch_sender_.cache_msg(mock_resource, mock_message))) {
      ++succ_times;
      batch_sender_.lcl_msg_map_.erase(mock_resource);
    } else {
      ++fail_times;
    }
  }
  int fail_percentage = fail_times * 100 / (succ_times + fail_times);
  ASSERT_GE(fail_percentage, 70);
  ASSERT_LE(fail_percentage, 80);
  DETECT_LOG(INFO, "print drop percentage", K(fail_percentage));
}

TEST_F(TestLCLMsgDrop, always_drop) {
  int ret = OB_SUCCESS;
  ObLCLMessage mock_message;
  ObAddr mock_addr(ObAddr::VER::IPV4, "127.0.0.1", 0);
  UserBinaryKey mock_self_key, mock_dest_key;
  ObLCLLabel mock_lcl_label(1, ObDetectorPriority(1));
  mock_lcl_label.addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1);
  ASSERT_EQ(OB_SUCCESS, mock_self_key.set_user_key(transaction::ObTransID(1)));
  ASSERT_EQ(OB_SUCCESS, mock_dest_key.set_user_key(transaction::ObTransID(1)));
  for (int i = LCL_MSG_CACHE_LIMIT / 2 + LCL_MSG_CACHE_LIMIT * 3 / 8; i < LCL_MSG_CACHE_LIMIT; ++i) {
    mock_addr.set_port(i + 1);
    ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                                mock_dest_key,
                                                ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                                mock_self_key,
                                                0,
                                                mock_lcl_label,
                                                1));
    ObDependencyResource mock_resource(mock_addr, mock_dest_key);
    ASSERT_EQ(OB_SUCCESS, batch_sender_.lcl_msg_map_.insert(mock_resource, mock_message));
  }

  mock_addr.set_port(65535);
  for (int i = 0; i < 100000; ++i) {
    ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                                mock_dest_key,
                                                ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                                mock_self_key,
                                                0,
                                                mock_lcl_label,
                                                1));
    ObDependencyResource mock_resource(mock_addr, mock_dest_key);
    ASSERT_EQ(OB_BUF_NOT_ENOUGH, batch_sender_.cache_msg(mock_resource, mock_message));
  }
}

TEST_F(TestLCLMsgDrop, message_merge_when_reach_limit) {
  int ret = OB_SUCCESS;
  ObLCLMessage mock_message;
  ObLCLMessage read_message;
  ObAddr mock_addr(ObAddr::VER::IPV4, "127.0.0.1", 1);
  UserBinaryKey mock_self_key, mock_dest_key;
  ObLCLLabel mock_lcl_label(0, ObDetectorPriority(0));
  mock_lcl_label.addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1);
  ASSERT_EQ(OB_SUCCESS, mock_self_key.set_user_key(transaction::ObTransID(1)));
  ASSERT_EQ(OB_SUCCESS, mock_dest_key.set_user_key(transaction::ObTransID(1)));

  ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                              mock_dest_key,
                                              ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                              mock_self_key,
                                              0,
                                              mock_lcl_label,
                                              1));
  ObDependencyResource mock_resource(mock_addr, mock_dest_key);
  ASSERT_EQ(OB_SUCCESS, batch_sender_.lcl_msg_map_.get(mock_resource, read_message));
  DETECT_LOG(INFO, "print mock resource message", K(read_message));
  ASSERT_EQ(OB_SUCCESS, batch_sender_.cache_msg(mock_resource, mock_message));
  ASSERT_EQ(OB_SUCCESS, batch_sender_.lcl_msg_map_.get(mock_resource, read_message));
  ASSERT_EQ(0, read_message.label_.id_);

  new (&mock_lcl_label) ObLCLLabel(1, ObDetectorPriority(1));
  mock_lcl_label.addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1);
  ASSERT_EQ(OB_SUCCESS, mock_message.set_args(mock_addr,
                                              mock_dest_key,
                                              ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1),
                                              mock_self_key,
                                              1,
                                              mock_lcl_label,
                                              1));
  ASSERT_EQ(OB_SUCCESS, batch_sender_.cache_msg(mock_resource, mock_message));
  ASSERT_EQ(OB_SUCCESS, batch_sender_.lcl_msg_map_.get(mock_resource, read_message));
  ASSERT_EQ(1, read_message.label_.id_);
}

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_lcl_message_drop.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_lcl_message_drop.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}