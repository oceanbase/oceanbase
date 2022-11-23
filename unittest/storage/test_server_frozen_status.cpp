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

#include "share/ob_common_rpc_proxy.h"
#include "lib/lock/ob_mutex.h"
#include "storage/ob_server_frozen_status.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::obrpc;

// > MAX_STORE_CNT_IN_STORAGE
const int32_t INIT_RPC_MAX_VERSION = 74;

class MockRootRpcProxy : public ObCommonRpcProxy
{
public:
  MockRootRpcProxy() : latest_version_lock_(),
                       rpc_read_count_(0)
  {}
  virtual ~MockRootRpcProxy() {}

  int get_frozen_status(const Int64 &arg,
                        ObFrozenStatus &frozen_status,
                        const ObRpcOpts &opts);
  int64_t get_rpc_read_count() const { return rpc_read_count_; }
private:
  oceanbase::lib::ObMutex latest_version_lock_;
  int64_t rpc_read_count_;
};

int MockRootRpcProxy::get_frozen_status(
    const Int64 &arg,
    ObFrozenStatus &frozen_status,
    const ObRpcOpts &opts)
{
  int ret = OB_SUCCESS;
  UNUSED(opts);
  if (0 == arg) {
    oceanbase::lib::ObMutexGuard guard(latest_version_lock_);
    frozen_status.frozen_version_.major_ = INIT_RPC_MAX_VERSION;
    frozen_status.frozen_timestamp_ = INIT_RPC_MAX_VERSION;
    frozen_status.status_ = COMMIT_SUCCEED;
    frozen_status.schema_version_ = INIT_RPC_MAX_VERSION;
    ++rpc_read_count_;
  } else {
    frozen_status.frozen_version_.major_ = static_cast<int32_t>(arg);
    frozen_status.frozen_timestamp_ = arg;
    frozen_status.status_ = COMMIT_SUCCEED;
    frozen_status.schema_version_ = arg;
    oceanbase::lib::ObMutexGuard guard(latest_version_lock_);
    ++rpc_read_count_;
  }

  return ret;
}

class TestObServerFrozenStatus : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase();
  static void TearDownTestCase();

  static MockRootRpcProxy rs_rpc_proxy_;
  static ObServerFrozenStatus observer_frozen_status_;
};
MockRootRpcProxy TestObServerFrozenStatus::rs_rpc_proxy_;
ObServerFrozenStatus TestObServerFrozenStatus::observer_frozen_status_;

void TestObServerFrozenStatus::SetUpTestCase()
{
  observer_frozen_status_.set_rs_rpc_proxy(&rs_rpc_proxy_);
}

void TestObServerFrozenStatus::TearDownTestCase()
{
}

void check_frozen_status(const int32_t major_version,
                         const int64_t freeze_status,
                         const ObFrozenStatus &frozen_status)
{
  ASSERT_EQ(frozen_status.frozen_version_, ObVersion(major_version, 0));
  ASSERT_EQ(frozen_status.status_, freeze_status);
  ASSERT_EQ(frozen_status.frozen_timestamp_, major_version);
  ASSERT_EQ(frozen_status.schema_version_, major_version);
}

TEST_F(TestObServerFrozenStatus, test_get_from_rpc)
{
  int ret = OB_SUCCESS;
  ObFrozenStatus frozen_status;



  ret = observer_frozen_status_.get_frozen_status(ObVersion(5, 0),
                                                  frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(5, COMMIT_SUCCEED, frozen_status);
  ASSERT_EQ(observer_frozen_status_.count(), 1);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 1);
  // get from local
  ret = observer_frozen_status_.get_frozen_status(ObVersion(5, 0),
                                                  frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(5, COMMIT_SUCCEED, frozen_status);
  ASSERT_EQ(observer_frozen_status_.count(), 1);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 1);


  ret = observer_frozen_status_.get_frozen_status(ObVersion(6, 0),
                                                  frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(6, COMMIT_SUCCEED, frozen_status);
  ASSERT_EQ(observer_frozen_status_.count(), 2);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 2);


  ret = observer_frozen_status_.get_frozen_status(frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(static_cast<int32_t>(INIT_RPC_MAX_VERSION),
                      COMMIT_SUCCEED, frozen_status);
  ASSERT_EQ(observer_frozen_status_.count(), 1);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 3);


  ret = observer_frozen_status_.get_frozen_status(ObVersion(150, 0),
                                                  frozen_status);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ASSERT_EQ(observer_frozen_status_.count(), 1);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 4);


  ret = observer_frozen_status_.get_frozen_status(ObVersion(1, 0),
                                                  frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(1, COMMIT_SUCCEED, frozen_status);
  ASSERT_EQ(observer_frozen_status_.count(), 1);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 5);

  ret = observer_frozen_status_.get_frozen_status(ObVersion(1, 0),
                                                  frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(1, COMMIT_SUCCEED, frozen_status);
  ASSERT_EQ(observer_frozen_status_.count(), 1);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 6);

  for (int32_t i = 2; OB_SUCC(ret) && i < 5; ++i) {
    ret = observer_frozen_status_.get_frozen_status(ObVersion(i, 0),
                                                    frozen_status);
    ASSERT_EQ(OB_SUCCESS, ret);
    check_frozen_status(i, COMMIT_SUCCEED, frozen_status);
  }
  ASSERT_EQ(observer_frozen_status_.count(), 1);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 9);


  for(int32_t i = 7; OB_SUCC(ret) && i < INIT_RPC_MAX_VERSION; ++i) {
    ret = observer_frozen_status_.get_frozen_status(ObVersion(i, 0),
                                                    frozen_status);
    ASSERT_EQ(OB_SUCCESS, ret);
    check_frozen_status(i, COMMIT_SUCCEED, frozen_status);
  }
  ASSERT_EQ(observer_frozen_status_.count(), MAX_STORE_CNT_IN_STORAGE);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 76);


  int32_t i = static_cast<int32_t>(INIT_RPC_MAX_VERSION) -
              MAX_STORE_CNT_IN_STORAGE + 1;
  for(; OB_SUCC(ret) && i <= INIT_RPC_MAX_VERSION; ++i) {
    ret = observer_frozen_status_.get_frozen_status(ObVersion(i, 0),
                                                    frozen_status);
    ASSERT_EQ(OB_SUCCESS, ret);
    check_frozen_status(i, COMMIT_SUCCEED, frozen_status);
  }
  ASSERT_EQ(observer_frozen_status_.count(), MAX_STORE_CNT_IN_STORAGE);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 76);
}

TEST_F(TestObServerFrozenStatus, set_frozen_status)
{
  int ret = OB_SUCCESS;
  int32_t next = INIT_RPC_MAX_VERSION + 1;
  ObVersion version(next, 0);

  // must be 75-prepare
  ObFrozenStatus frozen_status(version, next, COMMIT_SUCCEED, next);
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  frozen_status.status_ = INIT_STATUS;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  frozen_status.status_ = PREPARED_SUCCEED;
  frozen_status.frozen_version_ = ObVersion(INIT_RPC_MAX_VERSION + 2, 0);
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  frozen_status.frozen_version_ = next;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObFrozenStatus read_frozen_status;
  ret = observer_frozen_status_.get_frozen_status(read_frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(INIT_RPC_MAX_VERSION + 1,
                      PREPARED_SUCCEED,
                      read_frozen_status);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 76);

  // must be 75-commit or 75-abort
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  frozen_status.status_ = COMMIT_SUCCEED;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = observer_frozen_status_.get_frozen_status(read_frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(INIT_RPC_MAX_VERSION + 1,
                      COMMIT_SUCCEED,
                      read_frozen_status);
  ASSERT_EQ(rs_rpc_proxy_.get_rpc_read_count(), 76);

  // must be 76-prepare
  frozen_status.status_ = PREPARED_SUCCEED;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  frozen_status.status_ = COMMIT_SUCCEED;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  frozen_status.status_ = INIT_STATUS;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  frozen_status.status_ = PREPARED_SUCCEED;
  frozen_status.frozen_version_ = ObVersion(INIT_RPC_MAX_VERSION + 2, 0);
  frozen_status.frozen_timestamp_ = INIT_RPC_MAX_VERSION + 2;
  frozen_status.schema_version_ = INIT_RPC_MAX_VERSION + 2;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = observer_frozen_status_.get_frozen_status(read_frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(INIT_RPC_MAX_VERSION + 2,
                      PREPARED_SUCCEED,
                      read_frozen_status);

  // can abort unless committed
  frozen_status.status_ = INIT_STATUS;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = observer_frozen_status_.get_frozen_status(read_frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(INIT_RPC_MAX_VERSION + 2,
                      INIT_STATUS,
                      read_frozen_status);
  // abort again
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = observer_frozen_status_.get_frozen_status(read_frozen_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_frozen_status(INIT_RPC_MAX_VERSION + 2,
                      INIT_STATUS,
                      read_frozen_status);
  frozen_status.frozen_version_ = ObVersion(INIT_RPC_MAX_VERSION + 1, 0);
  frozen_status.frozen_timestamp_ = INIT_RPC_MAX_VERSION + 1;
  frozen_status.schema_version_ = INIT_RPC_MAX_VERSION + 1;
  ret = observer_frozen_status_.set_frozen_status(frozen_status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

}

TEST_F(TestObServerFrozenStatus, set_frozen_status_4096)
{
  int ret = OB_SUCCESS;
  ObFrozenStatus old;
  ObFrozenStatus tgt;
  ObFrozenStatus frozen_status;
  ret = observer_frozen_status_.get_frozen_status(old);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int32_t i = 0; OB_SUCC(ret) && i <= 4096; ++i) {
    tgt.frozen_version_.major_ = old.frozen_version_.major_ + i;
    tgt.frozen_version_.minor_ = 0;
    tgt.status_ = PREPARED_SUCCEED;
    tgt.frozen_timestamp_ = ObTimeUtility::current_time();
    tgt.schema_version_ = old.schema_version_;
    ret = observer_frozen_status_.set_frozen_status(tgt);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = observer_frozen_status_.get_frozen_status(tgt.frozen_version_, frozen_status);
    ASSERT_EQ(OB_SUCCESS, ret);
    tgt.status_ = COMMIT_SUCCEED;
    ret = observer_frozen_status_.set_frozen_status(tgt);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = observer_frozen_status_.get_frozen_status(tgt.frozen_version_, frozen_status);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_server_frozen_status.log");
  OB_LOGGER.set_log_level(OB_LOG_LEVEL_INFO);
  CLOG_LOG(INFO, "begin unittest: test_server_frozen_status");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

