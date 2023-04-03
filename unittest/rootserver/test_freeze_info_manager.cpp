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

#define USING_LOG_PREFIX RS

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#define private public
#include "lib/stat/ob_session_stat.h"
#include "../share/schema/db_initializer.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "rootserver/freeze/ob_freeze_info_manager.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "share/ob_zone_merge_table_operator.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_remote_sql_proxy.h"
#include "share/ob_freeze_info_proxy.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class TestFreezeInfoManager : public ::testing::Test
{
public:
  TestFreezeInfoManager() {}
  virtual ~TestFreezeInfoManager() {}
  virtual void SetUp();
  virtual void TearDown() {}
protected:
  virtual void check_info(const ObZoneMergeInfo &l,
                          const ObZoneMergeInfo &r);
  DBInitializer db_initer_;
  ObZoneMergeManager zone_merge_mgr_;
  ObFreezeInfoManager freeze_info_mgr_;
  ObRemoteSqlProxy remote_proxy_;
  ObGlobalMergeInfo global_merge_info_;

  const static uint64_t DEFAULT_TENANT_ID = 1001;

private:
  int init_zone_merge_manager();
  int init_freeze_info_manager();
  void build_global_merge_info();
};

int TestFreezeInfoManager::init_zone_merge_manager()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_merge_mgr_.init(DEFAULT_TENANT_ID, db_initer_.get_sql_proxy()))) {
    LOG_WARN("fail to init zone_merge_manager", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.reload())) {
    LOG_WARN("fail to reload zone_merge_mnnager", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    build_global_merge_info();
    if (OB_FAIL(ObZoneMergeTableOperator::insert_global_merge_info(
      db_initer_.get_sql_proxy(), global_merge_info_))) {
      LOG_WARN("fail to insert global_merge_info");
    } else {
      ObZone zones[] = { "zone1", "zone2" };
      for (int64_t i = 0; i < static_cast<int64_t>(sizeof(zones) / sizeof(ObZone)) && OB_SUCC(ret); ++i) {
        ObZoneMergeInfo info;
        info.tenant_id_ = DEFAULT_TENANT_ID;
        info.zone_ = zones[i];
        if (OB_FAIL(ObZoneMergeTableOperator::insert_zone_merge_info(db_initer_.get_sql_proxy(), info))) {
          LOG_WARN("fail to insert zone_merge_info");
        }
      }
    }
  }
  return ret;
}

int TestFreezeInfoManager::init_freeze_info_manager()
{
  int ret = OB_SUCCESS;
  // insert a row into __all_freeze_info
  {
    ObMySQLTransaction trans;
    ObFreezeInfoProxy freeze_proxy(DEFAULT_TENANT_ID);
    ObSimpleFrozenStatus frozen_status(1, ObTimeUtility::current_time(), 3330);
    ObMySQLProxy &sql_proxy = db_initer_.get_sql_proxy();
    if (OB_FAIL(trans.start(&sql_proxy, DEFAULT_TENANT_ID))) {
      LOG_WARN("fail to start transaction", KR(ret));
    } else if (OB_FAIL(freeze_proxy.set_freeze_info(trans, frozen_status))) {
      LOG_WARN("fail to set freeze info", KR(ret));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        LOG_WARN("fail to end trans", "is_commit", OB_SUCC(ret), KR(tmp_ret));
      }
    }
  }

  if (OB_FAIL(freeze_info_mgr_.init(DEFAULT_TENANT_ID, db_initer_.get_sql_proxy(), 
        remote_proxy_, zone_merge_mgr_))) {
    LOG_WARN("fail to init freeze_info_manager", KR(ret));
  } else if (OB_FAIL(freeze_info_mgr_.reload())) {
    LOG_WARN("fail to reload freeze_info_manager", KR(ret));
  } else if (OB_FAIL(freeze_info_mgr_.check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

void TestFreezeInfoManager::build_global_merge_info()
{
  global_merge_info_.tenant_id_ = DEFAULT_TENANT_ID;
  global_merge_info_.try_frozen_version_.value_ = 1;
  global_merge_info_.frozen_version_.value_ = 1;
  global_merge_info_.frozen_time_.value_ = 99999;
  global_merge_info_.global_broadcast_version_.value_ = 1;
  global_merge_info_.last_merged_version_.value_ = 1;
  global_merge_info_.is_merge_error_.value_ = 0;
}

void TestFreezeInfoManager::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());

  const bool only_core_tables = false;
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(only_core_tables));
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space_tables(DEFAULT_TENANT_ID));

  ASSERT_EQ(OB_SUCCESS, init_zone_merge_manager());
  ASSERT_EQ(OB_SUCCESS, init_freeze_info_manager());
}

void TestFreezeInfoManager::check_info(
    const ObZoneMergeInfo &l,
    const ObZoneMergeInfo &r)
{
  ASSERT_EQ(l.tenant_id_, r.tenant_id_);
  ASSERT_EQ(l.zone_, r.zone_);
  ASSERT_EQ(l.broadcast_version_.value_, r.broadcast_version_.value_);
  ASSERT_EQ(l.last_merged_version_.value_, r.last_merged_version_.value_);
  ASSERT_EQ(l.last_merged_time_.value_, r.last_merged_time_.value_);
  ASSERT_EQ(l.merge_start_time_.value_, r.merge_start_time_.value_);
  ASSERT_EQ(l.is_merge_timeout_.value_, r.is_merge_timeout_.value_);
}

TEST_F(TestFreezeInfoManager, common)
{
  int64_t frozen_version = 2;
  ObSimpleFrozenStatus frozen_status;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, freeze_info_mgr_.get_freeze_info(frozen_version, frozen_status));

  ASSERT_EQ(OB_SUCCESS, freeze_info_mgr_.set_freeze_info(frozen_version));

  ASSERT_EQ(false, frozen_status.is_valid());
  ASSERT_EQ(OB_SUCCESS, freeze_info_mgr_.get_freeze_info(frozen_version, frozen_status));
  ASSERT_EQ(true, frozen_status.is_valid());

  frozen_version = 3;
  frozen_status.reset();
  ASSERT_EQ(false, frozen_status.is_valid());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, freeze_info_mgr_.get_freeze_info(frozen_version, frozen_status));
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}