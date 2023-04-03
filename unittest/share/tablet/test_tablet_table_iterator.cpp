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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/schema/db_initializer.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/tablet/ob_tablet_info.h"

namespace oceanbase
{
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;
class TestTabletIterator : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  DBInitializer db_initer_;
  ObTabletTableOperator operator_;
  ObTenantTabletMetaIterator iterator_;
};

void TestTabletIterator::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = operator_.init(db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = iterator_.init(db_initer_.get_sql_proxy(), OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  int sql_port = 1234;
  std::string host = "1.2.3.4";
  ObAddr server(ObAddr::VER::IPV4, host.c_str(), sql_port);
  int64_t data_version = 1;
  int64_t data_size = 1;
  int64_t required_size = 1;

  ObSEArray<ObTabletReplica, 3> replicas;
  ObTabletReplica replica;
  ObLSID ls_id(1);
  replica.init(OB_SYS_TENANT_ID, ObTabletID(2), ls_id, server, data_version, data_size, required_size);
  replicas.push_back(replica);

  replica.reset();
  replica.init(OB_SYS_TENANT_ID, ObTabletID(3), ls_id, server, data_version, data_size, required_size);
  replicas.push_back(replica);

  replica.reset();
  replica.init(OB_SYS_TENANT_ID, ObTabletID(4), ls_id, server, data_version, data_size, required_size);
  replicas.push_back(replica);

  ObSEArray<ObTabletToLSInfo, 3> ls_infos;
  ObTabletToLSInfo info;
  const uint64_t table_id = 1;
  info.init(ObTabletID(2), ObLSID(2), table_id);
  ls_infos.push_back(info);

  info.reset();
  info.init(ObTabletID(3), ObLSID(3), table_id);
  ls_infos.push_back(info);

  info.reset();
  info.init(ObTabletID(4), ObLSID(4), table_id);
  ls_infos.push_back(info);

  ret = ObTabletToLSTableOperator::batch_update(db_initer_.get_sql_proxy(), OB_SYS_TENANT_ID, ls_infos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = operator_.batch_update(OB_SYS_TENANT_ID, replicas);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletIterator, ObTenantTabletIterator)
{
  int ret = OB_SUCCESS;
  ObTabletInfo tablet_info;

  int tablet_id_count[3] = {0};
  while (OB_SUCC(iterator_.next(tablet_info))) {
    ASSERT_EQ(OB_SYS_TENANT_ID, tablet_info.get_tenant_id());
    if (2 == tablet_info.get_tablet_id().id()) {
      tablet_id_count[0]++;
    } else if (3 == tablet_info.get_tablet_id().id()) {
      tablet_id_count[1]++;
    } else if (4 == tablet_info.get_tablet_id().id()) {
      tablet_id_count[2]++;
    }
    tablet_info.reset();
  }

  ASSERT_EQ(1, tablet_id_count[0]);
  ASSERT_EQ(1, tablet_id_count[1]);
  ASSERT_EQ(1, tablet_id_count[2]);
  ASSERT_EQ(OB_ITER_END, ret);
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}
