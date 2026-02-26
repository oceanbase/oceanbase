// owner: yangyi.yyy
// owner group: storage_ha

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
#include <gmock/gmock.h>
#define private public
#include "env/ob_simple_cluster_test_base.h"
#include "share/ob_tablet_reorganize_history_table_operator.h"
#include "share/backup/ob_backup_tablet_reorganize_helper.h"

namespace oceanbase
{
namespace unittest
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace rootserver;
using namespace common;
using namespace share;
using namespace backup;

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))

#define SWITCH_TENANT(tenant_id) \
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard); \
  EXPECT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

class TestBackupTabletReorganizeHelper : public unittest::ObSimpleClusterTestBase
{
public:
  TestBackupTabletReorganizeHelper()
    : unittest::ObSimpleClusterTestBase("test_backup_tablet_reorganize_helper") {}

private:
  void get_one_tablet_reorganize_record_(
       const int64_t src_tablet_id,
       const int64_t dest_tablet_id,
       ObTabletReorganizeRecord &record);
};

void TestBackupTabletReorganizeHelper::get_one_tablet_reorganize_record_(
     const int64_t src_tablet_id, const int64_t dest_tablet_id,
     ObTabletReorganizeRecord &record)
{
  record = ObTabletReorganizeRecord(1002,
                                    ObLSID(1001),
                                    ObTabletID(src_tablet_id),
                                    ObTabletID(dest_tablet_id),
                                    ObTabletReorganizeType::SPLIT,
                                    0,
                                    0);
}

TEST_F(TestBackupTabletReorganizeHelper, test_reorganize_info)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = 1002;

  OK(create_tenant());
  OK(get_curr_simple_server().init_sql_proxy2("tt1", "oceanbase"));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  SWITCH_TENANT(tenant_id);

  ObTabletReorganizeRecord record;
  get_one_tablet_reorganize_record_(1, 2, record);
  OK(ObTabletReorganizeHistoryTableOperator::insert(sql_proxy, record));

  get_one_tablet_reorganize_record_(1, 3, record);
  OK(ObTabletReorganizeHistoryTableOperator::insert(sql_proxy, record));

  get_one_tablet_reorganize_record_(2, 4, record);
  OK(ObTabletReorganizeHistoryTableOperator::insert(sql_proxy, record));

  get_one_tablet_reorganize_record_(2, 5, record);
  OK(ObTabletReorganizeHistoryTableOperator::insert(sql_proxy, record));

  get_one_tablet_reorganize_record_(3, 6, record);
  OK(ObTabletReorganizeHistoryTableOperator::insert(sql_proxy, record));

  get_one_tablet_reorganize_record_(3, 7, record);
  OK(ObTabletReorganizeHistoryTableOperator::insert(sql_proxy, record));

  get_one_tablet_reorganize_record_(3, 8, record);
  OK(ObTabletReorganizeHistoryTableOperator::insert(sql_proxy, record));

  bool reorganized = false;
  share::ObLSID reorganized_ls;
  OK(ObTabletReorganizeHistoryTableOperator::check_tablet_has_reorganized(sql_proxy, tenant_id, ObTabletID(1), reorganized_ls, reorganized));
  ASSERT_TRUE(reorganized);
  ASSERT_EQ(reorganized_ls.id(), 1001);

  ObArray<ObTabletID> tablet_ids;
  OK(ObBackupTabletReorganizeHelper::get_leaf_children(sql_proxy, tenant_id, ObTabletID(1), ObLSID(1001), tablet_ids));

  LOG_INFO("get leaf children", K(tablet_ids));
}

} // namespace
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
