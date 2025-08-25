/**
 * Copyright (c) 2024 OceanBase
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
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#define private public
#include "share/balance/ob_object_balance_weight_operator.h" // ObObjectBalanceWeightOperator

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace schema;
using namespace common;

class TestObObjectBalanceWeight : public ObSimpleClusterTestBase
{
public:
  TestObObjectBalanceWeight() : ObSimpleClusterTestBase("test_object_balance_weight") {}
};

TEST_F(TestObObjectBalanceWeight, test_operator)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  LOG_INFO("new tenant_id", K(tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();


  ObObjectBalanceWeight obj_weight;
  ObArray<ObObjectBalanceWeight> obj_weights;
  ObObjectID mock_table_id = 999999;
  ObObjectID mock_part_id = OB_INVALID_ID;
  ObObjectID mock_subpart_id = OB_INVALID_ID;
  int64_t mock_weight = 100;
  ASSERT_EQ(OB_SUCCESS, obj_weight.init(tenant_id, mock_table_id, mock_part_id, mock_subpart_id, mock_weight));
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::update(sql_proxy, obj_weight));
  obj_weight.reset();
  ASSERT_EQ(OB_SUCCESS, obj_weight.init(tenant_id, mock_table_id + 1, mock_part_id, mock_subpart_id, mock_weight));
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::update(sql_proxy, obj_weight));
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::get_by_tenant(sql_proxy, tenant_id, obj_weights));
  ASSERT_TRUE(2 == obj_weights.count());
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::remove(sql_proxy, obj_weight.get_obj_key()));
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::get_by_tenant(sql_proxy, tenant_id, obj_weights));
  ASSERT_TRUE(1 == obj_weights.count());
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::remove(sql_proxy, obj_weights.at(0).get_obj_key()));

  ObObjectBalanceWeightKey not_exist_key;
  ASSERT_EQ(OB_SUCCESS, not_exist_key.init(tenant_id, OB_INVALID_ID - 1, mock_part_id, mock_subpart_id));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObObjectBalanceWeightOperator::remove(sql_proxy, not_exist_key));

  ObArray<ObObjectBalanceWeightKey> obj_keys;
  const int64_t BATCH_COUNT = 1001;
  for (int64_t i = 0; i < BATCH_COUNT; ++i) {
    obj_weight.reset();
    ASSERT_EQ(OB_SUCCESS, obj_weight.init(tenant_id, mock_table_id + i, mock_part_id + i, mock_subpart_id + i, mock_weight));
    ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::update(sql_proxy, obj_weight));
    ASSERT_EQ(OB_SUCCESS, obj_keys.push_back(obj_weight.get_obj_key()));
  }
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::get_by_tenant(sql_proxy, tenant_id, obj_weights));
  ASSERT_TRUE(BATCH_COUNT == obj_weights.count());
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::batch_remove(sql_proxy, obj_keys));
  ASSERT_EQ(OB_SUCCESS, ObObjectBalanceWeightOperator::get_by_tenant(sql_proxy, tenant_id, obj_weights));
  ASSERT_TRUE(0 == obj_weights.count());
}

} // namespace share
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
