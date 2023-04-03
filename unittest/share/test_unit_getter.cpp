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
#include "lib/stat/ob_session_stat.h"
#include "../share/schema/db_initializer.h"
#include "unit_info_builder.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_unit_getter.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share::host;
namespace share
{
class TestUnitGetter : public ::testing::Test
{
public:
  TestUnitGetter() {}
  virtual ~TestUnitGetter() {}

  virtual void SetUp();
  virtual void TearDown() {}
protected:
  void check_unit(const ObUnit &l, const ObUnit &r);
  void check_config(const ObUnitConfig &l, const ObUnitConfig &r);
  void check_pool(const ObResourcePool &l, const ObResourcePool &r);
  DBInitializer db_initer_;
};

void TestUnitGetter::check_unit(const ObUnit &l, const ObUnit &r)
{
  ASSERT_EQ(l.unit_id_, r.unit_id_);
  ASSERT_EQ(l.resource_pool_id_, r.resource_pool_id_);
  ASSERT_EQ(l.zone_, r.zone_);
  ASSERT_EQ(l.server_, r.server_);
  ASSERT_EQ(l.migrate_from_server_, r.migrate_from_server_);
}

void TestUnitGetter::check_config(const ObUnitConfig &l, const ObUnitConfig &r)
{
  // only check resource
  ASSERT_EQ(l.max_cpu_, r.max_cpu_);
  ASSERT_EQ(l.max_memory_, r.max_memory_);
  ASSERT_EQ(l.max_iops_, r.max_iops_);
  ASSERT_EQ(l.log_disk_size_, r.log_disk_size_);
  ASSERT_EQ(l.max_session_num_, r.max_session_num_);
}

void TestUnitGetter::check_pool(const ObResourcePool &l, const ObResourcePool &r)
{
  ASSERT_EQ(l.resource_pool_id_, r.resource_pool_id_);
  ASSERT_EQ(l.name_, r.name_);
  ASSERT_EQ(l.unit_count_, r.unit_count_);
  ASSERT_EQ(l.unit_config_id_, r.unit_config_id_);
  ASSERT_EQ(l.tenant_id_, r.tenant_id_);
  ASSERT_EQ(l.zone_list_.count(), r.zone_list_.count());
  for (int64_t i = 0; i < l.zone_list_.count(); ++i) {
    ASSERT_EQ(l.zone_list_.at(i), r.zone_list_.at(i));
  }
}

void TestUnitGetter::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());

  const bool only_core_tables = false;
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(only_core_tables));
  ASSERT_EQ(OB_SUCCESS, db_initer_.fill_sys_stat_table());
}

TEST_F(TestUnitGetter, basic)
{
  UnitInfoBuilder builder(db_initer_.get_sql_proxy());
  ObArray<ObZone> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back("zone1"));
  builder.add_config(1, 50).add_config(2, 40);
  builder.add_pool(1, 2, 1, zone_list).add_pool(2, 2, 2, zone_list);
  builder.add_unit(1, 1, 1, A).add_unit(2, 1, 2, B)
      .add_unit(3, 2, 1, A).add_unit(4, 2, 1, B);
  ASSERT_EQ(OB_SUCCESS, builder.write_table());
  ObIArray<ObUnitConfig> &configs = builder.get_configs();

  ObUnitInfoGetter getter;
  ASSERT_EQ(OB_SUCCESS, getter.init(db_initer_.get_sql_proxy()));

  ObArray<ObUnitInfoGetter::ObTenantConfig> tenant_configs;
  int ret = getter.get_server_tenant_configs(A, tenant_configs);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, tenant_configs.count());
  ObUnitConfig sum_config;
  sum_config.reset();
  sum_config += configs.at(0);
  sum_config += configs.at(1);
  ASSERT_EQ(tenant_configs.at(0).tenant_id_, OB_SYS_TENANT_ID);
  check_config(tenant_configs.at(0).config_, sum_config);
  tenant_configs.reuse();
  ASSERT_EQ(OB_SUCCESS, getter.get_server_tenant_configs(E, tenant_configs));

  ObArray<ObUnitInfoGetter::ObServerConfig> server_configs;
  ASSERT_EQ(OB_SUCCESS, getter.get_tenant_server_configs(OB_SYS_TENANT_ID, server_configs));
  ASSERT_EQ(3, server_configs.count()); //include pre_server E
  ASSERT_EQ(A, server_configs.at(0).server_);
  check_config(configs.at(0) + configs.at(1), server_configs.at(0).config_);
  ASSERT_EQ(E, server_configs.at(1).server_);
  check_config(configs.at(0) + configs.at(1) + configs.at(0) + configs.at(1), server_configs.at(1).config_);
  ASSERT_EQ(B, server_configs.at(2).server_);
  check_config(configs.at(0) + configs.at(1), server_configs.at(2).config_);
}

TEST_F(TestUnitGetter, check_tenant_small)
{
  UnitInfoBuilder builder(db_initer_.get_sql_proxy());
  ObArray<ObZone> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back("zone1"));
  builder.add_config(1, 50);
  builder.add_pool(1, 1, 1, zone_list, 1)     // invalid pool, change unit_count zero with sql then
      .add_pool(2, 2, 1, zone_list, 2)        // pool with unit_count 2
      .add_pool(3, 1, 1, zone_list, 3)        // big tenant with two pools
      .add_pool(4, 1, 1, zone_list, 3)
      .add_pool(5, 1, 1, zone_list, 4);       // small tenant
  ASSERT_EQ(OB_SUCCESS, builder.write_table());

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, db_initer_.get_sql_proxy().write(
      "update __all_resource_pool set unit_count = 0 where resource_pool_id = 1", affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObUnitInfoGetter getter;
  bool small_tenant = true;
  ASSERT_EQ(OB_NOT_INIT, getter.check_tenant_small(1, small_tenant));
  ASSERT_EQ(OB_SUCCESS, getter.init(db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, getter.check_tenant_small(OB_INVALID_ID, small_tenant));
  ASSERT_EQ(OB_ERR_UNEXPECTED, getter.check_tenant_small(1, small_tenant));
  ASSERT_EQ(OB_TENANT_NOT_EXIST, getter.check_tenant_small(10, small_tenant));
  ASSERT_EQ(OB_SUCCESS, getter.check_tenant_small(2, small_tenant));
  ASSERT_FALSE(small_tenant);
  ASSERT_EQ(OB_SUCCESS, getter.check_tenant_small(3, small_tenant));
  ASSERT_FALSE(small_tenant);
  ASSERT_EQ(OB_SUCCESS, getter.check_tenant_small(4, small_tenant));
  ASSERT_TRUE(small_tenant);
}

TEST_F(TestUnitGetter, unit_stat)
{
  UnitInfoBuilder builder(db_initer_.get_sql_proxy());
  ObArray<ObZone> zone_list;
  ObZone zone("zone1");
  ObAddr empty_server;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back("zone1"));
  builder.add_config(1, 50).add_config(2, 40);
  builder.add_pool(1, 2, 1, zone_list, 1).add_pool(2, 2, 2, zone_list, 2)
      .add_pool(3, 1, 1, zone_list, 3);
  builder.add_unit(1, 1, 1, A, zone, empty_server).add_unit(2, 1, 2, B)
      .add_unit(3, 2, 1, A, zone, E).add_unit(4, 3, 1, D, zone, A);
  ASSERT_EQ(OB_SUCCESS, builder.write_table());

  ObUnitInfoGetter getter;
  ASSERT_EQ(OB_SUCCESS, getter.init(db_initer_.get_sql_proxy()));

  ObArray<ObUnitInfoGetter::ObTenantConfig> tenant_configs;
  int ret = getter.get_server_tenant_configs(A, tenant_configs);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, tenant_configs.count());
  for (int64_t i = 0; i < tenant_configs.count(); i++) {
    if (tenant_configs.at(i).tenant_id_ == 1) {
      ASSERT_EQ(ObUnitInfoGetter::UNIT_NORMAL, tenant_configs.at(i).unit_stat_);
    } else if (tenant_configs.at(i).tenant_id_ == 2) {
      ASSERT_EQ(ObUnitInfoGetter::UNIT_MIGRATE_IN, tenant_configs.at(i).unit_stat_);
    } else if (tenant_configs.at(i).tenant_id_ == 3) {
      ASSERT_EQ(ObUnitInfoGetter::UNIT_MIGRATE_OUT, tenant_configs.at(i).unit_stat_);
    } else {
      ASSERT_EQ(OB_SUCCESS, 1);
    }
  }
}

}//end namespace share
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}
