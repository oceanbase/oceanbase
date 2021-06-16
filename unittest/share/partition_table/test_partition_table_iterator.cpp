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

#define USING_LOG_PREFIX SHARE_PT

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/stat/ob_session_stat.h"
#include "share/config/ob_server_config.h"
#include "share/schema/db_initializer.h"
// TODO : rename .cpp to .h
#include "share/schema/ob_schema_test_utils.cpp"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_iter.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "fake_part_property_getter.h"
#include "../mock_ob_rs_mgr.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "../../rootserver/fake_rs_list_change_cb.h"
#include "rootserver/ob_root_service.h"
namespace oceanbase {
namespace share {

using namespace common;
using namespace schema;
using namespace host;
using namespace obrpc;
using namespace std;

static uint64_t& TEN = FakePartPropertyGetter::TEN();

using testing::_;
using ::testing::Invoke;
using ::testing::Return;
ObServerConfig& config = ObServerConfig::get_instance();
class TestPartitionTableIterator : public ::testing::Test {
public:
  TestPartitionTableIterator() : operator_(prop_getter_)
  {}

  int gen_tenant_schema(const uint64_t tenant_id, ObTenantSchema& tenant_schema);
  virtual void SetUp();
  virtual void TearDown();
  virtual int gen_table_schema(const uint64_t tenant_id, const uint64_t pure_id, const uint64_t pure_db_id,
      const uint64_t pure_tg_id, ObTableSchema& table_schema);
  void gen_schema_lack_partition_level_two(const uint64_t tenant_id, const uint64_t pure_id, const uint64_t pure_db_id,
      const uint64_t pure_tg_id, const int64_t lack_position, bool create_tanent, ObMetaTableMode mode);

  void gen_schema_lack_partition_level_one(const uint64_t tenant_id, const uint64_t pure_id, const uint64_t pure_db_id,
      const uint64_t pure_tg_id, const int64_t lack_position, bool create_tenant, ObMetaTableMode mode);

  void gen_partition_table_level_one(const uint64_t tenant_id, const uint64_t pure_id, const vector<int64_t>& part_ids,
      const vector<int64_t>& meta_part_projector);
  void gen_table_schema_with_specified_part_id(const uint64_t tenant_id, const uint64_t pure_id,
      const uint64_t pure_db_id, const uint64_t pure_tg_id_, const vector<int64_t>& part_ids);
  int gen_tenant_space_schema(uint64_t tenant_id);
  int release_tenant_space_schema(uint64_t tenant_id);

protected:
  DBInitializer db_initer_;
  FakePartPropertyGetter prop_getter_;
  MockObCommonRpcProxy rpc_proxy_;
  MockObRsMgr rs_mgr_;
  ObPartitionTableOperator operator_;
  // FakeSchemaService schema_service_;
  ObMultiVersionSchemaService multi_schema_service_;
  FakeRsListChangeCb cb_;
  FakeMergeErrorCb merge_error_cb_;
};

int TestPartitionTableIterator::gen_table_schema(const uint64_t tenant_id, const uint64_t pure_id,
    const uint64_t pure_db_id, const uint64_t pure_tg_id, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.reset();
  ObInnerTableSchema::all_core_table_schema(table_schema);
  char table_name[64];
  if (snprintf(table_name, 64, "table_%lu", combine_id(tenant_id, pure_id)) >= 64) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_SCHEMA_LOG(WARN, "buf not enough", K(ret));
  } else {
    table_schema.set_table_name(table_name);
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_table_id(combine_id(tenant_id, pure_id));
    table_schema.set_database_id(combine_id(tenant_id, pure_db_id));
    table_schema.set_tablegroup_id(combine_id(tenant_id, pure_tg_id));
  }
  return ret;
}
int TestPartitionTableIterator::gen_tenant_schema(const uint64_t tenant_id, ObTenantSchema& tenant_schema)
{
  int ret = OB_SUCCESS;
  char tenant_name[64];
  if (snprintf(tenant_name, 64, "tenant_%lu", tenant_id) >= 64) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    tenant_schema.reset();
    tenant_schema.set_tenant_id(tenant_id);
    tenant_schema.set_tenant_name(tenant_name);
    tenant_schema.set_comment("this is a test tenant");
    tenant_schema.add_zone("zone");
    tenant_schema.set_primary_zone("zone");
    tenant_schema.set_locality("");
  }
  return ret;
}

void TestPartitionTableIterator::gen_table_schema_with_specified_part_id(const uint64_t tenant_id,
    const uint64_t pure_id, const uint64_t pure_db_id, const uint64_t pure_tg_id, const vector<int64_t>& part_ids)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  if (OB_FAIL(gen_table_schema(tenant_id, pure_id, pure_db_id, pure_tg_id, table_schema))) {
    LOG_WARN("fail to gen table schema", K(ret), K(tenant_id), K(pure_id));
  }

  for (int i = 0; i < part_ids.size(); ++i) {
    ObPartition partition;
    int64_t value = i + 10;
    ObObj key(value);
    ObRowkey rowkey(&key, 1);
    partition.set_high_bound_val(rowkey);
    char part_name[50];
    memset(part_name, '\0', 50);
    snprintf(part_name, 50, "range%d", i);
    ObString name = ObString::make_string(part_name);
    partition.set_part_id(part_ids[i]);
    partition.set_part_name(name);
    table_schema.add_partition(partition);
  }
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_part_level(PARTITION_LEVEL_ONE);
  table_schema.get_part_option().set_part_num(part_ids.size());
  table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE);
  table_schema.get_part_option().set_part_expr(ObString::make_string("table_id mod 111"));

  CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  // ObMySQLTransaction trans;
  // ret = trans.start(&db_initer_.get_sql_proxy());
  // ret = multi_schema_service_.get_schema_service()->get_table_sql_service().create_table(table_schema, trans);
  // const bool commit = true;
  // ret = trans.end(commit);
  multi_schema_service_.refresh_and_add_schema();
}

void TestPartitionTableIterator::gen_schema_lack_partition_level_two(const uint64_t tenant_id, const uint64_t pure_id,
    const uint64_t pure_db_id, const uint64_t pure_tg_id, const int64_t lack_position, bool create_tenant,
    ObMetaTableMode mode)
{
  int ret = OB_SUCCESS;
  ObTenantSchema tenant_schema;
  ObTableSchema table_schema;
  if (create_tenant) {
    if (OB_FAIL(gen_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("fail to gen tenant schema", K(ret), K(tenant_id));
    }

    CREATE_TENANT(ret, tenant_schema);
    if (mode >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(tenant_schema.get_tenant_id()));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(gen_table_schema(tenant_id, pure_id, pure_db_id, pure_tg_id, table_schema))) {
    LOG_WARN("fail to gen table schema", K(ret), K(tenant_id), K(pure_id));
  }

  table_schema.set_table_type(USER_TABLE);
  table_schema.set_part_level(PARTITION_LEVEL_TWO);
  table_schema.get_part_option().set_part_num(2);
  table_schema.get_sub_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE);
  table_schema.get_sub_part_option().set_part_expr(ObString::make_string("table_id mod 111"));
  table_schema.get_sub_part_option().set_part_num(3);
  ObSubPartition subpartition;
  subpartition.set_part_id(0);
  subpartition.set_sub_part_id(0);
  ObString name1 = ObString::make_string("range1");
  subpartition.set_part_name(name1);
  int64_t value = 5;
  ObObj key1(value);
  ObRowkey rowkey1(&key1, 1);
  subpartition.set_high_bound_val(rowkey1);
  table_schema.add_partition(subpartition);
  subpartition.reset();
  ObString name2 = ObString::make_string("range2");
  subpartition.set_part_id(0);
  subpartition.set_sub_part_id(1);
  subpartition.set_part_name(name2);
  value = 15;
  ObObj key2(value);
  ObRowkey rowkey2(&key2, 1);
  subpartition.set_high_bound_val(rowkey2);
  table_schema.add_partition(subpartition);
  subpartition.reset();
  ObString name3 = ObString::make_string("range2");
  subpartition.set_part_id(0);
  subpartition.set_sub_part_id(2);
  subpartition.set_part_name(name3);
  value = 150;
  ObObj key3(value);
  ObRowkey rowkey3(&key3, 1);
  subpartition.set_high_bound_val(rowkey3);
  table_schema.add_partition(subpartition);

  CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  prop_getter_.clear();
  // generate two level partitions 2 * 3
  // 13 patterns
  // nothing                                                   --0
  //      <0,1>,<0,2>,      <1,0>,<1,1>,<1,2> //lack of front   --1
  //<0,0>,      <0,2>,      <1,0>,<1,1>,<1,2> //lack of middle ---2
  //<0,0>,<0,1>,     ,      <1,0>,<1,1>,<1,2> //lack of middle ---3
  //<0,0>,<0,1>,<0,2>,<0,3>,<1,0>,<1,1>,<1,2> //more in middle ---4
  //<0,0>,<0,1>,<0,2>,      <1,0>,<1,1>,<1,2> //normal         ---5
  //<0,0>,<0,1>,<0,2>,<0,3>,      <1,1>,<1,2> //more in middle ---6
  //<0,0>,<0,1>,<0,2>,            <1,1>,<1,2> //lack of front  ---7
  //<0,0>,<0,1>,<0,2>,                                ---8
  //<0,0>,<0,1>,                  <1,1>,<1,2> //lack of middle ---9
  //<0,0>,<0,1>,<0,2>,      <1,0>             //the back is missing ----10
  //<0,0>,<0,1>,<0,2>,      <1,0>,<1,1>,<1,2><1,3>,<1,4> more in back  ---11
  //                        <1,0>,<1,1>,<1,2>                  ----12
  //<0,0>,<0,1>,<0,2>,<0,3>,  <0,5>    <1,1>,<1,2> //more in middle----13
  // for (int64_t i = 0; i < 13; i++) {
  int64_t i = lack_position;
  int64_t phy_part_id = 0;
  if (i == 0) {
    // nothing to do
  } else {
    if (i != 1 && i != 12) {
      phy_part_id = generate_phy_part_id(0, 0, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
    if (i != 2 && i != 12) {
      phy_part_id = generate_phy_part_id(0, 1, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
    if (i != 3 && i != 9 && i != 12) {
      phy_part_id = generate_phy_part_id(0, 2, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
    if (i == 4 || i == 6 || i == 13) {
      phy_part_id = generate_phy_part_id(0, 3, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
    if (i == 13) {
      phy_part_id = generate_phy_part_id(0, 5, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
    if (i != 6 && i != 7 && i != 8 && i != 9 && i != 13) {
      phy_part_id = generate_phy_part_id(1, 0, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
    if (i != 8 && i != 10) {
      phy_part_id = generate_phy_part_id(1, 1, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
      phy_part_id = generate_phy_part_id(1, 2, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
    if (i == 11) {
      phy_part_id = generate_phy_part_id(1, 3, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
      phy_part_id = generate_phy_part_id(1, 4, PARTITION_LEVEL_TWO);
      prop_getter_.add(combine_id(tenant_id, pure_id), phy_part_id, A, LEADER)
          .add(combine_id(tenant_id, pure_id), phy_part_id, B, FOLLOWER);
    }
  }
  //}
  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    GCONF.self_addr_ = prop_getter_.get_replicas().at(i).server_;
    operator_.update(prop_getter_.get_replicas().at(i));
  }
  multi_schema_service_.refresh_and_add_schema();
}

void TestPartitionTableIterator::gen_partition_table_level_one(const uint64_t tenant_id, const uint64_t pure_id,
    const vector<int64_t>& part_ids, const vector<int64_t>& meta_part_projector)
{
  prop_getter_.clear();
  for (int i = 0; i < meta_part_projector.size(); ++i) {
    int64_t part_id = meta_part_projector[i];
    if (meta_part_projector[i] < part_ids.size()) {
      part_id = part_ids[meta_part_projector[i]];
    }
    prop_getter_.add(combine_id(tenant_id, pure_id), part_id, A, LEADER)
        .add(combine_id(tenant_id, pure_id), part_id, B, FOLLOWER);
  }
  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    operator_.update(prop_getter_.get_replicas().at(i));
  }
}

void TestPartitionTableIterator::gen_schema_lack_partition_level_one(const uint64_t tenant_id, const uint64_t pure_id,
    const uint64_t pure_db_id, const uint64_t pure_tg_id, const int64_t lack_position, bool create_tenant,
    ObMetaTableMode mode)
{
  int ret = OB_SUCCESS;
  ObTenantSchema tenant_schema;
  ObTableSchema table_schema;
  if (create_tenant) {
    if (OB_FAIL(gen_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("fail to gen tenant schema", K(ret), K(tenant_id));
    }

    CREATE_TENANT(ret, tenant_schema);
    if (mode >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(tenant_schema.get_tenant_id()));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(gen_table_schema(tenant_id, pure_id, pure_db_id, pure_tg_id, table_schema))) {
    LOG_WARN("fail to gen table schema", K(ret), K(tenant_id), K(pure_id));
  }

  // five replicas, five pattern
  // 1234 \ 0234 \ 0123 \ 01234 \ 0123456 \ empty\012367
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_part_level(PARTITION_LEVEL_ONE);
  table_schema.get_part_option().set_part_num(5);
  CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  prop_getter_.clear();
  if (lack_position != 0 && lack_position != 5) {
    prop_getter_.add(combine_id(tenant_id, pure_id), 0, A, LEADER).add(combine_id(tenant_id, pure_id), 0, B, FOLLOWER);
  }
  if (lack_position != 1 && lack_position != 5) {
    prop_getter_.add(combine_id(tenant_id, pure_id), 1, A, LEADER).add(combine_id(tenant_id, pure_id), 1, B, FOLLOWER);
  }
  if (lack_position != 5) {
    prop_getter_.add(combine_id(tenant_id, pure_id), 2, A, LEADER).add(combine_id(tenant_id, pure_id), 2, B, FOLLOWER);
    prop_getter_.add(combine_id(tenant_id, pure_id), 3, A, LEADER).add(combine_id(tenant_id, pure_id), 3, B, FOLLOWER);
  }
  if (lack_position != 2 && lack_position != 5 && lack_position != 6) {
    prop_getter_.add(combine_id(tenant_id, pure_id), 4, A, LEADER).add(combine_id(tenant_id, pure_id), 4, B, FOLLOWER);
  }
  if (lack_position == 4) {
    prop_getter_.add(combine_id(tenant_id, pure_id), 5, A, LEADER).add(combine_id(tenant_id, pure_id), 5, B, FOLLOWER);
    prop_getter_.add(combine_id(tenant_id, pure_id), 6, A, LEADER).add(combine_id(tenant_id, pure_id), 6, B, FOLLOWER);
  }
  if (lack_position == 6) {
    prop_getter_.add(combine_id(tenant_id, pure_id), 6, A, LEADER).add(combine_id(tenant_id, pure_id), 6, B, FOLLOWER);
    prop_getter_.add(combine_id(tenant_id, pure_id), 7, A, LEADER).add(combine_id(tenant_id, pure_id), 7, B, FOLLOWER);
  }
  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    GCONF.self_addr_ = prop_getter_.get_replicas().at(i).server_;
    operator_.update(prop_getter_.get_replicas().at(i));
  }
  multi_schema_service_.refresh_and_add_schema();
}

int TestPartitionTableIterator::gen_tenant_space_schema(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int idx = 0;
  std::string db = db_initer_.get_db_name();
  common::ObMySQLProxy& sql_proxy = db_initer_.get_sql_proxy();
  ObSqlString sql;
  int64_t affect_rows = 0;
  LOG_INFO("create db", K(db.c_str()), K(tenant_id));
  if (OB_FAIL(sql.assign_fmt("drop database if exists %s_%lu", db.c_str(), tenant_id))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affect_rows))) {
    LOG_WARN("execute create database sql failed", K(ret), K(sql));
  } else if (OB_FAIL(sql.assign_fmt("create database %s_%lu", db.c_str(), tenant_id))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affect_rows))) {
    LOG_WARN("execute create database sql failed", K(ret), K(sql));
  } else {
    const schema_create_func* creator_ptr_array[] = {
        share::core_table_schema_creators, share::sys_table_schema_creators};
    ObTableSchema tables[ARRAYSIZEOF(core_table_schema_creators) + ARRAYSIZEOF(sys_table_schema_creators)];
    // build system table schema to %tables
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < ARRAYSIZEOF(creator_ptr_array); i++) {
      for (const schema_create_func* creator_ptr = creator_ptr_array[i];
           common::OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        if (OB_FAIL((*creator_ptr)(tables[idx++]))) {
          LOG_WARN("create table schema fialed", K(ret));
          ret = common::OB_SCHEMA_ERROR;
        }
      }
    }

    ObTableSchema table_schema;
    char csql[common::OB_MAX_SQL_LENGTH];
    memset(csql, 0, sizeof(csql));
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tenant_space_tables); i++) {
      if (!is_virtual_table(tenant_space_tables[i])) {
        uint64_t tid = tenant_space_tables[i];
        // found table schema
        for (idx = 0; idx < ARRAYSIZEOF(tables); ++idx) {
          if (extract_pure_id(tables[idx].get_table_id()) == tid) {
            LOG_INFO("", K(tid));
            if (OB_FAIL(table_schema.assign(tables[idx]))) {
              LOG_WARN("fail to assign schema", K(ret));
            } else {
              ObSchemaTestUtils::table_set_tenant(table_schema, tenant_id);
              CREATE_USER_TABLE_SCHEMA(ret, table_schema);
            }
            if (OB_FAIL(ret)) {
            } else if (tables[idx].is_view_table()) {
              // skip
            } else if (OB_FAIL(rootserver::ObSchema2DDLSql::convert(table_schema, csql, sizeof(csql)))) {
              LOG_WARN("convert table schema to create table sql failed", K(ret));
            } else if (OB_FAIL(sql_proxy.write(tenant_id, csql, affect_rows))) {
              ret = OB_SUCCESS;
              LOG_WARN("execute sql failed", K(ret), K(csql));
            }
            break;
          }
        }
      }
    }
  }
  return ret;
}

int TestPartitionTableIterator::release_tenant_space_schema(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  std::string db = db_initer_.get_db_name();
  common::ObMySQLProxy& sql_proxy = db_initer_.get_sql_proxy();
  ObSqlString sql;
  int64_t affect_rows = 0;
  if (OB_FAIL(sql.assign_fmt("drop database if exists %s_%lu", db.c_str(), tenant_id))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affect_rows))) {
    LOG_WARN("execute create database sql failed", K(ret), K(sql));
  }
  return ret;
}

void TestPartitionTableIterator::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestPartitionTableIterator::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObKVGlobalCache::get_instance().init();

  GCONF.meta_table_read_write_mode = ObMetaTableMode::METATABLE_MODE_SYS_ONLY;
  GCONF.min_observer_version.set_value("2.0.0");

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = operator_.init(db_initer_.get_sql_proxy(), NULL);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, operator_.set_callback_for_rs(cb_, merge_error_cb_));
  // init schema service
  // ASSERT_EQ(OB_SUCCESS, schema_service_.init());
  ret = multi_schema_service_.init(&db_initer_.get_sql_proxy(),
      &db_initer_.get_config(),
      OB_MAX_VERSION_COUNT,
      OB_MAX_VERSION_COUNT_FOR_MERGE,
      false);
  ASSERT_EQ(OB_SUCCESS, ret);
  TEN = 1;
  // create sys tenant
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(OB_SYS_TENANT_ID);
  tenant_schema.set_tenant_name(OB_SYS_TENANT_NAME);
  tenant_schema.set_locality("");
  CREATE_TENANT(ret, tenant_schema);
  ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(TEN));
  ObTableSchema table_schema;
  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::sys_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::sys_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::core_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::core_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }
  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::virtual_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::virtual_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::sys_view_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::sys_view_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::information_schema_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::information_schema_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::mysql_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::mysql_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }
  TEN = 2;
  ASSERT_EQ(OB_SUCCESS, gen_tenant_schema(2, tenant_schema));
  CREATE_TENANT(ret, tenant_schema);
  ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(TEN));
  ASSERT_EQ(OB_SUCCESS, gen_table_schema(TEN, 50001, 1, 1, table_schema));
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_part_level(PARTITION_LEVEL_ONE);
  table_schema.get_part_option().set_part_num(2);
  CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  ASSERT_EQ(OB_SUCCESS, gen_table_schema(TEN, 50002, 1, 1, table_schema));
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  CREATE_USER_TABLE_SCHEMA(ret, table_schema);

  // init partition table
  TEN = 1;
  prop_getter_.clear().add(combine_id(TEN, 1), 0, A, LEADER);
  GCONF.self_addr_ = prop_getter_.get_replicas().at(0).server_;
  ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(0)));

  prop_getter_.clear().add(combine_id(TEN, 2), 0, A, LEADER);
  GCONF.self_addr_ = prop_getter_.get_replicas().at(0).server_;
  ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(0)));

  TEN = 2;
  prop_getter_.clear()
      .add(combine_id(TEN, 50001), 0, A, LEADER)
      .add(combine_id(TEN, 50001), 0, B, FOLLOWER)
      .add(combine_id(TEN, 50001), 1, A, LEADER)
      .add(combine_id(TEN, 50001), 1, B, FOLLOWER);
  for (int64_t i = 0; i < prop_getter_.get_replicas().count(); i++) {
    GCONF.self_addr_ = prop_getter_.get_replicas().at(i).server_;
    ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
}

TEST_F(TestPartitionTableIterator, table_partition_iterator_v2)
{
  const ObMetaTableMode modes[] = {ObMetaTableMode::METATABLE_MODE_SYS_ONLY,
      ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE,
      ObMetaTableMode::METATABLE_MODE_TENANT_ONLY};
  for (int idx = 0; idx < ARRAYSIZEOF(modes); idx++) {
    LOG_INFO("case table_partition_iterator_v2: use mode ", K(modes[idx]));
    GCONF.meta_table_read_write_mode = modes[idx];

    ObTablePartitionIterator iter;
    GCONF.partition_table_scan_batch_count = 4;
    ObSchemaGetterGuard schema_guard;
    EXPECT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard));
    ObPartitionInfo partition;
    // not init
    ASSERT_NE(OB_SUCCESS, iter.next(partition));
    ASSERT_NE(OB_SUCCESS, iter.init(OB_INVALID_ID, schema_guard, operator_));

    if (ObMetaTableMode::METATABLE_MODE_SYS_ONLY == modes[idx]) {
      // during setup, tenant 1,2 are created according to ObMetaTableRWMode::TENANT_RW
      // only verified in this mode
      ASSERT_EQ(OB_SUCCESS, iter.init(combine_id(2, 50001), schema_guard, operator_));
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(2, partition.replica_count());
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(2, partition.replica_count());

      ASSERT_EQ(OB_ITER_END, iter.next(partition));
      ASSERT_EQ(OB_ITER_END, iter.next(partition));

      // init again
      ASSERT_EQ(OB_SUCCESS, iter.init(combine_id(2, 50001), schema_guard, operator_));
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(2, partition.replica_count());
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(2, partition.replica_count());

      ASSERT_EQ(OB_ITER_END, iter.next(partition));
    }

    LOG_INFO("table_iterator", K(iter));
    // test exception, there is a hole in the partition table
    TEN = 20005 + idx * 10000;
    uint64_t table_id = 50005;
    // five replcias, 6 error pattern
    // 1234 \ 0234 \ 0123 \  01234 \ 0123456 \ empty \012367
    for (int64_t i = 0; i < 7; i++) {
      LOG_INFO("round", K(i));
      ObSchemaGetterGuard tmp_schema_guard;
      gen_schema_lack_partition_level_one(TEN, table_id, 1, 1, i, true, modes[idx]);
      gen_schema_lack_partition_level_one(TEN, table_id + 1, 1, 1, i, false, modes[idx]);
      EXPECT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(tmp_schema_guard));
      ASSERT_EQ(OB_SUCCESS, iter.init(combine_id(TEN, table_id), tmp_schema_guard, operator_));
      partition.reuse();
      // 1
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(0, partition.get_partition_id());
      if (0 == i || 5 == i) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      partition.reuse();
      // 2
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(1, partition.get_partition_id());
      if (1 == i || 5 == i) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 3
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(2, partition.get_partition_id());
      if (i == 5) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        if (partition.replica_count() != 2)
          OB_ASSERT(0);
        ASSERT_EQ(2, partition.replica_count());
      }
      // 4
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(3, partition.get_partition_id());
      if (i == 5) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 5
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(4, partition.get_partition_id());
      if (2 == i || 5 == i || 6 == i) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      ASSERT_EQ(OB_ITER_END, iter.next(partition));
      table_id += 2;
      TEN++;
    }

    // generate two level partitions 2 * 3
    // 13 patterns
    // nothing                                                   --0
    //      <0,1>,<0,2>,      <1,0>,<1,1>,<1,2> //lack of front   --1
    //<0,0>,      <0,2>,      <1,0>,<1,1>,<1,2> //lack of middle ---2
    //<0,0>,<0,1>,     ,      <1,0>,<1,1>,<1,2> //lack of middle ---3
    //<0,0>,<0,1>,<0,2>,<0,3>,<1,0>,<1,1>,<1,2> //more in middle ---4
    //<0,0>,<0,1>,<0,2>,      <1,0>,<1,1>,<1,2> //normal         ---5
    //<0,0>,<0,1>,<0,2>,<0,3>,      <1,1>,<1,2> //more in middle ---6
    //<0,0>,<0,1>,<0,2>,            <1,1>,<1,2> //lack of front  ---7
    //<0,0>,<0,1>,<0,2>,                                ---8
    //<0,0>,<0,1>,                  <1,1>,<1,2> //lack of middle ---9
    //<0,0>,<0,1>,<0,2>,      <1,0>             //the back is missing ----10
    //<0,0>,<0,1>,<0,2>,      <1,0>,<1,1>,<1,2><1,3>,<1,4> more in back  ---11
    //                        <1,0>,<1,1>,<1,2>                  ----12
    //<0,0>,<0,1>,<0,2>,<0,3>,  <0,5>    <1,1>,<1,2> //more in middle----13

    for (int64_t i = 0; i < 14; i++) {
      ObSchemaGetterGuard tmp_schema_guard;
      gen_schema_lack_partition_level_two(TEN, table_id, 1, 1, i, true, modes[idx]);
      gen_schema_lack_partition_level_two(TEN, table_id + 1, 1, 1, i, false, modes[idx]);
      EXPECT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(tmp_schema_guard));
      ASSERT_EQ(OB_SUCCESS, iter.init(combine_id(TEN, table_id), tmp_schema_guard, operator_));
      // 1
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(generate_phy_part_id(0, 0, PARTITION_LEVEL_TWO), partition.get_partition_id());
      if (i == 0 || i == 1 || i == 12) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 2
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(generate_phy_part_id(0, 1, PARTITION_LEVEL_TWO), partition.get_partition_id());
      if (i == 2 || i == 12 || i == 0) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 3
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(generate_phy_part_id(0, 2, PARTITION_LEVEL_TWO), partition.get_partition_id());
      if (i == 3 || i == 9 || i == 12 || i == 0) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 4
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(generate_phy_part_id(1, 0, PARTITION_LEVEL_TWO), partition.get_partition_id());
      if (i == 0 || i == 6 || i == 7 || i == 8 || i == 9 || i == 13) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 5
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(generate_phy_part_id(1, 1, PARTITION_LEVEL_TWO), partition.get_partition_id());
      if (i == 0 || i == 8 || i == 10) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 6
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(generate_phy_part_id(1, 2, PARTITION_LEVEL_TWO), partition.get_partition_id());
      if (i == 0 || i == 8 || i == 10) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      ASSERT_EQ(OB_ITER_END, iter.next(partition));
      table_id += 2;
      TEN++;
    }

    int ret = OB_SUCCESS;
    for (uint64_t i = 20005 + idx * 10000; i < TEN; i++) {
      if (modes[idx] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
        ASSERT_EQ(OB_SUCCESS, release_tenant_space_schema(i));
      }
      DROP_TENANT(ret, i);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
}

TEST_F(TestPartitionTableIterator, tenant_partition_iterator_for_non_continuous_partition_id)
{
  const ObMetaTableMode modes[] = {ObMetaTableMode::METATABLE_MODE_SYS_ONLY,
      ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE,
      ObMetaTableMode::METATABLE_MODE_TENANT_ONLY};
  for (int idx = 0; idx < ARRAYSIZEOF(modes); idx++) {
    LOG_INFO("case tenant_partition_iterator_for_non_continuous_partition_id: use mode ", K(modes[idx]));
    GCONF.meta_table_read_write_mode = modes[idx];

    int ret = OB_SUCCESS;
    int64_t tenant_id = 1001 + idx;
    uint64_t table_id = 50005;
    vector<int64_t> part_ids = {1, 3, 6, 7, 13};
    vector<vector<int64_t>> meta_part_projectors = {{0, 1, 2, 3, 4}};
    ObTenantSchema tenant_schema;
    if (OB_FAIL(gen_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("fail to gen tenant schema", K(ret), K(tenant_id));
    }
    CREATE_TENANT(ret, tenant_schema);
    if (modes[idx] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(tenant_schema.get_tenant_id()));
    }

    gen_table_schema_with_specified_part_id(tenant_id, table_id, 1, 1, part_ids);
    gen_partition_table_level_one(tenant_id, table_id, part_ids, meta_part_projectors[0]);

    ObSchemaGetterGuard tmp_schema_guard;
    EXPECT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(tmp_schema_guard));
    ObTenantPartitionIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, tenant_id, false));
    ObPartitionInfo partition;
    int i = 0;
    while (OB_SUCC(iter.next(partition))) {
      const uint64_t tid = combine_id(tenant_id, table_id);
      if (partition.get_table_id() != tid) {
        continue;
      } else {
        ASSERT_EQ(part_ids[i], partition.get_partition_id());
        ASSERT_EQ(2, partition.get_replicas_v2().count());
        ++i;
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(5, i);
    if (modes[idx] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, release_tenant_space_schema(tenant_schema.get_tenant_id()));
    }
    DROP_TENANT(ret, tenant_id);
  }
}

TEST_F(TestPartitionTableIterator, table_partition_iterator_for_non_continuous_partition_id)
{
  const ObMetaTableMode modes[] = {ObMetaTableMode::METATABLE_MODE_SYS_ONLY,
      ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE,
      ObMetaTableMode::METATABLE_MODE_TENANT_ONLY};
  for (int idx = 0; idx < ARRAYSIZEOF(modes); idx++) {
    LOG_INFO("case table_partition_iterator_for_non_continuous_partition_id: use mode ", K(modes[idx]));
    GCONF.meta_table_read_write_mode = modes[idx];

    int ret = OB_SUCCESS;
    int64_t tenant_id = 2001 + idx;
    uint64_t table_id = 50005;
    vector<int64_t> part_ids = {1, 3, 6, 7, 13};
    vector<vector<int64_t>> meta_part_projectors = {
        {1, 2, 3, 4},
        {0, 2, 3, 4},
        {0, 1, 2, 3},
        {0, 1, 2, 3, 4},
        {0, 1, 2, 3, 4, 14, 15},
        {},
        {0, 1, 2, 3, 15, 16},
    };
    ObTenantSchema tenant_schema;
    if (OB_FAIL(gen_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("fail to gen tenant schema", K(ret), K(tenant_id));
    }
    CREATE_TENANT(ret, tenant_schema);
    if (modes[idx] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(tenant_id));
    }
    // test exception. there is a hole in the partition_table
    // 5 replicas, 6 error pattern
    // 1234 \ 0234 \ 0123 \  01234 \ 0123456 \ empty \ 012367
    for (int64_t i = 0; i < 7; i++) {
      LOG_INFO("round", K(i));
      ObSchemaGetterGuard tmp_schema_guard;
      gen_table_schema_with_specified_part_id(tenant_id, table_id, 1, 1, part_ids);
      gen_partition_table_level_one(tenant_id, table_id, part_ids, meta_part_projectors[i]);

      EXPECT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(tmp_schema_guard));
      ObTablePartitionIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init(combine_id(tenant_id, table_id), tmp_schema_guard, operator_));
      ObPartitionInfo partition;
      // 1
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(part_ids[0], partition.get_partition_id());
      if (0 == i || 5 == i) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 2
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(part_ids[1], partition.get_partition_id());
      if (1 == i || 5 == i) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 3
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(part_ids[2], partition.get_partition_id());
      if (i == 5) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        if (partition.replica_count() != 2)
          OB_ASSERT(0);
        ASSERT_EQ(2, partition.replica_count());
      }
      // 4
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(part_ids[3], partition.get_partition_id());
      if (i == 5) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      // 5
      partition.reuse();
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(part_ids[4], partition.get_partition_id());
      if (2 == i || 5 == i || 6 == i) {
        LOG_INFO("expect error", K(partition.get_partition_id()));
        ASSERT_EQ(0, partition.replica_count());
      } else {
        ASSERT_EQ(2, partition.replica_count());
      }
      ASSERT_EQ(OB_ITER_END, iter.next(partition));
      table_id += 2;
    }
    if (modes[idx] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, release_tenant_space_schema(tenant_id));
    }
    DROP_TENANT(ret, tenant_id);
  }
}

TEST_F(TestPartitionTableIterator, all_table_partition)
{
  LOG_INFO("case TestPartitionTableIterator.all_table_partition");
  GCONF.meta_table_read_write_mode = ObMetaTableMode::METATABLE_MODE_TENANT_ONLY;
  ObPartitionInfo partition;
  {
    ObPartitionTableIterator iter;
    LOG_INFO("partition table iterator", K(iter));
    // not init
    ASSERT_NE(OB_SUCCESS, iter.next(partition));
    ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, true));
    // init twice
    ASSERT_NE(OB_SUCCESS, iter.init(operator_, multi_schema_service_, true));

    // tid: 1
    ASSERT_EQ(OB_SUCCESS, iter.next(partition));
    ASSERT_EQ(1, partition.replica_count());
    ASSERT_EQ(1UL, extract_pure_id(partition.get_replicas_v2().at(0).table_id_));

    // tid: 2
    ASSERT_EQ(OB_SUCCESS, iter.next(partition));
    ASSERT_EQ(1, partition.replica_count());
    ASSERT_EQ(2UL, extract_pure_id(partition.get_replicas_v2().at(0).table_id_));

    // ignore non exist table (tid: 3), virtual table (tid: 5)

    // tid: OB_ALL_META_TABLE, no replica
    for (int64_t i = 0; i < 16; ++i) {
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(0, partition.replica_count());
    }

    // tid: 50001
    while (OB_SUCCESS == iter.next(partition)) {
      if (partition.get_table_id() == combine_id(2, 50001)) {
        // ASSERT_EQ(OB_SUCCESS, iter.next(partition));
        LOG_INFO("xx", K(partition));
        ASSERT_EQ(2, partition.replica_count());
        ASSERT_EQ(50001UL, extract_pure_id(partition.get_replicas_v2().at(0).table_id_));
        ASSERT_EQ(OB_SUCCESS, iter.next(partition));
        ASSERT_EQ(2, partition.replica_count());
        ASSERT_EQ(50001UL, extract_pure_id(partition.get_replicas_v2().at(0).table_id_));
        break;
      }
    }
    while (OB_SUCCESS == iter.next(partition)) {
      LOG_INFO("tingting", K(partition.get_table_id()));
    }
    // ignore non exist table (tid: 50002)
    ASSERT_EQ(OB_ITER_END, iter.next(partition));
    ASSERT_EQ(OB_ITER_END, iter.next(partition));
  }
  {

    // iterator with filter
    {
      ObPartitionTableIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.get_filters().set_valid_version());
      ASSERT_EQ(OB_SUCCESS, iter.get_filters().set_server(A));

      ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, true));

      // tid: 1
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(1, partition.replica_count());

      // tid: 2
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(1, partition.replica_count());

      // tid: 50001
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(1, partition.replica_count());
      ASSERT_EQ(50001UL, extract_pure_id(partition.get_replicas_v2().at(0).table_id_));
      ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(1, partition.replica_count());
      ASSERT_EQ(50001UL, extract_pure_id(partition.get_replicas_v2().at(0).table_id_));
      ASSERT_EQ(A, partition.get_replicas_v2().at(0).server_);
    }

    // iter empty partition
    {
      ObPartitionTableIterator iter;
      iter.get_filters().set_zone("1024");  // non exist zone
      ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, true));

      // tid: 1
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(0, partition.replica_count());
      ASSERT_EQ(1UL, extract_pure_id(partition.get_table_id()));

      // tid: 2
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(0, partition.replica_count());
      ASSERT_EQ(2UL, extract_pure_id(partition.get_table_id()));

      // tid: 50001 ...
    }

    // fitler empty partition
    {
      ObPartitionTableIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.get_filters().set_valid_version());
      ASSERT_EQ(OB_SUCCESS, iter.get_filters().set_server(B));
      ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, true));
      // tid: 50001
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(1, partition.replica_count());
      ASSERT_EQ(50001UL, extract_pure_id(partition.get_table_id()));
      ASSERT_EQ(B, partition.get_replicas_v2().at(0).server_);
    }

    {
      ObPartitionTableIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.get_filters().set_valid_version());
      ASSERT_EQ(OB_SUCCESS, iter.get_filters().set_server(E));  // non exist server
      ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, false));
      ASSERT_EQ(OB_ITER_END, iter.next(partition));
    }
  }
}

class MockPartitionInfo : public ObPartitionInfo {
public:
  MOCK_METHOD1(filter, int(const ObIReplicaFilter&));
};

class MockPartitionTableOperator : public ObPartitionTableOperator {
public:
  MockPartitionTableOperator(ObIPartPropertyGetter& prop_getter, ObPartitionTableOperator& pt_operator)
      : ObPartitionTableOperator(prop_getter), pt_operator_(pt_operator), return_error_(false)
  {}

  MOCK_METHOD3(get, int(const uint64_t, const int64_t, ObPartitionInfo&));
  int prefetch(const uint64_t tenant_id, const uint64_t start_table_id, const int64_t start_partition_id,
      ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum, bool use_sys_tenant)
  {
    UNUSED(ignore_row_checksum);
    if (return_error_) {
      return OB_ERR_UNEXPECTED;
    } else {
      return pt_operator_.prefetch(
          tenant_id, start_table_id, start_partition_id, partition_infos, false, use_sys_tenant);
    }
  }

  void set_return_error(const bool return_error)
  {
    return_error_ = return_error;
  }

private:
  ObPartitionTableOperator& pt_operator_;
  bool return_error_;
};

TEST_F(TestPartitionTableIterator, fail)
{
  LOG_INFO("case TestPartitionTableIterator.fail");
  GCONF.meta_table_read_write_mode = ObMetaTableMode::METATABLE_MODE_TENANT_ONLY;
  ObPartitionInfo partition;
  ObSchemaGetterGuard schema_guard;
  EXPECT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard));
  // table partition iterator
  {
    ObTablePartitionIterator iter;

    ASSERT_NE(OB_SUCCESS, iter.init(combine_id(1, OB_INVALID_ID), schema_guard, operator_));

    ObPartitionTableOperator opt(prop_getter_);
    ASSERT_EQ(OB_SUCCESS, iter.init(combine_id(1, 1), schema_guard, opt));
    ASSERT_NE(OB_SUCCESS, iter.next(partition));  // fail: opt not init
  }
  // partition table iterator init
  {
    ObPartitionTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, true));
    ASSERT_EQ(OB_INIT_TWICE, iter.init(operator_, multi_schema_service_, true));
  }
  // partition operator get fail
  {
    MockPartitionTableOperator opt(prop_getter_, operator_);
    ASSERT_EQ(OB_SUCCESS, opt.init(db_initer_.get_sql_proxy(), NULL));
    // first call fail
    {
      opt.set_return_error(true);
      ObPartitionTableIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init(opt, multi_schema_service_, true));
      ASSERT_NE(OB_SUCCESS, iter.next(partition));
    }
    // second call fail
    {
      opt.set_return_error(false);
      ObPartitionTableIterator iter;
      rootserver::ObRootService rs;
      rootserver::ObRootService::RsListChangeCb cb(rs);
      opt.set_callback_for_rs(cb, merge_error_cb_);
      ASSERT_EQ(OB_SUCCESS, iter.init(opt, multi_schema_service_, true));
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
      opt.set_return_error(true);
      ASSERT_NE(OB_SUCCESS, iter.next(partition));
    }
  }
  // partition filter fail
  {
    MockPartitionInfo p;
    ObPartitionTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(operator_, multi_schema_service_, true));
    EXPECT_CALL(p, filter(_)).WillOnce(Return(OB_ERR_UNEXPECTED));
    ASSERT_NE(OB_SUCCESS, iter.next(p));
  }
}

TEST_F(TestPartitionTableIterator, tenant)
{
  int ret = OB_SUCCESS;
  const ObMetaTableMode modes[] = {ObMetaTableMode::METATABLE_MODE_SYS_ONLY,
      ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE,
      ObMetaTableMode::METATABLE_MODE_TENANT_ONLY};
  for (int i = 0; i < ARRAYSIZEOF(modes); i++) {
    LOG_INFO("case tenant: use mode ", K(modes[i]));
    GCONF.meta_table_read_write_mode = modes[i];

    const uint64_t tenant_id = 3001 + i;
    const int64_t table_count = 100;
    ObTenantSchema tenant;
    ObTableSchema table;
    ASSERT_EQ(OB_SUCCESS, gen_tenant_schema(tenant_id, tenant));
    // ASSERT_EQ(OB_SUCCESS, schema_service_.add_tenant(tenant));

    CREATE_TENANT(ret, tenant);
    if (modes[i] >= ObMetaTableMode::METATABLE_MODE_SYS_ONLY) {
      ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(tenant.get_tenant_id()));
    }
    // replica count is 200
    for (int64_t i = 0; i < table_count; ++i) {
      const uint64_t pure_id = 50001 + static_cast<uint64_t>(i);
      ASSERT_EQ(OB_SUCCESS, gen_table_schema(tenant_id, pure_id, 1, 1, table));
      table.set_part_level(PARTITION_LEVEL_ZERO);
      table.set_table_type(USER_TABLE);
      CREATE_USER_TABLE_SCHEMA(ret, table);
      // ASSERT_EQ(OB_SUCCESS, schema_service_.add_table(table));
      prop_getter_.clear()
          .add(combine_id(tenant_id, pure_id), 0, A, LEADER)
          .add(combine_id(tenant_id, pure_id), 0, B, FOLLOWER);
      for (int64_t j = 0; j < prop_getter_.get_replicas().count(); j++) {
        GCONF.self_addr_ = prop_getter_.get_replicas().at(j).server_;
        ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(j)));
      }
    }
    ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());

    // case 1: one prefetch get all replicas
    // case 2: one prefetch can just get all replicas, but we don't konw whether last partition
    // is complete, trim last partition, prefetch again
    // case 3: one prefetch can't not get all replicas
    const int64_t prefetch_counts[] = {400, 200, 80};
    for (int64_t idx = 0; idx < ARRAYSIZEOF(prefetch_counts); ++idx) {
      GCONF.partition_table_scan_batch_count = prefetch_counts[idx];
      ObTenantPartitionIterator tenant_partition_iter;
      ASSERT_EQ(OB_SUCCESS, tenant_partition_iter.init(operator_, multi_schema_service_, tenant_id, false));
      ObPartitionInfo partition;
      while (OB_SUCC(tenant_partition_iter.next(partition))) {
        if (partition.get_table_id() == combine_id(tenant_id, 50001)) {
          ASSERT_EQ(2, partition.get_replicas_v2().count());
          break;
        }
      }
      for (int64_t i = 1; i < table_count; ++i) {
        const uint64_t pure_id = 50001 + static_cast<uint64_t>(i);
        ASSERT_EQ(OB_SUCCESS, tenant_partition_iter.next(partition));
        ASSERT_EQ(combine_id(tenant_id, pure_id), partition.get_table_id());
        ASSERT_EQ(2, partition.get_replicas_v2().count());
      }
      ASSERT_EQ(OB_ITER_END, tenant_partition_iter.next(partition));
    }

    // iterator all partition
    ObPartitionTableIterator pt_iter;
    ASSERT_EQ(OB_SUCCESS, pt_iter.init(operator_, multi_schema_service_, true));
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObPartitionInfo partition;
      ret = pt_iter.next(partition);
    }
    ASSERT_EQ(OB_ITER_END, ret);
    if (modes[i] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, release_tenant_space_schema(tenant.get_tenant_id()));
    }
    DROP_TENANT(ret, tenant_id);
  }
}

TEST_F(TestPartitionTableIterator, by_partition)
{
  LOG_INFO("case by_partition");
  int ret = OB_SUCCESS;
  GCONF.meta_table_read_write_mode = ObMetaTableMode::METATABLE_MODE_TENANT_ONLY;
  // partition not in schema, full partition table iterator can still get them
  const uint64_t tenant_id = 1001;
  ObTenantSchema tenant;
  ObTableSchema table;
  ASSERT_EQ(OB_SUCCESS, gen_tenant_schema(tenant_id, tenant));
  // ASSERT_EQ(OB_SUCCESS, schema_service_.add_tenant(tenant));
  CREATE_TENANT(ret, tenant);
  ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(tenant.get_tenant_id()));
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());

  const int64_t table_count = 100;
  // replica count is 200
  for (int64_t i = 0; i < table_count; ++i) {
    const uint64_t pure_id = 50001 + static_cast<uint64_t>(i);
    prop_getter_.clear()
        .add(combine_id(tenant_id, pure_id), 0, A, LEADER)
        .add(combine_id(tenant_id, pure_id), 0, B, FOLLOWER);
    for (int64_t j = 0; j < prop_getter_.get_replicas().count(); j++) {
      GCONF.self_addr_ = prop_getter_.get_replicas().at(j).server_;
      ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(j)));
    }
  }

  // iterator all partition
  const int64_t prefetch_counts[] = {400, 200, 80};
  for (int64_t idx = 0; idx < ARRAYSIZEOF(prefetch_counts); ++idx) {
    int ret = OB_SUCCESS;
    GCONF.partition_table_scan_batch_count = prefetch_counts[idx];
    ObFullPartitionTableIterator pt_iter;
    ASSERT_EQ(OB_SUCCESS, pt_iter.init(operator_, multi_schema_service_));
    ObPartitionInfo partition;
    int64_t sys_tenant_part_cnt = 0;
    int64_t normal_tenant_part_cnt = 0;
    while (OB_SUCC(ret)) {
      ObPartitionInfo partition;
      ret = pt_iter.next(partition);
      LOG_INFO("xx", K(partition));
      if (OB_SUCC(ret)) {
        if (OB_SYS_TENANT_ID == partition.get_tenant_id()) {
          ++sys_tenant_part_cnt;
        } else {
          ++normal_tenant_part_cnt;
        }
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(2, sys_tenant_part_cnt);
    ASSERT_EQ(2 + table_count, normal_tenant_part_cnt);
  }
  ASSERT_EQ(OB_SUCCESS, release_tenant_space_schema(tenant.get_tenant_id()));
  DROP_TENANT(ret, tenant_id);
}

TEST_F(TestPartitionTableIterator, prefetch_second_fail)
{
  int ret = OB_SUCCESS;
  GCONF.partition_table_scan_batch_count = 300;
  const ObMetaTableMode modes[] = {ObMetaTableMode::METATABLE_MODE_SYS_ONLY,
      ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE,
      ObMetaTableMode::METATABLE_MODE_TENANT_ONLY};
  for (int i = 0; i < ARRAYSIZEOF(modes); i++) {
    LOG_INFO("case prefetch_second_fail: use mode ", K(modes[i]));
    GCONF.meta_table_read_write_mode = modes[i];

    const uint64_t tenant_id = 1001;
    const int64_t table_count = 3;
    for (int64_t i = 0; i < table_count; ++i) {
      const uint64_t pure_id = 50001 + static_cast<uint64_t>(i);
      const int64_t part_num = 100;
      for (int64_t j = 0; j < part_num; ++j) {
        prop_getter_.clear()
            .add(combine_id(tenant_id, pure_id), j, A, LEADER)
            .add(combine_id(tenant_id, pure_id), j, B, FOLLOWER);
        for (int64_t k = 0; k < prop_getter_.get_replicas().count(); k++) {
          GCONF.self_addr_ = prop_getter_.get_replicas().at(k).server_;
          ASSERT_EQ(OB_SUCCESS, operator_.update(prop_getter_.get_replicas().at(k)));
        }
      }
    }

    ObTenantSchema tenant;
    ObTableSchema table;
    ASSERT_EQ(OB_SUCCESS, gen_tenant_schema(tenant_id, tenant));
    // ASSERT_EQ(OB_SUCCESS, schema_service_.add_tenant(tenant));

    CREATE_TENANT(ret, tenant);
    if (modes[i] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, gen_tenant_space_schema(tenant.get_tenant_id()));
    }

    for (int64_t i = 0; i < table_count; ++i) {
      const uint64_t pure_id = 50001 + static_cast<uint64_t>(i);
      ASSERT_EQ(OB_SUCCESS, gen_table_schema(tenant_id, pure_id, 1, 1, table));
      table.set_table_type(USER_TABLE);
      table.set_part_level(PARTITION_LEVEL_ONE);
      table.get_part_option().set_part_num(100);
      CREATE_USER_TABLE_SCHEMA(ret, table);
      // ASSERT_EQ(OB_SUCCESS, schema_service.add_table(table)); // user table
    }
    ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
    common::ObMySQLProxy sql_proxy;
    MockPartitionTableOperator pt(prop_getter_, operator_);
    pt.init(sql_proxy, NULL);
    ObTenantPartitionIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(pt, multi_schema_service_, tenant_id, false));
    for (int64_t i = 0; i < 100; ++i) {
      ObPartitionInfo partition;
      ASSERT_EQ(OB_SUCCESS, iter.next(partition));
    }

    // ASSERT_EQ(OB_SUCCESS, schema_service.del_table(combine_id(tenant_id, 50002)));
    const ObTableSchema* table_schema = NULL;
    ObSchemaGetterGuard schema_guard;
    ASSERT_EQ(OB_SUCCESS, multi_schema_service_.get_schema_guard(schema_guard));
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(combine_id(tenant_id, 50002), table_schema));
    DROP_USER_TABLE_SCHEMA(ret, *table_schema);
    ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
    ObPartitionInfo partition;
    ASSERT_EQ(OB_SUCCESS, iter.next(partition));
    if (modes[i] >= ObMetaTableMode::METATABLE_MODE_DOUBLE_WRITE) {
      ASSERT_EQ(OB_SUCCESS, release_tenant_space_schema(tenant.get_tenant_id()));
    }
    DROP_TENANT(ret, tenant_id);
  }
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_partition_table_iterator.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_partition_table_iterator.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
