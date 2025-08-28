/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "observer/table/ob_table_meta_handler.h"
#include "observer/table/utils/ob_table_json_utils.h"

using namespace oceanbase::json;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;


class TestObHTableCreateHandler : public ::testing::Test
{
public:
  TestObHTableCreateHandler();
  virtual ~TestObHTableCreateHandler();
  virtual void SetUp();
  virtual void TearDown();
private:
  DISALLOW_COPY_AND_ASSIGN(TestObHTableCreateHandler);

  ObTenantBase tenant_base_;
};

TestObHTableCreateHandler::TestObHTableCreateHandler():
  tenant_base_(1)
{
  ObTenantEnv::set_tenant(&tenant_base_);
}

TestObHTableCreateHandler::~TestObHTableCreateHandler()
{
}

void TestObHTableCreateHandler::SetUp()
{
}

void TestObHTableCreateHandler::TearDown()
{
}

TEST_F(TestObHTableCreateHandler, parse_invalid_json)
{
  const char *json = R"({
    "htable_name": "test",
  })";
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObTableMetaRequest req;
  ObString json_str(strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, json_str, req.data_));
  ObHTableCreateHandler handler(allocator);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, handler.parse(req));
}

TEST_F(TestObHTableCreateHandler, parse_missing_htable_name)
{
  const char *json = R"({
    "column_families": {
      "cf1": {"ttl": 2147483647, "max_version": 2147483647}
    }
  })";
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObTableMetaRequest req;
  ObString json_str(strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, json_str, req.data_));
  ObHTableCreateHandler handler(allocator);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, handler.parse(req));
}

TEST_F(TestObHTableCreateHandler, parse_missing_column_families)
{
  const char *json = R"({
    "htable_name": "test"
  })";
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObTableMetaRequest req;
  ObString json_str(strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, json_str, req.data_));
  ObHTableCreateHandler handler(allocator);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, handler.parse(req));
}

TEST_F(TestObHTableCreateHandler, parse_and_generate_sql_with_no_ttl_and_max_version)
{
  const char *json = R"({
    "htable_name": "test",
    "column_families": {
      "cf1": {"ttl": 2147483647, "max_version": 2147483647},
      "cf2": {"ttl": 2147483647, "max_version": 2147483647}
    }
  })";
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  // 2. create ObTableMetaRequest
  ObTableMetaRequest req;
  ObString json_str(strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, json_str, req.data_));

  // 3. create handler and parse request
  ObHTableCreateHandler handler(allocator);
  ASSERT_EQ(OB_SUCCESS, handler.parse(req));
  // check parse result
  ASSERT_EQ(handler.htable_name_, ObString::make_string("test"));
  ASSERT_EQ(handler.column_families_.count(), 2);
  ASSERT_EQ(handler.column_families_[0]->name_, ObString::make_string("cf1"));
  ASSERT_EQ(handler.column_families_[1]->name_, ObString::make_string("cf2"));
  ASSERT_EQ(handler.column_families_[0]->ttl_, 2147483647);
  ASSERT_EQ(handler.column_families_[0]->max_version_, 2147483647);
  ASSERT_EQ(handler.column_families_[1]->ttl_, 2147483647);
  ASSERT_EQ(handler.column_families_[1]->max_version_, 2147483647);
  ASSERT_EQ(handler.column_families_[0]->has_ttl(), false);
  ASSERT_EQ(handler.column_families_[0]->has_max_version(), false);
  ASSERT_EQ(handler.column_families_[1]->has_ttl(), false);
  ASSERT_EQ(handler.column_families_[1]->has_max_version(), false);

  // 4. generate sql
  ObString tablegroup_sql;
  ObSEArray<ObString, 3> column_family_sql_array;
  ASSERT_EQ(OB_SUCCESS, handler.get_create_table_sql(tablegroup_sql, column_family_sql_array));

  // 5. check sql
  ASSERT_EQ(2, column_family_sql_array.count());
  std::string sql1(column_family_sql_array[0].ptr(), column_family_sql_array[0].length());
  std::string sql2(column_family_sql_array[1].ptr(), column_family_sql_array[1].length());
  // check table name, column family name, kv_attributes
  EXPECT_NE(sql1.find("CREATE TABLE `test$cf1`"), std::string::npos);
  EXPECT_EQ(sql1.find("kv_attributes = '{\"Hbase\": {\"TimeToLive\": 2147483647, \"MaxVersions\": 2147483647, \"CreatedBy\": \"Admin\"}}'"), std::string::npos);
  EXPECT_NE(sql1.find("kv_attributes = '{\"Hbase\": {\"CreatedBy\": \"Admin\"}}'"), std::string::npos);
  EXPECT_NE(sql2.find("CREATE TABLE `test$cf2`"), std::string::npos);
  EXPECT_EQ(sql2.find("kv_attributes = '{\"Hbase\": {\"TimeToLive\": 2147483647, \"MaxVersions\": 2147483647, \"CreatedBy\": \"Admin\"}}'"), std::string::npos);
  EXPECT_NE(sql2.find("kv_attributes = '{\"Hbase\": {\"CreatedBy\": \"Admin\"}}'"), std::string::npos);

  // check tablegroup
  std::string tg(tablegroup_sql.ptr(), tablegroup_sql.length());
  std::cout<<tg<<std::endl;
  EXPECT_NE(tg.find("CREATE TABLEGROUP `test`"), std::string::npos);
}

TEST_F(TestObHTableCreateHandler, parse_and_generate_sql_with_ttl)
{
  const char *json = R"({
    "htable_name": "test",
    "column_families": {
      "cf1": {"ttl": 3600, "max_version": 2147483647}
    }
  })";
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObTableMetaRequest req;
  ObString json_str(strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, json_str, req.data_));

  // 3. create handler and parse request
  ObHTableCreateHandler handler(allocator);
  ASSERT_EQ(OB_SUCCESS, handler.parse(req));
  // check parse result
  ASSERT_EQ(handler.htable_name_, ObString::make_string("test"));
  ASSERT_EQ(handler.column_families_.count(), 1);
  ASSERT_EQ(handler.column_families_[0]->name_, ObString::make_string("cf1"));
  ASSERT_EQ(handler.column_families_[0]->ttl_, 3600);
  ASSERT_EQ(handler.column_families_[0]->max_version_, 2147483647);
  ASSERT_EQ(handler.column_families_[0]->has_ttl(), true);
  ASSERT_EQ(handler.column_families_[0]->has_max_version(), false);

  // 4. generate sql
  ObString tablegroup_sql;
  ObSEArray<ObString, 3> column_family_sql_array;
  ASSERT_EQ(OB_SUCCESS, handler.get_create_table_sql(tablegroup_sql, column_family_sql_array));

  // 5. check sql 
  ASSERT_EQ(1, column_family_sql_array.count());
  std::string sql(column_family_sql_array[0].ptr(), column_family_sql_array[0].length());
  // check table name, column family name, kv_attributes
  EXPECT_NE(sql.find("CREATE TABLE `test$cf1`"), std::string::npos);
  EXPECT_NE(sql.find("kv_attributes = '{\"Hbase\": {\"TimeToLive\": 3600, \"CreatedBy\": \"Admin\"}}'"), std::string::npos); // no max_version

  // check tablegroup
  std::string tg(tablegroup_sql.ptr(), tablegroup_sql.length());
  EXPECT_NE(tg.find("CREATE TABLEGROUP `test`"), std::string::npos);
}

TEST_F(TestObHTableCreateHandler, parse_and_generate_sql_with_max_version)
{
  const char *json = R"({
    "htable_name": "test",
    "column_families": {
      "cf1": {"ttl": 2147483647, "max_version": 3}
    }
  })";
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObTableMetaRequest req;
  ObString json_str(strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, json_str, req.data_));

  // 3. create handler and parse request
  ObHTableCreateHandler handler(allocator);
  ASSERT_EQ(OB_SUCCESS, handler.parse(req));
  // check parse result
  ASSERT_EQ(handler.htable_name_, ObString::make_string("test"));
  ASSERT_EQ(handler.column_families_.count(), 1);
  ASSERT_EQ(handler.column_families_[0]->name_, ObString::make_string("cf1"));
  ASSERT_EQ(handler.column_families_[0]->ttl_, 2147483647);
  ASSERT_EQ(handler.column_families_[0]->max_version_, 3);
  ASSERT_EQ(handler.column_families_[0]->has_ttl(), false);
  ASSERT_EQ(handler.column_families_[0]->has_max_version(), true);

  // 4. generate sql
  ObString tablegroup_sql;
  ObSEArray<ObString, 3> column_family_sql_array;
  ASSERT_EQ(OB_SUCCESS, handler.get_create_table_sql(tablegroup_sql, column_family_sql_array));

  // 5. check sql
  ASSERT_EQ(1, column_family_sql_array.count());
  std::string sql(column_family_sql_array[0].ptr(), column_family_sql_array[0].length());
  // check table name, column family name, kv_attributes
  EXPECT_NE(sql.find("CREATE TABLE `test$cf1`"), std::string::npos);
  EXPECT_NE(sql.find("kv_attributes = '{\"Hbase\": {\"MaxVersions\": 3, \"CreatedBy\": \"Admin\"}}'"), std::string::npos); // no ttl

  // check tablegroup
  std::string tg(tablegroup_sql.ptr(), tablegroup_sql.length());
  EXPECT_NE(tg.find("CREATE TABLEGROUP `test`"), std::string::npos);
} 

TEST_F(TestObHTableCreateHandler, parse_and_generate_sql_with_ttl_and_max_version)
{
  const char *json = R"({
    "htable_name": "test",
    "column_families": {
      "cf1": {"ttl": 3600, "max_version": 3},
      "cf2": {"ttl": 100, "max_version": 1}
    }
  })";
  // 2. create ObTableMetaRequest
  ObTableMetaRequest req;
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObString json_str(strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, json_str, req.data_));

  // 3. create handler and parse request
  ObHTableCreateHandler handler(allocator);
  ASSERT_EQ(OB_SUCCESS, handler.parse(req));
  // check parse result
  ASSERT_EQ(handler.htable_name_, ObString::make_string("test"));
  ASSERT_EQ(handler.column_families_.count(), 2);
  ASSERT_EQ(handler.column_families_[0]->name_, ObString::make_string("cf1"));
  ASSERT_EQ(handler.column_families_[1]->name_, ObString::make_string("cf2"));
  ASSERT_EQ(handler.column_families_[0]->ttl_, 3600);
  ASSERT_EQ(handler.column_families_[0]->max_version_, 3);
  ASSERT_EQ(handler.column_families_[0]->has_ttl(), true);
  ASSERT_EQ(handler.column_families_[0]->has_max_version(), true);
  ASSERT_EQ(handler.column_families_[1]->ttl_, 100);
  ASSERT_EQ(handler.column_families_[1]->max_version_, 1);
  ASSERT_EQ(handler.column_families_[1]->has_ttl(), true);
  ASSERT_EQ(handler.column_families_[1]->has_max_version(), true);

  // 4. generate sql
  ObString tablegroup_sql;
  ObSEArray<ObString, 3> column_family_sql_array;
  ASSERT_EQ(OB_SUCCESS, handler.get_create_table_sql(tablegroup_sql, column_family_sql_array));

  // 5. check sql
  ASSERT_EQ(2, column_family_sql_array.count());
  std::string sql1(column_family_sql_array[0].ptr(), column_family_sql_array[0].length());
  std::string sql2(column_family_sql_array[1].ptr(), column_family_sql_array[1].length());
  // check table name, column family name, kv_attributes
  EXPECT_NE(sql1.find("CREATE TABLE `test$cf1`"), std::string::npos);
  EXPECT_NE(sql1.find("kv_attributes = '{\"Hbase\": {\"TimeToLive\": 3600, \"MaxVersions\": 3, \"CreatedBy\": \"Admin\"}}'"), std::string::npos);

  EXPECT_NE(sql2.find("CREATE TABLE `test$cf2`"), std::string::npos);
  EXPECT_NE(sql2.find("kv_attributes = '{\"Hbase\": {\"TimeToLive\": 100, \"MaxVersions\": 1, \"CreatedBy\": \"Admin\"}}'"), std::string::npos);

  // check tablegroup
  std::string tg(tablegroup_sql.ptr(), tablegroup_sql.length());
  EXPECT_NE(tg.find("CREATE TABLEGROUP `test`"), std::string::npos);
}

class TestObHTableRegionLocatorHandler : public ::testing::Test
{
public:
  TestObHTableRegionLocatorHandler();
  virtual ~TestObHTableRegionLocatorHandler();
  virtual void SetUp();
  virtual void TearDown();
private:
  void check_try_compress_json(ObIAllocator &allocator,
                              const ObTableMetaResponse &resp,
                              const ObIArray<ObHTableRegionLocatorHandler::TabletInfo*> &tablet_infos,
                              int table_count,
                              int partition_per_table,
                              int replica_per_partition,
                              TestObHTableRegionLocatorHandler *self);
  int build_tablet_infos(ObIAllocator &allocator,
                          int table_count,
                          int partition_per_table,
                          int replica_per_partition,
                          ObIArray<ObHTableRegionLocatorHandler::TabletInfo*> &tablet_infos);
  int replica_dict_to_array(ObIAllocator &allocator, Value *json_array, ObIArray<ObString> &array);
  DISALLOW_COPY_AND_ASSIGN(TestObHTableRegionLocatorHandler);

  ObTenantBase tenant_base_;
};

TestObHTableRegionLocatorHandler::TestObHTableRegionLocatorHandler():
  tenant_base_(1)
{
  ObTenantEnv::set_tenant(&tenant_base_);
}

TestObHTableRegionLocatorHandler::~TestObHTableRegionLocatorHandler()
{
}

void TestObHTableRegionLocatorHandler::SetUp()
{
}

void TestObHTableRegionLocatorHandler::TearDown()
{
}

/*
  N 张表，每张表有 M 个分区，每个分区有 K 个副本
  副本的role 0 表示Leader，1 表示Follower
  假设M个分区的副本都分布在K台服务器上，则每台服务器上都有M个副本
*/
int TestObHTableRegionLocatorHandler::build_tablet_infos(ObIAllocator &allocator,
                                                          int table_count,
                                                          int partition_per_table,
                                                          int replica_per_partition,
                                                          ObIArray<ObHTableRegionLocatorHandler::TabletInfo*> &tablet_infos)
{
  int ret = OB_SUCCESS;
  for (int table_idx = 0; table_idx < table_count; ++table_idx) {
    int64_t table_id = 1000 + table_idx;
    for (int part_idx = 0; part_idx < partition_per_table; ++part_idx) {
      int64_t tablet_id = table_id * 100 + part_idx;
      // 构造 upperbound
      char ub_buf[32];
      snprintf(ub_buf, sizeof(ub_buf), "ub%d", part_idx);
      ObString upperbound;
      ObHTableRegionLocatorHandler::TabletInfo *tinfo = nullptr;
      if (OB_FAIL(ob_write_string(allocator, ObString(strlen(ub_buf), ub_buf), upperbound))) {
        LOG_WARN("failed to write upperbound", K(ret));
      } else if (OB_ISNULL(tinfo = OB_NEWx(ObHTableRegionLocatorHandler::TabletInfo, &allocator, table_id, tablet_id, upperbound))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate tablet info", K(ret));
      } else {
        for (int rep_idx = 0; rep_idx < replica_per_partition; ++rep_idx) {
          ObHTableRegionLocatorHandler::TabletLocation *replica = nullptr;
          char ip_buf[32];
          snprintf(ip_buf, sizeof(ip_buf), "10.0.0.%d", rep_idx);
          ObString ip_str;
          int64_t port = 10000 + rep_idx;
          int64_t role = !!(rep_idx == 0);
          if (OB_FAIL(ob_write_string(allocator, ObString(strlen(ip_buf), ip_buf), ip_str))) {
            LOG_WARN("failed to write ip", K(ret));
          } else if (OB_ISNULL(replica = OB_NEWx(ObHTableRegionLocatorHandler::TabletLocation, &allocator, ip_str, port, role))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate tablet location", K(ret));
          } else {
            tinfo->replicas_.push_back(replica);
          }
        }
        tablet_infos.push_back(tinfo); 
      }
    }
  }
  return ret;
}



int TestObHTableRegionLocatorHandler::replica_dict_to_array(ObIAllocator &allocator, Value *json_array, ObIArray<ObString> &array)
{
  int ret = OB_SUCCESS;
  Value *iter = json_array->get_array().get_first();
  for (int i = 0; OB_NOT_NULL(iter) && i < json_array->get_array().get_size(); ++i, iter = iter->get_next()) {
    if (iter->get_type() == JT_ARRAY) {
      // {ip, port}
      Value *ip_iter = iter->get_array().get_first();
      Value *port_iter = ip_iter->get_next();
      if (ip_iter->get_type() == JT_STRING && port_iter->get_type() == JT_NUMBER) {
        ObString ip_str;
        ObString port_str;
        char ip_buf[32];
        snprintf(ip_buf, sizeof(ip_buf), "%.*s:%ld", ip_iter->get_string().length(), ip_iter->get_string().ptr(), port_iter->get_number());
        if (OB_FAIL(ob_write_string(allocator, ObString(strlen(ip_buf), ip_buf), ip_str))) {
          LOG_WARN("failed to write ip", K(ret));
        } else {
          array.push_back(ip_str);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json type", K(ret), K(iter->get_type()));
      break;
    }
  }
  return ret;
}

void TestObHTableRegionLocatorHandler::check_try_compress_json(ObIAllocator &allocator,
                             const ObTableMetaResponse &resp,
                             const ObIArray<ObHTableRegionLocatorHandler::TabletInfo*> &tablet_infos,
                             int table_count,
                             int partition_per_table,
                             int replica_per_partition,
                             TestObHTableRegionLocatorHandler *self)
{
  // 解析 JSON
  Value *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTableJsonUtils::parse(allocator, resp.data_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(JT_OBJECT, root->get_type());

  Value *table_id_dict_value = nullptr;
  Value *replica_dict_value = nullptr;
  Value *partitions_value = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTableJsonUtils::get_json_value(root, "table_id_dict", JT_ARRAY, table_id_dict_value));
  ASSERT_EQ(OB_SUCCESS, ObTableJsonUtils::get_json_value(root, "replica_dict", JT_ARRAY, replica_dict_value));
  ASSERT_EQ(OB_SUCCESS, ObTableJsonUtils::get_json_value(root, "partitions", JT_ARRAY, partitions_value));
  ASSERT_EQ(table_id_dict_value->get_array().get_size(), table_count);
  ASSERT_EQ(replica_dict_value->get_array().get_size(), replica_per_partition);
  ASSERT_EQ(partitions_value->get_array().get_size(), table_count * partition_per_table * replica_per_partition);

  // get all ip:port in tablet_infos, no duplicate
  ObSEArray<ObString, 10> ip_port_array;
  for (int i = 0; i < tablet_infos.count(); ++i) {
    for (int j = 0; j < tablet_infos.at(i)->replicas_.count(); ++j) {
      ObHTableRegionLocatorHandler::TabletLocation *replica = tablet_infos.at(i)->replicas_[j];
      ObSqlString ip_port;
      ip_port.append_fmt("%.*s:%ld", replica->svr_ip_.length(), replica->svr_ip_.ptr(), replica->svr_port_);
      if (std::find(ip_port_array.begin(), ip_port_array.end(), ip_port.string()) == ip_port_array.end()) {
        ObString ip_port_str;
        ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator, ip_port.string(), ip_port_str));
        ip_port_array.push_back(ip_port_str);
      }
    }
  }
  ObSEArray<ObString, 10> replica_dict_array;
  ASSERT_EQ(OB_SUCCESS, self->replica_dict_to_array(allocator, replica_dict_value, replica_dict_array));
  // check ip_port_array equal to replica_dict
  std::sort(ip_port_array.begin(), ip_port_array.end());
  std::sort(replica_dict_array.begin(), replica_dict_array.end());
  ASSERT_EQ(ip_port_array.count(), replica_dict_array.count());

  for (int i = 0; i < ip_port_array.count(); ++i) {
    std::string ip_port_str(ip_port_array[i].ptr(), ip_port_array[i].length());
    std::string replica_dict_str(replica_dict_array[i].ptr(), replica_dict_array[i].length());
    ASSERT_EQ(ip_port_str, replica_dict_str);
  }

  // check partitions info
  Value *partition_value = partitions_value->get_array().get_first();
  for (int i = 0; OB_NOT_NULL(partition_value) && i < table_count * partition_per_table * replica_per_partition; ++i, partition_value = partition_value->get_next()) {
    // [table_id_idx, tablet_id, upperbound, replica_idx, role]
    // table_id_idx: 0 ~ table_count - 1
    // replica_idx: 0 ~ replica_per_partition - 1
    Value *tb_idx_iter = partition_value->get_array().get_first();
    ASSERT_EQ(JT_NUMBER, tb_idx_iter->get_type());
    ASSERT_TRUE(tb_idx_iter->get_number() >= 0 && tb_idx_iter->get_number() < table_count);
    Value *replica_idx_iter = tb_idx_iter->get_next()->get_next()->get_next();
    ASSERT_EQ(JT_NUMBER, replica_idx_iter->get_type());
    ASSERT_TRUE(replica_idx_iter->get_number() >= 0 && replica_idx_iter->get_number() < replica_per_partition);
  }
}

TEST_F(TestObHTableRegionLocatorHandler, try_compress_with_range_part)
{
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObHTableRegionLocatorHandler handler(allocator);
  ObSEArray<ObHTableRegionLocatorHandler::TabletInfo*, 10> tablet_infos;
  int table_count = 3;
  int partition_per_table = 5;
  int replica_per_partition = 3;
  ASSERT_EQ(OB_SUCCESS, build_tablet_infos(allocator, table_count, partition_per_table, replica_per_partition, tablet_infos));
  ASSERT_EQ(table_count * partition_per_table, tablet_infos.count());
  handler.tablet_infos_.assign(tablet_infos);
  ObTableMetaResponse resp;
  ASSERT_EQ(OB_SUCCESS, handler.try_compress(resp));
  check_try_compress_json(allocator, resp, tablet_infos, table_count, partition_per_table, replica_per_partition, this);
}

TEST_F(TestObHTableRegionLocatorHandler, try_compress_with_hash_part)
{
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObHTableRegionLocatorHandler handler(allocator);
  ObSEArray<ObHTableRegionLocatorHandler::TabletInfo*, 10> tablet_infos;
  int table_count = 3;
  int partition_per_table = 5;
  int replica_per_partition = 1;
  ASSERT_EQ(OB_SUCCESS, build_tablet_infos(allocator, table_count, partition_per_table, replica_per_partition, tablet_infos));
  ASSERT_EQ(table_count * partition_per_table, tablet_infos.count());
  for (int i = 0; i < tablet_infos.count(); ++i) {
    tablet_infos[i]->upperbound_ = ObString::make_empty_string();
  }
  handler.tablet_infos_.assign(tablet_infos);
  ObTableMetaResponse resp;
  ASSERT_EQ(OB_SUCCESS, handler.try_compress(resp));
  check_try_compress_json(allocator, resp, tablet_infos, table_count, partition_per_table, replica_per_partition, this);
}

TEST_F(TestObHTableRegionLocatorHandler, try_compress_with_no_part_table)
{
  ObArenaAllocator allocator;
  allocator.set_tenant_id(1);
  ObHTableRegionLocatorHandler handler(allocator);
  ObSEArray<ObHTableRegionLocatorHandler::TabletInfo*, 10> tablet_infos;
  int table_count = 3;
  int partition_per_table = 1;
  int replica_per_partition = 3;
  ASSERT_EQ(OB_SUCCESS, build_tablet_infos(allocator, table_count, partition_per_table, replica_per_partition, tablet_infos));
  ASSERT_EQ(table_count * partition_per_table, tablet_infos.count());
  for (int i = 0; i < tablet_infos.count(); ++i) {
    tablet_infos[i]->upperbound_ = ObString::make_empty_string();
  }
  handler.tablet_infos_.assign(tablet_infos);
  ObTableMetaResponse resp;
  ASSERT_EQ(OB_SUCCESS, handler.try_compress(resp));
  check_try_compress_json(allocator, resp, tablet_infos, table_count, partition_per_table, replica_per_partition, this);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_meta_executor.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
