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

#include <unistd.h>
#include <sys/types.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#define private public
#define protected public

#include "env/ob_simple_cluster_test_base.h"
#include "share/config/ob_server_config.h"

#define ASSERT_SUCCESS(x) ASSERT_EQ((x), OB_SUCCESS)

#define TEST_NAME "test_create_tenant_compare"
#define PARALLEL_DATA_FILE "parallel_data"

bool is_child_program = false;
bool prepare_success = false;
int fd = 0;
#define CHILD_FLAG "is_a_child_program"

namespace oceanbase {
namespace unittest
{
struct Row
{
  OB_UNIS_VERSION(1);
  ObSArray<ObObj> values;
  void reset() { values.reset(); }
  TO_STRING_KV(K(values));
  bool operator == (const Row &row) const
  {
    int ret = OB_SUCCESS;
    bool equal = true;
    equal = values.count() == row.values.count();
    for (int64_t i = 0; i < values.count() && equal; i++) {
      if (OB_FAIL(values[i].equal(row.values[i], equal))) {
        equal = false;
      }
      if (!equal) {
        LOG_WARN("value not equal", KR(ret), K(i), K(values[i]), K(row.values[i]));
      }
    }
    return equal;
  }
};
OB_SERIALIZE_MEMBER(Row, values);
struct Table
{
  OB_UNIS_VERSION(1);
  ObString table_name;
  ObSArray<Row> rows;
  void reset() { rows.reset(); }
  TO_STRING_KV(K(table_name), K(rows));
  bool operator == (const Table &table) const
  {
    bool ret = true;
    ret = ((table.table_name == table_name) && (table.rows.count() == rows.count()));
    for (int64_t i = 0; i < rows.count() && ret; i++) {
      ret = (table.rows[i] == rows[i]);
      if (!ret) {
        LOG_WARN("row not equal", KR(ret), K(i), K(table_name), K(rows[i]), K(table.rows[i]));
      }
    }
    return ret;
  }
};
OB_SERIALIZE_MEMBER(Table, table_name, rows);
struct TenantSysTableData
{
  OB_UNIS_VERSION(1);
  ObSArray<Table> tables;
  void reset() { tables.reset(); }
  TO_STRING_KV(K(tables));
  bool operator == (const TenantSysTableData &data) const
  {
    bool ret = true;
    ret = (data.tables.count() == tables.count());
    for (int64_t i = 0; i < tables.count() && ret; i++) {
      ret = (data.tables[i] == tables[i]);
      if (!ret) {
        LOG_WARN("table not equal", KR(ret), K(i));
      }
    }
    return ret;
  }
};
OB_SERIALIZE_MEMBER(TenantSysTableData, tables);
struct Data2Compare
{
  OB_UNIS_VERSION(1);
  int ret;
  TenantSysTableData sys;
  TenantSysTableData meta;
  TenantSysTableData mysql;
  TenantSysTableData oracle;
  ObArenaAllocator allocator;
  void reset() {
    ret = OB_SUCCESS;
    allocator.reset();
    sys.reset();
    meta.reset();
    mysql.reset();
    oracle.reset();
  }
  TO_STRING_KV(K(ret), K(sys), K(meta), K(mysql), K(oracle));
  bool operator == (const Data2Compare &data) const 
  {
    return ret == data.ret && sys == data.sys && meta == data.meta && mysql == data.mysql 
      && oracle == data.oracle;
  }
};
OB_SERIALIZE_MEMBER(Data2Compare, ret, sys, meta, mysql, oracle);
Data2Compare parallel_data;
#define ALL_CORE_TABLE_SQL "select * from oceanbase.%s where table_name in ('__all_table', '__all_column') and column_name not in ('schema_version', 'gmt_create', 'gmt_modified') order by table_name, row_id, column_name" 
#define TABLE_SQL "select * from oceanbase.%s where table_id < 500000 and schema_version < (select column_value from oceanbase.__all_core_table where column_name='baseline_schema_version') "
#define ALL_TABLE_SQL TABLE_SQL "order by table_id"
#define ALL_COLUMN_SQL TABLE_SQL "order by table_id, column_id"
// 只获取创建租户时的第一条operation记录，检查时忽略schema_version
#define ALL_DDL_OPERATION_SQL "select * from oceanbase.%s where schema_version in " \
  "(select min(schema_version) a from __all_table_history where table_id < 500000 group by table_id" \
    " union "\
  "select column_value a from __all_core_table where column_name = 'schema_version')"\
  " and schema_version < (select column_value from oceanbase.__all_core_table where column_name='baseline_schema_version') " \
  "order by table_id"
class ObCreateTenantCompareTest : public ObSimpleClusterTestBase
{
public:
  ObCreateTenantCompareTest() : ObSimpleClusterTestBase(TEST_NAME "_") {}
  // 避免运行ObSimpleClusterTestBase里的SetUp和TearDown
  virtual void SetUp() {}
  virtual void TearDown() {}
  int start_observer(bool enable_parallel_tenant_creation = false);
  int dump_tenant_data(const uint64_t tenant_id, TenantSysTableData &data, ObIAllocator &allocator);
  int dump_table_data(const uint64_t tenant_id, const char *table_name, const char *sql_template,
      ObIAllocator &allocator, Table &table);
  int dump_data(Data2Compare &data);
  int run(const bool enable_parallel_tenant_creation, Data2Compare &data);
};
int ObCreateTenantCompareTest::start_observer(bool enable_parallel_tenant_creation) 
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(cluster_.get()));
  ObSqlString optstr;
  OZ (optstr.assign_fmt("_enable_parallel_tenant_creation=%s", enable_parallel_tenant_creation ? "1" : "0"));
  OX (cluster_->set_extra_optstr(optstr.ptr()));
  OZ (start());
  return ret;
}
int ObCreateTenantCompareTest::dump_table_data(const uint64_t tenant_id,  const char *table_name,
    const char *sql_template, ObIAllocator &allocator, Table &table)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  table.table_name = table_name;
  OZ (sql.assign_fmt(sql_template, table_name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    int64_t column_count = -1;
    OZ (sql_proxy.read(res, tenant_id, sql.ptr()), tenant_id, sql);
    result = res.get_result();
    CK (OB_NOT_NULL(result));
    int64_t schema_version_idx = -1;
    if (FAILEDx(static_cast<ObMySQLResultImpl*>(result)->column_map_.get_refactored("schema_version", schema_version_idx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get schema_version idx", KR(ret));
      }
    }
    while (OB_SUCC(ret)) {
      Row row;
      ObObj obj;
      ObObjMeta meta;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        break;
      }
      if (column_count != -1) {
        CK (result->get_column_count() == column_count);
      }
      OX (column_count = result->get_column_count());
      for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
        if (i != schema_version_idx) {
          OZ (result->get_type(i, meta));
          // ignore timestamp
          if (!meta.is_timestamp()) {
            OZ (result->get_obj(i, obj, nullptr/*tz_info*/, &allocator), i);
            int64_t size = obj.get_deep_copy_size();
            char *buf = (char *)allocator.alloc(size);
            int64_t pos = 0;
            ObObj real_obj;
            OZ (real_obj.deep_copy(obj, buf, size, pos));
            OZ (row.values.push_back(real_obj));
          }
        }
      }
      FLOG_INFO("get one row", KR(ret), K(row), K(column_count), K(result->get_column_count()));
      OZ (table.rows.push_back(row));
    }
  }
  return ret;
}
int ObCreateTenantCompareTest::dump_tenant_data(const uint64_t tenant_id, TenantSysTableData &data,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  OZ (data.tables.prepare_allocate(4));
  OZ (dump_table_data(tenant_id, OB_ALL_CORE_TABLE_TNAME, ALL_CORE_TABLE_SQL, allocator, data.tables[0]));
  OZ (dump_table_data(tenant_id, OB_ALL_TABLE_TNAME, ALL_TABLE_SQL, allocator, data.tables[1]));
  OZ (dump_table_data(tenant_id, OB_ALL_COLUMN_TNAME, ALL_COLUMN_SQL, allocator, data.tables[2]));
  OZ (dump_table_data(tenant_id, OB_ALL_DDL_OPERATION_TNAME, ALL_DDL_OPERATION_SQL, allocator, data.tables[3]));
  return ret;
}
int ObCreateTenantCompareTest::dump_data(Data2Compare &data)
{
  int ret = OB_SUCCESS;
  uint64_t mysql_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t oracle_tenant_id = OB_INVALID_TENANT_ID;
  data.reset();
  OZ (get_curr_simple_server().init_sql_proxy2("sys", "oceanbase", false/*oracle_mode*/, 
        ObMySQLConnection::OCEANBASE_MODE));
  OZ (dump_tenant_data(OB_SYS_TENANT_ID, data.sys, data.allocator));
  OZ (create_tenant("mysql", "2G", "2G"));
  OZ (get_tenant_id(mysql_tenant_id, "mysql"));
  OZ (dump_tenant_data(mysql_tenant_id - 1, data.meta, data.allocator));
  OZ (dump_tenant_data(mysql_tenant_id, data.mysql, data.allocator));
  OZ (create_tenant("oracle", "2G", "2G", true/*oracle_mode*/));
  OZ (get_tenant_id(oracle_tenant_id, "oracle"));
  OZ (dump_tenant_data(oracle_tenant_id, data.oracle, data.allocator));
  return ret;
}
int ObCreateTenantCompareTest::run(const bool enable_parallel_tenant_creation, Data2Compare &data)
{
  int ret = OB_SUCCESS;
  OZ (start_observer(enable_parallel_tenant_creation));
  OZ (dump_data(data));
  return ret;
}
class ObCreateTenantCompareTestChild : public ObCreateTenantCompareTest
{
public:
  ObCreateTenantCompareTestChild() : ObCreateTenantCompareTest() {}
  virtual void TestBody() {}
  void run_as_child();
};
TEST_F(ObCreateTenantCompareTest, check_prepare)
{
  ASSERT_TRUE(prepare_success);
}
TEST_F(ObCreateTenantCompareTest, compare_tables)
{
  int ret = OB_SUCCESS;
  Data2Compare standard_data;
  ASSERT_SUCCESS(run(false /* enable_parallel_tenant_creation*/, standard_data));
  ASSERT_SUCCESS(standard_data.ret);
  ASSERT_SUCCESS(parallel_data.ret);
  ASSERT_EQ(standard_data, parallel_data);
}
}
}

// load parallel_data from PARALLEL_DATA_FILE
void run_as_parent() 
{
  int64_t buf_size = 0;
  int ret = 0;
  int fd = ::open(PARALLEL_DATA_FILE, O_RDONLY);
  if (fd < 0) {
    perror("open");
  } else if (::read(fd, &buf_size, sizeof(buf_size)) != sizeof(buf_size)) {
    perror("read1");
  } else if (buf_size <= 0) {
    std::cerr << "buf_size <= 0, " << buf_size << std::endl;
  } else {
    std::cerr << "buf_size " << buf_size << std::endl;
    char *buf = new char[buf_size];
    int64_t pos = 0;
    if (::read(fd, buf, buf_size) != buf_size) {
      perror("read2");
    } else if (oceanbase::unittest::parallel_data.deserialize(buf, buf_size, pos) != OB_SUCCESS) {
      std::cerr << ("deserialize failed") << std::endl;
    } else if (oceanbase::unittest::parallel_data.ret != OB_SUCCESS) {
      perror("ret != OB_SUCCESS");
    } else if (::close(fd)) {
      perror("close");
    } else {
      prepare_success = true;
    }
  }
}

// dump inner table data in parallel create tenant mode and write to file PARALLEL_DATA_FILE
void oceanbase::unittest::ObCreateTenantCompareTestChild::run_as_child()
{
  int ret = OB_SUCCESS;
  std::cerr << "ObCreateTenantCompareTestChild::run_as_child" << std::endl;
  Data2Compare data;
  data.ret = run(true /* enable_parallel_tenant_creation*/, data);
  int64_t buf_size = data.get_serialize_size();
  char *buf = new char[buf_size];
  int64_t pos = 0;
  if (OB_FAIL(data.serialize(buf, buf_size, pos))) {
    std::cerr << "data.serialize" << std::endl;
  } else if (sizeof(buf_size) != ::write(fd, &buf_size, sizeof(buf_size))) {
    perror("write1");
  } else if (buf_size != ::write(fd, buf, buf_size)) {
    perror("write2");
  } else if (::close(fd) != 0) {
    perror("close");
  } else {
    std::cerr << "run_as_child done " << buf_size << std::endl;
  }
  close();
}

int main(int argc, char *argv[])
{
  const char *log_level = "INFO";
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  if (argc >= 2 && strcmp(argv[1], CHILD_FLAG) == 0) {
    is_child_program = true;
    if ((fd = ::open(PARALLEL_DATA_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
      perror("open");
    } else {
      oceanbase::unittest::ObCreateTenantCompareTestChild test;
      std::cerr << "run_as_child" << std::endl;
      test.run_as_child();
    }
  } else {
    system("rm -f " PARALLEL_DATA_FILE);
    pid_t pid = 0;
    if ((pid = ::fork()) < 0) {
      perror("fork");
      return 1;
    } else if (pid == 0) {
      char program_name[PATH_MAX];
      if (::realpath(argv[0], program_name) == nullptr) {
        perror(argv[0]);
      } else if (::execl(program_name, program_name, CHILD_FLAG, nullptr) == -1) {
        perror("execl");
      }
    } else {
      int status = 0;
      ::wait(&status);
      std::cerr << "status " << status << std::endl;
      run_as_parent();
      ::testing::InitGoogleTest(&argc, argv);
      return RUN_ALL_TESTS();
    } 
  }
  return 0;
}
