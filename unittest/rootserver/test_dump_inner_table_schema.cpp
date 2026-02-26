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
#include "src/share/inner_table/ob_inner_table_schema.h"
#include "deps/oblib/src/lib/string/ob_sql_string.h"
#include "src/share/schema/ob_table_sql_service.h"
#include "src/share/schema/ob_schema_utils.h"
#include "deps/oblib/src/lib/file/file_directory_utils.h"
#include "src/share/ob_dml_sql_splicer.h"
#include "src/share/inner_table/ob_load_inner_table_schema.h"
#include "share/inner_table/ob_dump_inner_table_schema.h"

#define ASSERT_SUCCESS(x) ASSERT_EQ((x), OB_SUCCESS)

#define TEST_NAME "test_dump_inner_table_schema"

namespace oceanbase
{
namespace share
{
int ObCompatModeGetter::check_is_oracle_mode_with_table_id(
    const uint64_t tenant_id,
    const int64_t table_id,
    bool &is_oracle_mode)
{
  return false;
}
namespace schema {
int ObSysTableChecker::check_inner_table_exist(const uint64_t tenant_id,
    const ObSimpleTableSchemaV2 &table,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = true;
  return ret;
}
}
}
namespace rootserver
{
using namespace share;
using namespace share::schema;
using namespace common;
using namespace std;
class TestDumpInnerTableSchema : public ::testing::Test
{
public:
  TestDumpInnerTableSchema() {}
protected:
  int get_target_file_name(ObSqlString &file, const ObString &name)
  {
    int ret = OB_SUCCESS;
    char cwd[1024] = {0};
    OV (OB_NOT_NULL(::getcwd(cwd, 1024)), OB_ERR_UNEXPECTED, "failed to getcwd");
    int len = strlen(cwd);
    OZ (file.assign_fmt("%.*s/%.*s", len, cwd, name.length(), name.ptr()));
    return ret;
  }
  int open_target_file(const ObString &file_name)
  {
    int ret = OB_SUCCESS;
    ObSqlString file;
    OZ (get_target_file_name(file, file_name));
    OZ (FSU::open(file.ptr(), O_WRONLY | O_CREAT | O_TRUNC, 0644, fd));
    return ret;
  }
  int close_target_file()
  {
    int ret = OB_SUCCESS;
    OZ (FSU::close(fd));
    return ret;
  }
  int write_target_file(ObSqlString &str)
  {
    int ret = OB_SUCCESS;
    OV (::write(fd, str.ptr(), str.length()) == str.length(), OB_ERR_UNEXPECTED, "failed to write_target_file");
    return ret;
  }
  int dump_tables(const char *table_name, bool empty_file)
  {
    int ret = OB_SUCCESS;
    ObInnerTableSchemaDumper dumper;
    ObInnerTableSchemaPrinter printer;
    ObArray<schema::ObTableSchema *> schema_ptrs;
    ObArray<ObLoadInnerTableSchemaInfo> infos;
    ObSqlString code;
    OZ (dumper.init());
    OZ (dumper.get_hard_code_schema(schema_ptrs));
    OZ (dumper.get_inner_table_schema_info(infos));
    OZ (printer.init(empty_file, schema_ptrs));
    OZ (printer.get_all_code(infos, code));
    OZ (open_target_file(table_name));
    OZ (write_target_file(code));
    OZ (close_target_file());
    return ret;
  }
protected:
  int fd = -1;
};

TEST_F(TestDumpInnerTableSchema, dump_all)
{
  ASSERT_SUCCESS(dump_tables("ob_load_inner_table_schema.cpp", false/*empty_file*/));
}

TEST_F(TestDumpInnerTableSchema, dump_empty)
{
  ASSERT_SUCCESS(dump_tables("ob_load_inner_table_schema_empty.cpp", true/*empty_file*/));
}

TEST_F(TestDumpInnerTableSchema, check)
{
#ifndef OB_BUILD_WITH_EMPTY_LOAD_SCHEMA
  ObInnerTableSchemaDumper dumper;
  ObArray<schema::ObTableSchema *> schema_ptrs;
  ObArray<ObLoadInnerTableSchemaInfo> infos;
  ASSERT_SUCCESS(dumper.init());
  ASSERT_SUCCESS(dumper.get_hard_code_schema(schema_ptrs));
  ASSERT_SUCCESS(dumper.get_inner_table_schema_info(infos));
  for (auto &info : {ALL_CORE_TABLE_LOAD_INFO, ALL_TABLE_LOAD_INFO, ALL_TABLE_HISTORY_LOAD_INFO,
      ALL_COLUMN_LOAD_INFO, ALL_COLUMN_HISTORY_LOAD_INFO, ALL_DDL_OPERATION_LOAD_INFO}) {
    bool find = false;
    for (int64_t i = 0; i < infos.count() && !find; i++) {
      auto &info1 = infos.at(i);
      // check generated load info equals to hard-coded
      if (info.inner_table_id_ == info1.inner_table_id_) {
        find = true;
        ASSERT_STREQ(info.inner_table_name_, info1.inner_table_name_);
        ASSERT_STREQ(info.inner_table_column_names_, info1.inner_table_column_names_);
        ASSERT_EQ(info.row_count_, info1.row_count_);
        for (int j = 0; j < info.row_count_; j++) {
          ASSERT_EQ(info.inner_table_table_ids_[j], info.inner_table_table_ids_[j]);
          ASSERT_STREQ(info.inner_table_rows_[j], info.inner_table_rows_[j]);
          ASSERT_EQ(info.row_checksum_[j], info.row_checksum_[j]);
        }
      }
    }
    ASSERT_TRUE(find);
  }
  // check generated schema version equals to hard-coded
  ASSERT_EQ(get_hard_code_schema_count(), schema_ptrs.count());
  for (int64_t i = 0; i < schema_ptrs.count(); i++) {
    int64_t schema_version = 0;
    ASSERT_NE(schema_ptrs.at(i), nullptr);
    ASSERT_SUCCESS(get_hard_code_schema_version_mapping(schema_ptrs.at(i)->get_table_id(), schema_version));
    ASSERT_EQ(schema_ptrs.at(i)->get_schema_version(), schema_version);
  }
#endif
}

TEST_F(TestDumpInnerTableSchema, utils_check)
{
  ObInnerTableSchemaDumper dumper;
  ObArray<schema::ObTableSchema *> schema_ptrs;
  ASSERT_SUCCESS(dumper.init());
  ASSERT_SUCCESS(dumper.get_hard_code_schema(schema_ptrs));
  ASSERT_GT(schema_ptrs.count(), 0);
  // check TableSchemaCmpByTableId can make ObTableSchema sorted by table id
  lib::ob_sort(schema_ptrs.get_data(), schema_ptrs.get_data() + schema_ptrs.count(), share::TableSchemaCmpByTableId());
  for (int64_t i = 1; i < schema_ptrs.count(); i++) {
    auto *last_schema = schema_ptrs.at(i - 1);
    auto *current_schema = schema_ptrs.at(i);
    ASSERT_NE(last_schema, nullptr);
    ASSERT_NE(current_schema, nullptr);
    ASSERT_LE(last_schema->get_table_id(), current_schema->get_table_id());
  }
  // check TableSchemaCmpByTableId can make ObTableSchema sorted by schema version
  lib::ob_sort(schema_ptrs.get_data(), schema_ptrs.get_data() + schema_ptrs.count(), share::TableSchemaCmpBySchemaVersion());
  for (int64_t i = 1; i < schema_ptrs.count(); i++) {
    auto *last_schema = schema_ptrs.at(i - 1);
    auto *current_schema = schema_ptrs.at(i);
    ASSERT_NE(last_schema, nullptr);
    ASSERT_NE(current_schema, nullptr);
    ASSERT_LE(last_schema->get_schema_version(), current_schema->get_schema_version());
  }
  std::map<uint64_t, ObTableSchema *> tid2table;
  for (int64_t i = 0; i < schema_ptrs.count(); i++) {
    auto *table = schema_ptrs.at(i);
    ASSERT_NE(table, nullptr);
    tid2table[table->get_table_id()] = table;
  }
  // check virtual table's index schema version is less than virtual table schema version
  for (int64_t i = 0; i < schema_ptrs.count(); i++) {
    auto *table = schema_ptrs.at(i);
    ASSERT_NE(table, nullptr);
    if (is_virtual_table(table->get_data_table_id()) && table->is_index_table()) {
      auto *data_table = tid2table[table->get_data_table_id()];
      ASSERT_NE(data_table, nullptr);
      ASSERT_LT(table->get_schema_version(), data_table->get_schema_version());
    }
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f " TEST_NAME ".log*");
  ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  OB_LOGGER.set_log_level("INFO");
  logger.set_file_name(TEST_NAME ".log", true);

  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
