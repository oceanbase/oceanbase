/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * This file defines test_ob_cdc_part_trans_resolver.cpp
 */


#include "data_dict_test_utils.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace share::schema;

namespace oceanbase
{
namespace datadict
{

TEST(ObDictMetaHeader, test_raw)
{
  ObDictMetaHeader header(ObDictMetaType::TABLE_META);
  ObDictMetaHeader header_after(ObDictMetaType::INVALID_META);
  SCN scn;

  header.version_ = OB_INVALID_VERSION;
  header.set_snapshot_scn(scn);
  header.set_storage_type(ObDictMetaStorageType::FULL);
  EXPECT_FALSE(header.is_valid());
  scn.convert_for_gts(get_timestamp());
  header.set_snapshot_scn(scn);
  header.set_dict_serialize_length(9678);
  EXPECT_TRUE(header.is_valid());

  const int64_t serialize_size = header.get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, header.serialize(buf, serialize_size, pos));
  EXPECT_EQ(serialize_size, pos);
  int64_t deserialize_pos = 0;

  EXPECT_EQ(OB_SUCCESS, header_after.deserialize(buf, serialize_size, deserialize_pos));
  EXPECT_EQ(serialize_size, deserialize_pos);
  EXPECT_TRUE(header == header_after);

  // verify serialize_size is not changed after change storage_type
  header.set_storage_type(ObDictMetaStorageType::MIDDLE);
  EXPECT_EQ(serialize_size, header.get_serialize_size());
}

TEST(ObDictTenantMeta, test_raw)
{
  ObArenaAllocator allocator;
  ObArenaAllocator allocator_for_deserialize;
  ObDictTenantMeta tenant_meta(&allocator);
  ObDictMetaHeader header(ObDictMetaType::TENANT_META);
  tenant_meta.tenant_id_ = 1001;
  tenant_meta.schema_version_=100003124341;
  tenant_meta.tenant_name_ = "md_tenant";
  tenant_meta.tenant_status_ = ObTenantStatus::TENANT_STATUS_NORMAL;
  tenant_meta.charset_type_ = ObCharsetType::CHARSET_GB18030;
  tenant_meta.collation_type_ = ObCollationType::CS_TYPE_GB18030_ZH3_0900_AS_CS;
  ObLSArray ls_arr;
  EXPECT_EQ(OB_SUCCESS, ls_arr.push_back(ObLSID(1)));
  EXPECT_EQ(OB_SUCCESS, ls_arr.push_back(ObLSID(1004)));
  tenant_meta.ls_arr_.assign(ls_arr);

  const int64_t serialize_size = tenant_meta.get_serialize_size();
  DATA_DICT_LOG(INFO, "tenant_meta", K(tenant_meta), K(serialize_size));
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, tenant_meta.serialize(buf, serialize_size, pos));
  DATA_DICT_LOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictTenantMeta tenant_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, tenant_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DATA_DICT_LOG(INFO, "deserialized_meta", K(deserialize_pos), K(tenant_meta_after));
  ob_free(buf);

  EXPECT_EQ(serialize_size, deserialize_pos);
  EXPECT_EQ(OB_INVALID_TENANT_ID, tenant_meta_after.tenant_id_);
  // EXPECT_EQ(tenant_meta.tenant_id_, tenant_meta_after.tenant_id_);
  EXPECT_EQ(tenant_meta.tenant_name_, tenant_meta_after.tenant_name_);
  EXPECT_EQ(ls_arr[1], tenant_meta_after.ls_arr_[1]);
  EXPECT_TRUE(tenant_meta == tenant_meta_after);
  tenant_meta_after.ls_arr_.push_back(ObLSID(8930));
  EXPECT_FALSE(tenant_meta == tenant_meta_after);
}

TEST(ObDictDatabaseMeta, test_raw)
{
  ObArenaAllocator allocator;
  ObArenaAllocator allocator_for_deserialize;
  ObDictDatabaseMeta db_meta(&allocator);
  ObDictMetaHeader header(ObDictMetaType::DATABASE_META);
  db_meta.database_id_ = 1003030501;
  db_meta.tenant_id_ = 23412;
  db_meta.schema_version_ = 790134621334;
  db_meta.database_name_ = "md_test_db";
  db_meta.charset_type_ = ObCharsetType::CHARSET_UTF8MB4;
  db_meta.collation_type_ = ObCollationType::CS_TYPE_UTF8MB4_UNICODE_CI;
  db_meta.name_case_mode_ = ObNameCaseMode::OB_ORIGIN_AND_INSENSITIVE;
  db_meta.in_recyclebin_ = true;

  const int64_t serialize_size = db_meta.get_serialize_size();
  DATA_DICT_LOG(INFO, "db_meta", K(db_meta), K(serialize_size));
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, db_meta.serialize(buf, serialize_size, pos));
  DATA_DICT_LOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictDatabaseMeta db_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, db_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DATA_DICT_LOG(INFO, "deserialized_meta", K(deserialize_pos), K(db_meta_after));
  ob_free(buf);

  EXPECT_EQ(serialize_size, deserialize_pos);
  EXPECT_EQ(db_meta.database_id_, db_meta_after.database_id_);
  EXPECT_EQ(OB_INVALID_TENANT_ID, db_meta_after.tenant_id_);
  //EXPECT_EQ(db_meta.tenant_id_, db_meta_after.tenant_id_);
  EXPECT_EQ(db_meta.database_name_, db_meta_after.database_name_);
  EXPECT_EQ(db_meta.schema_version_, db_meta_after.schema_version_);
  EXPECT_EQ(db_meta.name_case_mode_, db_meta_after.name_case_mode_);
  EXPECT_EQ(db_meta.in_recyclebin_, db_meta_after.in_recyclebin_);
  EXPECT_EQ(db_meta.collation_type_, db_meta_after.collation_type_);
  EXPECT_TRUE(db_meta == db_meta_after);
  db_meta_after.database_id_ = 4376212345;
  EXPECT_FALSE(db_meta == db_meta_after);
}

TEST(ObDictColumnMeta, test_raw)
{
  ObArenaAllocator allocator;
  ObArenaAllocator allocator_for_deserialize;
  DictTableMetaBuilder meta_builder;
  ObDictColumnMeta col_meta(&allocator);
  ObDictMetaHeader header(ObDictMetaType::TABLE_META);
  meta_builder.build_column_meta(&col_meta);
  DATA_DICT_LOG(INFO, "build_column_meta", K(col_meta));

  const int64_t serialize_size = col_meta.get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, col_meta.serialize(buf, serialize_size, pos));
  DATA_DICT_LOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictColumnMeta col_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, col_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DATA_DICT_LOG(INFO, "deserialized_meta", K(deserialize_pos), K(col_meta_after));
  ob_free(buf);

  EXPECT_EQ(serialize_size, deserialize_pos);
  EXPECT_EQ(col_meta.column_name_, col_meta_after.column_name_);
  EXPECT_EQ(col_meta.orig_default_value_, col_meta_after.orig_default_value_);
  EXPECT_TRUE(col_meta == col_meta_after);
  ObObjMeta new_meta;
  new_meta.set_clob();
  col_meta_after.orig_default_value_.set_meta_type(new_meta);
  EXPECT_FALSE(col_meta == col_meta_after);
}

#ifndef OB_USE_ASAN
// FIFO bypasses allocation accounting in ASAN builds.
class TestDictSessionVars : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, source_allocator_.init(64 * 1024 * 1024, 0, 64 * 1024));
    ASSERT_EQ(OB_SUCCESS, allocator_.init(64 * 1024 * 1024, 0, 64 * 1024));
    source_allocator_.set_nway(1);
    allocator_.set_nway(1);
    void *ptr = allocator_.alloc(1);
    ASSERT_NE(nullptr, ptr);
    allocator_.purge();
    ASSERT_GT(allocator_.allocated(), 0);
    allocator_.free(ptr);
    allocator_.purge();
    ASSERT_EQ(0, allocator_.allocated());
  }
  void TearDown() override
  {
    source_allocator_.purge();
    allocator_.purge();
    EXPECT_EQ(0, source_allocator_.allocated());
    EXPECT_EQ(0, allocator_.allocated());
  }
  void add_local_vars(sql::ObLocalSessionVar &vars)
  {
    ObObj value;
    ASSERT_EQ(OB_SUCCESS, vars.set_local_var_capacity(2));
    value.set_int(123);
    ASSERT_EQ(OB_SUCCESS, vars.add_local_var(SYS_VAR_SQL_MODE, value));
    value.set_varchar("Asia/Shanghai");
    ASSERT_EQ(OB_SUCCESS, vars.add_local_var(SYS_VAR_TIME_ZONE, value));
  }
  ObConcurrentFIFOAllocator source_allocator_;
  ObConcurrentFIFOAllocator allocator_;
};

TEST_F(TestDictSessionVars, column_reset_and_destruct)
{
  ObDictColumnMeta source(&source_allocator_);
  ObDictColumnMeta dest(&allocator_);
  ASSERT_EQ(OB_SUCCESS, dest.assign(source));
  dest.reset();
  allocator_.purge();
  EXPECT_EQ(0, allocator_.allocated());
  add_local_vars(source.get_local_session_var());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, dest.assign(source));
    ASSERT_EQ(OB_SUCCESS, dest.assign(source));
    EXPECT_TRUE(source.get_local_session_var() == dest.get_local_session_var());
    dest.reset();
    dest.reset();
    allocator_.purge();
    EXPECT_EQ(0, allocator_.allocated());
  }

  const int64_t size = source.get_serialize_size();
  char *buf = static_cast<char *>(source_allocator_.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));
  ObDictMetaHeader header(ObDictMetaType::TABLE_META);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dest.deserialize(header, buf, size, pos));
  EXPECT_EQ(size, pos);
  EXPECT_TRUE(source.get_local_session_var() == dest.get_local_session_var());
  source_allocator_.free(buf);
  dest.reset();
  allocator_.purge();
  EXPECT_EQ(0, allocator_.allocated());
  ASSERT_EQ(OB_SUCCESS, dest.assign(source));
}

TEST_F(TestDictSessionVars, table_replace_and_destruct)
{
  ObDictTableMeta source(&source_allocator_);
  ObDictTableMeta empty(&source_allocator_);
  ObDictTableMeta dest(&allocator_);
  void *buf = source_allocator_.alloc(sizeof(ObDictColumnMeta));
  ASSERT_NE(nullptr, buf);
  source.col_metas_ = new (buf) ObDictColumnMeta(&source_allocator_);
  source.column_count_ = 1;
  add_local_vars(source.col_metas_[0].get_local_session_var());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, dest.assign(source));
    ASSERT_EQ(OB_SUCCESS, dest.assign(source));
    ASSERT_EQ(OB_SUCCESS, dest.assign(empty));
    allocator_.purge();
    EXPECT_EQ(0, allocator_.allocated());
  }
  ASSERT_EQ(OB_SUCCESS, dest.assign(source));
}

TEST_F(TestDictSessionVars, repeated_table_allocation_and_free_releases_session_vars)
{
  const int64_t COLUMN_COUNT = 2;
  const int64_t ITERATION_COUNT = 1000;
  ObDictTableMeta source(&source_allocator_);
  void *buf = source_allocator_.alloc(sizeof(ObDictColumnMeta) * COLUMN_COUNT);
  ASSERT_NE(nullptr, buf);
  source.col_metas_ = static_cast<ObDictColumnMeta *>(buf);
  for (int64_t i = 0; i < COLUMN_COUNT; ++i) {
    new (source.col_metas_ + i) ObDictColumnMeta(&source_allocator_);
    ++source.column_count_;
  }
  ASSERT_NO_FATAL_FAILURE(add_local_vars(source.col_metas_[1].get_local_session_var()));
  sql::ObSessionSysVar *source_var = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      source.col_metas_[1].get_local_session_var().get_local_var(SYS_VAR_TIME_ZONE, source_var));
  ASSERT_NE(nullptr, source_var);

  for (int64_t i = 0; i < ITERATION_COUNT; ++i) {
    buf = allocator_.alloc(sizeof(ObDictTableMeta));
    ASSERT_NE(nullptr, buf);
    ObDictTableMeta *dest = new (buf) ObDictTableMeta(&allocator_);
    ASSERT_EQ(OB_SUCCESS, dest->assign(source));
    ASSERT_EQ(COLUMN_COUNT, dest->get_column_count());
    EXPECT_EQ(0, dest->col_metas_[0].get_local_session_var().get_var_count());
    EXPECT_EQ(2, dest->col_metas_[1].get_local_session_var().get_var_count());
    EXPECT_TRUE(source.col_metas_[1].get_local_session_var()
        == dest->col_metas_[1].get_local_session_var());
    sql::ObSessionSysVar *copied_var = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        dest->col_metas_[1].get_local_session_var().get_local_var(SYS_VAR_TIME_ZONE, copied_var));
    ASSERT_NE(nullptr, copied_var);
    EXPECT_NE(source_var, copied_var);
    EXPECT_NE(source_var->val_.get_string_ptr(), copied_var->val_.get_string_ptr());

    dest->~ObDictTableMeta();
    allocator_.free(dest);
    dest = nullptr;
    allocator_.purge();
    ASSERT_EQ(0, allocator_.allocated()) << "iteration=" << i;
    EXPECT_EQ(ObString("Asia/Shanghai"), source_var->val_.get_string());
  }
}

TEST_F(TestDictSessionVars, shallow_copy_keeps_source)
{
  sql::ObLocalSessionVar source(&source_allocator_);
  add_local_vars(source);
  sql::ObSessionSysVar *source_var = nullptr;
  ASSERT_EQ(OB_SUCCESS, source.get_local_var(SYS_VAR_TIME_ZONE, source_var));
  ASSERT_NE(nullptr, source_var);
  const char *value_ptr = source_var->val_.get_string_ptr();
  {
    sql::ObLocalSessionVar borrowed(&allocator_);
    ASSERT_EQ(OB_SUCCESS, borrowed.assign(source));
    borrowed.reset();
    ASSERT_EQ(OB_SUCCESS, borrowed.assign(source));
  }
  allocator_.purge();
  EXPECT_EQ(0, allocator_.allocated());
  sql::ObSessionSysVar *var = nullptr;
  ASSERT_EQ(OB_SUCCESS, source.get_local_var(SYS_VAR_TIME_ZONE, var));
  ASSERT_EQ(source_var, var);
  EXPECT_EQ(value_ptr, var->val_.get_string_ptr());
  EXPECT_EQ(ObString("Asia/Shanghai"), var->val_.get_string());
  source.reset_and_free();
  source.reset_and_free();
}
#endif

TEST(ObObjMeta, test_objmeta_serialize)
{
  ObObjMeta objmeta;
  objmeta.set_collation_type(ObCollationType::CS_TYPE_GB18030_BIN);
  objmeta.set_collation_level(ObCollationLevel::CS_LEVEL_IGNORABLE);
  DATA_DICT_LOG(INFO, "build_objmeta", K(objmeta));

  const int64_t serialize_size = objmeta.get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, objmeta.serialize(buf, serialize_size, pos));
  DATA_DICT_LOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObObjMeta objmet_after;
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, objmet_after.deserialize(buf, serialize_size, deserialize_pos));
  DATA_DICT_LOG(INFO, "deserialized_meta", K(deserialize_pos), K(objmet_after));

  EXPECT_EQ(serialize_size, deserialize_pos);
  EXPECT_TRUE(objmeta == objmet_after);
  ob_free(buf);
}

TEST(ObRowkeyColumn, test_rowkey_col)
{
  ObRowkeyColumn rcol;
  rcol.column_id_ = 245763451346;
  rcol.length_ = 431652345;
  rcol.order_ = ObOrderType::DESC;
  rcol.fulltext_flag_ = true;
  DATA_DICT_LOG(INFO, "build_rowkey_column", K(rcol));

  const int64_t serialize_size = rcol.get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, rcol.serialize(buf, serialize_size, pos));
  DATA_DICT_LOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObRowkeyColumn rcol_after_serialize;
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, rcol_after_serialize.deserialize(buf, serialize_size, deserialize_pos));
  DATA_DICT_LOG(INFO, "deserialized_meta", K(deserialize_pos), K(rcol_after_serialize));
  ob_free(buf);

  EXPECT_EQ(serialize_size, deserialize_pos);
}

TEST(ObDictTableMeta, test_raw)
{
  ObArenaAllocator allocator_for_deserialize;
  DictTableMetaBuilder meta_builder;
  ObDictTableMeta *tb_meta;
  ObDictMetaHeader header(ObDictMetaType::TABLE_META);
  const int64_t col_count = 4000;
  const int64_t rowkey_count = 57;
  const int64_t index_column_count = 1235;
  EXPECT_EQ(OB_SUCCESS, meta_builder.build_table_meta(tb_meta, rowkey_count, col_count, index_column_count));
  EXPECT_NE(nullptr, tb_meta);
  DATA_DICT_LOG(INFO, "build_table_meta", KPC(tb_meta));
  EXPECT_EQ(rowkey_count, tb_meta->rowkey_column_count_);
  EXPECT_EQ(col_count, tb_meta->column_count_);

  const int64_t serialize_size = tb_meta->get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, tb_meta->serialize(buf, serialize_size, pos));
  DATA_DICT_LOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictTableMeta tb_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, tb_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DATA_DICT_LOG(INFO, "deserialized_meta", K(deserialize_pos), K(tb_meta_after));
  ob_free(buf);

  EXPECT_EQ(serialize_size, deserialize_pos);
  EXPECT_EQ(tb_meta->table_name_, tb_meta_after.table_name_);
  EXPECT_EQ(rowkey_count, tb_meta_after.rowkey_column_count_);
  EXPECT_EQ(col_count, tb_meta_after.column_count_);
  EXPECT_TRUE(*tb_meta == tb_meta_after);
  tb_meta_after.index_column_count_ ++;
  EXPECT_FALSE(*tb_meta == tb_meta_after);
}

} // namespace datadict
} // namespace oceanbase

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_data_dict_struct.log");
  ObLogger &logger = ObLogger::get_logger();
  bool not_output_obcdc_log = true;
  logger.set_file_name("test_data_dict_struct.log", not_output_obcdc_log, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG, DATA_DICT.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
