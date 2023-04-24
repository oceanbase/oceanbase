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

#include "gtest/gtest.h"

#include "data_dict_test_utils.h"

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
  DDLOG(INFO, "tenant_meta", K(tenant_meta), K(serialize_size));
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, tenant_meta.serialize(buf, serialize_size, pos));
  DDLOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictTenantMeta tenant_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, tenant_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DDLOG(INFO, "deserialized_meta", K(deserialize_pos), K(tenant_meta_after));
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
  DDLOG(INFO, "db_meta", K(db_meta), K(serialize_size));
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, db_meta.serialize(buf, serialize_size, pos));
  DDLOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictDatabaseMeta db_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, db_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DDLOG(INFO, "deserialized_meta", K(deserialize_pos), K(db_meta_after));
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
  DDLOG(INFO, "build_column_meta", K(col_meta));

  const int64_t serialize_size = col_meta.get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, col_meta.serialize(buf, serialize_size, pos));
  DDLOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictColumnMeta col_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, col_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DDLOG(INFO, "deserialized_meta", K(deserialize_pos), K(col_meta_after));
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

TEST(ObObjMeta, test_objmeta_serialize)
{
  ObObjMeta objmeta;
  objmeta.set_collation_type(ObCollationType::CS_TYPE_GB18030_BIN);
  objmeta.set_collation_level(ObCollationLevel::CS_LEVEL_IGNORABLE);
  DDLOG(INFO, "build_objmeta", K(objmeta));

  const int64_t serialize_size = objmeta.get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, objmeta.serialize(buf, serialize_size, pos));
  DDLOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObObjMeta objmet_after;
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, objmet_after.deserialize(buf, serialize_size, deserialize_pos));
  DDLOG(INFO, "deserialized_meta", K(deserialize_pos), K(objmet_after));

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
  DDLOG(INFO, "build_rowkey_column", K(rcol));

  const int64_t serialize_size = rcol.get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, rcol.serialize(buf, serialize_size, pos));
  DDLOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObRowkeyColumn rcol_after_serialize;
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, rcol_after_serialize.deserialize(buf, serialize_size, deserialize_pos));
  DDLOG(INFO, "deserialized_meta", K(deserialize_pos), K(rcol_after_serialize));
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
  DDLOG(INFO, "build_table_meta", KPC(tb_meta));
  EXPECT_EQ(rowkey_count, tb_meta->rowkey_column_count_);
  EXPECT_EQ(col_count, tb_meta->column_count_);

  const int64_t serialize_size = tb_meta->get_serialize_size();
  char *buf = (char*)ob_malloc(serialize_size, "test");
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, tb_meta->serialize(buf, serialize_size, pos));
  DDLOG(INFO, "serialize_info", K(serialize_size), K(pos));

  ObDictTableMeta tb_meta_after(&allocator_for_deserialize);
  int64_t deserialize_pos = 0;
  EXPECT_EQ(OB_SUCCESS, tb_meta_after.deserialize(header, buf, serialize_size, deserialize_pos));
  DDLOG(INFO, "deserialized_meta", K(deserialize_pos), K(tb_meta_after));
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
