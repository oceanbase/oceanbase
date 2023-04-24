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

#define private public
#include "logservice/data_dictionary/ob_data_dict_storager.h"
#include "logservice/data_dictionary/ob_data_dict_iterator.h"
#undef private


using namespace oceanbase;
using namespace common;
using namespace share;
using namespace share::schema;

namespace oceanbase
{
namespace datadict
{

class TestDictStorage : public ObDataDictStorage
{
public:
  TestDictStorage(ObIAllocator &allocator)
    : ObDataDictStorage(allocator),
      all_palf_buf_(NULL),
      all_palf_len_(0),
      all_palf_pos_(0)
  {
    tenant_id_ = OB_SYS_TENANT_ID;
    snapshot_scn_.convert_for_gts(get_timestamp());
    all_palf_buf_ = static_cast<char*>(ob_malloc(8 * _M_, "datadict_test")); // 8M shoule be enough for test
    palf_buf_ = static_cast<char*>(ob_dict_malloc(DEFAULT_PALF_BUF_SIZE, tenant_id_));
    dict_buf_ = static_cast<char*>(ob_dict_malloc(DEFAULT_DICT_BUF_SIZE, tenant_id_));
  }
  ~TestDictStorage()
  {
    ob_free(all_palf_buf_);
  }
public:
  int next_palf_buf_for_iterator_(char *&buf, int64_t &buf_len)
  {
    int ret = OB_SUCCESS;
    if (all_palf_len_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(ERROR, "all_palf_len_ shoule be valid", KR(ret), K_(all_palf_len));
    } else if (all_palf_pos_ >= all_palf_len_) {
      ret = OB_ITER_END;
    } else {
      buf = all_palf_buf_ + all_palf_pos_;
      int64_t tmp_palf_pos = all_palf_pos_ + 2 * _M_;
      if (tmp_palf_pos > all_palf_len_) {
        buf_len = all_palf_len_ - all_palf_pos_;
        all_palf_pos_ = all_palf_len_;
      } else {
        buf_len = 2 * _M_;
        all_palf_pos_ = tmp_palf_pos;
      }
      if (buf_len <= 0) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(ERROR, "buf_len shoule be valid", KR(ret), K(buf_len), K_(all_palf_len),
            K_(all_palf_pos), K(tmp_palf_pos));
      }
    }
    return ret;
  }
public:
  int submit_to_palf_() override
  {
    DDLOG(DEBUG, "submit_to_palf_", K_(palf_pos), K_(all_palf_len), K_(all_palf_pos));
    MEMCPY(all_palf_buf_ + all_palf_len_, palf_buf_, palf_pos_);
    all_palf_len_ += palf_pos_;
    palf_pos_ = 0;
    return OB_SUCCESS;
  }
public:
  // public for test.
  char *all_palf_buf_;
  int64_t all_palf_len_;
  int64_t all_palf_pos_;
};

TEST(ObDataDictStorage, test_storage_in_palf)
{
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader base_header;
  ObArenaAllocator allocator;
  DictTableMetaBuilder tb_meta_builder;
  TestDictStorage storage(allocator);
  ObDictTableMeta *tb_meta = NULL;
  int64_t rowkey_count = 100;
  int64_t col_count = 400;
  int64_t index_count = 100;
  EXPECT_EQ(OB_SUCCESS, tb_meta_builder.build_table_meta(tb_meta, rowkey_count, col_count, index_count));
  ObDictMetaHeader header(ObDictMetaType::TABLE_META);
  storage.handle_dict_meta(*tb_meta, header);
  int64_t total_serialize_size = header.get_serialize_size()
      + base_header.get_serialize_size()
      + tb_meta->get_serialize_size();
  EXPECT_EQ(storage.palf_pos_, total_serialize_size);
  EXPECT_EQ(OB_SUCCESS, storage.submit_to_palf_());
  EXPECT_EQ(storage.all_palf_len_, total_serialize_size);
  EXPECT_TRUE(header.is_valid());
  // test iterator
  ObDataDictIterator iterator;

  if (OB_FAIL(iterator.init(OB_SYS_TENANT_ID))) {
    DDLOG(ERROR, "iterator init failed", KR(ret));
  }
  char *itr_buf = NULL;
  int64_t itr_buf_len = 0;
  ObDictMetaHeader header_after;
  //ObDictMetaHeader header_after(ObDictMetaType::INVALID_META);
  ObDictTableMeta tb_meta_after(&allocator);
  while (OB_SUCC(ret)) {
    if (OB_FAIL(storage.next_palf_buf_for_iterator_(itr_buf, itr_buf_len))) {
      if (OB_ITER_END != ret) {
        DDLOG(ERROR, "next_palf_buf_for_iterator_ ", KR(ret), K(itr_buf_len));
      }
    } else if (OB_FAIL(iterator.append_log_buf_with_base_header_(itr_buf, itr_buf_len))) {
      DDLOG(ERROR, "append_log_buf for test failed", KR(ret));
    } else if (OB_FAIL(iterator.next_dict_header(header_after))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        DDLOG(ERROR, "next_dict_header failed", KR(ret), K(header));
      }
    } else if (OB_FAIL(iterator.next_dict_entry(header, tb_meta_after))) {
      DDLOG(ERROR, "next_dict_entry failed", KR(ret), K(header));
    } else {
      EXPECT_TRUE(*tb_meta == tb_meta_after);
      EXPECT_TRUE(header == header_after);
    }
  }
  EXPECT_EQ(OB_ITER_END, ret);
  EXPECT_EQ(header.get_storage_type(), ObDictMetaStorageType::FULL);
}

TEST(ObDataDictStorage, test_storage_in_dict)
{
  DDLOG(INFO, "test_storage_in_dict");
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  logservice::ObLogBaseHeader base_header;
  DictTableMetaBuilder tb_meta_builder;
  TestDictStorage storage(allocator);
  ObDictTableMeta *tb_meta = NULL;
  int64_t rowkey_count = 100;
  int64_t col_count = 40000;
  int64_t index_count = 100;
  EXPECT_EQ(OB_SUCCESS, tb_meta_builder.build_table_meta(tb_meta, rowkey_count, col_count, index_count));
  ObDictMetaHeader header(ObDictMetaType::TABLE_META);
  storage.handle_dict_meta(*tb_meta, header);
  int64_t tb_meta_size = tb_meta->get_serialize_size();
  int64_t header_size = header.get_serialize_size();
  int64_t block_size = 2 * _M_;
  int64_t seg_cnt = tb_meta_size / block_size + 1;
  int64_t total_serialize_size = seg_cnt * (base_header.get_serialize_size() + header_size) + tb_meta_size;
  DDLOG(DEBUG, "serialize tb_meta", K(seg_cnt), K(header_size), K(tb_meta_size),
      K(total_serialize_size), K(block_size), K(header), KPC(tb_meta));
  EXPECT_TRUE(header.is_valid());
  EXPECT_EQ(storage.dict_pos_, tb_meta_size);
  EXPECT_EQ(OB_SUCCESS, storage.submit_to_palf_());
  EXPECT_EQ(storage.all_palf_len_, total_serialize_size);
  EXPECT_EQ(header.get_storage_type(), ObDictMetaStorageType::LAST);
  // test iterator
  ObDataDictIterator iterator;

  if (OB_FAIL(iterator.init(OB_SYS_TENANT_ID))) {
    DDLOG(ERROR, "iterator init failed", KR(ret));
  }
  char *itr_buf = NULL;
  int64_t itr_buf_len = 0;
  ObDictMetaHeader header_after;
  //ObDictMetaHeader header_after(ObDictMetaType::INVALID_META);
  ObDictTableMeta tb_meta_after(&allocator);
  while (OB_SUCC(ret)) {
    header_after.reset();
    if (OB_FAIL(storage.next_palf_buf_for_iterator_(itr_buf, itr_buf_len))) {
      if (OB_ITER_END != ret) {
        DDLOG(ERROR, "next_palf_buf_for_iterator_ ", KR(ret), K(itr_buf_len));
      }
    } else if (OB_FAIL(iterator.append_log_buf_with_base_header_(itr_buf, itr_buf_len))) {
      DDLOG(ERROR, "append_log_buf for test failed", KR(ret));
    } else if (OB_FAIL(iterator.next_dict_header(header_after))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        DDLOG(ERROR, "next_dict_header failed", KR(ret), K(header));
      }
    } else if (OB_FAIL(iterator.next_dict_entry(header, tb_meta_after))) {
      DDLOG(ERROR, "next_dict_entry failed", KR(ret), K(header));
    } else {
      EXPECT_TRUE(*tb_meta == tb_meta_after);
      EXPECT_TRUE(header == header_after);
    }
  }
  EXPECT_EQ(OB_ITER_END, ret);
}

TEST(ObDataDictStorage, test_empty_schema)
{
  DDLOG(INFO, "test_empty_schema");
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DictTest");
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObSEArray<const ObTenantSchema*, 8> tenant_schemas;
  ObSEArray<const ObDatabaseSchema*, 8> database_schemas;
  ObSEArray<const ObTableSchema*, 8> table_schemas;
  EXPECT_EQ(OB_SUCCESS, ObDataDictStorage::gen_and_serialize_dict_metas(
      allocator, tenant_schemas, database_schemas, table_schemas, buf, buf_len, pos));
  EXPECT_TRUE(buf_len > 0);
  EXPECT_TRUE(pos > 0);
  EXPECT_TRUE(buf_len >= pos);
  EXPECT_EQ(0, strcmp(buf, "ddl_trans commit"));
  DDLOG(INFO, "generate meta buf", KCSTRING(buf), K(buf_len), K(pos));
  ObSEArray<const ObDictTenantMeta*, 8> tenant_metas;
  ObSEArray<const ObDictDatabaseMeta*, 8> db_metas;
  ObSEArray<const ObDictTableMeta*, 8> tb_metas;
  EXPECT_EQ(OB_SUCCESS, ObDataDictStorage::parse_dict_metas(allocator, buf, pos, 0, tenant_metas, db_metas, tb_metas));
  EXPECT_EQ(0, tenant_metas.count());
  EXPECT_EQ(0, db_metas.count());
  EXPECT_EQ(0, tb_metas.count());
}

TEST(ObDataDictStorage, test_schema_storage)
{
  DDLOG(INFO, "test_schema_storage");
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DictTest");
  ObMockSchemaBuilder schema_builder;
  int expected_tenant_cnt = 10000;
  int expected_db_cnt = 100000;
  int expected_tb_cnt = 0; // cause MockSchemaBuilder can't build table_schema currently.
  ObSEArray<const ObTenantSchema*, 8> tenant_schemas;
  ObSEArray<const ObDatabaseSchema*, 8> database_schemas;
  ObSEArray<const ObTableSchema*, 8> table_schemas;
  ObTenantSchema *tenant_schema = NULL;
  for (int i = 0; i < expected_tenant_cnt; i++) {
    tenant_schema = static_cast<ObTenantSchema*>(allocator.alloc(sizeof(ObTableSchema)));
    EXPECT_TRUE(NULL != tenant_schema);
    new(tenant_schema) ObTableSchema();
    tenant_schema->reset();
    EXPECT_EQ(OB_SUCCESS, schema_builder.build_tenant_schema(*tenant_schema));
    EXPECT_EQ(OB_SUCCESS, tenant_schemas.push_back(tenant_schema));
  }
  ObDatabaseSchema *database_schema = NULL;
  for (int i = 0; i < expected_db_cnt; i++) {
    database_schema = static_cast<ObDatabaseSchema*>(allocator.alloc(sizeof(ObDatabaseSchema)));
    EXPECT_TRUE(NULL != database_schema);
    new(database_schema) ObDatabaseSchema();
    database_schema->reset();
    EXPECT_EQ(OB_SUCCESS, schema_builder.build_database_schema(*database_schema));
    EXPECT_EQ(OB_SUCCESS, database_schemas.push_back(database_schema));
  }
  //for (int i = 0; i < expected_tb_cnt; i++) {
  //ObTableSchema table_schema;
  //EXPECT_EQ(OB_SUCCESS, schema_builder.build_table_schema(table_schema));
  //EXPECT_EQ(OB_SUCCESS, table_schemas.push_back(&table_schema));
  //}
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, ObDataDictStorage::gen_and_serialize_dict_metas(
      allocator, tenant_schemas, database_schemas, table_schemas, buf, buf_len, pos));
  EXPECT_TRUE(buf_len > 0);
  EXPECT_TRUE(pos > 0);
  EXPECT_TRUE(buf_len >= pos);
  EXPECT_TRUE(buf_len > 2 * _M_);
  DDLOG(INFO, "generate meta buf", KP(buf), K(buf_len), K(pos));
  ObSEArray<const ObDictTenantMeta*, 8> tenant_metas;
  ObSEArray<const ObDictDatabaseMeta*, 8> db_metas;
  ObSEArray<const ObDictTableMeta*, 8> tb_metas;
  EXPECT_EQ(OB_SUCCESS, ObDataDictStorage::parse_dict_metas(allocator, buf, pos, 0, tenant_metas, db_metas, tb_metas));
  EXPECT_EQ(expected_tenant_cnt, tenant_metas.count());
  EXPECT_EQ(expected_db_cnt, db_metas.count());
  EXPECT_EQ(expected_tb_cnt, tb_metas.count());
}

} // namespace datadict
} // namespace oceanbase

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_data_dict_storager_iterator.log");
  ObLogger &logger = ObLogger::get_logger();
  bool not_output_obcdc_log = true;
  logger.set_file_name("test_data_dict_storager_iterator.log", not_output_obcdc_log, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG, DATA_DICT.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
