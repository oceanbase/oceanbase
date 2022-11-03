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

#include <gtest/gtest.h>
#include <codecvt>
#define private public
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_bloom_filter_data_writer.h"
#include "storage/blocksstable/ob_bloom_filter_data_reader.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "ob_row_generate.h"
#include "ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"
#undef private
namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share::schema;
static ObSimpleMemLimitGetter getter;

class TestBloomFilterDataReaderWriter : public TestDataFilePrepare
{
public:
  TestBloomFilterDataReaderWriter();
  virtual ~TestBloomFilterDataReaderWriter();
  virtual void SetUp();
  virtual void TearDown();
protected:
  int prepare_rowkey(ObDatumRowkey &rowkey, const int64_t rowkey_column_cnt = TEST_ROWKEY_COLUMN_CNT);
  void prepare_schema();
  int prepare_bloom_filter_cache(ObBloomFilterCacheValue &bf_cache_value,
                                 const int64_t rowkey_column_cnt = TEST_ROWKEY_COLUMN_CNT,
                                 const int64_t row_count = TEST_ROW_COUNT,
                                 const int64_t max_row_count = TEST_ROW_COUNT * 10);
  int prepare_bloom_filter_cache(ObBloomFilterDataWriter &bf_writer,
                                 const int64_t row_count = TEST_ROW_COUNT);
  int check_bloom_filter_cache(const ObBloomFilterCacheValue &bf_cache_value,
                               const ObBloomFilterCacheValue &bf_cache_value2);
  int check_bloom_filter_cache(const ObBloomFilterCacheValue &new_bf_cache_value,
                               const ObBloomFilterCacheValue &bf_cache_value,
                               const ObBloomFilterCacheValue &bf_cache_value2);
protected:
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  static const int64_t TEST_ROW_COUNT = 100;
  common::ObArenaAllocator allocator_;
  ObDatumRow row_;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  uint16_t column_id_;
};

TestBloomFilterDataReaderWriter::TestBloomFilterDataReaderWriter()
  : TestDataFilePrepare(&getter, "TestBloomFilterDataReaderWriter", 2 * 1024 * 1024, 100),
    allocator_(ObModIds::TEST), table_schema_(), row_generate_(),
    column_id_(0)
{
}

TestBloomFilterDataReaderWriter::~TestBloomFilterDataReaderWriter()
{
}

void TestBloomFilterDataReaderWriter::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();
  prepare_schema();
  row_generate_.reset();
  ret = row_generate_.init(table_schema_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, row_.init(allocator_, TEST_COLUMN_CNT));
  static ObTenantBase tenant_ctx(1);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
}

void TestBloomFilterDataReaderWriter::TearDown()
{
  row_generate_.reset();
  table_schema_.reset();
  TestDataFilePrepare::TearDown();
}

void TestBloomFilterDataReaderWriter::prepare_schema()
{
  ObColumnSchemaV2 column;
  uint64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_bloom_filter_data"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(ENCODING_ROW_STORE);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  column_id_ = 28;
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i+1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_data_length(1);
    column.set_order_in_rowkey(ObOrderType::ASC);
    if(obj_type == common::ObIntType){
      column.set_rowkey_position(1);
    } else if(obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

int TestBloomFilterDataReaderWriter::prepare_rowkey(ObDatumRowkey &rowkey, const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_generate_.get_next_row(row_))) {
    STORAGE_LOG(WARN, "fail to get next row", K(ret));
  } else {
    rowkey.assign(row_.storage_datums_, rowkey_column_cnt);
  }
  return ret;
}

int TestBloomFilterDataReaderWriter::prepare_bloom_filter_cache(ObBloomFilterCacheValue &bf_cache_value,
                                                                const int64_t rowkey_column_cnt,
                                                                const int64_t row_count,
                                                                const int64_t max_row_count)
{
  int ret = OB_SUCCESS;

  bf_cache_value.reset();
  ObDataStoreDesc desc;
  desc.init(table_schema_, 1, MINOR_MERGE);
  if (OB_FAIL(bf_cache_value.init(rowkey_column_cnt, max_row_count))) {
    STORAGE_LOG(WARN, "Failed to init bloomfilter cache value", K(ret));
  } else {
    ObDatumRowkey rowkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      uint64_t key_hash = 0;
      if (OB_FAIL(prepare_rowkey(rowkey, rowkey_column_cnt))) {
        STORAGE_LOG(WARN, "Failed to prepare rowkey", K(ret));
      } else if (OB_FAIL(rowkey.murmurhash(0, desc.datum_utils_, key_hash))) {
        STORAGE_LOG(WARN, "Failed to calc rowkey hash ", K(ret), K(rowkey));
      } else if (OB_FAIL(bf_cache_value.insert(key_hash))) {
        STORAGE_LOG(WARN, "Failed to insert rowkey to bloom filter cache", K(rowkey), K(ret));
      }
    }
  }

  return ret;
}

int TestBloomFilterDataReaderWriter::prepare_bloom_filter_cache(ObBloomFilterDataWriter &bf_writer,
                                                                const int64_t row_count)
{
  int ret = OB_SUCCESS;

  ObDatumRowkey rowkey;
  ObDataStoreDesc desc;
  desc.init(table_schema_, 1, MINOR_MERGE);
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(prepare_rowkey(rowkey))) {
      STORAGE_LOG(WARN, "Failed to prepare rowkey", K(ret));
    } else if (OB_FAIL(bf_writer.append(rowkey, desc.datum_utils_))) {
      STORAGE_LOG(WARN, "Failed to insert rowkey to bloom filter cache", K(rowkey), K(ret));
    }
  }

  return ret;
}

int TestBloomFilterDataReaderWriter::check_bloom_filter_cache(const ObBloomFilterCacheValue &bf_cache_value,
                                                              const ObBloomFilterCacheValue &bf_cache_value2)
{
  int ret = OB_SUCCESS;

  if (!bf_cache_value.is_valid() || !bf_cache_value2.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else if (!bf_cache_value.could_merge_bloom_filter(bf_cache_value2)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else if (bf_cache_value.get_prefix_len() != bf_cache_value2.get_prefix_len()
      || bf_cache_value.get_row_count() != bf_cache_value2.get_row_count()
      || bf_cache_value.get_nhash() != bf_cache_value2.get_nhash()
      || bf_cache_value.get_nbit() != bf_cache_value2.get_nbit()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else {
    int64_t nbytes = bf_cache_value.get_nbytes();
    for (int64_t i = 0; OB_SUCC(ret) && i < nbytes; i++) {
      if (bf_cache_value.get_bloom_filter_bits()[i] != bf_cache_value2.get_bloom_filter_bits()[i]) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid bloom filter cache", K(i), K(nbytes), K(bf_cache_value), K(bf_cache_value2), K(ret));
      }
    }
  }

  return ret;
}

int TestBloomFilterDataReaderWriter::check_bloom_filter_cache(const ObBloomFilterCacheValue &new_bf_cache_value,
                                                             const ObBloomFilterCacheValue &bf_cache_value,
                                                             const ObBloomFilterCacheValue &bf_cache_value2)
{
  int ret = OB_SUCCESS;

  if (!new_bf_cache_value.is_valid() || !bf_cache_value.is_valid() || !bf_cache_value2.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(new_bf_cache_value), K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else if (!new_bf_cache_value.could_merge_bloom_filter(bf_cache_value)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(new_bf_cache_value), K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else if (!bf_cache_value.could_merge_bloom_filter(bf_cache_value2)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(new_bf_cache_value), K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else if (new_bf_cache_value.get_prefix_len() != bf_cache_value.get_prefix_len()
      || new_bf_cache_value.get_nhash() != bf_cache_value.get_nhash()
      || new_bf_cache_value.get_nbit() != bf_cache_value.get_nbit()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(new_bf_cache_value), K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else if (bf_cache_value.get_prefix_len() != bf_cache_value2.get_prefix_len()
      || bf_cache_value.get_nhash() != bf_cache_value2.get_nhash()
      || bf_cache_value.get_nbit() != bf_cache_value2.get_nbit()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(new_bf_cache_value), K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else if (new_bf_cache_value.get_row_count() != bf_cache_value.get_row_count() + bf_cache_value2.get_row_count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid bloom filter cache", K(new_bf_cache_value), K(bf_cache_value), K(bf_cache_value2), K(ret));
  } else {
    int64_t nbytes = new_bf_cache_value.get_nbytes();
    for (int64_t i = 0; OB_SUCC(ret) && i < nbytes; i++) {
      if (new_bf_cache_value.get_bloom_filter_bits()[i] !=
            (bf_cache_value.get_bloom_filter_bits()[i] | bf_cache_value2.get_bloom_filter_bits()[i])) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid bloom filter cache", K(i), K(nbytes), K(new_bf_cache_value.get_bloom_filter_bits()[i]), K(bf_cache_value.get_bloom_filter_bits()[i]), K(bf_cache_value2.get_bloom_filter_bits()[i]), K(ret));
      }
    }
  }

  return ret;
}


TEST_F(TestBloomFilterDataReaderWriter, test_cache_value_serial)
{
  int ret = OB_SUCCESS;
  int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  int64_t pos = 0;

  ObBloomFilterCacheValue bf_cache_value;
  ObBloomFilterCacheValue bf_cache_value2;

  ret = prepare_bloom_filter_cache(bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);

  char *buf = (char *)allocator_.alloc(macro_block_size);
  ASSERT_TRUE(NULL != buf);

  ret = bf_cache_value.serialize(buf, macro_block_size, pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  pos = 0;
  ret = bf_cache_value2.deserialize(buf, macro_block_size, pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = check_bloom_filter_cache(bf_cache_value, bf_cache_value2);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBloomFilterDataReaderWriter, test_cache_value_merge)
{
  int ret = OB_SUCCESS;

  ObBloomFilterCacheValue bf_cache_value;
  ObBloomFilterCacheValue bf_cache_value2;
  ObBloomFilterCacheValue tmp_bf_cache_value;

  ret = prepare_bloom_filter_cache(bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = prepare_bloom_filter_cache(bf_cache_value2);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = bf_cache_value.deep_copy(tmp_bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(bf_cache_value.could_merge_bloom_filter(bf_cache_value2));

  ret = bf_cache_value.merge_bloom_filter(bf_cache_value2);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = check_bloom_filter_cache(bf_cache_value, bf_cache_value2, tmp_bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBloomFilterDataReaderWriter, test_writer_reader)
{
  int ret = OB_SUCCESS;
  ObBloomFilterDataWriter writer;
  ObBloomFilterDataReader reader;
  ObDataStoreDesc desc;
  ObDatumRowkey rowkey;
  ObArray<MacroBlockId> macro_blocks;
  ObBloomFilterCacheValue bf_cache_value;
  ObBloomFilterCacheValue read_bf_cache_value;

  ret = desc.init(table_schema_, 1, MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.init(desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.flush_bloom_filter();
  ASSERT_NE(OB_SUCCESS, ret);

  ret = prepare_rowkey(rowkey, TEST_ROWKEY_COLUMN_CNT + 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.append(rowkey, desc.datum_utils_);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = prepare_rowkey(rowkey);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.append(rowkey, desc.datum_utils_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = prepare_bloom_filter_cache(bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = writer.append(bf_cache_value);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = prepare_bloom_filter_cache(bf_cache_value, TEST_ROWKEY_COLUMN_CNT + 1);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = writer.append(bf_cache_value);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = prepare_bloom_filter_cache(bf_cache_value, TEST_ROWKEY_COLUMN_CNT, TEST_ROW_COUNT, ObBloomFilterDataWriter::BLOOM_FILTER_MAX_ROW_COUNT);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = writer.append(bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = writer.flush_bloom_filter();
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = reader.read_bloom_filter(writer.get_block_write_ctx().get_macro_block_list().at(0), read_bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(read_bf_cache_value.is_valid());
  EXPECT_EQ(bf_cache_value.get_row_count() + 1, read_bf_cache_value.get_row_count());
}

/*TEST_F(TestBloomFilterDataReaderWriter, test_normal)
{
  int ret = OB_SUCCESS;
  ObBloomFilterDataWriter writer;
  ObBloomFilterDataReader reader;
  ObDataStoreDesc desc;
  int64_t data_version = 1;
  ObStoreRowkey rowkey;
  ObBloomFilterCacheValue bf_cache_value;

  ret = desc.init(table_schema_, data_version, desc.default_encoder_opt(), NULL, 1,
      MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.init(desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = prepare_bloom_filter_cache(writer);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.flush_bloom_filter();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMacroBlocksWriteCtx &block_write_ctx = writer.get_block_write_ctx();
  ASSERT_TRUE(block_write_ctx.macro_block_list_.count() == 1);

  ObMacroBlockCtx bf_block_ctx;
  ret = prepare_block_ctx(block_write_ctx.macro_block_list_.at(0), bf_block_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.read_bloom_filter(bf_block_ctx, bf_cache_value);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = check_bloom_filter_cache(writer.get_bloomfilter_cache_value(), bf_cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);
}*/
}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_bloom_filter_data.log*");
  OB_LOGGER.set_file_name("test_bloom_filter_data.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_sstable");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
