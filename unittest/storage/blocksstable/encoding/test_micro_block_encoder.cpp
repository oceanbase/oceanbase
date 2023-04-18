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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/encoding/ob_micro_block_encoder.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/ob_i_store.h"
#include "lib/string/ob_sql_string.h"
#include "../ob_row_generate.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

enum EncodeTestCase
{
  FORCE_RAW_ON_SPAN_COLUMN_WITH_NULL,
  MAX_TEST_CASE,
};

class TestIColumnEncoder : public ::testing::Test
{
public:
  TestIColumnEncoder(const bool is_multi_version_row = false)
    : is_multi_version_row_(is_multi_version_row), tenant_ctx_(OB_SERVER_TENANT_ID)
  {
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
  }
  virtual ~TestIColumnEncoder() {}
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  ObObjType *col_types_;
  ObRowGenerate row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  ObTableReadInfo read_info_;
  ObArenaAllocator allocator_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  bool is_multi_version_row_;
  share::ObTenantBase tenant_ctx_;
};

void TestIColumnEncoder::SetUp()
{
  const int64_t tid = 200001;
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(1);
  table.set_database_id(1);
  table.set_table_id(tid);
  table.set_table_name("test_micro_decoder_schema");
  table.set_rowkey_column_num(rowkey_cnt_);
  table.set_max_column_id(column_cnt_ * 2);

  ObSqlString str;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    col.reset();
    col.set_table_id(tid);
    col.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    str.assign_fmt("test%ld", i);
    col.set_column_name(str.ptr());
    ObObjType type = col_types_[i];
    col.set_data_type(type);
    col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if (type == ObIntType) {
      col.set_rowkey_position(1);
    } else{
      col.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table.add_column(col));
  }

  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table, is_multi_version_row_));
  if (is_multi_version_row_) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_multi_version_column_descs(col_descs_));
  } else {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(col_descs_));
  }
  ASSERT_EQ(OB_SUCCESS, read_info_.init(allocator_,
                                      row_generate_.get_schema().get_column_count(),
                                      row_generate_.get_schema().get_rowkey_column_num(),
                                      lib::is_oracle_mode(),
                                      col_descs_,
                                      true));

  ctx_.micro_block_size_ = 1L << 20; // 1MB, maximum micro block size;
  ctx_.macro_block_size_ = 2L << 20;
  ctx_.rowkey_column_cnt_ = rowkey_cnt_;
  ctx_.column_cnt_ = is_multi_version_row_ ? column_cnt_ + 2 : column_cnt_;
  ctx_.col_descs_ = &col_descs_;
  ctx_.major_working_cluster_version_=cal_version(3, 1, 0, 0);
  ctx_.row_store_type_ = common::ENCODING_ROW_STORE;
}

static const int64_t test_encoder_overflow = 3;
static ObObjType test_encoder_over_col_types[3] = {ObIntType, ObTimestampTZType, ObTextType};
class TestEncoderOverFlow : public TestIColumnEncoder
{
public:
  TestEncoderOverFlow()
  {
    rowkey_cnt_ = 1;
    column_cnt_ = test_encoder_overflow;
    col_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
    for (int64_t i = 0; i < column_cnt_; ++i) {
      col_types_[i] = test_encoder_over_col_types[i];
    }
  }
  virtual ~TestEncoderOverFlow()
  {
    allocator_.free(col_types_);
  }
};

TEST_F(TestEncoderOverFlow, test_append_row_with_timestamp_and_max_estimate_limit)
{
  common::ObClusterVersion::get_instance().update_cluster_version(cal_version(2, 2, 0, 75));
  ASSERT_TRUE(IS_CLUSTER_VERSION_AFTER_2274);
  if (IS_CLUSTER_VERSION_AFTER_2274) {
    ObMicroBlockEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));

    encoder.estimate_size_limit_ = ctx_.macro_block_size_;

    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, row.init(allocator_, column_cnt_));
    int ret = OB_SUCCESS;
    int row_cnt = 0;
    // calculated max size 4444 for this schema
    while(row_cnt < 4443) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
      ret = encoder.append_row(row);
      row_cnt++;
    }
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    row.storage_datums_[2].len_ = 105000;
    encoder.append_row(row);

    LOG_INFO("Data buffer size", K(encoder.data_buffer_),
        K(row_cnt), K(encoder.estimate_size_limit_), K(encoder.estimate_size_),
        K(encoder.length_));

    char *buf = NULL;
    int64_t size = 0;
    ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, size));
  }
}

static ObObjType test_dict_large_varchar[2] = {ObIntType, ObVarcharType};
class TestDictLargeVarchar : public TestIColumnEncoder
{
public:
  TestDictLargeVarchar() : TestIColumnEncoder(true)
  {
    rowkey_cnt_ = 1;
    column_cnt_ = 2;
    col_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
    for (int64_t i = 0; i < column_cnt_; ++i) {
      col_types_[i] = test_dict_large_varchar[i];
    }
  }
  virtual ~TestDictLargeVarchar()
  {
    allocator_.free(col_types_);
  }

  int64_t full_column_cnt_ = 4;
};

TEST_F(TestDictLargeVarchar, test_dict_large_varchar)
{
  ctx_.column_encodings_ = static_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * full_column_cnt_));
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    ctx_.column_encodings_[i] = ObColumnHeader::Type::DICT;
  }
  ObMicroBlockEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  const int64_t varchar_data_size = UINT16_MAX * 2 + 1;
  char *varchar_data = static_cast<char *>(allocator_.alloc(varchar_data_size));
  ASSERT_TRUE(nullptr != varchar_data);
  MEMSET(varchar_data, 7, varchar_data_size);
  row.storage_datums_[0].set_int(1);
  row.storage_datums_[3].set_string(varchar_data, varchar_data_size);

  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));

  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, size));

  ObMicroBlockData micro_data(buf, size);
  ObMicroBlockDecoder decoder;
  ObDatumRow read_row;
  ASSERT_EQ(OB_SUCCESS, read_row.init(full_column_cnt_));
  ASSERT_EQ(OB_SUCCESS, decoder.init(micro_data, read_info_));
  ASSERT_EQ(OB_SUCCESS, decoder.get_row(0, read_row));
  STORAGE_LOG(INFO, "[Salton]", K(read_row));

  ASSERT_EQ(row.storage_datums_[3].len_, read_row.storage_datums_[3].len_);
  ASSERT_TRUE(ObDatum::binary_equal(row.storage_datums_[3], read_row.storage_datums_[3]));
}

class TestEncodingRowBufHolder : public ::testing::Test
{
public:
  TestEncodingRowBufHolder() {}
  virtual ~TestEncodingRowBufHolder() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestEncodingRowBufHolder, test_encoding_row_buf_holder)
{
  ObEncodingRowBufHolder buf_holder;
  ASSERT_EQ(OB_NOT_INIT, buf_holder.try_alloc(12345));
  ASSERT_EQ(OB_SUCCESS, buf_holder.init(OB_DEFAULT_MACRO_BLOCK_SIZE));
  ASSERT_EQ(OB_INVALID_ARGUMENT, buf_holder.try_alloc(4 * OB_DEFAULT_MACRO_BLOCK_SIZE));
  const char *test_mark = "fly me to the moon";
  
  const int test_mark_len = strlen(test_mark);
  const int64_t first_alloc_size = 4096;
  const int64_t snd_alloc_size = static_cast<int64_t>(first_alloc_size * 1.2);
  const int64_t trd_alloc_size = first_alloc_size * 10;
  ASSERT_EQ(OB_SUCCESS, buf_holder.try_alloc(first_alloc_size));
  MEMCPY(buf_holder.get_buf(), test_mark, test_mark_len);
  const char *first_alloc_buf = buf_holder.get_buf();

  ASSERT_EQ(OB_SUCCESS, buf_holder.try_alloc(snd_alloc_size));
  ASSERT_EQ(first_alloc_buf, buf_holder.get_buf());
  int cmp_ret = MEMCMP(test_mark, buf_holder.get_buf(), test_mark_len);
  ASSERT_EQ(cmp_ret, 0);
  ASSERT_EQ(buf_holder.alloc_size_, first_alloc_size * ObEncodingRowBufHolder::EXTRA_MEM_FACTOR);

  ASSERT_EQ(OB_SUCCESS, buf_holder.try_alloc(trd_alloc_size));
  ASSERT_EQ(buf_holder.alloc_size_, trd_alloc_size * ObEncodingRowBufHolder::EXTRA_MEM_FACTOR);

  buf_holder.reset();
  ASSERT_EQ(buf_holder.allocator_.arena_.used_, 0);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_micro_block_encoder.log*");
  OB_LOGGER.set_file_name("test_micro_block_encoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
