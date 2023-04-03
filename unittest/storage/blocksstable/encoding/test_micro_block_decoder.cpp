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

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class TestMicroBlockDecoder : public ::testing::Test
{
public:
  static const int64_t ROWKEY_CNT = 2;
  static const int64_t COLUMN_CNT = ObExtendType - 1 + 4;
  static const int64_t ROW_CNT = 64;

  virtual void SetUp();
  virtual void TearDown() {}

protected:
  ObRowGenerate row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  ObMicroBlockEncoder encoder_;
  ObTableReadInfo read_info_;
  ObArenaAllocator allocator_;
};

void TestMicroBlockDecoder::SetUp()
{
  const int64_t tid = combine_id(1, 50001);
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(combine_id(1, 1));
  table.set_database_id(combine_id(1, 1));
  table.set_table_id(tid);
  table.set_table_name("test_micro_decoder_schema");
  table.set_rowkey_column_num(ROWKEY_CNT);
  table.set_max_column_id(COLUMN_CNT * 2);

  ObSqlString str;
  for (int64_t i = 0; i < COLUMN_CNT; ++i) {
    col.reset();
    col.set_table_id(tid);
    col.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    str.assign_fmt("test%ld", i);
    col.set_column_name(str.ptr());
    ObObjType type = static_cast<ObObjType>(i + 1); // 0 is ObNullType
    if (COLUMN_CNT - 1 == i) { // test urowid for last column
      type = ObURowIDType;
    } else if (type >= ObExtendType) {
      type = ObVarcharType;
    }
    col.set_data_type(type);
    col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if (type == ObIntType) {
      col.set_rowkey_position(1);
    } else if (type == ObUInt64Type) {
      col.set_rowkey_position(2);
    } else{
      col.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table.add_column(col));
  }

  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(col_descs_));
  ASSERT_EQ(OB_SUCCESS, read_info_.init(allocator_,
                                      row_generate_.get_schema().get_schema_version(),
                                      row_generate_.get_schema().get_rowkey_column_num(),
                                      lib::is_oracle_mode(),
                                      col_descs_));

  ctx_.micro_block_size_ = 64L << 10;
  ctx_.macro_block_size_ = 2L << 20;
  ctx_.rowkey_column_cnt_ = ROWKEY_CNT;
  ctx_.column_cnt_ = COLUMN_CNT;
  ctx_.col_descs_ = &col_descs_;
  ctx_.row_store_type_ = common::ENCODING_ROW_STORE;

  ASSERT_EQ(OB_SUCCESS, encoder_.init(ctx_));

  ObStoreRow row;
  oceanbase::common::ObObj objs[COLUMN_CNT];
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = COLUMN_CNT;
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    LOG_DEBUG("row", K(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
}

TEST_F(TestMicroBlockDecoder, perf)
{
  int64_t i_size = sizeof(ObIEncodeBlockReader);
  int64_t r_size = sizeof(ObEncodeBlockGetReader);
  LOG_INFO("class size", K(i_size), K(r_size));

  const int64_t count = 100;
  int64_t i = 0;
  int64_t start = 0;
  int64_t duration = 0;
  int ret = common::OB_SUCCESS;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());

  start = common::ObTimeUtility::current_time();
  for (i = 0; i < count; ++i) {
    ObEncodeBlockGetReader reader;
    UNUSED(reader);
  }
  duration = common::ObTimeUtility::current_time() - start;
  LOG_INFO("perf ctr", K(ret), K(i), K(duration));

  ObEncodeBlockGetReader reader2;
  start = common::ObTimeUtility::current_time();
  for (i = 0; OB_SUCC(ret) && i < count; ++i) {
    reader2.reuse();
    ret = reader2.init_by_read_info(data, read_info_);
  }
  duration = common::ObTimeUtility::current_time() - start;
  LOG_INFO("perf init", K(ret), K(i), K(duration));

  start = common::ObTimeUtility::current_time();
  for (i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObEncodeBlockGetReader reader3;
    ret = reader3.init_by_read_info(data, read_info_);
  }
  duration = common::ObTimeUtility::current_time() - start;
  LOG_INFO("perf all", K(ret), K(i), K(duration));
}

TEST_F(TestMicroBlockDecoder, read)
{
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  ObMicroBlockDecoder batch_decoder;
  ObMicroBlockData batch_data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, batch_decoder.init(batch_data, read_info_));
  int64_t iter = 0;
  ObStoreRow *rows = new ObStoreRow[ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT];
  char *obj_buf = new char[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj) * ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT];
  for (int64_t i = 0; i < ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT; ++i) {
    rows[i].row_val_.cells_ = reinterpret_cast<ObObj *>(obj_buf) + i * common::OB_ROW_MAX_COLUMNS_COUNT;
    rows[i].row_val_.count_ = common::OB_ROW_MAX_COLUMNS_COUNT;
  }
  int64_t batch_count = 0;
  int64_t batch_num = ROW_CNT;
  ASSERT_EQ(OB_SUCCESS, batch_decoder.get_rows(iter, iter + batch_num,
      ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT, rows, batch_count));
  ASSERT_LE(batch_count, batch_num);
  LOG_INFO("batch get rows", K(batch_count), K(batch_num));

  ObStoreRow single_row;
  char *single_row_buf = new char[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj)];
  for (int64_t i = 0; i < batch_count; ++i) {
    const ObStoreRow *cur_row = rows + i;
    LOG_INFO("read row", K(i), K(*cur_row));
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, row_generate_.check_one_row(*cur_row, exist));
    ASSERT_TRUE(exist) << "\n index: " << i;

    single_row.row_val_.cells_ = reinterpret_cast<ObObj *>(single_row_buf);
    single_row.row_val_.count_ = common::OB_ROW_MAX_COLUMNS_COUNT;
    int64_t tmp_iter = i;
    ASSERT_EQ(OB_SUCCESS, decoder.get_row(tmp_iter, single_row));
    LOG_INFO("read row", K(i), K(single_row));
    ASSERT_EQ(OB_SUCCESS, row_generate_.check_one_row(single_row, exist));
    ASSERT_TRUE(exist) << "\n index: " << i;

    ASSERT_EQ(cur_row->row_val_.count_, single_row.row_val_.count_);
    for (int64_t j = 0; j < single_row.row_val_.count_; ++j) {
      ASSERT_TRUE(cur_row->row_val_.cells_[j] == single_row.row_val_.cells_[j])
        << "\n i: " << i << " j: " << j
        << "\n writer:  "<< to_cstring(single_row.row_val_.cells_[j])
        << "\n reader:  " << to_cstring(cur_row->row_val_.cells_[j]);
    }
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
