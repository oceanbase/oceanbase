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
#include "storage/blocksstable/encoding/ob_raw_decoder.h"
#include "storage/blocksstable/encoding/ob_raw_encoder.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/access/ob_block_row_store.h"
#include "storage/ob_i_store.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "lib/string/ob_sql_string.h"
#include "../ob_row_generate.h"
#include "common/rowkey/ob_rowkey.h"
#include "unittest/storage/mock_ob_table_read_info.h"


namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class ObMicroBlockRawEncoder : public ObMicroBlockEncoder
{
public:
  int build_block(char *&buf, int64_t &size);

};

/** This function is to override the function in ObMicroBlockEncoder to perform unittest on
 *  raw_encoding and filter pushdown operators.
 *  Any modify on the original function should be synchronized to here.
 */
int ObMicroBlockRawEncoder::build_block(char *&buf, int64_t &size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (datum_rows_.empty()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("empty micro block", K(ret));
  } else if (OB_FAIL(pivot())) {
    LOG_WARN("pivot rows to columns failed", K(ret));
  } else if (OB_FAIL(row_indexs_.reserve(datum_rows_.count()))) {
    LOG_WARN("array reserve failed", K(ret), "count", datum_rows_.count());
  } else if (OB_FAIL(encoder_detection())) {
    LOG_WARN("detect column encoding failed", K(ret));
  } else {

    for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
      const bool force_var_store = false;
      if (NULL != encoders_[i]) {
        free_encoder(encoders_[i]);
        encoders_[i] = NULL;
      }

      ObIColumnEncoder *e = NULL;
      if (OB_FAIL(force_raw_encoding(i, force_var_store, e))) {
        LOG_WARN("force_raw_encoding failed", K(ret), K(i), K(force_var_store));
      } else if (OB_ISNULL(e)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("encoder is NULL", K(ret), K(i));
      } else {
        encoders_[i] = e;
      }
    }


    // <1> store encoding metas and fix cols data
    int64_t encoding_meta_offset = 0;
    if (OB_FAIL(store_encoding_meta_and_fix_cols(encoding_meta_offset))) {
      LOG_WARN("failed to store encoding meta and fixed col data", K(ret));
    }

    // <2> set row data store offset
    int64_t fix_data_size = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_row_data_pos(fix_data_size))) {
        LOG_WARN("set row data position failed", K(ret));
      } else {
        header_->var_column_count_ = static_cast<int16_t>(var_data_encoders_.count());
      }
    }

    // <3> fill row data (i.e. var data)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_row_data(fix_data_size))) {
        LOG_WARN("fill row data failed", K(ret));
      }
    }

    // <4> fill row index
    if (OB_SUCC(ret)) {
      if (var_data_encoders_.empty()) {
        header_->row_index_byte_ = 0;
      } else {
        header_->row_index_byte_ = 2;
        if (row_indexs_.at(row_indexs_.count() - 1) > UINT16_MAX) {
          header_->row_index_byte_ = 4;
        }
        ObIntegerArrayGenerator gen;
        const int64_t row_index_size = row_indexs_.count() * header_->row_index_byte_;
        if (OB_FAIL(gen.init(data_buffer_.current(), header_->row_index_byte_))) {
          LOG_WARN("init integer array generator failed",
              K(ret), "byte", header_->row_index_byte_);
        } else if (OB_FAIL(data_buffer_.advance_zero(row_index_size))) {
          LOG_WARN("advance data buffer failed", K(ret), K(row_index_size));
        } else {
          for (int64_t idx = 0; idx < row_indexs_.count(); ++idx) {
            gen.get_array().set(idx, row_indexs_.at(idx));
          }
        }
      }
    }

    // <5> fill header
    if (OB_SUCC(ret)) {
      header_->row_count_ = static_cast<int16_t>(datum_rows_.count());
      header_->has_string_out_row_ = has_string_out_row_;
      header_->all_lob_in_row_ = !has_lob_out_row_;


      const int64_t header_size = header_->header_size_;
      char *data = data_buffer_.data() + header_size;
      FOREACH(e, encoders_) {
        MEMCPY(data, &(*e)->get_column_header(), sizeof(ObColumnHeader));
        data += sizeof(ObColumnHeader);
      }
    }

    if (OB_SUCC(ret)) {
      // update encoding context
      ctx_.estimate_block_size_ += estimate_size_;
      ctx_.real_block_size_ += data_buffer_.length() - encoding_meta_offset;
      ctx_.micro_block_cnt_++;
      ObPreviousEncoding pe;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < encoders_.count(); ++idx) {
        ObIColumnEncoder *e = encoders_.at(idx);
        pe.type_ = static_cast<ObColumnHeader::Type>(e->get_column_header().type_);
        if (ObColumnHeader::is_inter_column_encoder(pe.type_)) {
          pe.ref_col_idx_ = static_cast<ObColumnEqualEncoder *>(e)->get_ref_col_idx();
        } else {
          pe.ref_col_idx_ = 0;
        }
        if (ObColumnHeader::STRING_PREFIX == pe.type_) {
          pe.last_prefix_length_ = col_ctxs_.at(idx).last_prefix_length_;
        }
        if (idx < ctx_.previous_encodings_.count()) {
          if (OB_FAIL(ctx_.previous_encodings_.at(idx).put(pe))) {
            LOG_WARN("failed to store previous encoding", K(ret), K(idx), K(pe));
          }

          //if (ctx_->previous_encodings_.at(idx).last_1 != pe.type_) {
            //LOG_DEBUG("encoder changing", K(idx),
                //"previous type", ctx_->previous_encodings_.at(idx).last_,
                //"current type", pe);
          //}
        } else {
          ObPreviousEncodingArray<ObMicroBlockEncodingCtx::MAX_PREV_ENCODING_COUNT> pe_array;
          if (OB_FAIL(pe_array.put(pe))) {
            LOG_WARN("failed to store previous encoding", K(ret), K(idx), K(pe));
          } else if (OB_FAIL(ctx_.previous_encodings_.push_back(pe_array))) {
            LOG_WARN("push back previous encoding failed");
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      buf = data_buffer_.data();
      size = data_buffer_.pos();
    }
  }

  return ret;
}

class TestRawDecoder : public ::testing::Test
{
public:
  static const int64_t ROWKEY_CNT = 2;
  static const int64_t COLUMN_CNT = ObExtendType - 1 + 7;

  static const int64_t ROW_CNT = 64;

  virtual void SetUp();
  virtual void TearDown() {}

  TestRawDecoder(): tenant_ctx_(OB_SERVER_TENANT_ID)
  {
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
  }
  virtual ~TestRawDecoder() {}

  void setup_obj(ObObj& obj, int64_t column_id, int64_t seed);
  int test_filter_pushdown(
      const uint64_t col_id,
      ObMicroBlockDecoder& decoder,
      sql::ObPushdownWhiteFilterNode &filter_node,
      common::ObBitmap &result_bitmap,
      common::ObFixedArray<ObObj, ObIAllocator> &objs);

protected:
  ObRowGenerate row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  ObMicroBlockRawEncoder encoder_;
  MockObTableReadInfo read_info_;
  int64_t full_column_cnt_;
  ObArenaAllocator allocator_;
  share::ObTenantBase tenant_ctx_;
};

void TestRawDecoder::SetUp()
{
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  const int64_t tid = 200001;
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(1);
  table.set_database_id(1);
  table.set_table_id(tid);
  table.set_table_name("test_raw_decoder_schema");
  table.set_rowkey_column_num(ROWKEY_CNT);
  table.set_max_column_id(COLUMN_CNT * 2);
  table.set_block_size(2 * 1024);
  table.set_compress_func_name("none");
  table.set_row_store_type(ENCODING_ROW_STORE);
  table.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

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
    } else if (COLUMN_CNT - 2 == i) {
      type = ObIntervalYMType;
    } else if (COLUMN_CNT - 3 == i) {
      type = ObIntervalDSType;
    } else if (COLUMN_CNT - 4 == i) {
      type = ObTimestampTZType;
    } else if (COLUMN_CNT - 5 == i) {
      type = ObTimestampLTZType;
    } else if (COLUMN_CNT - 6 == i) {
      type = ObTimestampNanoType;
    } else if (COLUMN_CNT - 7 == i) {
      type = ObRawType;
    } else if (type == ObExtendType || type == ObUnknownType) {
      type = ObVarcharType;
    }
    col.set_data_type(type);

    if ( ObVarcharType == type || ObCharType == type || ObHexStringType == type
        || ObNVarchar2Type == type || ObNCharType == type || ObTextType == type){
      col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    } else {
      col.set_collation_type(CS_TYPE_BINARY);
    }

    if (type == ObIntType) {
      col.set_rowkey_position(1);
    } else if (type == ObUInt64Type) {
      col.set_rowkey_position(2);
    } else{
      col.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table.add_column(col));
  }

  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table, true/*multi_version*/));
  ASSERT_EQ(OB_SUCCESS, table.get_multi_version_column_descs(col_descs_));
  ASSERT_EQ(OB_SUCCESS, read_info_.init(
      allocator_,
      table.get_column_count(),
      table.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      col_descs_));

  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  full_column_cnt_ = COLUMN_CNT + extra_rowkey_cnt;
  ctx_.micro_block_size_ = 64L << 11;
  ctx_.macro_block_size_ = 2L << 20;
  ctx_.rowkey_column_cnt_ = ROWKEY_CNT + extra_rowkey_cnt;
  ctx_.column_cnt_ = COLUMN_CNT + extra_rowkey_cnt;
  ctx_.col_descs_ = &col_descs_;

  int64_t *column_encodings = reinterpret_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * ctx_.column_cnt_));
  ctx_.column_encodings_ = column_encodings;
  for (int64_t i = 0; i < ctx_.column_cnt_; ++i) {
    ctx_.column_encodings_[i] = ObColumnHeader::Type::RAW;
  }
  ctx_.row_store_type_ = common::ENCODING_ROW_STORE;

  ASSERT_EQ(OB_SUCCESS, encoder_.init(ctx_));
}

void TestRawDecoder::setup_obj(ObObj& obj, int64_t column_id, int64_t seed)
{
  obj.copy_meta_type(row_generate_.column_list_.at(column_id).col_type_);
  ObObjType column_type = row_generate_.column_list_.at(column_id).col_type_.get_type();
  LOG_INFO("Type of current column is: ", K(column_type));
  row_generate_.set_obj(column_type, row_generate_.column_list_.at(column_id).col_id_, seed, obj, 0);
  if ( ObVarcharType == column_type || ObCharType == column_type || ObHexStringType == column_type
      || ObNVarchar2Type == column_type || ObNCharType == column_type || ObTextType == column_type){
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    obj.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    obj.set_collation_type(CS_TYPE_BINARY);
    obj.set_collation_level(CS_LEVEL_NUMERIC);
  }
}

int TestRawDecoder::test_filter_pushdown(
    const uint64_t col_idx,
    ObMicroBlockDecoder& decoder,
    sql::ObPushdownWhiteFilterNode &filter_node,
    common::ObBitmap &result_bitmap,
    common::ObFixedArray<ObObj, ObIAllocator> &objs)
{
  int ret = OB_SUCCESS;
  storage::PushdownFilterInfo pd_filter_info;
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  sql::ObWhiteFilterExecutor filter(allocator_, filter_node, op);
  filter.col_offsets_.init(COLUMN_CNT);
  filter.col_params_.init(COLUMN_CNT);
  const ObColumnParam *col_param = nullptr;
  filter.col_params_.push_back(col_param);
  filter.col_offsets_.push_back(col_idx);
  filter.n_cols_ = 1;
  void *obj_buf = allocator_.alloc(sizeof(ObObj) * COLUMN_CNT);
  EXPECT_TRUE(obj_buf != nullptr);
  ObObj *col_buf = new (obj_buf) ObObj [COLUMN_CNT]();
  filter.params_ = objs;
  filter.init_obj_set();
  pd_filter_info.col_buf_ = col_buf;
  pd_filter_info.col_capacity_ = full_column_cnt_;
  pd_filter_info.start_ = 0;
  pd_filter_info.end_ = decoder.row_count_;

  ret = decoder.filter_pushdown_filter(nullptr, filter, pd_filter_info, result_bitmap);
  return ret;
}

TEST_F(TestRawDecoder, filter_pushdown_all_eq_ne)
{
  // Generate data and encode
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed1 = 0xF;
  int64_t seed2 = 0x0;
  for (int64_t i = 0; i < ROW_CNT - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 20; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }


  int64_t seed1_count = ROW_CNT - 20;
  int64_t seed2_count = 10;
  int64_t null_count = 10;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));


  // Dedcode and filter_push_down
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);


  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    // Test Equal operator
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    objs.push_back(ref_obj);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_EQ;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());

    // Test Not Equal operator
    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
  }
}

TEST_F(TestRawDecoder, filter_push_down_gt_lt_ge_le)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  for (int64_t i = 0; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 20; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  int64_t seed0_count = ROW_CNT - 30;
  int64_t seed1_count = 10;
  int64_t seed2_count = 10;
  int64_t null_count = 10;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);


  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);

    objs.push_back(ref_obj);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_GT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    // Greater Than
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());

    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());
  }
}

TEST_F(TestRawDecoder, filter_push_down_bt)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  for (int64_t i = 0; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  int64_t seed0_count = ROW_CNT - 10;
  int64_t seed1_count = 10;
  int64_t seed3_count = 0;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 2);
    objs.init(2);

    ObObj ref_obj1;
    setup_obj(ref_obj1, i, seed0);

    ObObj ref_obj2;
    setup_obj(ref_obj2, i, seed2);

    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);


    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_BT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());


    objs.reuse();
    objs.init(2);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj1);

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
  }
}

TEST_F(TestRawDecoder, filter_push_down_in_nu)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  int64_t seed3 = 0x3;
  int64_t seed4 = 0x4;
  int64_t seed5 = 0x5;

  for (int64_t i = 0; i < ROW_CNT - 40; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 40; i < ROW_CNT - 30; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 30; i < ROW_CNT - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 20; i < ROW_CNT - 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed3, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 10; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    LOG_INFO("Null row appended: ", K(row));
  }

  int64_t seed0_count = ROW_CNT - 40;
  int64_t seed1_count = 10;
  int64_t seed2_count = 10;
  int64_t seed3_count = 10;
  int64_t null_count = 10;
  int64_t seed5_count = 0;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    sql::ObPushdownWhiteFilterNode white_filter_2(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("RawDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer);
    objs.init(3);

    ObObj ref_obj0;
    setup_obj(ref_obj0, i, seed0);
    ObObj ref_obj1;
    setup_obj(ref_obj1, i, seed1);
    ObObj ref_obj2;
    setup_obj(ref_obj2, i, seed2);
    ObObj ref_obj5;
    setup_obj(ref_obj5, i, seed5);

    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj5);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_IN;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());


    objs.reuse();
    objs.init(3);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    white_filter_2.op_type_ = sql::WHITE_OP_IN;

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter_2, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_NU;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(null_count, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_NN;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(ROW_CNT - null_count, result_bitmap.popcnt());
  }
}

TEST_F(TestRawDecoder, batch_decode_to_datum)
{
  // Generate data and encode
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  for (int64_t i = 0; i < ROW_CNT - 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 35; i < 40; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t i = 40; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  const ObRowHeader *row_header = nullptr;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  const char *cell_datas[ROW_CNT];
  void *datum_buf_1 = allocator_.alloc(sizeof(int8_t) * 128 * ROW_CNT);
  void *datum_buf_2 = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_descs_.at(i)),  K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int64_t row_ids[ROW_CNT];
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf_1) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
        .batch_decode(decoder.row_index_, row_ids, cell_datas, ROW_CNT, datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObObj obj;
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(row_ids[j], row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(obj, row_ids[j], bs, row_data, row_len));
      ObObj obj_cast_from_datum;
      ASSERT_EQ(OB_SUCCESS, datums[j].to_obj(obj_cast_from_datum, col_descs_.at(i).col_type_));
      LOG_INFO("Current row: ", K(j), K(obj), K(obj_cast_from_datum));
      ObDatum datum_cast_from_obj;
      datum_cast_from_obj.ptr_ = reinterpret_cast<char *>(datum_buf_2);
      ASSERT_EQ(OB_SUCCESS, datum_cast_from_obj.from_obj(obj));
      LOG_INFO("Current row: ", K(j), K(datum_cast_from_obj), K(datums[j]));
      ASSERT_EQ(obj, obj_cast_from_datum);
      ASSERT_TRUE(ObDatum::binary_equal(datum_cast_from_obj, datums[j]));
    }
  }
}


TEST_F(TestRawDecoder, opt_batch_decode_to_datum)
{
  // Generate data and encode
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  for (int64_t i = 0; i < ROW_CNT ; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    for (int64_t j = 0; j < full_column_cnt_; ++j) {
      // Generate data for var_length
      if (col_descs_[j].col_type_.get_type() == ObVarcharType) {
        ObStorageDatum &datum = row.storage_datums_[j];
        datum.len_ = i < datum.len_ ? i : datum.len_;
      }
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  const_cast<bool &>(encoder_.ctx_.encoder_opt_.enable_bit_packing_) = false;
  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  const ObRowHeader *row_header = nullptr;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  const char *cell_datas[ROW_CNT];
  void *datum_buf_1 = allocator_.alloc(sizeof(int8_t) * 128 * ROW_CNT);
  void *datum_buf_2 = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    int32_t col_offset = i;
    STORAGE_LOG(INFO, "Current col: ", K(i),K(col_descs_.at(i)), K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int64_t row_ids[ROW_CNT];
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf_1) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
        .batch_decode(decoder.row_index_, row_ids, cell_datas, ROW_CNT, datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObObj obj;
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(row_ids[j], row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(obj, row_ids[j], bs, row_data, row_len));
      ObObj obj_cast_from_datum;
      ASSERT_EQ(OB_SUCCESS, datums[j].to_obj(obj_cast_from_datum, col_descs_.at(i).col_type_));
      LOG_INFO("Current row: ", K(j), K(obj), K(obj_cast_from_datum));
      ObDatum datum_cast_from_obj;
      datum_cast_from_obj.ptr_ = reinterpret_cast<char *>(datum_buf_2);
      ASSERT_EQ(OB_SUCCESS, datum_cast_from_obj.from_obj(obj));
      LOG_INFO("Current row: ", K(j), K(datum_cast_from_obj), K(datums[j]));
      ASSERT_EQ(obj, obj_cast_from_datum);
      ASSERT_TRUE(ObDatum::binary_equal(datum_cast_from_obj, datums[j]));
    }
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_raw_decoder.log*");
  OB_LOGGER.set_file_name("test_raw_decoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
