// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include <errno.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#define protected public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "share/config/ob_server_config.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_i_store.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "ob_row_generate.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;
using namespace std;
using namespace sql;

namespace unittest
{

class TestSkipIndexFilter : public ::testing::Test
{
public:
  static const int64_t ROWKEY_CNT = 2;
  static const int64_t COLUMN_CNT = ObExtendType - 1 + 7;
  TestSkipIndexFilter();
  virtual ~TestSkipIndexFilter();
  virtual void SetUp();
  virtual void TearDown();

  void setup_obj(ObObj& obj, int64_t column_id, int64_t seed);

  void init_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf);

  void init_in_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf);

  int test_filter_pushdown(const uint64_t col_idx,
    sql::ObPushdownWhiteFilterNode &filter_node,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    bool &filtered,
    ObSkipIndexFilterExecutor &skip_index_filter);


  int test_skip_index_filter_pushdown(const uint64_t col_idx,
    sql::ObPushdownWhiteFilterNode &filter_node,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    ObObj &min_obj,
    ObObj &max_obj,
    ObObj &null_count_obj,
    ObBoolMask &fal_desc);
protected:
  ObRowGenerate row_generate_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  ObTableReadInfo read_info_;
  int64_t full_column_cnt_;
  uint64_t row_count_;
  ObArenaAllocator allocator_;
};

TestSkipIndexFilter::TestSkipIndexFilter()
    : allocator_()
{
}
TestSkipIndexFilter::~TestSkipIndexFilter()
{
}

void TestSkipIndexFilter::SetUp()
{
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  row_count_ = 10;
  const int64_t tid = 200001;
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(1);
  table.set_database_id(1);
  table.set_table_id(tid);
  table.set_table_name("test_skip_index_filter_schema");
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
      col_descs_,
      nullptr));
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  full_column_cnt_ = COLUMN_CNT + extra_rowkey_cnt;
}

void TestSkipIndexFilter::setup_obj(ObObj& obj, int64_t column_id, int64_t seed)
{
  obj.copy_meta_type(row_generate_.column_list_.at(column_id).col_type_);
  ObObjType column_type = row_generate_.column_list_.at(column_id).col_type_.get_type();
  STORAGE_LOG(INFO, "Type of current column is: ", K(column_type));
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

void TestSkipIndexFilter::TearDown()
{
}

void TestSkipIndexFilter::init_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf)
{
  int count = filter_objs.count();
  ObWhiteFilterOperatorType op_type = filter.filter_.get_op_type();
  if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
    count = 1;
  }

  filter.filter_.expr_ = new (expr_buf) ObExpr();
  filter.filter_.expr_->arg_cnt_ = count + 1;
  filter.filter_.expr_->args_ = expr_p_buf;
  ASSERT_EQ(OB_SUCCESS, filter.datum_params_.init(count));

  for (int64_t i = 0; i <= count; ++i) {
    filter.filter_.expr_->args_[i] = new (expr_buf + 1 + i) ObExpr();
    if (i < count) {
      if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
        filter.filter_.expr_->args_[i]->obj_meta_.set_null();
        filter.filter_.expr_->args_[i]->datum_meta_.type_ = ObNullType;
      } else {
        filter.filter_.expr_->args_[i]->obj_meta_ = filter_objs.at(i).get_meta();
        filter.filter_.expr_->args_[i]->datum_meta_.type_ = filter_objs.at(i).get_meta().get_type();
        datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
        datums[i].from_obj(filter_objs.at(i));
        ASSERT_EQ(OB_SUCCESS, filter.datum_params_.push_back(datums[i]));
        if (filter.is_null_param(datums[i], filter_objs.at(i).get_meta())) {
          filter.null_param_contained_ = true;
        }
      }
    } else {
      filter.filter_.expr_->args_[i]->type_ = T_REF_COLUMN;
      filter.filter_.expr_->args_[i]->obj_meta_.set_null(); // unused
    }
  }
  filter.cmp_func_ = get_datum_cmp_func(filter.filter_.expr_->args_[0]->obj_meta_, filter.filter_.expr_->args_[0]->obj_meta_);
}

void TestSkipIndexFilter::init_in_filter(
    sql::ObWhiteFilterExecutor &filter,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf)
{
  int count = filter_objs.count();
  ASSERT_TRUE(count > 0);
  filter.filter_.expr_ = new (expr_buf) ObExpr();
  filter.filter_.expr_->arg_cnt_ = 2;
  filter.filter_.expr_->args_ = expr_p_buf;
  filter.filter_.expr_->args_[0] = new (expr_buf + 1) ObExpr();
  filter.filter_.expr_->args_[1] = new (expr_buf + 2) ObExpr();
  filter.filter_.expr_->inner_func_cnt_ = count;
  filter.filter_.expr_->args_[1]->args_ = expr_p_buf + 2;

  ObObjMeta obj_meta = filter_objs.at(0).get_meta();
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
    obj_meta.get_type(), obj_meta.get_collation_type(), obj_meta.get_scale(), false, obj_meta.has_lob_header());
  ObDatumCmpFuncType cmp_func = get_datum_cmp_func(obj_meta, obj_meta);

  filter.filter_.expr_->args_[0]->type_ = T_REF_COLUMN;
  filter.filter_.expr_->args_[0]->obj_meta_ = obj_meta;
  filter.filter_.expr_->args_[0]->datum_meta_.type_ = obj_meta.get_type();
  filter.filter_.expr_->args_[0]->basic_funcs_ = basic_funcs;

  ASSERT_EQ(OB_SUCCESS, filter.datum_params_.init(count));
  ASSERT_EQ(OB_SUCCESS, filter.param_set_.create(count * 2));
  filter.param_set_.set_hash_and_cmp_func(basic_funcs->murmur_hash_v2_, basic_funcs->null_first_cmp_);
  for (int64_t i = 0; i < count; ++i) {
    filter.filter_.expr_->args_[1]->args_[i] = new (expr_buf + 3 + i) ObExpr();
    filter.filter_.expr_->args_[1]->args_[i]->obj_meta_ = obj_meta;
    filter.filter_.expr_->args_[1]->args_[i]->datum_meta_.type_ = obj_meta.get_type();
    filter.filter_.expr_->args_[1]->args_[i]->basic_funcs_ = basic_funcs;
    datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
    datums[i].from_obj(filter_objs.at(i));
    if (!filter.is_null_param(datums[i], filter_objs.at(i).get_meta())) {
      ASSERT_EQ(OB_SUCCESS, filter.add_to_param_set_and_array(datums[i], filter.filter_.expr_->args_[1]->args_[i]));
    }
  }
  std::sort(filter.datum_params_.begin(), filter.datum_params_.end(),
            [cmp_func] (const ObDatum datum1, const ObDatum datum2) {
                int cmp_ret = 0;
                cmp_func(datum1, datum2, cmp_ret);
                return cmp_ret < 0;
            });
  filter.cmp_func_ = cmp_func;
  filter.cmp_func_rev_ = cmp_func;
  filter.param_set_.set_hash_and_cmp_func(basic_funcs->murmur_hash_v2_, filter.cmp_func_rev_);
}

int TestSkipIndexFilter::test_skip_index_filter_pushdown (
    const uint64_t col_idx,
    sql::ObPushdownWhiteFilterNode &filter_node,
    common::ObFixedArray<ObObj, ObIAllocator> &filter_objs,
    ObObj &min_obj,
    ObObj &max_obj,
    ObObj &null_count_obj,
    ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  // genereate filter
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  sql::ObWhiteFilterExecutor filter(allocator_, filter_node, op);
  eval_ctx.batch_size_ = 256;
  filter.col_offsets_.init(COLUMN_CNT);
  filter.col_params_.init(COLUMN_CNT);
  const ObColumnParam *col_param = nullptr;
  filter.col_params_.push_back(col_param);
  filter.col_offsets_.push_back(col_idx);
  filter.n_cols_ = 1;

  int count = filter_objs.count();
  ObWhiteFilterOperatorType op_type = filter_node.get_op_type();
  if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
    count = 1;
  }
  int count_expr = WHITE_OP_IN == op_type ? count + 3 : count + 2;
  int count_expr_p = WHITE_OP_IN == op_type ? count + 2 : count + 1;
  sql::ObExpr *expr_buf = reinterpret_cast<sql::ObExpr *>(allocator_.alloc(sizeof(sql::ObExpr) * count_expr));
  sql::ObExpr **expr_p_buf = reinterpret_cast<sql::ObExpr **>(allocator_.alloc(sizeof(sql::ObExpr*) * count_expr_p));
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128 * count);
  ObDatum datums[count];
  EXPECT_TRUE(OB_NOT_NULL(expr_buf));
  EXPECT_TRUE(OB_NOT_NULL(expr_p_buf));

  if (WHITE_OP_IN == op_type) {
    init_in_filter(filter, filter_objs, expr_buf, expr_p_buf, datums, datum_buf);
  } else {
    init_filter(filter, filter_objs, expr_buf, expr_p_buf, datums, datum_buf);
  }

  // generate agg_row_writer and reader
  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  agg_row.init(3); // min, max, null_count

  ObSkipIndexColMeta skip_col_meta;
  skip_col_meta.col_idx_ = col_idx;
  skip_col_meta.col_type_ = SK_IDX_MIN;
  agg_cols.push_back(skip_col_meta);
  agg_row.storage_datums_[0].from_obj_enhance(min_obj);

  skip_col_meta.col_type_ = SK_IDX_MAX;
  agg_cols.push_back(skip_col_meta);
  agg_row.storage_datums_[1].from_obj_enhance(max_obj);

  skip_col_meta.col_type_ = SK_IDX_NULL_COUNT;
  agg_cols.push_back(skip_col_meta);
  agg_row.storage_datums_[2].from_obj_enhance(null_count_obj);

  ObAggRowWriter row_writer;
  row_writer.init(agg_cols, agg_row, allocator_);
  int64_t buf_size = row_writer.get_data_size();
  char *buf = reinterpret_cast<char *>(allocator_.alloc(buf_size));
  EXPECT_TRUE(buf != nullptr);
  MEMSET(buf, 0, buf_size);
  int64_t pos = 0;
  row_writer.write_agg_data(buf, buf_size, pos);
  EXPECT_TRUE(buf_size == pos);


  ObMicroIndexInfo index_info;
  ObIndexBlockRowHeader row_header;
  ObSkipIndexFilterExecutor skip_index_filter;
  row_header.row_count_ = row_count_;
  index_info.agg_row_buf_ = buf;
  index_info.agg_buf_size_ = buf_size;
  index_info.row_header_ = &row_header;
  EXPECT_EQ(OB_SUCCESS, skip_index_filter.init(op.get_eval_ctx().get_batch_size(), &allocator_));

  ret = skip_index_filter.falsifiable_pushdown_filter(col_idx, filter.filter_.expr_->args_[0]->obj_meta_,
      ObSkipIndexType::MIN_MAX, index_info, filter, allocator_, true);
  fal_desc = filter.get_filter_bool_mask();


  if (nullptr != expr_buf) {
    allocator_.free(expr_buf);
  }
  if (nullptr != expr_p_buf) {
    allocator_.free(expr_p_buf);
  }
  if (nullptr != buf) {
    allocator_.free(buf);
  }
  if (nullptr != datum_buf) {
    allocator_.free(datum_buf);
  }
  return ret;
}


TEST_F(TestSkipIndexFilter, test_eq)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_EQ;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;

    // a. min = max > cell, nullcount = 0, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed2);
    ObObj max_obj;
    setup_obj(max_obj, i, seed2);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. min = max < cell, nullcount = 0, expect filtered = SK_IDX_FAL_TRUE

    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // d. min = max = cell, nullcount = 0, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // e. min < cell < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // f. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // g. ref obj is null
    filter_objs.at(0).set_null();
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

  }
}

TEST_F(TestSkipIndexFilter, test_ne)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_NE;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;

    // a. min = max = cell, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed1);
    ObObj max_obj;
    setup_obj(max_obj, i, seed1);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c.  min = cell, max > cell, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // d. min < cell, max = cell, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // e. min < cell < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // f. min = max < cell, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // g. cell < min = max, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // h. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_lt)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_LT;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;

    // a. min = max = cell, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed1);
    ObObj max_obj;
    setup_obj(max_obj, i, seed1);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. min = max > cell, expect filtered = SK_IDX_FAL_TRUE
    setup_obj(min_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // d. min < cell < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // d. min = max < cell, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // f. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_le)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_LE;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;



    // a. min = max > cell, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed2);
    ObObj max_obj;
    setup_obj(max_obj, i, seed2);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c. min = max = cell, expect filtered = SK_IDX_FAL_FALSE;
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // d. min = max < cell, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // e. min < cell < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // e. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_gt)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_GT;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;

    // a. min = max = cell, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed1);
    ObObj max_obj;
    setup_obj(max_obj, i, seed1);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. min = max < cell, expect filtered = SK_IDX_FAL_TRUE
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // d. min < cell < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // d. min = max > cell, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // e. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_ge)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_GE;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;

    // a. min = max < cell, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed0);
    ObObj max_obj;
    setup_obj(max_obj, i, seed0);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c. min < cell < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // d. min = max = cell, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // e. min = max > cell, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // f. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_nu)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_NU;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;

    // a. null_count = 0, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed1);
    ObObj max_obj;
    setup_obj(max_obj, i, seed1);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. 0 < null_count < row_count, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    null_count_obj.set_int(row_count_ - 1);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // c. null_count = row_count, expect filtered = SK_IDX_FAL_FALSE
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // d. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_nn)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_NN;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 1);
    filter_objs.init(1);

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    filter_objs.push_back(ref_obj);

    int32_t col_idx = i;

    // a. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    min_obj.set_null();
    ObObj max_obj;
    max_obj.set_null();
    ObObj null_count_obj;
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. 0 < null_count < row_count, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(row_count_ - 1);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // c. null_count = 0, expect filtered = SK_IDX_FAL_FALSE
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // d. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_bt)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_BT;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  int64_t seed3 = 0x3;
  int64_t seed4 = 0x4;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 2);
    filter_objs.init(2);

    // generate whitefilter op and filter cell
    ObObj left_obj;
    setup_obj(left_obj, i, seed1);
    filter_objs.push_back(left_obj);
    ObObj right_obj;
    setup_obj(right_obj, i, seed3);
    filter_objs.push_back(right_obj);

    int32_t col_idx = i;

    // a. min = max > right, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed4);
    ObObj max_obj;
    setup_obj(max_obj, i, seed4);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // b. min = max < left, min = null, expect filtered = SK_IDX_FAL_TRUE
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // d. min < left < right < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed4);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // d. min < left < max < right, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // d. left < min < right < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed4);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // d. min = max = left, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // d. min = max = left, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // e. min = max = right, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed3);
    setup_obj(max_obj, i, seed3);
    null_count_obj.set_int(0);

    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // f. left < min = max < right, expect filtered = SK_IDX_FAL_FALSE
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);

    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // g. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}

TEST_F(TestSkipIndexFilter, test_in)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  white_filter.op_type_ = sql::WHITE_OP_IN;

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  int64_t seed3 = 0x3;
  int64_t seed4 = 0x4;
  ObBoolMask fal_desc;;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 2);
    filter_objs.init(2);

    // generate whitefilter op and filter cell

    ObObj left_obj;
    setup_obj(left_obj, i, seed1);
    filter_objs.push_back(left_obj);
    ObObj right_obj;
    setup_obj(right_obj, i, seed3);
    filter_objs.push_back(right_obj);

    int32_t col_idx = i;

    // a. min = max != cell, expect filtered = SK_IDX_FAL_TRUE
    ObObj min_obj;
    setup_obj(min_obj, i, seed0);
    ObObj max_obj;
    setup_obj(max_obj, i, seed0);
    ObObj null_count_obj;
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    setup_obj(min_obj, i, seed4);
    setup_obj(max_obj, i, seed4);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // a. min = max = cell
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_true());

    // b. min > cells or max < cells, expect filtered = SK_IDX_FAL_TRUE
    setup_obj(min_obj, i, seed4);
    setup_obj(max_obj, i, seed4);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // c. min = max > cell or min = max < cell, expect filtered = SK_IDX_FAL_TRUE
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    setup_obj(min_obj, i, seed4);
    setup_obj(max_obj, i, seed4);
    null_count_obj.set_int(0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // d. null_count = row_count, expect filtered = SK_IDX_FAL_TRUE
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_int(row_count_);

    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_always_false());

    // e. left < min < right < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed4);
    null_count_obj.set_int(0);

    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // f. min < left < max < right, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed2);
    null_count_obj.set_int(0);

    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // g. min < left < right < max, expect filtered = SK_IDX_FAL_NOT_CERTAIN
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed4);
    null_count_obj.set_int(0);

    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // h. min max null_count all null, expect uncertain cause by progressive merge;
    min_obj.set_null();
    max_obj.set_null();
    null_count_obj.set_null();
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

  }
}


TEST_F(TestSkipIndexFilter, test_has_null)
{
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  int64_t seed3 = 0x3;
  int64_t seed4 = 0x4;
  ObBoolMask fal_desc;

  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  OK(agg_row.init(3)); // min, max, null_count

  for (int64_t i = 0; i < full_column_cnt_ -1; ++i) {
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      continue;
    }
    ObMalloc mallocer;
    mallocer.set_label("SkipIndexFilter");
    ObFixedArray<ObObj, ObIAllocator> filter_objs(mallocer, 2);
    ASSERT_EQ(OB_SUCCESS, filter_objs.init(2));

    // generate whitefilter op and filter cell
    ObObj ref_obj;
    setup_obj(ref_obj, i, seed1);
    ASSERT_EQ(OB_SUCCESS, filter_objs.push_back(ref_obj));

    int32_t col_idx = i;

    ObObj min_obj;
    ObObj max_obj;
    ObObj null_count_obj;
    null_count_obj.set_int(row_count_ / 2);

    // 0 < null_count < row_count, WHITE_OP_NU
    white_filter.op_type_ = sql::WHITE_OP_NU;
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(row_count_ - 1);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // 0 < null_count < row_count, WHITE_OP_NN
    white_filter.op_type_ = sql::WHITE_OP_NN;
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    null_count_obj.set_int(row_count_ - 1);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // min = max = cell, WHITE_OP_EQ
    white_filter.op_type_ = sql::WHITE_OP_EQ;
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // min = max < cell, WHITE_OP_NE
    white_filter.op_type_ = sql::WHITE_OP_NE;
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // min = max < cell, WHITE_OP_LT
    white_filter.op_type_ = sql::WHITE_OP_LT;
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // min = max < cell, WHITE_OP_LE
    white_filter.op_type_ = sql::WHITE_OP_LE;
    setup_obj(min_obj, i, seed0);
    setup_obj(max_obj, i, seed0);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // min = max > cell, WHITE_OP_GT
    white_filter.op_type_ = sql::WHITE_OP_GT;
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // min = max > cell, WHITE_OP_GE
    white_filter.op_type_ = sql::WHITE_OP_GE;
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    ObObj right_obj;
    setup_obj(right_obj, i, seed3);
    ASSERT_EQ(OB_SUCCESS, filter_objs.push_back(right_obj));

    // left < min = max < cell, WHITE_OP_BT
    white_filter.op_type_ = sql::WHITE_OP_BT;
    setup_obj(min_obj, i, seed2);
    setup_obj(max_obj, i, seed2);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());

    // min = max = left, WHITE_OP_IN
    white_filter.op_type_ = sql::WHITE_OP_IN;
    setup_obj(min_obj, i, seed1);
    setup_obj(max_obj, i, seed1);
    ASSERT_EQ(OB_SUCCESS, test_skip_index_filter_pushdown(col_idx, white_filter, filter_objs, min_obj, max_obj, null_count_obj, fal_desc));
    ASSERT_TRUE(fal_desc.is_uncertain());
  }
}


}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_skip_index_filter.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_skip_index_filter.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
