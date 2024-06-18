// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))

#include "share/schema/ob_table_param.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/access/ob_sstable_index_filter.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace sql;
using namespace share::schema;

namespace unittest
{

class TestSSTableIndexFilter : public ::testing::Test
{
public:
  static const uint32_t TEST_COLUMN_ID = 1;
  static const uint32_t TEST_COLUMN_INDEX = 1;
  static const uint32_t TEST_ROW_CNT = 10;
  TestSSTableIndexFilter();
  virtual ~TestSSTableIndexFilter();
  virtual void SetUp();
  virtual void TearDown();

  void init();
  void test_skipping_filter_nodes_builder_1();
  void test_skipping_filter_nodes_builder_2();
  void test_skipping_filter_nodes_builder_3();
  void test_sstable_index_filter_init_1();
  void test_sstable_index_filter_check_range_1();
  ObPushdownFilterExecutor* create_physical_filter(bool is_white);
  ObPushdownFilterExecutor* create_lt_white_filter(uint64_t value);
  ObPushdownFilterExecutor* create_logical_filter(bool is_and);
  ObPushdownFilterExecutor* create_hybrid_filter_1(bool is_and);
  ObPushdownFilterExecutor* create_hybrid_filter_2();
  ObPushdownFilterExecutor* create_hybrid_filter_3();
  void init_micro_index_info(
      const ObObj &max_obj,
      const ObObj &min_obj,
      ObMicroIndexInfo &index_info);

public:
  ObExecContext *exec_ctx_;
  ObEvalCtx *eval_ctx_;
  ObPushdownOperator *pushdown_operator_;
  ObPushdownExprSpec *expr_spec_;
  ObIndexBlockRowHeader row_header_;
  ObTableReadInfo read_info_;
  ObArenaAllocator allocator_;
};

TestSSTableIndexFilter::TestSSTableIndexFilter()
    : allocator_() {}
TestSSTableIndexFilter::~TestSSTableIndexFilter() {}
void TestSSTableIndexFilter::SetUp()
{
  init();
}
void TestSSTableIndexFilter::TearDown() {}

void TestSSTableIndexFilter::init()
{
  ObIAllocator* allocator_ptr = &allocator_;
  exec_ctx_ = OB_NEWx(ObExecContext, allocator_ptr, allocator_);
  eval_ctx_ = OB_NEWx(ObEvalCtx, allocator_ptr, *exec_ctx_);
  expr_spec_ = OB_NEWx(ObPushdownExprSpec, allocator_ptr, allocator_);
  pushdown_operator_ = OB_NEWx(ObPushdownOperator, allocator_ptr, *eval_ctx_, *expr_spec_);
  ASSERT_NE(nullptr, exec_ctx_);
  ASSERT_NE(nullptr, eval_ctx_);
  ASSERT_NE(nullptr, expr_spec_);
  ASSERT_NE(nullptr, pushdown_operator_);
  eval_ctx_->batch_size_ = 256;
  row_header_.row_count_ = TEST_ROW_CNT;
  read_info_.cols_index_.array_.init(1, allocator_);
  read_info_.cols_index_.array_.push_back(TEST_COLUMN_INDEX);
  read_info_.cols_param_.init(1, allocator_);
  void *buf = allocator_.alloc(sizeof(ObColumnParam));
  ObColumnParam *column_param = new (buf) ObColumnParam(allocator_);
  column_param->set_column_id(TEST_COLUMN_ID);
  read_info_.cols_param_.push_back(column_param);
  read_info_.cols_extend_.init(1, allocator_);
  ObColExtend col_extend;
  col_extend.skip_index_attr_.set_min_max();
  read_info_.cols_extend_.push_back(col_extend);
  read_info_.cols_desc_.init(1,allocator_);
  ObColDesc col_desc;
  col_desc.col_type_.set_uint64();
  read_info_.cols_desc_.push_back(col_desc);
}

void TestSSTableIndexFilter::test_skipping_filter_nodes_builder_1()
{
  ObPushdownFilterExecutor *filter = create_hybrid_filter_1(true);
  ObSSTableIndexFilter sstable_index_filter;
  OK(sstable_index_filter.build_skipping_filter_nodes(&read_info_, *filter));
  ObSSTableIndexFilter::ObSkippingFilterNodes &nodes = sstable_index_filter.skipping_filter_nodes_;
  ASSERT_EQ(2, sstable_index_filter.skipping_filter_nodes_.count());

  ASSERT_EQ(ObSkipIndexType::MIN_MAX, nodes[0].skip_index_type_);
  ASSERT_EQ(ObSkipIndexType::MIN_MAX, nodes[1].skip_index_type_);

  ASSERT_TRUE(nullptr != nodes[0].filter_);
  ASSERT_TRUE(nullptr != nodes[1].filter_);
}

void TestSSTableIndexFilter::test_skipping_filter_nodes_builder_2()
{
  ObPushdownFilterExecutor* filter = create_hybrid_filter_2();
  ObSSTableIndexFilter sstable_index_filter;
  OK(sstable_index_filter.build_skipping_filter_nodes(&read_info_, *filter));
  ObSSTableIndexFilter::ObSkippingFilterNodes &nodes = sstable_index_filter.skipping_filter_nodes_;
  ASSERT_EQ(3, nodes.count());

  ASSERT_EQ(ObSkipIndexType::MIN_MAX, nodes[0].skip_index_type_);
  ASSERT_EQ(ObSkipIndexType::MIN_MAX, nodes[1].skip_index_type_);
  ASSERT_EQ(ObSkipIndexType::MIN_MAX, nodes[2].skip_index_type_);

  ASSERT_TRUE(nullptr != nodes[0].filter_);
  ASSERT_TRUE(nullptr != nodes[1].filter_);
  ASSERT_TRUE(nullptr != nodes[2].filter_);
}

void TestSSTableIndexFilter::test_skipping_filter_nodes_builder_3()
{
  ObPushdownFilterExecutor* filter = create_hybrid_filter_3();
  ObSSTableIndexFilter sstable_index_filter;
  OK(sstable_index_filter.build_skipping_filter_nodes(&read_info_, *filter));
  ObSSTableIndexFilter::ObSkippingFilterNodes &nodes = sstable_index_filter.skipping_filter_nodes_;
  ASSERT_EQ(0, nodes.count());
}

void TestSSTableIndexFilter::test_sstable_index_filter_init_1()
{
  ObSSTableIndexFilter index_filter;
  ObPushdownFilterExecutor* filter = create_hybrid_filter_1(true);
  OK(index_filter.init(false, &read_info_, *filter, &allocator_));

  ASSERT_TRUE(index_filter.can_use_skipping_index());

  ObSSTableIndexFilter index_filter2;
  ObPushdownFilterExecutor* filter2 = create_hybrid_filter_3();
  OK(index_filter2.init(false, &read_info_, *filter2, &allocator_));

  ASSERT_FALSE(index_filter2.can_use_skipping_index());
}

void TestSSTableIndexFilter::test_sstable_index_filter_check_range_1()
{
  ObSSTableIndexFilter index_filter;

  ObPushdownFilterExecutor *sub_filter = create_logical_filter(false);
  ObPushdownFilterExecutor **sub_filter_childs = new ObPushdownFilterExecutor*[3];
  sub_filter_childs[0] = create_lt_white_filter(10);
  sub_filter_childs[1] = create_lt_white_filter(30);
  sub_filter_childs[2] = create_physical_filter(false);
  sub_filter->set_childs(3, sub_filter_childs);

  ObPushdownFilterExecutor *filter = create_logical_filter(true);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[3];
  childs[0] = sub_filter;
  childs[1] = create_lt_white_filter(50);
  childs[2] = create_physical_filter(false);
  filter->set_childs(3, childs);

  OK(index_filter.init(false, &read_info_, *filter, &allocator_));
  ASSERT_TRUE(index_filter.can_use_skipping_index());
  ASSERT_EQ(3, index_filter.skipping_filter_nodes_.count());

  ObMicroIndexInfo index_info;
  ObObj max_obj;
  max_obj.set_uint64(1000);
  ObObj min_obj;
  min_obj.set_uint64(100);
  init_micro_index_info(max_obj, min_obj, index_info);
  OK(index_filter.check_range(&read_info_, index_info, allocator_, true));
  ASSERT_TRUE(index_info.is_filter_always_false());

  ObMicroIndexInfo index_info2;
  ObObj max_obj2;
  max_obj2.set_uint64(1000);
  ObObj min_obj2;
  min_obj2.set_uint64(40);
  init_micro_index_info(max_obj2, min_obj2, index_info2);
  OK(index_filter.check_range(&read_info_, index_info2, allocator_, true));
  ASSERT_TRUE(index_info2.is_filter_uncertain());

  childs[2] = create_lt_white_filter(100);
  ObSSTableIndexFilter index_filter2;
  OK(index_filter2.init(false, &read_info_, *filter, &allocator_));
  ASSERT_TRUE(index_filter2.can_use_skipping_index());
  ASSERT_EQ(4, index_filter2.skipping_filter_nodes_.count());
  ObMicroIndexInfo index_info3;
  ObObj max_obj3;
  max_obj3.set_uint64(1);
  ObObj min_obj3;
  min_obj3.set_uint64(0);
  init_micro_index_info(max_obj3, min_obj3, index_info3);
  OK(index_filter2.check_range(&read_info_, index_info3, allocator_, true));
  ASSERT_TRUE(index_info3.is_filter_always_true());
}

ObPushdownFilterExecutor* TestSSTableIndexFilter::create_physical_filter(bool is_white)
{
  ObPushdownFilterExecutor* filter = nullptr;
  ObIAllocator* allocator_ptr = &allocator_;
  if (is_white) {
    ObPushdownWhiteFilterNode* white_node =
      OB_NEWx(ObPushdownWhiteFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObWhiteFilterExecutor, allocator_ptr, allocator_,
                      *white_node, *pushdown_operator_);
    white_node->col_ids_.init(1);
    filter->get_col_ids().push_back(TEST_COLUMN_ID);
    filter->n_cols_ = 1;
  } else {
    ObPushdownBlackFilterNode* black_node =
      OB_NEWx(ObPushdownBlackFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObBlackFilterExecutor, allocator_ptr, allocator_,
                      *black_node, *pushdown_operator_);
  }
  return filter;
}

ObPushdownFilterExecutor* TestSSTableIndexFilter::create_lt_white_filter(uint64_t value)
{
  ObWhiteFilterExecutor* filter = nullptr;
  ObIAllocator* allocator_ptr = &allocator_;
  ObPushdownWhiteFilterNode* white_node =
    OB_NEWx(ObPushdownWhiteFilterNode, allocator_ptr, allocator_);
  white_node->op_type_ = ObWhiteFilterOperatorType::WHITE_OP_LT;
  filter = OB_NEWx(ObWhiteFilterExecutor, allocator_ptr, allocator_,
                    *white_node, *pushdown_operator_);
  white_node->col_ids_.init(1);
  filter->get_col_ids().push_back(TEST_COLUMN_ID);
  filter->col_params_.init(1);
  filter->col_params_.push_back(nullptr);
  filter->col_offsets_.init(1);
  filter->col_offsets_.push_back(0);
  filter->n_cols_ = 1;
  void *expr_buf1 = allocator_.alloc(sizeof(sql::ObExpr));
  void *expr_buf2 = allocator_.alloc(sizeof(sql::ObExpr*));
  void *expr_buf3 = allocator_.alloc(sizeof(sql::ObExpr));
  filter->filter_.expr_ = reinterpret_cast<sql::ObExpr *>(expr_buf1);
  filter->filter_.expr_->arg_cnt_ = 1;
  filter->filter_.expr_->args_ = reinterpret_cast<sql::ObExpr **>(expr_buf2);
  filter->datum_params_.init(1);

  ObObj ref_obj;
  ref_obj.set_uint64(value);
  void *datum_buf = allocator_.alloc(128);
  ObDatum datum;
  ObObjMeta obj_meta;
  obj_meta.set_uint64();
  filter->filter_.expr_->args_[0] = reinterpret_cast<sql::ObExpr *>(expr_buf3);
  filter->filter_.expr_->args_[0]->obj_meta_ = obj_meta;
  datum.ptr_ = reinterpret_cast<char *>(datum_buf) + 128;
  datum.from_obj(ref_obj);
  filter->datum_params_.push_back(datum);
  filter->cmp_func_ = get_datum_cmp_func(obj_meta, obj_meta);
  return filter;
}

ObPushdownFilterExecutor* TestSSTableIndexFilter::create_logical_filter(bool is_and)
{
  ObPushdownFilterExecutor* filter = nullptr;
  ObIAllocator* allocator_ptr = &allocator_;
  if (is_and) {
    ObPushdownAndFilterNode* and_node = OB_NEWx(ObPushdownAndFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObAndFilterExecutor, allocator_ptr, allocator_,
                     *and_node, *pushdown_operator_);
  } else {
    ObPushdownOrFilterNode* or_node = OB_NEWx(ObPushdownOrFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObOrFilterExecutor, allocator_ptr, allocator_,
                     *or_node, *pushdown_operator_);
  }
  return filter;
}

ObPushdownFilterExecutor* TestSSTableIndexFilter::create_hybrid_filter_1(bool is_and)
{
  ObPushdownFilterExecutor *filter = create_logical_filter(is_and);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[3];
  childs[0] = create_physical_filter(true);
  childs[1] = create_physical_filter(true);
  childs[2] = create_physical_filter(false);
  filter->set_childs(3, childs);
  return filter;
}

ObPushdownFilterExecutor* TestSSTableIndexFilter::create_hybrid_filter_2()
{
  ObPushdownFilterExecutor *filter = create_logical_filter(true);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[3];
  childs[0] = create_hybrid_filter_1(false);
  childs[1] = create_physical_filter(true);
  childs[2] = create_physical_filter(false);
  filter->set_childs(3, childs);
  return filter;
}

ObPushdownFilterExecutor* TestSSTableIndexFilter::create_hybrid_filter_3()
{
  ObPushdownFilterExecutor *filter = create_logical_filter(true);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[3];
  childs[0] = create_physical_filter(false);
  childs[1] = create_physical_filter(false);
  childs[2] = create_physical_filter(false);
  filter->set_childs(3, childs);
  return filter;
}

void TestSSTableIndexFilter::init_micro_index_info(
    const ObObj &max_obj,
    const ObObj &min_obj,
    ObMicroIndexInfo &index_info)
{
  ObArray<ObSkipIndexColMeta> agg_cols;
  ObDatumRow agg_row;
  agg_row.init(3);

  ObSkipIndexColMeta skip_col_meta;
  skip_col_meta.col_idx_ = TEST_COLUMN_ID;
  skip_col_meta.col_type_ = SK_IDX_MIN;
  agg_cols.push_back(skip_col_meta);
  agg_row.storage_datums_[0].from_obj_enhance(min_obj);

  skip_col_meta.col_type_ = SK_IDX_MAX;
  agg_cols.push_back(skip_col_meta);
  agg_row.storage_datums_[1].from_obj_enhance(max_obj);

  skip_col_meta.col_type_ = SK_IDX_NULL_COUNT;
  agg_cols.push_back(skip_col_meta);
  ObObj null_count_obj;
  null_count_obj.set_int(0);
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

  index_info.agg_row_buf_ = buf;
  index_info.agg_buf_size_ = buf_size;
  index_info.row_header_ = &row_header_;
}

TEST_F(TestSSTableIndexFilter, test_bool_mask)
{
  ObBoolMask bm1(ObBoolMaskType::PROBABILISTIC);
  ObBoolMask bm2(ObBoolMaskType::ALWAYS_TRUE);
  ObBoolMask bm3(ObBoolMaskType::ALWAYS_FALSE);
  ASSERT_TRUE(bm1.is_uncertain());
  ASSERT_FALSE(bm2.is_uncertain());
  ASSERT_FALSE(bm3.is_uncertain());

  ASSERT_TRUE((bm1 & bm2).is_uncertain());
  ASSERT_TRUE((bm1 & bm3).is_always_false());
  ASSERT_TRUE((bm3 & bm2).is_always_false());
  ASSERT_TRUE((bm1 | bm2).is_always_true());
  ASSERT_TRUE((bm1 | bm3).is_uncertain());
  ASSERT_TRUE((bm2 | bm3).is_always_true());
}

TEST_F(TestSSTableIndexFilter, test_sstable_index_filter_extracter)
{
  ObPhysicalFilterExecutor *white_filter = static_cast<ObPhysicalFilterExecutor *>(create_physical_filter(true));
  ObPhysicalFilterExecutor *black_filter = static_cast<ObPhysicalFilterExecutor *>(create_physical_filter(false));
  ASSERT_TRUE(nullptr != white_filter);
  ASSERT_TRUE(nullptr != black_filter);
  ObSkippingFilterNode node1;
  ObSkippingFilterNode node2;
  ObSkipIndexType skip_index_type = ObSkipIndexType::MIN_MAX;
  OK(ObSSTableIndexFilterExtracter::extract_skipping_filter(*white_filter, skip_index_type, node1));
  OK(ObSSTableIndexFilterExtracter::extract_skipping_filter(*black_filter, skip_index_type, node2));
  ASSERT_TRUE(node1.is_useful());
  ASSERT_TRUE(node2.is_useful());
}

TEST_F(TestSSTableIndexFilter, test_skipping_filter_nodes_builder)
{
  test_skipping_filter_nodes_builder_1();
  test_skipping_filter_nodes_builder_2();
  test_skipping_filter_nodes_builder_3();
}

TEST_F(TestSSTableIndexFilter, test_sstable_index_filter_init)
{
  test_sstable_index_filter_init_1();
}

TEST_F(TestSSTableIndexFilter, test_sstable_index_filter_check_range)
{
  // Will be supported in the future when ddl supported.
  test_sstable_index_filter_check_range_1();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_index_filter.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_sstable_index_filter.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
