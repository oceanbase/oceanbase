/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>
#include <set>

#define private public
#define protected public

#include "storage/access/ob_vector_store.h"
#include "storage/access/ob_aggregated_store.h"
#include "storage/column_store/ob_virtual_cg_scanner.h"
#include "storage/column_store/ob_cg_scanner.h"
#include "storage/column_store/ob_cg_tile_scanner.h"
#include "storage/column_store/ob_co_sstable_rows_filter.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
using namespace storage;
using namespace blocksstable;

namespace unittest
{
class CGParamTrackingAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t size) override
  {
    void *ptr = allocations_left_-- > 0 ? arena_.alloc(size) : nullptr;
    if (nullptr != ptr) {
      live_allocations_.insert(ptr);
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      EXPECT_EQ(1, live_allocations_.erase(ptr));
      arena_.free(ptr);
    }
  }
  ObArenaAllocator arena_;
  std::set<void *> live_allocations_;
  int64_t allocations_left_ = INT64_MAX;
};

class CleanupTestCGScanner : public ObCGScanner
{
public:
  explicit CleanupTestCGScanner(int64_t &destruct_count) : destruct_count_(destruct_count) {}
  ~CleanupTestCGScanner() override { ++destruct_count_; }
  int init(const ObTableIterParam &param, ObTableAccessContext &context, ObSSTableWrapper &wrapper) override
  {
    iter_param_ = &param;
    access_ctx_ = &context;
    return OB_SUCCESS;
  }
  int64_t &destruct_count_;
};

class CleanupTestCOSSTable : public ObCOSSTableV2
{
public:
  CleanupTestCOSSTable()
  {
    valid_for_cs_reading_ = true;
    base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
    key_.column_group_idx_ = 0;
    cs_meta_.column_group_cnt_ = 2;
  }
  int fetch_cg_sstable(const uint32_t cg_idx, ObSSTableWrapper &wrapper) const override { return OB_SUCCESS; }
};

class MockObCOSSTableRowsFilter : public ObCOSSTableRowsFilter
{
public:
  int rewrite_filter()
  {
    int ret = OB_SUCCESS;
    uint32_t depth = 0;
    if (OB_FAIL(judge_whether_use_common_cg_iter(filter_))) {
      LOG_WARN("Failed to judge where use common column group iterator",
                K(ret), KPC_(filter));
    } else if (OB_FAIL(rewrite_filter_tree(filter_, depth))) {
      LOG_WARN("Failed to rewrite filter",
                K(ret), KPC_(filter));
    } else if (OB_UNLIKELY(depth < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected depth", K(ret), K(depth), KPC_(filter));
    } else if (OB_FAIL(extend_bitmap_buffer(depth))) {
      LOG_WARN("Failed to init bitmap buffer", K(ret), K(depth));
    }
    return ret;
  }

  int rewrite_filter_tree(
      sql::ObPushdownFilterExecutor *filter,
      uint32_t &depth)
  {
    int ret = OB_SUCCESS;
    depth = 1;
    if (OB_UNLIKELY(nullptr == filter
       || sql::ObPushdownFilterExecutor::INVALID_CG_ITER_IDX
            != filter->get_cg_iter_idx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected filter", K(ret), KPC(filter));
    } else if (filter->is_filter_node()) {
      if (OB_FAIL(push_cg_iter(filter))) {
        LOG_WARN("Failed to produce new coulmn group iterator",
                  K(ret), KPC(filter));
      }
    } else if (filter->is_logic_op_node()) {
      if (OB_UNLIKELY(filter->get_child_count() < 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected number of child in filter executor", K(ret),
                  K(filter->get_child_count()), KP(filter));
      } else if (!filter->get_cg_idxs().empty()) {
        if (OB_FAIL(push_cg_iter(filter))) {
          LOG_WARN("Failed to produce new coulmn group iterator",
                    K(ret), KPC(filter));
        }
      } else {
        sql::ObPushdownFilterExecutor **children = filter->get_childs();
        uint32_t max_sub_tree_depth = 0;
        uint32_t sub_tree_depth = 0;
        for (uint32_t i = 0; OB_SUCC(ret) && i < filter->get_child_count(); ++i) {
          if (OB_FAIL(rewrite_filter_tree(children[i], sub_tree_depth))) {
            LOG_WARN("Failed to rewrite filter", K(ret), K(i), KPC(children[i]));
          } else {
            max_sub_tree_depth = MAX(max_sub_tree_depth, sub_tree_depth);
          }
        }
        if (OB_SUCC(ret)) {
          depth = max_sub_tree_depth + 1;
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unspported executor type", K(ret), K(filter->get_type()));
    }
    return ret;
  }

  int push_cg_iter(sql::ObPushdownFilterExecutor *filter)
  {
    int ret = OB_SUCCESS;
    ObICGIterator *cg_iter = nullptr;
    const common::ObIArray<uint32_t> &col_group_idxs = filter->get_cg_idxs();
    if (1 == col_group_idxs.count()) {
      cg_iter = OB_NEWx(ObCGScanner, allocator_);
    } else {
      cg_iter = OB_NEWx(ObCGTileScanner, allocator_);
    }
    if (OB_FAIL(filter_iters_.push_back(cg_iter))) {
      LOG_WARN("Failed to push cg iter", K(ret));
    } else if (OB_FAIL(iter_filter_node_.push_back(filter))) {
      LOG_WARN("Failed to push filter node", K(ret));
    } else {
      filter->set_cg_iter_idx(filter_iters_.count() - 1);
    }
    return ret;
  }
};

class TestCOSSTableRowsFilter : public ::testing::Test
{
public:
  TestCOSSTableRowsFilter() = default;
  ~TestCOSSTableRowsFilter() = default;
public:
  virtual void SetUp() {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  };
  virtual void TearDown() {};
  static void SetUpTestCase() {};
  static void TearDownTestCase() {};

  void init_vector_store();
  void init_iter_param();
  void init_table_access_context();
  void init_filter_param();
  void init_all();
  void init_single_white_filter();
  void init_single_black_filter();
  void init_multi_white_filter(bool is_common);
  void init_multi_black_filter(bool is_common);
  void init_multi_white_and_black_filter_case_one();
  void init_multi_white_and_black_filter_case_two();
  void reset_filter();
  ObPushdownFilterExecutor* create_physical_filter(
      const ObSEArray<uint32_t, 4> &_cg_idxes,
      bool is_white);
  ObPushdownFilterExecutor* create_logical_filter(
      bool is_and);
public:
  ObArenaAllocator allocator_;
  ObTableIterParam iter_param_;
  ObTableAccessContext context_;
  ObCOSSTableV2 co_sstable_;
  ObSEArray<ObColDesc, 3> col_descs_;
  ObTableReadInfo read_info_;
  ObExecContext *exec_ctx_;
  ObEvalCtx *eval_ctx_;
  ObVectorStore *vector_store_;
  ObPushdownFilterExecutor *filter_;
  ObPushdownOperator *pushdown_operator_;
  ObPushdownExprSpec *expr_spec_;
  ObBitVector *skip_bit_;
  MockObCOSSTableRowsFilter co_filter_;
};

TEST_F(TestCOSSTableRowsFilter, skip_index_failure_destroys_scanner)
{
  init_all();
  init_single_white_filter();
  CGParamTrackingAllocator failing_allocator;
  ObStoreCtx store_ctx;
  ObCGIterParamPool param_pool(allocator_);
  int64_t destruct_count = 0;
  ObStoreRowIterPool<ObICGIterator> iter_pool(allocator_);
  CleanupTestCOSSTable sstable;
  ObCOSSTableRowsFilter rows_filter;
  rows_filter.iter_param_ = &iter_param_;
  rows_filter.access_ctx_ = &context_;
  rows_filter.co_sstable_ = &sstable;
  rows_filter.allocator_ = &allocator_;
  context_.store_ctx_ = &store_ctx;
  context_.allocator_ = &failing_allocator;
  context_.cg_param_pool_ = &param_pool;
  context_.cg_iter_pool_ = &iter_pool;

  // Cache a parameter and scanner so the only failing allocation is the skip index filter.
  ObTableIterParam *cg_param = OB_NEWx(ObTableIterParam, &allocator_);
  ASSERT_NE(nullptr, cg_param);
  ExprFixedArray *exprs = OB_NEWx(ExprFixedArray, &allocator_, allocator_);
  ASSERT_NE(nullptr, exprs);
  ASSERT_EQ(OB_SUCCESS, exprs->init(0));
  cg_param->cg_idx_ = 1;
  cg_param->output_exprs_ = exprs;
  cg_param->pd_storage_flag_.set_enable_base_skip_index(true);
  ASSERT_EQ(OB_SUCCESS, param_pool.put_iter_param(cg_param));
  CleanupTestCGScanner *scanner = OB_NEWx(CleanupTestCGScanner, &allocator_, destruct_count);
  ASSERT_NE(nullptr, scanner);
  iter_pool.return_cg_iter(scanner, 1);
  ASSERT_EQ(1, iter_pool.table_iters_array_.count());
  iter_pool.table_iters_array_.at(0)->type_info_ = &typeid(ObCGScanner);
  failing_allocator.allocations_left_ = 0;

  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, rows_filter.push_cg_iter(filter_));
  EXPECT_EQ(1, destruct_count);
  EXPECT_EQ(0, rows_filter.filter_iters_.count());
  EXPECT_EQ(0, rows_filter.iter_filter_node_.count());
  rows_filter.reset();
  iter_pool.reset();
  EXPECT_EQ(1, destruct_count);
  context_.store_ctx_ = nullptr;
  context_.allocator_ = nullptr;
  context_.cg_param_pool_ = nullptr;
  context_.cg_iter_pool_ = nullptr;
  reset_filter();
}

TEST_F(TestCOSSTableRowsFilter, cg_param_failure_cleanup)
{
  init_iter_param();
  ASSERT_EQ(OB_SUCCESS, read_info_.cols_param_.init(1, allocator_));
  ASSERT_EQ(OB_SUCCESS, read_info_.cols_param_.push_back(nullptr));
  ObSEArray<ObExpr *, 1> exprs;
  ObExpr expr;
  ASSERT_EQ(OB_SUCCESS, exprs.push_back(&expr));
  ObSEArray<ObTableReadInfo *, 1> cg_read_infos;
  ASSERT_EQ(OB_SUCCESS, cg_read_infos.push_back(nullptr));
  iter_param_.cg_read_infos_ = &cg_read_infos;

  // Reject a missing CG list, an unknown CG, or a null read info after ownership transfer.
  for (int64_t failure_case = 0; failure_case < 3; ++failure_case) {
    if (1 == failure_case) {
      ASSERT_EQ(OB_SUCCESS, read_info_.cg_idxs_.init(1, allocator_));
      ASSERT_EQ(OB_SUCCESS, read_info_.cg_idxs_.push_back(1));
    }
    const int32_t cg_idx = 2 == failure_case ? 1 : 2;
    for (int64_t aggregate = 0; aggregate < 2; ++aggregate) {
      // Also fail each allocation leading up to parameter validation.
      for (int64_t allowed_allocations = 0; allowed_allocations <= 8; ++allowed_allocations) {
        CGParamTrackingAllocator allocator;
        ObCGIterParamPool pool(allocator);
        allocator.allocations_left_ = allowed_allocations;
        ObTableIterParam *cg_param = nullptr;
        const int ret = pool.get_iter_param(cg_idx, iter_param_, exprs, cg_param, aggregate ? &exprs : nullptr);
        EXPECT_TRUE(OB_ERR_UNEXPECTED == ret || OB_ALLOCATE_MEMORY_FAILED == ret);
        if (8 == allowed_allocations) {
          EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
        }
        EXPECT_EQ(nullptr, cg_param);
        EXPECT_EQ(0, pool.iter_params_.count());
        EXPECT_TRUE(allocator.live_allocations_.empty());
        pool.reset();
        EXPECT_TRUE(allocator.live_allocations_.empty());
      }
    }
  }
  cg_read_infos.at(0) = &read_info_;
  for (int64_t aggregate = 0; aggregate < 2; ++aggregate) {
    CGParamTrackingAllocator allocator;
    ObCGIterParamPool pool(allocator);
    ObTableIterParam *cg_param = nullptr;
    ASSERT_EQ(OB_SUCCESS, pool.get_iter_param(1, iter_param_, exprs, cg_param, aggregate ? &exprs : nullptr));
    ASSERT_NE(nullptr, cg_param);
    EXPECT_EQ(&expr, cg_param->output_exprs_->at(0));
    EXPECT_EQ(&read_info_, cg_param->read_info_);
    EXPECT_FALSE(allocator.live_allocations_.empty());
    pool.reset();
    EXPECT_TRUE(allocator.live_allocations_.empty());
  }
  iter_param_.cg_read_infos_ = nullptr;
}

TEST_F(TestCOSSTableRowsFilter, reused_aggregate_group_failure)
{
  init_vector_store();
  ObAggregatedStore agg_store(64, *eval_ctx_, context_);
  context_.stmt_allocator_ = &allocator_;
  context_.block_row_store_ = &agg_store;
  ExprFixedArray exprs(allocator_);
  ObExpr expr;
  ASSERT_EQ(OB_SUCCESS, exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, exprs.push_back(&expr));
  iter_param_.aggregate_exprs_ = &exprs;
  iter_param_.pd_storage_flag_.set_aggregate_pushdown(true);
  iter_param_.plan_enable_rich_format_ = false;

  // An existing group is reused, then get_agg_cell fails on the uninitialized store.
  ObVirtualCGScanner virtual_scanner;
  virtual_scanner.access_ctx_ = &context_;
  virtual_scanner.agg_group_ = OB_NEWx(ObCGAggCells, &allocator_);
  ASSERT_NE(nullptr, virtual_scanner.agg_group_);
  EXPECT_EQ(OB_NOT_INIT, virtual_scanner.init_agg_group(iter_param_, context_));
  ASSERT_EQ(nullptr, virtual_scanner.agg_group_);
  virtual_scanner.reset();

  ObDefaultCGScanner default_scanner;
  default_scanner.stmt_allocator_ = &allocator_;
  default_scanner.agg_group_ = OB_NEWx(ObCGAggCells, &allocator_);
  ASSERT_NE(nullptr, default_scanner.agg_group_);
  EXPECT_EQ(OB_NOT_INIT, default_scanner.init_agg_group(iter_param_, context_));
  ASSERT_EQ(nullptr, default_scanner.agg_group_);
  default_scanner.reset();
  context_.block_row_store_ = nullptr;
  iter_param_.aggregate_exprs_ = nullptr;
}

void TestCOSSTableRowsFilter::init_vector_store()
{
  ObIAllocator* allocator_ptr = &allocator_;
  exec_ctx_ = OB_NEWx(ObExecContext, allocator_ptr, allocator_);
  eval_ctx_ = OB_NEWx(ObEvalCtx, allocator_ptr, *exec_ctx_);
  skip_bit_ = to_bit_vector(allocator_.alloc(ObBitVector::memory_size(64)));
  ASSERT_NE(nullptr, skip_bit_);
  vector_store_ = OB_NEWx(ObVectorStore, allocator_ptr, 64, *eval_ctx_, context_, skip_bit_);
  vector_store_->is_inited_ = true;
}

void TestCOSSTableRowsFilter::init_iter_param()
{
  ObColDesc col_desc;
  col_desc.col_id_ = 1;
  col_desc.col_type_.set_int();
  col_descs_.reset();
  read_info_.reset();
  ASSERT_EQ(OB_SUCCESS, col_descs_.push_back(col_desc));
  ASSERT_EQ(OB_SUCCESS, col_descs_.push_back(col_desc));
  ASSERT_EQ(OB_SUCCESS, storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(col_descs_));
  ASSERT_EQ(OB_SUCCESS, read_info_.init(allocator_, 16000, 1, lib::is_oracle_mode(), col_descs_, nullptr, nullptr));
  iter_param_.table_id_ = 1;
  iter_param_.tablet_id_.id_ = 1;
  iter_param_.read_info_ = &read_info_;
}

void TestCOSSTableRowsFilter::init_table_access_context()
{
  context_.stmt_allocator_ = &allocator_;
  context_.block_row_store_ = vector_store_;
  co_sstable_.key_.table_type_ = ObITable::TableType::COLUMN_ORIENTED_SSTABLE;
  context_.is_inited_ = true;
}

void TestCOSSTableRowsFilter::init_filter_param()
{
  ObIAllocator* allocator_ptr = &allocator_;
  expr_spec_ = OB_NEWx(ObPushdownExprSpec, allocator_ptr, allocator_);
  pushdown_operator_ = OB_NEWx(ObPushdownOperator, allocator_ptr, *eval_ctx_, *expr_spec_);
}

void TestCOSSTableRowsFilter::init_all()
{
  init_vector_store();
  init_iter_param();
  init_table_access_context();
  init_filter_param();
}

ObPushdownFilterExecutor* TestCOSSTableRowsFilter::create_physical_filter(
    const ObSEArray<uint32_t, 4> &_cg_idxes,
    bool is_white)
{
  ObPushdownFilterExecutor* filter = nullptr;
  ObIAllocator* allocator_ptr = &allocator_;
  ExprFixedArray *column_exprs = nullptr;
  if (is_white) {
    ObPushdownWhiteFilterNode* white_node =
      OB_NEWx(ObPushdownWhiteFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObWhiteFilterExecutor, allocator_ptr, allocator_,
                       *white_node, pushdown_operator_);
    column_exprs = &(white_node->column_exprs_);
  } else {
    ObPushdownBlackFilterNode* black_node =
      OB_NEWx(ObPushdownBlackFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObBlackFilterExecutor, allocator_ptr, allocator_,
                      *black_node, pushdown_operator_);
    column_exprs = &(black_node->column_exprs_);
  }
  common::ObFixedArray<uint32_t, common::ObIAllocator> &cg_idxes = filter->cg_idxs_;
  common::ObIArray<ObExpr *> *cg_col_exprs = new ObSEArray<ObExpr *, 4>();
  column_exprs->init(_cg_idxes.count());
  cg_idxes.init(_cg_idxes.count());
  for (int i = 0; i < _cg_idxes.count(); ++i) {
    cg_col_exprs->push_back(nullptr);
    column_exprs->push_back(nullptr);
    cg_idxes.push_back(_cg_idxes.at(i));
  }
  filter->cg_col_exprs_.assign(*cg_col_exprs);
  return filter;
}

ObPushdownFilterExecutor* TestCOSSTableRowsFilter::create_logical_filter(
    bool is_and)
{
  ObPushdownFilterExecutor* filter = nullptr;
  ObIAllocator* allocator_ptr = &allocator_;
  if (is_and) {
    ObPushdownAndFilterNode* and_node = OB_NEWx(ObPushdownAndFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObAndFilterExecutor, allocator_ptr, allocator_,
                     *and_node, pushdown_operator_);
  } else {
    ObPushdownOrFilterNode* or_node = OB_NEWx(ObPushdownOrFilterNode, allocator_ptr, allocator_);
    filter = OB_NEWx(ObOrFilterExecutor, allocator_ptr, allocator_,
                     *or_node, pushdown_operator_);
  }
  return filter;
}

void TestCOSSTableRowsFilter::init_single_white_filter()
{
  ObSEArray<uint32_t, 4> cg_idxes;
  cg_idxes.push_back(1);
  filter_ = create_physical_filter(cg_idxes, true);
  ASSERT_FALSE(nullptr == filter_);
  co_filter_.filter_ = filter_;
  co_filter_.allocator_ = &allocator_;
  co_filter_.access_ctx_ = &context_;
}

void TestCOSSTableRowsFilter::init_single_black_filter()
{
  ObSEArray<uint32_t, 4> cg_idxes;
  cg_idxes.push_back(1);
  cg_idxes.push_back(3);
  filter_ = create_physical_filter(cg_idxes, false);
  ASSERT_FALSE(nullptr == filter_);
  co_filter_.filter_ = filter_;
  co_filter_.allocator_ = &allocator_;
  co_filter_.access_ctx_ = &context_;
}

void TestCOSSTableRowsFilter::init_multi_white_filter(bool is_common)
{
  filter_ = create_logical_filter(true);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[3];
  if (is_common) {
    ObSEArray<uint32_t, 4> cg_idxes;
    cg_idxes.push_back(1);
    for (int i = 0; i < 3; ++i) {
      childs[i] = create_physical_filter(cg_idxes, true);
    }
  } else {
    for (int i = 0; i < 3; ++i) {
      ObSEArray<uint32_t, 4> cg_idxes;
      cg_idxes.push_back(i + 1);
      childs[i] = create_physical_filter(cg_idxes, true);
    }
  }
  filter_->set_childs(3, childs);
  co_filter_.filter_ = filter_;
  co_filter_.allocator_ = &allocator_;
  co_filter_.access_ctx_ = &context_;
}

void TestCOSSTableRowsFilter::init_multi_black_filter(bool is_common)
{
  filter_ = create_logical_filter(true);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[3];
  if (is_common) {
    ObSEArray<uint32_t, 4> cg_idxes;
    cg_idxes.push_back(1);
    childs[0] = create_physical_filter(cg_idxes, false);
    cg_idxes.reuse();
    cg_idxes.push_back(1);
    cg_idxes.push_back(3);
    childs[1] = create_physical_filter(cg_idxes, false);
    cg_idxes.reuse();
    cg_idxes.push_back(1);
    cg_idxes.push_back(3);
    cg_idxes.push_back(7);
    childs[2] = create_physical_filter(cg_idxes, false);
  } else {
    for (int i = 0; i < 3; ++i) {
      ObSEArray<uint32_t, 4> cg_idxes;
      cg_idxes.push_back(i + 1);
      childs[i] = create_physical_filter(cg_idxes, false);
    }
  }
  filter_->set_childs(3, childs);
  co_filter_.filter_ = filter_;
  co_filter_.allocator_ = &allocator_;
  co_filter_.access_ctx_ = &context_;
}

void TestCOSSTableRowsFilter::init_multi_white_and_black_filter_case_one()
{
  filter_ = create_logical_filter(true);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[2];
  childs[0] = create_logical_filter(true);
  ObPushdownFilterExecutor **sub_childs = new ObPushdownFilterExecutor*[2];
  ObSEArray<uint32_t, 4> cg_idxes;
  cg_idxes.push_back(1);
  sub_childs[0] = create_physical_filter(cg_idxes, true);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  cg_idxes.push_back(3);
  sub_childs[1] = create_physical_filter(cg_idxes, false);
  cg_idxes.reuse();
  childs[0]->set_childs(2, sub_childs);

  childs[1] = create_logical_filter(true);
  ObPushdownFilterExecutor **sub_childs2 = new ObPushdownFilterExecutor*[2];
  cg_idxes.push_back(1);
  sub_childs2[0] = create_physical_filter(cg_idxes, false);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  cg_idxes.push_back(3);
  sub_childs2[1] = create_physical_filter(cg_idxes, false);
  cg_idxes.reuse();
  childs[1]->set_childs(2, sub_childs2);

  filter_->set_childs(2, childs);
  co_filter_.filter_ = filter_;
  co_filter_.allocator_ = &allocator_;
  co_filter_.access_ctx_ = &context_;
}

void TestCOSSTableRowsFilter::init_multi_white_and_black_filter_case_two()
{
  filter_ = create_logical_filter(true);
  ObPushdownFilterExecutor **childs = new ObPushdownFilterExecutor*[3];
  childs[0] = create_logical_filter(true);
  ObPushdownFilterExecutor **sub_childs = new ObPushdownFilterExecutor*[3];
  ObSEArray<uint32_t, 4> cg_idxes;
  cg_idxes.push_back(1);
  sub_childs[0] = create_physical_filter(cg_idxes, true);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  cg_idxes.push_back(3);
  sub_childs[1] = create_physical_filter(cg_idxes, false);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  cg_idxes.push_back(3);
  cg_idxes.push_back(5);
  sub_childs[2] = create_physical_filter(cg_idxes, false);
  childs[0]->set_childs(3, sub_childs);

  childs[1] = create_logical_filter(true);
  ObPushdownFilterExecutor **sub_childs2 = new ObPushdownFilterExecutor*[3];
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  sub_childs2[0] = create_physical_filter(cg_idxes, true);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  sub_childs2[1] = create_physical_filter(cg_idxes, true);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  sub_childs2[2] = create_physical_filter(cg_idxes, false);
  childs[1]->set_childs(3, sub_childs2);

  childs[2] = create_logical_filter(true);
  ObPushdownFilterExecutor **sub_childs3 = new ObPushdownFilterExecutor*[3];
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  sub_childs3[0] = create_physical_filter(cg_idxes, false);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  sub_childs3[1] = create_physical_filter(cg_idxes, false);
  cg_idxes.reuse();
  cg_idxes.push_back(1);
  sub_childs3[2] = create_physical_filter(cg_idxes, false);
  childs[2]->set_childs(3, sub_childs3);

  filter_->set_childs(3, childs);
  co_filter_.filter_ = filter_;
  co_filter_.allocator_ = &allocator_;
  co_filter_.access_ctx_ = &context_;
}

void TestCOSSTableRowsFilter::reset_filter()
{
  filter_->~ObPushdownFilterExecutor();
  co_filter_.reset();
}

TEST_F(TestCOSSTableRowsFilter, co_sstable_rows_filter_test_init)
{
  int ret = OB_SUCCESS;
  ret = co_filter_.init(iter_param_, context_, &co_sstable_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  init_all();
  ret = co_filter_.init(iter_param_, context_, &co_sstable_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestCOSSTableRowsFilter, co_sstable_rows_filter_test_rewrite_filter_case_one)
{
  int ret = OB_SUCCESS;
  init_all();
  init_single_white_filter();
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, co_filter_.filter_iters_.count());
  ASSERT_EQ(1, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(1, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  reset_filter();

  init_single_black_filter();
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, co_filter_.filter_iters_.count());
  ASSERT_EQ(1, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(1, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_TILE_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  reset_filter();

  init_multi_white_filter(true);
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, co_filter_.filter_iters_.count());
  ASSERT_EQ(1, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(1, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  reset_filter();

  init_multi_white_filter(false);
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, co_filter_.filter_iters_.count());
  ASSERT_EQ(3, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(2, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[1]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[2]->get_type());
  reset_filter();

  init_multi_black_filter(true);
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, co_filter_.filter_iters_.count());
  ASSERT_EQ(1, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(1, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_TILE_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  reset_filter();

  init_multi_black_filter(false);
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, co_filter_.filter_iters_.count());
  ASSERT_EQ(3, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(2, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[1]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[2]->get_type());
  reset_filter();

  init_multi_white_and_black_filter_case_one();
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, co_filter_.filter_iters_.count());
  ASSERT_EQ(3, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(3, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_TILE_SCANNER,
             co_filter_.filter_iters_[1]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_TILE_SCANNER,
             co_filter_.filter_iters_[2]->get_type());
  reset_filter();
}

TEST_F(TestCOSSTableRowsFilter, co_sstable_rows_filter_test_rewrite_filter_case_two)
{
  int ret = OB_SUCCESS;
  init_all();
  init_multi_white_and_black_filter_case_two();
  ret = co_filter_.rewrite_filter();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, co_filter_.filter_iters_.count());
  ASSERT_EQ(5, co_filter_.iter_filter_node_.count());
  ASSERT_EQ(3, co_filter_.bitmap_buffer_.count());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[0]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_TILE_SCANNER,
             co_filter_.filter_iters_[1]->get_type());
  ASSERT_EQ(ObICGIterator::ObCGIterType::OB_CG_SCANNER,
             co_filter_.filter_iters_[2]->get_type());
  reset_filter();
}

} //namespace unittest
} //namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_co_sstable_rows_filter.log");
  OB_LOGGER.set_file_name("test_co_sstable_rows_filter.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
