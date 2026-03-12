/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "sql/das/iter/ob_das_fusion_iter.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/datum/ob_datum.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_obj_type.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace
{

/**
 * Mock child iterator for testing ObDASFusionIter
 * Provides rows with rowkey and score expressions
 */
class MockDASChildIter : public ObDASIter
{
public:
  struct MockRow {
    uint64_t rowkey_val;  // Store value instead of ObRowkey to avoid memory issues
    double score;

    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV("rowkey_val", rowkey_val, "score", score);
      J_OBJ_END();
      return pos;
    }
  };

  MockDASChildIter(const ObIArray<MockRow> &rows, ObArenaAllocator *alloc)
      : ObDASIter(ObDASIterType::DAS_ITER_SCAN),
        cur_idx_(-1),
        eval_ctx_(nullptr),
        output_exprs_(),
        rowid_exprs_(),
        score_expr_(nullptr),
        allocator_(alloc),
        rowkey_objs_(),
        rows_()
  {
    // Copy rows
    for (int64_t i = 0; i < rows.count(); ++i) {
      rows_.push_back(rows.at(i));
    }
    // Pre-allocate ObObj for rowkeys
    if (OB_NOT_NULL(allocator_)) {
      for (int64_t i = 0; i < rows_.count(); ++i) {
        void *buf = allocator_->alloc(sizeof(ObObj));
        if (OB_NOT_NULL(buf)) {
          ObObj *obj = new (buf) ObObj();
          obj->set_uint64(rows_.at(i).rowkey_val);
          rowkey_objs_.push_back(obj);
        }
      }
    }
  }

  virtual ~MockDASChildIter()
  {
    release();
  }

  void set_eval_ctx(ObEvalCtx *eval_ctx) { eval_ctx_ = eval_ctx; }
  void set_output_exprs(const ObIArray<ObExpr*> &exprs)
  {
    output_exprs_.reset();
    for (int64_t i = 0; i < exprs.count(); ++i) {
      output_exprs_.push_back(exprs.at(i));
    }
  }
  void set_rowid_exprs(const ObIArray<ObExpr*> &exprs)
  {
    rowid_exprs_.reset();
    for (int64_t i = 0; i < exprs.count(); ++i) {
      rowid_exprs_.push_back(exprs.at(i));
    }
  }
  void set_score_expr(ObExpr *expr) { score_expr_ = expr; }

  virtual int do_table_scan() override { return OB_SUCCESS; }
  virtual int rescan() override { cur_idx_ = -1; return OB_SUCCESS; }
  virtual void clear_evaluated_flag() override {}

protected:
  virtual int inner_init(ObDASIterParam &param) override { return OB_SUCCESS; }
  virtual int inner_reuse() override { cur_idx_ = -1; return OB_SUCCESS; }
  virtual int inner_release() override { return OB_SUCCESS; }

  virtual int inner_get_next_row() override
  {
    int ret = OB_SUCCESS;
    cur_idx_++;
    if (cur_idx_ >= rows_.count()) {
      return OB_ITER_END;
    }

    if (OB_ISNULL(eval_ctx_)) {
      return OB_ERR_UNEXPECTED;
    }

    // Ensure batch_idx_ is 0 for single-row mode
    // This is important for locate_expr_datum() to work correctly in extract_rowkey()
    eval_ctx_->batch_idx_ = 0;
    eval_ctx_->batch_size_ = 1;

    // Initialize vectors for single row mode (child iter should initialize vectors)
    // IMPORTANT: Use VEC_UNIFORM_CONST format for single row mode
    // VEC_UNIFORM_CONST is compatible with all value_tc types
    for (int64_t j = 0; OB_SUCC(ret) && j < rowid_exprs_.count(); ++j) {
      ObExpr *expr = rowid_exprs_.at(j);
      if (OB_NOT_NULL(expr) && OB_FAIL(expr->init_vector(*eval_ctx_, VEC_UNIFORM_CONST, 1))) {
        LOG_WARN("failed to init vector for rowkey expr", K(ret), K(j));
      }
    }
    if (OB_NOT_NULL(score_expr_) && OB_SUCC(ret)) {
      if (OB_FAIL(score_expr_->init_vector(*eval_ctx_, VEC_UNIFORM_CONST, 1))) {
        LOG_WARN("failed to init vector for score expr", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < output_exprs_.count(); ++j) {
      ObExpr *expr = output_exprs_.at(j);
      if (OB_NOT_NULL(expr) && OB_FAIL(expr->init_vector(*eval_ctx_, VEC_UNIFORM_CONST, 1))) {
        LOG_WARN("failed to init vector for output expr", K(ret), K(j));
      }
    }

    if (OB_FAIL(ret)) {
      return ret;
    }

    const MockRow &row = rows_[cur_idx_];

    // Set rowkey expressions
    if (rowid_exprs_.count() > 0) {
      for (int64_t i = 0; i < rowid_exprs_.count(); ++i) {
        ObExpr *expr = rowid_exprs_.at(i);
        int64_t batch_idx = eval_ctx_->get_batch_idx();
        char *frame = eval_ctx_->frames_[expr->frame_idx_];
        int64_t datum_idx = expr->get_datum_idx(*eval_ctx_);
        char *expected_data_pos = frame + expr->res_buf_off_ + expr->res_buf_len_ * datum_idx;

        ObDatum &datum = expr->locate_datum_for_write(*eval_ctx_);
        datum.set_uint(row.rowkey_val);

        expr->set_evaluated_projected(*eval_ctx_);
      }
    }

    // Set score expression
    if (OB_NOT_NULL(score_expr_)) {
      ObDatum &datum = score_expr_->locate_datum_for_write(*eval_ctx_);
      datum.set_double(row.score);
      score_expr_->set_evaluated_projected(*eval_ctx_);
      if (OB_FAIL(ret)) {
        return ret;
      }
    }

    return ret;
  }

  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override
  {
    count = 0;
    if (cur_idx_ < 0) {
      cur_idx_ = 0;
    }

    if (OB_ISNULL(eval_ctx_)) {
      return OB_ERR_UNEXPECTED;
    }

    int64_t batch_size = OB_MIN(capacity, rows_.count() - cur_idx_);
    if (batch_size <= 0) {
      return OB_ITER_END;
    }

    // Set batch size (DO NOT modify max_batch_size_, it should remain constant)
    eval_ctx_->batch_idx_ = 0;
    eval_ctx_->batch_size_ = batch_size;

    int ret = OB_SUCCESS;
    const int64_t max_batch_size = eval_ctx_->max_batch_size_;

    // Initialize vectors for all expressions (child iter should initialize vectors)
    // This ensures vectors are ready before data is written to them
    // IMPORTANT: Use VEC_UNIFORM format for batch mode to ensure compatibility with all value_tc types
    for (int64_t j = 0; OB_SUCC(ret) && j < rowid_exprs_.count(); ++j) {
      ObExpr *expr = rowid_exprs_.at(j);
      if (OB_NOT_NULL(expr) && OB_FAIL(expr->init_vector(*eval_ctx_, VEC_UNIFORM, max_batch_size))) {
        LOG_WARN("failed to init vector for rowkey expr", K(ret), K(j));
      }
    }
    if (OB_NOT_NULL(score_expr_) && OB_SUCC(ret)) {
      if (OB_FAIL(score_expr_->init_vector(*eval_ctx_, VEC_UNIFORM, max_batch_size))) {
        LOG_WARN("failed to init vector for score expr", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < output_exprs_.count(); ++j) {
      ObExpr *expr = output_exprs_.at(j);
      if (OB_NOT_NULL(expr) && OB_FAIL(expr->init_vector(*eval_ctx_, VEC_UNIFORM, max_batch_size))) {
        LOG_WARN("failed to init vector for output expr", K(ret), K(j));
      }
    }

    if (OB_FAIL(ret)) {
      return ret;
    }

    // Get datums pointers ONCE before the loop (more efficient and correct)
    ObSEArray<ObDatum*, 8> rowkey_datums;
    ObDatum *score_datums = nullptr;
    ObSEArray<ObDatum*, 8> output_datums;

    // Allocate datums arrays for rowkey expressions
    for (int64_t j = 0; j < rowid_exprs_.count(); ++j) {
      ObExpr *expr = rowid_exprs_.at(j);
      ObDatum *datums = expr->locate_datums_for_update(*eval_ctx_, max_batch_size);
      if (OB_NOT_NULL(datums)) {
        rowkey_datums.push_back(datums);
      } else {
        rowkey_datums.push_back(nullptr);
      }
    }

    // Allocate datums array for score expression
    if (OB_NOT_NULL(score_expr_)) {
      score_datums = score_expr_->locate_datums_for_update(*eval_ctx_, max_batch_size);
    }

    // Allocate datums arrays for output expressions
    for (int64_t j = 0; j < output_exprs_.count(); ++j) {
      ObExpr *expr = output_exprs_.at(j);
      ObDatum *datums = expr->locate_datums_for_update(*eval_ctx_, max_batch_size);
      if (OB_NOT_NULL(datums)) {
        output_datums.push_back(datums);
      } else {
        output_datums.push_back(nullptr);
      }
    }

    // Now set values for each row in the batch
    for (int64_t i = 0; i < batch_size; ++i) {
      const MockRow &row = rows_[cur_idx_ + i];

      // Set rowkey expressions (batch mode)
      for (int64_t j = 0; j < rowkey_datums.count(); ++j) {
        ObDatum *datums = rowkey_datums.at(j);
        if (OB_NOT_NULL(datums)) {
          datums[i].set_uint(row.rowkey_val);
        }
      }

      // Set score expression (batch mode)
      if (OB_NOT_NULL(score_datums)) {
        score_datums[i].set_double(row.score);
      }
    }

    // Mark expressions as evaluated
    for (int64_t i = 0; i < rowid_exprs_.count(); ++i) {
      rowid_exprs_.at(i)->set_evaluated_projected(*eval_ctx_);
    }
    if (OB_NOT_NULL(score_expr_)) {
      score_expr_->set_evaluated_projected(*eval_ctx_);
    }
    for (int64_t i = 0; i < output_exprs_.count(); ++i) {
      output_exprs_.at(i)->set_evaluated_projected(*eval_ctx_);
    }

    cur_idx_ += batch_size;
    count = batch_size;
    return OB_SUCCESS;
  }

private:
  int64_t cur_idx_;
  ObEvalCtx *eval_ctx_;
  ObSEArray<ObExpr*, 8> output_exprs_;
  ObSEArray<ObExpr*, 2> rowid_exprs_;
  ObExpr *score_expr_;
  ObArenaAllocator *allocator_;
  ObSEArray<ObObj*, 8> rowkey_objs_;  // Store allocated ObObj for rowkeys
  ObSEArray<MockRow, 32> rows_;  // Store rows
};

/**
 * Test fixture for ObDASFusionIter
 */
class ObDASFusionIterTest : public ::testing::Test
{
protected:
  ObDASFusionIterTest()
      : allocator_("TestDASFusion"),
        exec_ctx_(allocator_),
        eval_ctx_(exec_ctx_),
        fusion_param_(),
        fusion_ctdef_(allocator_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    // Initialize exec_ctx_ frames for expression evaluation
    // Allocate enough space for expressions:
    // - rowid_exprs (1) + score_exprs (up to 3) + result_output (3) = up to 7 expressions
    // Each expression needs (for batch mode):
    //   - sizeof(ObDatum) * batch_size (datum array)
    //   - sizeof(ObEvalInfo)
    //   - res_buf_len * batch_size (res_buf array)
    //   - sizeof(VectorHeader)
    //   - null_bitmap (for batch mode with VEC_FIXED format)
    const int64_t max_exprs = 10;  // Safety margin
    const int64_t max_batch_size = 256;
    const int64_t res_buf_len = 8;  // 8 bytes for double
    // For batch mode: datums array + res_buf array + null_bitmap
    const int64_t item_size = (max_batch_size * sizeof(ObDatum)) + sizeof(ObEvalInfo)
                            + (max_batch_size * res_buf_len) + sizeof(sql::VectorHeader)
                            + sql::ObBitVector::memory_size(max_batch_size);
    const int64_t frame_size = max_exprs * item_size;
    exec_ctx_.frames_ = (char **)allocator_.alloc(2 * sizeof(char*));
    if (OB_NOT_NULL(exec_ctx_.frames_)) {
      exec_ctx_.frames_[0] = (char *)allocator_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[0])) {
        memset(exec_ctx_.frames_[0], 0, frame_size);
      }
      exec_ctx_.frames_[1] = (char *)allocator_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[1])) {
        memset(exec_ctx_.frames_[1], 0, frame_size);
      }
      exec_ctx_.frame_cnt_ = 2;
    }
    eval_ctx_.frames_ = exec_ctx_.frames_;
  }

  void TearDown() override
  {
    if (fusion_iter_.is_inited()) {
      fusion_iter_.release();
    }
  }

  /**
   * Create a simple ObRowkey from uint64_t values (with proper memory management)
   */
  ObRowkey create_rowkey(uint64_t val, ObArenaAllocator &alloc)
  {
    void *buf = alloc.alloc(sizeof(ObObj));
    if (OB_NOT_NULL(buf)) {
      ObObj *obj = new (buf) ObObj();
      obj->set_uint64(val);
      return ObRowkey(obj, 1);
    }
    return ObRowkey();
  }

  /**
   * Create mock expressions for testing with proper initialization
   */
  template<int64_t N>
  void create_mock_exprs(ObSEArray<ObExpr*, N> &exprs, int64_t count, ObObjType type = ObUInt64Type, bool enable_batch = false)
  {
    // Don't reset exprs - we want to append to existing expressions
    // Calculate pos from the last expression if any, otherwise start from 0
    int64_t pos = 0;
    if (exprs.count() > 0) {
      ObExpr *last_expr = exprs.at(exprs.count() - 1);
      pos = last_expr->vector_header_off_ + sizeof(sql::VectorHeader);
    }
    for (int64_t i = 0; i < count; ++i) {
      void *buf = allocator_.alloc(sizeof(ObExpr));
      if (OB_NOT_NULL(buf)) {
        ObExpr *expr = new (buf) ObExpr();
        expr->eval_func_ = nullptr;
        expr->obj_meta_.set_type(type);
        expr->obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        expr->datum_meta_.type_ = type;
        expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
        // Set vec_value_tc_ based on ObObjType
        expr->vec_value_tc_ = get_vec_value_tc(type, expr->datum_meta_.scale_, expr->datum_meta_.precision_);
        if (enable_batch) {
          expr->batch_result_ = true;
          // For batch mode, DO NOT set is_fixed_length_data_!
          // Leave it as false (default) so row_store_ uses variable-length row format.
          // IMPORTANT: Set batch_idx_mask_ to UINT64_MAX for batch mode
          // This allows get_datum_idx() to correctly use batch_idx to index into the datums array
          expr->batch_idx_mask_ = UINT64_MAX;
        } else {
          // For non-batch mode, batch_idx_mask_ should be 0 (default)
          // This ensures get_datum_idx() returns batch_idx_default_val_ (0) in single-row mode
          expr->batch_idx_mask_ = 0;
        }
        // Initialize frame offsets for expression evaluation
        expr->frame_idx_ = 0;
        expr->datum_off_ = pos;
        // For batch mode, allocate datum array; for non-batch mode, single datum
        const int64_t max_batch_size = 256;
        if (enable_batch) {
          pos += max_batch_size * sizeof(ObDatum);  // Datum array for batch
        } else {
          pos += sizeof(ObDatum);  // Single datum
        }
        expr->eval_info_off_ = pos;
        pos += sizeof(ObEvalInfo);
        expr->res_buf_off_ = pos;
        expr->res_buf_len_ = 8;  // 8 bytes for uint64_t
        // For batch mode, allocate res_buf array; for non-batch mode, single res_buf
        if (enable_batch) {
          pos += max_batch_size * expr->res_buf_len_;  // Res_buf array for batch
        } else {
          pos += expr->res_buf_len_;  // Single res_buf
        }
        expr->vector_header_off_ = pos;
        pos += sizeof(sql::VectorHeader);
        // For batch mode, allocate null_bitmap and discrete vector buffers
        if (enable_batch) {
          expr->null_bitmap_off_ = pos;
          pos += sql::ObBitVector::memory_size(max_batch_size);
          expr->len_arr_off_ = pos;
          pos += sizeof(uint32_t) * max_batch_size;
          expr->offset_off_ = pos;
          pos += sizeof(char*) * max_batch_size;
        }
        exprs.push_back(expr);
      }
    }
  }

  /**
   * Initialize ObExpr for integer constant (for limit_expr)
   */
  void init_int_const_expr(ObExpr *expr, int64_t &pos, int64_t value)
  {
    if (OB_NOT_NULL(expr)) {
      expr->eval_func_ = nullptr;
      expr->obj_meta_.set_type(ObIntType);
      expr->obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      expr->datum_meta_.type_ = ObIntType;
      expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
      expr->vec_value_tc_ = get_vec_value_tc(ObIntType, expr->datum_meta_.scale_, expr->datum_meta_.precision_);
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos;
      const int64_t max_batch_size = 256;  // Must match SetUp()
      pos += sizeof(ObDatum);  // int const expr always uses single datum
      expr->eval_info_off_ = pos;
      pos += sizeof(ObEvalInfo);
      expr->res_buf_off_ = pos;
      expr->res_buf_len_ = 8;  // 8 bytes for int64_t
      pos += expr->res_buf_len_;  // int const expr always uses single res_buf
      expr->vector_header_off_ = pos;
      pos += sizeof(sql::VectorHeader);

      // Set constant value
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      datum.set_int(value);
      expr->set_evaluated_projected(eval_ctx_);
    }
  }

  /**
   * Initialize ObExpr for score (double type)
   */
  void init_score_expr(ObExpr *expr, int64_t &pos, bool enable_batch = false)
  {
    if (OB_NOT_NULL(expr)) {
      expr->eval_func_ = nullptr;
      expr->obj_meta_.set_type(ObDoubleType);
      expr->obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      expr->datum_meta_.type_ = ObDoubleType;
      expr->obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
      // Set vec_value_tc_ based on ObObjType
      expr->vec_value_tc_ = get_vec_value_tc(ObDoubleType, expr->datum_meta_.scale_, expr->datum_meta_.precision_);
      if (enable_batch) {
        expr->batch_result_ = true;
        // For batch mode, DO NOT set is_fixed_length_data_!
        // Leave it as false (default) so row_store_ uses variable-length row format.
        // IMPORTANT: Set batch_idx_mask_ to UINT64_MAX for batch mode
        // This allows get_datum_idx() to correctly use batch_idx to index into the datums array
        expr->batch_idx_mask_ = UINT64_MAX;
      } else {
        // For non-batch mode, batch_idx_mask_ should be 0 (default)
        // This ensures get_datum_idx() returns batch_idx_default_val_ (0) in single-row mode
        expr->batch_idx_mask_ = 0;
      }
      // Initialize frame offsets for expression evaluation
      expr->frame_idx_ = 0;
      expr->datum_off_ = pos;
      // For batch mode, allocate datum array; for non-batch mode, single datum
      const int64_t max_batch_size = 256;
      if (enable_batch) {
        pos += max_batch_size * sizeof(ObDatum);  // Datum array for batch
      } else {
        pos += sizeof(ObDatum);  // Single datum
      }
      expr->eval_info_off_ = pos;
      pos += sizeof(ObEvalInfo);
      expr->res_buf_off_ = pos;
      expr->res_buf_len_ = 8;  // 8 bytes for double
      // For batch mode, allocate res_buf array; for non-batch mode, single res_buf
      if (enable_batch) {
        pos += max_batch_size * expr->res_buf_len_;  // Res_buf array for batch
      } else {
        pos += expr->res_buf_len_;  // Single res_buf
      }
      expr->vector_header_off_ = pos;
      pos += sizeof(sql::VectorHeader);
      // For batch mode, allocate null_bitmap and discrete vector buffers
      if (enable_batch) {
        expr->null_bitmap_off_ = pos;
        pos += sql::ObBitVector::memory_size(max_batch_size);
        // For VEC_DISCRETE format, also need len_arr and offset buffers
        expr->len_arr_off_ = pos;
        pos += sizeof(uint32_t) * max_batch_size;
        expr->offset_off_ = pos;
        pos += sizeof(char*) * max_batch_size;
      }
    }
  }

  /**
   * Initialize fusion_ctdef_ with basic structure
   */
  void init_fusion_ctdef(bool has_search = true, bool has_vec = true, bool enable_batch = false)
  {
    int ret = OB_SUCCESS;
    fusion_ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
    fusion_ctdef_.has_search_subquery_ = has_search;
    fusion_ctdef_.has_vector_subquery_ = has_vec;
    // Set children_cnt_ based on has_search and has_vec
    fusion_ctdef_.children_cnt_ = (has_search ? 1 : 0) + (has_vec ? 1 : 0);
    // Set search_index_: if has_search, search is always the first child (index 0)
    // In score_exprs_, search_score is also at index 0 if has_search
    fusion_ctdef_.set_search_index(has_search ? 0 : -1);

    // Create rowid expressions
    ObSEArray<ObExpr*, 2> rowid_exprs;
    int64_t pos = 0;
    create_mock_exprs(rowid_exprs, 1, ObUInt64Type, enable_batch);
    // Update pos to continue after rowid expressions
    if (rowid_exprs.count() > 0) {
      ObExpr *last_expr = rowid_exprs.at(rowid_exprs.count() - 1);
      pos = last_expr->vector_header_off_ + sizeof(sql::VectorHeader);
    }
    fusion_ctdef_.rowid_exprs_.reset();
    if (OB_FAIL(fusion_ctdef_.rowid_exprs_.assign(rowid_exprs))) {
      LOG_WARN("failed to assign rowid_exprs", K(ret));
    }

    // Create result output expressions
    // result_output_ should contain: [rowid, search_score, vec_score, fusion_score]
    // Fusion iter only handles rowkey and score expressions, not data columns
    ObSEArray<ObExpr*, 8> result_output;
    // Add rowkey expressions first
    if (rowid_exprs.count() > 0) {
      for (int64_t i = 0; i < rowid_exprs.count(); ++i) {
        if (OB_FAIL(result_output.push_back(rowid_exprs.at(i)))) {
          LOG_WARN("failed to push back rowid_expr to result_output", K(ret));
        }
      }
    }
    // Update pos after rowkey expressions
    if (result_output.count() > 0) {
      ObExpr *last_expr = result_output.at(result_output.count() - 1);
      pos = last_expr->vector_header_off_ + sizeof(sql::VectorHeader);
    }

    // Create score expressions and add to score_exprs_ array
    // Format: [path0_score, path1_score, ..., fusion_score]
    // Also add them to result_output after rowkey
    ObSEArray<ObExpr*, 4> score_exprs;
    if (has_search) {
      void *buf = allocator_.alloc(sizeof(ObExpr));
      if (OB_NOT_NULL(buf)) {
        ObExpr *search_score = new (buf) ObExpr();
        init_score_expr(search_score, pos, enable_batch);
        score_exprs.push_back(search_score);
        result_output.push_back(search_score);  // Add to result_output
      }
    }
    if (has_vec) {
      void *buf = allocator_.alloc(sizeof(ObExpr));
      if (OB_NOT_NULL(buf)) {
        ObExpr *vec_score = new (buf) ObExpr();
        init_score_expr(vec_score, pos, enable_batch);
        score_exprs.push_back(vec_score);
        result_output.push_back(vec_score);  // Add to result_output
      }
    }
    // Add fusion_score_expr as the last element
    void *buf = allocator_.alloc(sizeof(ObExpr));
    ObExpr *fusion_score = nullptr;
    if (OB_NOT_NULL(buf)) {
      fusion_score = new (buf) ObExpr();
      init_score_expr(fusion_score, pos, enable_batch);
      score_exprs.push_back(fusion_score);
      result_output.push_back(fusion_score);  // Add to result_output
    }
    fusion_ctdef_.score_exprs_.reset();
    if (OB_FAIL(fusion_ctdef_.score_exprs_.assign(score_exprs))) {
      LOG_WARN("failed to assign score_exprs", K(ret));
    }

    fusion_ctdef_.result_output_.reset();
    if (OB_FAIL(fusion_ctdef_.result_output_.assign(result_output))) {
      LOG_WARN("failed to assign result_output", K(ret));
    }

    // IMPORTANT: Pre-initialize all expression vectors
    // - For batch mode: use VEC_UNIFORM format
    // - For single row mode: use VEC_UNIFORM_CONST format
    // This ensures that when child iterators or fusion_iter call init_vector_default(),
    // the vectors are already initialized and won't try to use incompatible formats like VEC_DISCRETE
    if (enable_batch) {
      const int64_t max_batch_size = eval_ctx_.max_batch_size_;
      for (int64_t i = 0; i < result_output.count(); ++i) {
        ObExpr *expr = result_output.at(i);
        if (OB_NOT_NULL(expr) && OB_FAIL(expr->init_vector(eval_ctx_, VEC_UNIFORM, max_batch_size))) {
          LOG_WARN("failed to init vector for result_output expr", K(ret), K(i));
        }
      }
    } else {
      // Single row mode: use VEC_UNIFORM_CONST format
      for (int64_t i = 0; i < result_output.count(); ++i) {
        ObExpr *expr = result_output.at(i);
        if (OB_NOT_NULL(expr) && OB_FAIL(expr->init_vector(eval_ctx_, VEC_UNIFORM_CONST, 1))) {
          LOG_WARN("failed to init vector for result_output expr", K(ret), K(i));
        }
      }
    }

    // Initialize limit_expr_ and offset_expr_ to nullptr by default
    fusion_ctdef_.rank_window_size_expr_ = nullptr;
    fusion_ctdef_.size_expr_ = nullptr;
    fusion_ctdef_.offset_expr_ = nullptr;
  }

  /**
   * Set limit_expr_ for testing LIMIT functionality
   */
  void set_limit_expr(int64_t limit_value)
  {
    void *buf = allocator_.alloc(sizeof(ObExpr));
    if (OB_NOT_NULL(buf)) {
      ObExpr *limit_expr = new (buf) ObExpr();
      int64_t pos = 0;
      // Find the last pos from existing expressions to continue
      if (fusion_ctdef_.result_output_.count() > 0) {
        ObExpr *last_expr = fusion_ctdef_.result_output_.at(fusion_ctdef_.result_output_.count() - 1);
        pos = last_expr->vector_header_off_ + sizeof(sql::VectorHeader);
      }
      init_int_const_expr(limit_expr, pos, limit_value);
      fusion_iter_.rank_window_size_ = limit_value;
      fusion_param_.size_ = limit_value;
    }
  }

  /**
   * Setup fusion iter with mock children
   */
  int setup_fusion_iter(const ObIArray<MockDASChildIter::MockRow> &search_rows,
                        const ObIArray<MockDASChildIter::MockRow> &vec_rows,
                        int64_t rank_window_size = 10,
                        const ObIArray<double> *path_weights = nullptr,
                        bool enable_batch = false)
  {
    int ret = OB_SUCCESS;

    // Initialize fusion_ctdef_
    init_fusion_ctdef(!search_rows.empty(), !vec_rows.empty(), enable_batch);

    // Create fusion_param_
    fusion_param_.fusion_ctdef_ = &fusion_ctdef_;
    fusion_param_.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
    fusion_param_.rank_window_size_ = rank_window_size;
    fusion_param_.size_ = rank_window_size;
    fusion_param_.offset_ = 0;
    // Set path weights: use provided weights or default to [0.5, 0.5]
    fusion_param_.weights_.reset();
    if (OB_NOT_NULL(path_weights) && path_weights->count() > 0) {
      for (int64_t i = 0; i < path_weights->count(); ++i) {
        fusion_param_.weights_.push_back(path_weights->at(i));
      }
    } else {
      // Default weights: equal weight for each path
    fusion_param_.weights_.push_back(0.5);  // search weight
    fusion_param_.weights_.push_back(0.5);  // vec weight
    }

    // Create fusion_score_expr_
    // Note: fusion_score_expr is already in fusion_ctdef_.score_exprs_ as the last element
    // We can reuse it or create a new one with proper offset
    // For now, we'll get it from fusion_ctdef_
    fusion_param_.fusion_score_expr_ = fusion_ctdef_.get_fusion_score_expr();

    // Create child iterators array
    void *children_buf = allocator_.alloc(sizeof(ObDASIter*) * 2);
    ObDASIter **children = nullptr;
    int64_t children_cnt = 0;
    if (OB_NOT_NULL(children_buf)) {
      children = static_cast<ObDASIter**>(children_buf);
    }

    // Create search child iterator
    if (!search_rows.empty() && OB_NOT_NULL(children)) {
      void *search_iter_buf = allocator_.alloc(sizeof(MockDASChildIter));
      if (OB_NOT_NULL(search_iter_buf)) {
        MockDASChildIter *search_iter = new (search_iter_buf) MockDASChildIter(search_rows, &allocator_);
        children[0] = search_iter;
        children_cnt = 1;
      }
    }

    // Create vector child iterator
    if (!vec_rows.empty() && OB_NOT_NULL(children)) {
      void *vec_iter_buf = allocator_.alloc(sizeof(MockDASChildIter));
      if (OB_NOT_NULL(vec_iter_buf)) {
        MockDASChildIter *vec_iter = new (vec_iter_buf) MockDASChildIter(vec_rows, &allocator_);
        children[children_cnt] = vec_iter;
        children_cnt++;
      }
    }

    // Initialize fusion_iter_
    fusion_param_.eval_ctx_ = &eval_ctx_;
    fusion_param_.exec_ctx_ = &exec_ctx_;
    fusion_param_.max_size_ = 1024 * 1024;  // 1MB
    fusion_param_.output_ = &fusion_ctdef_.result_output_;

    if (OB_FAIL(fusion_iter_.init(fusion_param_))) {
      LOG_WARN("failed to init fusion iter", K(ret));
    } else {
      // Set children to fusion_iter_ (not fusion_param_)
      fusion_iter_.get_children() = children;
      fusion_iter_.set_children_cnt(children_cnt);

      // Setup child iterators
      // Track which child is search and which is vector based on creation order
      int64_t search_child_idx = -1;
      int64_t vec_child_idx = -1;
      if (!search_rows.empty()) {
        search_child_idx = 0;
        if (!vec_rows.empty()) {
          vec_child_idx = 1;
        }
      } else if (!vec_rows.empty()) {
        vec_child_idx = 0;
      }

      for (int64_t i = 0; i < children_cnt; ++i) {
        MockDASChildIter *child = static_cast<MockDASChildIter*>(children[i]);
        child->set_eval_ctx(&eval_ctx_);
        // Child iterator's output_exprs should only contain score expressions
        // (excluding rowkey and fusion_score)
        // fusion_ctdef_.result_output_ format: [rowkey, search_score, vec_score, fusion_score]
        // Child iterator sets rowkey via rowid_exprs_ (which matches fusion_ctdef_.rowid_exprs_)
        // Child iterator sets its own score via output_exprs_ (which should match the path score expr)
        ObSEArray<ObExpr*, 8> output_exprs;
        ObExpr *fusion_score_expr = fusion_ctdef_.get_fusion_score_expr();
        int64_t rowid_count = fusion_ctdef_.rowid_exprs_.count();
        // Skip rowkey expressions (first rowid_count elements) and fusion_score (last element)
        // Include only the path score expression for this child
        if (i == search_child_idx && fusion_ctdef_.has_search_subquery_) {
          ObExpr *search_score_expr = fusion_ctdef_.get_search_score_expr();
          if (OB_NOT_NULL(search_score_expr) && OB_FAIL(output_exprs.push_back(search_score_expr))) {
            LOG_WARN("failed to push back search_score_expr to output_exprs", K(ret));
          }
        } else if (i == vec_child_idx && fusion_ctdef_.has_vector_subquery_) {
          // vec_child_idx directly corresponds to the index in score_exprs_ array
          const ExprFixedArray &score_exprs = fusion_ctdef_.get_score_exprs();
          if (vec_child_idx >= 0 && vec_child_idx < score_exprs.count() - 1) {
            ObExpr *vec_score_expr = score_exprs.at(vec_child_idx);
            if (OB_NOT_NULL(vec_score_expr) && OB_FAIL(output_exprs.push_back(vec_score_expr))) {
              LOG_WARN("failed to push back vec_score_expr to output_exprs", K(ret));
            }
          }
        }
        child->set_output_exprs(output_exprs);
        // rowid_exprs_ should directly reference fusion_ctdef_.rowid_exprs_ expressions
        // so that extract_rowkey() can read the values set by child iterator
        child->set_rowid_exprs(fusion_ctdef_.rowid_exprs_);
        if (i == search_child_idx && fusion_ctdef_.has_search_subquery_) {
          child->set_score_expr(fusion_ctdef_.get_search_score_expr());
        } else if (i == vec_child_idx && fusion_ctdef_.has_vector_subquery_) {
          // vec_child_idx directly corresponds to the index in score_exprs_ array
          const ExprFixedArray &score_exprs = fusion_ctdef_.get_score_exprs();
          if (vec_child_idx >= 0 && vec_child_idx < score_exprs.count() - 1) {
            ObExpr *vec_score_expr = score_exprs.at(vec_child_idx);
            child->set_score_expr(vec_score_expr);
          }
        }

        // Initialize child iterator
        ObDASIterParam child_param(ObDASIterType::DAS_ITER_SCAN);
        child_param.eval_ctx_ = &eval_ctx_;
        child_param.exec_ctx_ = &exec_ctx_;
        child_param.max_size_ = 1024;
        child_param.output_ = &fusion_ctdef_.result_output_;
        if (OB_FAIL(child->init(child_param))) {
          LOG_WARN("failed to init child iter", K(ret), K(i));
        }
      }
    }

    return ret;
  }

  /**
   * Verify fusion results
   */
  void verify_results(const ObIArray<uint64_t> &expected_rowkeys,
                      const ObIArray<double> &expected_scores = ObSEArray<double, 8>())
  {
    ObSEArray<ObExpr*, 8> exprs;
    exprs.assign(fusion_ctdef_.result_output_);

    int64_t idx = 0;
    while (true) {
      int ret = fusion_iter_.inner_get_next_row();
      if (OB_ITER_END == ret) {
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);

      if (idx < expected_rowkeys.count()) {
        // Verify rowkey (simplified - would need to extract from expressions)
        // Verify fusion score
        if (expected_scores.count() > 0 && idx < expected_scores.count()) {
          ObExpr *fusion_score_expr = fusion_param_.fusion_score_expr_;
          if (OB_NOT_NULL(fusion_score_expr)) {
            ObDatum &datum = fusion_score_expr->locate_expr_datum(eval_ctx_);
            EXPECT_DOUBLE_EQ(expected_scores.at(idx), datum.get_double());
          }
        }
      }
      idx++;
    }

    EXPECT_EQ(expected_rowkeys.count(), idx);
  }

protected:
  ObArenaAllocator allocator_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionIter fusion_iter_;
  ObDASFusionIterParam fusion_param_;
  ObDASFusionCtDef fusion_ctdef_;
};

// Test basic fusion with two paths
TEST_F(ObDASFusionIterTest, test_basic_fusion)
{
  // Create mock rows for search path
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.8};
  MockDASChildIter::MockRow row2 = {2, 0.6};
  search_rows.push_back(row1);
  search_rows.push_back(row2);

  // Create mock rows for vector path
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row3 = {1, 0.7};
  MockDASChildIter::MockRow row4 = {3, 0.9};
  vec_rows.push_back(row3);
  vec_rows.push_back(row4);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  // Trigger fusion
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Get results - rowkey 1 appears in both paths: (0.8+0.7)/2 = 0.75
  // rowkey 3 only in vec: 0.9*0.5 = 0.45
  // rowkey 2 only in search: 0.6*0.5 = 0.3
  // Expected order: 1 (0.75), 3 (0.45), 2 (0.3)
  ObSEArray<uint64_t, 8> expected_rowkeys;
  expected_rowkeys.push_back(1);
  expected_rowkeys.push_back(3);
  expected_rowkeys.push_back(2);
  ObSEArray<double, 8> expected_scores;
  expected_scores.push_back(0.75);
  expected_scores.push_back(0.45);
  expected_scores.push_back(0.3);

  verify_results(expected_rowkeys, expected_scores);
}

// Test fusion with only search path
TEST_F(ObDASFusionIterTest, test_search_only)
{
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.8};
  MockDASChildIter::MockRow row2 = {2, 0.6};
  search_rows.push_back(row1);
  search_rows.push_back(row2);

  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;  // Empty

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  ObSEArray<uint64_t, 8> expected_rowkeys;
  expected_rowkeys.push_back(1);
  expected_rowkeys.push_back(2);
  verify_results(expected_rowkeys);
}

// Test fusion with only vector path
TEST_F(ObDASFusionIterTest, test_vec_only)
{
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;  // Empty

  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row1 = {1, 0.9};
  MockDASChildIter::MockRow row2 = {2, 0.7};
  vec_rows.push_back(row1);
  vec_rows.push_back(row2);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  ObSEArray<uint64_t, 8> expected_rowkeys;
  expected_rowkeys.push_back(1);
  expected_rowkeys.push_back(2);
  verify_results(expected_rowkeys);
}

// Test top-k limit
TEST_F(ObDASFusionIterTest, test_topk_limit)
{
  ObSEArray<MockDASChildIter::MockRow, 32> search_rows;
  for (uint64_t i = 1; i <= 20; ++i) {
    MockDASChildIter::MockRow row = {i, 1.0 - i * 0.01};
    search_rows.push_back(row);
  }

  ObSEArray<MockDASChildIter::MockRow, 32> vec_rows;
  for (uint64_t i = 1; i <= 20; ++i) {
    MockDASChildIter::MockRow row = {i, 1.0 - i * 0.01};
    vec_rows.push_back(row);
  }

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows, 5));  // Top-5 only

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Verify only top-5 results are returned
  int64_t count = 0;
  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    count++;
  }
  EXPECT_EQ(5, count);
}

// Test rescan functionality
TEST_F(ObDASFusionIterTest, test_rescan)
{
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.8};
  MockDASChildIter::MockRow row2 = {2, 0.6};
  search_rows.push_back(row1);
  search_rows.push_back(row2);

  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row3 = {1, 0.7};
  vec_rows.push_back(row3);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  // First scan
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());
  int64_t count1 = 0;
  while (fusion_iter_.inner_get_next_row() == OB_SUCCESS) {
    count1++;
  }

  // Rescan
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.rescan());
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());
  int64_t count2 = 0;
  while (fusion_iter_.inner_get_next_row() == OB_SUCCESS) {
    count2++;
  }

  EXPECT_EQ(count1, count2);
}

// Test reuse functionality
TEST_F(ObDASFusionIterTest, test_reuse)
{
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.8};
  search_rows.push_back(row1);

  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row2 = {1, 0.7};
  vec_rows.push_back(row2);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  // First use
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());
  int64_t count1 = 0;
  while (fusion_iter_.inner_get_next_row() == OB_SUCCESS) {
    count1++;
  }

  // Reuse
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.inner_reuse());
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());
  int64_t count2 = 0;
  while (fusion_iter_.inner_get_next_row() == OB_SUCCESS) {
    count2++;
  }

  EXPECT_EQ(count1, count2);
}

// Test empty results
TEST_F(ObDASFusionIterTest, test_empty_results)
{
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;  // Empty
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;    // Empty

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Should return OB_ITER_END immediately
  int ret = fusion_iter_.inner_get_next_row();
  ASSERT_EQ(OB_ITER_END, ret);
}

// Test weighted fusion
TEST_F(ObDASFusionIterTest, test_weighted_fusion)
{
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.8};
  search_rows.push_back(row1);

  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row2 = {1, 0.6};
  vec_rows.push_back(row2);

  ObSEArray<double, 4> path_weights;
  path_weights.push_back(0.7);  // search weight = 0.7
  path_weights.push_back(0.3);  // vec weight = 0.3

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows, 10, &path_weights));

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Expected score: 0.8*0.7 + 0.6*0.3 = 0.56 + 0.18 = 0.74
  ObSEArray<uint64_t, 8> expected_rowkeys;
  expected_rowkeys.push_back(1);
  ObSEArray<double, 8> expected_scores;
  expected_scores.push_back(0.74);

  verify_results(expected_rowkeys, expected_scores);
}

// Test score sorting output - verify results are sorted by fusion score in descending order
TEST_F(ObDASFusionIterTest, test_score_sorting_output)
{
  // Create multiple documents with different scores to test sorting
  // Search path scores: rowkey 1=0.9, 2=0.7, 3=0.5, 4=0.3, 5=0.1
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.9};
  MockDASChildIter::MockRow row2 = {2, 0.7};
  MockDASChildIter::MockRow row3 = {3, 0.5};
  MockDASChildIter::MockRow row4 = {4, 0.3};
  MockDASChildIter::MockRow row5 = {5, 0.1};
  search_rows.push_back(row1);
  search_rows.push_back(row2);
  search_rows.push_back(row3);
  search_rows.push_back(row4);
  search_rows.push_back(row5);

  // Vector path scores: rowkey 1=0.8, 2=0.6, 3=0.4, 4=0.2, 6=0.95
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow vrow1 = {1, 0.8};
  MockDASChildIter::MockRow vrow2 = {2, 0.6};
  MockDASChildIter::MockRow vrow3 = {3, 0.4};
  MockDASChildIter::MockRow vrow4 = {4, 0.2};
  MockDASChildIter::MockRow vrow6 = {6, 0.95};
  vec_rows.push_back(vrow1);
  vec_rows.push_back(vrow2);
  vec_rows.push_back(vrow3);
  vec_rows.push_back(vrow4);
  vec_rows.push_back(vrow6);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Calculate expected fusion scores (weight_sum with 0.5, 0.5):
  // rowkey 1: (0.9 + 0.8) / 2 = 0.85
  // rowkey 2: (0.7 + 0.6) / 2 = 0.65
  // rowkey 3: (0.5 + 0.4) / 2 = 0.45
  // rowkey 4: (0.3 + 0.2) / 2 = 0.25
  // rowkey 5: 0.1 / 2 = 0.05 (only in search)
  // rowkey 6: 0.95 / 2 = 0.475 (only in vec)
  // Expected order (descending): 1(0.85), 2(0.65), 6(0.475), 3(0.45), 4(0.25), 5(0.05)
  ObSEArray<uint64_t, 8> expected_rowkeys;
  expected_rowkeys.push_back(1);
  expected_rowkeys.push_back(2);
  expected_rowkeys.push_back(6);
  expected_rowkeys.push_back(3);
  expected_rowkeys.push_back(4);
  expected_rowkeys.push_back(5);
  ObSEArray<double, 8> expected_scores;
  expected_scores.push_back(0.85);
  expected_scores.push_back(0.65);
  expected_scores.push_back(0.475);
  expected_scores.push_back(0.45);
  expected_scores.push_back(0.25);
  expected_scores.push_back(0.05);

  // Verify results are sorted correctly
  ObSEArray<ObExpr*, 8> exprs;
  exprs.assign(fusion_ctdef_.result_output_);

  int64_t idx = 0;
  double prev_score = 1.0;  // Start with a score higher than any possible score

  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // Verify we get the expected rowkey
    if (idx < expected_rowkeys.count()) {
      // Extract rowkey from expressions (simplified - in real test would extract from rowid_exprs)
      // For now, we verify the score ordering
    }

    // Verify fusion score is in descending order
    ObExpr *fusion_score_expr = fusion_param_.fusion_score_expr_;
    if (OB_NOT_NULL(fusion_score_expr)) {
      ObDatum &datum = fusion_score_expr->locate_expr_datum(eval_ctx_);
      double current_score = datum.get_double();

      // Verify score matches expected
      if (idx < expected_scores.count()) {
        EXPECT_DOUBLE_EQ(expected_scores.at(idx), current_score)
            << "Score mismatch at index " << idx;
      }

      // Verify descending order: current score should be <= previous score
      EXPECT_LE(current_score, prev_score)
          << "Scores not in descending order at index " << idx
          << ": prev=" << prev_score << ", current=" << current_score;

      prev_score = current_score;
    }

    idx++;
  }

  // Verify we got all expected results
  EXPECT_EQ(expected_rowkeys.count(), idx) << "Result count mismatch";
}

// Helper structure for expected scores
struct ExpectedScore {
  uint64_t rowkey;
  double score;
  ExpectedScore() : rowkey(0), score(0.0) {}
  ExpectedScore(uint64_t rk, double sc) : rowkey(rk), score(sc) {}

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV("rowkey", rowkey, "score", score);
    J_OBJ_END();
    return pos;
  }
};

// Simple bubble sort for descending order
void sort_scores_descending(ObSEArray<ExpectedScore, 32> &scores)
{
  for (int64_t i = 0; i < scores.count() - 1; ++i) {
    for (int64_t j = 0; j < scores.count() - 1 - i; ++j) {
      if (scores.at(j).score < scores.at(j + 1).score) {
        ExpectedScore temp = scores.at(j);
        scores.at(j) = scores.at(j + 1);
        scores.at(j + 1) = temp;
      }
    }
  }
}

// Test score sorting with large dataset
TEST_F(ObDASFusionIterTest, test_score_sorting_large_dataset)
{
  // Create 20 documents with varying scores
  ObSEArray<MockDASChildIter::MockRow, 32> search_rows;
  ObSEArray<MockDASChildIter::MockRow, 32> vec_rows;

  // Calculate expected fusion scores for verification
  ObSEArray<ExpectedScore, 32> expected_scores;

  for (uint64_t i = 1; i <= 20; ++i) {
    double search_score = 1.0 - (i - 1) * 0.05;  // 1.0, 0.95, 0.9, ..., 0.05
    double vec_score = 1.0 - i * 0.04;            // 0.96, 0.92, 0.88, ..., 0.04
    MockDASChildIter::MockRow srow = {i, search_score};
    MockDASChildIter::MockRow vrow = {i, vec_score};
    search_rows.push_back(srow);
    vec_rows.push_back(vrow);

    // Calculate expected fusion score (weight_sum with 0.5, 0.5)
    double fusion_score = (search_score + vec_score) / 2.0;
    expected_scores.push_back(ExpectedScore(i, fusion_score));
  }

  // Sort expected scores in descending order
  sort_scores_descending(expected_scores);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows, 20));  // Get all results

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Verify results are sorted correctly
  ObSEArray<ObExpr*, 8> exprs;
  exprs.assign(fusion_ctdef_.result_output_);

  int64_t count = 0;
  double prev_score = 1.0;
  ObSEArray<double, 32> actual_scores;

  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    ObExpr *fusion_score_expr = fusion_param_.fusion_score_expr_;
    if (OB_NOT_NULL(fusion_score_expr)) {
      ObDatum &datum = fusion_score_expr->locate_expr_datum(eval_ctx_);
      double current_score = datum.get_double();
      actual_scores.push_back(current_score);

      // Verify descending order
      EXPECT_LE(current_score, prev_score)
          << "Scores not in descending order at index " << count
          << ": prev=" << prev_score << ", current=" << current_score;

      // Verify score matches expected value (with tolerance for floating point)
      if (count < expected_scores.count()) {
        EXPECT_DOUBLE_EQ(expected_scores.at(count).score, current_score)
            << "Score mismatch at index " << count
            << ": expected=" << expected_scores.at(count).score
            << ", actual=" << current_score;
      }

      prev_score = current_score;
    }

    count++;
  }

  // Verify we got all 20 results
  EXPECT_EQ(20, count);
  EXPECT_EQ(20, actual_scores.count());

  // Verify all scores match expected sorted scores
  for (int64_t i = 0; i < count && i < expected_scores.count(); ++i) {
    EXPECT_DOUBLE_EQ(expected_scores.at(i).score, actual_scores.at(i))
        << "Score mismatch at sorted position " << i
        << ": expected=" << expected_scores.at(i).score
        << ", actual=" << actual_scores.at(i);
  }

  // Verify the first few and last few scores are correct
  // First score should be the highest
  if (count > 0) {
    EXPECT_DOUBLE_EQ(expected_scores.at(0).score, actual_scores.at(0))
        << "Highest score mismatch";
  }

  // Last score should be the lowest
  if (count > 0 && expected_scores.count() > 0) {
    EXPECT_DOUBLE_EQ(expected_scores.at(expected_scores.count() - 1).score,
                actual_scores.at(actual_scores.count() - 1))
        << "Lowest score mismatch";
  }
}

// Test NULL score handling (when a document doesn't appear in all paths)
TEST_F(ObDASFusionIterTest, test_null_scores)
{
  // Create mock rows where some documents only appear in one path
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.8};
  MockDASChildIter::MockRow row2 = {2, 0.6};
  search_rows.push_back(row1);
  search_rows.push_back(row2);

  // Vector path only has rowkey 1, not rowkey 2
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row3 = {1, 0.7};
  vec_rows.push_back(row3);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Get results and verify NULL scores
  ObSEArray<ObExpr*, 8> exprs;
  exprs.assign(fusion_ctdef_.result_output_);

  // Get search_score_expr and vec_score_expr from score_exprs_
  ObExpr *search_score_expr = fusion_ctdef_.get_search_score_expr();
  // Find vec_score_expr by iterating through score_exprs_ (excluding fusion_score at the end)
  ObExpr *vec_score_expr = nullptr;
  const ExprFixedArray &score_exprs = fusion_ctdef_.get_score_exprs();
  int64_t vec_idx = fusion_ctdef_.has_search_subquery_ ? 1 : 0;
  if (vec_idx >= 0 && vec_idx < score_exprs.count() - 1) {
    vec_score_expr = score_exprs.at(vec_idx);
  }

  int64_t idx = 0;
  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    if (idx == 0) {
      // First result: rowkey 1 appears in both paths
      // Search score should be 0.8, vec score should be 0.7
      if (OB_NOT_NULL(search_score_expr)) {
        ObDatum &search_datum = search_score_expr->locate_expr_datum(eval_ctx_);
        ASSERT_FALSE(search_datum.is_null());
        EXPECT_DOUBLE_EQ(0.8, search_datum.get_double());
      }
      if (OB_NOT_NULL(vec_score_expr)) {
        ObDatum &vec_datum = vec_score_expr->locate_expr_datum(eval_ctx_);
        ASSERT_FALSE(vec_datum.is_null());
        EXPECT_DOUBLE_EQ(0.7, vec_datum.get_double());
      }
    } else if (idx == 1) {
      // Second result: rowkey 2 only appears in search path
      // Search score should be 0.6, vec score should be NULL
      if (OB_NOT_NULL(search_score_expr)) {
        ObDatum &search_datum = search_score_expr->locate_expr_datum(eval_ctx_);
        ASSERT_FALSE(search_datum.is_null());
        EXPECT_DOUBLE_EQ(0.6, search_datum.get_double());
      }
      if (OB_NOT_NULL(vec_score_expr)) {
        ObDatum &vec_datum = vec_score_expr->locate_expr_datum(eval_ctx_);
        // vec_score should be NULL for rowkey 2 since it doesn't appear in vec path
        EXPECT_TRUE(vec_datum.is_null()) << "vec_score should be NULL for rowkey 2";
      }
    }
    idx++;
  }

  ASSERT_EQ(2, idx);  // Should have 2 results
}

// Test LIMIT functionality
TEST_F(ObDASFusionIterTest, test_limit)
{
  // Create multiple documents
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;

  for (uint64_t i = 1; i <= 10; ++i) {
    double search_score = 1.0 - (i - 1) * 0.1;  // 1.0, 0.9, 0.8, ..., 0.1
    double vec_score = 1.0 - i * 0.08;            // 0.92, 0.84, 0.76, ..., 0.12
    MockDASChildIter::MockRow srow = {i, search_score};
    MockDASChildIter::MockRow vrow = {i, vec_score};
    search_rows.push_back(srow);
    vec_rows.push_back(vrow);
  }

  // Set LIMIT to 5 via rank_window_size
  // rank_window_size effectively limits results to top-k
  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows, 5));  // Top-5 only

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Verify only 5 results are returned
  int64_t count = 0;
  ObSEArray<double, 8> actual_scores;

  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    ObExpr *fusion_score_expr = fusion_param_.fusion_score_expr_;
    if (OB_NOT_NULL(fusion_score_expr)) {
      ObDatum &datum = fusion_score_expr->locate_expr_datum(eval_ctx_);
      actual_scores.push_back(datum.get_double());
    }
    count++;
  }

  ASSERT_EQ(5, count) << "Should return exactly 5 results with rank_window_size=5";
  ASSERT_EQ(5, actual_scores.count());

  // Verify scores are in descending order
  for (int64_t i = 1; i < actual_scores.count(); ++i) {
    EXPECT_LE(actual_scores.at(i), actual_scores.at(i-1))
        << "Scores not in descending order at index " << i;
  }
}

// Test full flow with complete verification (rowkey and fusion score)
TEST_F(ObDASFusionIterTest, test_full_flow_with_data_verification)
{
  // Create mock rows for search path
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.8};
  MockDASChildIter::MockRow row2 = {2, 0.6};
  search_rows.push_back(row1);
  search_rows.push_back(row2);

  // Create mock rows for vector path
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row3 = {1, 0.7};
  MockDASChildIter::MockRow row4 = {3, 0.9};
  vec_rows.push_back(row3);
  vec_rows.push_back(row4);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  // Trigger fusion
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Expected results:
  // rowkey 1: fusion_score = (0.8+0.7)/2 = 0.75
  // rowkey 3: fusion_score = 0.9*0.5 = 0.45
  // rowkey 2: fusion_score = 0.6*0.5 = 0.3
  // Expected order: 1 (0.75), 3 (0.45), 2 (0.3)

  struct ExpectedResult {
    uint64_t rowkey;
    double fusion_score;

    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV("rowkey", rowkey, "fusion_score", fusion_score);
      J_OBJ_END();
      return pos;
    }
  };
  ObSEArray<ExpectedResult, 8> expected_results;
  expected_results.push_back({1, 0.75});
  expected_results.push_back({3, 0.45});
  expected_results.push_back({2, 0.3});

  // Get rowid_exprs and result_output_ exprs
  const ExprFixedArray &rowid_exprs = fusion_ctdef_.rowid_exprs_;
  const ExprFixedArray &result_output = fusion_ctdef_.result_output_;

  // result_output_ contains: [rowkey, search_score, vec_score, fusion_score]
  // rowid_exprs contains: [rowkey]

  int64_t idx = 0;
  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    if (idx < expected_results.count()) {
      const ExpectedResult &expected = expected_results.at(idx);

      // 1. Verify rowkey
      // result_output_ format: [rowkey, search_score, vec_score, fusion_score]
      if (result_output.count() >= 1 && rowid_exprs.count() > 0) {
        // Rowkey is the first element in result_output_
        ObExpr *rowkey_expr = result_output.at(0);
        ObDatum &rowkey_datum = rowkey_expr->locate_expr_datum(eval_ctx_);
        ASSERT_FALSE(rowkey_datum.is_null());
        EXPECT_EQ(expected.rowkey, rowkey_datum.get_uint())
            << "Rowkey mismatch at index " << idx;
      }

      // 2. Verify fusion score
      ObExpr *fusion_score_expr = fusion_param_.fusion_score_expr_;
      ASSERT_TRUE(OB_NOT_NULL(fusion_score_expr)) << "Fusion score expr is null";
      ObDatum &fusion_score_datum = fusion_score_expr->locate_expr_datum(eval_ctx_);
      ASSERT_FALSE(fusion_score_datum.is_null());
      EXPECT_DOUBLE_EQ(expected.fusion_score, fusion_score_datum.get_double())
          << "Fusion score mismatch at index " << idx << ", rowkey=" << expected.rowkey;
    }

    idx++;
  }

  // Verify we got all expected results
  EXPECT_EQ(expected_results.count(), idx) << "Result count mismatch";
}

// Test multiple paths consistency - verify fusion scores are correct when same rowkey appears in multiple paths
TEST_F(ObDASFusionIterTest, test_multiple_paths_data_consistency)
{
  // Create mock rows where same rowkey appears in multiple paths
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  MockDASChildIter::MockRow row1 = {1, 0.9};
  MockDASChildIter::MockRow row2 = {2, 0.7};
  search_rows.push_back(row1);
  search_rows.push_back(row2);

  // Vector path: rowkey 1 appears again
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;
  MockDASChildIter::MockRow row3 = {1, 0.8};
  MockDASChildIter::MockRow row4 = {3, 0.6};
  vec_rows.push_back(row3);
  vec_rows.push_back(row4);

  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows));

  // Trigger fusion
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Expected results:
  // rowkey 1: appears in both paths, fusion_score = (0.9+0.8)/2 = 0.85
  // rowkey 3: only in vec, fusion_score = 0.6*0.5 = 0.3
  // rowkey 2: only in search, fusion_score = 0.7*0.5 = 0.35
  // Expected order: 1 (0.85), 2 (0.35), 3 (0.3)

  struct ExpectedResult {
    uint64_t rowkey;
    double fusion_score;
    bool appears_in_both_paths;

    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV("rowkey", rowkey, "fusion_score", fusion_score,
           "appears_in_both_paths", appears_in_both_paths);
      J_OBJ_END();
      return pos;
    }
  };
  ObSEArray<ExpectedResult, 8> expected_results;
  expected_results.push_back({1, 0.85, true});
  expected_results.push_back({2, 0.35, false});
  expected_results.push_back({3, 0.3, false});

  const ExprFixedArray &rowid_exprs = fusion_ctdef_.rowid_exprs_;
  const ExprFixedArray &result_output = fusion_ctdef_.result_output_;

  int64_t idx = 0;
  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    if (idx < expected_results.count()) {
      const ExpectedResult &expected = expected_results.at(idx);

      // Verify rowkey
      // result_output_ format: [rowkey, search_score, vec_score, fusion_score]
      if (result_output.count() >= 1 && rowid_exprs.count() > 0) {
        // Rowkey is the first element in result_output_
        ObExpr *rowkey_expr = result_output.at(0);
        ObDatum &rowkey_datum = rowkey_expr->locate_expr_datum(eval_ctx_);
        ASSERT_FALSE(rowkey_datum.is_null());
        EXPECT_EQ(expected.rowkey, rowkey_datum.get_uint())
            << "Rowkey mismatch at index " << idx;
      }

      // Verify fusion score
      ObExpr *fusion_score_expr = fusion_param_.fusion_score_expr_;
      if (OB_NOT_NULL(fusion_score_expr)) {
        ObDatum &fusion_score_datum = fusion_score_expr->locate_expr_datum(eval_ctx_);
        ASSERT_FALSE(fusion_score_datum.is_null());
        EXPECT_DOUBLE_EQ(expected.fusion_score, fusion_score_datum.get_double())
            << "Fusion score mismatch at index " << idx << ", rowkey=" << expected.rowkey;
      }

    }

    idx++;
  }

  // Verify we got all expected results
  EXPECT_EQ(expected_results.count(), idx) << "Result count mismatch";
}

// Test LIMIT expression functionality
TEST_F(ObDASFusionIterTest, test_limit_expr)
{
  // Create multiple documents
  ObSEArray<MockDASChildIter::MockRow, 8> search_rows;
  ObSEArray<MockDASChildIter::MockRow, 8> vec_rows;

  for (uint64_t i = 1; i <= 10; ++i) {
    double search_score = 1.0 - (i - 1) * 0.1;  // 1.0, 0.9, 0.8, ..., 0.1
    double vec_score = 1.0 - i * 0.08;            // 0.92, 0.84, 0.76, ..., 0.12
    MockDASChildIter::MockRow srow = {i, search_score};
    MockDASChildIter::MockRow vrow = {i, vec_score};
    search_rows.push_back(srow);
    vec_rows.push_back(vrow);
  }

  // Setup fusion iter with large rank_window_size to get all results
  ASSERT_EQ(OB_SUCCESS, setup_fusion_iter(search_rows, vec_rows, 20));

  // Set LIMIT expression to 3 (should limit output to 3 rows)
  set_limit_expr(3);

  // Re-initialize fusion_iter_ to pick up the limit_expr_
  // Note: limit_expr_ is evaluated in inner_init()
  fusion_param_.fusion_ctdef_ = &fusion_ctdef_;
  fusion_param_.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  fusion_param_.rank_window_size_ = 3;
  fusion_param_.size_ = 3;
  fusion_param_.weights_.reset();
  fusion_param_.weights_.push_back(0.5);
  fusion_param_.weights_.push_back(0.5);
  fusion_param_.fusion_score_expr_ = fusion_ctdef_.get_fusion_score_expr();
  fusion_param_.eval_ctx_ = &eval_ctx_;
  fusion_param_.exec_ctx_ = &exec_ctx_;
  fusion_param_.max_size_ = 1024 * 1024;
  fusion_param_.output_ = &fusion_ctdef_.result_output_;

  // Release and re-init to pick up limit_expr_
  if (fusion_iter_.is_inited()) {
    fusion_iter_.release();
  }
  ASSERT_EQ(OB_SUCCESS, fusion_iter_.init(fusion_param_));

  // Recreate children array (release() sets children_ to nullptr)
  void *children_buf = allocator_.alloc(sizeof(ObDASIter*) * 2);
  ObDASIter **children = nullptr;
  int64_t children_cnt = 0;
  if (OB_NOT_NULL(children_buf)) {
    children = static_cast<ObDASIter**>(children_buf);
  }

  // Create search child iterator
  if (!search_rows.empty() && OB_NOT_NULL(children)) {
    void *search_iter_buf = allocator_.alloc(sizeof(MockDASChildIter));
    if (OB_NOT_NULL(search_iter_buf)) {
      MockDASChildIter *search_iter = new (search_iter_buf) MockDASChildIter(search_rows, &allocator_);
      children[0] = search_iter;
      children_cnt = 1;
    }
  }

  // Create vector child iterator
  if (!vec_rows.empty() && OB_NOT_NULL(children)) {
    void *vec_iter_buf = allocator_.alloc(sizeof(MockDASChildIter));
    if (OB_NOT_NULL(vec_iter_buf)) {
      MockDASChildIter *vec_iter = new (vec_iter_buf) MockDASChildIter(vec_rows, &allocator_);
      children[children_cnt] = vec_iter;
      children_cnt++;
    }
  }

  // Set children to fusion_iter_
  fusion_iter_.get_children() = children;
  fusion_iter_.set_children_cnt(children_cnt);

  // Setup child iterators
  int64_t search_child_idx = -1;
  int64_t vec_child_idx = -1;
  if (!search_rows.empty()) {
    search_child_idx = 0;
    if (!vec_rows.empty()) {
      vec_child_idx = 1;
    }
  } else if (!vec_rows.empty()) {
    vec_child_idx = 0;
  }

  for (int64_t i = 0; i < children_cnt; ++i) {
    MockDASChildIter *child = static_cast<MockDASChildIter*>(children[i]);
    child->set_eval_ctx(&eval_ctx_);
    ObSEArray<ObExpr*, 8> output_exprs;
    ObExpr *fusion_score_expr = fusion_ctdef_.get_fusion_score_expr();
    int64_t rowid_count = fusion_ctdef_.rowid_exprs_.count();
    // Skip rowkey expressions (first rowid_count elements) and fusion_score (last element)
    for (int64_t j = rowid_count; j < fusion_ctdef_.result_output_.count(); ++j) {
      ObExpr *expr = fusion_ctdef_.result_output_.at(j);
      if (expr != fusion_score_expr) {
        output_exprs.push_back(expr);
      }
    }
    child->set_output_exprs(output_exprs);
    child->set_rowid_exprs(fusion_ctdef_.rowid_exprs_);
    if (i == search_child_idx && fusion_ctdef_.has_search_subquery_) {
      child->set_score_expr(fusion_ctdef_.get_search_score_expr());
    } else if (i == vec_child_idx && fusion_ctdef_.has_vector_subquery_) {
      // vec_child_idx directly corresponds to the index in score_exprs_ array
      const ExprFixedArray &score_exprs = fusion_ctdef_.get_score_exprs();
      if (vec_child_idx >= 0 && vec_child_idx < score_exprs.count() - 1) {
        ObExpr *vec_score_expr = score_exprs.at(vec_child_idx);
        child->set_score_expr(vec_score_expr);
      }
    }

    ObDASIterParam child_param(ObDASIterType::DAS_ITER_SCAN);
    child_param.eval_ctx_ = &eval_ctx_;
    child_param.exec_ctx_ = &exec_ctx_;
    child_param.max_size_ = 1024;
    child_param.output_ = &fusion_ctdef_.result_output_;
    ASSERT_EQ(OB_SUCCESS, child->init(child_param));
  }

  ASSERT_EQ(OB_SUCCESS, fusion_iter_.do_table_scan());

  // Verify only 3 results are returned (LIMIT = 3)
  int64_t count = 0;
  ObSEArray<double, 8> actual_scores;

  while (true) {
    int ret = fusion_iter_.inner_get_next_row();
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    ObExpr *fusion_score_expr = fusion_param_.fusion_score_expr_;
    if (OB_NOT_NULL(fusion_score_expr)) {
      ObDatum &datum = fusion_score_expr->locate_expr_datum(eval_ctx_);
      actual_scores.push_back(datum.get_double());
    }
    count++;
  }

  ASSERT_EQ(3, count) << "Should return exactly 3 results with LIMIT=3";
  ASSERT_EQ(3, actual_scores.count());

  // Verify scores are in descending order
  for (int64_t i = 1; i < actual_scores.count(); ++i) {
    EXPECT_LE(actual_scores.at(i), actual_scores.at(i-1))
        << "Scores not in descending order at index " << i;
  }
}

} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
