/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


/*
 * Overview
 * - Abstract base class for DAS search operators.
 *
 * Key Responsibilities
 * - Lifecycle management: open / rescan / close.
 * - Iterative APIs: advance_to, advance_shallow, next_rowid, and optional next_rowids (batch).
 * - Context access: retrieving rowid metadata, eval ctx, max batch size, etc.
 * - Operator identity and children management for composite search plans.
 */
#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_I_DAS_SEARCH_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_I_DAS_SEARCH_OP_H_

#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "lib/container/ob_fixed_array.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace common
{
enum class ObProfileId;
}
namespace sql
{

class ObDASSearchCtx;

/*
 * MaxScoreTuple represents a [mid_id, max_id] interval and its score upper bound.
 * It is returned by the score-contributing operators to the upper layer for score-based dynamic pruning.
*/
struct MaxScoreTuple
{
public:
  MaxScoreTuple()
    : min_id_(),
      max_id_(),
      max_score_(0.0)
  { }

public:
  OB_INLINE void reset()
  {
    min_id_.reset();
    max_id_.reset();
    max_score_ = 0.0;
  }

  OB_INLINE void set(const ObDASRowID &min_id, const ObDASRowID &max_id, double max_score)
  {
    min_id_ = min_id;
    max_id_ = max_id;
    max_score_ = max_score;
  }

  OB_INLINE const ObDASRowID &get_min_id() const { return min_id_; }
  OB_INLINE const ObDASRowID &get_max_id() const { return max_id_; }
  OB_INLINE double get_max_score() const { return max_score_; }

private:
  ObDASRowID min_id_;
  ObDASRowID max_id_;
  double max_score_;
};

enum ObDASSearchOpType
{
  DAS_SEARCH_OP_CONJUNCTION = 0,
  DAS_SEARCH_OP_DISJUNCTION,
  DAS_SEARCH_OP_DISJUNCTION_FILTER,
  DAS_SEARCH_OP_REQ_OPT,
  DAS_SEARCH_OP_REQ_EXCL,
  DAS_SEARCH_OP_SCALAR_INDEX_ROR,
  DAS_SEARCH_OP_SCALAR_PRIMARY_ROR,
  DAS_SEARCH_OP_SCALAR_SCAN,
  DAS_SEARCH_OP_BITMAP,
  DAS_SEARCH_OP_SORT,
  DAS_SEARCH_OP_TOPK_COLLECT,
  DAS_SEARCH_OP_DISJUNCTIVE_MAX,
  DAS_SEARCH_OP_TOKEN,
  DAS_SEARCH_OP_MATCH_PHRASE,
  DAS_SEARCH_OP_DUMMY,
  DAS_SEARCH_OP_BMW,
  DAS_SEARCH_OP_BMM,
  // ...TBD
  DAS_SEARCH_OP_MAX
};

/*
 * RowIDStore - Batch storage for ObDASRowID, materializing rowids from expressions.
 */
class RowIDStore
{
friend class Iterator;
public:
  class Iterator
  {
  public:
    Iterator() : cur_idx_(-1), store_(nullptr) {}
    Iterator(const RowIDStore *store) : cur_idx_(-1), store_(store) {}
    ~Iterator() { reset(); }

    void init(const RowIDStore *store) { store_ = store; cur_idx_ = -1; }
    void reset() { cur_idx_ = -1; store_ = nullptr; }
    void reuse() { cur_idx_ = 0; }
    OB_INLINE bool is_empty() const { return nullptr == store_ || cur_idx_ < 0 || cur_idx_ >= store_->count(); }
    OB_INLINE int64_t cur_idx() const { return cur_idx_; }
    OB_INLINE int64_t count() const { return nullptr == store_ ? 0 : store_->count(); }
    OB_INLINE void next_idx() { ++ cur_idx_; }
    OB_INLINE int get_cur_rowid(ObDASRowID &rowid) const
    {
      return (nullptr != store_) ? store_->get_rowid(cur_idx_, rowid) : OB_NOT_INIT;
    }
    OB_INLINE int lower_bound(const ObDASRowID &target)
    {
      return (nullptr != store_) ? store_->lower_bound(target, cur_idx_, cur_idx_) : OB_NOT_INIT;
    }

  private:
    int64_t cur_idx_;
    const RowIDStore *store_;
  };

  explicit RowIDStore(ObDASSearchCtx &search_ctx) : search_ctx_(search_ctx) {}
  virtual ~RowIDStore() = default;

  virtual int init(int64_t capacity) = 0;
  // fill the rowid store with the rowids from the expressions with range [begin_idx, end_idx)
  virtual int fill(const int64_t begin_idx_inclusive, const int64_t end_idx_exclusive, const common::ObIArray<ObExpr *> &rowid_exprs) = 0;
  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual int64_t count() const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

private:
  virtual int get_rowid(int64_t idx, ObDASRowID &rowid) const = 0;
  virtual int lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const = 0;

protected:
  ObDASSearchCtx &search_ctx_;
};

class UInt64RowIDStore : public RowIDStore
{
public:
  explicit UInt64RowIDStore(ObDASSearchCtx &search_ctx)
    : RowIDStore(search_ctx), data_(nullptr), capacity_(0), count_(0) {}
  ~UInt64RowIDStore() override { reset(); }

  int init(int64_t capacity) override;
  int fill(const int64_t begin_idx_inclusive, const int64_t end_idx_exclusive, const common::ObIArray<ObExpr *> &rowid_exprs) override;
  void reset() override;
  void reuse() override { count_ = 0; }
  int64_t count() const override { return count_; }

  TO_STRING_KV(KP_(data), K_(capacity), K_(count));

private:
  virtual int get_rowid(int64_t idx, ObDASRowID &rowid) const override;
  virtual int lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const override;

private:
  uint64_t *data_;
  int64_t capacity_;
  int64_t count_;
};

class CompactRowIDStore : public RowIDStore
{
public:
  explicit CompactRowIDStore(ObDASSearchCtx &search_ctx)
    : RowIDStore(search_ctx),
      store_(),
      rowids_(nullptr),
      capacity_(0),
      count_(0)
  {}
  ~CompactRowIDStore() override { reset(); }

  int init(int64_t capacity) override;
  int fill(const int64_t begin_idx_inclusive, const int64_t end_idx_exclusive, const common::ObIArray<ObExpr *> &rowid_exprs) override;
  void reset() override;
  void reuse() override;
  int64_t count() const override { return count_; }

  TO_STRING_KV(K_(count));

private:
  virtual int get_rowid(int64_t idx, ObDASRowID &rowid) const override;
  virtual int lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const override;

private:
  ObTempRowStore store_;
  ObCompactRow **rowids_;
  int64_t capacity_;
  int64_t count_;
};

class ObIDASSearchOpParam
{
public:
  ObIDASSearchOpParam(ObDASSearchOpType op_type) : op_type_(op_type) {}
  virtual ~ObIDASSearchOpParam() {}
  OB_INLINE ObDASSearchOpType get_op_type() const { return op_type_; }
  virtual int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const = 0;
  VIRTUAL_TO_STRING_KV(K_(op_type));
private:
  ObDASSearchOpType op_type_;
};

class ObIDASSearchOp
{
public:
  ObIDASSearchOp(ObDASSearchCtx &search_ctx)
    : search_ctx_(search_ctx),
      op_type_(DAS_SEARCH_OP_MAX),
      op_id_(OB_INVALID_ID),
      max_score_tuple_(),
      children_(nullptr),
      children_cnt_(0),
      min_competitive_score_(0.0),
      profile_(nullptr)
  { }
  virtual ~ObIDASSearchOp() {}
  VIRTUAL_TO_STRING_KV(K_(op_id), K_(search_ctx));

  OB_INLINE void set_op_id(const int64_t id) { op_id_ = id; }
  OB_INLINE int64_t get_op_id() const { return op_id_; }
  OB_INLINE ObIDASSearchOp **get_children() { return children_; }

  OB_INLINE const common::ObIArray<ObExpr *> &get_rowid_exprs() { return *search_ctx_.rowid_exprs_; }
  OB_INLINE const RowMeta &get_rowid_meta() { return search_ctx_.rowid_meta_; }
  OB_INLINE int64_t max_batch_size() { return search_ctx_.max_batch_size(); }
  OB_INLINE ObEvalCtx &eval_ctx() { return *search_ctx_.eval_ctx_; }
  OB_INLINE sql::ObBitVector &mock_skip() { return *search_ctx_.mock_skip_; }
  OB_INLINE ObDASSearchOpType get_op_type() const { return op_type_; }
  OB_INLINE ObDASRowIDType get_rowid_type() const { return search_ctx_.rowid_type_; }
  common::ObProfileId get_profile_id() const;
  int open_profile();
  void close_profile();
  void reset_profile();

  // RowID utility methods forwarded from search_ctx_
  OB_INLINE int compare_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2, int &cmp) const
  { return search_ctx_.compare_rowid(rowid1, rowid2, cmp); }
  OB_INLINE int deep_copy_rowid(const ObDASRowID &src, ObDASRowID &dst, common::ObIAllocator &alloc) const
  { return search_ctx_.deep_copy_rowid(src, dst, alloc); }
  OB_INLINE int create_rowid_store(int64_t capacity, RowIDStore *&store)
  { return search_ctx_.create_rowid_store(capacity, store); }
  OB_INLINE int rowid_to_expr(const ObDASRowID &rowid, int64_t batch_idx)
  { return search_ctx_.rowid_to_expr(rowid, batch_idx); }
  OB_INLINE int write_datum_to_rowid(const ObDatum &datum, ObDASRowID &rowid, common::ObIAllocator &alloc) const
  { return search_ctx_.write_datum_to_rowid(datum, rowid, alloc); }
  OB_INLINE int get_datum_from_rowid(const ObDASRowID &rowid, ObDatum &datum, int64_t col_idx = 0) const
  { return search_ctx_.get_datum_from_rowid(rowid, datum, col_idx); }

  int get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id);
  static void switch_tablet_id(const common::ObTabletID &new_tablet_id, storage::ObTableScanParam &scan_param);

public:
// ---------------- core interfaces ----------------
  OB_INLINE int init(const ObIDASSearchOpParam &op_param);

  /*
   * Initializes the operator, opens children operators on demand, and advances to iterable state.
  */
  OB_INLINE int open();

  /*
   * Resets the operator to the state after open(), used in partition rescan.
  */
  OB_INLINE int rescan();

  /*
   * Closes the operator and its children, make the operator to the state before open().
  */
  OB_INLINE int close();

  /*
   * Advances to the next rowid that is greater than or equal to the target rowid.
   * @in param target: The target rowid to advance to.
   * @out param curr_id: The current rowid.
   * @out param score: The score of the current rowid.
   * Ensures returned curr_id lifetime is guaranteed until the next advance_to/next_rowid.
  */
  OB_INLINE int advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score);

  /*
   * Fetches the next rowid.
   * @out param next_id: The next rowid to be returned.
   * @out param score: The score of the next rowid.
   * Ensures returned next_id lifetime is guaranteed until the next advance_to/next_rowid.
  */
  OB_INLINE int next_rowid(ObDASRowID &next_id, double &score);

  /*
   * Advances shallowly using block max information, does not actually advance the rowid.
   * @in param target: The target rowid to advance to.
   * @in param inclusive: Whether target row is is inclusive in max score range,
   * @out param max_score_tuple: A possible MaxScoreTuple.
   * Ensures returned max_score_tuple lifetime is guaranteed until the next advance_to/next_rowid.
  */
  OB_INLINE int advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple);

  /*
   * Fetches the next batch of rowids.
   * @in param capacity: The capacity of the batch.
   * @out param count: The number of rowids fetched.
   * Unlike single-row interface, the batch interface does not support dynamic pruning, thus it directly
   * returns results via expressions instead of explicitly passing parameters.
   * Since most composite operators cannot truly be vectorized in complex query, the batch interface will
   * not be implemented for now.
   */
  OB_INLINE int next_rowids(int64_t capacity, int64_t &count);

  /*
   * Sets the minimum competitive score for dynamic pruning.
   * @in param threshold: The minimum competitive score.
   * Recursively pass the threshold to children operators by default.
   */
  OB_INLINE int set_min_competitive_score(const double &threshold);

  /*
   * Calculates the dimension-level max score, most operators do not need to implement.
   * @out param max_score: The dimension-level max score.
   */
  OB_INLINE int calc_max_score(double &threshold);
// ---------------- core interfaces end ----------------

protected:
// ---------------- interfaces implementation ----------------
  virtual int do_init(const ObIDASSearchOpParam &op_param) { return OB_SUCCESS; }
  virtual int do_open() = 0;
  virtual int do_close() = 0;
  virtual int do_rescan() = 0;
  virtual int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) = 0;
  virtual int do_next_rowid(ObDASRowID &next_id, double &score) = 0;
  virtual int do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple);
  virtual int do_next_rowids(int64_t capacity, int64_t &count);
  virtual int do_set_min_competitive_score(const double &threshold);
  virtual int do_calc_max_score(double &threshold);
// ---------------- interfaces implementation end ----------------

  OB_INLINE common::ObIAllocator &ctx_allocator() const { return search_ctx_.get_allocator(); }

private:
  int init_children_ops_array(const ObIArray<ObIDASSearchOp *> &children);

protected:
  ObDASSearchCtx &search_ctx_;
  ObDASSearchOpType op_type_;
  int64_t op_id_;
  MaxScoreTuple max_score_tuple_;
  ObIDASSearchOp **children_;
  int64_t children_cnt_;
  double min_competitive_score_;
  common::ObOpProfile<common::ObMetric> *profile_;
};

OB_INLINE int ObIDASSearchOp::init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObIDASSearchOp *> children;
  if (OB_FAIL(do_init(op_param))) {
    LOG_WARN("failed to init impl", K(ret));
  } else if (OB_FAIL(op_param.get_children_ops(children))) {
    LOG_WARN("failed to get children ops", KR(ret));
  } else if (OB_FAIL(init_children_ops_array(children))) {
    LOG_WARN("failed to init children ops array", KR(ret));
  } else {
    op_type_ = op_param.get_op_type();
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_profile())) {
    LOG_WARN("failed to open profile", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(profile_);
    if (OB_FAIL(do_open())) {
      LOG_WARN("failed to open impl", KR(ret));
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_close())) {
    LOG_WARN("failed to close impl", KR(ret));
  }
  op_type_ = DAS_SEARCH_OP_MAX;
  op_id_ = OB_INVALID_ID;
  max_score_tuple_.reset();
  min_competitive_score_ = 0.0;
  children_cnt_ = 0;
  children_ = nullptr;
  close_profile();
  return ret;
}

OB_INLINE int ObIDASSearchOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_profile())) {
    LOG_WARN("failed to open profile", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(profile_);
    min_competitive_score_ = 0.0;
    if (OB_FAIL(do_rescan())) {
      LOG_WARN("failed to rescan impl", KR(ret));
    }
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_TIME);
  INC_METRIC_VAL(common::ObMetricId::HS_ADVANCE_COUNT, 1);
  if (OB_FAIL(do_advance_to(target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to impl", KR(ret));
  } else {
    INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 1);
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWID_TIME);
  INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWID_COUNT, 1);
  if (OB_FAIL(do_next_rowid(next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid impl", KR(ret));
  } else {
    INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 1);
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_SHALLOW_TIME);
  INC_METRIC_VAL(common::ObMetricId::HS_ADVANCE_SHALLOW_COUNT, 1);
  if (OB_FAIL(do_advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance shallow impl", KR(ret));
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::next_rowids(int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
  INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
  if (OB_FAIL(do_next_rowids(capacity, count))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowids impl", KR(ret));
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer timer(common::ObMetricId::HS_SET_MIN_COMPETITIVE_SCORE_TIME);
  INC_METRIC_VAL(common::ObMetricId::HS_SET_MIN_COMPETITIVE_SCORE_COUNT, 1);
  if (OB_FAIL(do_set_min_competitive_score(threshold))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to set min competitive score impl", KR(ret));
  }
  return ret;
}

OB_INLINE int ObIDASSearchOp::calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer timer(common::ObMetricId::HS_CALC_MAX_SCORE_TIME);
  INC_METRIC_VAL(common::ObMetricId::HS_CALC_MAX_SCORE_COUNT, 1);
  if (OB_FAIL(do_calc_max_score(threshold))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to calc max score impl", KR(ret));
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_I_DAS_SEARCH_OP_H_
