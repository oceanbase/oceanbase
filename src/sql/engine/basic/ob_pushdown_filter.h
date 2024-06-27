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

#ifndef OB_SQL_ENGINE_BASIC_OB_PUSHDOWN_FILTER_H_
#define OB_SQL_ENGINE_BASIC_OB_PUSHDOWN_FILTER_H_

#include "lib/container/ob_bitmap.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashset.h"
#include "common/object/ob_obj_compare.h"
#include "share/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_in.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColumnParam;
class ColumnMap;
}
}
namespace storage
{
struct ObTableIterParam;
struct ObTableAccessContext;
}

namespace blocksstable
{
struct ObStorageDatum;
struct ObDatumRow;
struct ObSqlDatumInfo;
class ObIMicroBlockRowScanner;
};
namespace sql
{
class ObRawExpr;
class ObOpRawExpr;
class ObExprOperatorCtx;
class ObStaticEngineCG;
class ObPushdownOperator;
struct ObExprFrameInfo;
struct PushdownFilterInfo;
typedef common::ObFixedArray<const share::schema::ObColumnParam*, common::ObIAllocator> ColumnParamFixedArray;

enum PushdownFilterType
{
  BLACK_FILTER,
  WHITE_FILTER,
  AND_FILTER,
  OR_FILTER,
  DYNAMIC_FILTER,
  SAMPLE_FILTER,
  MAX_FILTER_TYPE
};

enum PushdownExecutorType
{
  BLACK_FILTER_EXECUTOR,
  WHITE_FILTER_EXECUTOR,
  AND_FILTER_EXECUTOR,
  OR_FILTER_EXECUTOR,
  DYNAMIC_FILTER_EXECUTOR,
  SAMPLE_FILTER_EXECUTOR,
  MAX_EXECUTOR_TYPE
};

enum ObBoolMaskType
{
  PROBABILISTIC = 0,
  ALWAYS_TRUE,
  ALWAYS_FALSE
};

enum DynamicFilterAction
{
  DO_FILTER = 0, // this filter needs to been done
  FILTER_ALL, // if the filter is empty, filter all data
  PASS_ALL,  // if the filter not ready or not active, all data are selected
};

enum DynamicFilterType
{
  JOIN_RUNTIME_FILTER= 0,
  PD_TOPN_FILTER = 1,
  MAX_DYNAMIC_FILTER_TYPE
};

struct ObBoolMask
{
  ObBoolMask()
    : bmt_(ObBoolMaskType::PROBABILISTIC) {};
  ObBoolMask(ObBoolMaskType bmt)
    : bmt_(bmt) {};
  OB_INLINE ObBoolMask operator &(const ObBoolMask &m) const
  {
    if (m.bmt_ == ObBoolMaskType::ALWAYS_TRUE) {
      return {bmt_};
    } else if (bmt_ == ObBoolMaskType::ALWAYS_TRUE) {
      return {m.bmt_};
    } else if (m.bmt_ == ObBoolMaskType::ALWAYS_FALSE
                || bmt_ == ObBoolMaskType::ALWAYS_FALSE) {
      return {ObBoolMaskType::ALWAYS_FALSE};
    } else {
      return {ObBoolMaskType::PROBABILISTIC};
    }
  }
  OB_INLINE ObBoolMask operator |(const ObBoolMask &m) const
  {
    if (m.bmt_ == ObBoolMaskType::ALWAYS_FALSE) {
      return {bmt_};
    } else if (bmt_ == ObBoolMaskType::ALWAYS_FALSE) {
      return {m.bmt_};
    } else if (m.bmt_ == ObBoolMaskType::ALWAYS_TRUE
                || bmt_ == ObBoolMaskType::ALWAYS_TRUE) {
      return {ObBoolMaskType::ALWAYS_TRUE};
    } else {
      return {ObBoolMaskType::PROBABILISTIC};
    }
  }
  OB_INLINE bool is_always_true() const
  {
    return ObBoolMaskType::ALWAYS_TRUE == bmt_;
  }
  OB_INLINE bool is_always_false() const
  {
    return ObBoolMaskType::ALWAYS_FALSE == bmt_;
  }
  OB_INLINE bool is_uncertain() const
  {
    return ObBoolMaskType::PROBABILISTIC == bmt_;
  }
  OB_INLINE void set_always_true()
  {
    bmt_ = ObBoolMaskType::ALWAYS_TRUE;
  }
  OB_INLINE void set_always_false()
  {
    bmt_ = ObBoolMaskType::ALWAYS_FALSE;
  }
  OB_INLINE void set_uncertain()
  {
    bmt_ = ObBoolMaskType::PROBABILISTIC;
  }
  TO_STRING_KV(K_(bmt));

  ObBoolMaskType bmt_;
};

struct ObStoragePushdownFlag
{
public:
  ObStoragePushdownFlag() : pd_flag_(0) {};
  ObStoragePushdownFlag(const int32_t flag) : pd_flag_(flag) {}
  ~ObStoragePushdownFlag() = default;
  const ObStoragePushdownFlag &operator =(const ObStoragePushdownFlag &other) {
    if (this != &other) {
      this->pd_flag_ = other.pd_flag_;
    }
    return *this;
  }

  OB_INLINE void set_blockscan_pushdown(const bool block_scan) { pd_blockscan_ = block_scan; }
  OB_INLINE void set_filter_pushdown(const bool filter) { pd_filter_ = filter; }
  OB_INLINE void set_aggregate_pushdown(const bool aggregate) { pd_aggregate_ = aggregate; }
  OB_INLINE void set_group_by_pushdown(const bool groupby) { pd_group_by_ = groupby; }
  OB_INLINE void set_filter_reorder(const bool filter_reorder) { pd_filter_reorder_ = filter_reorder; }
  OB_INLINE void set_enable_skip_index(const bool skip_index) { enable_skip_index_ = skip_index; }
  OB_INLINE void set_use_stmt_iter_pool(const bool use_pool) { use_stmt_iter_pool_ = use_pool; }
  OB_INLINE void set_use_column_store(const bool use_cs) { use_column_store_ = use_cs; }
  OB_INLINE void set_enable_prefetch_limiting(const bool enable_limit) { enable_prefetch_limiting_ = enable_limit; }
  OB_INLINE void set_use_global_iter_pool(const bool use_iter_mgr) { use_global_iter_pool_ = use_iter_mgr; }
  OB_INLINE void set_flags(const bool block_scan, const bool filter, const bool skip_index,
                           const bool use_cs, const bool enable_limit, const bool filter_reorder = true)
  {
    set_blockscan_pushdown(block_scan);
    set_filter_pushdown(filter);
    set_filter_reorder(filter_reorder);
    set_enable_skip_index(skip_index);
    set_use_column_store(use_cs);
    set_enable_prefetch_limiting(enable_limit);
  }

  OB_INLINE bool is_blockscan_pushdown() const { return pd_blockscan_; }
  OB_INLINE bool is_filter_pushdown() const { return pd_filter_; }
  OB_INLINE bool is_aggregate_pushdown() const { return pd_aggregate_; }
  OB_INLINE bool is_group_by_pushdown() const { return pd_group_by_; }
  OB_INLINE bool is_filter_reorder() const { return pd_filter_reorder_; }
  OB_INLINE bool is_apply_skip_index() const { return enable_skip_index_; }
  OB_INLINE bool is_use_stmt_iter_pool() const { return use_stmt_iter_pool_; }
  OB_INLINE bool is_use_column_store() const { return use_column_store_; }
  OB_INLINE bool is_enable_prefetch_limiting() const { return enable_prefetch_limiting_; }
  OB_INLINE bool is_use_global_iter_pool() const { return use_global_iter_pool_; }
  TO_STRING_KV(K_(pd_flag));

  union {
    struct {
      int32_t pd_blockscan_ : 1;
      int32_t pd_filter_ : 1;
      int32_t pd_aggregate_ : 1;
      int32_t pd_group_by_ : 1;
      int32_t pd_filter_reorder_ : 1;
      int32_t enable_skip_index_ : 1;
      int32_t use_stmt_iter_pool_:1;
      int32_t use_column_store_:1;
      int32_t enable_prefetch_limiting_ : 1;
      int32_t use_global_iter_pool_:1;
      int32_t reserved_ : 22;
    };
    int32_t pd_flag_;
  };
};

class ObPushdownFilterUtils
{
public:
  OB_INLINE static bool is_blockscan_pushdown_enabled(int32_t pd_level)
  { return pd_level > 0; }
  OB_INLINE static bool is_filter_pushdown_enabled(int32_t pd_level)
  { return pd_level > 1; }
  OB_INLINE static bool is_aggregate_pushdown_enabled(int32_t pd_level)
  { return pd_level > 2; }
  OB_INLINE static bool is_group_by_pushdown_enabled(int32_t pd_level)
  { return pd_level > 3; }
};

class ObPushdownFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownFilterNode(common::ObIAllocator &alloc,
                       PushdownFilterType type = PushdownFilterType::MAX_FILTER_TYPE)
      : alloc_(alloc), type_(type), n_child_(0), childs_(nullptr),
      col_ids_(alloc)
  {}
  PushdownFilterType get_type() const { return type_; }
  common::ObIArray<uint64_t> &get_col_ids() { return col_ids_; }
  void set_type(PushdownFilterType type) { type_ = type; }
  virtual int merge(common::ObIArray<ObPushdownFilterNode*> &merged_node)
  {
    UNUSED(merged_node);
    return common::OB_SUCCESS;
  }
  virtual int postprocess() { return common::OB_SUCCESS; }
  VIRTUAL_TO_STRING_KV(K_(type), K_(n_child), K_(col_ids));
public:
  common::ObIAllocator &alloc_;
  PushdownFilterType type_;
  uint32_t n_child_;
  ObPushdownFilterNode **childs_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> col_ids_;           // 这个node涉及到的列集合
};

class ObPushdownAndFilterNode : public ObPushdownFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownAndFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::AND_FILTER),
        is_runtime_filter_root_node_(false)
  {}
public:
  // for runtime filter with multiple columns, we need a 'and' node
  // to organize all dynamic nodes(each dynamic node corresponds to one column)
  bool is_runtime_filter_root_node_;
};

class ObPushdownOrFilterNode : public ObPushdownFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownOrFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::OR_FILTER)
  {}
};

// Suppose f(x) is a black filter, the monotonicity of f(x) can be utilized to filter rows and accelerate query.
// The rules for different monotonicities are as follows:
enum PushdownFilterMonotonicity
{
  MON_NON = 0,    // no monotonicity
  MON_ASC = 1,    // If f(min) is true, all f(x) is true if x >= min. If f(max) is false, all f(x) is false if x <= max.
  MON_DESC = 2,   // If f(max) is true, all f(x) if true if x <= max. If f(min) is false, all f(x) is false if x >= min.
  MON_EQ_ASC = 3, // f(x) = const and f(x) is monotonicity asc. If f(min) > const || f(max) < const, f(x) is false if x in [min, max].
  MON_EQ_DESC = 4 // f(x) = const and f(x) is monotonicity desc. If f(min) < const || f(max) > const, f(x) is false if x in [min, max].
};

class ObPushdownBlackFilterNode : public ObPushdownFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownBlackFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::BLACK_FILTER),
      column_exprs_(alloc),
      filter_exprs_(alloc),
      tmp_expr_(nullptr),
      assist_exprs_(alloc),
      mono_(MON_NON)
  {}
  ~ObPushdownBlackFilterNode() {}

  int merge(common::ObIArray<ObPushdownFilterNode*> &merged_node) override;
  virtual int postprocess() override;
  OB_INLINE bool is_monotonic() const { return MON_NON != mono_; }
  INHERIT_TO_STRING_KV("ObPushdownBlackFilterNode", ObPushdownFilterNode,
                       K_(column_exprs), K_(filter_exprs), K_(assist_exprs), K_(mono));

  int64_t get_filter_expr_count()
  { return filter_exprs_.empty() ? 1 : filter_exprs_.count(); }
public:
  ExprFixedArray column_exprs_; // 列对应的表达式
  ExprFixedArray filter_exprs_; // 下压filters
  // 下压临时保存的filter，如果发生merge，则所有的filter放入filter_exprs_
  // 如果没有发生merge，则将自己的tmp_expr_放入filter_exprs_中
  ObExpr *tmp_expr_;
  // The exprs to judge greater or less when mono_ is MON_EQ_ASC/MON_EQ_DESC.
  // assist_exprs_[0] is greater expr, assist_exprs_[1] is less expr.
  ExprFixedArray assist_exprs_;
  PushdownFilterMonotonicity mono_;
};

enum ObWhiteFilterOperatorType
{
  WHITE_OP_EQ = 0, // ==
  WHITE_OP_LE, // <=
  WHITE_OP_LT, // <
  WHITE_OP_GE, // >=
  WHITE_OP_GT, // >
  WHITE_OP_NE, // <>
  WHITE_OP_BT, // between(1, 10)
  WHITE_OP_IN, // in (1, 2, 3)
  WHITE_OP_NU, // is null
  WHITE_OP_NN, // is not null
  // WHITE_OP_LI, // like
  WHITE_OP_MAX,
};
class ObPushdownWhiteFilterNode : public ObPushdownFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownWhiteFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::WHITE_FILTER),
      expr_(nullptr), op_type_(WHITE_OP_MAX) , column_exprs_(alloc)
  {}
  ~ObPushdownWhiteFilterNode() {}
  virtual OB_INLINE int set_op_type(const ObRawExpr &raw_expr);
  OB_INLINE ObWhiteFilterOperatorType get_op_type() const { return op_type_; }
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const;
  inline virtual ObObjType get_filter_arg_obj_type(int64_t arg_idx) const
  {
    const ObExpr *expr = WHITE_OP_IN == op_type_ ? expr_->args_[1] : expr_;
    return expr->args_[arg_idx]->obj_meta_.get_type();
  }

  // mapping array from white filter's operation type to common::ObCmpOp
  static const common::ObCmpOp WHITE_OP_TO_CMP_OP[WHITE_OP_MAX];
  INHERIT_TO_STRING_KV("ObPushdownWhiteFilterNode", ObPushdownFilterNode,
                       KPC_(expr), K_(op_type));
public:
  ObExpr *expr_;
protected:
  ObWhiteFilterOperatorType op_type_;
public:
  ExprFixedArray column_exprs_; // 列对应的表达式
};

class ObPushdownDynamicFilterNode : public ObPushdownWhiteFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownDynamicFilterNode(common::ObIAllocator &alloc)
      : ObPushdownWhiteFilterNode(alloc), col_idx_(0), is_first_child_(false),
        is_last_child_(false), val_meta_(),
        dynamic_filter_type_(DynamicFilterType::MAX_DYNAMIC_FILTER_TYPE)
  {
    type_ = PushdownFilterType::DYNAMIC_FILTER;
  }
  ~ObPushdownDynamicFilterNode() {}
  int set_op_type(const ObRawExpr &raw_expr) override;
  // for topn filter, we set op_type during runtime rather than compile time
  inline void set_op_type(const ObWhiteFilterOperatorType &op_type) { op_type_ = op_type; }
  inline int64_t get_col_idx() const
  {
    return col_idx_;
  }
  inline void set_col_idx(int col_idx) { col_idx_ = col_idx; }
  inline void set_first_child(bool val) { is_first_child_ = val; }
  inline bool is_first_child() { return is_first_child_; }
  inline void set_last_child(bool val) { is_last_child_ = val; }
  inline bool is_last_child() { return is_last_child_; }
  inline void set_filter_val_meta(const ObObjMeta &val_meta) { val_meta_ = val_meta; }
  inline ObObjMeta get_filter_val_meta() const { return val_meta_; }
  inline int get_filter_val_meta(ObObjMeta &obj_meta) const override final
  {
    obj_meta = val_meta_;
    return OB_SUCCESS;
  }
  inline ObObjType get_filter_arg_obj_type(int64_t arg_idx) const override final
  {
    UNUSED(arg_idx);
    return val_meta_.get_type();
  }
  inline void set_dynamic_filter_type(DynamicFilterType func_type)
  {
    dynamic_filter_type_ = func_type;
  }
  inline DynamicFilterType get_dynamic_filter_type() const
  {
    return dynamic_filter_type_;
  }
  inline bool need_continuous_update()
  {
    return DynamicFilterType::PD_TOPN_FILTER == dynamic_filter_type_;
  }

private:
  int64_t col_idx_; // mark which column for multi columns runtime filter
  /*
    when a multi columns runtime filter splitted into multiple push down nodes,
    the first child node need to update the total rows and check rows,
    all childs need to update the filterd rows.
    note that, all multiple push down nodes split from one runtime filter share the same slide
    window, so the last child need to update the slide window to dynamic disable/enable these
    filters.
  */
  bool is_first_child_;
  bool is_last_child_;
  ObObjMeta val_meta_;
  DynamicFilterType dynamic_filter_type_; // FARM COMPAT WHITELIST for prepare_data_func_type_
};

class ObPushdownFilterExecutor;
class ObPushdownFilterNode;
class ObPushdownFilterFactory
{
public:
  ObPushdownFilterFactory(common::ObIAllocator *alloc)
      : alloc_(alloc)
  {}
  int alloc(PushdownFilterType type, uint32_t n_child, ObPushdownFilterNode *&pd_filter);
  int alloc(PushdownExecutorType type,
            uint32_t n_child,
            ObPushdownFilterNode &filter_node,
            ObPushdownFilterExecutor *&filter_executor,
            ObPushdownOperator &op);

private:
  // pushdown filter
  typedef int (*PDFilterAllocFunc) (common::ObIAllocator &alloc, uint32_t n_child, ObPushdownFilterNode *&filter_node);
  template <typename ClassT, PushdownFilterType type>
      static int alloc(common::ObIAllocator &alloc, uint32_t n_child, ObPushdownFilterNode *&filter_executor);
  static PDFilterAllocFunc PD_FILTER_ALLOC[PushdownFilterType::MAX_FILTER_TYPE];

  // filter executor
  typedef int (*FilterExecutorAllocFunc) (common::ObIAllocator &alloc, uint32_t n_child,
                                          ObPushdownFilterNode &filter_node,
                                          ObPushdownFilterExecutor *&filter_executor,
                                          ObPushdownOperator &op);
  template <typename ClassT, typename FilterNodeT, PushdownExecutorType type>
      static int alloc(common::ObIAllocator &alloc,
                       uint32_t n_child,
                       ObPushdownFilterNode &filter_node,
                       ObPushdownFilterExecutor *&filter_executor,
                       ObPushdownOperator &op);
  static FilterExecutorAllocFunc FILTER_EXECUTOR_ALLOC[PushdownExecutorType::MAX_EXECUTOR_TYPE];
private:
  common::ObIAllocator *alloc_;
};

class ObPushdownFilterConstructor
{
public:
  ObPushdownFilterConstructor(common::ObIAllocator *alloc,
                              ObStaticEngineCG &static_cg,
                              const ObLogTableScan *op,
                              bool use_column_store)
      : alloc_(alloc), factory_(alloc), static_cg_(static_cg), op_(op), use_column_store_(use_column_store)
  {}
  int apply(common::ObIArray<ObRawExpr*> &exprs, ObPushdownFilterNode *&filter_tree);

private:
  int is_white_mode(const ObRawExpr* raw_expr, bool &is_white);
  int create_black_filter_node(ObRawExpr *raw_expr, ObPushdownFilterNode *&filter_tree);
  int get_black_filter_monotonicity(const ObRawExpr *raw_expr,
                                   common::ObIArray<ObRawExpr *> &column_exprs,
                                   ObPushdownBlackFilterNode *black_filter_node);
  template <typename ClassT, PushdownFilterType type>
  int create_white_or_dynamic_filter_node(ObRawExpr *raw_expr,
      ObPushdownFilterNode *&filter_tree, int64_t col_idx = 0);
  int merge_filter_node(
      ObPushdownFilterNode *dst,
      ObPushdownFilterNode *other,
      common::ObIArray<ObPushdownFilterNode*> &merged_node,
      bool &merged);
  int deduplicate_filter_node(common::ObIArray<ObPushdownFilterNode*> &filter_nodes, uint32_t &n_node);
  int generate_and_deduplicate(const common::ObIArray<ObRawExpr*> &exprs,
               common::ObArray<ObPushdownFilterNode*> &filter_nodes,
               uint32_t &valid_nodes,
               const bool need_dedup);
  int generate(ObRawExpr *raw_expr, ObPushdownFilterNode *&filter_tree);
  int split_multi_cols_runtime_filter(ObOpRawExpr *raw_expr, ObPushdownFilterNode *&filter_tree);
  int remove_runtime_filter_root_node(ObArray<ObPushdownFilterNode *> &filter_nodes,
                                uint32_t &valid_nodes);

private:
  common::ObIAllocator *alloc_;
  ObPushdownFilterFactory factory_;
  ObStaticEngineCG &static_cg_;
  const ObLogTableScan *op_;
  bool use_column_store_;
};

// wrapper for PushdownFilterNode to support serilize
class ObPushdownFilter
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownFilter(common::ObIAllocator &alloc)
      : alloc_(alloc), filter_tree_(nullptr)
  {}

  void set_filter_tree(ObPushdownFilterNode *filter_tree) { filter_tree_ = filter_tree; }
  ObPushdownFilterNode *&get_pushdown_filter() { return filter_tree_; }
  ObPushdownFilterNode *get_pushdown_filter() const { return filter_tree_; }
  static int serialize_pushdown_filter(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       ObPushdownFilterNode *pd_storage_filter);
  static int deserialize_pushdown_filter(ObPushdownFilterFactory filter_factory,
                                         const char *buf,
                                         int64_t data_len,
                                         int64_t &pos,
                                         ObPushdownFilterNode *&pd_storage_filter);
  static int64_t get_serialize_pushdown_filter_size(ObPushdownFilterNode *pd_filter_node);
  // NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(KP(filter_tree_));
private:
  common::ObIAllocator &alloc_;
  ObPushdownFilterNode *filter_tree_;
};

enum ObCommonFilterTreeStatus : uint8_t
{
  NONE_FILTER = 0,
  WHITE = 1,
  SINGLE_BLACK = 2,
  MULTI_BLACK = 3,
  MAX_STATUS = 4
};

// executor interface
// 类似新框架的operator，而ObPushdownFilterNode则对应的是ObOpSpec接口
// 即一个是编译器的接口，一个是运行时接口
class ObPushdownFilterExecutor
{
public:
  static const int64_t INVALID_CG_ITER_IDX = -1;
public:
  ObPushdownFilterExecutor(common::ObIAllocator &alloc,
                           ObPushdownOperator &op,
                           PushdownExecutorType type = PushdownExecutorType::MAX_EXECUTOR_TYPE);
  virtual ~ObPushdownFilterExecutor();

  // interface for storage
  virtual OB_INLINE bool is_filter_black_node() const { return type_ == BLACK_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_filter_white_node() const
  {
    return type_ == WHITE_FILTER_EXECUTOR || type_ == DYNAMIC_FILTER_EXECUTOR;
  }
  virtual OB_INLINE bool is_sample_node() const { return type_ == SAMPLE_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_filter_node() const { return is_filter_black_node() || is_filter_white_node() || is_sample_node(); }
  virtual OB_INLINE bool is_logic_and_node() const { return type_ == AND_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_logic_or_node() const { return type_ == OR_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_logic_op_node() const { return is_logic_and_node() || is_logic_or_node(); }
  OB_INLINE bool is_filter_dynamic_node() const { return type_ == DYNAMIC_FILTER_EXECUTOR; }
  virtual OB_INLINE bool filter_can_continuous_filter() const { return true; }
  int prepare_skip_filter();
  OB_INLINE bool can_skip_filter(int64_t row) const
  {
    bool fast_skip = false;
    if (OB_LIKELY(need_check_row_filter_ && nullptr != filter_bitmap_)) {
      if (PushdownExecutorType::AND_FILTER_EXECUTOR == type_) {
        fast_skip = !filter_bitmap_->test(row);
      } else if (PushdownExecutorType::OR_FILTER_EXECUTOR == type_) {
        fast_skip = filter_bitmap_->test(row);
      }
    }
    skipped_rows_ += fast_skip;
    return fast_skip;
  }
  OB_INLINE bool can_skip_filter(int64_t start, int64_t end) const
  {
    bool fast_skip = false;
    if (OB_LIKELY(need_check_row_filter_ && nullptr != filter_bitmap_)) {
      if (PushdownExecutorType::AND_FILTER_EXECUTOR == type_) {
        fast_skip = filter_bitmap_->is_all_false(start, end);
      } else if (PushdownExecutorType::OR_FILTER_EXECUTOR == type_) {
        fast_skip = filter_bitmap_->is_all_true(start, end);
      }
    }
    return fast_skip;
  }
  OB_INLINE bool need_check_row_filter() const { return need_check_row_filter_; }
  void set_type(PushdownExecutorType type) { type_ = type; }
  OB_INLINE PushdownExecutorType get_type() const { return type_; }
  OB_INLINE void set_status(ObCommonFilterTreeStatus filter_tree_status) { filter_tree_status_ = filter_tree_status; }
  OB_INLINE ObCommonFilterTreeStatus get_status() const { return filter_tree_status_; }
  virtual common::ObIArray<uint64_t> &get_col_ids() = 0;
  OB_INLINE int64_t get_col_count() const { return n_cols_; }
  OB_INLINE virtual ObPushdownOperator & get_op() { return op_; }
  OB_INLINE const common::ObIArray<int32_t> &get_col_offsets(const bool is_cg = false) const
  {
    return is_cg ? cg_col_offsets_ : col_offsets_;
  }
  OB_INLINE const ColumnParamFixedArray &get_col_params() const { return col_params_; }
  OB_INLINE const common::ObFixedArray<blocksstable::ObStorageDatum, common::ObIAllocator> &get_default_datums() const
  { return default_datums_; }
  OB_INLINE const common::ObIArray<uint32_t> &get_cg_idxs() const { return cg_idxs_; }
  virtual const common::ObIArray<ObExpr *> *get_cg_col_exprs() const
  { return cg_col_exprs_.empty() ? nullptr : &cg_col_exprs_; }
  OB_INLINE bool is_cg_param_valid() const
  { return !cg_idxs_.empty() && cg_col_exprs_.count() <= cg_idxs_.count(); }
  OB_INLINE uint32_t get_child_count() const { return n_child_; }
  OB_INLINE int64_t get_cg_iter_idx() const { return cg_iter_idx_; }
  OB_INLINE void set_cg_iter_idx(const int64_t cg_iter_idx)
  {
    cg_iter_idx_ = cg_iter_idx;
  }
  OB_INLINE bool is_filter_always_true() const { return filter_bool_mask_.is_always_true(); }
  OB_INLINE bool is_filter_always_false() const { return filter_bool_mask_.is_always_false(); }
  OB_INLINE bool is_filter_constant() const { return !filter_bool_mask_.is_uncertain(); }
  OB_INLINE void set_filter_uncertain()
  {
    filter_bool_mask_.set_uncertain();
  }
  OB_INLINE void set_filter_bool_mask(const ObBoolMask &bool_mask)
  {
    filter_bool_mask_ = bool_mask;
  }
  OB_INLINE ObBoolMask &get_filter_bool_mask()
  {
    return filter_bool_mask_;
  }
  OB_INLINE void set_filter_rewrited() { is_rewrited_ = true; }
  OB_INLINE bool is_filter_rewrited() const { return is_rewrited_; }
  OB_INLINE int64_t get_skipped_rows() const { return skipped_rows_; }
  OB_INLINE void clear_skipped_rows() { skipped_rows_ = 0; };
  inline int get_child(uint32_t nth_child, ObPushdownFilterExecutor *&filter_executor)
  {
    int ret = common::OB_SUCCESS;
    if (nth_child >= n_child_) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalid child", K(nth_child), K(n_child_));
    } else {
      filter_executor = childs_[nth_child];
    }
    return ret;
  }
  void set_child(uint32_t i, ObPushdownFilterExecutor *child)
  {
    childs_[i] = child;
  }
  void set_childs(uint32_t n_child, ObPushdownFilterExecutor **childs)
  {
    n_child_ = n_child;
    childs_ = childs;
  }
  ObPushdownFilterExecutor **get_childs() const { return childs_; }
  const common::ObBitmap *get_result() const { return filter_bitmap_; }
  int init_bitmap(const int64_t row_count, common::ObBitmap *&bitmap);
  int init_filter_param(
      const common::ObIArray<share::schema::ObColumnParam *> &col_params,
      const common::ObIArray<int32_t> &output_projector,
      const bool need_padding);
  int init_co_filter_param(const storage::ObTableIterParam &iter_param, const bool need_padding);
  int set_cg_param(const common::ObIArray<uint32_t> &cg_idxs, const common::ObIArray<ObExpr *> &exprs);
  int pull_up_common_node(
      const common::ObIArray<uint32_t> &filter_indexes,
      ObPushdownFilterExecutor *&common_filter_executor);
  virtual int init_evaluated_datums() { return common::OB_NOT_SUPPORTED; }
  int execute(
      ObPushdownFilterExecutor *parent,
      PushdownFilterInfo &filter_info,
      blocksstable::ObIMicroBlockRowScanner *micro_scanner,
      const bool use_vectorize);
  int execute_skipping_filter(ObBoolMask &bm);
  virtual void clear(); // release array and set memory used by WHITE_OP_IN filter.
  DECLARE_VIRTUAL_TO_STRING;
protected:
  int find_evaluated_datums(
      ObExpr *expr, const common::ObIArray<ObExpr*> &calc_exprs, common::ObIArray<ObExpr*> &eval_exprs);
  int find_evaluated_datums(
      common::ObIArray<ObExpr*> &src_exprs,
      const common::ObIArray<ObExpr*> &calc_exprs,
      common::ObIArray<ObExpr*> &eval_exprs);
  template<typename T>
  int init_array_param(common::ObFixedArray<T, common::ObIAllocator> &param, const int64_t size);
private:
  bool check_sstable_index_filter();
  int build_new_sub_filter_tree(
      const common::ObIArray<uint32_t> &filter_indexes,
      ObPushdownFilterExecutor *&new_filter_executor);
  int do_filter(
    ObPushdownFilterExecutor *parent,
    PushdownFilterInfo &filter_info,
    blocksstable::ObIMicroBlockRowScanner *micro_scanner,
    const bool use_vectorize,
    common::ObBitmap &result_bitmap);
protected:
  PushdownExecutorType type_;
  bool need_check_row_filter_;
  ObCommonFilterTreeStatus filter_tree_status_;
  int64_t n_cols_;
  uint32_t n_child_;
  int64_t cg_iter_idx_;
  mutable int64_t skipped_rows_;
  ObPushdownFilterExecutor **childs_;
  common::ObBitmap *filter_bitmap_;
  ColumnParamFixedArray col_params_;
  common::ObFixedArray<int32_t, common::ObIAllocator> col_offsets_;
  common::ObFixedArray<int32_t, common::ObIAllocator> cg_col_offsets_;
  common::ObFixedArray<blocksstable::ObStorageDatum, common::ObIAllocator> default_datums_;
  common::ObFixedArray<uint32_t, common::ObIAllocator> cg_idxs_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> cg_col_exprs_;
  common::ObIAllocator &allocator_;
  ObPushdownOperator &op_;
private:
  bool is_rewrited_;
  ObBoolMask filter_bool_mask_;
};

class ObPhysicalFilterExecutor : public ObPushdownFilterExecutor
{
public:
  ObPhysicalFilterExecutor(common::ObIAllocator &alloc,
                           ObPushdownOperator &op,
                           PushdownExecutorType type)
      : ObPushdownFilterExecutor(alloc, op, type),
        n_eval_infos_(0),
        eval_infos_(nullptr),
        n_datum_eval_flags_(0),
        datum_eval_flags_(nullptr)
  {}
  virtual ~ObPhysicalFilterExecutor();
  int filter(blocksstable::ObStorageDatum *datums, int64_t col_cnt, const sql::ObBitVector &skip_bit, bool &ret_val);
  virtual int init_evaluated_datums() override;
  virtual int filter(ObEvalCtx &eval_ctx, const sql::ObBitVector &skip_bit, bool &filtered) = 0;
  INHERIT_TO_STRING_KV("ObPhysicalFilterExecutor", ObPushdownFilterExecutor,
                       K_(n_eval_infos), KP_(eval_infos));
protected:
  int init_eval_param(const int32_t cur_eval_info_cnt, const int64_t eval_expr_cnt);
  void clear_evaluated_flags();
  void clear_evaluated_infos();
protected:
  int32_t n_eval_infos_;
  ObEvalInfo **eval_infos_;
  int32_t n_datum_eval_flags_;
  ObBitVector **datum_eval_flags_;
};

class ObBlackFilterExecutor : public ObPhysicalFilterExecutor
{
public:
  ObBlackFilterExecutor(common::ObIAllocator &alloc,
                        ObPushdownBlackFilterNode &filter,
                        ObPushdownOperator &op)
      : ObPhysicalFilterExecutor(alloc, op, PushdownExecutorType::BLACK_FILTER_EXECUTOR),
        filter_(filter), skip_bit_(nullptr)
  {}
  ~ObBlackFilterExecutor();

  OB_INLINE ObPushdownBlackFilterNode &get_filter_node() { return filter_; }
  OB_INLINE virtual common::ObIArray<uint64_t> &get_col_ids() override
  { return filter_.get_col_ids(); }
  virtual const common::ObIArray<ObExpr *> *get_cg_col_exprs() const override { return &filter_.column_exprs_; }
  OB_INLINE bool can_vectorized();
  int filter_batch(ObPushdownFilterExecutor *parent,
                   const int64_t start,
                   const int64_t end,
                   common::ObBitmap &result_bitmap);
  int get_datums_from_column(common::ObIArray<blocksstable::ObSqlDatumInfo> &datum_infos);
  INHERIT_TO_STRING_KV("ObPushdownBlackFilterExecutor", ObPhysicalFilterExecutor,
                       K_(filter), KP_(skip_bit));
  virtual int filter(ObEvalCtx &eval_ctx, const sql::ObBitVector &skip_bit, bool &filtered) override;
  OB_INLINE bool filter_can_continuous_filter() const override final {
    bool can_continuous_filter = true;
    for (int64_t i = 0; i < filter_.filter_exprs_.count();++i) {
      if (T_OP_PUSHDOWN_TOPN_FILTER == filter_.filter_exprs_.at(i)->type_) {
        can_continuous_filter = false;
        break;
      }
    }
    return can_continuous_filter;
  }
  virtual int filter(blocksstable::ObStorageDatum &datum, const sql::ObBitVector &skip_bit, bool &ret_val);
  virtual int judge_greater_or_less(blocksstable::ObStorageDatum &datum,
                                   const sql::ObBitVector &skip_bit,
                                   const bool is_greater,
                                   bool &ret_val);
  OB_INLINE bool is_monotonic() const { return filter_.is_monotonic(); }
  OB_INLINE PushdownFilterMonotonicity get_monotonicity() const { return filter_.mono_; }
private:
  int eval_exprs_batch(ObBitVector &skip, const int64_t bsize);

private:
  ObPushdownBlackFilterNode &filter_;
  ObBitVector *skip_bit_;
};

class ObWhiteFilterParam
{
public:
  ObWhiteFilterParam() : datum_(nullptr), hash_func_(nullptr), cmp_func_(nullptr) {}
  ObWhiteFilterParam(const ObDatum *datum,
      const ObExprHashFuncType *hash_func,
      const ObDatumCmpFuncType *cmp_func)
    : datum_(datum),
      hash_func_(hash_func),
      cmp_func_(cmp_func)
  {}
  ~ObWhiteFilterParam() {}
  inline bool operator==(const ObWhiteFilterParam &other) const
  {
    bool equal_ret = true;
    if (datum_->is_null() && other.datum_->is_null()) {
    } else if (datum_->is_null() || other.datum_->is_null()) {
      equal_ret = false;
    } else {
      int cmp_ret = 0;
      (*cmp_func_)(*datum_, *other.datum_, cmp_ret);
      equal_ret = cmp_ret == 0;
    }
    return equal_ret;
  }
  inline int hash(uint64_t &hash_val, uint64_t seed = 0) const
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL((*hash_func_)(*datum_, seed, seed))) {
      STORAGE_LOG(WARN, "Failed to do hash for datum", K_(datum));
    } else {
      hash_val = seed;
    }
    return ret;
  }
  TO_STRING_KV(KPC_(datum), KP_(hash_func), KP_(cmp_func));
private:
  const ObDatum *datum_;
  const ObExprHashFuncType *hash_func_;
  const ObDatumCmpFuncType *cmp_func_;
};

class ObWhiteFilterParamHashSet
{
public:
  ObWhiteFilterParamHashSet() : set_(), hash_func_(nullptr), cmp_func_(nullptr) {}
  ~ObWhiteFilterParamHashSet()
  {
    destroy();
  }
  void destroy()
  {
    if (set_.created()) {
      (void)set_.destroy();
    }
  }
  inline bool created() const
  {
    return set_.created();
  }
  inline int create(int param_num)
  {
    ObMemAttr attr(MTL_ID(), common::ObModIds::OB_HASH_BUCKET);
    return set_.create(param_num, attr);
  }
  inline int64_t count() const
  {
    return set_.size();
  }
  inline int set_refactored(const ObDatum &datum, int flag = 0 /*deduplicated*/)
  {
    ObWhiteFilterParam param(&datum, &hash_func_, &cmp_func_);
    return set_.set_refactored(param, flag);
  }
  inline int exist_refactored(const ObDatum &datum, bool &is_exist) const
  {
    ObWhiteFilterParam param(&datum, &hash_func_, &cmp_func_);
    int ret = set_.exist_refactored(param);
    if (OB_HASH_EXIST == ret) {
      is_exist = true;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "Failed to search in hashset", K(ret), K(param));
    }
    return ret;
  }
  inline void set_hash_and_cmp_func(const ObExprHashFuncType hash_func, const ObDatumCmpFuncType cmp_func)
  {
    hash_func_ = hash_func;
    cmp_func_ = cmp_func;
  }
  TO_STRING_KV(K_(set), K_(hash_func), K_(cmp_func));
private:
  common::hash::ObHashSet<ObWhiteFilterParam, common::hash::NoPthreadDefendMode> set_;
  ObExprHashFuncType hash_func_;
  ObDatumCmpFuncType cmp_func_;
};

class ObWhiteFilterExecutor : public ObPhysicalFilterExecutor
{
public:
  ObWhiteFilterExecutor(common::ObIAllocator &alloc,
                        ObPushdownWhiteFilterNode &filter,
                        ObPushdownOperator &op)
      : ObPhysicalFilterExecutor(alloc, op, PushdownExecutorType::WHITE_FILTER_EXECUTOR),
        cmp_func_(nullptr), cmp_func_rev_(nullptr), null_param_contained_(false), datum_params_(alloc), filter_(filter)
      {}
  ~ObWhiteFilterExecutor()
  {
    datum_params_.reset();
    param_set_.destroy();
  }

  OB_INLINE ObPushdownWhiteFilterNode &get_filter_node() { return filter_; }
  OB_INLINE const ObPushdownWhiteFilterNode &get_filter_node() const  { return filter_; }
  OB_INLINE virtual common::ObIArray<uint64_t> &get_col_ids() override
  { return filter_.get_col_ids(); }
  virtual const common::ObIArray<ObExpr *> *get_cg_col_exprs() const override { return &filter_.column_exprs_; }
  virtual int init_evaluated_datums() override;
  OB_INLINE const common::ObIArray<common::ObDatum> &get_datums() const
  { return datum_params_; }
  OB_INLINE const common::ObDatum &get_min_param() const
  { return datum_params_.at(0); }
  OB_INLINE const common::ObDatum &get_max_param() const
  { return datum_params_.at(datum_params_.count() - 1); }
  OB_INLINE bool null_param_contained() const { return null_param_contained_; }
  int exist_in_datum_set(const common::ObDatum &datum, bool &is_exist) const;
  int exist_in_datum_array(const common::ObDatum &datum, bool &is_exist, const int64_t offset = 0) const;
  OB_INLINE ObWhiteFilterOperatorType get_op_type() const
  { return filter_.get_op_type(); }
  bool is_cmp_op_with_null_ref_value() const;
  INHERIT_TO_STRING_KV("ObPushdownWhiteFilterExecutor", ObPushdownFilterExecutor,
                       K_(null_param_contained), K_(datum_params), K_(param_set),
                       K_(filter));
  virtual int filter(ObEvalCtx &eval_ctx, const sql::ObBitVector &skip_bit, bool &filtered) override;
  virtual void clear_in_datums()
  {
    if (WHITE_OP_IN == filter_.get_op_type()) {
      datum_params_.clear();
      param_set_.destroy();
    }
  }
protected:
  OB_INLINE bool is_null_param(const ObDatum &datum, const ObObjMeta obj_meta)
  {
    return datum.is_null() || (lib::is_oracle_mode() && obj_meta.is_character_type() && (0 == datum.len_));
  }
  int init_compare_eval_datums();
  int init_in_eval_datums();
  int init_param_set(const int64_t count, const ObExpr *cur_arg);
  int add_to_param_set_and_array(const ObDatum &datum, const ObExpr *cur_arg);
public:
  common::ObDatumCmpFuncType cmp_func_;
  common::ObDatumCmpFuncType cmp_func_rev_;
protected:
  bool null_param_contained_;
  common::ObFixedArray<common::ObDatum, common::ObIAllocator> datum_params_;
  ObWhiteFilterParamHashSet param_set_;
  ObPushdownWhiteFilterNode &filter_;
};

class ObAndFilterExecutor : public ObPushdownFilterExecutor
{
public:
  ObAndFilterExecutor(common::ObIAllocator &alloc,
                      ObPushdownAndFilterNode &filter,
                      ObPushdownOperator &op)
      : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::AND_FILTER_EXECUTOR),
      filter_(filter) {}
  OB_INLINE ObPushdownAndFilterNode &get_filter_node() { return filter_; }
  OB_INLINE virtual common::ObIArray<uint64_t> &get_col_ids() override
  { return filter_.get_col_ids(); }
  virtual int init_evaluated_datums() override;
  INHERIT_TO_STRING_KV("ObPushdownAndFilterExecutor", ObPushdownFilterExecutor, K_(filter));
private:
  ObPushdownAndFilterNode &filter_;
};

class ObOrFilterExecutor : public ObPushdownFilterExecutor
{
public:
  ObOrFilterExecutor(common::ObIAllocator &alloc,
                     ObPushdownOrFilterNode &filter,
                     ObPushdownOperator &op)
      : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::OR_FILTER_EXECUTOR),
      filter_(filter) {}

  OB_INLINE ObPushdownOrFilterNode &get_filter_node() { return filter_; }
  OB_INLINE virtual common::ObIArray<uint64_t> &get_col_ids() override
  { return filter_.get_col_ids(); }
  virtual int init_evaluated_datums() override;
  INHERIT_TO_STRING_KV("ObPushdownOrFilterExecutor", ObPushdownFilterExecutor, K_(filter));
private:
  ObPushdownOrFilterNode &filter_;
};

class ObDynamicFilterExecutor : public ObWhiteFilterExecutor
{
public:
  static const uint32_t DEFAULT_CHECK_INTERVAL = 16;
  ObDynamicFilterExecutor(common::ObIAllocator &alloc,
                          ObPushdownDynamicFilterNode &filter,
                          ObPushdownOperator &op)
      : ObWhiteFilterExecutor(alloc, filter, op),
        is_data_prepared_(false),
        batch_cnt_(0),
        runtime_filter_ctx_(nullptr),
        is_first_check_(true),
        build_obj_type_(ObNullType),
        filter_action_(DO_FILTER),
        stored_data_version_(0)
  {}
  OB_INLINE ObPushdownDynamicFilterNode &get_filter_node()
  {
    return static_cast<ObPushdownDynamicFilterNode &>(filter_);
  }
  OB_INLINE const ObPushdownDynamicFilterNode &get_filter_node() const
  {
    return static_cast<const ObPushdownDynamicFilterNode &>(filter_);
  }
  virtual int init_evaluated_datums() override;
  int check_runtime_filter(ObPushdownFilterExecutor* parent_filter, bool &is_needed);
  void filter_on_bypass(ObPushdownFilterExecutor* parent_filter);
  void filter_on_success(ObPushdownFilterExecutor* parent_filter);
  int64_t get_col_idx() const
  {
    return static_cast<const ObPushdownDynamicFilterNode *>(&filter_)->get_col_idx();
  }
  void locate_runtime_filter_ctx();
  inline void set_filter_val_meta(const ObObjMeta &val_meta_)
  {
    return static_cast<ObPushdownDynamicFilterNode &>(filter_).set_filter_val_meta(val_meta_);
  }
  inline ObObjMeta get_filter_val_meta() const
  {
    return static_cast<ObPushdownDynamicFilterNode &>(filter_).get_filter_val_meta();
  }
  inline void set_filter_action(DynamicFilterAction value) {filter_action_ = value; }
  inline DynamicFilterAction get_filter_action() const { return filter_action_; }
  inline bool is_filter_all_data() { return DynamicFilterAction::FILTER_ALL == filter_action_; }
  inline bool is_pass_all_data() { return DynamicFilterAction::PASS_ALL == filter_action_; }
  inline bool is_check_all_data() { return DynamicFilterAction::DO_FILTER == filter_action_; }
  inline bool is_data_prepared() const { return is_data_prepared_; }
  inline void set_stored_data_version(int64_t data_version)
  {
    stored_data_version_ = data_version;
  };
  OB_INLINE bool filter_can_continuous_filter() const override final
  {
    // for topn sort runtime filter, the filter can not do continuouly check
    return DynamicFilterType::PD_TOPN_FILTER != get_filter_node().get_dynamic_filter_type();
  }
  INHERIT_TO_STRING_KV("ObDynamicFilterExecutor", ObWhiteFilterExecutor, K_(is_data_prepared),
                       K_(batch_cnt), KP_(runtime_filter_ctx));
public:
  using ObRuntimeFilterParams = common::ObSEArray<common::ObDatum, 4>;
  typedef int (*PreparePushdownDataFunc) (const ObExpr &expr,
                                 ObDynamicFilterExecutor &dynamic_filter,
                                 ObEvalCtx &eval_ctx,
                                 ObRuntimeFilterParams &params,
                                 bool &is_data_prepared);
  typedef int (*UpdatePushdownDataFunc) (const ObExpr &expr,
                                 ObDynamicFilterExecutor &dynamic_filter,
                                 ObEvalCtx &eval_ctx,
                                 ObRuntimeFilterParams &params,
                                 bool &is_update);
  static PreparePushdownDataFunc PREPARE_PD_DATA_FUNCS[DynamicFilterType::MAX_DYNAMIC_FILTER_TYPE];
  static UpdatePushdownDataFunc UPDATE_PD_DATA_FUNCS[DynamicFilterType::MAX_DYNAMIC_FILTER_TYPE];

private:
  int try_preparing_data();
  int try_updating_data();
  inline bool is_data_version_updated();
private:
  bool is_data_prepared_;
  int64_t batch_cnt_;
  ObExprOperatorCtx *runtime_filter_ctx_;
  bool is_first_check_;
  ObObjType build_obj_type_; // for runtime filter, the datum_params_ are from the build table
  DynamicFilterAction filter_action_;
  // for topn runtime filter, we need continuosly update the dynamic filter data
  // stored_data_version_ means the data version now the dynamic filter use
  int64_t stored_data_version_;
};

class ObFilterExecutorConstructor
{
public:
  ObFilterExecutorConstructor(common::ObIAllocator *alloc)
      : factory_(alloc)
  {}
  int apply(ObPushdownFilterNode *filter_tree, ObPushdownFilterExecutor *&filter_executor, ObPushdownOperator &op);

private:
  template<typename CLASST, PushdownExecutorType type>
      int create_filter_executor(ObPushdownFilterNode *filter_tree,
                                 ObPushdownFilterExecutor *&filter_executor,
                                 ObPushdownOperator &op);
private:
  ObPushdownFilterFactory factory_;
};

class ObPushdownExprSpec
{
  OB_UNIS_VERSION(1);
public:
  ObPushdownExprSpec(common::ObIAllocator &alloc);
  ~ObPushdownExprSpec() = default;
  TO_STRING_KV(K_(calc_exprs),
               K_(access_exprs),
               K_(ext_file_column_exprs),
               K_(ext_column_convert_exprs),
               K_(max_batch_size),
               K_(pushdown_filters),
               K_(pd_storage_flag),
               KPC_(trans_info_expr));

  int set_calc_exprs(const ExprFixedArray &calc_exprs, int64_t max_batch_size)
  {
    max_batch_size_ = max_batch_size;
    return calc_exprs_.assign(calc_exprs);
  }
public:
  ExprFixedArray calc_exprs_; //所有需要下压到存储层的表达式
  ExprFixedArray access_exprs_;
  int64_t max_batch_size_;

  // filters push down to storage.
  ExprFixedArray pushdown_filters_;

  ObStoragePushdownFlag pd_storage_flag_;
  ObPushdownFilter pd_storage_filters_;
  // used to pushdown aggregate expression now.
  ExprFixedArray pd_storage_aggregate_output_;
  // used by external table
  ExprFixedArray ext_file_column_exprs_;
  ExprFixedArray ext_column_convert_exprs_;
  ObExpr *trans_info_expr_;
  uint64_t auto_split_filter_type_;
  ObExpr *auto_split_expr_;
  ExprFixedArray auto_split_params_;
};

//下压到存储层的表达式执行依赖的op ctx
class ObPushdownOperator
{
public:
  ObPushdownOperator(
      ObEvalCtx &eval_ctx,
      const ObPushdownExprSpec &expr_spec,
      const bool use_rich_format = false);
  ~ObPushdownOperator() = default;

  int init_pushdown_storage_filter();
  OB_INLINE ObEvalCtx &get_eval_ctx() { return eval_ctx_; }
  OB_INLINE bool is_vectorized() const { return 0 != expr_spec_.max_batch_size_; }
  OB_INLINE int64_t get_batch_size() const { return expr_spec_.max_batch_size_; }
  // filter row for storage callback.
  // clear expression evaluated flag if row filtered.
  OB_INLINE int filter_row_outside(const ObExprPtrIArray &exprs, const sql::ObBitVector &skip_bit, bool &filtered);
  // Notice:
  // clear one/current datum eval flag at a time, do NOT call it
  // unless fully understand this API.
  int clear_datum_eval_flag();
  // clear eval flag of all datums within a batch
  int clear_evaluated_flag();
  int deep_copy(const sql::ObExprPtrIArray *exprs, const int64_t batch_idx);
  int reset_trans_info_datum();
  int write_trans_info_datum(blocksstable::ObDatumRow &out_row);
public:
  ObPushdownFilterExecutor *pd_storage_filters_;
  ObEvalCtx &eval_ctx_;
  const ObPushdownExprSpec &expr_spec_;
  // enable new vec2.0 format
  const bool enable_rich_format_;
};

// filter row for storage callback.
OB_INLINE int ObPushdownOperator::filter_row_outside(const ObExprPtrIArray &exprs, const sql::ObBitVector &skip_bit, bool &filtered)
{
  int ret = common::OB_SUCCESS;
  if (!enable_rich_format_) {
    ret = ObOperator::filter_row(eval_ctx_, exprs, filtered);
  } else {
    ret = ObOperator::filter_row_vector(eval_ctx_, exprs, skip_bit, filtered);
  }
  // always clear evaluated flag, because filter expr and table scan output expr may have
  // common expr, when eval filter expr, memory of dependence column may from storage,
  // if not filter and we don't clear eval flag, output expr will used the result datum
  // of filter expr which memory may expired, so we need clear eval flag after eval filter expr,
  // and the common expr in table scan output need evaluate again,
  // now the memory of dependence column have been deep copy
  if (OB_SUCC(ret)) {
    clear_datum_eval_flag();
  }
  return ret;
}

OB_INLINE bool ObBlackFilterExecutor::can_vectorized()
{ return op_.is_vectorized(); }

OB_INLINE bool is_row_filtered(const common::ObDatum &d)
{
  return (d.is_null() || 0 == d.get_int());
}

struct PushdownFilterInfo
{
  PushdownFilterInfo() :
      is_inited_(false),
      is_pd_filter_(false),
      is_pd_to_cg_(false),
      start_(-1),
      count_(-1),
      col_capacity_(0),
      batch_size_(0),
      datum_buf_(nullptr),
      filter_(nullptr),
      cell_data_ptrs_(nullptr),
      row_ids_(nullptr),
      len_array_(nullptr),
      ref_bitmap_(nullptr),
      skip_bit_(nullptr),
      col_datum_buf_(),
      allocator_(nullptr),
      param_(nullptr),
      context_(nullptr)
  {}
  ~PushdownFilterInfo();
  void reset();
  void reuse();
  OB_INLINE bool is_valid()
  {
    bool ret = is_inited_;
    if (is_pd_filter_ && nullptr != filter_ && !filter_->is_sample_node()) {
      ret = ret && (nullptr != datum_buf_);
    }
    if (0 < batch_size_) {
      ret = ret && (nullptr != cell_data_ptrs_) && (nullptr != row_ids_)
          && (nullptr != skip_bit_);
    }
    return ret;
  }
  int init(const storage::ObTableIterParam &iter_param, common::ObIAllocator &alloc);
  int init_bitmap(const int64_t row_count, common::ObBitmap *&bitmap);
  int get_col_datum(ObDatum *&datums) const;
  struct TmpColDatumBuf
  {
    TmpColDatumBuf() : datums_(nullptr), data_buf_(nullptr), batch_size_(0), allocator_(nullptr) {}
    ~TmpColDatumBuf() { reset(); }
    void reset();
    int init(const int64_t batch_size, common::ObIAllocator &allocator);
    int get_col_datum(const int64_t batch_size, ObDatum *&datums);
    TO_STRING_KV(KP_(datums), KP_(data_buf), K_(batch_size), KP_(allocator));
    ObDatum *datums_;
    char *data_buf_;
    int64_t batch_size_;
    common::ObIAllocator *allocator_;
  };

  TO_STRING_KV(K_(is_pd_filter), K_(is_pd_to_cg), K_(start), K_(count), K_(col_capacity), K_(batch_size),
               KP_(datum_buf), KP_(filter), KP_(cell_data_ptrs), KP_(row_ids), KP_(ref_bitmap),
               K_(col_datum_buf), KP_(param), KP_(context), KP_(skip_bit));

  bool is_inited_;
  bool is_pd_filter_;
  bool is_pd_to_cg_;
  int64_t start_; // inclusive
  int64_t count_;
  int64_t col_capacity_;
  int64_t batch_size_;
  blocksstable::ObStorageDatum *datum_buf_;
  sql::ObPushdownFilterExecutor *filter_;
  // for black filter vectorize
  const char **cell_data_ptrs_;
  int32_t *row_ids_;
  uint32_t *len_array_;
  common::ObBitmap *ref_bitmap_;
  sql::ObBitVector *skip_bit_;
  mutable TmpColDatumBuf col_datum_buf_;
  common::ObIAllocator *allocator_;
  const storage::ObTableIterParam *param_;
  storage::ObTableAccessContext *context_;
};

}
}

#endif // OB_SQL_ENGINE_BASIC_OB_PUSHDOWN_FILTER_H_
