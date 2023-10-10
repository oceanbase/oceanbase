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
#include "lib/hash/ob_hashset.h"
#include "common/object/ob_obj_compare.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
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
};
namespace sql
{
class ObRawExpr;
class ObStaticEngineCG;
class ObPushdownOperator;
struct ObExprFrameInfo;
typedef common::ObFixedArray<const share::schema::ObColumnParam*, common::ObIAllocator> ColumnParamFixedArray;

enum PushdownFilterType
{
  BLACK_FILTER,
  WHITE_FILTER,
  AND_FILTER,
  OR_FILTER,
  MAX_FILTER_TYPE
};

enum PushdownExecutorType
{
  BLACK_FILTER_EXECUTOR,
  WHITE_FILTER_EXECUTOR,
  AND_FILTER_EXECUTOR,
  OR_FILTER_EXECUTOR,
  MAX_EXECUTOR_TYPE
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

  OB_INLINE static void set_blockscan_pushdown_storage(int32_t &pd_storage_flag)
  { pd_storage_flag |= 0x01; }
  OB_INLINE static void set_filter_pushdown_storage(int32_t &pd_storage_flag)
  { pd_storage_flag |= 0x02; }
  OB_INLINE static void set_aggregate_pushdown_storage(int32_t &pd_storage_flag)
  { pd_storage_flag |= 0x04; }
  OB_INLINE static bool is_blockscan_pushdown_storage(int32_t pd_storage_flag)
  { return pd_storage_flag & 0x01; }
  OB_INLINE static bool is_filter_pushdown_storage(int32_t pd_storage_flag)
  { return pd_storage_flag & 0x02; }
  OB_INLINE static bool is_aggregate_pushdown_storage(int32_t pd_storage_flag)
  { return pd_storage_flag & 0x04; }
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
      : ObPushdownFilterNode(alloc, PushdownFilterType::AND_FILTER)
  {}
};

class ObPushdownOrFilterNode : public ObPushdownFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownOrFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::OR_FILTER)
  {}
};

class ObPushdownBlackFilterNode : public ObPushdownFilterNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPushdownBlackFilterNode(common::ObIAllocator &alloc)
      : ObPushdownFilterNode(alloc, PushdownFilterType::BLACK_FILTER),
      column_exprs_(alloc),
      filter_exprs_(alloc),
      tmp_expr_(nullptr)
  {}
  ~ObPushdownBlackFilterNode() {}

  int merge(common::ObIArray<ObPushdownFilterNode*> &merged_node) override;
  virtual int postprocess() override;
  INHERIT_TO_STRING_KV("ObPushdownBlackFilterNode", ObPushdownFilterNode,
                       K_(column_exprs), K_(filter_exprs));

private:
  int64_t get_filter_expr_count()
  { return filter_exprs_.empty() ? 1 : filter_exprs_.count(); }
public:
  ExprFixedArray column_exprs_; // 列对应的表达式
  ExprFixedArray filter_exprs_; // 下压filters
  // 下压临时保存的filter，如果发生merge，则所有的filter放入filter_exprs_
  // 如果没有发生merge，则将自己的tmp_expr_放入filter_exprs_中
  ObExpr *tmp_expr_;
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
      expr_(nullptr), op_type_(WHITE_OP_MAX)
  {}
  ~ObPushdownWhiteFilterNode() {}
  OB_INLINE int set_op_type(const ObItemType &type);
  OB_INLINE ObWhiteFilterOperatorType get_op_type() const { return op_type_; }

  // mapping array from white filter's operation type to common::ObCmpOp
  static const common::ObCmpOp WHITE_OP_TO_CMP_OP[WHITE_OP_MAX];
  INHERIT_TO_STRING_KV("ObPushdownWhiteFilterNode", ObPushdownFilterNode,
                       KPC_(expr), K_(op_type));
public:
  ObExpr *expr_;
private:
  ObWhiteFilterOperatorType op_type_;
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
  ObPushdownFilterConstructor(common::ObIAllocator *alloc, ObStaticEngineCG &static_cg)
      : alloc_(alloc), factory_(alloc), static_cg_(static_cg)
  {}
  int apply(common::ObIArray<ObRawExpr*> &exprs, ObPushdownFilterNode *&filter_tree);

private:
  int is_white_mode(const ObRawExpr* raw_expr, bool &is_white);
  int create_black_filter_node(ObRawExpr *raw_expr, ObPushdownFilterNode *&filter_tree);
  int create_white_filter_node(ObRawExpr *raw_expr, ObPushdownFilterNode *&filter_tree);
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
private:
  common::ObIAllocator *alloc_;
  ObPushdownFilterFactory factory_;
  ObStaticEngineCG &static_cg_;
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
private:
  common::ObIAllocator &alloc_;
  ObPushdownFilterNode *filter_tree_;
};

// executor interface
// 类似新框架的operator，而ObPushdownFilterNode则对应的是ObOpSpec接口
// 即一个是编译器的接口，一个是运行时接口
class ObPushdownFilterExecutor
{
public:
  ObPushdownFilterExecutor(common::ObIAllocator &alloc,
                           ObPushdownOperator &op,
                           PushdownExecutorType type = PushdownExecutorType::MAX_EXECUTOR_TYPE);
  virtual ~ObPushdownFilterExecutor();

  // interface for storage
  virtual OB_INLINE bool is_filter_black_node() const { return type_ == BLACK_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_filter_white_node() const { return type_ == WHITE_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_filter_node() const { return is_filter_black_node() || is_filter_white_node(); }
  virtual OB_INLINE bool is_logic_and_node() const { return type_ == AND_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_logic_or_node() const { return type_ == OR_FILTER_EXECUTOR; }
  virtual OB_INLINE bool is_logic_op_node() const { return is_logic_and_node() || is_logic_or_node(); }
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
    return fast_skip;
  }
  void set_type(PushdownExecutorType type) { type_ = type; }
  OB_INLINE PushdownExecutorType get_type() { return type_; }
  virtual common::ObIArray<uint64_t> &get_col_ids() = 0;
  OB_INLINE int64_t get_col_count() const { return n_cols_; }
  OB_INLINE const common::ObIArray<int32_t> &get_col_offsets() const { return col_offsets_; }
  OB_INLINE const ColumnParamFixedArray &get_col_params() const { return col_params_; }
  OB_INLINE const common::ObFixedArray<blocksstable::ObStorageDatum, common::ObIAllocator> &get_default_datums() const
  { return default_datums_; }
  OB_INLINE uint32_t get_child_count() const { return n_child_; }
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
  virtual int init_evaluated_datums() { return common::OB_NOT_SUPPORTED; }
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
protected:
  PushdownExecutorType type_;
  bool need_check_row_filter_;
  int64_t n_cols_;
  uint32_t n_child_;
  ObPushdownFilterExecutor **childs_;
  common::ObBitmap *filter_bitmap_;
  ColumnParamFixedArray col_params_;
  common::ObFixedArray<int32_t, common::ObIAllocator> col_offsets_;
  common::ObFixedArray<blocksstable::ObStorageDatum, common::ObIAllocator> default_datums_;
  common::ObIAllocator &allocator_;
  ObPushdownOperator &op_;
};

class ObBlackFilterExecutor : public ObPushdownFilterExecutor
{
public:
  ObBlackFilterExecutor(common::ObIAllocator &alloc,
                        ObPushdownBlackFilterNode &filter,
                        ObPushdownOperator &op)
      : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::BLACK_FILTER_EXECUTOR),
      filter_(filter), n_eval_infos_(0), eval_infos_(nullptr),
      n_datum_eval_flags_(0), datum_eval_flags_(NULL), skip_bit_(NULL)
  {}
  ~ObBlackFilterExecutor();

  OB_INLINE ObPushdownBlackFilterNode &get_filter_node() { return filter_; }
  OB_INLINE virtual common::ObIArray<uint64_t> &get_col_ids() override
  { return filter_.get_col_ids(); }
  int filter(common::ObObj *objs, int64_t col_cnt, bool &ret_val);
  int filter(blocksstable::ObStorageDatum *datums, int64_t col_cnt, bool &ret_val);
  virtual int init_evaluated_datums() override;
  OB_INLINE bool can_vectorized();
  int filter_batch(ObPushdownFilterExecutor *parent,
                   const int64_t start,
                   const int64_t end,
                   common::ObBitmap &result_bitmap);
  int get_datums_from_column(common::ObIArray<common::ObDatum *> &datums);
  INHERIT_TO_STRING_KV("ObPushdownBlackFilterExecutor", ObPushdownFilterExecutor,
                       K_(filter), K_(n_eval_infos),
                       KP_(eval_infos), KP_(skip_bit));
private:
  int filter(ObEvalCtx &eval_ctx, bool &filtered);
  int eval_exprs_batch(ObBitVector &skip, const int64_t bsize);
  int init_eval_param(const int32_t cur_eval_info_cnt, const int64_t eval_expr_cnt);
  OB_INLINE void clear_evaluated_datums();
  OB_INLINE void clear_evaluated_infos();

private:
  ObPushdownBlackFilterNode &filter_;
  int32_t n_eval_infos_;
  ObEvalInfo **eval_infos_;
  int32_t n_datum_eval_flags_;
  ObBitVector **datum_eval_flags_;
  ObBitVector *skip_bit_;
};

class ObWhiteFilterExecutor : public ObPushdownFilterExecutor
{
public:
  ObWhiteFilterExecutor(common::ObIAllocator &alloc,
                        ObPushdownWhiteFilterNode &filter,
                        ObPushdownOperator &op)
      : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::WHITE_FILTER_EXECUTOR),
      null_param_contained_(false), params_(alloc), filter_(filter) {}
  ~ObWhiteFilterExecutor()
  {
    params_.reset();
    if (param_set_.created()) {
      (void)param_set_.destroy();
    }
  }

  OB_INLINE ObPushdownWhiteFilterNode &get_filter_node() { return filter_; }
  OB_INLINE virtual common::ObIArray<uint64_t> &get_col_ids() override
  { return filter_.get_col_ids(); }
  virtual int init_evaluated_datums() override;
  OB_INLINE const common::ObIArray<common::ObObj> &get_objs() const
  { return params_; }
  OB_INLINE bool null_param_contained() const { return null_param_contained_; }
  int exist_in_obj_set(const common::ObObj &obj, bool &is_exist) const;
  bool is_obj_set_created() const { return param_set_.created(); };
  OB_INLINE ObWhiteFilterOperatorType get_op_type() const
  { return filter_.get_op_type(); }
  INHERIT_TO_STRING_KV("ObPushdownWhiteFilterExecutor", ObPushdownFilterExecutor,
                       K_(null_param_contained), K_(params), K(param_set_.created()),
                       K_(filter));
private:
  void check_null_params();
  int init_obj_set();
private:
  bool null_param_contained_;
  common::ObFixedArray<common::ObObj, common::ObIAllocator> params_;
  common::hash::ObHashSet<common::ObObj> param_set_;
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

  int32_t pd_storage_flag_;
  ObPushdownFilter pd_storage_filters_;
  // used to pushdown aggregate expression now.
  ExprFixedArray pd_storage_aggregate_output_;
  // used by external table
  ExprFixedArray ext_file_column_exprs_;
  ExprFixedArray ext_column_convert_exprs_;
  ObExpr *trans_info_expr_;
};

//下压到存储层的表达式执行依赖的op ctx
class ObPushdownOperator
{
public:
  ObPushdownOperator(ObEvalCtx &eval_ctx, const ObPushdownExprSpec &expr_spec);
  ~ObPushdownOperator() = default;

  int init_pushdown_storage_filter();
  OB_INLINE ObEvalCtx &get_eval_ctx() { return eval_ctx_; }
  OB_INLINE bool is_vectorized() const { return 0 != expr_spec_.max_batch_size_; }
  OB_INLINE int64_t get_batch_size() const { return expr_spec_.max_batch_size_; }
  // filter row for storage callback.
  // clear expression evaluated flag if row filtered.
  OB_INLINE int filter_row_outside(const ObExprPtrIArray &exprs, bool &filtered);
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
  // The datum of the trans_info expression that records transaction information
};

// filter row for storage callback.
OB_INLINE int ObPushdownOperator::filter_row_outside(const ObExprPtrIArray &exprs, bool &filtered)
{
  int ret = common::OB_SUCCESS;
  ret = ObOperator::filter_row(eval_ctx_, exprs, filtered);
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

OB_INLINE void ObBlackFilterExecutor::clear_evaluated_datums()
{
  for (int i = 0; i < n_datum_eval_flags_; i++) {
    datum_eval_flags_[i]->unset(op_.get_eval_ctx().get_batch_idx());
  }
}

OB_INLINE void ObBlackFilterExecutor::clear_evaluated_infos()
{
  for (int i = 0; i < n_eval_infos_; i++) {
    eval_infos_[i]->clear_evaluated_flag();
  }
}

OB_INLINE bool is_row_filtered(const common::ObDatum &d)
{
  return (d.is_null() || 0 == d.get_int());
}
}
}

#endif // OB_SQL_ENGINE_BASIC_OB_PUSHDOWN_FILTER_H_
