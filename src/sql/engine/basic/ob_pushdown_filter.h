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
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_operator.h"
#include "share/datum/ob_datum.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObColumnParam;
class ColumnMap;
}  // namespace schema
}  // namespace share
namespace storage {
struct ObTableIterParam;
struct ObTableAccessContext;
}  // namespace storage

namespace sql {

class ObStaticEngineCG;

enum PushdownFilterType { BLACK_FILTER, WHITE_FILTER, AND_FILTER, OR_FILTER, MAX_FILTER_TYPE };

enum FilterExecutorType {
  BLACK_FILTER_EXECUTOR,
  WHITE_FILTER_EXECUTOR,
  AND_FILTER_EXECUTOR,
  OR_FILTER_EXECUTOR,
  MAX_EXECUTOR_TYPE
};

class ObPushdownFilterUtils {
public:
  static bool is_pushdown_storage(int32_t pd_storage_flag)
  {
    return pd_storage_flag & 0x01;
  }
  static bool is_pushdown_storage_index_back(int32_t pd_storage_flag)
  {
    return pd_storage_flag & 0x02;
  }
};

class ObPushdownFilterNode {
  OB_UNIS_VERSION_V(1);

public:
  ObPushdownFilterNode(common::ObIAllocator& alloc)
      : alloc_(alloc), type_(PushdownFilterType::MAX_FILTER_TYPE), n_child_(0), childs_(nullptr), col_ids_(alloc)
  {}

  PushdownFilterType get_type() const
  {
    return type_;
  }
  common::ObIArray<uint64_t>& get_col_ids()
  {
    return col_ids_;
  }
  void set_type(PushdownFilterType type)
  {
    type_ = type;
  }

  virtual int merge(common::ObIArray<ObPushdownFilterNode*>& merged_node)
  {
    UNUSED(merged_node);
    return common::OB_SUCCESS;
  }
  virtual int postprocess()
  {
    return common::OB_SUCCESS;
  }
  TO_STRING_KV(K_(type), K_(n_child), K_(col_ids));

public:
  common::ObIAllocator& alloc_;
  PushdownFilterType type_;
  uint32_t n_child_;
  ObPushdownFilterNode** childs_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> col_ids_;  // cols this node is associated with
};

class ObPushdownAndFilterNode : public ObPushdownFilterNode {
  OB_UNIS_VERSION_V(1);

public:
  ObPushdownAndFilterNode(common::ObIAllocator& alloc) : ObPushdownFilterNode(alloc)
  {}
};

class ObPushdownOrFilterNode : public ObPushdownFilterNode {
  OB_UNIS_VERSION_V(1);

public:
  ObPushdownOrFilterNode(common::ObIAllocator& alloc) : ObPushdownFilterNode(alloc)
  {}
};

class ObPushdownBlackFilterNode : public ObPushdownFilterNode {
  OB_UNIS_VERSION_V(1);

public:
  ObPushdownBlackFilterNode(common::ObIAllocator& alloc)
      : ObPushdownFilterNode(alloc), column_exprs_(alloc), filter_exprs_(alloc), tmp_expr_(nullptr)
  {}
  ~ObPushdownBlackFilterNode()
  {}

  int merge(common::ObIArray<ObPushdownFilterNode*>& merged_node) override;
  virtual int postprocess() override;
  OB_INLINE void clear_evaluated_datums();

public:
  ExprFixedArray column_exprs_;
  ExprFixedArray filter_exprs_;
  ObExpr* tmp_expr_;
};

class ObPushdownWhiteFilterNode : public ObPushdownFilterNode {
  OB_UNIS_VERSION_V(1);

public:
  ObPushdownWhiteFilterNode(common::ObIAllocator& alloc) : ObPushdownFilterNode(alloc)
  {}
  ~ObPushdownWhiteFilterNode()
  {}
};

class ObPushdownFilterExecutor;
class ObPushdownFilterNode;
class ObPushdownFilterFactory {
public:
  ObPushdownFilterFactory(common::ObIAllocator* alloc) : alloc_(alloc)
  {}
  int alloc(PushdownFilterType type, uint32_t n_child, ObPushdownFilterNode*& pd_filter);
  int alloc(FilterExecutorType type, uint32_t n_child, ObPushdownFilterNode& filter_node,
      ObPushdownFilterExecutor*& filter_executor);

private:
  // pushdown filter
  typedef int (*PDFilterAllocFunc)(common::ObIAllocator& alloc, uint32_t n_child, ObPushdownFilterNode*& filter_node);
  template <typename ClassT, PushdownFilterType type>
  static int alloc(common::ObIAllocator& alloc, uint32_t n_child, ObPushdownFilterNode*& filter_executor);
  static PDFilterAllocFunc PD_FILTER_ALLOC[PushdownFilterType::MAX_FILTER_TYPE];

  // filter executor
  typedef int (*FilterExecutorAllocFunc)(common::ObIAllocator& alloc, uint32_t n_child,
      ObPushdownFilterNode& filter_node, ObPushdownFilterExecutor*& filter_executor);
  template <typename ClassT, typename FilterNodeT, FilterExecutorType type>
  static int alloc(common::ObIAllocator& alloc, uint32_t n_child, ObPushdownFilterNode& filter_node,
      ObPushdownFilterExecutor*& filter_executor);
  static FilterExecutorAllocFunc FILTER_EXECUTOR_ALLOC[FilterExecutorType::MAX_EXECUTOR_TYPE];

private:
  common::ObIAllocator* alloc_;
};

class ObPushdownFilterConstructor {
public:
  ObPushdownFilterConstructor(common::ObIAllocator* alloc, ObStaticEngineCG& static_cg)
      : alloc_(alloc), factory_(alloc), static_cg_(static_cg)
  {}
  int apply(ObRawExpr* raw_expr, ObPushdownFilterNode*& filter_tree);
  int apply(common::ObIArray<ObRawExpr*>& exprs, ObPushdownFilterNode*& filter_tree);

private:
  int merge_filter_node(ObPushdownFilterNode* dst, ObPushdownFilterNode* other,
      common::ObIArray<ObPushdownFilterNode*>& merged_node, bool& merged);
  int deduplicate_filter_node(common::ObIArray<ObPushdownFilterNode*>& filter_nodes, uint32_t& n_node);
  int create_black_filter_node(ObRawExpr* raw_expr, ObPushdownFilterNode*& filter_tree);
  bool can_split_or(ObRawExpr* raw_expr)
  {
    UNUSED(raw_expr);
    return false;
  }
  bool is_white_mode(ObRawExpr* raw_expr)
  {
    UNUSED(raw_expr);
    return false;
  }

private:
  common::ObIAllocator* alloc_;
  ObPushdownFilterFactory factory_;
  ObStaticEngineCG& static_cg_;
};

// wrapper for PushdownFilterNode to support serilize
class ObPushdownFilter {
  OB_UNIS_VERSION_V(1);

public:
  ObPushdownFilter(common::ObIAllocator& alloc) : alloc_(alloc), filter_tree_(nullptr)
  {}

  void set_filter_tree(ObPushdownFilterNode* filter_tree)
  {
    filter_tree_ = filter_tree;
  }
  ObPushdownFilterNode*& get_pushdown_filter()
  {
    return filter_tree_;
  }
  ObPushdownFilterNode* get_pushdown_filter() const
  {
    return filter_tree_;
  }
  static int serialize_pushdown_filter(
      char* buf, int64_t buf_len, int64_t& pos, ObPushdownFilterNode* pd_storage_filter);
  static int deserialize_pushdown_filter(ObPushdownFilterFactory filter_factory, const char* buf, int64_t data_len,
      int64_t& pos, ObPushdownFilterNode*& pd_storage_filter);
  static int64_t get_serialize_pushdown_filter_size(ObPushdownFilterNode* pd_filter_node);
  // NEED_SERIALIZE_AND_DESERIALIZE;
private:
  common::ObIAllocator& alloc_;
  ObPushdownFilterNode* filter_tree_;
};

// executor interface
class ObPushdownFilterExecutor {
public:
  ObPushdownFilterExecutor(common::ObIAllocator& alloc, ObPushdownFilterNode& filter)
      : type_(FilterExecutorType::MAX_EXECUTOR_TYPE),
        filter_(filter),
        n_cols_(0),
        col_offsets_(nullptr),
        n_child_(0),
        childs_(nullptr),
        filter_bitmap_(nullptr),
        col_params_(nullptr),
        allocator_(alloc)
  {}
  ~ObPushdownFilterExecutor()
  {
    if (nullptr != filter_bitmap_) {
      filter_bitmap_->~ObBitmap();
      allocator_.free(filter_bitmap_);
    }
    filter_bitmap_ = nullptr;
    reset_filter_param();
    for (uint32_t i = 0; i < n_child_; i++) {
      if (OB_NOT_NULL(childs_[i])) {
        childs_[i]->~ObPushdownFilterExecutor();
        childs_[i] = nullptr;
      }
    }
    n_child_ = 0;
  }
  void reset_filter_param()
  {
    if (nullptr != col_offsets_) {
      allocator_.free(col_offsets_);
    }
    if (nullptr != col_params_) {
      allocator_.free(col_params_);
    }
    col_offsets_ = nullptr;
    col_params_ = nullptr;
  }

  // interface for storage
  virtual OB_INLINE bool is_filter_black_node() const
  {
    return type_ == BLACK_FILTER_EXECUTOR;
  }
  virtual OB_INLINE bool is_filter_white_node() const
  {
    return type_ == WHITE_FILTER_EXECUTOR;
  }
  virtual OB_INLINE bool is_filter_node() const
  {
    return is_filter_black_node() || is_filter_white_node();
  }
  virtual OB_INLINE bool is_logic_and_node() const
  {
    return type_ == AND_FILTER_EXECUTOR;
  }
  virtual OB_INLINE bool is_logic_or_node() const
  {
    return type_ == OR_FILTER_EXECUTOR;
  }
  virtual OB_INLINE bool is_logic_op_node() const
  {
    return is_logic_and_node() || is_logic_or_node();
  }
  void set_type(FilterExecutorType type)
  {
    type_ = type;
  }
  OB_INLINE FilterExecutorType get_type()
  {
    return type_;
  }
  OB_INLINE ObPushdownFilterNode& get_filter_node()
  {
    return filter_;
  }
  OB_INLINE common::ObIArray<uint64_t>& get_col_ids()
  {
    return filter_.get_col_ids();
  }
  OB_INLINE int64_t get_col_count() const
  {
    return n_cols_;
  }
  OB_INLINE const int32_t* get_col_offsets() const
  {
    return col_offsets_;
  }
  OB_INLINE const share::schema::ObColumnParam** get_col_params() const
  {
    return col_params_;
  }
  OB_INLINE uint32_t get_child_count() const
  {
    return n_child_;
  }
  inline int get_child(uint32_t nth_child, ObPushdownFilterExecutor*& filter_executor)
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
  void set_child(uint32_t i, ObPushdownFilterExecutor* child)
  {
    childs_[i] = child;
  }
  void set_childs(uint32_t n_child, ObPushdownFilterExecutor** childs)
  {
    n_child_ = n_child;
    childs_ = childs;
  }
  ObPushdownFilterExecutor** get_childs() const
  {
    return childs_;
  }
  const common::ObBitmap* get_result() const
  {
    return filter_bitmap_;
  }
  // for testing purpose
  virtual int filter(bool& filtered)
  {
    UNUSED(filtered);
    return common::OB_NOT_SUPPORTED;
  }
  int init_bitmap(const int64_t row_count, common::ObBitmap*& bitmap);
  int init_filter_param(const share::schema::ColumnMap* col_id_map,
      const common::ObIArray<share::schema::ObColumnParam*>* col_params, const bool need_padding);
  virtual int init_evaluated_datums(
      common::ObIAllocator& alloc, const common::ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx)
  {
    UNUSED(alloc);
    UNUSED(calc_exprs);
    UNUSED(eval_ctx);
    return common::OB_NOT_SUPPORTED;
  }
  VIRTUAL_TO_STRING_KV(K_(type), K_(n_cols), "col_offsets", common::ObArrayWrap<int32_t>(col_offsets_, n_cols_),
      K_(n_child), KP_(childs), KP_(filter_bitmap), KP_(col_params));

protected:
  int find_evaluated_datums(
      ObExpr* expr, const common::ObIArray<ObExpr*>& calc_exprs, common::ObIArray<ObExpr*>& eval_exprs);
  int find_evaluated_datums(common::ObIArray<ObExpr*>& src_exprs, const common::ObIArray<ObExpr*>& calc_exprs,
      common::ObIArray<ObExpr*>& eval_exprs);

protected:
  FilterExecutorType type_;
  ObPushdownFilterNode& filter_;
  int64_t n_cols_;
  int32_t* col_offsets_;
  uint32_t n_child_;
  ObPushdownFilterExecutor** childs_;
  common::ObBitmap* filter_bitmap_;
  const share::schema::ObColumnParam** col_params_;
  common::ObIAllocator& allocator_;
};

class ObBlackFilterExecutor : public ObPushdownFilterExecutor {
public:
  ObBlackFilterExecutor(common::ObIAllocator& alloc, ObPushdownBlackFilterNode& filter)
      : ObPushdownFilterExecutor(alloc, filter), n_eval_infos_(0), eval_infos_(nullptr), eval_ctx_(nullptr)
  {}
  ~ObBlackFilterExecutor()
  {}

  int filter(common::ObObj* objs, int64_t col_cnt, bool& ret_val);
  virtual int filter(bool& filtered) override;
  virtual int init_evaluated_datums(
      common::ObIAllocator& alloc, const common::ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx) override;
  void clear_evaluated_datums();
  INHERIT_TO_STRING_KV("ObPushdownBlackFilterExecutor", ObPushdownFilterExecutor, K_(filter), K_(n_eval_infos),
      K_(n_cols), "col_offsets", common::ObArrayWrap<int32_t>(col_offsets_, n_cols_), KP_(eval_infos), KP_(eval_ctx));

private:
  int32_t n_eval_infos_;
  ObEvalInfo** eval_infos_;
  ObEvalCtx* eval_ctx_;
};

OB_INLINE void ObBlackFilterExecutor::clear_evaluated_datums()
{
  for (int i = 0; i < n_eval_infos_; i++) {
    eval_infos_[i]->clear_evaluated_flag();
  }
}

class ObWhiteFilterExecutor : public ObPushdownFilterExecutor {
public:
  ObWhiteFilterExecutor(common::ObIAllocator& alloc, ObPushdownWhiteFilterNode& filter)
      : ObPushdownFilterExecutor(alloc, filter)
  {}
  ~ObWhiteFilterExecutor()
  {}

  virtual int filter(bool& filtered) override;
  virtual int init_evaluated_datums(
      common::ObIAllocator& alloc, const common::ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx) override;
  INHERIT_TO_STRING_KV("ObPushdownFilterExecutor", ObPushdownFilterExecutor, K_(filter));

private:
};

class ObAndFilterExecutor : public ObPushdownFilterExecutor {
public:
  ObAndFilterExecutor(common::ObIAllocator& alloc, ObPushdownAndFilterNode& filter)
      : ObPushdownFilterExecutor(alloc, filter)
  {}
  virtual int filter(bool& filtered) override;
  virtual int init_evaluated_datums(
      common::ObIAllocator& alloc, const common::ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx) override;
  INHERIT_TO_STRING_KV("ObPushdownFilterExecutor", ObPushdownFilterExecutor, K_(filter));

private:
};

class ObOrFilterExecutor : public ObPushdownFilterExecutor {
public:
  ObOrFilterExecutor(common::ObIAllocator& alloc, ObPushdownOrFilterNode& filter)
      : ObPushdownFilterExecutor(alloc, filter)
  {}

  virtual int filter(bool& filtered) override;
  virtual int init_evaluated_datums(
      common::ObIAllocator& alloc, const common::ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx) override;
  INHERIT_TO_STRING_KV("ObPushdownFilterExecutor", ObPushdownFilterExecutor, K_(filter));

private:
};

class ObFilterExecutorConstructor {
public:
  ObFilterExecutorConstructor(common::ObIAllocator* alloc) : alloc_(alloc), factory_(alloc)
  {}
  int apply(ObPushdownFilterNode* filter_tree, ObPushdownFilterExecutor*& filter_executor);

private:
  template <typename CLASST, FilterExecutorType type>
  int create_filter_executor(ObPushdownFilterNode* filter_tree, ObPushdownFilterExecutor*& filter_executor);

private:
  common::ObIAllocator* alloc_;
  ObPushdownFilterFactory factory_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_SQL_ENGINE_BASIC_OB_PUSHDOWN_FILTER_H_
