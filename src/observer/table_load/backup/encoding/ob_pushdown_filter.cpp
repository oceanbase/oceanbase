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

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/encoding/ob_pushdown_filter.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace share;

ObPushdownFilterFactory::PDFilterAllocFunc ObPushdownFilterFactory::PD_FILTER_ALLOC[PushdownFilterType::MAX_FILTER_TYPE] =
{
  ObPushdownFilterFactory::alloc<ObPushdownBlackFilterNode, BLACK_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownWhiteFilterNode, WHITE_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownAndFilterNode, AND_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownOrFilterNode, OR_FILTER>
};

ObPushdownFilterFactory::FilterExecutorAllocFunc ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[PushdownExecutorType::MAX_EXECUTOR_TYPE] =
{
  ObPushdownFilterFactory::alloc<ObBlackFilterExecutor, ObPushdownBlackFilterNode, BLACK_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObWhiteFilterExecutor, ObPushdownWhiteFilterNode, WHITE_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObAndFilterExecutor, ObPushdownAndFilterNode, AND_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObOrFilterExecutor, ObPushdownOrFilterNode, OR_FILTER_EXECUTOR>
};

OB_SERIALIZE_MEMBER(ObPushdownFilterNode, type_, n_child_, col_ids_);
OB_SERIALIZE_MEMBER((ObPushdownAndFilterNode,ObPushdownFilterNode));
OB_SERIALIZE_MEMBER((ObPushdownOrFilterNode,ObPushdownFilterNode));
OB_SERIALIZE_MEMBER((ObPushdownBlackFilterNode,ObPushdownFilterNode),
                    column_exprs_, filter_exprs_);
OB_SERIALIZE_MEMBER((ObPushdownWhiteFilterNode,ObPushdownFilterNode),
                    expr_, op_type_);

int ObPushdownBlackFilterNode::merge(ObIArray<ObPushdownFilterNode*> &merged_node)
{
  int ret = OB_SUCCESS;
  int64_t merge_expr_count = 0;
  for (int64_t i = 0; i < merged_node.count(); i++) {
    merge_expr_count += static_cast<ObPushdownBlackFilterNode *>(merged_node.at(i))->get_filter_expr_count();
  }
  if (0 < filter_exprs_.count()) {
    common::ObArray<ObExpr *> tmp_expr;
    if (OB_FAIL(tmp_expr.assign(filter_exprs_))) {
      LOG_WARN("failed to assign filter exprs", K(ret));
    } else if (FALSE_IT(filter_exprs_.reuse())) {
    } else if (OB_FAIL(filter_exprs_.init(tmp_expr.count() + merge_expr_count))) {
      LOG_WARN("failed to init filter exprs", K(ret));
    } else if (OB_FAIL(filter_exprs_.assign(tmp_expr))) {
      LOG_WARN("failed to assign filter exprs", K(ret));
    }
  } else if (OB_FAIL(filter_exprs_.init(1 + merge_expr_count))) {
    LOG_WARN("failed to init exprs", K(ret));
  } else if (OB_FAIL(filter_exprs_.push_back(tmp_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < merged_node.count(); ++i) {
    ObPushdownBlackFilterNode *black_node = static_cast<ObPushdownBlackFilterNode*>(merged_node.at(i));
    if (!black_node->filter_exprs_.empty()) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < black_node->filter_exprs_.count(); idx++) {
        if (OB_FAIL(filter_exprs_.push_back(black_node->filter_exprs_.at(idx)))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    } else if (OB_ISNULL(black_node->tmp_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: exprs must be only one", K(ret));
    } else if (OB_FAIL(filter_exprs_.push_back(black_node->tmp_expr_))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObPushdownBlackFilterNode::postprocess()
{
  int ret = OB_SUCCESS;
  if (0 == filter_exprs_.count()) {
    // 没有merge
    OZ(filter_exprs_.init(1));
    OZ(filter_exprs_.push_back(tmp_expr_));
  }
  return ret;
}

const common::ObCmpOp ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[] = {
  CO_EQ,  // WHITE_OP_EQ
  CO_LE,  // WHITE_OP_LE
  CO_LT,  // WHITE_OP_LT
  CO_GE,  // WHITE_OP_GE
  CO_GT,  // WHITE_OP_GT
  CO_NE,  // WHITE_OP_NE
  CO_MAX, // WHITE_OP_BT
  CO_MAX, // WHITE_OP_IN
  CO_MAX, // WHITE_OP_NU
  CO_MAX  // WHITE_OP_NN
};

int ObPushdownWhiteFilterNode::set_op_type(const ObItemType &type)
{
  int ret = OB_SUCCESS;
  switch (type) {
    case T_OP_EQ:
      op_type_ = WHITE_OP_EQ;
      break;
    case T_OP_LE:
      op_type_ = WHITE_OP_LE;
      break;
    case T_OP_LT:
      op_type_ = WHITE_OP_LT;
      break;
    case T_OP_GE:
      op_type_ = WHITE_OP_GE;
      break;
    case T_OP_GT:
      op_type_ = WHITE_OP_GT;
      break;
    case T_OP_NE:
      op_type_ = WHITE_OP_NE;
      break;
    case T_OP_BTW:
      op_type_ = WHITE_OP_BT;
      break;
    case T_OP_IN:
      op_type_ = WHITE_OP_IN;
      break;
    case T_FUN_SYS_ISNULL:
      op_type_ = WHITE_OP_NU;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObPushdownFilterFactory::alloc(PushdownFilterType type, uint32_t n_child, ObPushdownFilterNode *&pd_filter)
{
  int ret = OB_SUCCESS;
  if (!(BLACK_FILTER <= type && type < MAX_FILTER_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter type", K(ret), K(type));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null allocator", K(ret));
  } else if (OB_ISNULL(ObPushdownFilterFactory::PD_FILTER_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter type to alloc", K(ret), K(type));
  } else if (OB_FAIL(ObPushdownFilterFactory::PD_FILTER_ALLOC[type](*alloc_, n_child, pd_filter))) {
    LOG_WARN("fail to alloc pushdown filter", K(ret), K(type));
  } else {
  }
  return ret;
}

template <typename ClassT, PushdownFilterType type>
int ObPushdownFilterFactory::alloc(common::ObIAllocator &alloc, uint32_t n_child, ObPushdownFilterNode *&pd_filter)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc pushdown filter", K(ret));
  } else {
    pd_filter = new(buf) ClassT(alloc);
    if (0 < n_child) {
      void *tmp_buf = alloc.alloc(n_child * sizeof(ObPushdownFilterNode*));
      if (OB_ISNULL(tmp_buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc pushdown filter", K(ret));
      } else {
        pd_filter->childs_ = reinterpret_cast<ObPushdownFilterNode**>(tmp_buf);
      }
      pd_filter->n_child_ = n_child;
    } else {
      pd_filter->childs_ = nullptr;
    }
    pd_filter->set_type(type);
  }
  return ret;
}

int ObPushdownFilterFactory::alloc(
    PushdownExecutorType type,
    uint32_t n_child,
    ObPushdownFilterNode &filter_node,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = OB_SUCCESS;
  if (!(BLACK_FILTER_EXECUTOR <= type && type < MAX_EXECUTOR_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter type", K(ret), K(type));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null allocator", K(ret));
  } else if (OB_ISNULL(ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter type to alloc", K(ret), K(type));
  } else if (OB_FAIL(ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[type](*alloc_, n_child, filter_node, filter_executor, op))) {
    LOG_WARN("fail to alloc pushdown filter", K(ret), K(type));
  } else {
  }
  return ret;
}

template <typename ClassT, typename FilterNodeT, PushdownExecutorType type>
int ObPushdownFilterFactory::alloc(
    common::ObIAllocator &alloc,
    uint32_t n_child,
    ObPushdownFilterNode &filter_node,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc pushdown filter", K(ret), K(sizeof(ClassT)));
  } else {
    filter_executor = new(buf) ClassT(alloc, *static_cast<FilterNodeT*>(&filter_node), op);
    if (0 < n_child) {
      void *tmp_buf = alloc.alloc(n_child * sizeof(ObPushdownFilterExecutor*));
      if (OB_ISNULL(tmp_buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc pushdown filter", K(ret));
      }
      filter_executor->set_childs(n_child, reinterpret_cast<ObPushdownFilterExecutor**>(tmp_buf));
    }
    filter_executor->set_type(type);
  }
  return ret;
}

int ObPushdownFilter::serialize_pushdown_filter(
    char *buf,
    int64_t buf_len,
    int64_t &pos,
    ObPushdownFilterNode *pd_storage_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pd_storage_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pd filter is null", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, pd_storage_filter->type_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, pd_storage_filter->n_child_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(pd_storage_filter->serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to encode", K(ret));
  } else {
    for (int64_t i = 0; i < pd_storage_filter->n_child_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(serialize_pushdown_filter(
                  buf, buf_len, pos, pd_storage_filter->childs_[i]))) {
        LOG_WARN("failed to serialize pushdown storage filter", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownFilter::deserialize_pushdown_filter(
    ObPushdownFilterFactory filter_factory,
    const char *buf,
    int64_t data_len,
    int64_t &pos,
    ObPushdownFilterNode *&pd_storage_filter)
{
  int ret = OB_SUCCESS;
  int32_t filter_type;
  uint32_t child_cnt = 0;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &filter_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, child_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else {
    if (OB_FAIL(filter_factory.alloc(
                static_cast<PushdownFilterType>(filter_type), child_cnt, pd_storage_filter))) {
      LOG_WARN("failed to allocate filter", K(ret));
    } else if (OB_FAIL(pd_storage_filter->deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize", K(ret));
    } else if (0 < child_cnt) {
      for (uint32_t i = 0; i < child_cnt && OB_SUCC(ret); ++i) {
        ObPushdownFilterNode *sub_pd_storage_filter = nullptr;
        if (OB_FAIL(deserialize_pushdown_filter(
                    filter_factory, buf, data_len, pos, sub_pd_storage_filter))) {
          LOG_WARN("failed to deserialize child", K(ret));
        } else {
          pd_storage_filter->childs_[i] = sub_pd_storage_filter;
        }
      }
      if (pd_storage_filter->n_child_ != child_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child count is not match", K(ret), K(pd_storage_filter->n_child_), K(child_cnt));
      }
    }
  }
  return ret;
}

int64_t ObPushdownFilter::get_serialize_pushdown_filter_size(
    ObPushdownFilterNode *pd_filter_node)
{
  int64_t len = 0;
  if (OB_NOT_NULL(pd_filter_node)) {
    len += serialization::encoded_length_vi32(pd_filter_node->type_);
    len += serialization::encoded_length(pd_filter_node->n_child_);
    len += pd_filter_node->get_serialize_size();
    for (int64_t i = 0; i < pd_filter_node->n_child_; ++i) {
      len += get_serialize_pushdown_filter_size(pd_filter_node->childs_[i]);
    }
  } else {
    int ret = OB_SUCCESS;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pushdown filter is null", K(ret));
  }
  return len;
}

OB_DEF_SERIALIZE(ObPushdownFilter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_tree_)) {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, false))) {
      LOG_WARN("fail to encode op type", K(ret));
    }
  } else {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, true))) {
      LOG_WARN("fail to encode op type", K(ret));
    } else if (OB_FAIL(serialize_pushdown_filter(buf, buf_len, pos, filter_tree_))) {
      LOG_WARN("failed to serialize pushdown filter", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPushdownFilter)
{
  int ret = OB_SUCCESS;
  bool has_filter = false;
  filter_tree_ = nullptr;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, has_filter))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (has_filter) {
    ObPushdownFilterFactory filter_factory(&alloc_);
    if (OB_FAIL(deserialize_pushdown_filter(filter_factory, buf, data_len, pos, filter_tree_))) {
      LOG_WARN("failed to deserialize pushdown filter", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushdownFilter)
{
  int64_t len = 0;
  if (OB_ISNULL(filter_tree_)) {
    len += serialization::encoded_length(false);
  } else {
    len += serialization::encoded_length(true);
    len += get_serialize_pushdown_filter_size(filter_tree_);
  }
  return len;
}

//--------------------- start filter executor ----------------------------
int ObPushdownFilterExecutor::find_evaluated_datums(
    ObExpr *expr, const ObIArray<ObExpr*> &calc_exprs, ObIArray<ObExpr*> &eval_exprs)
{
  int ret = OB_SUCCESS;
  if (is_contain(calc_exprs, expr)) {
    if (OB_FAIL(eval_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
    for (uint32_t i = 0; i < expr->arg_cnt_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(find_evaluated_datums(expr->args_[i], calc_exprs, eval_exprs))) {
        LOG_WARN("failed to find evaluated datums", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownFilterExecutor::find_evaluated_datums(
    ObIArray<ObExpr*> &src_exprs,
    const ObIArray<ObExpr*> &calc_exprs,
    ObIArray<ObExpr*> &eval_exprs)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < src_exprs.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(find_evaluated_datums(src_exprs.at(i), calc_exprs, eval_exprs))) {
      LOG_WARN("failed to find evaluated datums", K(ret));
    }
  }
  return ret;
}

int ObPushdownFilterExecutor::init_bitmap(const int64_t row_count, ObBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_NOT_NULL(filter_bitmap_)) {
    if (OB_FAIL(filter_bitmap_->expand_size(row_count))) {
      LOG_WARN("Failed to expand size of filter bitmap", K(ret));
    } else if (FALSE_IT(filter_bitmap_->reuse(is_logic_and_node()))) {
    }
  } else {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBitmap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for filter bitmap", K(ret));
    } else if (FALSE_IT(filter_bitmap_ = new (buf) ObBitmap(allocator_))) {
    } else if (OB_FAIL(filter_bitmap_->init(row_count, is_logic_and_node()))) {
      LOG_WARN("Failed to init result bitmap", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bitmap = filter_bitmap_;
  }
  return ret;
}

int ObPushdownFilterExecutor::init_filter_param(
    const common::ObIArray<share::schema::ObColumnParam *> &col_params,
    const common::ObIArray<int32_t> &output_projector,
    const bool need_padding)
{
  int ret = OB_SUCCESS;
  const ObIArray<uint64_t> &col_ids = get_col_ids();
  const int64_t col_count = col_ids.count();
  if (is_filter_node()) {
    if (0 == col_count) {
    } else if (OB_FAIL(init_array_param(col_params_, col_count))) {
      LOG_WARN("Fail to init col params", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(col_offsets_, col_count))) {
      LOG_WARN("Fail to init col offsets", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(default_datums_, col_count))) {
      LOG_WARN("Fail to init default datums", K(ret), K(col_count));
    } else {
      const share::schema::ObColumnParam *col_param = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
        int32_t idx = OB_INVALID_INDEX;
        for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < output_projector.count(); ++j) {
          if (OB_UNLIKELY(output_projector.at(j) >= col_params.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected col projector", K(ret), K(output_projector.at(j)), K(col_params.count()));
          } else if (col_ids.at(i) == col_params.at(output_projector.at(j))->get_column_id()) {
            idx = output_projector.at(j);
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected idx found", K(ret));
        } else if (OB_FAIL(col_offsets_.push_back(idx))) {
          LOG_WARN("failed to push back col offset", K(ret));
        } else {
          col_param = nullptr;
          blocksstable::ObStorageDatum default_datum;
          const common::ObObj &def_cell = col_params.at(idx)->get_orig_default_value();
          if (need_padding && col_params.at(idx)->get_meta_type().is_fixed_len_char_type()) {
            col_param = col_params.at(idx);
          }
          if (OB_FAIL(col_params_.push_back(col_param))) {
            LOG_WARN("failed to push back col param", K(ret));
          } else if (!def_cell.is_nop_value()) {
            if (OB_FAIL(default_datum.from_obj(def_cell))) {
              LOG_WARN("convert obj to datum failed", K(ret), K(col_params_.count()), K(def_cell));
            } else if (col_params.at(idx)->get_meta_type().is_lob_storage() && !def_cell.is_null()) {
              // lob def value must have no lob header when not null
              // When do lob pushdown, should add lob header for default value
              ObString data = default_datum.get_string();
              ObString out;
              if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, data, out))) {
                LOG_WARN("failed to fill lob header for column.", K(idx), K(def_cell), K(data));
              } else {
                default_datum.set_string(out);
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(default_datums_.push_back(default_datum))) {
            LOG_WARN("Fail to push back default datum", K(ret));
          }
        }
      }
    }
  } else {
    for (uint32_t i = 0; OB_SUCC(ret) && i < n_child_; i++) {
      if (OB_NOT_NULL(childs_[i]) &&
          OB_FAIL(childs_[i]->init_filter_param(col_params, output_projector, need_padding))) {
        LOG_WARN("Failed to init pushdown filter param", K(ret), K(i), KP(childs_[i]));
      }
    }
  }

  if (OB_SUCC(ret)) {
    n_cols_ = col_count;
  }
  return ret;
}

template<typename T>
int ObPushdownFilterExecutor::init_array_param(common::ObFixedArray<T, common::ObIAllocator> &param, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(param.clear())) {
  } else if (OB_FAIL(param.reserve(size))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WARN("Failed to init params", K(ret));
    } else {
      param.reset();
      if (OB_FAIL(param.init(size))) {
        LOG_WARN("Failed to init params", K(ret), K(size));
      }
    }
  }
  return ret;
}

ObPushdownFilterExecutor::ObPushdownFilterExecutor(common::ObIAllocator &alloc,
                                                   ObPushdownOperator &op,
                                                   PushdownExecutorType type)
  : type_(type), need_check_row_filter_(false),
    n_cols_(0), n_child_(0), childs_(nullptr),
    filter_bitmap_(nullptr), col_params_(alloc),
    col_offsets_(alloc), default_datums_(alloc),
    allocator_(alloc), op_(op)
{}

ObPushdownFilterExecutor::~ObPushdownFilterExecutor()
{
  if (nullptr != filter_bitmap_) {
    filter_bitmap_->~ObBitmap();
    allocator_.free(filter_bitmap_);
  }
  filter_bitmap_ = nullptr;
  col_params_.reset();
  col_offsets_.reset();
  default_datums_.reset();
  for (uint32_t i = 0; i < n_child_; i++) {
    if (OB_NOT_NULL(childs_[i])) {
      childs_[i]->~ObPushdownFilterExecutor();
      childs_[i] = nullptr;
    }
  }
  n_child_ = 0;
  need_check_row_filter_ = false;
}

DEF_TO_STRING(ObPushdownFilterExecutor)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), K_(need_check_row_filter), K_(n_cols),
       K_(n_child), KP_(childs), KP_(filter_bitmap),
       K_(col_params), K_(default_datums), K_(col_offsets));
  J_OBJ_END();
  return pos;
}

int ObPushdownFilterExecutor::prepare_skip_filter()
{
  int ret = OB_SUCCESS;
  need_check_row_filter_ = false;
  if (OB_ISNULL(filter_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter bitmap", K(ret));
  } else if (PushdownExecutorType::AND_FILTER_EXECUTOR == type_) {
    need_check_row_filter_ = !filter_bitmap_->is_all_true();
  } else if (PushdownExecutorType::OR_FILTER_EXECUTOR == type_) {
    need_check_row_filter_ = !filter_bitmap_->is_all_false();
  }

  return ret;
}

ObBlackFilterExecutor::~ObBlackFilterExecutor()
{
  if (nullptr != eval_infos_) {
    allocator_.free(eval_infos_);
    eval_infos_ = nullptr;
  }
  if (nullptr != datum_eval_flags_) {
    allocator_.free(datum_eval_flags_);
    datum_eval_flags_ = nullptr;
  }
  if (nullptr != skip_bit_) {
    allocator_.free(skip_bit_);
    skip_bit_ = nullptr;
  }
}

int ObBlackFilterExecutor::filter(ObEvalCtx &eval_ctx, bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  ObDatum *cmp_res = NULL;
  FOREACH_CNT_X(e, filter_.filter_exprs_, OB_SUCC(ret) && !filtered) {
    if (OB_FAIL((*e)->eval(eval_ctx, cmp_res))) {
      LOG_WARN("failed to filter child", K(ret));
    } else {
      filtered = is_row_filtered(*cmp_res);
    }
  }

  if (op_.is_vectorized()) {
    clear_evaluated_datums();
  } else {
    clear_evaluated_infos();
  }
  return ret;
}

// 提供给存储如果发现是黑盒filter，则调用该接口来判断是否被过滤掉
int ObBlackFilterExecutor::filter(ObObj *objs, int64_t col_cnt, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_cnt != filter_.column_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: column count not match", K(ret), K(col_cnt), K(filter_.col_ids_));
  } else {
    ObEvalCtx &eval_ctx = op_.get_eval_ctx();
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_.column_exprs_.count(); ++i) {
      filter_.column_exprs_.at(i)->get_eval_info(eval_ctx).projected_ = true;
      ObExpr * const &expr = filter_.column_exprs_.at(i);
      ObDatum &expr_datum = expr->locate_datum_for_write(eval_ctx);
      if (OB_FAIL(expr_datum.from_obj(objs[i]))) {
        LOG_WARN("Failed to convert object from datum", K(ret), K(objs[i]));
      } else if (is_lob_storage(objs[i].get_type()) &&
                 OB_FAIL(ob_adjust_lob_datum(objs[i], expr->obj_meta_, allocator_, expr_datum))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(objs[i]), K(expr->obj_meta_));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(filter(eval_ctx, filtered))) {
      LOG_WARN("failed to calc filter", K(ret));
    }
  }
  return ret;
}

int ObBlackFilterExecutor::filter(blocksstable::ObStorageDatum *datums, int64_t col_cnt, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_cnt != filter_.column_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: column count not match", K(ret), K(col_cnt), K(filter_.col_ids_));
  } else {
    ObEvalCtx &eval_ctx = op_.get_eval_ctx();
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_.column_exprs_.count(); ++i) {
      ObDatum &expr_datum = filter_.column_exprs_.at(i)->locate_datum_for_write(eval_ctx);
      if (OB_FAIL(expr_datum.from_storage_datum(datums[i], filter_.column_exprs_.at(i)->obj_datum_map_))) {
        LOG_WARN("Failed to convert object from datum", K(ret), K(datums[i]));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(filter(eval_ctx, filtered))) {
      LOG_WARN("failed to calc filter", K(ret));
    }
  }
  return ret;
}

// 根据calc expr来设置每个列（空集）对应的清理Datum
// 这里将clear的datum放在filter node是为了更精准处理，其实只有涉及到的表达式清理即可，其他不需要清理
// 还有类似空集需要清理
int ObBlackFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const int32_t cur_eval_info_cnt = n_eval_infos_;
  n_eval_infos_ = 0;
  n_datum_eval_flags_ = 0;
  ObSEArray<ObExpr*, 4> eval_exprs;
  if (OB_FAIL(find_evaluated_datums(filter_.filter_exprs_, op_.expr_spec_.calc_exprs_, eval_exprs))) {
    LOG_WARN("failed to find evaluated datums", K(ret));
  } else if (0 < eval_exprs.count()) {
    if (OB_FAIL(init_eval_param(cur_eval_info_cnt, eval_exprs.count()))) {
       LOG_WARN("failed to reuse filter param", K(ret));
    }
    FOREACH_CNT_X(e, eval_exprs, OB_SUCC(ret)) {
      eval_infos_[n_eval_infos_++] = &(*e)->get_eval_info(op_.get_eval_ctx());
      if (op_.is_vectorized() && (*e)->is_batch_result()) {
        datum_eval_flags_[n_datum_eval_flags_++] = &(*e)->get_evaluated_flags(op_.get_eval_ctx());
      }
    }
    if (OB_SUCC(ret)) {
      clear_evaluated_infos();
    }
  }
  return ret;
}

int ObBlackFilterExecutor::init_eval_param(const int32_t cur_eval_info_cnt, const int64_t eval_expr_cnt)
{
  int ret = OB_SUCCESS;
  if (eval_expr_cnt > cur_eval_info_cnt) {
    if (nullptr != eval_infos_) {
      allocator_.free(eval_infos_);
      eval_infos_ = nullptr;
    }
    if (nullptr != datum_eval_flags_) {
      allocator_.free(datum_eval_flags_);
      datum_eval_flags_ = nullptr;
    }
  }
  if (nullptr == eval_infos_) {
    if (OB_ISNULL(eval_infos_ = static_cast<ObEvalInfo **>(allocator_.alloc(
                  eval_expr_cnt * sizeof(ObEvalInfo*))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate eval infos", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr == datum_eval_flags_) {
    if (OB_ISNULL(datum_eval_flags_ = static_cast<ObBitVector **>(allocator_.alloc(
                  eval_expr_cnt * sizeof(ObBitVector*))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate eval flags", K(ret));
    }
  }
  return ret;
}

int ObBlackFilterExecutor::get_datums_from_column(common::ObIArray<common::ObDatum *> &datums)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  FOREACH_CNT_X(e, filter_.column_exprs_, OB_SUCC(ret)) {
    if (OB_FAIL(datums.push_back((*e)->locate_batch_datums(eval_ctx)))) {
      LOG_WARN("fail to push back datum", K(ret));
    }
  }
  return ret;
}

int ObWhiteFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  if (OB_ISNULL(filter_.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null expr", K(ret));
  } else if (OB_FAIL(init_array_param(params_, filter_.expr_->arg_cnt_))) {
    LOG_WARN("Failed to alloc params", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_.expr_->arg_cnt_; i++) {
      if (OB_ISNULL(filter_.expr_->args_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null expr arguments", K(ret), K(i));
      } else if (filter_.expr_->args_[i]->type_ == T_REF_COLUMN) {
        // skip column reference expr
        continue;
      } else {
        ObObj param;
        ObDatum *datum = NULL;
        if (OB_FAIL(filter_.expr_->args_[i]->eval(eval_ctx, datum))) {
          if (lib::is_oracle_mode()) {
            is_valid = false;
          } else {
            LOG_WARN("evaluate filter arg expr failed", K(ret), K(i));
          }
        } else if (OB_FAIL(datum->to_obj(param, filter_.expr_->args_[i]->obj_meta_, filter_.expr_->args_[i]->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        } else if (OB_FAIL(params_.push_back(param))) {
          LOG_WARN("Failed to push back param", K(ret));
        }
      }
    }
    LOG_DEBUG("[PUSHDOWN], white pushdown filter inited params", K(params_));
  }

  if (OB_SUCC(ret)) {
    check_null_params();
    if (WHITE_OP_IN == filter_.get_op_type() && OB_FAIL(init_obj_set())) {
      LOG_WARN("Failed to init Object hash set in filter node", K(ret));
    }
  }
  if (OB_UNLIKELY(!is_valid)) {
    ret = OB_SUCCESS;
  }
  return ret;
}

void ObWhiteFilterExecutor::check_null_params()
{
  null_param_contained_ = false;
  for (int64_t i = 0; !null_param_contained_ && i < params_.count(); i++) {
    if ((lib::is_mysql_mode() && params_.at(i).is_null())
        || (lib::is_oracle_mode() && params_.at(i).is_null_oracle())) {
      null_param_contained_ = true;
    }
  }
  return;
}

int ObWhiteFilterExecutor::init_obj_set()
{
  int ret = OB_SUCCESS;
  if (param_set_.created()) {
    param_set_.destroy();
  }
  if (OB_FAIL(param_set_.create(params_.count() * 2))) {
    LOG_WARN("Failed to create hash set", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < params_.count(); ++i) {
    if (OB_FAIL(param_set_.set_refactored(params_.at(i)))) {
      if (OB_UNLIKELY(ret != OB_HASH_EXIST)) {
        LOG_WARN("Failed to insert object into hashset", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObWhiteFilterExecutor::exist_in_obj_set(const ObObj &obj, bool &is_exist) const
{
  int ret = param_set_.exist_refactored(obj);
  if (OB_HASH_EXIST == ret) {
    ret = OB_SUCCESS;
    is_exist = true;
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    is_exist = false;
  } else {
    LOG_WARN("Failed to search in obj_set in pushed down filter node", K(ret), K(obj));
  }
  return ret;
}

int ObAndFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret) && OB_LIKELY(is_valid); ++i) {
    if (OB_FAIL(childs_[i]->init_evaluated_datums(is_valid))) {
      LOG_WARN("failed to filter child", K(ret));
    }
  }
  return ret;
}

int ObOrFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret) && OB_LIKELY(is_valid); ++i) {
    if (OB_FAIL(childs_[i]->init_evaluated_datums(is_valid))) {
      LOG_WARN("failed to filter child", K(ret));
    }
  }
  return ret;
}

template<typename CLASST, PushdownExecutorType type>
int ObFilterExecutorConstructor::create_filter_executor(
    ObPushdownFilterNode *filter_tree,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = OB_SUCCESS;
  ObPushdownFilterExecutor *tmp_filter_executor = nullptr;
  if (OB_ISNULL(filter_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter tree is null", K(ret));
  } else if (OB_FAIL(factory_.alloc(type, filter_tree->n_child_, *filter_tree, tmp_filter_executor, op))) {
    LOG_WARN("failed to alloc pushdown filter", K(ret), K(filter_tree->n_child_));
  } else {
    filter_executor = static_cast<CLASST*>(tmp_filter_executor);
    for (int64_t i = 0; i < filter_tree->n_child_ && OB_SUCC(ret); ++i) {
      ObPushdownFilterExecutor *sub_filter_executor = nullptr;
      if (OB_FAIL(apply(filter_tree->childs_[i], sub_filter_executor, op))) {
        LOG_WARN("failed to apply filter node", K(ret));
      } else if (OB_ISNULL(sub_filter_executor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub filter executor is null", K(ret));
      } else if (OB_ISNULL(filter_executor->get_childs())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("childs is null", K(ret));
      } else {
        filter_executor->set_child(i, sub_filter_executor);
      }
    }
  }
  return ret;
}

int ObFilterExecutorConstructor::apply(
    ObPushdownFilterNode *filter_tree,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter node is null", K(ret));
  } else {
    switch(filter_tree->get_type()) {
      case BLACK_FILTER: {
        ret = create_filter_executor<ObBlackFilterExecutor, BLACK_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case WHITE_FILTER: {
        ret = create_filter_executor<ObWhiteFilterExecutor, WHITE_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case AND_FILTER: {
        ret = create_filter_executor<ObAndFilterExecutor, AND_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case OR_FILTER: {
        ret = create_filter_executor<ObOrFilterExecutor, OR_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected filter type", K(ret));
        break;
    }
  }
  return ret;
}

ObPushdownExprSpec::ObPushdownExprSpec(ObIAllocator &alloc)
  : calc_exprs_(alloc),
    access_exprs_(alloc),
    max_batch_size_(0),
    pushdown_filters_(alloc),
    pd_storage_flag_(0),
    pd_storage_filters_(alloc),
    pd_storage_aggregate_output_(alloc),
    ext_file_column_exprs_(alloc),
    ext_column_convert_exprs_(alloc),
    trans_info_expr_(nullptr)
{
}

OB_DEF_SERIALIZE(ObPushdownExprSpec)
{
  int ret = OB_SUCCESS;
  ExprFixedArray fake_filters_before_index_back(CURRENT_CONTEXT->get_allocator());
  ObPushdownFilter fake_pd_storage_index_back_filters(CURRENT_CONTEXT->get_allocator());
  LST_DO_CODE(OB_UNIS_ENCODE,
              calc_exprs_,
              access_exprs_,
              max_batch_size_,
              pushdown_filters_,
              fake_filters_before_index_back, //mock a fake filters to compatible with 4.0
              pd_storage_flag_,
              pd_storage_filters_,
              fake_pd_storage_index_back_filters, //mock a fake filters to compatible with 4.0
              pd_storage_aggregate_output_,
              ext_file_column_exprs_,
              ext_column_convert_exprs_,
              trans_info_expr_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPushdownExprSpec)
{
  int ret = OB_SUCCESS;
  ExprFixedArray fake_filters_before_index_back(CURRENT_CONTEXT->get_allocator());
  ObPushdownFilter fake_pd_storage_index_back_filters(CURRENT_CONTEXT->get_allocator());
  LST_DO_CODE(OB_UNIS_DECODE,
              calc_exprs_,
              access_exprs_,
              max_batch_size_,
              pushdown_filters_,
              fake_filters_before_index_back, //mock a fake filters to compatible with 4.0
              pd_storage_flag_,
              pd_storage_filters_,
              fake_pd_storage_index_back_filters, //mock a fake filters to compatible with 4.0
              pd_storage_aggregate_output_,
              ext_file_column_exprs_,
              ext_column_convert_exprs_,
              trans_info_expr_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushdownExprSpec)
{
  int64_t len = 0;
  ExprFixedArray fake_filters_before_index_back(CURRENT_CONTEXT->get_allocator());
  ObPushdownFilter fake_pd_storage_index_back_filters(CURRENT_CONTEXT->get_allocator());
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              calc_exprs_,
              access_exprs_,
              max_batch_size_,
              pushdown_filters_,
              fake_filters_before_index_back, //mock a fake filters to compatible with 4.0
              pd_storage_flag_,
              pd_storage_filters_,
              fake_pd_storage_index_back_filters, //mock a fake filters to compatible with 4.0
              pd_storage_aggregate_output_,
              ext_file_column_exprs_,
              ext_column_convert_exprs_,
              trans_info_expr_);
  return len;
}

ObPushdownOperator::ObPushdownOperator(ObEvalCtx &eval_ctx, const ObPushdownExprSpec &expr_spec)
  : pd_storage_filters_(nullptr),
    eval_ctx_(eval_ctx),
    expr_spec_(expr_spec)
{
}

int ObPushdownOperator::init_pushdown_storage_filter()
{
  int ret = OB_SUCCESS;
  if (0 != expr_spec_.pd_storage_flag_) {
    ObFilterExecutorConstructor filter_exec_constructor(&eval_ctx_.exec_ctx_.get_allocator());
    if (OB_NOT_NULL(expr_spec_.pd_storage_filters_.get_pushdown_filter())) {
      if (OB_FAIL(filter_exec_constructor.apply(expr_spec_.pd_storage_filters_.get_pushdown_filter(),
                                                pd_storage_filters_,
                                                *this))) {
        LOG_WARN("failed to create filter executor", K(ret));
      } else if (OB_ISNULL(pd_storage_filters_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter executor is null", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownOperator::reset_trans_info_datum()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(expr_spec_.trans_info_expr_)) {
    if (expr_spec_.trans_info_expr_->is_batch_result()) {
      ObDatum *datums = expr_spec_.trans_info_expr_->locate_datums_for_update(eval_ctx_, expr_spec_.max_batch_size_);
      for (int64_t i = 0; i < expr_spec_.max_batch_size_; i++) {
        datums[i].set_null();
      }
    } else {
      ObDatum &datum = expr_spec_.trans_info_expr_->locate_datum_for_write(eval_ctx_);
      datum.set_null();
    }
  }
  return ret;
}

int ObPushdownOperator::write_trans_info_datum(blocksstable::ObDatumRow &out_row)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(expr_spec_.trans_info_expr_) &&
      OB_NOT_NULL(out_row.trans_info_)) {
    ObDatum &datum = expr_spec_.trans_info_expr_->locate_datum_for_write(eval_ctx_);
    char *dst_ptr = const_cast<char *>(datum.ptr_);
    int64_t pos = 0;
    if (OB_ISNULL(dst_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else if (OB_FAIL(databuff_memcpy(dst_ptr,
                                       ObDASWriteBuffer::DAS_ROW_TRANS_STRING_SIZE,
                                       pos,
                                       strlen(out_row.trans_info_),
                                       out_row.trans_info_))) {
      LOG_WARN("fail to copy trans info to datum", K(ret));
    } else {
      datum.pack_ = pos;
      // out_row.trans_info_ must be reset to nullptr to prevent affecting the next row
      out_row.trans_info_ = nullptr;
    }
  }
  return ret;
}

int ObPushdownOperator::clear_datum_eval_flag()
{
  int ret = OB_SUCCESS;
  FOREACH_CNT(e, expr_spec_.calc_exprs_) {
    if ((*e)->is_batch_result()) {
      (*e)->get_evaluated_flags(eval_ctx_).unset(eval_ctx_.get_batch_idx());
    } else {
      (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
    }
  }
  return ret;
}

int ObPushdownOperator::clear_evaluated_flag()
{
  int ret = OB_SUCCESS;
  FOREACH_CNT(e, expr_spec_.calc_exprs_) {
    (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
  }
  return ret;
}

int ObPushdownOperator::deep_copy(const sql::ObExprPtrIArray *exprs, const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == exprs || batch_idx >= expr_spec_.max_batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exprs), K(batch_idx), K(expr_spec_.max_batch_size_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs->count(); i++) {
      char *ptr = nullptr;
      sql::ObExpr *e = exprs->at(i);
      if (OBJ_DATUM_STRING == e->obj_datum_map_) {
        ObDatum &datum = e->locate_expr_datum(eval_ctx_, batch_idx);
        if (!datum.null_) {
          if (OB_ISNULL(ptr = e->get_str_res_mem(eval_ctx_, datum.len_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            MEMMOVE(const_cast<char *>(ptr), datum.ptr_, datum.len_);
            datum.ptr_ = ptr;
          }
        }
      }
    }
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
