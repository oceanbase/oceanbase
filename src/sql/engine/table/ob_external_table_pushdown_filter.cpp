/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "ob_external_table_pushdown_filter.h"
#include "share/external_table/ob_external_table_utils.h"

namespace oceanbase
{

using namespace common;
using namespace share;
namespace sql
{

OB_SERIALIZE_MEMBER((ObColumnMeta, ObDatumMeta), max_length_, has_lob_header_, is_valid_);

int ObColumnMeta::from_ob_expr(const ObExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr");
  } else {
    type_ = expr->datum_meta_.type_;
    cs_type_ = expr->datum_meta_.cs_type_;
    scale_ = expr->datum_meta_.scale_;
    precision_ = expr->datum_meta_.precision_;
    max_length_ = expr->max_length_;
    has_lob_header_ = expr->obj_meta_.has_lob_header();
    is_valid_ = true;
  }
  return ret;
}

int ObColumnMeta::from_ob_raw_expr_res_type(const ObRawExprResType &res_type)
{
  int ret = OB_SUCCESS;
  type_ = res_type.get_type();
  cs_type_ = res_type.get_collation_type();
  scale_ = res_type.get_scale();
  precision_ = res_type.get_precision();
  max_length_ = res_type.get_length();
  has_lob_header_ = res_type.has_lob_header();
  if (res_type.is_xml_sql_type()) {
    // set xml subschema id = ObXMLSqlType
    cs_type_ = CS_TYPE_INVALID;
  } else if (res_type.is_collection_sql_type()) {
    cs_type_ = static_cast<ObCollationType>(res_type.get_subschema_id());
  }
  is_valid_ = true;
  return ret;
}

int ObColumnMeta::assign(const ObColumnMeta &other)
{
  int ret = OB_SUCCESS;
  type_ = other.type_;
  cs_type_ = other.cs_type_;
  scale_ = other.scale_;
  precision_ = other.precision_;
  max_length_ = other.max_length_;
  has_lob_header_ = other.has_lob_header_;
  is_valid_ = other.is_valid_;
  return ret;
}

ObColumnMeta& ObColumnMeta::operator=(const ObColumnMeta &other)
{
  IGNORE_RETURN this->assign(other);
  return *this;
}

int ObExternalTablePushdownFilter::init(sql::ObPushdownFilterExecutor *pd_storage_filters,
                                        int64_t ext_tbl_filter_pd_level,
                                        const ObIArray<uint64_t> &column_ids,
                                        ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pd_storage_filters)) {
    // no pushdown filters
  } else {
    pd_storage_filters_ = pd_storage_filters;
    ext_tbl_filter_pd_level_ = ext_tbl_filter_pd_level;
    column_ids_ = &column_ids;
    allocator_.set_attr(ObMemAttr(MTL_ID(), "ExtTblFtPD"));
    if (OB_FAIL(skip_filter_executor_.init(MAX(1, eval_ctx.max_batch_size_), &allocator_))) {
      LOG_WARN("Failed to init skip filter executor", K(ret));
    } else if (OB_FAIL(build_skipping_filter_nodes(*pd_storage_filters))) {
      LOG_WARN("failed to build skip index", K(ret));
    } else if (OB_FAIL(file_filter_col_ids_.prepare_allocate(skipping_filter_nodes_.count()))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    } else if (OB_FAIL(file_filter_col_metas_.prepare_allocate(skipping_filter_nodes_.count()))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::prepare_filter_col_meta(
    const common::ObArrayWrap<int> &file_col_index,
    const common::ObIArray<uint64_t> &col_ids,
    const common::ObIArray<ObColumnMeta> &col_metas)
{
  int ret = OB_SUCCESS;
  CK (file_col_index.count() == col_metas.count());
  CK (col_ids.count() == col_metas.count());
  filter_enabled_ = true;
  int64_t real_filter_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && filter_enabled_ && i < file_col_index.count(); ++i) {
    const int file_col_id = file_col_index.at(i);
    for (int64_t f_idx = 0; OB_SUCC(ret) && f_idx < skipping_filter_nodes_.count(); ++f_idx) {
      const ObSkippingFilterNode &node = skipping_filter_nodes_.at(f_idx);
      if (OB_ISNULL(node.filter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null filter", K(ret));
      } else if (OB_INVALID_ID == col_ids.at(i) || !col_metas.at(i).is_valid_) {
        filter_enabled_ = false;
      } else if (node.filter_->get_col_ids().at(0) == col_ids.at(i) && file_col_id >= 0) {
        file_filter_col_ids_.at(f_idx) = file_col_id;
        file_filter_col_metas_.at(f_idx) = col_metas.at(i);
        ++real_filter_cnt;
      }
    }
  }
  if (0 == real_filter_cnt) {
    filter_enabled_ = false;
  }
  return ret;
}

int ObExternalTablePushdownFilter::apply_skipping_index_filter(const PushdownLevel filter_level,
                                                               MinMaxFilterParamBuilder &param_builder,
                                                               bool &skipped,
                                                               const int64_t row_count/*= MOCK_ROW_COUNT*/)
{
  int ret = OB_SUCCESS;
  skipped = false;
  if (filter_enabled_
      && nullptr != pd_storage_filters_
      && can_apply_filters(filter_level)) {
    blocksstable::ObMinMaxFilterParam filter_param;
    for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
      filter_param.set_uncertain();
      ObSkippingFilterNode &node = skipping_filter_nodes_.at(i);
      const int ext_tbl_col_id = file_filter_col_ids_.at(i);
      if (!file_filter_col_metas_.at(i).is_valid_) {
      } else if (OB_FAIL(param_builder.build(ext_tbl_col_id, file_filter_col_metas_.at(i), filter_param))) {
        LOG_WARN("fail to build param", K(ret), K(i));
      } else if (!filter_param.is_uncertain()) {
        bool filter_valid = true;
        const uint64_t ob_col_id = node.filter_->get_col_ids().at(0);
        if (OB_FAIL(node.filter_->init_evaluated_datums(filter_valid))) {
          LOG_WARN("failed to init filter", K(ret));
        } else if (!filter_valid) {
        } else if (OB_FAIL(skip_filter_executor_.falsifiable_pushdown_filter(
            ob_col_id, node.skip_index_type_, row_count, filter_param, *node.filter_, true))) {
          LOG_WARN("failed to apply skip index", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObBoolMask bm;
      if (OB_FAIL(pd_storage_filters_->execute_skipping_filter(bm))) {
        LOG_WARN("fail to execute skipping filter", K(ret));
      } else {
        skipped = bm.is_always_false();
      }
    }
    // reset filters
    for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
      skipping_filter_nodes_[i].filter_->set_filter_uncertain();
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::apply_skipping_index_filter(const PushdownLevel filter_level,
                                                               MinMaxFilterParamBuilder &param_builder,
                                                               ObBitVector *rg_bitmap,
                                                               const int64_t num_rows,
                                                               ObIAllocator &tmp_alloc)
{
  int ret = OB_SUCCESS;
  if (filter_enabled_
    && nullptr != pd_storage_filters_
    && can_apply_filters(filter_level)
    && param_builder.has_page_index()) {
    blocksstable::ObMinMaxFilterParam filter_param;
    ObArray<ObArray<PageSkipInfo>> skip_infos;
    ObArray<PageSkipInfo> tmp_info;
    ObMemAttr attr(MTL_ID(), common::ObModIds::OB_HASH_BUCKET);
    common::hash::ObHashMap<uint64_t,
                            ObArray<PageSkipInfo> *,
                            common::hash::NoPthreadDefendMode> info_map;
    OZ (info_map.create(2 * skipping_filter_nodes_.count(), attr));
    for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
      tmp_info.reset();
      ObSkippingFilterNode &node = skipping_filter_nodes_.at(i);
      const int ext_tbl_col_id = file_filter_col_ids_.at(i);
      int64_t offset = -1;
      int64_t rows = -1;
      param_builder.rescan();
      while (OB_SUCC(param_builder.next_range(ext_tbl_col_id, offset, rows))) {
        filter_param.set_uncertain();
        PageSkipInfo skip_info(ext_tbl_col_id, offset, rows);
        if (OB_FAIL(param_builder.build(ext_tbl_col_id, file_filter_col_metas_.at(i), filter_param))) {
          LOG_WARN("fail to build param", K(ret), K(i));
        } else if (!filter_param.is_uncertain()) {
          bool filter_valid = true;
          const uint64_t ob_col_id = node.filter_->get_col_ids().at(0);
          if (OB_FAIL(node.filter_->init_evaluated_datums(filter_valid))) {
            LOG_WARN("failed to init filter", K(ret));
          } else if (!filter_valid) {
          } else if (OB_FAIL(skip_filter_executor_.falsifiable_pushdown_filter(
              ob_col_id, node.skip_index_type_, MOCK_ROW_COUNT,
              filter_param, *node.filter_, true))) {
            LOG_WARN("failed to apply skip index", K(ret));
          } else if (node.filter_->is_filter_always_false()) {
            skip_info.skipped_ = true;
          }
        }
        // reset filters
        skipping_filter_nodes_[i].filter_->set_filter_uncertain();
        OZ (tmp_info.push_back(skip_info));
        //LOG_INFO("print skip info", K(ext_tbl_col_id), K(skip_info));
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      OZ (skip_infos.push_back(tmp_info));
      OZ (info_map.set_refactored(reinterpret_cast<uint64_t> (node.filter_), &skip_infos[i]));
    }
    ObBitVector *filter_rg_bitmap = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(execute_skip_index(info_map, num_rows, filter_rg_bitmap,
                                          pd_storage_filters_, tmp_alloc))) {
      LOG_WARN("failed to calc skip index", K(ret));
    } else {
      rg_bitmap->bit_calculate(*rg_bitmap, *filter_rg_bitmap, num_rows, bit_or_op);
    }
    info_map.destroy();
  }
  return ret;
}

int ObExternalTablePushdownFilter::execute_skip_index(const common::hash::ObHashMap<uint64_t,
                                                      ObArray<PageSkipInfo> *,
                                                      common::hash::NoPthreadDefendMode> &info_map,
                                                      const int64_t num_rows,
                                                      ObBitVector *&filter_rg_bitmap,
                                                      ObPushdownFilterExecutor *root_filter,
                                                      ObIAllocator &tmp_alloc)
{
  int ret = OB_SUCCESS;
  if (nullptr != root_filter) {
    if (root_filter->is_filter_node()) {
      ObArray<PageSkipInfo> *curr_info = nullptr;
      if (OB_FAIL(info_map.get_refactored(reinterpret_cast<uint64_t> (root_filter), curr_info))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to find in map", K(ret));
        } else if (OB_FAIL(generate_default_bitmap(num_rows, filter_rg_bitmap, tmp_alloc))) {
          LOG_WARN("failed to generate default bitmap", K(ret), K(num_rows));
        }
      } else if (OB_FAIL(generate_bitmap(num_rows, *curr_info, filter_rg_bitmap, tmp_alloc))) {
        LOG_WARN("failed to generate bitmap", K(ret), K(num_rows), KP(root_filter));
      }
    } else if (root_filter->is_logic_op_node()) {
      sql::ObPushdownFilterExecutor **children = root_filter->get_childs();
      ObBitVector *bm = nullptr;
      ObBitVector *child_bm = nullptr;
      for (uint32_t i = 0; OB_SUCC(ret) && i < root_filter->get_child_count(); i++) {
        ObArray<PageSkipInfo> *curr_info = nullptr;
        if (OB_ISNULL(children[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child filter", K(ret));
        } else if (OB_FAIL(execute_skip_index(info_map, num_rows, child_bm, children[i], tmp_alloc))) {
          LOG_WARN("Fail to execute skipping filter", K(ret), K(i), KP(children[i]));
        } else if (0 == i) {
          bm = child_bm;
        } else if (root_filter->is_logic_and_node()) {
          bm->bit_calculate(*bm, *child_bm, num_rows, bit_or_op);
        } else {
          bm->bit_calculate(*bm, *child_bm, num_rows, bit_and_op);
        }
      }
      filter_rg_bitmap = bm;
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::generate_bitmap(const int64_t num_rows,
                                                   const ObArray<PageSkipInfo> &curr_info,
                                                   ObBitVector *&filter_rg_bitmap,
                                                   ObIAllocator &tmp_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_rg_bitmap = to_bit_vector(tmp_alloc.alloc(ObBitVector::memory_size(num_rows))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc rg bitmap", K(ret), K(num_rows));
  } else {
    int64_t sum_rows = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < curr_info.count(); ++i) {
      sum_rows += curr_info.at(i).rows_;
      if (sum_rows > num_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sum rows unexpected", K(ret), K(sum_rows), K(num_rows), K(i), K(curr_info.count()));
      } else {
        int64_t start_idx = curr_info.at(i).offset_;
        int64_t end_idx = curr_info.at(i).offset_ + curr_info.at(i).rows_;
        if (curr_info.at(i).skipped_) {
          filter_rg_bitmap->set_all(start_idx, end_idx);
        } else {
          filter_rg_bitmap->unset_all(start_idx, end_idx);
        }
      }
    }
    if (OB_SUCC(ret) && sum_rows != num_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sum rows unexpected", K(ret), K(sum_rows), K(num_rows), K(curr_info.count()));
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::generate_default_bitmap(const int64_t num_rows,
                                                           ObBitVector *&filter_rg_bitmap,
                                                           ObIAllocator &tmp_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_rg_bitmap = to_bit_vector(tmp_alloc.alloc(ObBitVector::memory_size(num_rows))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc rg bitmap", K(ret), K(num_rows));
  } else {
    filter_rg_bitmap->reset(num_rows);
  }
  return ret;
}

int ObExternalTablePushdownFilter::build_skipping_filter_nodes(sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  if (filter.is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = filter.get_childs();
    for (int64_t i = 0; OB_SUCC(ret) && i < filter.get_child_count(); ++i) {
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr filter", K(ret));
      } else if (OB_FAIL(build_skipping_filter_nodes(*children[i]))) {
        LOG_WARN("Fail to traverse filter tree", K(ret), K(i), KP(children[i]));
      }
    }
  } else if (OB_FAIL(extract_skipping_filter_from_tree(filter))) {
    LOG_WARN("Fail to extract physical operator from tree", K(ret));
  }
  return ret;
}

int ObExternalTablePushdownFilter::extract_skipping_filter_from_tree(
    sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  sql::ObPhysicalFilterExecutor &physical_filter =
    static_cast<sql::ObPhysicalFilterExecutor &>(filter);
  if (physical_filter.is_filter_white_node() ||
      static_cast<sql::ObBlackFilterExecutor &>(physical_filter).is_monotonic()) {
    const uint64_t column_id = physical_filter.get_col_ids().at(0);
    int64_t index = -1;
    for (int64_t i = 0; i < column_ids_->count() ; ++i) {
      if (column_id == column_ids_->at(i)) {
        index = i;
        break;
      }
    }
    if (OB_UNLIKELY(index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column not found", K(column_id));
    } else {
      ObSkippingFilterNode node;
      if (OB_FAIL(ObSSTableIndexFilterExtracter::extract_skipping_filter(
          physical_filter, blocksstable::ObSkipIndexType::MIN_MAX, node))) {
        LOG_WARN("Fail to extract index skipping filter", K(ret));
      } else if (node.is_useful()) {
        node.filter_ = &physical_filter;
        if (OB_FAIL(skipping_filter_nodes_.push_back(node))) {
          LOG_WARN("Fail to push back skipping filter node", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::build_filter_expr_rels(
    sql::ObPushdownFilterExecutor *root,
    const ObExternalTableRowIterator *row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root) || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(root), KP(row_iter));
  } else if (OB_FAIL(is_eager_column_.prepare_allocate(row_iter->file_column_exprs_.count()))) {
    LOG_WARN("fail to prepare allocate array", K(ret));
  } else if (OB_FAIL(is_dup_project_.prepare_allocate(row_iter->file_column_exprs_.count()))) {
    LOG_WARN("fail to prepare allocate array", K(ret));
  } else if (OB_FAIL(filter_expr_rels_.create(64, ObMemAttr(MTL_ID(), "ExtTblFtPD")))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < is_eager_column_.count(); ++i) {
      is_eager_column_.at(i) = false;
      is_dup_project_.at(i) = false;
    }
    if (OB_FAIL(build_filter_expr_rels_recursive(root, row_iter))) {
      LOG_WARN("fail to build filter expr rels", K(ret));
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::build_filter_expr_rels_recursive(
    sql::ObPushdownFilterExecutor *filter,
    const ObExternalTableRowIterator *row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (filter->is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = filter->get_childs();
    for (int64_t i = 0; OB_SUCC(ret) && i < filter->get_child_count(); i++) {
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null child filter", K(ret));
      } else if (OB_FAIL(build_filter_expr_rels_recursive(children[i], row_iter))) {
        LOG_WARN("Fail to build filter expr rels", K(ret), K(i), KP(children[i]));
      }
    }
  } else if (filter->is_filter_node()) {
    if (OB_UNLIKELY(!filter->is_filter_black_node())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported non-black filter in external table", K(ret), K(filter));
    } else {
      sql::ObBlackFilterExecutor *black_filter =
        static_cast<sql::ObBlackFilterExecutor *>(filter);
      const common::ObIArray<uint64_t> &col_ids = black_filter->get_col_ids();
      const common::ObIArray<ObExpr*> *col_exprs = black_filter->get_cg_col_exprs();
      if (OB_ISNULL(col_exprs) || col_ids.count() != col_exprs->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null col exprs or col ids", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
          const uint64_t col_id = col_ids.at(i);
          const ObExpr *col_expr = col_exprs->at(i);
          if (OB_FAIL(build_filter_expr_rel(col_id, col_expr, row_iter))) {
            LOG_WARN("fail to build filter expr rel", K(ret), K(i), K(col_id), KP(col_expr));
          }
        }
      }
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::build_filter_expr_rel(
    const uint64_t col_id, const ObExpr *col_expr, const ObExternalTableRowIterator *row_iter)
{
  int ret = OB_SUCCESS;
  FilterExprRel rel;
  if (OB_ISNULL(col_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_SUCCESS == filter_expr_rels_.get_refactored(col_id, rel)) {
    // the record already exists, skip
  } else {
    const ObExpr *column_expr = col_expr;
    const ObExpr *column_conv_expr = nullptr;
    // step 1: find the index of column_expr in column_exprs
    int64_t index = -1;
    bool is_file_meta_column = false;
    if (col_expr == row_iter->line_number_expr_ || col_expr == row_iter->file_id_expr_) {
      is_file_meta_column = true;
    } else {
      for (int64_t i = 0; i < row_iter->column_exprs_.count(); ++i) {
        if (row_iter->column_exprs_.at(i) == column_expr) {
          index = i;
          break;
        }
      }
      if (OB_UNLIKELY(index < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file column expr not found", K(ret));
      } else {
        column_conv_expr = row_iter->scan_param_->ext_column_dependent_exprs_->at(index);
      }
    }
    if (OB_SUCC(ret)) {
      // step 2: find the file column expr in column convert.
      int64_t file_col_expr_index = -1;
      if (!is_file_meta_column && OB_FAIL(find_ext_tbl_expr_index(column_conv_expr,
          row_iter->file_column_exprs_, row_iter->file_meta_column_exprs_, file_col_expr_index,
          is_file_meta_column))) {
        LOG_WARN("fail to find ext tbl expr index", K(ret), K(col_id), K(col_expr));
      } else {
        // step 3: insert filter relation item to hash map
        rel.column_expr_ = column_expr;
        rel.column_conv_expr_ = column_conv_expr;
        rel.file_col_expr_index_ = file_col_expr_index;
        rel.is_file_meta_column_ = is_file_meta_column;
        if (!rel.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid filter expr rel", K(ret), K(col_id), K(col_expr));
        } else if (OB_FAIL(filter_expr_rels_.set_refactored(col_id, rel))) {
          LOG_WARN("fail to set filter expr rel", K(ret), K(col_id));
        } else if (!is_file_meta_column) {
          // update eager column flag
          is_eager_column_.at(file_col_expr_index) = true;
          is_dup_project_.at(file_col_expr_index) = row_iter->column_sel_mask_.at(index);
        }
      }
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::find_ext_tbl_expr_index(
    const ObExpr *expr,
    const common::ObIArray<ObExpr*> &file_column_exprs,
    const common::ObIArray<ObExpr*> &file_meta_column_exprs,
    int64_t &file_col_expr_index,
    bool &is_file_meta_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL ||
             expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL ||
             expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
    is_file_meta_column = (expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL ||
                           expr->type_ == T_PSEUDO_PARTITION_LIST_COL);
    int64_t col_expr_index = -1;
    const common::ObIArray<ObExpr*> *file_column_exprs_array =
      is_file_meta_column ? &file_meta_column_exprs : &file_column_exprs;
    for (int64_t i = 0; i < file_column_exprs_array->count(); ++i) {
      if (file_column_exprs_array->at(i) == expr) {
        col_expr_index = i;
        break;
      }
    }
    if (OB_UNLIKELY(col_expr_index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file column expr not found", K(ret), K(expr));
    } else if (OB_UNLIKELY(file_col_expr_index >= 0)) {
      // the file column expr index already exists, check if it is the same column
      if (OB_UNLIKELY(col_expr_index != file_col_expr_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file column expr index mismatch", K(ret), K(expr), K(col_expr_index),
                                                        K(file_col_expr_index));
      }
    } else {
      file_col_expr_index = col_expr_index;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->arg_cnt_; ++i) {
      if (OB_FAIL(find_ext_tbl_expr_index(expr->args_[i], file_column_exprs,
                                          file_meta_column_exprs, file_col_expr_index,
                                          is_file_meta_column))) {
        LOG_WARN("fail to find ext tbl expr index", K(ret), K(i), K(expr));
      }
    }
  }
  return ret;
}



int ObExternalTablePushdownFilter::gather_eager_exprs(
                            const common::ObIArray<std::pair<uint64_t, uint64_t>> &mapping_col_ids,
                            sql::ObPushdownFilterExecutor *root_filter)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && nullptr != root_filter) {
    if (root_filter->is_filter_node()) {
      const common::ObIArray<uint64_t> &column_ids = root_filter->get_col_ids();
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        bool is_matched = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < mapping_col_ids.count() && !is_matched; ++j) {
          if (column_ids.at(i) == mapping_col_ids.at(j).first) {
            if (!has_exist_in_array(eager_columns_, static_cast<uint64_t> (j))) {
              OZ (eager_columns_.push_back(j));
            }
            is_matched = true;
          }
        }
        if (!is_matched) {
          ret = OB_SEARCH_NOT_FOUND;
          LOG_WARN("get lazy filter", K(ret), K(column_ids.at(i)), K(mapping_col_ids), K(eager_columns_));
        }
      }
    } else if (root_filter->is_logic_op_node()) {
      sql::ObPushdownFilterExecutor **children = root_filter->get_childs();
      for (uint32_t i = 0; OB_SUCC(ret) && i < root_filter->get_child_count(); i++) {
        if (OB_ISNULL(children[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child filter", K(ret));
        } else if (OB_FAIL(gather_eager_exprs(mapping_col_ids, children[i]))) {
          LOG_WARN("Fail to gather exprs", K(ret), K(i), KP(children[i]));
        }
      }
    }
  }
  return ret;
}

int ObExternalTablePushdownFilter::generate_lazy_exprs(const ObIArray<std::pair<uint64_t, uint64_t>> &mapping_col_ids,
                                                       const common::ObIArray<ObExpr *> &column_exprs,
                                                       const ObIArray<bool> &column_sel_mask)
{
  int ret = OB_SUCCESS;
  CK (column_sel_mask.count() == column_exprs.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    if (column_sel_mask.at(i)) {
      bool is_found = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < mapping_col_ids.count(); ++j) {
        if (mapping_col_ids.at(j).second == i) {
          OZ (lazy_columns_.push_back(j));
          is_found = true;
          break;
        }
      }
      if (!is_found) {
        ret = OB_SEARCH_NOT_FOUND;
        LOG_WARN("get lazy filter", K(ret));
      }
    }
  }
  LOG_TRACE("print lazy exprs", K(lazy_columns_), K(mapping_col_ids), K(column_sel_mask));
  return ret;
}


}
}
