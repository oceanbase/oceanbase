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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_iceberg_file_filter.h"
#include "common/ob_smart_call.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/table_format/iceberg/scan/conversions.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

OB_SERIALIZE_MEMBER(PartColDesc, spec_id_, column_id_, offset_);

// ObIcebergFileFilter::ObIcebergFileFilter(ObIAllocator &allocator,
//                                          ObExecContext &exec_ctx,
//                                          ObFileFilterSpec &file_filter_spec)
// : is_inited_(false),
//   allocator_(allocator),
//   exec_ctx_(exec_ctx),
//   file_filter_spec_(file_filter_spec),
//   eval_ctx_(nullptr),
//   pd_expr_op_(nullptr),
//   ori_frames_(exec_ctx.get_frames()),
//   ori_frame_cnt_(exec_ctx.get_frame_cnt()),
//   column_ids_(nullptr),
//   column_metas_(nullptr),
//   identity_part_columns_(nullptr),
//   pushdown_filter_(nullptr),
//   skipping_filter_nodes_(),
//   skip_filter_executor_(),
//   buf_(),
//   skip_bit_(nullptr)
// {
//   skipping_filter_nodes_.set_attr(ObMemAttr(MTL_ID(), "FileFilters"));
// }

// ObIcebergFileFilter::~ObIcebergFileFilter()
// {
//   exec_ctx_.set_frames(ori_frames_);
//   exec_ctx_.set_frame_cnt(ori_frame_cnt_);
// }

// int ObIcebergFileFilter::generate_pd_filter_spec(ObIAllocator &allocator,
//                                                  ObExecContext &exec_ctx,
//                                                  const ObDMLStmt *stmt,
//                                                  const ObIArray<ObRawExpr*> &filter_exprs,
//                                                  ObFileFilterSpec &file_filter_sepc)
// {
//   int ret = OB_SUCCESS;
//   ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
//   if (OB_ISNULL(plan_ctx)) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("get unexpected null");
//   } else {
//     ObRawExprFactory expr_factory(allocator);
//     ObRawExprCopier copier(expr_factory);
//     ObSEArray<ObRawFilterMonotonicity,4> filter_monotonicity;
//     ObSEArray<ObRawExpr*, 4> copied_filters;
//     ObSEArray<ObRawExpr*, 4> assist_exprs;
//     ObRawExprUniqueSet unique_exprs(/*need_unique=*/false);
//     void *ptr = nullptr;
//     ObStaticEngineExprCG expr_cg(allocator,
//                                  exec_ctx.get_my_session(),
//                                  exec_ctx.get_sql_ctx()->schema_guard_,
//                                  plan_ctx->get_original_param_cnt(),
//                                  plan_ctx->get_param_store().count(),
//                                  exec_ctx.get_min_cluster_version());
//     ObStaticEngineCG static_engin_cg(exec_ctx.get_min_cluster_version());
//     expr_cg.set_batch_size(1);

//     if (OB_FAIL(copier.copy(filter_exprs, copied_filters))) {
//       LOG_WARN("failed to copy on replace repart expr", K(ret));
//     } else if (OB_FAIL(ObLogTableScan::generate_filter_monotonicity(stmt,
//                                                                     &exec_ctx,
//                                                                     &plan_ctx->get_param_store(),
//                                                                     expr_factory,
//                                                                     allocator,
//                                                                     copied_filters,
//                                                                     filter_monotonicity,
//                                                                     NULL))) {
//       LOG_WARN("failed to generate filter monotonicity");
//     } else if (OB_FAIL(ObLogTableScan::get_filter_assist_exprs(filter_monotonicity, assist_exprs))) {
//       LOG_WARN("failed to get filter assist exprs");
//     } else if (OB_FAIL(unique_exprs.append(copied_filters))) {
//       LOG_WARN("failed to append exprs");
//     } else if (OB_FAIL(unique_exprs.append(assist_exprs))) {
//       LOG_WARN("failed to append exprs");
//     } else if (OB_FAIL(expr_cg.generate(unique_exprs, file_filter_sepc.expr_frame_info_))) {
//       LOG_WARN("fail to generate expr");
//     } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObPushdownExprSpec)))) {
//       ret = OB_ALLOCATE_MEMORY_FAILED;
//       LOG_WARN("failed to allocator memory for ObPushdownExprSpec");
//     } else {
//       file_filter_sepc.pd_expr_spec_ = new(ptr) ObPushdownExprSpec(allocator);
//       ObPushdownFilterConstructor filter_constructor(&allocator, static_engin_cg,
//                                                      &filter_monotonicity,
//                                                      /*use_column_store=*/false,
//                                                      /*enable_semistruct_pushdown=*/false);
//       file_filter_sepc.pd_expr_spec_->pd_storage_flag_.set_filter_pushdown(true);
//       file_filter_sepc.pd_expr_spec_->max_batch_size_ = 1;
//       if (OB_FAIL(filter_constructor.apply(copied_filters,
//                                            file_filter_sepc.pd_expr_spec_->pd_storage_filters_.get_pushdown_filter()))) {
//         LOG_WARN("failed to apply filter constructor");
//       }
//     }
//   }
//   return ret;
// }

// int ObIcebergFileFilter::init(ObIArray<int64_t> &column_ids,
//                               ObIArray<ObObjMeta> &column_metas,
//                               ObIArray<PartColDesc> &part_column_descs)
// {
//   int ret = OB_SUCCESS;
//   bool is_valid = true;
//   if (IS_INIT) {
//     ret = OB_INIT_TWICE;
//     LOG_WARN("init ObIcebergFileFilter twice", K_(is_inited));
//   } else {
//     column_ids_ = &column_ids;
//     column_metas_ = &column_metas;
//     identity_part_columns_ = &part_column_descs;
//     if (OB_FAIL(generate_pd_filter())) {
//       LOG_WARN("failed to generate pd filter");
//     } else if (OB_ISNULL(pushdown_filter_)) {
//       // do nothing
//     } else if (OB_FAIL(pushdown_filter_->init_evaluated_datums(is_valid))) {
//       LOG_WARN("failed to init evaluated datums");
//     } else if (!is_valid) {
//       // do nothing
//     } else if (OB_FAIL(build_skipping_filter_nodes(*pushdown_filter_, part_column_descs))) {
//       LOG_WARN("failed to build_skipping filter nodes");
//     } else if (OB_FAIL(skip_filter_executor_.init(MAX(1, pushdown_filter_->get_op().get_batch_size()), &allocator_))) {
//       LOG_WARN("failed to init skip filter executor");
//     } else {
//       buf_[0] = 0;
//       skip_bit_ = to_bit_vector(buf_);
//       skip_bit_->init(1);
//       is_inited_ = true;
//     }
//   }
//   return ret;
// }

// int ObIcebergFileFilter::generate_pd_filter()
// {
//   int ret = OB_SUCCESS;
//   if (OB_ISNULL(file_filter_spec_.pd_expr_spec_)) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("get null pd expr spec");
//   } else if (OB_FAIL(file_filter_spec_.expr_frame_info_.pre_alloc_exec_memory(exec_ctx_, &allocator_))) {
//     LOG_WARN("fail to pre allocate memory", K(file_filter_spec_.expr_frame_info_));
//   } else if (OB_ISNULL(eval_ctx_ = static_cast<ObEvalCtx*>(allocator_.alloc(sizeof(ObEvalCtx))))) {
//     ret = OB_ALLOCATE_MEMORY_FAILED;
//     LOG_WARN("failed to allocator memory for ObEvalCtx");
//   } else if (OB_ISNULL(pd_expr_op_ = static_cast<ObPushdownOperator*>(allocator_.alloc(sizeof(ObPushdownOperator))))) {
//     ret = OB_ALLOCATE_MEMORY_FAILED;
//     LOG_WARN("failed to allocator memory for ObPushdownOperator");
//   } else {
//     eval_ctx_ = new(eval_ctx_) ObEvalCtx(exec_ctx_, &allocator_);
//     pd_expr_op_ = new(pd_expr_op_) ObPushdownOperator(*eval_ctx_, *file_filter_spec_.pd_expr_spec_);
//     if (OB_FAIL(pd_expr_op_->init_pushdown_storage_filter())) {
//       LOG_WARN("failed to init pushdown storage filter failed");
//     } else {
//       pushdown_filter_ = pd_expr_op_->pd_storage_filters_;
//     }
//   }
//   return ret;
// }

// int ObIcebergFileFilter::build_skipping_filter_nodes(ObPushdownFilterExecutor &filter,
//                                                      const ObIArray<PartColDesc> &part_column_descs)
// {
//   int ret = OB_SUCCESS;
//   if (filter.is_truncate_node()) {
//   } else if (filter.is_logic_op_node()) {
//     sql::ObPushdownFilterExecutor **children = filter.get_childs();
//     for (int64_t i = 0; OB_SUCC(ret) && i < filter.get_child_count(); ++i) {
//       if (OB_ISNULL(children[i])) {
//         ret = OB_ERR_UNEXPECTED;
//         LOG_WARN("get unexpected pushdown fitler");
//       } else if (OB_FAIL(build_skipping_filter_nodes(*children[i], part_column_descs))) {
//         LOG_WARN("failed to build skiping filter nodes", K(i), KP(children[i]));
//       }
//     }
//   } else if (OB_FAIL(extract_skipping_filter_from_tree(filter, part_column_descs))) {
//     LOG_WARN("failed to extract skipping filter from tree");
//   }
//   return ret;
// }

// int ObIcebergFileFilter::extract_skipping_filter_from_tree(ObPushdownFilterExecutor &filter,
//                                                            const ObIArray<PartColDesc> &part_column_descs)
// {
//   int ret = OB_SUCCESS;
//   ObPhysicalFilterExecutor &physical_filter = static_cast<ObPhysicalFilterExecutor &>(filter);
//   bool is_valid = false;
//   bool is_min_max_filter = false;
//   uint64_t column_id = -1;
//   if (physical_filter.is_filter_white_node() ||
//       static_cast<sql::ObBlackFilterExecutor &>(physical_filter).is_monotonic()) {
//     column_id = filter.get_col_ids().at(0);
//     is_valid = true;
//     is_min_max_filter = true;
//   } else if (filter.get_col_ids().count() == 1) {
//     column_id = filter.get_col_ids().at(0);
//     for (int64_t i = 0; !is_valid && i < part_column_descs.count(); ++i) {
//       if (column_id == part_column_descs.at(i).column_id_) {
//         is_valid = true;
//         is_min_max_filter = false;
//       }
//     }
//   }

//   if (OB_SUCC(ret) && is_valid) {
//     int64_t index = -1;
//     if (OB_UNLIKELY(!ObOptimizerUtil::find_item(*column_ids_, column_id, &index))) {
//       ret = OB_ERR_UNEXPECTED;
//       LOG_WARN("get unexpected column id", K(column_id));
//     } else if (OB_FAIL(skipping_filter_nodes_.push_back(
//                 ObFileFilterNode(index, is_min_max_filter, &physical_filter)))) {
//       LOG_WARN("failed to push back file filter node");
//     }
//   }
//   return ret;
// }

// int ObIcebergFileFilter::check_file(iceberg::ManifestEntry &manifest_entry,
//                                     bool &is_filtered)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY(skipping_filter_nodes_.empty())) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("get unexpected skipping filter nodes", K(skipping_filter_nodes_.count()));
//   } else {
//     bool need_check = false;
//     for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
//       ObFileFilterNode &node = skipping_filter_nodes_.at(i);
//       if (OB_FAIL(is_filtered_by_skipping_index(manifest_entry, node))) {
//         LOG_WARN("Fail to do filter by skipping index");
//       } else {
//         need_check = (need_check || node.filter_->is_filter_constant());
//       }
//     }
//     if (OB_SUCC(ret) && need_check) {
//       ObBoolMask bm;
//       if (OB_FAIL(pushdown_filter_->execute_skipping_filter(bm))) {
//         LOG_WARN("Fail to execute skipping filter", K(ret), KP_(pushdown_filter));
//       } else {
//         is_filtered = bm.is_always_false();
//         // Recover ObBoolMask of the filter.
//         for (int64_t i = 0; OB_SUCC(ret) && i < skipping_filter_nodes_.count(); ++i) {
//           ObFileFilterNode &node = skipping_filter_nodes_[i];
//           if (node.filter_->is_filter_constant()) {
//             node.filter_->set_filter_uncertain();
//           }
//         }
//       }
//     }
//   }
//   return ret;
// }

// int ObIcebergFileFilter::is_filtered_by_skipping_index(iceberg::ManifestEntry &manifest_entry,
//                                                        ObFileFilterNode &node)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY(nullptr == node.filter_ || 1 != node.filter_->get_col_ids().count())) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("Unexpected filter in skipping filter node", K(ret), KPC_(node.filter));
//   } else if (node.is_min_max_filter_) {
//     const uint32_t col_offset = node.col_idx_;
//     const uint64_t column_id = column_ids_->at(col_offset);
//     const ObObjMeta obj_meta = column_metas_->at(col_offset);
//     const int64_t row_count = INT64_MAX;
//     bool can_use_min_max = true;
//     ObMinMaxFilterParam param;

//     if (OB_FAIL(construnct_min_max_param(param, manifest_entry, column_id, can_use_min_max))) {
//       LOG_WARN("failed to construnct min max param");
//     } else if (OB_FAIL(skip_filter_executor_.falsifiable_pushdown_filter(column_id,
//                                                                          blocksstable::ObSkipIndexType::MIN_MAX,
//                                                                          row_count,
//                                                                          param,
//                                                                          *node.filter_,
//                                                                          true))) {
//         LOG_WARN("Fail to falsifiable pushdown filter", K(ret), K(node.filter_));
//     }
//   } else {
//     blocksstable::ObStorageDatum datum;
//     bool filtered;
//     const uint64_t column_id = column_ids_->at(node.col_idx_);
//     bool is_part_col = false;
//     int64_t offset = -1;
//     for (int64_t i = 0; !is_part_col && i < identity_part_columns_->count(); ++i) {
//       PartColDesc &desc = identity_part_columns_->at(i);
//       if (desc.spec_id_ == manifest_entry.data_file.spec_id && desc.column_id_ == column_id) {
//         is_part_col = true;
//         offset = desc.offset_;
//       }
//     }
//     if (!is_part_col) {
//       // do nothing
//     } else if (OB_UNLIKELY(offset <0 || offset > manifest_entry.data_file.partition.part_value.count())) {
//       ret = OB_ERR_UNEXPECTED;
//       LOG_WARN("get unexpected offset");
//     // TODO binary to obj
//     // } else if (OB_FAIL(datum.from_obj_enhance(binary_to_obj(data_file.partition.part_value.at(offset))))) {
//     //   LOG_WARN("failed to init datum from obj");
//     } else if (OB_FAIL(node.filter_->filter(&datum, 1, *skip_bit_, filtered))) {
//       LOG_WARN("failed to filter black filter");
//     } else if (filtered) {
//       node.filter_->get_filter_bool_mask().set_always_false();
//     }
//   }
//   return ret;
// }

// int ObIcebergFileFilter::construnct_min_max_param(ObMinMaxFilterParam &param,
//                                                   iceberg::ManifestEntry &manifest_entry,
//                                                   const uint64_t column_id,
//                                                   bool &can_use_min_max)
// {
//   int ret = OB_SUCCESS;
//   int64_t null_count = 0;
//   ObString upper_bound;
//   ObString lower_bound;
//   ObObj null_value;
//   ObObj min_value;
//   ObObj max_value;
//   if (iceberg::DataFileUtil::get_count(manifest_entry.data_file.null_value_counts, column_id, null_count)) {
//     null_value.set_int(null_count);
//   } else {
//     can_use_min_max = false;
//   }
//   if (OB_SUCC(ret) && can_use_min_max) {
//     if (iceberg::DataFileUtil::get_bound(manifest_entry.data_file.upper_bounds, column_id, upper_bound)) {
//       // TODO binary to obj
//       // max_value = binary_to_obj(upper_bound);
//     } else {
//       can_use_min_max = false;
//     }
//   }
//   if (OB_SUCC(ret) && can_use_min_max) {
//     if (iceberg::DataFileUtil::get_bound(manifest_entry.data_file.lower_bounds, column_id, lower_bound)) {
//       // TODO binary to obj
//       // min_value = binary_to_obj(lower_bound);
//     } else {
//       can_use_min_max = false;
//     }
//   }
//   if (OB_SUCC(ret) && can_use_min_max) {
//     if (OB_FAIL(param.null_count_.from_obj_enhance(null_value))) {
//       LOG_WARN("failed to form obj enhance");
//     } else if (OB_FAIL(param.min_datum_.from_obj_enhance(min_value))) {
//       LOG_WARN("failed to form obj enhance");
//     } else if (OB_FAIL(param.max_datum_.from_obj_enhance(max_value))) {
//       LOG_WARN("failed to form obj enhance");
//     } else {
//       param.is_min_prefix_ = false;
//       param.is_max_prefix_ = false;
//     }
//   }
//   return ret;
// }

int ObIcebergFileFilter::IcebergMinMaxFilterParamBuilder::build(const int32_t ext_tbl_col_id,
                                                                const ObColumnMeta &column_meta,
                                                                blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  param.set_uncertain();
  bool need_min_max = true;
  bool is_valid = true;
  int64_t null_count = 0;
  ObString upper_bound;
  ObString lower_bound;
  ObObj null_value;
  ObObj min_value;
  ObObj max_value;
  if (iceberg::ObIcebergUtils::get_map_value(manifest_entry_.data_file.null_value_counts,
                                             ext_tbl_col_id,
                                             null_count)) {
    null_value.set_int(null_count);
    if (null_count == manifest_entry_.data_file.record_count) {
      // all values are null
      need_min_max = false;
    }
  } else {
    is_valid = false;
  }
  if (OB_SUCC(ret) && need_min_max && is_valid) {
    if (iceberg::ObIcebergUtils::get_map_value(manifest_entry_.data_file.upper_bounds,
                                               ext_tbl_col_id,
                                               upper_bound)) {
      if (OB_FAIL(iceberg::Conversions::convert_statistics_binary_to_ob_obj(allocator_, upper_bound,
                                                                            column_meta, max_value))) {
        LOG_WARN("failed to convert statistic binart to obj");
      }
    } else {
      is_valid = false;
    }
  }
  if (OB_SUCC(ret) && need_min_max && is_valid) {
    if (iceberg::ObIcebergUtils::get_map_value(manifest_entry_.data_file.lower_bounds,
                                               ext_tbl_col_id,
                                               lower_bound)) {
      if (OB_FAIL(iceberg::Conversions::convert_statistics_binary_to_ob_obj(allocator_, lower_bound,
                                                                            column_meta, min_value))) {
        LOG_WARN("failed to convert statistic binart to obj");
      }
    } else {
      is_valid = false;
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(param.null_count_.from_obj_enhance(null_value))) {
      LOG_WARN("failed to form obj enhance");
    } else if (need_min_max && OB_FAIL(param.min_datum_.from_obj_enhance(min_value))) {
      LOG_WARN("failed to form obj enhance");
    } else if (need_min_max && OB_FAIL(param.max_datum_.from_obj_enhance(max_value))) {
      LOG_WARN("failed to form obj enhance");
    } else {
      param.is_min_prefix_ = false;
      param.is_max_prefix_ = false;
    }
  }
  LOG_TRACE("print min/max info", K(column_meta), K(param.min_datum_), K(param.max_datum_));
  return ret;
}

int ObIcebergFileFilter::check_file(iceberg::ManifestEntry &manifest_entry,
                                    bool &is_filtered)
{
  int ret = OB_SUCCESS;
  IcebergMinMaxFilterParamBuilder param_builder(manifest_entry, temp_allocator_);
  if (OB_FAIL(apply_skipping_index_filter(ObExternalTablePushdownFilter::PushdownLevel::FILE,
                                          param_builder,
                                          is_filtered,
                                          manifest_entry.data_file.record_count))) {
    LOG_WARN("fail to apply skipping index filter", K(ret));
  }
  // free temp allocator after filtered
  temp_allocator_.reset();
  return ret;
}
