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

#include "ob_i_lake_table_file_pruner.h"

#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

OB_DEF_SERIALIZE(ObFieldBound)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, is_valid_range_, contains_null_, include_lower_,
              include_upper_, lower_bound_, upper_bound_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObFieldBound)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, is_valid_range_, contains_null_, include_lower_,
              include_upper_, lower_bound_, upper_bound_);
  return len;
}

OB_DEF_DESERIALIZE(ObFieldBound)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, is_valid_range_, contains_null_, include_lower_,
              include_upper_, lower_bound_, upper_bound_);
  return ret;
}

ObFieldBound::ObFieldBound()
: is_valid_range_(true),
  contains_null_(false),
  include_lower_(false),
  include_upper_(false),
  lower_bound_(),
  upper_bound_()
{
  lower_bound_.set_min_value();
  upper_bound_.set_max_value();
}

int ObFieldBound::deep_copy(common::ObIAllocator &allocator, ObFieldBound &src)
{
  int ret = OB_SUCCESS;
  is_valid_range_ = src.is_valid_range_;
  contains_null_ = src.contains_null_;
  include_lower_ = src.include_lower_;
  include_upper_ = src.include_upper_;
  if (OB_FAIL(ob_write_obj(allocator, src.lower_bound_, lower_bound_))) {
    LOG_WARN("failed to write obj");
  } else if (OB_FAIL(ob_write_obj(allocator, src.upper_bound_, upper_bound_))) {
    LOG_WARN("failed to write obj");
  }
  return ret;
}

int ObFieldBound::from_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range.start_key_.get_obj_cnt() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range count", K(range.start_key_.get_obj_cnt()));
  } else {
    ObObj &start_obj = range.start_key_.get_obj_ptr()[0];
    ObObj &end_obj = range.end_key_.get_obj_ptr()[0];
    if (start_obj.is_null()) {
      if (lib::is_mysql_mode()) {
        // null first {min, null, 1, max}
        lower_bound_.set_min_value();
      } else {
        // null last {min, 1, null, max}
        is_valid_range_ = false;
      }
      if (range.border_flag_.inclusive_start()) {
        contains_null_ = true;
      }
    } else {
      lower_bound_ = start_obj;
      include_lower_ = range.border_flag_.inclusive_start();
    }
    if (end_obj.is_null()) {
      if (lib::is_mysql_mode()) {
        // null first {min, null, 1, max}
        is_valid_range_ = false;
      } else {
        // null last {min, 1, null, max}
        upper_bound_.set_max_value();
      }
      if (range.border_flag_.inclusive_end()) {
        contains_null_ = true;
      }
    } else {
      upper_bound_ = end_obj;
      include_upper_ = range.border_flag_.inclusive_end();
    }
  }
  return ret;
}


int ObFieldBound::from_data_file_partition(ObIArray<ObObj> &partition, int64_t offset)
{
  int ret = OB_SUCCESS;
  ObObj partition_value = partition.at(offset);
  contains_null_ = partition_value.is_null();
  if (contains_null_) {
    is_valid_range_ = false;
  } else {
    include_lower_ = true;
    lower_bound_ = partition_value;
    include_upper_ = true;
    upper_bound_ = partition_value;
  }
  return ret;
}

bool ObFieldBound::is_intersect(const ObFieldBound &r_bound)
{
  bool intersect = false;
  if (contains_null_ && r_bound.contains_null_) {
    intersect = true;
  } else if (is_valid_range_ && r_bound.is_valid_range_) {
    int cmp = -1;
    if (upper_bound_.is_max_value()) {
      cmp = 1;
    } else {
      cmp = upper_bound_.compare(r_bound.lower_bound_);
      if (0 == cmp) {
        if (!include_upper_ || !r_bound.include_lower_) {
          cmp = -1;
        }
      }
    }
    if (cmp < 0) {
      // do nothing
    } else if (r_bound.upper_bound_.is_max_value()) {
      cmp = 1;
    } else {
      cmp = r_bound.upper_bound_.compare(lower_bound_);
      if (0 == cmp) {
        if (!include_lower_ || !r_bound.include_upper_) {
          cmp = -1;
        }
      }
    }
    intersect = (cmp >= 0);
  }
  return intersect;
}

ObILakeTableFilePruner::ObILakeTableFilePruner(common::ObIAllocator &allocator, PrunnerType type)
    : is_partitioned_(true), inited_(false), need_all_(false), allocator_(allocator),
      loc_meta_(allocator_), column_ids_(allocator_), column_metas_(allocator_),
      file_filter_spec_(allocator_), type_(type), partition_values_(allocator_)
{
}

int ObILakeTableFilePruner::assign(const ObILakeTableFilePruner &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    inited_ = other.inited_;
    is_partitioned_ = other.is_partitioned_;
    need_all_ = other.need_all_;
    type_ = other.type_;
    if (OB_FAIL(loc_meta_.assign(other.loc_meta_))) {
      LOG_WARN("assign loc meta failed", K(ret), K(other.loc_meta_));
    } else if (OB_FAIL(column_ids_.assign(other.column_ids_))) {
      LOG_WARN("failed to assign column ids");
    } else if (OB_FAIL(column_metas_.assign(other.column_metas_))) {
      LOG_WARN("failed to assign column metas");
    } else if (OB_FAIL(file_filter_spec_.deep_copy(allocator_, other.file_filter_spec_))) {
      LOG_WARN("failed to deep copy file filter spec");
    } else if (OB_FAIL(partition_values_.assign(other.partition_values_))) {
      LOG_WARN("failed to assign partition values");
    }
  }
  if (OB_FAIL(ret)) {
    inited_ = false;
  }
  return ret;
}

void ObILakeTableFilePruner::reset()
{
  inited_ = false;
  is_partitioned_ = true;
  need_all_ = false;
  loc_meta_.reset();
  column_ids_.reset();
  column_metas_.reset();
  partition_values_.reset();
}

int ObILakeTableFilePruner::generate_column_meta_info(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 4> column_items;
  if (OB_FAIL(stmt.get_column_items(loc_meta_.table_loc_id_, column_items))) {
  } else if (OB_FAIL(column_ids_.init(column_items.count()))) {
    LOG_WARN("failed to init fixed array");
  } else if (OB_FAIL(column_metas_.prepare_allocate(column_items.count()))) {
    LOG_WARN("failed to init fixed array");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      ColumnItem &item = column_items.at(i);
      if (OB_ISNULL(item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column expr");
      } else if (OB_FAIL(column_ids_.push_back(item.column_id_))) {
        LOG_WARN("failed to push back column id");
      } else if (OB_FAIL(column_metas_.at(i).from_ob_raw_expr_res_type(
                     item.expr_->get_result_type()))) {
        LOG_WARN("failed to init column meta from ob raw expr res type");
      }
    }
  }
  return ret;
}

ObTempFrameInfoCtxReplaceGuard::ObTempFrameInfoCtxReplaceGuard(ObExecContext &exec_ctx)
    : exec_ctx_(exec_ctx), frames_(exec_ctx.get_frames()), frame_cnt_(exec_ctx.get_frame_cnt()),
      expr_op_size_(exec_ctx.get_expr_op_size()),
      expr_op_ctx_store_(exec_ctx.get_expr_op_ctx_store())
{
}

ObTempFrameInfoCtxReplaceGuard::~ObTempFrameInfoCtxReplaceGuard()
{
  exec_ctx_.set_frames(frames_);
  exec_ctx_.set_frame_cnt(frame_cnt_);
  exec_ctx_.set_expr_op_size(expr_op_size_);
  exec_ctx_.set_expr_op_ctx_store(expr_op_ctx_store_);
}

OB_DEF_SERIALIZE(ObLakeTablePushDownFilterSpec)
{
  int ret = OB_SUCCESS;
  bool has_expr_spec = pd_expr_spec_ != nullptr;
  LST_DO_CODE(OB_UNIS_ENCODE, expr_frame_info_, has_expr_spec);
  if (OB_LIKELY(has_expr_spec)) {
    ObIArray<ObExpr> *seri_arr_bak = ObExpr::get_serialize_array();
    ObExpr::get_serialize_array() = &expr_frame_info_.rt_exprs_;
    OB_UNIS_ENCODE(*pd_expr_spec_);
    ObExpr::get_serialize_array() = seri_arr_bak;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLakeTablePushDownFilterSpec)
{
  int64_t len = 0;
  bool has_expr_spec = pd_expr_spec_ != nullptr;
  LST_DO_CODE(OB_UNIS_ADD_LEN, expr_frame_info_, has_expr_spec);
  if (OB_LIKELY(has_expr_spec)) {
    OB_UNIS_ADD_LEN(*pd_expr_spec_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObLakeTablePushDownFilterSpec)
{
  int ret = OB_SUCCESS;
  bool has_expr_spec = false;
  LST_DO_CODE(OB_UNIS_DECODE, expr_frame_info_, has_expr_spec);
  if (OB_LIKELY(has_expr_spec)) {
    ObIArray<ObExpr> *seri_arr_bak = ObExpr::get_serialize_array();
    ObExpr::get_serialize_array() = &expr_frame_info_.rt_exprs_;
    OB_UNIS_DECODE(*pd_expr_spec_);
    ObExpr::get_serialize_array() = seri_arr_bak;
  }
  return ret;
}

int ObLakeTablePushDownFilterSpec::deep_copy(ObIAllocator &allocator,
                                             const ObLakeTablePushDownFilterSpec &src)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_frame_info_.assign(src.expr_frame_info_, allocator))) {
    LOG_WARN("failed to assign expr frame info");
  } else if (OB_NOT_NULL(src.pd_expr_spec_)) {
    ObIArray<ObExpr> *seri_arr_bak = ObExpr::get_serialize_array();
    ObExpr::get_serialize_array() = &src.expr_frame_info_.rt_exprs_;
    // 暂时通过序列化+反序列化完成
    int64_t ser_len = src.pd_expr_spec_->get_serialize_size();
    void *ser_ptr = nullptr;
    void *spec_ptr = nullptr;
    int64_t ser_pos = 0;
    int64_t des_pos = 0;
    // TODO yibo 序列化数据的内存是不是可以用临时的
    if (OB_ISNULL(ser_ptr = allocator.alloc(ser_len))
        || OB_ISNULL(spec_ptr = allocator.alloc(sizeof(ObPushdownExprSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ser_len), KP(ser_ptr), KP(spec_ptr));
    } else if (OB_FAIL(
                   src.pd_expr_spec_->serialize(static_cast<char *>(ser_ptr), ser_len, ser_pos))) {
      LOG_WARN("failed to serialize ObPushdownExprSpec", KP(ser_ptr), K(ser_len), K(ser_pos));
    } else {
      pd_expr_spec_ = new (spec_ptr) ObPushdownExprSpec(allocator);
      ObExpr::get_serialize_array() = &expr_frame_info_.rt_exprs_;
      if (OB_FAIL(
              pd_expr_spec_->deserialize(static_cast<const char *>(ser_ptr), ser_pos, des_pos))) {
        LOG_WARN("failed to deserialize ObPushdownExprSpec", KP(ser_ptr), K(ser_pos), K(des_pos));
      } else if (OB_UNLIKELY(ser_pos != des_pos)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("data_len and pos mismatch", K(ser_len), K(ser_pos), K(des_pos));
      }
    }
    ObExpr::get_serialize_array() = seri_arr_bak;
  }
  return ret;
}

int ObLakeTablePushDownFilter::generate_pd_filter_spec(
    ObIAllocator &allocator,
    ObExecContext &exec_ctx,
    const ObDMLStmt *stmt,
    const ObIArray<ObRawExpr *> &filter_exprs,
    ObLakeTablePushDownFilterSpec &file_filter_sepc)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else {
    // TODO yibo expr_factory的结果是不用的，可以用临时内存
    ObRawExprFactory expr_factory(allocator);
    ObRawExprCopier copier(expr_factory);
    ObSEArray<ObRawFilterMonotonicity, 4> filter_monotonicity;
    ObSEArray<ObRawExpr *, 4> copied_filters;
    ObSEArray<ObRawExpr *, 4> assist_exprs;
    ObRawExprUniqueSet unique_exprs(/*need_unique=*/false);
    ObSEArray<ObExpr*, 4> calc_exprs;
    void *ptr = nullptr;
    ObStaticEngineExprCG expr_cg(allocator,
                                 exec_ctx.get_my_session(),
                                 exec_ctx.get_sql_ctx()->schema_guard_,
                                 plan_ctx->get_original_param_cnt(),
                                 plan_ctx->get_param_store().count(),
                                 exec_ctx.get_min_cluster_version());
    ObStaticEngineCG static_engin_cg(exec_ctx.get_min_cluster_version());

    if (OB_FAIL(copier.copy(filter_exprs, copied_filters))) {
      LOG_WARN("failed to copy on replace repart expr", K(ret));
    } else if (OB_FAIL(ObLogTableScan::generate_filter_monotonicity(stmt,
                                                                    &exec_ctx,
                                                                    &plan_ctx->get_param_store(),
                                                                    expr_factory,
                                                                    allocator,
                                                                    copied_filters,
                                                                    filter_monotonicity,
                                                                    NULL))) {
      LOG_WARN("failed to generate filter monotonicity");
    } else if (OB_FAIL(
                   ObLogTableScan::get_filter_assist_exprs(filter_monotonicity, assist_exprs))) {
      LOG_WARN("failed to get filter assist exprs");
    } else if (OB_FAIL(unique_exprs.append(copied_filters))) {
      LOG_WARN("failed to append exprs");
    } else if (OB_FAIL(unique_exprs.append(assist_exprs))) {
      LOG_WARN("failed to append exprs");
    } else if (OB_FAIL(expr_cg.generate(unique_exprs, file_filter_sepc.expr_frame_info_))) {
      LOG_WARN("fail to generate expr");
    } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObPushdownExprSpec)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator memory for ObPushdownExprSpec");
    } else {
      file_filter_sepc.pd_expr_spec_ = new (ptr) ObPushdownExprSpec(allocator);
      ObPushdownFilterConstructor filter_constructor(&allocator,
                                                     static_engin_cg,
                                                     &filter_monotonicity,
                                                     /*use_column_store=*/false,
                                                     /*table_type=*/share::schema::EXTERNAL_TABLE,
                                                     /*enable_semistruct_pushdown=*/false);
      file_filter_sepc.pd_expr_spec_->pd_storage_flag_.set_filter_pushdown(true);
      if (OB_FAIL(filter_constructor.apply(
              copied_filters,
              file_filter_sepc.pd_expr_spec_->pd_storage_filters_.get_pushdown_filter()))) {
        LOG_WARN("failed to apply filter constructor");
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < file_filter_sepc.expr_frame_info_.rt_exprs_.count(); ++i) {
          if (OB_FAIL(calc_exprs.push_back(&file_filter_sepc.expr_frame_info_.rt_exprs_.at(i)))) {
            LOG_WARN("failed to push back expr frame info");
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(file_filter_sepc.pd_expr_spec_->set_calc_exprs(calc_exprs, 0))) {
            LOG_WARN("failed to set calc exprs");
          }
        }
      }
    }
  }
  return ret;
}

int ObLakeTablePushDownFilter::normalization_column_id(uint64_t ob_column_id)
{
  // 在做pushdown的时候，需要根据 column id 找到对应的 column 信息
  // 比如：
  //   - hive 需要根据 column id 判断当前 column 是否是分区键，并获取对应的分区值
  //   - iceberg 需要根据 column id ， 从 manifest 里面获取对应的最大值、最小值
  // 因为 iceberg 的 manifest 里面的 column id 是原始的 column id （没有经过 ob 加上偏移量的）
  // （虽然 hive 其实可以不用）所以这里统一都用了原始 column id （后面 paimon 也需要用）
  return iceberg::ObIcebergUtils::get_iceberg_field_id(ob_column_id);
}


int ObLakeTablePushDownFilter::init(ObIArray<uint64_t> &column_ids,
                                    ObIArray<ObColumnMeta> &column_metas)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  mem_attr_ = ObMemAttr(MTL_ID(), "IbFileFilter");
  allocator_.set_attr(mem_attr_);
  temp_allocator_.set_attr(ObMemAttr(MTL_ID(), "IbFileFilterTmp"));

  if (OB_FAIL(generate_pd_filter())) {
    LOG_WARN("failed to generate pd filter");
  } else if (OB_ISNULL(pushdown_filter_)) {
    // do nothing
  } else if (OB_FAIL(pushdown_filter_->init_evaluated_datums(is_valid))) {
    LOG_WARN("failed to init evaluated datums");
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(ObExternalTablePushdownFilter::init(
                 pushdown_filter_,
                 ObExternalTablePushdownFilter::PushdownLevel::FILE,
                 column_ids,
                 *eval_ctx_))) {
    LOG_WARN("failed to init external table pushdown filter");
  } else if (OB_FAIL(prepare_filter_col_meta(column_ids, column_metas))) {
    LOG_WARN("failed to prepare filter col meta");
  }
  return ret;
}

int ObLakeTablePushDownFilter::generate_pd_filter()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_filter_spec_.pd_expr_spec_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pd expr spec");
  } else if (OB_FAIL(file_filter_spec_.expr_frame_info_.pre_alloc_exec_memory(exec_ctx_,
                                                                              &allocator_))) {
    LOG_WARN("fail to pre allocate memory", K(file_filter_spec_.expr_frame_info_));
  } else if (OB_ISNULL(eval_ctx_ = static_cast<ObEvalCtx *>(allocator_.alloc(sizeof(ObEvalCtx))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator memory for ObEvalCtx");
  } else if (OB_ISNULL(pd_expr_op_ = static_cast<ObPushdownOperator *>(
                           allocator_.alloc(sizeof(ObPushdownOperator))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator memory for ObPushdownOperator");
  } else {
    eval_ctx_ = new (eval_ctx_) ObEvalCtx(exec_ctx_, &allocator_);
    pd_expr_op_
        = new (pd_expr_op_) ObPushdownOperator(*eval_ctx_, *file_filter_spec_.pd_expr_spec_);
    if (OB_FAIL(pd_expr_op_->init_pushdown_storage_filter())) {
      LOG_WARN("failed to init pushdown storage filter failed");
    } else {
      pushdown_filter_ = pd_expr_op_->pd_storage_filters_;
    }
  }
  return ret;
}

int ObLakeTablePushDownFilter::prepare_filter_col_meta(ObIArray<uint64_t> &column_ids,
                                                       ObIArray<ObColumnMeta> &column_metas)
{
  int ret = OB_SUCCESS;
  common::ObArrayWrap<int> column_indexs;
  if (OB_UNLIKELY(column_ids.count() != column_metas.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column count", K(column_ids.count()), K(column_metas.count()));
  } else if (OB_FAIL(column_indexs.allocate_array(allocator_, column_ids.count()))) {
    LOG_WARN("failed to allocate array for column indexs");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      column_indexs.at(i) = normalization_column_id(column_ids.at(i));
    }

    if (OB_SUCC(ret)
        && OB_FAIL(ObExternalTablePushdownFilter::prepare_filter_col_meta(column_indexs,
                                                                          column_ids,
                                                                          column_metas))) {
      LOG_WARN("fail to prepare filter col meta",
               K(ret),
               K(column_indexs.count()),
               K(column_metas.count()));
    }
  }
  return ret;
}
