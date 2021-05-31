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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_table_scan_op.h"
#include "ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_spliter.h"
#include "lib/profile/ob_perf_event.h"
#include "storage/ob_table_scan_iterator.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {

#define USE_MULTI_GET_ARRAY_BINDING 1
#define range_array_pos (USE_MULTI_GET_ARRAY_BINDING ? scan_param_.range_array_pos_ : MY_INPUT.range_array_pos_)

ObTableScanOpInput::ObTableScanOpInput(ObExecContext& ctx, const ObOpSpec& spec)
    : ObOpInput(ctx, spec), location_idx_(OB_INVALID_INDEX)
{}

ObTableScanOpInput::~ObTableScanOpInput()
{}

void ObTableScanOpInput::reset()
{
  location_idx_ = OB_INVALID_INDEX;
  key_ranges_.reset();
  range_array_pos_.reset();
}

OB_DEF_SERIALIZE_SIZE(ObTableScanOpInput)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, location_idx_, key_ranges_);
  // range_array_pos_ not serialized
  return len;
}

OB_DEF_SERIALIZE(ObTableScanOpInput)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, location_idx_, key_ranges_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableScanOpInput)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, location_idx_);
  if (OB_SUCC(ret)) {
    int64_t cnt = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &cnt))) {
      LOG_WARN("decode failed", K(ret));
    } else if (OB_FAIL(key_ranges_.prepare_allocate(cnt))) {
      LOG_WARN("array prepare allocate failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
        if (OB_FAIL(key_ranges_.at(i).deserialize(exec_ctx_.get_allocator(), buf, data_len, pos))) {
          LOG_WARN("range deserialize failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableScanOpInput::init(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (PHY_FAKE_CTE_TABLE == MY_SPEC.type_) {
    LOG_DEBUG("CTE TABLE do not need init", K(ret));
  } else if (ObTaskSpliter::INVALID_SPLIT == task_info.get_task_split_type()) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec type is INVALID_SPLIT", K(ret));
  } else {
    if (ObTaskSpliter::LOCAL_IDENTITY_SPLIT == task_info.get_task_split_type()) {
      location_idx_ = 0;
      // validate
#ifndef NDEBUG
      const ObPhyTableLocation* table_loc = NULL;
      if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
              exec_ctx_, MY_SPEC.table_location_key_, MY_SPEC.get_location_table_id(), table_loc))) {
        LOG_WARN("fail to get table location", K(ret));
      } else if (OB_ISNULL(table_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get phy table location", K(ret));
      } else {
        const ObPartitionReplicaLocationIArray& partition_loc_list = table_loc->get_partition_location_list();
        if (OB_UNLIKELY(partition_loc_list.count() > 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition location list count is greater than 1", K(ret), "count", partition_loc_list.count());
        }
      }
#endif
    } else {
      location_idx_ = task_info.get_location_idx();
      if (1 == task_info.get_range_location().part_locs_.count()  // only one table
          && 0 < task_info.get_range_location().part_locs_.at(0).scan_ranges_.count()) {
        // multi-range
        ret = key_ranges_.assign(task_info.get_range_location().part_locs_.at(0).scan_ranges_);
      }
    }
  }
  return ret;
}

int ObTableScanOpInput::translate_pid_to_ldx(
    int64_t partition_id, int64_t table_location_key, int64_t ref_table_id, int64_t& location_idx)
{
  return ObTaskExecutorCtxUtil::translate_pid_to_ldx(
      exec_ctx_.get_task_exec_ctx(), partition_id, table_location_key, ref_table_id, location_idx);
}

int ObTableScanOp::init_table_allocator()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  lib::ContextParam param;
  param.set_mem_attr(my_session->get_effective_tenant_id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  MemoryContext* mem_context = nullptr;
  if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context, param))) {
    LOG_WARN("fail to create entity", K(ret));
  } else if (OB_ISNULL(mem_context)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create entity ", K(ret));
  } else {
    table_allocator_ = &mem_context->get_arena_allocator();
  }
  if (OB_SUCC(ret)) {
    MemoryContext* mem_context = nullptr;
    lib::ContextParam param;
    param.set_mem_attr(my_session->get_effective_tenant_id(), ObModIds::OB_TABLE_SCAN_ITER, ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context, param))) {
      LOG_WARN("fail to create entity", K(ret));
    } else if (OB_ISNULL(mem_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create entity ", K(ret));
    } else {
      scan_param_.iterator_mementity_ = mem_context;
      scan_param_.allocator_ = &mem_context->get_arena_allocator();
    }
  }
  return ret;
}

ObTableScanSpec::ObTableScanSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      ref_table_id_(OB_INVALID_ID),
      index_id_(OB_INVALID_ID),
      table_location_key_(OB_INVALID_ID),
      is_index_global_(false),
      output_column_ids_(alloc),
      storage_output_(alloc),
      pushdown_filters_(alloc),
      filters_before_index_back_(alloc),
      flags_(0),
      limit_(NULL),
      offset_(NULL),
      for_update_(false),
      for_update_wait_us_(-1), /* default infinite */
      hint_(),
      exist_hint_(false),
      is_get_(false),
      pre_query_range_(alloc),
      is_top_table_scan_(false),
      table_param_(alloc),
      schema_version_(-1),
      part_level_(ObPartitionLevel::PARTITION_LEVEL_MAX),
      part_type_(ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
      subpart_type_(ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
      part_expr_(NULL),
      subpart_expr_(NULL),
      part_range_pos_(alloc),
      subpart_range_pos_(alloc),
      part_dep_cols_(alloc),
      subpart_dep_cols_(alloc),
      is_vt_mapping_(false),
      use_real_tenant_id_(false),
      has_tenant_id_col_(false),
      vt_table_id_(UINT64_MAX),
      real_schema_version_(INT64_MAX),
      mapping_exprs_(alloc),
      output_row_types_(alloc),
      key_types_(alloc),
      key_with_tenant_ids_(alloc),
      has_extra_tenant_ids_(alloc),
      org_output_column_ids_(alloc),

      optimization_method_(MAX_METHOD),
      available_index_count_(0),

      table_row_count_(0),
      output_row_count_(0),
      phy_query_range_row_count_(0),
      query_range_row_count_(0),
      index_back_row_count_(0),
      estimate_method_(INVALID_METHOD),
      est_records_(alloc),
      available_index_name_(alloc),
      pruned_index_name_(alloc),
      gi_above_(false),
      expected_part_id_(NULL),
      need_scn_(false),
      batch_scan_flag_(false),
      pd_storage_flag_(false),
      pd_storage_filters_(alloc),
      pd_storage_index_back_filters_(alloc)
{}

OB_SERIALIZE_MEMBER((ObTableScanSpec, ObOpSpec), ref_table_id_, index_id_, table_location_key_, is_index_global_,
    output_column_ids_, storage_output_, pushdown_filters_, filters_before_index_back_, flags_, limit_, offset_,
    for_update_, for_update_wait_us_, hint_, exist_hint_, is_get_, pre_query_range_, is_top_table_scan_, table_param_,
    schema_version_, part_level_, part_type_, subpart_type_, part_expr_, subpart_expr_, part_range_pos_,
    subpart_range_pos_,
    // no need to serialize acs members
    // no need to serialize plan explain members.
    gi_above_, expected_part_id_, need_scn_, batch_scan_flag_, part_dep_cols_, subpart_dep_cols_, is_vt_mapping_,
    use_real_tenant_id_, has_tenant_id_col_, vt_table_id_, real_schema_version_, mapping_exprs_, output_row_types_,
    key_types_, key_with_tenant_ids_, has_extra_tenant_ids_, org_output_column_ids_, pd_storage_flag_,
    pd_storage_filters_, pd_storage_index_back_filters_);

DEF_TO_STRING(ObTableScanSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K(ref_table_id_),
      K(index_id_),
      K(table_location_key_),
      K(is_index_global_),
      K(output_column_ids_),
      K(storage_output_),
      K(flags_),
      K(limit_),
      K(offset_),
      K(for_update_),
      K(for_update_wait_us_),
      K(hint_),
      K(exist_hint_),
      K(is_get_),
      K(pre_query_range_),
      K(is_top_table_scan_),
      K(table_param_),
      K(schema_version_),
      K(gi_above_),
      K(need_scn_),
      K(batch_scan_flag_));
  J_OBJ_END();
  return pos;
}

int ObTableScanSpec::set_est_row_count_record(const ObIArray<ObEstRowCountRecord>& est_records)
{
  int ret = OB_SUCCESS;
  OZ(est_records_.init(est_records.count()));
  OZ(append(est_records_, est_records));
  return ret;
}

int ObTableScanSpec::set_available_index_name(const ObIArray<ObString>& idx_name, ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  OZ(available_index_name_.init(idx_name.count()));
  FOREACH_CNT_X(n, idx_name, OB_SUCC(ret))
  {
    ObString name;
    OZ(ob_write_string(phy_alloc, *n, name));
    OZ(available_index_name_.push_back(name));
  }
  return ret;
}

int ObTableScanSpec::set_pruned_index_name(const ObIArray<ObString>& idx_name, ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  OZ(pruned_index_name_.init(idx_name.count()));
  FOREACH_CNT_X(n, idx_name, OB_SUCC(ret))
  {
    ObString name;
    OZ(ob_write_string(phy_alloc, *n, name));
    OZ(pruned_index_name_.push_back(name));
  }
  return ret;
}

int ObTableScanSpec::explain_index_selection_info(char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const char* method[5] = {"invalid_method", "remote_storage", "local_storage", "basic_stat", "histogram"};
  if (OB_UNLIKELY(estimate_method_ < 0 || estimate_method_ >= 5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(estimate_method_), K(ret));
  } else if (OB_FAIL(BUF_PRINTF("table_rows:%ld, physical_range_rows:%ld, logical_range_rows:%ld, "
                                "index_back_rows:%ld, output_rows:%ld, est_method:%s",
                 table_row_count_,
                 phy_query_range_row_count_,
                 query_range_row_count_,
                 index_back_row_count_,
                 output_row_count_,
                 method[estimate_method_]))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  if (OB_SUCC(ret) && available_index_name_.count() > 0) {
    // print available index id
    if (OB_FAIL(BUF_PRINTF(", avaiable_index_name["))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < available_index_name_.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%.*s", available_index_name_.at(i).length(), available_index_name_.at(i).ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (i != available_index_name_.count() - 1) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* do nothing*/
        }
      } else { /* do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF("]"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
  }

  if (OB_SUCC(ret) && pruned_index_name_.count() > 0) {
    if (OB_FAIL(BUF_PRINTF(", pruned_index_name["))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pruned_index_name_.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%.*s", pruned_index_name_.at(i).length(), pruned_index_name_.at(i).ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (i != pruned_index_name_.count() - 1) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* do nothing*/
        }
      } else { /* do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF("]"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
  }

  if (OB_SUCC(ret) && est_records_.count() > 0) {
    // print est row count infos
    if (OB_FAIL(BUF_PRINTF(", estimation info[table_id:%ld,", est_records_.at(0).table_id_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < est_records_.count(); ++i) {
      const ObEstRowCountRecord& record = est_records_.at(i);
      if (OB_FAIL(BUF_PRINTF(" (table_type:%ld, version:%ld-%ld-%ld, logical_rc:%ld, physical_rc:%ld)%c",
              record.table_type_,
              record.version_range_.base_version_,
              record.version_range_.multi_version_start_,
              record.version_range_.snapshot_version_,
              record.logical_row_count_,
              record.physical_row_count_,
              i == est_records_.count() - 1 ? ']' : ','))) {
        LOG_WARN("BUF PRINTF fails", K(ret));
      }
    }
  }
  return ret;
}

ObTableScanOp::ObTableScanOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      result_(NULL),
      ab_iters_(NULL),
      bnl_iters_(NULL),
      row2exprs_projector_(exec_ctx.get_allocator()),
      table_allocator_(NULL),
      output_row_count_(-1),
      iter_end_(false),
      partition_id_(OB_INVALID_INDEX),
      iterated_rows_(0),
      is_partition_list_empty_(false),
      got_feedback_(false),
      expected_part_id_(OB_INVALID_ID),
      vt_result_converter_(nullptr),
      expr_ctx_alloc_(ObModIds::OB_SQL_EXPR_CALC, OB_MALLOC_NORMAL_BLOCK_SIZE,
          exec_ctx.get_my_session()->get_effective_tenant_id()),
      filter_executor_(nullptr),
      index_back_filter_executor_(nullptr),
      cur_trace_id_(nullptr)
{
  scan_param_.partition_guard_ = &partition_guard_;
}

ObTableScanOp::~ObTableScanOp()
{
  scan_param_.partition_guard_ = NULL;
  partition_guard_.reset();
}

int ObTableScanOp::get_partition_service(ObTaskExecutorCtx& executor_ctx, ObIDataAccessService*& das) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSQLUtils::get_partition_service(executor_ctx, MY_SPEC.ref_table_id_, das))) {
    LOG_WARN("fail to get partition service", K(ret));
  }
  return ret;
}

int ObTableScanOp::prepare_scan_param()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObTaskExecutorCtx& task_exec_ctx = ctx_.get_task_exec_ctx();
  const ObPhyTableLocation* table_location = NULL;
  if (OB_ISNULL(plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get plan ctx", K(ret), KP(plan_ctx));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 task_exec_ctx, MY_SPEC.table_location_key_, MY_SPEC.get_location_table_id(), table_location))) {
    LOG_WARN("failed to get physical table location", K(ret), K(MY_SPEC.table_location_key_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (table_location->get_partition_location_list().count() < 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("empty partition scan is not supported at the moment!",
        "# partitions",
        table_location->get_partition_location_list().count());
  } else if (table_location->get_partition_location_list().count() <= MY_INPUT.get_location_idx()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("partition index is out-of-range",
        "# partitions",
        table_location->get_partition_location_list().count(),
        "location_idx",
        MY_INPUT.get_location_idx());
  } else if (OB_INVALID_INDEX == MY_INPUT.get_location_idx()) {
    iter_end_ = true;
    ret = OB_ITER_END;
    LOG_DEBUG("scan input has a invalid location idx", K(ret), K(MY_SPEC.id_), K(this), K(lbt()));
  } else {
    partition_id_ = table_location->get_partition_location_list().at(MY_INPUT.get_location_idx()).get_partition_id();
    for (int i = 0; i < MY_SPEC.output_.count(); i++) {
      ObExpr* expr = MY_SPEC.output_.at(i);
      if (expr->type_ == T_PDML_PARTITION_ID) {
        // There is a corresponding PDML partition id pseudo column
        expr->locate_datum_for_write(eval_ctx_).set_int(partition_id_);
        expr->get_eval_info(eval_ctx_).evaluated_ = true;
        LOG_TRACE("find the partition id expr in pdml table scan", K(ret), K(i), K(partition_id_), K(partition_id_));
      }
    }
  }

  /*
   * init ObTableScanParam
   * 2. Initialize plan static data
   */
  /* step 2 */
  if (OB_SUCC(ret)) {
    scan_param_.ref_table_id_ = MY_SPEC.ref_table_id_;
    scan_param_.index_id_ = ObSQLMockSchemaUtils::is_mock_index(MY_SPEC.index_id_)
                                ? ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(MY_SPEC.index_id_)
                                : MY_SPEC.index_id_;
    scan_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    scan_param_.scan_flag_.flag_ = MY_SPEC.flags_;
    set_cache_stat(plan_ctx->get_phy_plan()->stat_, scan_param_);
    scan_param_.reserved_cell_count_ = MY_SPEC.output_column_ids_.count();
    scan_param_.for_update_ = MY_SPEC.for_update_;
    scan_param_.for_update_wait_timeout_ = MY_SPEC.for_update_wait_us_ > 0
                                               ? MY_SPEC.for_update_wait_us_ + my_session->get_query_start_time()
                                               : MY_SPEC.for_update_wait_us_;
    scan_param_.sql_mode_ = my_session->get_sql_mode();
    scan_param_.scan_allocator_ = &ctx_.get_allocator();  // used by virtual table
    if (MY_SPEC.exist_hint_) {
      scan_param_.frozen_version_ = MY_SPEC.hint_.frozen_version_;
      scan_param_.force_refresh_lc_ = MY_SPEC.hint_.force_refresh_lc_;
    }
    if (MY_SPEC.is_vt_mapping_) {
      scan_param_.output_exprs_ = &MY_SPEC.mapping_exprs_;
    } else {
      scan_param_.output_exprs_ = &MY_SPEC.storage_output_;
    }
    scan_param_.op_ = this;
    if (!MY_SPEC.pushdown_filters_.empty()) {
      scan_param_.op_filters_ = &MY_SPEC.pushdown_filters_;
    }
    scan_param_.pd_storage_filters_ = filter_executor_;
    if (!MY_SPEC.filters_before_index_back_.empty()) {
      scan_param_.op_filters_before_index_back_ = &MY_SPEC.filters_before_index_back_;
      ;
    }
    scan_param_.pd_storage_index_back_filters_ = index_back_filter_executor_;
    scan_param_.pd_storage_flag_ = MY_SPEC.pd_storage_flag_;
    scan_param_.row2exprs_projector_ = &row2exprs_projector_;

    // multi-partition scan is not supported at the moment! so hard code 0 below
    const share::ObPartitionReplicaLocation& part_loc =
        table_location->get_partition_location_list().at(MY_INPUT.get_location_idx());

    if (0 != scan_param_.pd_storage_flag_ && OB_ISNULL(index_back_filter_executor_) && OB_ISNULL(filter_executor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("enable filter pushdown, but no filter pushdown", K(ret));
    } else if (OB_FAIL(part_loc.get_partition_key(scan_param_.pkey_))) {
      LOG_WARN("get partition key fail", K(ret), K(part_loc));
    } else if (OB_FAIL(MY_SPEC.plan_->get_base_table_version(MY_SPEC.ref_table_id_, scan_param_.schema_version_))) {
      LOG_WARN("get base table version failed", K(ret), K_(MY_SPEC.ref_table_id));
    } else if (MY_SPEC.is_vt_mapping_ && FALSE_IT(scan_param_.schema_version_ = MY_SPEC.real_schema_version_)) {
    } else {
      if (NULL != MY_SPEC.expected_part_id_) {
        // Initialize the param datum data pointed to
        // by the partition id expression expected in the partition filter
        ObDatum& param_datum = MY_SPEC.expected_part_id_->locate_expr_datum(eval_ctx_);
        param_datum.ptr_ = reinterpret_cast<char*>(&expected_part_id_);
        param_datum.set_int(scan_param_.pkey_.get_partition_id());
        MY_SPEC.expected_part_id_->get_eval_info(eval_ctx_).evaluated_ = true;
      }
    }
    LOG_DEBUG("debug location",
        K(part_loc),
        K(scan_param_.pkey_),
        K(scan_param_),
        K(scan_param_.schema_version_),
        K(scan_param_.timeout_));
  }
  if (OB_SUCC(ret)) {
    scan_param_.column_ids_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_column_ids_.count(); ++i) {
      if (OB_FAIL(scan_param_.column_ids_.push_back(MY_SPEC.output_column_ids_.at(i)))) {
        LOG_WARN("fail to push back key range", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    scan_param_.limit_param_.limit_ = -1;
    scan_param_.limit_param_.offset_ = 0;
    scan_param_.trans_desc_ = &my_session->get_trans_desc();
    bool is_null_value = false;
    if (NULL != MY_SPEC.limit_) {
      if (OB_FAIL(calc_expr_int_value(*MY_SPEC.limit_, scan_param_.limit_param_.limit_, is_null_value))) {
        LOG_WARN("fail get val", K(ret));
      } else if (scan_param_.limit_param_.limit_ < 0) {
        scan_param_.limit_param_.limit_ = 0;
      }
    }

    if (OB_SUCC(ret) && NULL != MY_SPEC.offset_ && !is_null_value) {
      if (OB_FAIL(calc_expr_int_value(*MY_SPEC.offset_, scan_param_.limit_param_.offset_, is_null_value))) {
        LOG_WARN("fail get val", K(ret));
      } else if (scan_param_.limit_param_.offset_ < 0) {
        scan_param_.limit_param_.offset_ = 0;
      } else if (is_null_value) {
        scan_param_.limit_param_.limit_ = 0;
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t schema_version = is_sys_table(MY_SPEC.ref_table_id_)
                                 ? task_exec_ctx.get_query_sys_begin_schema_version()
                                 : task_exec_ctx.get_query_tenant_begin_schema_version();
    scan_param_.query_begin_schema_version_ = schema_version;
    scan_param_.table_param_ = &MY_SPEC.table_param_;
  }
  if (OB_SUCC(ret) && NULL != MY_SPEC.expected_part_id_) {
    bool is_get = false;
    int64_t schema_version = 0;
    if (OB_FAIL(MY_SPEC.pre_query_range_.is_get(is_get))) {
      LOG_WARN("extract pre query range get info failed", K(ret));
    } else if (!is_get) {
      // For table get, there is no need to do partition filtering
      if (OB_ISNULL(task_exec_ctx.schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service is null");
      } else if (OB_FAIL(task_exec_ctx.schema_service_->get_tenant_schema_guard(
                     my_session->get_effective_tenant_id(), schema_guard_))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard_.get_table_schema_version(MY_SPEC.ref_table_id_, schema_version))) {
        LOG_WARN("get table schema version failed", K(ret), K(MY_SPEC.ref_table_id_));
      } else if (OB_UNLIKELY(schema_version != scan_param_.schema_version_)) {
        if (OB_INVALID_VERSION == schema_version) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", K(ret), K(schema_version), K(MY_SPEC.ref_table_id_));
        } else {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("unexpected schema version, need retry", K(schema_version), K(scan_param_.schema_version_));
        }
      } else {
        scan_param_.part_mgr_ = &schema_guard_;
      }
    }
  }
  LOG_DEBUG("debug trans", K(*scan_param_.trans_desc_), K(ret));
  return ret;
}

int ObTableScanOp::prepare(bool is_rescan)
{
  int ret = OB_SUCCESS;
  ObQueryRangeArray key_ranges;
  ObGetMethodArray get_method;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObTaskExecutorCtx& task_ctx = ctx_.get_task_exec_ctx();

  if (OB_UNLIKELY(!need_extract_range())) {
    // virtual table, do nothing
  } else if (OB_UNLIKELY(is_partition_list_empty_ = partition_list_is_empty(task_ctx.get_table_locations()))) {
    // The number of partitions involved is 0, do nothing
  } else if (OB_ISNULL(table_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret));
  } else if (OB_FAIL(ObSQLUtils::extract_pre_query_range(MY_SPEC.pre_query_range_,
                 is_rescan ? *table_allocator_ : ctx_.get_allocator(),
                 plan_ctx->get_param_store(),
                 key_ranges,
                 get_method,
                 ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session()),
                 &range_array_pos))) {
    LOG_WARN("failed to extract pre query ranges", K(ret));
  } else {
    if (USE_MULTI_GET_ARRAY_BINDING && !range_array_pos.empty()) {
      scan_param_.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }
    // transform rowid query ranges to primary key ranges
    if (ObSQLMockSchemaUtils::is_mock_index(MY_SPEC.index_id_) &&
        OB_FAIL(ObTableScan::transform_rowid_ranges(is_rescan ? *table_allocator_ : ctx_.get_allocator(),
            MY_SPEC.table_param_,
            MY_SPEC.index_id_,
            key_ranges))) {
      LOG_WARN("failed to transform rowid ranges", K(ret));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 < MY_INPUT.key_ranges_.count() && !is_rescan) {
      // intra-partition parallelism, do nothing
    } else {
      MY_INPUT.key_ranges_.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
        ObNewRange* key_range = key_ranges.at(i);
        if (OB_ISNULL(key_range)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("key range is null", K(ret), K(i));
        } else {
          if (MY_SPEC.should_scan_index()) {
            key_range->table_id_ = MY_SPEC.index_id_;
          } else {
            key_range->table_id_ = MY_SPEC.ref_table_id_;
          }
          ret = MY_INPUT.key_ranges_.push_back(*key_range);
        }
      }
    }
    LOG_DEBUG("table scan final range", K(key_ranges), K(scan_param_.range_array_pos_), K(MY_INPUT.range_array_pos_));
    if (OB_SUCC(ret) && plan_ctx->get_bind_array_count() > 0) {
      if (range_array_pos.empty() && MY_SPEC.filters_.count() > 0) {
        // filter contain bind array condition, but query range is normal condition, not support
        // because bind array condition must be driven by multi table iterator
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Bind array with filter");
      }
    }
  }
  return ret;
}

bool ObTableScanOp::partition_list_is_empty(const ObPhyTableLocationIArray& phy_table_locs) const
{
  bool part_list_is_empty = true;
  int ret = OB_SUCCESS;
  const int64_t loc_cnt = phy_table_locs.count();
  const uint64_t loc_table_id = MY_SPEC.get_location_table_id();
  for (int64_t i = 0; OB_SUCC(ret) && part_list_is_empty && i < loc_cnt; ++i) {
    const ObPhyTableLocation& phy_table_loc = phy_table_locs.at(i);
    if (loc_table_id == phy_table_loc.get_ref_table_id()) {
      const auto& part_locs = phy_table_loc.get_partition_location_list();
      if (OB_UNLIKELY(0 != part_locs.count())) {
        part_list_is_empty = false;
      }
    }
    LOG_DEBUG("debug get partition",
        K(phy_table_loc.get_ref_table_id()),
        K(loc_table_id),
        K(MY_SPEC.is_index_global_),
        K(phy_table_loc));
  }
  LOG_DEBUG("is partition list empty", K(part_list_is_empty));
  return part_list_is_empty;
}

int ObTableScanOp::init_pushdown_storage_filter()
{
  int ret = OB_SUCCESS;
  if (0 != MY_SPEC.pd_storage_flag_) {
    ObFilterExecutorConstructor filter_exec_constructor(&ctx_.get_allocator());
    if (OB_NOT_NULL(MY_SPEC.pd_storage_filters_.get_pushdown_filter())) {
      if (OB_FAIL(filter_exec_constructor.apply(MY_SPEC.pd_storage_filters_.get_pushdown_filter(), filter_executor_))) {
        LOG_WARN("failed to create filter executor", K(ret));
      } else if (OB_ISNULL(filter_executor_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter executor is null", K(ret));
      } else if (OB_FAIL(
                     filter_executor_->init_evaluated_datums(ctx_.get_allocator(), spec_.calc_exprs_, &eval_ctx_))) {
        LOG_WARN("failed to init evaluated datums", K(ret));
      } else {
        LOG_DEBUG("debug filter pushdown", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(MY_SPEC.pd_storage_index_back_filters_.get_pushdown_filter())) {
      if (OB_FAIL(filter_exec_constructor.apply(
              MY_SPEC.pd_storage_index_back_filters_.get_pushdown_filter(), index_back_filter_executor_))) {
        LOG_WARN("failed to create filter executor", K(ret));
      } else if (OB_ISNULL(index_back_filter_executor_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter executor is null", K(ret));
      } else if (OB_FAIL(index_back_filter_executor_->init_evaluated_datums(
                     ctx_.get_allocator(), spec_.calc_exprs_, &eval_ctx_))) {
        LOG_WARN("failed to init evaluated datums", K(ret));
      } else {
        LOG_DEBUG("debug index back filter pushdown", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanOp::init_converter()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_vt_mapping_) {
    ObSqlCtx* sql_ctx = NULL;
    if (MY_SPEC.ref_table_id_ != MY_SPEC.index_id_ || MY_SPEC.is_index_global_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "table id is not match", K(ret), K(MY_SPEC.ref_table_id_), K(MY_SPEC.index_id_), K(MY_SPEC.is_index_global_));
    } else if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: sql ctx or schema guard is null", K(ret));
    } else {
      if (OB_NOT_NULL(vt_result_converter_)) {
        vt_result_converter_->destroy();
        vt_result_converter_->~ObVirtualTableResultConverter();
        vt_result_converter_ = nullptr;
      }
      const ObTableSchema* org_table_schema = NULL;
      void* buf = ctx_.get_allocator().alloc(sizeof(ObVirtualTableResultConverter));
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator", K(ret));
      } else if (FALSE_IT(vt_result_converter_ = new (buf) ObVirtualTableResultConverter)) {
      } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(MY_SPEC.vt_table_id_, org_table_schema))) {
        LOG_WARN("get table schema failed", K(MY_SPEC.vt_table_id_), K(ret));
      } else if (OB_FAIL(vt_result_converter_->reset_and_init(table_allocator_,
                     GET_MY_SESSION(ctx_),
                     &MY_SPEC.output_row_types_,
                     &MY_SPEC.key_types_,
                     &MY_SPEC.key_with_tenant_ids_,
                     &MY_SPEC.has_extra_tenant_ids_,
                     &ctx_.get_allocator(),
                     org_table_schema,
                     &MY_SPEC.org_output_column_ids_,
                     MY_SPEC.use_real_tenant_id_,
                     MY_SPEC.has_tenant_id_col_))) {
        LOG_WARN("failed to init converter", K(ret));
      }
    }
    LOG_TRACE("debug init converter", K(ret), K(MY_SPEC.ref_table_id_));
  }
  return ret;
}

int ObTableScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = ctx_.get_task_exec_ctx();
  MY_INPUT.set_location_idx(0);
  if (OB_FAIL(init_old_expr_ctx())) {
    LOG_WARN("init old expr ctx failed", K(ret));
  } else if (OB_FAIL(init_table_allocator())) {
    LOG_WARN("init table allocator failed", K(ret));
  } else if (OB_UNLIKELY(partition_list_is_empty(task_exec_ctx.get_table_locations()))) {
    // The number of partitions involved is 0, do nothing
  } else if (OB_FAIL(init_pushdown_storage_filter())) {
    LOG_WARN("failed to init pushdown storage filter", K(ret));
  } else if (OB_FAIL(prepare_scan_param())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("prepare scan param failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(init_converter())) {
    LOG_WARN("failed to init converter", K(ret));
  } else if (MY_SPEC.gi_above_) {
    if (OB_FAIL(get_gi_task_and_restart())) {
      LOG_WARN("fail to get gi task and scan", K(ret));
    }
  } else if (OB_FAIL(do_table_scan(false))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to do table scan", K(ret));
    }
  }
  cur_trace_id_ = ObCurTraceId::get();

  return ret;
}

int ObTableScanOp::init_old_expr_ctx()
{
  int ret = OB_SUCCESS;
  const ObTimeZoneInfo* tz_info = NULL;
  int64_t tz_offset = 0;
  expr_ctx_.my_session_ = GET_MY_SESSION(ctx_);
  if (OB_ISNULL(tz_info = get_timezone_info(expr_ctx_.my_session_))) {
    LOG_WARN("get timezone info failed", K(ret));
  } else if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
    LOG_WARN("get tz offset failed", K(ret));
  } else {
    expr_ctx_.phy_plan_ctx_ = GET_PHY_PLAN_CTX(ctx_);
    expr_ctx_.exec_ctx_ = &ctx_;
    expr_ctx_.calc_buf_ = &expr_ctx_alloc_;
    expr_ctx_.tz_offset_ = tz_offset;
    if (OB_FAIL(ObSQLUtils::get_default_cast_mode(
            MY_SPEC.plan_->get_stmt_type(), expr_ctx_.my_session_, expr_ctx_.cast_mode_))) {
      LOG_WARN("get default cast mode failed", K(ret));
    } else if (OB_FAIL(ObSQLUtils::wrap_column_convert_ctx(expr_ctx_, expr_ctx_.column_conv_ctx_))) {
      LOG_WARN("wrap column convert ctx failed", K(ret));
    } else {
      scan_param_.expr_ctx_ = expr_ctx_;
    }
  }
  return ret;
}

int ObTableScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  ObIDataAccessService* das = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx_.get_task_exec_ctx();
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);

  if (MY_SPEC.batch_scan_flag_ ? NULL == bnl_iters_ : NULL == result_) {
    // this is normal case, so we need NOT set ret or LOG_WARN.
  } else {
    if (OB_FAIL(fill_storage_feedback_info())) {
      LOG_WARN("failed to fill storage feedback info", K(ret));
    } else if (OB_FAIL(get_partition_service(task_exec_ctx, das))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get data access service", K(ret));
    } else if (plan_ctx->get_bind_array_count() > 0 && !scan_param_.range_array_pos_.empty()) {
      if (OB_FAIL(das->revert_scan_iter(ab_iters_))) {
        LOG_WARN("fail to revert row iterator", K(ret));
      }
    } else if (MY_SPEC.batch_scan_flag_) {
      if (OB_FAIL(das->revert_scan_iter(bnl_iters_))) {
        LOG_ERROR("fail to revert row iterator", K(ret));
      }
    } else if (OB_FAIL(das->revert_scan_iter(result_))) {
      LOG_ERROR("fail to revert row iterator", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(table_allocator_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        table_allocator_->reuse();
        if (OB_NOT_NULL(scan_param_.iterator_mementity_)) {
          scan_param_.iterator_mementity_->reuse();
        }
        scan_param_.allocator_->reuse();
        result_ = NULL;
        ab_iters_ = NULL;
        bnl_iters_ = NULL;
        output_row_count_ = -1;
        iter_end_ = false;
      }
    }
  }

  return ret;
}

void ObTableScanOp::destroy()
{
  expr_ctx_alloc_.reset();
  row2exprs_projector_.destroy();
  scan_param_.destroy_schema_guard();
  schema_guard_.~ObSchemaGetterGuard();
  scan_param_.partition_guard_ = NULL;
  partition_guard_.reset();
  ObOperator::destroy();
  if (OB_NOT_NULL(vt_result_converter_)) {
    vt_result_converter_->destroy();
    vt_result_converter_->~ObVirtualTableResultConverter();
    vt_result_converter_ = nullptr;
  }
}

int ObTableScanOp::fill_storage_feedback_info()
{
  int ret = OB_SUCCESS;
  // fill storage feedback info for acs
  bool is_index_back = scan_param_.scan_flag_.index_back_;
  ObTableScanStat& table_scan_stat = GET_PHY_PLAN_CTX(ctx_)->get_table_scan_stat();
  if (MY_SPEC.should_scan_index()) {
    table_scan_stat.query_range_row_count_ = scan_param_.idx_table_scan_stat_.access_row_cnt_;
    if (is_index_back) {
      table_scan_stat.indexback_row_count_ = scan_param_.idx_table_scan_stat_.out_row_cnt_;
      table_scan_stat.output_row_count_ = scan_param_.main_table_scan_stat_.out_row_cnt_;
    } else {
      table_scan_stat.indexback_row_count_ = -1;
      table_scan_stat.output_row_count_ = scan_param_.idx_table_scan_stat_.out_row_cnt_;
    }
    LOG_DEBUG("index scan feedback info for acs", K(scan_param_.idx_table_scan_stat_), K(table_scan_stat));
  } else {
    table_scan_stat.query_range_row_count_ = scan_param_.main_table_scan_stat_.access_row_cnt_;
    table_scan_stat.indexback_row_count_ = -1;
    table_scan_stat.output_row_count_ = scan_param_.main_table_scan_stat_.out_row_cnt_;
    LOG_DEBUG("table scan feedback info for acs", K(scan_param_.main_table_scan_stat_), K(table_scan_stat));
  }

  ObIArray<ObTableRowCount>& table_row_count_list = GET_PHY_PLAN_CTX(ctx_)->get_table_row_count_list();
  if (!got_feedback_) {
    got_feedback_ = true;
    if (MY_SPEC.should_scan_index() && scan_param_.scan_flag_.is_index_back()) {
      if (scan_param_.scan_flag_.is_need_feedback()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = table_row_count_list.push_back(
                               ObTableRowCount(MY_SPEC.id_, scan_param_.idx_table_scan_stat_.access_row_cnt_)))) {
          LOG_WARN("push back table_id-row_count failed",
              K(tmp_ret),
              K(MY_SPEC.ref_table_id_),
              "access row count",
              scan_param_.idx_table_scan_stat_.access_row_cnt_);
        }
      }
    } else {
      if (scan_param_.scan_flag_.is_need_feedback()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = table_row_count_list.push_back(
                               ObTableRowCount(MY_SPEC.id_, scan_param_.main_table_scan_stat_.access_row_cnt_)))) {
          LOG_WARN("push back table_id-row_count failed but we won't stop execution", K(tmp_ret));
        }
      }
    }
  }
  LOG_DEBUG("table scan feed back info for buffer table",
      K(MY_SPEC.ref_table_id_),
      K(MY_SPEC.index_id_),
      K(MY_SPEC.should_scan_index()),
      "is_need_feedback",
      scan_param_.scan_flag_.is_need_feedback(),
      "idx access row count",
      scan_param_.idx_table_scan_stat_.access_row_cnt_,
      "main access row count",
      scan_param_.main_table_scan_stat_.access_row_cnt_);
  return ret;
}

int ObTableScanOp::rescan()
{
  int ret = OB_SUCCESS;
  if (ctx_.is_gi_restart()) {
    // this scan is started by a gi operator, so, scan a new range.
    if (OB_FAIL(get_gi_task_and_restart())) {
      LOG_WARN("fail to get gi task and scan", K(ret));
    }
  } else if (MY_SPEC.batch_scan_flag_) {
    // do nothing
  } else if (OB_INVALID_INDEX == MY_INPUT.get_location_idx()) {
    iter_end_ = true;
    if (OB_UNLIKELY(NULL != result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("result_ should be NULL", K(ret));
    }
  } else if (is_virtual_table(MY_SPEC.ref_table_id_) || MY_SPEC.for_update_) {
    ret = vt_rescan();
  } else if (!range_array_pos.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Batch iterator rescan");
  } else {
    ret = rt_rescan();
  }
  return ret;
}

int ObTableScanOp::vt_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_FAIL(inner_close())) {
    LOG_WARN("fail to close op", K(ret));
  } else if (OB_FAIL(do_table_scan(true))) {
    LOG_WARN("fail to do table rescan", K(ret));
  }
  return ret;
}

int ObTableScanOp::rt_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reset_query_range())) {
    LOG_WARN("failed to reset query range", K(ret));
  } else if (OB_FAIL(add_query_range())) {
    LOG_WARN("failed to add query range", K(ret));
  } else if (OB_FAIL(rescan_after_adding_query_range())) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
  }
  return ret;
}

/*
 * the following three functions are used for blocked nested loop join
 */
int ObTableScanOp::reset_query_range()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    table_allocator_->reuse();  // reset allocator for prepare
    scan_param_.key_ranges_.reuse();
    scan_param_.range_array_pos_.reuse();
  }
  return ret;
}

int ObTableScanOp::add_query_range()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare(true))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  } else {
    if (OB_FAIL(append(scan_param_.key_ranges_, MY_INPUT.key_ranges_))) {
      LOG_WARN("failed to append key ranges", K(ret));
    } else if (MY_INPUT.key_ranges_.count() == 0) {
      /*do nothing*/
    } else if (OB_FAIL(prune_query_range_by_partition_id(scan_param_.key_ranges_))) {
      LOG_WARN("failed to prune query range by partition id", K(ret));
    } else if (scan_param_.key_ranges_.count() == 0) {
      if (OB_FAIL(scan_param_.key_ranges_.push_back(MY_INPUT.key_ranges_.at(0)))) {
        LOG_WARN("failed to push back query range", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanOp::rescan_after_adding_query_range()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("result_ is NULL", K(ret));
  } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(vt_result_converter_->convert_key_ranges(scan_param_.key_ranges_))) {
    LOG_WARN("failed to convert key ranges", K(ret));
  } else if (OB_FAIL(static_cast<ObTableScanIterator*>(result_)->rescan(scan_param_))) {
    LOG_WARN("failed to rescan", K(ret), K(scan_param_));
  } else {
    iter_end_ = false;
  }
  return ret;
}

// @see ObNestedLoopJoin::batch_index_scan_get_next
int ObTableScanOp::batch_rescan_init()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == MY_INPUT.get_location_idx()) {
    if (OB_UNLIKELY(NULL != result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("result_ should be NULL", K(ret));
    }
  } else if (OB_ISNULL(table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    scan_param_.key_ranges_.reuse();
    table_allocator_->reuse();  // reset allocator for prepare
    // batch rescan should always use KeepOrder scan order
    ObQueryFlag query_flag = scan_param_.scan_flag_;
    query_flag.scan_order_ = ObQueryFlag::KeepOrder;
    scan_param_.scan_flag_ = query_flag;
  }
  return ret;
}

int ObTableScanOp::batch_rescan_add_key()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == MY_INPUT.get_location_idx()) {
    if (OB_UNLIKELY(NULL != result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("result_ should be NULL", K(ret));
    }
  } else if (OB_FAIL(prepare(true))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  } else if (1 != MY_INPUT.key_ranges_.count() ||
             (!MY_INPUT.key_ranges_.at(0).is_single_rowkey() && !MY_INPUT.key_ranges_.at(0).empty())) {
    ret = OB_NOT_SUPPORTED;
    if (1 == MY_INPUT.key_ranges_.count()) {
      LOG_ERROR("batch_rescan support get/seek but not scan",
          K(ret),
          "range_count",
          MY_INPUT.key_ranges_.count(),
          "range",
          MY_INPUT.key_ranges_.at(0));
    } else {
      LOG_ERROR("batch_rescan support get/seek but not scan", K(ret), "range_count", MY_INPUT.key_ranges_.count());
    }
  } else if (OB_FAIL(scan_param_.key_ranges_.push_back(MY_INPUT.key_ranges_.at(0)))) {
    LOG_WARN("fail to push back key range", K(ret));
  }

  return ret;
}

int ObTableScanOp::batch_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_INVALID_INDEX == MY_INPUT.get_location_idx()) {
    iter_end_ = true;
    LOG_DEBUG("batch_rescan got a invalid index, set table scan to end", K(MY_SPEC.id_), K(this), K(lbt()));
    if (OB_UNLIKELY(NULL != result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("result_ should be NULL", K(ret));
    }
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("result_ is NULL", K(ret));
  } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(vt_result_converter_->convert_key_ranges(scan_param_.key_ranges_))) {
    LOG_WARN("failed to convert key ranges", K(ret));
  } else if (OB_FAIL(static_cast<ObTableScanIterator*>(result_)->rescan(scan_param_))) {
    LOG_WARN("failed to rescan", K(ret), K(scan_param_));
  } else {
    NG_TRACE_TIMES(10, rescan);
  }
  return ret;
}

int ObTableScanOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null", K(ret), K(result_));
  } else if (OB_LIKELY(range_array_pos.count() <= 1)) {
    ret = OB_ITER_END;
  } else {
    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    if (plan_ctx->get_bind_array_idx() >= range_array_pos.count()) {
      ret = OB_ITER_END;
    }
  }
  if (OB_SUCC(ret)) {
    if (!MY_INPUT.range_array_pos_.empty()) {
      if (OB_FAIL(extract_scan_ranges())) {
        LOG_WARN("extract scan ranges failed", K(ret));
      } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(vt_result_converter_->convert_key_ranges(scan_param_.key_ranges_))) {
        LOG_WARN("failed to convert key ranges", K(ret));
      } else if (OB_FAIL(static_cast<ObTableScanIterator*>(result_)->rescan(scan_param_))) {
        LOG_WARN("failed to rescan", K(ret), K(scan_param_));
      }
    } else if (OB_ISNULL(ab_iters_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter result is null", K(ret));
    } else if (OB_FAIL(ab_iters_->get_next_iter(result_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next iterator failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    startup_passed_ = MY_SPEC.startup_filters_.empty();
    iter_end_ = false;
  }
  return ret;
}

int ObTableScanOp::bnl_switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(bnl_iters_->get_next_iter(result_))) {
    LOG_WARN("failed to get next iter", K(ret));
  } else {
    set_iter_end(false);
  }

  return ret;
}

int ObTableScanOp::group_rescan_init(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else if (OB_FAIL(reset_query_range())) {
    LOG_WARN("failed to reset query ranges", K(ret));
  } else if (OB_FAIL(scan_param_.key_ranges_.reserve(size))) {
    LOG_WARN("failed to reserve space", K(ret));
  } else if (OB_FAIL(scan_param_.range_array_pos_.reserve(size))) {
    LOG_WARN("failed to reserve space", K(ret));
  } else {
    scan_param_.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
  }
  return ret;
}

int ObTableScanOp::group_add_query_range()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare(true))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  } else if (OB_FAIL(append(scan_param_.key_ranges_, MY_INPUT.key_ranges_))) {
    LOG_WARN("failed to append key ranges", K(ret));
  } else if (OB_FAIL(scan_param_.range_array_pos_.push_back(scan_param_.key_ranges_.count() - 1))) {
    LOG_WARN("failed to push back range array pos", K(ret));
  } else {
    LOG_DEBUG("group add query range", K(MY_INPUT.key_ranges_), K(scan_param_.range_array_pos_));
  }
  return ret;
}

int ObTableScanOp::group_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_virtual_table(MY_SPEC.ref_table_id_) || MY_SPEC.for_update_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to perform table scan for update or on virtual table", K(ret));
  } else if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_INVALID_INDEX == MY_INPUT.get_location_idx()) {
    iter_end_ = true;
    LOG_DEBUG("this a mock table scan", K(ret), K(MY_INPUT.get_location_idx()));
  } else if (OB_ISNULL(bnl_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bnl_iters_ is NULL", K(ret));
  } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(vt_result_converter_->convert_key_ranges(scan_param_.key_ranges_))) {
    LOG_WARN("failed to convert key ranges", K(ret));
  } else if (OB_FAIL(static_cast<ObTableScanIterIterator*>(bnl_iters_)->rescan(scan_param_))) {
    LOG_WARN("failed to rescan", K(ret), K(scan_param_));
  } else if (OB_FAIL(bnl_iters_->get_next_iter(result_))) {
    LOG_WARN("failed to get next iter", K(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(result_), K(ret));
  } else {
    iter_end_ = false;
  }
  return ret;
}

int ObTableScanOp::get_next_row_with_mode()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_vt_mapping_) {
    // switch to mysql mode
    CompatModeGuard g(ObWorker::CompatMode::MYSQL);
    ret = result_->get_next_row();
  } else {
    ret = result_->get_next_row();
  }
  return ret;
}

int ObTableScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_UNLIKELY(is_partition_list_empty_ || 0 == scan_param_.limit_param_.limit_)) {
    ret = OB_ITER_END;
  } else if (iter_end_) {
    ret = OB_ITER_END;
    LOG_DEBUG("inner get next row meet a iter end", K(MY_SPEC.id_), K(this), K(lbt()));
  } else if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table scan result is not init", K(ret));
  } else if (0 == (++iterated_rows_ % CHECK_STATUS_ROWS_INTERVAL) && OB_FAIL(ctx_.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(get_next_row_with_mode())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row from ObNewRowIterator", K(ret));
    } else {
      if (MY_SPEC.is_top_table_scan_ && (scan_param_.limit_param_.offset_ > 0)) {
        if (output_row_count_ < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid output_row_count_", K(output_row_count_), K(ret));
        } else if (output_row_count_ > 0) {
          int64_t total_count = output_row_count_ + scan_param_.limit_param_.offset_;
          ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
          NG_TRACE_EXT(found_rows, OB_ID(total_count), total_count, OB_ID(offset), scan_param_.limit_param_.offset_);
          plan_ctx->set_found_rows(total_count);
        }
      }
      if (MY_SPEC.for_update_ && share::is_oracle_mode()) {
        // we get affected rows here now, but we need get it in for_update operator in future.
        ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
        plan_ctx->set_affected_rows(output_row_count_);
      }
    }
  } else {
    output_row_count_++;
    NG_TRACE_TIMES_WITH_TRACE_ID(1, cur_trace_id_, get_row);
    if (MY_SPEC.is_vt_mapping_ &&
        OB_FAIL(vt_result_converter_->convert_output_row(eval_ctx_, MY_SPEC.mapping_exprs_, MY_SPEC.storage_output_))) {
      LOG_WARN("failed to convert output row", K(ret));
    }
  }
  if (OB_UNLIKELY(OB_ITER_END == ret)) {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard* guard = scan_param_.partition_guard_;
    if (OB_ISNULL(guard)) {
    } else if (OB_ISNULL(partition = guard->get_partition_group())) {
    } else if (scan_param_.main_table_scan_stat_.bf_access_cnt_ > 0) {
      partition->feedback_scan_access_stat(scan_param_);
    }
    ObTableScanStat& table_scan_stat = GET_PHY_PLAN_CTX(ctx_)->get_table_scan_stat();
    fill_table_scan_stat(scan_param_.main_table_scan_stat_, table_scan_stat);
    if (MY_SPEC.should_scan_index() && scan_param_.scan_flag_.index_back_) {
      fill_table_scan_stat(scan_param_.idx_table_scan_stat_, table_scan_stat);
    }
    scan_param_.main_table_scan_stat_.reset_cache_stat();
    scan_param_.idx_table_scan_stat_.reset_cache_stat();
    iter_end_ = true;
  }
  return ret;
}

int ObTableScanOp::calc_expr_int_value(const ObExpr& expr, int64_t& retval, bool& is_null_value)
{
  int ret = OB_SUCCESS;
  is_null_value = false;
  OB_ASSERT(ob_is_int_tc(expr.datum_meta_.type_));
  ObDatum* datum = NULL;
  if (OB_FAIL(expr.eval(eval_ctx_, datum))) {
    LOG_WARN("expr evaluate failed", K(ret));
  } else if (datum->null_) {
    is_null_value = true;
    retval = 0;
  } else {
    retval = *datum->int_;
  }
  return ret;
}

int ObTableScanOp::extract_scan_ranges()
{
  int ret = OB_SUCCESS;
  scan_param_.key_ranges_.reuse();
  if (!USE_MULTI_GET_ARRAY_BINDING && !MY_INPUT.range_array_pos_.empty()) {
    int64_t iter_idx = ctx_.get_physical_plan_ctx()->get_bind_array_idx();
    int64_t start_pos = MY_INPUT.range_array_pos_.at(iter_idx);
    int64_t end_pos = MY_INPUT.range_array_pos_.count() > iter_idx + 1 ? MY_INPUT.range_array_pos_.at(iter_idx + 1)
                                                                       : MY_INPUT.key_ranges_.count();
    for (; OB_SUCC(ret) && start_pos < end_pos; ++start_pos) {
      if (OB_UNLIKELY(start_pos < 0) || OB_UNLIKELY(start_pos >= MY_INPUT.key_ranges_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start_pos is invalid", K(ret), K(start_pos), K(MY_INPUT.key_ranges_.count()));
      } else if (OB_FAIL(scan_param_.key_ranges_.push_back(MY_INPUT.key_ranges_.at(start_pos)))) {
        LOG_WARN("store key range failed", K(ret), K(start_pos), K(end_pos), K_(MY_INPUT.key_ranges));
      }
    }
  } else if (OB_FAIL(append(scan_param_.key_ranges_, MY_INPUT.key_ranges_))) {
    LOG_WARN("failed to append key query range", K(ret));
  }
  LOG_DEBUG("extract scan ranges", K(ret), K(scan_param_.key_ranges_), K(MY_INPUT.key_ranges_));
  return ret;
}

inline int ObTableScanOp::do_table_scan(bool is_rescan, bool need_prepare /*=true*/)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = ctx_.get_task_exec_ctx();
  if (!need_prepare) {
    // The prepare function uses pre_query_range to extract the range and fill it into the input,
    // If the target range has been filled in the input,
    // Skip the prepare step
  } else if (OB_FAIL(prepare(is_rescan))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  }
  /* step 1 */
  if (OB_SUCC(ret)) {
    output_row_count_ = 0;
    scan_param_.key_ranges_.reuse();
    scan_param_.range_array_pos_.reuse();
    scan_param_.table_param_ = &MY_SPEC.table_param_;
    scan_param_.is_get_ = MY_SPEC.is_get_;
    if (OB_FAIL(extract_scan_ranges())) {
      LOG_WARN("extract scan ranges failed", K(ret));
    } else if (MY_INPUT.key_ranges_.count() == 0 || ObSQLMockSchemaUtils::is_mock_index(MY_SPEC.index_id_)) {
      /*do nothing*/
    } else if (OB_FAIL(prune_query_range_by_partition_id(scan_param_.key_ranges_))) {
      LOG_WARN("failed to prune query range by partition id", K(ret));
    } else if (scan_param_.key_ranges_.count() == 0) {
      if (OB_FAIL(scan_param_.key_ranges_.push_back(MY_INPUT.key_ranges_.at(0)))) {
        LOG_WARN("failed to push back query range", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObIDataAccessService* das = NULL;
    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);

    if (MY_SPEC.need_scn_) {
      scan_param_.need_scn_ = true;
    }
    LOG_DEBUG("print need_scn", K(scan_param_.need_scn_));

    if (OB_FAIL(get_partition_service(task_exec_ctx, das))) {
      LOG_WARN("fail to get partition service", K(ret));
    } else if (plan_ctx->get_bind_array_count() > 0 && MY_SPEC.batch_scan_flag_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array binding not support batch nestloop join", K(ret));
    } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(vt_result_converter_->convert_key_ranges(scan_param_.key_ranges_))) {
      LOG_WARN("failed to convert key ranges", K(ret));
    } else if (USE_MULTI_GET_ARRAY_BINDING && plan_ctx->get_bind_array_count() > 0) {
      if (OB_FAIL(das->table_scan(scan_param_, ab_iters_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to scan table", K(scan_param_), K(MY_SPEC.ref_table_id_), K(ret));
        }
      } else if (OB_FAIL(ab_iters_->get_next_iter(result_))) {
        LOG_WARN("get next iterator failed", K(ret));
      }
    } else if (MY_SPEC.batch_scan_flag_) {
      if (OB_FAIL(das->table_scan(scan_param_, bnl_iters_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to scan table", K(scan_param_), K(MY_SPEC.ref_table_id_), K(ret));
        }
      } else if (OB_ISNULL(bnl_iters_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(bnl_iters_->get_next_iter(result_))) {
        LOG_WARN("failed to get next iter", K(ret));
      } else if (OB_ISNULL(result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        LOG_DEBUG("table_scan(batch) begin", K(scan_param_), K(*result_), K(MY_SPEC.id_));
      }
    } else {
      if (OB_FAIL(das->table_scan(scan_param_, result_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to scan table", K(scan_param_), K(MY_SPEC.ref_table_id_), K(ret));
        }
      } else {
        LOG_DEBUG("table_scan begin", K(scan_param_), K(*result_), "op_id", MY_SPEC.id_);
        LOG_DEBUG("debug trans", K(*scan_param_.trans_desc_), K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanOp::prune_query_range_by_partition_id(ObIArray<ObNewRange>& scan_ranges)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);

  int64_t schema_version = -1;
  const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
  if (ObPartitionLevel::PARTITION_LEVEL_MAX == MY_SPEC.part_level_ ||
      ObPartitionLevel::PARTITION_LEVEL_ZERO == MY_SPEC.part_level_ || scan_ranges.count() <= 1) {
    /*do nothing*/
  } else if (MY_SPEC.part_range_pos_.count() == 0 || (ObPartitionLevel::PARTITION_LEVEL_TWO == MY_SPEC.part_level_ &&
                                                         MY_SPEC.subpart_range_pos_.count() == 0)) {
    /*do nothing*/
  } else if (OB_FAIL(
                 gctx.schema_service_->get_tenant_schema_guard(my_session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema_version(MY_SPEC.ref_table_id_, schema_version))) {
    LOG_WARN("get table schema version failed", K(ret), K_(MY_SPEC.ref_table_id));
  } else if (OB_FAIL(check_cache_table_version(schema_version))) {
    LOG_WARN("check cache table version failed", K(ret), K(schema_version));
  } else {
    int pos = 0;
    bool can_prune = false;
    LOG_DEBUG("range count before pruning", K(scan_ranges.count()));
    while (OB_SUCC(ret) && pos < scan_ranges.count()) {
      clear_evaluated_flag();
      if (OB_FAIL(can_prune_by_partition_id(schema_guard, partition_id_, scan_ranges.at(pos), can_prune))) {
        LOG_WARN("failed to check whether can prune by partition id", K(ret));
      } else if (can_prune) {
        if (OB_FAIL(scan_ranges.remove(pos))) {
          LOG_WARN("failed to remove query range", K(ret));
        } else { /*do nothing*/
        }
      } else {
        pos++;
      }
    }
    LOG_DEBUG("range count after pruning", K(scan_ranges.count()));
  }
  return ret;
}

int ObTableScanOp::can_prune_by_partition_id(
    ObSchemaGetterGuard& schema_guard, const int64_t partition_id, ObNewRange& scan_range, bool& can_prune)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObNewRange partition_range;
  ObNewRange subpartition_range;
  can_prune = true;
  if (MY_SPEC.is_vt_mapping_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("virtual table is not partition table", K(ret));
  } else if (OB_FAIL(construct_partition_range(allocator,
                 MY_SPEC.part_type_,
                 MY_SPEC.part_range_pos_,
                 scan_range,
                 MY_SPEC.part_expr_,
                 MY_SPEC.part_dep_cols_,
                 can_prune,
                 partition_range))) {
    LOG_WARN("failed to construct partition range", K(ret));
  } else if (can_prune && OB_FAIL(construct_partition_range(allocator,
                              MY_SPEC.subpart_type_,
                              MY_SPEC.subpart_range_pos_,
                              scan_range,
                              MY_SPEC.subpart_expr_,
                              MY_SPEC.subpart_dep_cols_,
                              can_prune,
                              subpartition_range))) {
    LOG_WARN("failed to construct subpartition range", K(ret));
  } else if (can_prune) {
    ObSEArray<int64_t, 16> partition_ids;
    ObSEArray<int64_t, 16> subpartition_ids;
    if (OB_FAIL(schema_guard.get_part(MY_SPEC.get_location_table_id(),
            ObPartitionLevel::PARTITION_LEVEL_ONE,
            OB_INVALID_INDEX,
            partition_range,
            false,
            partition_ids))) {
      LOG_WARN("failed to get partition ids", K(ret));
    } else if (partition_ids.count() == 0) {
      /*do nothing*/
    } else if (partition_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should have only one partition id", K(partition_ids), K(partition_range), K(ret));
    } else if (ObPartitionLevel::PARTITION_LEVEL_ONE == MY_SPEC.part_level_) {
      if (partition_ids.at(0) == partition_id) {
        can_prune = false;
      }
    } else if (OB_FAIL(schema_guard.get_part(MY_SPEC.get_location_table_id(),
                   ObPartitionLevel::PARTITION_LEVEL_TWO,
                   partition_ids.at(0),
                   subpartition_range,
                   false,
                   subpartition_ids))) {
      LOG_WARN("failed to get subpartition ids", K(subpartition_range), K(ret));
    } else if (subpartition_ids.count() == 0) {
      /*do nothing*/
    } else if (subpartition_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should have only one partition id", K(ret));
    } else if (generate_phy_part_id(partition_ids.at(0), subpartition_ids.at(0)) == partition_id) {
      can_prune = false;
    }
  }
  return ret;
}

int ObTableScanOp::construct_partition_range(ObArenaAllocator& allocator, const ObPartitionFuncType part_type,
    const ObIArray<int64_t>& part_range_pos, const ObNewRange& scan_range, const ObExpr* part_expr,
    const ExprFixedArray& part_dep_cols, bool& can_prune, ObNewRange& part_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_range.start_key_.get_obj_ptr()) || OB_ISNULL(scan_range.end_key_.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(scan_range.start_key_.get_obj_ptr()), K(scan_range.end_key_.get_obj_ptr()), K(ret));
  } else if (OB_UNLIKELY(scan_range.start_key_.get_obj_cnt() != scan_range.end_key_.get_obj_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have the same range key count",
        K(scan_range.start_key_.get_obj_cnt()),
        K(scan_range.end_key_.get_obj_cnt()),
        K(ret));
  } else if (part_range_pos.count() > 0) {
    int64_t range_key_count = part_range_pos.count();
    ObObj* start_row_key = NULL;
    ObObj* end_row_key = NULL;
    ObObj* function_obj = NULL;
    if (OB_ISNULL(start_row_key = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * range_key_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for start_obj failed", K(ret));
    } else if (OB_ISNULL(end_row_key = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * range_key_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for end_obj failed", K(ret));
    } else if (OB_ISNULL(function_obj = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for function obj failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && can_prune && i < range_key_count; i++) {
        int64_t pos = part_range_pos.at(i);
        if (OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos >= scan_range.start_key_.get_obj_cnt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid array pos", K(pos), K(scan_range.start_key_.get_obj_cnt()), K(ret));
        } else if (scan_range.start_key_.get_obj_ptr()[pos].is_max_value() ||
                   scan_range.start_key_.get_obj_ptr()[pos].is_min_value() ||
                   scan_range.end_key_.get_obj_ptr()[pos].is_max_value() ||
                   scan_range.end_key_.get_obj_ptr()[pos].is_min_value()) {
          can_prune = false;
        } else if (scan_range.start_key_.get_obj_ptr()[pos] != scan_range.end_key_.get_obj_ptr()[pos]) {
          can_prune = false;
        } else {
          start_row_key[i] = scan_range.start_key_.get_obj_ptr()[pos];
          end_row_key[i] = scan_range.end_key_.get_obj_ptr()[pos];
          sql::ObExpr* expr = part_dep_cols.at(i);
          sql::ObDatum& datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(start_row_key[i], expr->obj_datum_map_))) {
            LOG_WARN("convert obj to datum failed", K(ret));
          } else {
            expr->get_eval_info(eval_ctx_).evaluated_ = true;
          }
        }
      }
      if (OB_SUCC(ret) && can_prune) {
        if (OB_FAIL(ObSQLUtils::get_partition_range(start_row_key,
                end_row_key,
                function_obj,
                part_type,
                part_expr,
                range_key_count,
                scan_range.table_id_,
                eval_ctx_,
                part_range))) {
          LOG_WARN("get partition real range failed", K(ret));
        }
        LOG_DEBUG("part range info", K(part_range), K(can_prune), K(ret));
      }
    }
  }
  return ret;
}

inline int ObTableScanOp::check_cache_table_version(int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(schema_version != MY_SPEC.schema_version_)) {
    // schema version in the operator and the schema version in the schema service may not be equal
    // because the schema version in the operator may not be set correctly in the old server
    // to be compatible with the old server, if schema version is not equal,
    // go to check the version in physical plan
    int64_t version_in_plan = -1;
    CK(OB_NOT_NULL(MY_SPEC.plan_));
    OZ(MY_SPEC.plan_->get_base_table_version(MY_SPEC.ref_table_id_, version_in_plan), MY_SPEC.ref_table_id_);
    if (OB_SUCC(ret) && schema_version != version_in_plan) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("unexpected schema error, need retry",
          K(ret),
          K(schema_version),
          K(MY_SPEC.schema_version_),
          K(version_in_plan));
    }
  }
  return ret;
}

int ObTableScanOp::reassign_task_and_do_table_scan(ObGranuleTaskInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t location_idx = -1;
  if (OB_FAIL(MY_INPUT.translate_pid_to_ldx(
          info.partition_id_, MY_SPEC.table_location_key_, MY_SPEC.get_location_table_id(), location_idx))) {
  } else {
    MY_INPUT.set_location_idx(location_idx);
    if (info.ranges_.count() > 0 && !info.ranges_.at(0).is_whole_range()) {
      if (ObSQLMockSchemaUtils::is_mock_index(MY_SPEC.index_id_)) {
        // transform rowid range here
        ObNewRange tmp_range;
        info.ranges_.reuse();
        ObSEArray<ObColDesc, 4> rowkey_descs;
        const ObIArray<ObColDesc>& col_descs = MY_SPEC.table_param_.get_col_descs();
        for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.table_param_.get_main_rowkey_cnt(); ++i) {
          if (OB_FAIL(rowkey_descs.push_back(col_descs.at(i)))) {
            LOG_WARN("failed to push col desc", K(ret));
          }
        }
        for (int i = 0; OB_SUCC(ret) && i < info.ranges_.count(); i++) {
          if (OB_FAIL(ObTableScan::transform_rowid_range(ctx_.get_allocator(), rowkey_descs, tmp_range))) {
            LOG_WARN("failed to transform rowid range to rowkey range", K(ret));
          } else if (OB_FAIL(info.ranges_.push_back(tmp_range))) {
            LOG_WARN("failed to push back element", K(ret));
          } else {
            tmp_range.reset();
          }
        }
      }
      if (OB_FAIL(MY_INPUT.reassign_ranges(info.ranges_))) {
        LOG_WARN("the scan input reassign ragnes failed");
      }
    } else {
      // use prepare() to set key ranges if info.range is whole range scan (partition granule).
      MY_INPUT.key_ranges_.reuse();
      LOG_DEBUG("do prepare!!!");
    }
    /*update scan partition key*/
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(update_scan_param_pkey())) {
      LOG_WARN("fail to update scan param pkey", K(ret));
    }
    LOG_TRACE(
        "TSC has been reassign a new task", K(info.partition_id_), K(info.ranges_), K(info.task_id_), K(location_idx));
    /*try to do table scan*/
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOperator::rescan())) {
      LOG_WARN("rescan operator failed", K(ret));
    } else if (OB_FAIL(inner_close())) {
      LOG_WARN("fail to close op", K(ret));
    } else if (OB_FAIL(do_table_scan(true /* rescan */, MY_INPUT.key_ranges_.empty() /* need prepare key ranges */))) {
      LOG_WARN("fail to do table rescan", K(ret));
    }
  }
  return ret;
}

int ObTableScanOp::update_scan_param_pkey()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = ctx_.get_task_exec_ctx();
  const ObPhyTableLocation* tl = NULL;
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
          task_exec_ctx, MY_SPEC.table_location_key_, MY_SPEC.get_location_table_id(), tl))) {
    LOG_WARN("failed to get physical table location", K(ret), K(MY_SPEC.table_location_key_));
  } else if (OB_ISNULL(tl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (tl->get_partition_location_list().count() < 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("empty partition scan is not supported at the moment!",
        "# partitions",
        tl->get_partition_location_list().count());
  } else if (tl->get_partition_location_list().count() <= MY_INPUT.get_location_idx()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("partition index is out-of-range",
        "# partitions",
        tl->get_partition_location_list().count(),
        "location_idx",
        MY_INPUT.get_location_idx());
  } else if (OB_INVALID_INDEX == MY_INPUT.get_location_idx()) {
    iter_end_ = true;
    ret = OB_ITER_END;
    LOG_DEBUG("update scan's pkey meet a invalid index, set table scan to end", K(MY_SPEC.id_), K(this), K(lbt()));
  } else {
    partition_id_ = tl->get_partition_location_list().at(MY_INPUT.get_location_idx()).get_partition_id();

    for (int i = 0; i < MY_SPEC.output_.count(); i++) {
      ObExpr* expr = MY_SPEC.output_.at(i);
      if (expr->type_ == T_PDML_PARTITION_ID) {
        expr->locate_datum_for_write(eval_ctx_).set_int(partition_id_);
        expr->get_eval_info(eval_ctx_).evaluated_ = true;
        LOG_TRACE("find the partition id expr in pdml table scan", K(ret), K(i), K(partition_id_));
      }
    }

    const auto& part_loc = tl->get_partition_location_list().at(MY_INPUT.get_location_idx());

    if (OB_FAIL(part_loc.get_partition_key(scan_param_.pkey_))) {
      LOG_WARN("get partition key fail", K(ret), K(part_loc));
    } else if (NULL != MY_SPEC.expected_part_id_) {
      // Initialize the param datum data pointed to
      // by the partition id expression expected in the partition filter
      ObDatum& param_datum = MY_SPEC.expected_part_id_->locate_expr_datum(eval_ctx_);
      param_datum.ptr_ = reinterpret_cast<char*>(&expected_part_id_);
      param_datum.set_int(scan_param_.pkey_.get_partition_id());
      MY_SPEC.expected_part_id_->get_eval_info(eval_ctx_).evaluated_ = true;
    }
  }
  return ret;
}

int ObTableScanOp::get_gi_task_and_restart()
{
  int ret = OB_SUCCESS;
  ObGranuleTaskInfo gi_task_info;
  GIPrepareTaskMap* gi_prepare_map = nullptr;
  if (OB_FAIL(ctx_.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (OB_FAIL(gi_prepare_map->get_refactored(MY_SPEC.id_, gi_task_info))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get prepare gi task", K(ret), K(MY_SPEC.id_));
    } else {
      // OB_HASH_NOT_EXIST mean no more task for tsc.
      LOG_DEBUG("no prepared task info, set table scan to end", K(MY_SPEC.id_), K(this), K(lbt()));
      iter_end_ = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(reassign_task_and_do_table_scan(gi_task_info))) {
    LOG_WARN("failed to do gi task scan", K(ret));
  } else {
    LOG_DEBUG("TSC consume a task", K(gi_task_info));
  }
  return ret;
}

OB_INLINE void ObTableScanOp::fill_table_scan_stat(
    const ObTableScanStatistic& statistic, ObTableScanStat& scan_stat) const
{
  scan_stat.bf_filter_cnt_ += statistic.bf_filter_cnt_;
  scan_stat.bf_access_cnt_ += statistic.bf_access_cnt_;
  scan_stat.fuse_row_cache_hit_cnt_ += statistic.fuse_row_cache_hit_cnt_;
  scan_stat.fuse_row_cache_miss_cnt_ += statistic.fuse_row_cache_miss_cnt_;
  scan_stat.row_cache_hit_cnt_ += statistic.row_cache_hit_cnt_;
  scan_stat.row_cache_miss_cnt_ += statistic.row_cache_miss_cnt_;
}

void ObTableScanOp::set_cache_stat(const ObPlanStat& plan_stat, ObTableScanParam& param)
{
  const int64_t TRY_USE_CACHE_INTERVAL = 15;
  ObQueryFlag& query_flag = param.scan_flag_;
  bool try_use_cache = !(plan_stat.execute_times_ & TRY_USE_CACHE_INTERVAL);
  if (try_use_cache) {
    query_flag.set_use_row_cache();
    query_flag.set_use_bloomfilter_cache();
  } else {
    if (plan_stat.enable_bf_cache_) {
      query_flag.set_use_bloomfilter_cache();
    } else {
      query_flag.set_not_use_bloomfilter_cache();
    }
    if (plan_stat.enable_row_cache_) {
      query_flag.set_use_row_cache();
    } else {
      query_flag.set_not_use_row_cache();
    }
  }
  const int64_t fuse_row_cache_access_cnt = plan_stat.fuse_row_cache_hit_cnt_ + plan_stat.fuse_row_cache_miss_cnt_;
  if (fuse_row_cache_access_cnt > ObPlanStat::CACHE_ACCESS_THRESHOLD) {
    param.fuse_row_cache_hit_rate_ =
        100.0 * static_cast<double>(plan_stat.fuse_row_cache_hit_cnt_) / static_cast<double>(fuse_row_cache_access_cnt);
  } else {
    param.fuse_row_cache_hit_rate_ = 100L;
  }
  const int64_t block_cache_access_cnt = plan_stat.block_cache_hit_cnt_ + plan_stat.block_cache_miss_cnt_;
  if (block_cache_access_cnt > ObPlanStat::CACHE_ACCESS_THRESHOLD) {
    param.block_cache_hit_rate_ =
        100.0 * static_cast<double>(plan_stat.block_cache_hit_cnt_) / static_cast<double>(block_cache_access_cnt);
  } else {
    param.block_cache_hit_rate_ = 0L;
  }
}

}  // end namespace sql
}  // end namespace oceanbase
