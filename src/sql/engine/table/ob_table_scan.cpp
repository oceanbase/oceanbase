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

#include "share/ob_define.h"
#include "lib/profile/ob_perf_event.h"
#include "share/ob_i_data_access_service.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/executor/ob_task_info.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_table_scan_iterator.h"
#include "share/ob_i_data_access_service.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "common/object/ob_object.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"

#define USE_MULTI_GET_ARRAY_BINDING 1
#define my_range_array_pos \
  (USE_MULTI_GET_ARRAY_BINDING ? scan_ctx->scan_param_.range_array_pos_ : scan_input->range_array_pos_)
namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {
static constexpr int CHECK_STATUS_ROWS_INTERVAL = 10000;

ObTableScanInput::ObTableScanInput() : location_idx_(OB_INVALID_INDEX), key_ranges_(), deserialize_allocator_(NULL)
{}

ObTableScanInput::~ObTableScanInput()
{
  location_idx_ = OB_INVALID_INDEX;
  key_ranges_.reset();
  range_array_pos_.reuse();
  range_array_pos_.reset();
  deserialize_allocator_ = NULL;
}

void ObTableScanInput::reset()
{
  location_idx_ = OB_INVALID_INDEX;
  key_ranges_.reset();
  range_array_pos_.reuse();
  range_array_pos_.reset();
}

int ObTableScanInput::translate_pid_to_ldx(
    ObExecContext& ctx, int64_t partition_id, int64_t table_location_key, int64_t ref_table_id, int64_t& location_idx)
{
  return ObTaskExecutorCtxUtil::translate_pid_to_ldx(
      ctx.get_task_exec_ctx(), partition_id, table_location_key, ref_table_id, location_idx);
}

int ObTableScanInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  UNUSED(ctx);
  UNUSED(op);
  int ret = OB_SUCCESS;
  if (PHY_FAKE_CTE_TABLE == op.get_type()) {
    LOG_DEBUG("op type is CTE TABLE do not need init", K(ret), "op_type", op.get_type());
  } else if (!op.is_table_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op type is not any known kind of TABLE SCAN", K(ret), "op_type", op.get_type());
  } else if (ObTaskSpliter::INVALID_SPLIT == task_info.get_task_split_type()) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec type is INVALID_SPLIT", K(ret));
  } else {
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (ObTaskSpliter::LOCAL_IDENTITY_SPLIT == task_info.get_task_split_type()) {
      location_idx_ = 0;
      // validate
#ifndef NDEBUG
      const ObPhyTableLocation* table_loc = NULL;
      const ObTableScan& scan_op = static_cast<const ObTableScan&>(op);
      if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
              ctx, scan_op.get_table_location_key(), scan_op.get_location_table_id(), table_loc))) {
        LOG_WARN("fail to get table location", K(ret));
      } else if (OB_ISNULL(table_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get phy table location", K(ret));
      } else {
        const ObPartitionReplicaLocationIArray& partition_loc_list = table_loc->get_partition_location_list();
        if (OB_UNLIKELY(partition_loc_list.count() > 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition location list count is greater than 1", K(ret), "count", partition_loc_list.count());
        } else {
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

int ObTableScanInput::reassign_ranges(ObIArray<ObNewRange>& ranges)
{
  return key_ranges_.assign(ranges);
}

int64_t ObTableScanInput::get_location_idx() const
{
  return location_idx_;
}

ObPhyOperatorType ObTableScanInput::get_phy_op_type() const
{
  return PHY_TABLE_SCAN;
}

void ObTableScanInput::set_deserialize_allocator(ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

int ObTableScanInput::deep_copy_range(ObIAllocator* allocator, const ObNewRange& src, ObNewRange& dst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_SUCC(src.start_key_.deep_copy(dst.start_key_, *allocator)) &&
             OB_SUCC(src.end_key_.deep_copy(dst.end_key_, *allocator))) {
    dst.table_id_ = src.table_id_;
    dst.border_flag_ = src.border_flag_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableScanInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTableScanInput);
  LST_DO_CODE(OB_UNIS_ENCODE, location_idx_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, key_ranges_.count()))) {
      LOG_WARN("fail to encode key ranges count", K(ret), K(key_ranges_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
      if (OB_FAIL(key_ranges_.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize key range", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, partition_ranges_.count()))) {
        LOG_WARN("fail to encode partition ranges count", K(ret), K(key_ranges_));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ranges_.count(); ++i) {
      if (OB_FAIL(partition_ranges_.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize key ranges in all partitions", K(ret), K(i));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableScanInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObTableScanInput);
  LST_DO_CODE(OB_UNIS_DECODE, location_idx_);
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    key_ranges_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else {
        ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
        ObNewRange copy_range;
        ObNewRange key_range;
        copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
        copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize range", K(ret));
        } else if (OB_FAIL(deep_copy_range(deserialize_allocator_, copy_range, key_range))) {
          LOG_WARN("fail to deep copy range", K(ret));
        } else if (OB_FAIL(key_ranges_.push_back(key_range))) {
          LOG_WARN("fail to add key range to array", K(ret));
        }
      }
    }
  }
  /* pos <data_len is used for compatibility requirements of version 147 sending serialized data */
  if (OB_SUCC(ret) && pos < data_len) {
    int64_t count = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else {
        ObPartitionScanRanges ranges(deserialize_allocator_);
        if (OB_FAIL(ranges.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize range", K(ret));
        } else if (OB_FAIL(partition_ranges_.push_back(ranges))) {
          LOG_WARN("fail to add key range to array", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableScanInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTableScanInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, location_idx_);
  len += serialization::encoded_length_vi64(key_ranges_.count());
  for (int64_t i = 0; i < key_ranges_.count(); ++i) {
    len += key_ranges_.at(i).get_serialize_size();
  }
  len += serialization::encoded_length_vi64(partition_ranges_.count());
  for (int64_t i = 0; i < partition_ranges_.count(); ++i) {
    len += partition_ranges_.at(i).get_serialize_size();
  }
  return len;
}

int ObTableScan::ObTableScanCtx::init_table_allocator(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(my_session->get_effective_tenant_id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    MemoryContext* mem_context = nullptr;
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context, param))) {
      LOG_WARN("fail to create entity", K(ret));
    } else if (OB_ISNULL(mem_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create entity ", K(ret));
    } else {
      set_table_allocator(&mem_context->get_arena_allocator());
    }
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
      set_scan_iterator_mementity(mem_context);
    }
  }
  return ret;
}

ObTableScan::ObTableScan(ObIAllocator& allocator)
    : ObNoChildrenPhyOperator(allocator),
      ref_table_id_(OB_INVALID_ID),
      index_id_(OB_INVALID_ID),
      is_index_global_(false),
      output_column_ids_(allocator),
      filter_before_index_back_(allocator),
      flags_(0),
      limit_(allocator, 0),
      offset_(allocator, 0),
      for_update_(false),
      for_update_wait_us_(-1), /* default infinite */
      hint_(),
      exist_hint_(false),
      is_get_(false),
      pre_query_range_(allocator),
      table_location_key_(OB_INVALID_ID),
      is_top_table_scan_(false),
      table_param_(allocator),
      schema_version_(-1),
      part_level_(ObPartitionLevel::PARTITION_LEVEL_MAX),
      part_type_(ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
      subpart_type_(ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
      part_expr_(allocator, 0),
      subpart_expr_(allocator, 0),
      part_range_pos_(allocator),
      subpart_range_pos_(allocator),
      optimization_method_(MAX_METHOD),
      available_index_count_(0),
      table_row_count_(0),
      output_row_count_(0),
      phy_query_range_row_count_(0),
      query_range_row_count_(0),
      index_back_row_count_(0),
      estimate_method_(INVALID_METHOD),
      est_records_(allocator),
      available_index_name_(allocator),
      pruned_index_name_(allocator),
      gi_above_(false),
      is_vt_mapping_(false),
      use_real_tenant_id_(false),
      has_tenant_id_col_(false),
      vt_table_id_(UINT64_MAX),
      real_schema_version_(INT64_MAX),
      output_row_types_(allocator),
      key_types_(allocator),
      key_with_tenant_ids_(allocator),
      has_extra_tenant_ids_(allocator),
      org_output_column_ids_(allocator),
      part_filter_(allocator),
      is_whole_range_scan_(false),
      batch_scan_flag_(false),
      need_scn_(false)
{}

ObTableScan::~ObTableScan()
{}

void ObTableScan::reset()
{
  ref_table_id_ = OB_INVALID_ID;
  index_id_ = OB_INVALID_ID;
  is_index_global_ = false;
  output_column_ids_.reset();
  filter_before_index_back_.reset();
  flags_ = 0;
  limit_.reset();
  offset_.reset();
  for_update_ = false;
  for_update_wait_us_ = -1;
  hint_.reset();
  exist_hint_ = false;
  is_get_ = false;
  pre_query_range_.reset();
  table_location_key_ = OB_INVALID_ID;
  is_top_table_scan_ = false;
  table_param_.reset();
  schema_version_ = -1;
  part_level_ = ObPartitionLevel::PARTITION_LEVEL_MAX;
  part_type_ = ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;
  subpart_type_ = ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;
  part_expr_.reset();
  subpart_expr_.reset();
  part_range_pos_.reset();
  subpart_range_pos_.reset();
  optimization_method_ = MAX_METHOD;
  available_index_count_ = 0;
  table_row_count_ = 0;
  output_row_count_ = 0;
  phy_query_range_row_count_ = 0;
  query_range_row_count_ = 0;
  index_back_row_count_ = 0;
  estimate_method_ = INVALID_METHOD;
  part_filter_.reset();
  ObNoChildrenPhyOperator::reset();
}

void ObTableScan::reuse()
{
  ref_table_id_ = OB_INVALID_ID;
  index_id_ = OB_INVALID_ID;
  is_index_global_ = false;
  output_column_ids_.reset();
  filter_before_index_back_.reset();
  flags_ = 0;
  limit_.reset();
  offset_.reset();
  for_update_ = false;
  for_update_wait_us_ = -1;
  hint_.reset();
  exist_hint_ = false;
  is_get_ = false;
  pre_query_range_.reset();
  table_location_key_ = OB_INVALID_ID;
  is_top_table_scan_ = false;
  table_param_.reset();
  schema_version_ = -1;
  part_level_ = ObPartitionLevel::PARTITION_LEVEL_MAX;
  part_type_ = ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;
  subpart_type_ = ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX;
  part_expr_.reset();
  subpart_expr_.reset();
  part_range_pos_.reset();
  subpart_range_pos_.reset();
  optimization_method_ = MAX_METHOD;
  available_index_count_ = 0;
  table_row_count_ = 0;
  output_row_count_ = 0;
  phy_query_range_row_count_ = 0;
  query_range_row_count_ = 0;
  index_back_row_count_ = 0;
  estimate_method_ = INVALID_METHOD;
  ObNoChildrenPhyOperator::reuse();
}

int ObTableScan::get_partition_service(ObTaskExecutorCtx& executor_ctx, ObIDataAccessService*& das) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSQLUtils::get_partition_service(executor_ctx, ref_table_id_, das))) {
    LOG_WARN("fail to get partition service", K(ret));
  }
  return ret;
}

int ObTableScan::prepare_scan_param(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  const ObPhyTableLocation* table_location = NULL;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)) || OB_ISNULL(plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get plan ctx", K(ret), KP(plan_ctx));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 *executor_ctx, table_location_key_, get_location_table_id(), table_location))) {
    LOG_WARN("failed to get physical table location", K(table_location_key_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (table_location->get_partition_location_list().count() < 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("empty partition scan is not supported at the moment!",
        "# partitions",
        table_location->get_partition_location_list().count());
  } else if (table_location->get_partition_location_list().count() <= scan_input->get_location_idx()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("partition index is out-of-range",
        "# partitions",
        table_location->get_partition_location_list().count(),
        "location_idx",
        scan_input->get_location_idx());
  } else if (OB_INVALID_INDEX == scan_input->get_location_idx()) {
    scan_ctx->iter_end_ = true;
    ret = OB_ITER_END;
    LOG_DEBUG("scan input has a invalid location idx", K(ret), K(get_id()), K(this), K(lbt()));
  } else {
    scan_ctx->partition_id_ =
        table_location->get_partition_location_list().at(scan_input->get_location_idx()).get_partition_id();
    // update expr ctx partition id
    scan_ctx->expr_ctx_.pdml_partition_id_ = scan_ctx->partition_id_;  // just for pdml
  }
  /* step 2 */
  if (OB_SUCC(ret)) {
    scan_ctx->scan_param_.ref_table_id_ = ref_table_id_;
    scan_ctx->scan_param_.index_id_ = ObSQLMockSchemaUtils::is_mock_index(index_id_)
                                          ? ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(index_id_)
                                          : index_id_;
    scan_ctx->scan_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    scan_ctx->scan_param_.scan_flag_.flag_ = flags_;
    scan_ctx->scan_param_.scan_flag_.is_large_query_ = plan_ctx->is_large_query();
    set_cache_stat(plan_ctx->get_phy_plan()->stat_, scan_ctx->scan_param_);
    scan_ctx->scan_param_.reserved_cell_count_ = column_count_;
    // Storage engine convention: if for_update_wait_us_>0,
    // you need to pass in the absolute timeout timestamp
    scan_ctx->scan_param_.for_update_ = for_update_;
    scan_ctx->scan_param_.for_update_wait_timeout_ =
        for_update_wait_us_ > 0 ? for_update_wait_us_ + my_session->get_query_start_time() : for_update_wait_us_;
    scan_ctx->scan_param_.sql_mode_ = my_session->get_sql_mode();
    scan_ctx->scan_param_.scan_allocator_ = &ctx.get_allocator();  // used by virtual table
    if (exist_hint_) {
      scan_ctx->scan_param_.frozen_version_ = hint_.frozen_version_;
      scan_ctx->scan_param_.force_refresh_lc_ = hint_.force_refresh_lc_;
    }
    // multi-partition scan is not supported at the moment! so hard code 0 below
    const share::ObPartitionReplicaLocation& part_loc =
        table_location->get_partition_location_list().at(scan_input->get_location_idx());

    if (OB_FAIL(part_loc.get_partition_key(scan_ctx->scan_param_.pkey_))) {
      LOG_WARN("get partition key fail", K(ret), K(part_loc));
    } else if (OB_FAIL(my_phy_plan_->get_base_table_version(ref_table_id_, scan_ctx->scan_param_.schema_version_))) {
      LOG_WARN("get base table version failed", K(ret), K_(ref_table_id));
    } else if (is_vt_mapping_ && FALSE_IT(scan_ctx->scan_param_.schema_version_ = real_schema_version_)) {
    }
  }
  if (OB_SUCC(ret)) {
    scan_ctx->scan_param_.column_ids_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      if (OB_FAIL(scan_ctx->scan_param_.column_ids_.push_back(output_column_ids_.at(i)))) {
        LOG_WARN("fail to push back key range", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t i = 0;
    scan_ctx->scan_param_.filters_.reuse();
    if (filter_exprs_.get_size() != filter_before_index_back_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "Filter before index back flags count not match!", K(filter_exprs_), K(filter_before_index_back_), K(ret));
    } else if (!is_vt_mapping_) {
      DLIST_FOREACH(node, filter_exprs_)
      {
        ObSqlExpression* expr = const_cast<ObSqlExpression*>(node);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node or node expr is NULL", K(ret));
        } else {
          if (filter_before_index_back_.at(i)) {
            ret = scan_ctx->scan_param_.filters_before_index_back_.push_back(expr);
          } else {
            ret = scan_ctx->scan_param_.filters_.push_back(expr);
          }
          ++i;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t i = 0;
    scan_ctx->scan_param_.virtual_column_exprs_.reuse();
    DLIST_FOREACH(node, virtual_column_exprs_)
    {
      const ObColumnExpression* expr = static_cast<const ObColumnExpression*>(node);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node or node expr is NULL", K(ret));
      } else if (OB_FAIL(scan_ctx->scan_param_.virtual_column_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        ++i;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(wrap_expr_ctx(ctx, scan_ctx->scan_param_.expr_ctx_))) {
      LOG_WARN("fail to wrap expr ctx", K(ctx));
    } else {
      scan_ctx->scan_param_.limit_param_.limit_ = -1;
      scan_ctx->scan_param_.limit_param_.offset_ = 0;
      scan_ctx->scan_param_.trans_desc_ = &my_session->get_trans_desc();
      bool is_null_value = false;
      if (!limit_.is_empty()) {
        if (OB_FAIL(calc_expr_int_value(
                scan_ctx->scan_param_.expr_ctx_, limit_, scan_ctx->scan_param_.limit_param_.limit_, is_null_value))) {
          LOG_WARN("fail get val", K(ret));
        } else if (scan_ctx->scan_param_.limit_param_.limit_ < 0) {
          scan_ctx->scan_param_.limit_param_.limit_ = 0;
        }
      }

      if (OB_SUCC(ret) && !offset_.is_empty() && !is_null_value) {
        if (OB_FAIL(calc_expr_int_value(
                scan_ctx->scan_param_.expr_ctx_, offset_, scan_ctx->scan_param_.limit_param_.offset_, is_null_value))) {
          LOG_WARN("fail get val", K(ret));
        } else if (scan_ctx->scan_param_.limit_param_.offset_ < 0) {
          scan_ctx->scan_param_.limit_param_.offset_ = 0;
        } else if (is_null_value) {
          scan_ctx->scan_param_.limit_param_.limit_ = 0;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t schema_version = is_sys_table(ref_table_id_) ? executor_ctx->get_query_sys_begin_schema_version()
                                                         : executor_ctx->get_query_tenant_begin_schema_version();
    scan_ctx->scan_param_.query_begin_schema_version_ = schema_version;
    scan_ctx->scan_param_.table_param_ = &table_param_;
  }
  if (OB_SUCC(ret) && part_filter_.is_inited()) {
    // inner table does not support split operation temporarily
    // Partition table without primary key does not support split operation temporarily
    bool is_get = false;
    int64_t schema_version = 0;
    scan_ctx->scan_param_.part_filter_ = &part_filter_;
    if (OB_FAIL(pre_query_range_.is_get(is_get))) {
      LOG_WARN("extract pre query range get info failed", K(ret));
    } else if (!is_get) {
      // For table get, there is no need to do partition pruning
      if (OB_ISNULL(executor_ctx->schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service is null");
      } else if (OB_FAIL(executor_ctx->schema_service_->get_tenant_schema_guard(
                     my_session->get_effective_tenant_id(), scan_ctx->schema_guard_))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(scan_ctx->schema_guard_.get_table_schema_version(get_ref_table_id(), schema_version))) {
        LOG_WARN("get table schema version failed", K(ret), K(get_ref_table_id()));
      } else if (OB_UNLIKELY(schema_version != scan_ctx->scan_param_.schema_version_)) {
        if (OB_INVALID_VERSION == schema_version) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", K(ret), K(schema_version), K(get_ref_table_id()));
        } else {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN(
              "unexpected schema version, need retry", K(schema_version), K(scan_ctx->scan_param_.schema_version_));
        }
      } else {
        scan_ctx->scan_param_.part_mgr_ = &scan_ctx->schema_guard_;
      }
    }
  }
  return ret;
}

int ObTableScan::prepare(ObExecContext& ctx, bool is_rescan) const
{
  int ret = OB_SUCCESS;
  ObQueryRangeArray key_ranges;
  ObGetMethodArray get_method;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  bool is_partition_list_empty = false;

  if (OB_UNLIKELY(!need_extract_range())) {
    // virtual table, do nothing
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get operator context", K(ret));
  } else if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_UNLIKELY(is_partition_list_empty = partition_list_is_empty(task_exec_ctx.get_table_locations()))) {
    // do nothing is no partitions
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx()) || OB_ISNULL(scan_ctx->table_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret));
  } else if (OB_FAIL(ObSQLUtils::extract_pre_query_range(pre_query_range_,
                 is_rescan ? *scan_ctx->table_allocator_ : ctx.get_allocator(),
                 plan_ctx->get_param_store(),
                 key_ranges,
                 get_method,
                 ObBasicSessionInfo::create_dtc_params(ctx.get_my_session()),
                 &my_range_array_pos))) {
    LOG_WARN("failed to extract pre query ranges", K(ret));
  } else if (OB_UNLIKELY(filter_exprs_.get_size() != filter_before_index_back_.count())) {
    LOG_WARN("filter before index back count not match",
        K(ret),
        "filter count",
        filter_exprs_.get_size(),
        "flag count",
        filter_before_index_back_.count());
  } else {
    if (USE_MULTI_GET_ARRAY_BINDING && !my_range_array_pos.empty()) {
      scan_ctx->scan_param_.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }
    // transform rowid query ranges to primary key ranges
    if (ObSQLMockSchemaUtils::is_mock_index(index_id_) &&
        OB_FAIL(transform_rowid_ranges(
            is_rescan ? *scan_ctx->table_allocator_ : ctx.get_allocator(), table_param_, index_id_, key_ranges))) {
      LOG_WARN("failed to transform rowid ranges", K(ret));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 < scan_input->key_ranges_.count() && !is_rescan) {
      // intra-partition parallelism, do nothing
    } else {
      scan_input->key_ranges_.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
        ObNewRange* key_range = key_ranges.at(i);
        if (OB_ISNULL(key_range)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("key range is null", K(ret), K(i));
        } else {
          if (should_scan_index()) {
            key_range->table_id_ = index_id_;
          } else {
            key_range->table_id_ = ref_table_id_;
          }
          ret = scan_input->key_ranges_.push_back(*key_ranges.at(i));
        }
      }
    }
    LOG_DEBUG("table scan final range",
        K(key_ranges),
        K(scan_ctx->scan_param_.range_array_pos_),
        K(scan_input->range_array_pos_),
        K(scan_input->key_ranges_));
    if (OB_SUCC(ret) && plan_ctx->get_bind_array_count() > 0) {
      if (my_range_array_pos.empty() && filter_exprs_.get_size() > 0) {
        // filter contain bind array condition, but query range is normal condtion, not support
        // because bind array condition must be driven by multi table iterator
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Bind array with filter");
      }
    }
  }
  if (NULL != scan_ctx) {
    scan_ctx->set_partition_list_empty(is_partition_list_empty);
  }
  return ret;
}

bool ObTableScan::partition_list_is_empty(const ObPhyTableLocationIArray& phy_table_locs) const
{
  bool part_list_is_empty = true;
  int ret = OB_SUCCESS;
  const int64_t loc_cnt = phy_table_locs.count();
  const uint64_t loc_table_id = get_location_table_id();
  for (int64_t i = 0; OB_SUCC(ret) && part_list_is_empty && i < loc_cnt; ++i) {
    const ObPhyTableLocation& phy_table_loc = phy_table_locs.at(i);
    if (loc_table_id == phy_table_loc.get_ref_table_id()) {
      const ObPartitionReplicaLocationIArray& part_locs = phy_table_loc.get_partition_location_list();
      if (OB_UNLIKELY(0 != part_locs.count())) {
        part_list_is_empty = false;
      }
    }
  }
  LOG_DEBUG("is partition list empty", K(part_list_is_empty));
  return part_list_is_empty;
}

int ObTableScan::init_converter(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (is_vt_mapping_) {
    ObTableScanCtx* scan_ctx = NULL;
    ObSqlCtx* sql_ctx = NULL;
    if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get op ctx", K(ret));
    } else if (ref_table_id_ != index_id_ || is_index_global_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table id is not match", K(ret), K(ref_table_id_), K(index_id_), K(is_index_global_));
    } else if (OB_ISNULL(sql_ctx = ctx.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: sql ctx or schema guard is null", K(ret));
    } else {
      if (OB_NOT_NULL(scan_ctx->vt_result_converter_)) {
        scan_ctx->vt_result_converter_->destroy();
        scan_ctx->vt_result_converter_->~ObVirtualTableResultConverter();
        scan_ctx->vt_result_converter_ = nullptr;
      }
      const ObTableSchema* org_table_schema = NULL;
      void* buf = ctx.get_allocator().alloc(sizeof(ObVirtualTableResultConverter));
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator", K(ret));
      } else if (FALSE_IT(scan_ctx->vt_result_converter_ = new (buf) ObVirtualTableResultConverter)) {
      } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(vt_table_id_, org_table_schema))) {
        LOG_WARN("get table schema failed", K(vt_table_id_), K(ret));
      } else if (OB_FAIL(scan_ctx->vt_result_converter_->reset_and_init(scan_ctx->table_allocator_,
                     GET_MY_SESSION(ctx),
                     &output_row_types_,
                     &key_types_,
                     &key_with_tenant_ids_,
                     &has_extra_tenant_ids_,
                     &scan_ctx->scan_param_.iterator_mementity_->get_malloc_allocator(),
                     org_table_schema,
                     &org_output_column_ids_,
                     use_real_tenant_id_,
                     has_tenant_id_col_,
                     max(column_count_, table_param_.get_col_descs().count())))) {
        LOG_WARN("failed to init converter", K(ret));
      }
    }
    LOG_TRACE("debug init converter", K(ret), K(ref_table_id_));
  }
  return ret;
}

int ObTableScan::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = ctx.get_task_executor_ctx();
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task exec ctx is NULL", K(ret));
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_UNLIKELY(partition_list_is_empty(task_exec_ctx->get_table_locations()))) {
    // do nothing is no partitions
  } else if (OB_FAIL(prepare_scan_param(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("prepare scan param failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (is_vt_mapping_ && OB_FAIL(init_converter(ctx))) {
    LOG_WARN("failed to init converter", K(ret));
  } else if (gi_above_) {
    if (OB_FAIL(get_gi_task_and_restart(ctx))) {
      LOG_WARN("fail to get gi task and scan", K(ret));
    }
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(do_table_scan(ctx, false))) {
    if (is_data_not_readable_err(ret)) {
      // Indicates that the current copy is behind or unreadable, add the current addr to session
      // invalid servers to avoid invalid retries.
      ObQueryRetryInfo& retry_info = my_session->get_retry_info_for_update();
      int add_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (add_ret = retry_info.add_invalid_server_distinctly(ctx.get_addr(), true)))) {
        LOG_WARN(
            "fail to add addr to invalid servers distinctly", K(ret), K(add_ret), K(ctx.get_addr()), K(retry_info));
      }
    } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to do table scan", K(ret));
    }
  } else {
    LOG_DEBUG("open table scan normal", K(ret), K(ref_table_id_));
  }
  return ret;
}

int ObTableScan::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIDataAccessService* das = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObPhysicalPlanCtx* plan_ctx = NULL;

  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    // The case of failure without creating ctx will come to this, it is normal, do nothing.
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret));
  } else if (get_batch_scan_flag() ? NULL == scan_ctx->result_iters_ : NULL == scan_ctx->result_) {
    // this is normal case, so we need NOT set ret or LOG_WARN.
  } else {
    if (OB_FAIL(fill_storage_feedback_info(ctx))) {
      LOG_WARN("failed to fill storage feedback info", K(ret));
    } else if (OB_FAIL(get_partition_service(task_exec_ctx, das))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get data access service", K(ret));
    } else if (plan_ctx->get_bind_array_count() > 0 && !scan_ctx->scan_param_.range_array_pos_.empty()) {
      if (OB_FAIL(das->revert_scan_iter(scan_ctx->iter_result_))) {
        LOG_WARN("fail to revert row iterator", K(ret));
      }
    } else if (get_batch_scan_flag()) {
      if (OB_FAIL(das->revert_scan_iter(scan_ctx->result_iters_))) {
        LOG_ERROR("fail to revert row iterator", K(ret));
      }
    } else if (OB_FAIL(das->revert_scan_iter(scan_ctx->result_))) {
      LOG_ERROR("fail to revert row iterator", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(scan_ctx->table_allocator_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        scan_ctx->table_allocator_->reuse();
        if (OB_NOT_NULL(scan_ctx->scan_param_.iterator_mementity_)) {
          scan_ctx->scan_param_.iterator_mementity_->reuse();
        }
        scan_ctx->scan_param_.allocator_->reuse();
        scan_ctx->result_ = NULL;
        scan_ctx->result_iters_ = NULL;
        scan_ctx->iter_result_ = NULL;
        scan_ctx->output_row_count_ = -1;
        scan_ctx->iter_end_ = false;
      }
    }
  }

  return ret;
}

int ObTableScan::fill_storage_feedback_info(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = NULL;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret));
  } else if (scan_ctx->got_feedback_) {
    // do nothing
  } else {
    scan_ctx->got_feedback_ = true;
    // fill storage feedback info for acs
    bool is_index_back = scan_ctx->scan_param_.scan_flag_.index_back_;
    ObTableScanStat& table_scan_stat = ctx.get_physical_plan_ctx()->get_table_scan_stat();
    if (should_scan_index()) {
      table_scan_stat.query_range_row_count_ = scan_ctx->scan_param_.idx_table_scan_stat_.access_row_cnt_;
      if (is_index_back) {
        table_scan_stat.indexback_row_count_ = scan_ctx->scan_param_.idx_table_scan_stat_.out_row_cnt_;
        table_scan_stat.output_row_count_ = scan_ctx->scan_param_.main_table_scan_stat_.out_row_cnt_;
      } else {
        table_scan_stat.indexback_row_count_ = -1;
        table_scan_stat.output_row_count_ = scan_ctx->scan_param_.idx_table_scan_stat_.out_row_cnt_;
      }
      LOG_DEBUG("index scan feedback info for acs", K(scan_ctx->scan_param_.idx_table_scan_stat_), K(table_scan_stat));
    } else {
      table_scan_stat.query_range_row_count_ = scan_ctx->scan_param_.main_table_scan_stat_.access_row_cnt_;
      table_scan_stat.indexback_row_count_ = -1;
      table_scan_stat.output_row_count_ = scan_ctx->scan_param_.main_table_scan_stat_.out_row_cnt_;
      LOG_DEBUG("table scan feedback info for acs", K(scan_ctx->scan_param_.main_table_scan_stat_), K(table_scan_stat));
    }

    ObIArray<ObTableRowCount>& table_row_count_list = ctx.get_physical_plan_ctx()->get_table_row_count_list();
    if (should_scan_index() && scan_ctx->scan_param_.scan_flag_.is_index_back()) {
      if (scan_ctx->scan_param_.scan_flag_.is_need_feedback()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = table_row_count_list.push_back(ObTableRowCount(
                               get_id(), scan_ctx->scan_param_.idx_table_scan_stat_.access_row_cnt_)))) {
          LOG_WARN("push back table_id-row_count failed",
              K(ref_table_id_),
              K(tmp_ret),
              "access row count",
              scan_ctx->scan_param_.idx_table_scan_stat_.access_row_cnt_);
        }
      }
    } else {
      if (scan_ctx->scan_param_.scan_flag_.is_need_feedback()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = table_row_count_list.push_back(ObTableRowCount(
                               get_id(), scan_ctx->scan_param_.main_table_scan_stat_.access_row_cnt_)))) {
          LOG_WARN("push back table_id-row_count failed but we won't stop execution", K(tmp_ret));
        }
      }
    }
    LOG_DEBUG("table scan feed back info for buffer table",
        K(ref_table_id_),
        K(index_id_),
        K(should_scan_index()),
        "is_need_feedback",
        scan_ctx->scan_param_.scan_flag_.is_need_feedback(),
        "idx access row count",
        scan_ctx->scan_param_.idx_table_scan_stat_.access_row_cnt_,
        "main access row count",
        scan_ctx->scan_param_.main_table_scan_stat_.access_row_cnt_);
  }
  return ret;
}

int ObTableScan::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  if (ctx.is_gi_restart()) {
    // this scan is started by a gi operator, so, scan a new range.
    if (OB_FAIL(get_gi_task_and_restart(ctx))) {
      LOG_WARN("fail to get gi task and scan", K(ret));
    }
  } else if (get_batch_scan_flag()) {
    // do nothing
    // batch scan will do rescan by itself
  } else if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get op ctx", K(ret));
  } else if (OB_INVALID_INDEX == scan_input->get_location_idx()) {
    // nil scan
    scan_ctx->iter_end_ = true;
    if (OB_UNLIKELY(NULL != scan_ctx->result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("scan_ctx->result_ should be NULL", K(ret));
    }
  } else if (is_virtual_table(ref_table_id_) || for_update_) {
    // The lock of select for update is currently performed independently in
    // ObPartitionStorage::table_scan, not in the process of get_next_row
    // So in the case of for update, the optimization of rescan cannot be used at present
    ret = vt_rescan(ctx);
  } else if (!my_range_array_pos.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Batch iterator rescan");
  } else {
    ret = rt_rescan(ctx);
  }
  return ret;
}

int ObTableScan::vt_rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_FAIL(inner_close(ctx))) {
    LOG_WARN("fail to close op", K(ret));
  } else if (OB_FAIL(do_table_scan(ctx, true))) {
    LOG_WARN("fail to do table rescan", K(ret));
  }
  return ret;
}

int ObTableScan::rt_rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reset_query_range(ctx))) {
    LOG_WARN("failed to reset query range", K(ret));
  } else if (OB_FAIL(add_query_range(ctx))) {
    LOG_WARN("failed to add query range", K(ret));
  } else if (OB_FAIL(rescan_after_adding_query_range(ctx))) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
  }
  return ret;
}

/*
 * the following three functions are used for blocked nested loop join
 */
int ObTableScan::reset_query_range(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = NULL;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(scan_ctx->table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    scan_ctx->table_allocator_->reuse();  // reset allocator for prepare
    scan_ctx->scan_param_.key_ranges_.reuse();
    scan_ctx->scan_param_.range_array_pos_.reuse();
  }
  return ret;
}

int ObTableScan::add_query_range(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else {
    if (OB_FAIL(prepare(ctx, true))) {  // prepare scan input param
      LOG_WARN("fail to prepare scan param", K(ret));
    } else {
      if (OB_FAIL(append(scan_ctx->scan_param_.key_ranges_, scan_input->key_ranges_))) {
        LOG_WARN("failed to append key ranges", K(ret));
      } else if (scan_input->key_ranges_.count() == 0) {
        /*do nothing*/
      } else if (OB_FAIL(prune_query_range_by_partition_id(ctx, scan_ctx->scan_param_.key_ranges_))) {
        LOG_WARN("failed to prune query range by partition id", K(ret));
      } else if (scan_ctx->scan_param_.key_ranges_.count() == 0) {
        if (OB_FAIL(scan_ctx->scan_param_.key_ranges_.push_back(scan_input->key_ranges_.at(0)))) {
          LOG_WARN("failed to push back query range", K(ret));
        } else { /*do nothing*/
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTableScan::rescan_after_adding_query_range(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id());

  if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(scan_ctx->result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scan_ctx->result_ is NULL", K(ret));
  } else if (is_vt_mapping_ &&
             OB_FAIL(scan_ctx->vt_result_converter_->convert_key_ranges(scan_ctx->scan_param_.key_ranges_))) {
    LOG_WARN("failed to convert key ranges", K(ret));
  } else if (OB_FAIL(static_cast<storage::ObTableScanIterator*>(scan_ctx->result_)->rescan(scan_ctx->scan_param_))) {
    LOG_WARN("failed to rescan", K(ret), "scan_param", scan_ctx->scan_param_);
  } else {
    scan_ctx->iter_end_ = false;
  }
  return ret;
}

// @see ObNestedLoopJoin::batch_index_scan_get_next
int ObTableScan::batch_rescan_init(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_INVALID_INDEX == scan_input->get_location_idx()) {
    // nil scan
    if (OB_UNLIKELY(NULL != scan_ctx->result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("scan_ctx->result_ should be NULL", K(ret));
    }
  } else if (OB_ISNULL(scan_ctx->table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    scan_ctx->scan_param_.key_ranges_.reuse();
    scan_ctx->table_allocator_->reuse();  // reset allocator for prepare
    // batch rescan should always use KeepOrder scan order
    ObQueryFlag query_flag = scan_ctx->scan_param_.scan_flag_;
    query_flag.scan_order_ = ObQueryFlag::KeepOrder;
    scan_ctx->scan_param_.scan_flag_ = query_flag;
  }
  return ret;
}

int ObTableScan::batch_rescan_add_key(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = NULL;
  ObTableScanInput* scan_input = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_INVALID_INDEX == scan_input->get_location_idx()) {
    // nil scan
    if (OB_UNLIKELY(NULL != scan_ctx->result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("scan_ctx->result_ should be NULL", K(ret));
    }
  } else if (OB_FAIL(prepare(ctx, true))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  } else if (1 != scan_input->key_ranges_.count() ||
             (!scan_input->key_ranges_.at(0).is_single_rowkey() && !scan_input->key_ranges_.at(0).empty())) {
    ret = OB_NOT_SUPPORTED;
    if (1 == scan_input->key_ranges_.count()) {
      LOG_ERROR("batch_rescan support get/seek but not scan",
          K(ret),
          "range_count",
          scan_input->key_ranges_.count(),
          "range",
          scan_input->key_ranges_.at(0));
    } else {
      LOG_ERROR("batch_rescan support get/seek but not scan", K(ret), "range_count", scan_input->key_ranges_.count());
    }
  } else if (OB_FAIL(scan_ctx->scan_param_.key_ranges_.push_back(scan_input->key_ranges_.at(0)))) {
    LOG_WARN("fail to push back key range", K(ret));
  }

  return ret;
}

int ObTableScan::batch_rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = NULL;
  ObTableScanInput* scan_input = NULL;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id())) ||
      OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(scan_ctx), K(scan_input));
  } else if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_INVALID_INDEX == scan_input->get_location_idx()) {
    // nill scan
    scan_ctx->iter_end_ = true;
    LOG_DEBUG("batch_rescan got a invalid index, set table scan to end", K(get_id()), K(this), K(lbt()));
    if (OB_UNLIKELY(NULL != scan_ctx->result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("scan_ctx->result_ should be NULL", K(ret));
    }
  } else if (OB_ISNULL(scan_ctx->result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scan_ctx->result_ is NULL", K(ret));
  } else if (is_vt_mapping_ &&
             OB_FAIL(scan_ctx->vt_result_converter_->convert_key_ranges(scan_ctx->scan_param_.key_ranges_))) {
    LOG_WARN("failed to convert key ranges", K(ret));
  } else if (OB_FAIL(static_cast<storage::ObTableScanIterator*>(scan_ctx->result_)->rescan(scan_ctx->scan_param_))) {
    LOG_WARN("failed to rescan", K(ret), "scan_param", scan_ctx->scan_param_);
  } else {
    NG_TRACE_TIMES(10, rescan);
  }
  return ret;
}

int ObTableScan::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id());
  ObTableScanInput* scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id());
  if (OB_ISNULL(scan_ctx) || OB_ISNULL(scan_ctx->result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_ctx is null", K(ret), K(scan_ctx));
  } else if (OB_LIKELY(my_range_array_pos.count() <= 1)) {
    ret = OB_ITER_END;
  } else {
    ++scan_ctx->expr_ctx_.cur_array_index_;
  }
  if (OB_SUCC(ret)) {
    if (scan_ctx->expr_ctx_.cur_array_index_ >= my_range_array_pos.count()) {
      ret = OB_ITER_END;
    }
  }
  if (OB_SUCC(ret)) {
    if (!scan_input->range_array_pos_.empty()) {
      if (OB_FAIL(extract_scan_ranges(*scan_input, *scan_ctx))) {
        LOG_WARN("extract scan ranges failed", K(ret));
      } else if (is_vt_mapping_ &&
                 OB_FAIL(scan_ctx->vt_result_converter_->convert_key_ranges(scan_ctx->scan_param_.key_ranges_))) {
        LOG_WARN("failed to convert key ranges", K(ret));
      } else if (OB_FAIL(
                     static_cast<storage::ObTableScanIterator*>(scan_ctx->result_)->rescan(scan_ctx->scan_param_))) {
        LOG_WARN("failed to rescan", K(ret), "scan_param", scan_ctx->scan_param_);
      }
    } else if (OB_ISNULL(scan_ctx->iter_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter result is null", K(ret));
    } else if (OB_FAIL(scan_ctx->iter_result_->get_next_iter(scan_ctx->result_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next iterator failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    scan_ctx->is_filtered_has_set_ = false;
    scan_ctx->iter_end_ = false;
  }
  return ret;
}

int ObTableScan::group_rescan_init(ObExecContext& ctx, int64_t size) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = NULL;
  if (OB_UNLIKELY(size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(reset_query_range(ctx))) {
    LOG_WARN("failed to reset query ranges", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_FAIL(scan_ctx->scan_param_.key_ranges_.reserve(size))) {
    LOG_WARN("failed to reserve space", K(ret));
  } else if (OB_FAIL(scan_ctx->scan_param_.range_array_pos_.reserve(size))) {
    LOG_WARN("failed to reserve space", K(ret));
  } else {
    scan_ctx->scan_param_.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
  }
  return ret;
}

int ObTableScan::group_add_query_range(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_FAIL(prepare(ctx, true))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  } else if (OB_FAIL(append(scan_ctx->scan_param_.key_ranges_, scan_input->key_ranges_))) {
    LOG_WARN("failed to append key ranges", K(ret));
  } else if (OB_FAIL(scan_ctx->scan_param_.range_array_pos_.push_back(scan_ctx->scan_param_.key_ranges_.count() - 1))) {
    LOG_WARN("failed to push back range array pos", K(ret));
  } else {
    LOG_DEBUG("group add query range", K(scan_input->key_ranges_), K(scan_ctx->scan_param_.range_array_pos_));
  }
  return ret;
}

int ObTableScan::group_rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanCtx* scan_ctx = NULL;
  ObTableScanInput* scan_input = NULL;
  if (OB_UNLIKELY(is_virtual_table(ref_table_id_) || for_update_)) {
    // Processing is_virtual_table(ref_table_id_) || for_update_:
    // The lock of select for update is currently performed independently in ObPartitionStorage::table_scan,
    // not in the process of get_next_row.
    // So in the case of for update, the optimization of rescan cannot be used at present
    // The above two cases do not go rescan. The optimizer guarantees not to go batch
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to perform table scan for update or on virtual table", K(ret));
  } else if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get scan_input", K(ret));
  } else if (OB_INVALID_INDEX == scan_input->get_location_idx()) {
    scan_ctx->iter_end_ = true;
    LOG_DEBUG("this a mock table scan", K(ret), K(scan_input->get_location_idx()));
  } else if (OB_ISNULL(scan_ctx->result_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scan_ctx->result_iter_ is NULL", K(ret));
  } else if (is_vt_mapping_ &&
             OB_FAIL(scan_ctx->vt_result_converter_->convert_key_ranges(scan_ctx->scan_param_.key_ranges_))) {
    LOG_WARN("failed to convert key ranges", K(ret));
  } else if (OB_FAIL(static_cast<storage::ObTableScanIterIterator*>(scan_ctx->result_iters_)
                         ->rescan(scan_ctx->scan_param_))) {
    LOG_WARN("failed to rescan", K(ret), "scan_param", scan_ctx->scan_param_);
  } else if (OB_ISNULL(scan_ctx->result_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexected null", K(scan_ctx->result_iters_), K(ret));
  } else if (OB_FAIL(scan_ctx->result_iters_->get_next_iter(scan_ctx->result_))) {
    LOG_WARN("failed to get next iter", K(ret));
  } else if (OB_ISNULL(scan_ctx->result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(scan_ctx->result_), K(ret));
  } else {
    scan_ctx->iter_end_ = false;
  }
  return ret;
}

int ObTableScan::get_next_row_with_mode(ObTableScanCtx* scan_ctx, ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (is_vt_mapping_) {
    // switch to mysql mode
    CompatModeGuard g(ObWorker::CompatMode::MYSQL);
    ret = scan_ctx->result_->get_next_row(row);
  } else {
    ret = scan_ctx->result_->get_next_row(row);
  }
  return ret;
}

int ObTableScan::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  common::ObNewRow* cur_row = NULL;
  ObTableScanCtx* scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id());
  ObTaskExecutorCtx* task_exec_ctx = ctx.get_task_executor_ctx();
  if (OB_ISNULL(scan_ctx) || OB_ISNULL(task_exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan ctx or task exec ctx is NULL", K(ret), K(scan_ctx), K(task_exec_ctx));
  } else if (OB_UNLIKELY(scan_ctx->get_partition_list_empty() || 0 == scan_ctx->scan_param_.limit_param_.limit_)) {
    // zero part or limit 0, just return iter end
    ret = OB_ITER_END;
  } else if (scan_ctx->iter_end_) {
    ret = OB_ITER_END;
    LOG_DEBUG("inner get next row meet a iter end", K(get_id()), K(this), K(lbt()));
  } else if (OB_ISNULL(scan_ctx->result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table scan result is not init", K(ret));
  } else if (0 == (++scan_ctx->iterated_rows_ % CHECK_STATUS_ROWS_INTERVAL) && OB_FAIL(ctx.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(get_next_row_with_mode(scan_ctx, cur_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row from ObNewRowIterator", K(ret));
    } else {
      // set found_rows
      if (is_top_table_scan_ && (scan_ctx->scan_param_.limit_param_.offset_ > 0)) {
        if (scan_ctx->output_row_count_ < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid output_row_count_", K(scan_ctx->output_row_count_), K(ret));
        } else if (scan_ctx->output_row_count_ > 0) {
          int64_t total_count = scan_ctx->output_row_count_ + scan_ctx->scan_param_.limit_param_.offset_;
          ObPhysicalPlanCtx* plan_ctx = NULL;
          if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("get physical plan context failed");
          } else {
            NG_TRACE_EXT(
                found_rows, OB_ID(total_count), total_count, OB_ID(offset), scan_ctx->scan_param_.limit_param_.offset_);
            plan_ctx->set_found_rows(total_count);
          }
        } else { /*undefined, do nothing*/
        }
      }
      if (for_update_ && share::is_oracle_mode()) {
        // we get affected rows here now, but we need get it in for_update operator in future.
        ObPhysicalPlanCtx* plan_ctx = NULL;
        if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("get physical plan context failed");
        } else {
          plan_ctx->set_affected_rows(scan_ctx->output_row_count_);
        }
      }
    }
  } else {
    NG_TRACE_TIMES(1, get_row);
    scan_ctx->output_row_count_++;
    if (is_vt_mapping_) {
      if (OB_FAIL(scan_ctx->vt_result_converter_->convert_output_row(cur_row))) {
        LOG_WARN("failed to convert output row", K(ret));
      }
    }
    scan_ctx->cur_row_.cells_ = cur_row->cells_;
    row = &scan_ctx->get_cur_row();
  }
  if (OB_UNLIKELY(OB_ITER_END == ret)) {
    if (OB_ISNULL(scan_ctx)) {
      LOG_ERROR("ret is OB_ITER_END, but scan_ctx is NULL", K(ret));
    } else {
      ObIPartitionGroup* partition = NULL;
      ObIPartitionGroupGuard* guard = scan_ctx->scan_param_.partition_guard_;
      if (OB_ISNULL(guard)) {
      } else if (OB_ISNULL(partition = guard->get_partition_group())) {
      } else if (scan_ctx->scan_param_.main_table_scan_stat_.bf_access_cnt_ > 0) {
        partition->feedback_scan_access_stat(scan_ctx->scan_param_);
      }
      ObTableScanStat& table_scan_stat = ctx.get_physical_plan_ctx()->get_table_scan_stat();
      fill_table_scan_stat(scan_ctx->scan_param_.main_table_scan_stat_, table_scan_stat);
      if (should_scan_index() && scan_ctx->scan_param_.scan_flag_.index_back_) {
        fill_table_scan_stat(scan_ctx->scan_param_.idx_table_scan_stat_, table_scan_stat);
      }
      scan_ctx->scan_param_.main_table_scan_stat_.reset_cache_stat();
      scan_ctx->scan_param_.idx_table_scan_stat_.reset_cache_stat();
      scan_ctx->iter_end_ = true;
    }
  }
  return ret;
}

int ObTableScan::calc_expr_int_value(
    ObExprCtx& expr_ctx, const ObSqlExpression& expr, int64_t& retval, bool& is_null_value) const
{
  int ret = OB_SUCCESS;
  ObObj result;
  common::ObNewRow dummy_row;
  is_null_value = false;
  if (OB_FAIL(expr.calc(expr_ctx, dummy_row, result))) {
    LOG_WARN("fail calc val", K(ret));
  } else if (OB_FAIL(expr.calc(expr_ctx, dummy_row, result))) {
    LOG_WARN("Failed to calculate expression", K(ret));
  } else if (result.is_int()) {
    if (OB_FAIL(result.get_int(retval))) {
      LOG_WARN("get_int error", K(ret), K(result));
    }
  } else if (result.is_null()) {
    is_null_value = true;
    retval = 0;
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_INT64_V2(result, retval);
    if (OB_FAIL(ret)) {
      LOG_WARN("get_int error", K(ret), K(result));
    }
  }
  return ret;
}

inline int ObTableScan::extract_scan_ranges(ObTableScanInput& scan_input, ObTableScanCtx& scan_ctx) const
{
  int ret = OB_SUCCESS;
  scan_ctx.scan_param_.key_ranges_.reuse();
  if (!USE_MULTI_GET_ARRAY_BINDING && !scan_input.range_array_pos_.empty()) {
    int64_t iter_idx = scan_ctx.expr_ctx_.cur_array_index_;
    int64_t start_pos = scan_input.range_array_pos_.at(iter_idx);
    int64_t end_pos = scan_input.range_array_pos_.count() > iter_idx + 1 ? scan_input.range_array_pos_.at(iter_idx + 1)
                                                                         : scan_input.key_ranges_.count();
    for (; OB_SUCC(ret) && start_pos < end_pos; ++start_pos) {
      if (OB_UNLIKELY(start_pos < 0) || OB_UNLIKELY(start_pos >= scan_input.key_ranges_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start_pos is invalid", K(ret), K(start_pos), K(scan_input.key_ranges_.count()));
      } else if (OB_FAIL(scan_ctx.scan_param_.key_ranges_.push_back(scan_input.key_ranges_.at(start_pos)))) {
        LOG_WARN("store key range failed", K(ret), K(start_pos), K(end_pos), K_(scan_input.key_ranges));
      }
    }
  } else if (OB_FAIL(append(scan_ctx.scan_param_.key_ranges_, scan_input.key_ranges_))) {
    LOG_WARN("failed to append key query range", K(ret));
  }
  LOG_DEBUG("extract scan ranges", K(ret), K(scan_ctx.scan_param_.key_ranges_), K(scan_input.key_ranges_));
  return ret;
}

inline int ObTableScan::do_table_scan(ObExecContext& ctx, bool is_rescan, bool need_prepare /*=true*/) const
{
  int ret = OB_SUCCESS;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (!need_prepare) {
  } else if (OB_FAIL(prepare(ctx, is_rescan))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  }
  /* step 1 */
  if (OB_SUCC(ret)) {
    scan_ctx->output_row_count_ = 0;
    scan_ctx->scan_param_.key_ranges_.reuse();
    scan_ctx->scan_param_.range_array_pos_.reuse();
    scan_ctx->scan_param_.table_param_ = &table_param_;
    scan_ctx->scan_param_.is_get_ = is_get_;
    if (OB_FAIL(extract_scan_ranges(*scan_input, *scan_ctx))) {
      LOG_WARN("extract scan ranges failed", K(ret));
    } else if (scan_input->key_ranges_.count() == 0 || ObSQLMockSchemaUtils::is_mock_index(index_id_)) {
      /*do nothing*/
    } else if (OB_FAIL(prune_query_range_by_partition_id(ctx, scan_ctx->scan_param_.key_ranges_))) {
      LOG_WARN("failed to prune query range by partition id", K(ret));
    } else if (scan_ctx->scan_param_.key_ranges_.count() == 0) {
      if (OB_FAIL(scan_ctx->scan_param_.key_ranges_.push_back(scan_input->key_ranges_.at(0)))) {
        LOG_WARN("failed to push back query range", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    ObIDataAccessService* das = NULL;
    ObPhysicalPlanCtx* plan_ctx = NULL;

    if (need_scn_) {
      scan_ctx->scan_param_.need_scn_ = true;
    }
    LOG_TRACE("do table scan info",
        K(scan_ctx->scan_param_.need_scn_),
        K(scan_ctx->scan_param_.range_array_pos_),
        K(scan_ctx->scan_param_.key_ranges_),
        K(get_batch_scan_flag()),
        K(ctx.is_gi_restart()));

    if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get plan ctx", K(ret));
    } else if (OB_FAIL(get_partition_service(task_exec_ctx, das))) {
      LOG_WARN("fail to get partition service", K(ret));
    } else if (plan_ctx->get_bind_array_count() > 0 && get_batch_scan_flag()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array binding not support batch nestloop join", K(ret));
    } else if (is_vt_mapping_ &&
               OB_FAIL(scan_ctx->vt_result_converter_->convert_key_ranges(scan_ctx->scan_param_.key_ranges_))) {
      LOG_WARN("failed to convert key ranges", K(ret));
    } else if (USE_MULTI_GET_ARRAY_BINDING && plan_ctx->get_bind_array_count() > 0) {
      if (OB_FAIL(das->table_scan(scan_ctx->scan_param_, scan_ctx->iter_result_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to scan table", K(scan_ctx->scan_param_), K(ref_table_id_), K(ret));
        }
      } else if (OB_FAIL(scan_ctx->iter_result_->get_next_iter(scan_ctx->result_))) {
        LOG_WARN("get next iterator failed", K(ret));
      }
    } else if (get_batch_scan_flag()) {
      if (OB_FAIL(das->table_scan(scan_ctx->scan_param_, scan_ctx->result_iters_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to scan table", K(scan_ctx->scan_param_), K(ref_table_id_), K(ret));
        }
      } else if (OB_ISNULL(scan_ctx->result_iters_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(scan_ctx->result_iters_->get_next_iter(scan_ctx->result_))) {
        LOG_WARN("failed to get next iter", K(ret));
      } else if (OB_ISNULL(scan_ctx->result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        LOG_DEBUG("table_scan(batch) begin", K(scan_ctx->scan_param_), K(*scan_ctx->result_), "op_id", get_id());
      }
    } else {
      if (OB_FAIL(das->table_scan(scan_ctx->scan_param_, scan_ctx->result_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to scan table", K(scan_ctx->scan_param_), K(ref_table_id_), K(ret));
        }
      } else {
        LOG_DEBUG("table_scan begin", K(scan_ctx->scan_param_), K(*scan_ctx->result_), "op_id", get_id());
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = sim_err(my_session);
  }
  return ret;
}

int ObTableScan::sim_err(ObSQLSessionInfo* my_session) const
{
  int ret = OB_SUCCESS;
#ifndef ERRSIM
  UNUSED(my_session);
#endif
#ifdef ERRSIM
  // err injection, return `is_data_not_readable_err` to test the case local read returns
  // unreadable error. make sure blacklist policy works OK
  if (my_session->is_master_session() && !is_inner_table(get_index_table_id())) {
    // don't inject to inner table scan
    ret = E(EventTable::EN_INVALID_ADDR_WEAK_READ_FAILED) OB_SUCCESS;
    if (is_data_not_readable_err(ret)) {
      LOG_INFO("errsin produce invalid addr error");
    }
  }
#endif
  return ret;
}

int ObTableScan::prune_query_range_by_partition_id(ObExecContext& ctx, ObIArray<ObNewRange>& scan_ranges) const
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObTableScanCtx* scan_ctx = NULL;
  ObSchemaGetterGuard schema_guard;
  ObSQLSessionInfo* my_session = NULL;

  int64_t schema_version = -1;
  const observer::ObGlobalContext& gctx = observer::ObServer::get_instance().get_gctx();
  if (ObPartitionLevel::PARTITION_LEVEL_MAX == part_level_ || ObPartitionLevel::PARTITION_LEVEL_ZERO == part_level_ ||
      scan_ranges.count() <= 1) {
    /*do nothing*/
  } else if (part_range_pos_.count() == 0 ||
             (ObPartitionLevel::PARTITION_LEVEL_TWO == part_level_ && subpart_range_pos_.count() == 0)) {
    /*do nothing*/
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get scan ctx", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(
                 gctx.schema_service_->get_tenant_schema_guard(my_session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema_version(ref_table_id_, schema_version))) {
    LOG_WARN("get table schema version failed", K(ret), K_(ref_table_id));
  } else if (OB_FAIL(check_cache_table_version(schema_version))) {
    LOG_WARN("check cache table version failed", K(ret), K(schema_version));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("failed to wrap expr ctx", K(ret));
  } else {
    int pos = 0;
    bool can_prune = false;
    LOG_DEBUG("range count before pruning", K(scan_ranges.count()));
    while (OB_SUCC(ret) && pos < scan_ranges.count()) {
      if (OB_FAIL(can_prune_by_partition_id(
              schema_guard, expr_ctx, scan_ctx->partition_id_, scan_ranges.at(pos), can_prune))) {
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

int ObTableScan::can_prune_by_partition_id(ObSchemaGetterGuard& schema_guard, ObExprCtx& expr_ctx,
    const int64_t partition_id, ObNewRange& scan_range, bool& can_prune) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObNewRange partition_range;
  ObNewRange subpartition_range;
  can_prune = true;
  if (is_vt_mapping_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("virtual table is not partition table", K(ret));
  } else if (OB_FAIL(construct_partition_range(allocator,
                 expr_ctx,
                 part_type_,
                 part_range_pos_,
                 scan_range,
                 part_expr_,
                 can_prune,
                 partition_range))) {
    LOG_WARN("failed to construct partition range", K(ret));
  } else if (can_prune && OB_FAIL(construct_partition_range(allocator,
                              expr_ctx,
                              subpart_type_,
                              subpart_range_pos_,
                              scan_range,
                              subpart_expr_,
                              can_prune,
                              subpartition_range))) {
    LOG_WARN("failed to construct subpartition range", K(ret));
  } else if (can_prune) {
    ObSEArray<int64_t, 16> partition_ids;
    ObSEArray<int64_t, 16> subpartition_ids;
    if (OB_FAIL(schema_guard.get_part(get_location_table_id(),
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
    } else if (ObPartitionLevel::PARTITION_LEVEL_ONE == part_level_) {
      if (partition_ids.at(0) == partition_id) {
        can_prune = false;
      } else { /*do nothing*/
      }
    } else if (OB_FAIL(schema_guard.get_part(get_location_table_id(),
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
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTableScan::construct_partition_range(ObArenaAllocator& allocator, ObExprCtx& expr_ctx,
    const ObPartitionFuncType part_type, const ObIArray<int64_t>& part_range_pos, const ObNewRange& scan_range,
    const ObSqlExpression& part_expr, bool& can_prune, ObNewRange& part_range) const
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
                expr_ctx,
                part_range))) {
          LOG_WARN("get partition real range failed", K(ret));
        }
        LOG_DEBUG("part range info", K(part_range), K(can_prune), K(ret));
      }
    }
  }
  return ret;
}

inline int ObTableScan::check_cache_table_version(int64_t schema_version) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(schema_version != schema_version_)) {
    // schema version in the operator and the schema version in the schema service may not be equal
    // because the schema version in the operator may not be set correctly in the old server
    // to be compatible with the old server, if schema version is not equal,
    // go to check the version in physical plan
    int64_t version_in_plan = -1;
    CK(OB_NOT_NULL(my_phy_plan_));
    OZ(my_phy_plan_->get_base_table_version(ref_table_id_, version_in_plan), ref_table_id_);
    if (OB_SUCC(ret) && schema_version != version_in_plan) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN(
          "unexpected schema error, need retry", K(ret), K(schema_version), K(schema_version_), K(version_in_plan));
    }
  }
  return ret;
}

int ObTableScan::revise_hash_part_object(ObObj& obj) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj.is_null())) {
    obj.set_int(0);
  } else if (OB_UNLIKELY(ObIntTC != obj.get_type_class() && ObUIntTC != obj.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is wrong", K(ret));
  } else {
    int64_t num = (ObIntTC == obj.get_type_class()) ? obj.get_int() : static_cast<int64_t>(obj.get_uint64());
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(INT64_MIN == num)) {
        num = INT64_MAX;
      } else {
        num = num < 0 ? -num : num;
      }
      obj.set_int(num);
    } else {
      LOG_WARN("Failed to get value", K(ret));
    }
  }
  return ret;
}

inline int ObTableScan::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;

  ObTableScanInput* scan_input = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableScanInput, ctx, get_id(), get_type(), scan_input))) {
      LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
    } else {
      scan_input->set_location_idx(0);
    }
  }

  if (OB_SUCC(ret)) {
    ObSQLSessionInfo* my_session = NULL;
    if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id(), get_type(), op_ctx))) {
      LOG_WARN("create physical operator context failed", K(ret), K(get_type()));
    } else if (OB_ISNULL(op_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator context is null");
    } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
      LOG_WARN("init current row failed", K(ret));
    } else if (OB_FAIL(static_cast<ObTableScanCtx*>(op_ctx)->init_table_allocator(ctx))) {
      LOG_WARN("fail to init table allocator", K(ret));
    } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get my session", K(ret));
    }
  }
  return ret;
}
inline bool ObTableScan::need_filter_row() const
{
  return is_vt_mapping_ || is_virtual_table(get_ref_table_id());
}

int ObTableScan::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableScanInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
  } else {
    UNUSED(input);
  }
  return ret;
}

int ObTableScan::set_limit_offset(const ObSqlExpression* limit, const ObSqlExpression* offset)
{
  int ret = OB_SUCCESS;
  if (NULL != limit) {
    if (OB_FAIL(limit_.assign(*limit))) {
      LOG_WARN("Assignment of limit fails", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL != offset) {
    if (OB_FAIL(offset_.assign(*offset))) {
      LOG_WARN("Assignment of offset fails", K(ret));
    }
  }
  return ret;
}

int ObTableScan::set_part_expr(const ObSqlExpression* part_expr)
{
  int ret = OB_SUCCESS;
  if (NULL != part_expr) {
    if (OB_FAIL(part_expr_.assign(*part_expr))) {
      LOG_WARN("failed to assign part expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTableScan::set_subpart_expr(const ObSqlExpression* subpart_expr)
{
  int ret = OB_SUCCESS;
  if (NULL != subpart_expr) {
    if (OB_FAIL(subpart_expr_.assign(*subpart_expr))) {
      LOG_WARN("failed to assign subpart expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObTableScan, ObNoChildrenPhyOperator), ref_table_id_, index_id_, output_column_ids_,
    filter_before_index_back_, flags_, limit_, offset_, for_update_, for_update_wait_us_, table_location_key_,
    pre_query_range_, hint_, exist_hint_, is_top_table_scan_, table_param_, schema_version_, part_level_, part_type_,
    subpart_type_, part_expr_, subpart_expr_, part_range_pos_, subpart_range_pos_, part_filter_, is_index_global_,
    gi_above_, is_get_, need_scn_, is_vt_mapping_, use_real_tenant_id_, has_tenant_id_col_, vt_table_id_,
    real_schema_version_, output_row_types_, key_types_, key_with_tenant_ids_, has_extra_tenant_ids_,
    org_output_column_ids_);

int64_t ObTableScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_ID,
      id_,
      N_COLUMN_COUNT,
      column_count_,
      N_PROJECTOR,
      ObArrayWrap<int32_t>(projector_, projector_size_),
      N_FILTER_EXPRS,
      filter_exprs_,
      N_CALC_EXPRS,
      calc_exprs_,
      N_REF_TID,
      ref_table_id_,
      N_INDEX_TID,
      index_id_,
      N_FLAG,
      flags_,
      N_LIMIT,
      limit_,
      N_OFFSET,
      offset_,
      N_FOR_UPDATE,
      for_update_,
      N_WAIT,
      for_update_wait_us_,
      N_QUERY_RANGE,
      pre_query_range_,
      "is_top_table_scan",
      is_top_table_scan_,
      N_VIRTUAL_COLUMN_EXPRS,
      virtual_column_exprs_,
      K_(table_location_key),
      K_(output_column_ids),
      K_(table_param),
      K_(is_index_global));
  if (exist_hint_) {
    J_COMMA();
    J_KV("hint", hint_);
  }
  J_OBJ_END();
  return pos;
}

int ObTableScan::set_est_row_count_record(const common::ObIArray<ObEstRowCountRecord>& est_records)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_array_size<>(est_records_, est_records.count()))) {
    LOG_WARN("failed to init est_row_count_records array", K(ret));
  } else if (OB_FAIL(append(est_records_, est_records))) {
    LOG_WARN("failed to append estimation row count records", K(ret));
  }
  return ret;
}

int ObTableScan::set_available_index_name(const common::ObIArray<common::ObString>& idx_name, ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_array_size<>(available_index_name_, idx_name.count()))) {
    LOG_WARN("init available_index_name failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < idx_name.count(); ++i) {
    ObString name;
    if (OB_FAIL(ob_write_string(phy_alloc, idx_name.at(i), name))) {
      LOG_WARN("copy available index name failed", K(ret));
    } else if (OB_FAIL(available_index_name_.push_back(name))) {
      LOG_WARN("push available index name failed", K(ret));
    }
  }
  return ret;
}

int ObTableScan::set_pruned_index_name(const common::ObIArray<common::ObString>& idx_name, ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_array_size<>(pruned_index_name_, idx_name.count()))) {
    LOG_WARN("init pruned_index_name failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < idx_name.count(); ++i) {
    ObString name;
    if (OB_FAIL(ob_write_string(phy_alloc, idx_name.at(i), name))) {
      LOG_WARN("copy pruned index name failed", K(ret));
    } else if (OB_FAIL(pruned_index_name_.push_back(name))) {
      LOG_WARN("push available index name failed", K(ret));
    }
  }
  return ret;
}

int ObTableScan::explain_index_selection_info(char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const char* method[5] = {"invalid_method", "remote_storage", "local_storage", "basic_stat", "histogram"};
  if (OB_UNLIKELY(estimate_method_ < 0 || estimate_method_ >= 5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(estimate_method_), K(ret));
  } else if (OB_FAIL(BUF_PRINTF("table_rows:%ld, physical_range_rows:%ld, logical_range_rows:%ld, index_back_rows:%ld, "
                                "output_rows:%ld, est_method:%s",
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

int ObTableScan::reassign_task_and_do_table_scan(ObExecContext& ctx, ObGranuleTaskInfo& info) const
{
  int ret = OB_SUCCESS;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  int64_t location_idx = -1;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get session", K(ret));
  } else if (OB_FAIL(scan_input->translate_pid_to_ldx(
                 ctx, info.partition_id_, table_location_key_, get_location_table_id(), location_idx))) {
  } else {
    scan_input->set_location_idx(location_idx);
    if (info.ranges_.count() > 0 && !info.ranges_.at(0).is_whole_range()) {
      if (ObSQLMockSchemaUtils::is_mock_index(index_id_)) {
        // transform rowid range here
        ObNewRange tmp_range;
        info.ranges_.reuse();
        ObSEArray<ObColDesc, 4> rowkey_descs;
        const ObIArray<ObColDesc>& col_descs = table_param_.get_col_descs();
        for (int64_t i = 0; OB_SUCC(ret) && i < table_param_.get_main_rowkey_cnt(); ++i) {
          if (OB_FAIL(rowkey_descs.push_back(col_descs.at(i)))) {
            LOG_WARN("failed to push col desc", K(ret));
          }
        }
        for (int i = 0; OB_SUCC(ret) && i < info.ranges_.count(); i++) {
          if (OB_FAIL(transform_rowid_range(ctx.get_allocator(), rowkey_descs, tmp_range))) {
            LOG_WARN("failed to transform rowid range to rowkey range", K(ret));
          } else if (OB_FAIL(info.ranges_.push_back(tmp_range))) {
            LOG_WARN("failed to push back element", K(ret));
          } else {
            tmp_range.reset();
          }
        }
      }
      if (OB_FAIL(scan_input->reassign_ranges(info.ranges_))) {
        LOG_WARN("the scan input reassign ragnes failed");
      }
    } else {
      // use prepare() to set key ranges if info.range is whole range scan (partition granule).
      scan_input->key_ranges_.reuse();
      LOG_DEBUG("do prepare!!!");
    }
    /*update scan partition key*/
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(update_scan_param_pkey(ctx))) {
      LOG_WARN("fail to update scan param pkey", K(ret));
    }
    LOG_TRACE(
        "TSC has been reassign a new task", K(info.partition_id_), K(info.ranges_), K(info.task_id_), K(location_idx));
    /*try to do table scan*/
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
      LOG_WARN("rescan operator failed", K(ret));
    } else if (OB_FAIL(inner_close(ctx))) {
      LOG_WARN("fail to close op", K(ret));
    } else if (OB_FAIL(do_table_scan(ctx,
                   true, /* rescan */
                   /* need prepare key ranges */
                   scan_input->key_ranges_.empty()))) {
      if (is_data_not_readable_err(ret)) {
        ObQueryRetryInfo& retry_info = my_session->get_retry_info_for_update();
        int add_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (add_ret = retry_info.add_invalid_server_distinctly(ctx.get_addr(), true)))) {
          LOG_WARN(
              "fail to add addr to invalid servers distinctly", K(ret), K(add_ret), K(ctx.get_addr()), K(retry_info));
        }
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to do table scan", K(ret));
      }
    } else {
    }
  }
  return ret;
}

int ObTableScan::update_scan_param_pkey(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTableScanInput* scan_input = NULL;
  ObTableScanCtx* scan_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  const ObPhyTableLocation* table_location = NULL;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get plan ctx", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 *executor_ctx, table_location_key_, get_location_table_id(), table_location))) {
    LOG_WARN("failed to get physical table location", K(table_location_key_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy table location", K(ret));
  } else if (table_location->get_partition_location_list().count() < 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("empty partition scan is not supported at the moment!",
        "# partitions",
        table_location->get_partition_location_list().count());
  } else if (table_location->get_partition_location_list().count() <= scan_input->get_location_idx()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("partition index is out-of-range",
        "# partitions",
        table_location->get_partition_location_list().count(),
        "location_idx",
        scan_input->get_location_idx());
  } else if (OB_INVALID_INDEX == scan_input->get_location_idx()) {
    scan_ctx->iter_end_ = true;
    ret = OB_ITER_END;
    LOG_DEBUG("update scan's pkey meet a invalid index, set table scan to end", K(get_id()), K(this), K(lbt()));
  } else {
    scan_ctx->partition_id_ =
        table_location->get_partition_location_list().at(scan_input->get_location_idx()).get_partition_id();
    // set partition id for PDML
    scan_ctx->expr_ctx_.pdml_partition_id_ = scan_ctx->partition_id_;  // just for pdml

    const share::ObPartitionReplicaLocation& part_loc =
        table_location->get_partition_location_list().at(scan_input->get_location_idx());

    if (OB_FAIL(part_loc.get_partition_key(scan_ctx->scan_param_.pkey_))) {
      LOG_WARN("get partition key fail", K(ret), K(part_loc));
    }
  }
  return ret;
}

int ObTableScan::get_gi_task_and_restart(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObGranuleTaskInfo gi_task_info;
  GIPrepareTaskMap* gi_prepare_map = nullptr;
  ObTableScanCtx* scan_ctx = nullptr;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get op ctx", K(ret));
  } else if (OB_FAIL(ctx.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (OB_FAIL(gi_prepare_map->get_refactored(get_id(), gi_task_info))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get prepare gi task", K(ret), K(get_id()));
    } else {
      // OB_HASH_NOT_EXIST mean no more task for tsc.
      LOG_DEBUG("no prepared task info, set table scan to end", K(get_id()), K(this), K(lbt()));
      scan_ctx->iter_end_ = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(reassign_task_and_do_table_scan(ctx, gi_task_info))) {
    LOG_WARN("failed to do gi task scan", K(ret));
  } else {
    LOG_DEBUG("TSC consume a task", K(ret), K(get_id()), K(gi_task_info));
  }
  return ret;
}

int ObTableScan::set_part_filter(const ObTableLocation& part_filter)
{
  int ret = OB_SUCCESS;
  if (part_filter.is_inited()) {
    part_filter_ = part_filter;
    if (!part_filter_.is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part filter is not init", K(ret), K(part_filter_), K(part_filter));
    }
  }
  return ret;
}

OB_INLINE void ObTableScan::fill_table_scan_stat(
    const ObTableScanStatistic& statistic, ObTableScanStat& scan_stat) const
{
  scan_stat.bf_filter_cnt_ += statistic.bf_filter_cnt_;
  scan_stat.bf_access_cnt_ += statistic.bf_access_cnt_;
  scan_stat.fuse_row_cache_hit_cnt_ += statistic.fuse_row_cache_hit_cnt_;
  scan_stat.fuse_row_cache_miss_cnt_ += statistic.fuse_row_cache_miss_cnt_;
  scan_stat.row_cache_hit_cnt_ += statistic.row_cache_hit_cnt_;
  scan_stat.row_cache_miss_cnt_ += statistic.row_cache_miss_cnt_;
}

OB_INLINE void ObTableScan::set_cache_stat(const ObPlanStat& plan_stat, ObTableScanParam& param) const
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

int ObTableScan::transform_rowid_ranges(
    ObIAllocator& allocator, const ObTableParam& table_param, const uint64_t index_id, ObQueryRangeArray& key_ranges)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("transform rowid ranges", K(key_ranges));
  for (int i = 0; i < key_ranges.count(); i++) {
    if (key_ranges.at(i))
      if (OB_ISNULL(key_ranges.at(i)) || OB_ISNULL(key_ranges.at(i)->get_start_key().get_obj_ptr()) ||
          OB_UNLIKELY(key_ranges.at(i)->get_start_key().get_obj_cnt() <= 0) ||
          OB_ISNULL(key_ranges.at(i)->get_end_key().get_obj_ptr()) ||
          OB_UNLIKELY(key_ranges.at(i)->get_end_key().get_obj_cnt() <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid key ranges", K(ret));
      }
  }  // for end
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObQueryRangeArray tmp_ranges;
    ObSEArray<ObColDesc, 4> rowkey_descs;
    const ObIArray<ObColDesc>& col_descs = table_param.get_col_descs();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_param.get_main_rowkey_cnt(); ++i) {
      if (OB_FAIL(rowkey_descs.push_back(col_descs.at(i)))) {
        LOG_WARN("failed to push col desc", K(ret));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < key_ranges.count(); i++) {
      if (OB_FAIL(transform_rowid_range(allocator, rowkey_descs, *key_ranges.at(i)))) {
        LOG_WARN("failed to transform rowid range", K(ret));
      } else {
        key_ranges.at(i)->table_id_ = ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(index_id);
      }
    }  // for end
  }

  return ret;
}

int ObTableScan::transform_rowid_range(
    ObIAllocator& allocator, const ObIArray<ObColDesc>& rowkey_descs, ObNewRange& new_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transform_rowid_rowkey(allocator, rowkey_descs, new_range.start_key_)) ||
      OB_FAIL(transform_rowid_rowkey(allocator, rowkey_descs, new_range.end_key_))) {
    LOG_WARN("failed to transform rowid rowkey", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObTableScan::transform_rowid_rowkey(
    ObIAllocator& allocator, const ObIArray<ObColDesc>& rowkey_descs, ObRowkey& row_key)
{
  int ret = OB_SUCCESS;
  ObObj* obj_buf = NULL;
  ObArray<ObObj> pk_vals;
  const int64_t rowkey_cnt = rowkey_descs.count();
  if (OB_UNLIKELY(!row_key.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey", K(ret));
  } else if (row_key.is_min_row() || row_key.is_max_row() || row_key.get_obj_ptr()[0].is_null()) {
    // oracle mode, max value is null
    if (OB_ISNULL(obj_buf = (ObObj*)allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      for (int i = 0; i < rowkey_cnt; i++) {
        new (obj_buf + i) ObObj();
        if (row_key.is_min_row()) {
          obj_buf[i].set_min_value();
        } else if (row_key.is_max_row()) {
          obj_buf[i].set_max_value();
        } else {
          obj_buf[i].set_null();
        }
      }
      row_key.assign(obj_buf, rowkey_cnt);
    }
  } else if (OB_ISNULL(row_key.get_obj_ptr()) || OB_UNLIKELY(row_key.get_obj_cnt() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_key.get_obj_ptr()), K(row_key.get_obj_cnt()));
  } else if (OB_UNLIKELY(!ob_is_urowid(row_key.get_obj_ptr()[0].get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got invalid obj type", K(row_key.get_obj_ptr()[0].get_type()));
  } else if (OB_FAIL(row_key.get_obj_ptr()[0].get_urowid().get_pk_vals(pk_vals))) {
    LOG_WARN("failed to get pk values", K(ret));
  } else if (OB_UNLIKELY(rowkey_cnt > pk_vals.count())) {
    // pk_vals may store generated col which is partition key
    ret = OB_INVALID_ROWID;
    LOG_WARN("invalid rowid, table rowkey cnt and encoded row cnt mismatch", K(ret));
  } else if (OB_ISNULL(obj_buf = (ObObj*)allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
      if (!pk_vals.at(i).meta_.is_null() &&
          !ObSQLUtils::is_same_type_for_compare(pk_vals.at(i).meta_, rowkey_descs.at(i).col_type_)) {
        ret = OB_INVALID_ROWID;
        LOG_WARN("invalid rowid, table rowkey type and encoded type mismatch",
            K(ret),
            K(pk_vals.at(i).meta_),
            K(rowkey_descs.at(i).col_type_));
      } else {
        obj_buf[i] = pk_vals.at(i);
      }
    }
    if (OB_SUCC(ret)) {
      row_key.assign(obj_buf, rowkey_cnt);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
