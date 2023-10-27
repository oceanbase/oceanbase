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
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/das/ob_das_group_scan_op.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_utils.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/geo/ob_s2adapter.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/ob_ddl_common.h"
#include "share/ob_ddl_checksum.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "observer/virtual_table/ob_virtual_data_access_service.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "observer/omt/ob_tenant_srs.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_external_table_utils.h"
#include "lib/container/ob_array_wrap.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql
{
#define MY_CTDEF (MY_SPEC.tsc_ctdef_)

int FlashBackItem::set_flashback_query_info(ObEvalCtx &eval_ctx, ObDASScanRtDef &scan_rtdef) const
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  const ObExpr *expr = flashback_query_expr_;
  scan_rtdef.need_scn_ = need_scn_;
  if (TableItem::NOT_USING == flashback_query_type_) {
    // do nothing
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flash back query expr is NULL", K(ret));
  } else if (OB_FAIL(expr->eval(eval_ctx, datum))) {
    LOG_WARN("expr evaluate failed", K(ret));
  } else if (datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL value", K(ret));
  } else {
    scan_rtdef.fb_read_tx_uncommitted_ = fq_read_tx_uncommitted_;
    if (TableItem::USING_TIMESTAMP == flashback_query_type_) {
      if (ObTimestampTZType != expr->datum_meta_.type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type not match", K(ret));
      } else if (OB_FAIL(scan_rtdef.fb_snapshot_.convert_from_ts(datum->get_otimestamp_tz().time_us_))) {
        LOG_WARN("failed to convert from ts", K(ret));
      } else {
        LOG_TRACE("fb_snapshot_ result", K(scan_rtdef.fb_snapshot_), K(*datum));
      }
    } else if (TableItem::USING_SCN == flashback_query_type_) {
      if (ObUInt64Type != expr->datum_meta_.type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type not match", K(ret));
      } else if (OB_FAIL(scan_rtdef.fb_snapshot_.convert_for_sql(datum->get_int()))) {
        LOG_WARN("failed to convert for gts", K(ret));
      } else {
        LOG_TRACE("fb_snapshot_ result", K(scan_rtdef.fb_snapshot_), K(*datum));
      }
    }
  }

  //对于同时存在hint指定的frozen_version和flashback query指定了snapshot version的情况下, 选择保留
  //flashback query指定的snapshot version, 忽略hint指定的frozen_version
  if (OB_SUCC(ret)) {
    if (scan_rtdef.fb_snapshot_.is_valid()) {
      scan_rtdef.frozen_version_ = transaction::ObTransVersion::INVALID_TRANS_VERSION;
    } else {
      /*do nothing*/
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER(AgentVtAccessMeta,
                    vt_table_id_,
                    access_exprs_,
                    access_column_ids_,
                    access_row_types_,
                    key_types_);

OB_DEF_SERIALIZE(ObTableScanCtDef)
{
  int ret = OB_SUCCESS;
  bool has_lookup = (lookup_ctdef_ != nullptr);
  OB_UNIS_ENCODE(pre_query_range_);
  OB_UNIS_ENCODE(flashback_item_.need_scn_);
  OB_UNIS_ENCODE(flashback_item_.flashback_query_expr_);
  OB_UNIS_ENCODE(flashback_item_.flashback_query_type_);
  OB_UNIS_ENCODE(bnlj_param_idxs_);
  OB_UNIS_ENCODE(scan_flags_);
  OB_UNIS_ENCODE(scan_ctdef_);
  OB_UNIS_ENCODE(has_lookup);
  if (OB_SUCC(ret) && has_lookup) {
    OB_UNIS_ENCODE(*lookup_ctdef_);
    OB_UNIS_ENCODE(*lookup_loc_meta_);
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_ENCODE(has_dppr_tbl);
  if (OB_SUCC(ret) && has_dppr_tbl) {
    OB_UNIS_ENCODE(*das_dppr_tbl_);
  }
  OB_UNIS_ENCODE(calc_part_id_expr_);
  OB_UNIS_ENCODE(global_index_rowkey_exprs_);
  OB_UNIS_ENCODE(flashback_item_.fq_read_tx_uncommitted_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableScanCtDef)
{
  int64_t len = 0;
  bool has_lookup = (lookup_ctdef_ != nullptr);
  OB_UNIS_ADD_LEN(pre_query_range_);
  OB_UNIS_ADD_LEN(flashback_item_.need_scn_);
  OB_UNIS_ADD_LEN(flashback_item_.flashback_query_expr_);
  OB_UNIS_ADD_LEN(flashback_item_.flashback_query_type_);
  OB_UNIS_ADD_LEN(bnlj_param_idxs_);
  OB_UNIS_ADD_LEN(scan_flags_);
  OB_UNIS_ADD_LEN(scan_ctdef_);
  OB_UNIS_ADD_LEN(has_lookup);
  if (has_lookup) {
    OB_UNIS_ADD_LEN(*lookup_ctdef_);
    OB_UNIS_ADD_LEN(*lookup_loc_meta_);
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_ADD_LEN(has_dppr_tbl);
  if (has_dppr_tbl) {
    OB_UNIS_ADD_LEN(*das_dppr_tbl_);
  }
  OB_UNIS_ADD_LEN(calc_part_id_expr_);
  OB_UNIS_ADD_LEN(global_index_rowkey_exprs_);
  OB_UNIS_ADD_LEN(flashback_item_.fq_read_tx_uncommitted_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableScanCtDef)
{
  int ret = OB_SUCCESS;
  bool has_lookup = false;
  OB_UNIS_DECODE(pre_query_range_);
  OB_UNIS_DECODE(flashback_item_.need_scn_);
  OB_UNIS_DECODE(flashback_item_.flashback_query_expr_);
  OB_UNIS_DECODE(flashback_item_.flashback_query_type_);
  OB_UNIS_DECODE(bnlj_param_idxs_);
  OB_UNIS_DECODE(scan_flags_);
  OB_UNIS_DECODE(scan_ctdef_);
  OB_UNIS_DECODE(has_lookup);
  if (OB_SUCC(ret) && has_lookup) {
    void *ctdef_buf = allocator_.alloc(sizeof(ObDASScanCtDef));
    if (OB_ISNULL(ctdef_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate das scan ctdef buffer failed", K(ret), K(sizeof(ObDASScanCtDef)));
    } else {
      lookup_ctdef_ = new(ctdef_buf) ObDASScanCtDef(allocator_);
      OB_UNIS_DECODE(*lookup_ctdef_);
    }
    if (OB_SUCC(ret)) {
      void *loc_meta_buf = allocator_.alloc(sizeof(ObDASTableLocMeta));
      if (OB_ISNULL(loc_meta_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate table loc meta failed", K(ret));
      } else {
        lookup_loc_meta_ = new(loc_meta_buf) ObDASTableLocMeta(allocator_);
        OB_UNIS_DECODE(*lookup_loc_meta_);
      }
    }
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_DECODE(has_dppr_tbl);
  if (OB_SUCC(ret) && has_dppr_tbl) {
    OZ(allocate_dppr_table_loc());
    OB_UNIS_DECODE(*das_dppr_tbl_);
  }
  OB_UNIS_DECODE(calc_part_id_expr_);
  OB_UNIS_DECODE(global_index_rowkey_exprs_);
  OB_UNIS_DECODE(flashback_item_.fq_read_tx_uncommitted_);
  return ret;
}

int ObTableScanCtDef::allocate_dppr_table_loc()
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObTableLocation));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate table location buffer failed", K(ret));
  } else {
    das_dppr_tbl_ = new(buf) ObTableLocation(allocator_);
  }
  return ret;
}

OB_INLINE void ObTableScanRtDef::prepare_multi_part_limit_param()
{
  /* for multi-partition scanning, */
  /* the limit operation pushed down to the partition TSC needs to be adjusted */
  /* its rule: */
  /*                TSC(limit m, n) */
  /*                   /      \ */
  /*                  /        \ */
  /*          DAS Scan(p0)    DAS Scan(p1) */
  /*        (p0, limit m+n)  (p1, limit m+n) */
  /* each partition scans limit m+n rows of data, */
  /* and TSC operator selects the offset (m) limit (n) rows in the final result */
  int64_t offset = scan_rtdef_.limit_param_.offset_;
  int64_t limit = scan_rtdef_.limit_param_.limit_;
  scan_rtdef_.limit_param_.limit_ = offset + limit;
  scan_rtdef_.limit_param_.offset_ = 0;
  if (lookup_rtdef_ != nullptr) {
    offset = lookup_rtdef_->limit_param_.offset_;
    limit = lookup_rtdef_->limit_param_.limit_;
    lookup_rtdef_->limit_param_.limit_ = offset + limit;
    lookup_rtdef_->limit_param_.offset_ = 0;
  }
}

ObTableScanOpInput::ObTableScanOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
    tablet_loc_(nullptr),
    not_need_extract_query_range_(false)
{
}

ObTableScanOpInput::~ObTableScanOpInput()
{
}

void ObTableScanOpInput::reset()
{
  tablet_loc_ = nullptr;
  key_ranges_.reset();
  ss_key_ranges_.reset();
  mbr_filters_.reset();
  range_array_pos_.reset();
  not_need_extract_query_range_ = false;
}

OB_DEF_SERIALIZE_SIZE(ObTableScanOpInput)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              key_ranges_,
              not_need_extract_query_range_,
              ss_key_ranges_);
  return len;
}

OB_DEF_SERIALIZE(ObTableScanOpInput)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              key_ranges_,
              not_need_extract_query_range_,
              ss_key_ranges_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableScanOpInput)
{
  int ret  = OB_SUCCESS;
  int64_t cnt = 0;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &cnt))) {
    LOG_WARN("decode failed", K(ret));
  } else if (OB_FAIL(key_ranges_.prepare_allocate(cnt))) {
    LOG_WARN("array prepare allocate failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
      if (OB_FAIL(key_ranges_.at(i).deserialize(
                  exec_ctx_.get_allocator(), buf, data_len, pos))) {
        LOG_WARN("range deserialize failed", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &cnt))) {
        LOG_WARN("decode failed", K(ret));
      } else if (OB_FAIL(ss_key_ranges_.prepare_allocate(cnt))) {
        LOG_WARN("array prepare allocate failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
        if (OB_FAIL(ss_key_ranges_.at(i).deserialize(exec_ctx_.get_allocator(),
                                                     buf, data_len, pos))) {
          LOG_WARN("range deserialize failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_DECODE, not_need_extract_query_range_);
    }
  }
  return ret;
}

int ObTableScanOpInput::init(ObTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  if (PHY_FAKE_CTE_TABLE == MY_SPEC.type_) {
    LOG_DEBUG("CTE TABLE do not need init", K(ret));
  } else if (ObTaskSpliter::INVALID_SPLIT == task_info.get_task_split_type()) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec type is INVALID_SPLIT", K(ret));
  } else {
    if (1 == task_info.get_range_location().part_locs_.count()  // only one table
               && 0 < task_info.get_range_location().part_locs_.at(0).scan_ranges_.count()) {
      // multi-range
      ret = key_ranges_.assign(task_info.get_range_location().part_locs_.at(0).scan_ranges_);
    }
  }
  return ret;
}

OB_INLINE int ObTableScanOp::reuse_table_rescan_allocator()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_rescan_allocator_)) {
    ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
    lib::ContextParam param;
    param.set_mem_attr(my_session->get_effective_tenant_id(),
                       "TableRescanCtx", ObCtxIds::DEFAULT_CTX_ID)
       .set_properties(lib::USE_TL_PAGE_OPTIONAL)
       .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    lib::MemoryContext mem_context;
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context, param))) {
      LOG_WARN("fail to create entity", K(ret));
    } else if (OB_ISNULL(mem_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create entity ", K(ret));
    } else {
      table_rescan_allocator_ = &mem_context->get_arena_allocator();
    }
  } else {
    table_rescan_allocator_->reuse();
  }
  return ret;
}

ObTableScanSpec::ObTableScanSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
    table_loc_id_(OB_INVALID_ID),
    ref_table_id_(OB_INVALID_ID),
    limit_(NULL),
    offset_(NULL),
    frozen_version_(-1),
    part_level_(ObPartitionLevel::PARTITION_LEVEL_MAX),
    part_type_(ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
    subpart_type_(ObPartitionFuncType::PARTITION_FUNC_TYPE_MAX),
    part_expr_(NULL),
    subpart_expr_(NULL),
    part_range_pos_(alloc),
    subpart_range_pos_(alloc),
    part_dep_cols_(alloc),
    subpart_dep_cols_(alloc),
    table_row_count_(0),
    output_row_count_(0),
    phy_query_range_row_count_(0),
    query_range_row_count_(0),
    index_back_row_count_(0),
    estimate_method_(INVALID_METHOD),
    est_records_(alloc),
    available_index_name_(alloc),
    pruned_index_name_(alloc),
    unstable_index_name_(alloc),
    ddl_output_cids_(alloc),
    tsc_ctdef_(alloc),
    pdml_partition_id_(NULL),
    agent_vt_meta_(alloc),
    flags_(0),
    tenant_id_col_idx_(0),
    partition_id_calc_type_(0)
{
}

OB_SERIALIZE_MEMBER((ObTableScanSpec, ObOpSpec),
                    table_loc_id_,
                    ref_table_id_,
                    flags_,
                    limit_,
                    offset_,
                    frozen_version_,
                    part_level_,
                    part_type_,
                    subpart_type_,
                    part_expr_,
                    subpart_expr_,
                    part_range_pos_,
                    subpart_range_pos_,
                    part_dep_cols_,
                    subpart_dep_cols_,
                    tsc_ctdef_,
                    pdml_partition_id_,
                    agent_vt_meta_,
                    ddl_output_cids_,
                    tenant_id_col_idx_,
                    partition_id_calc_type_);

DEF_TO_STRING(ObTableScanSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K(table_loc_id_),
       K(ref_table_id_),
       K(is_index_global_),
       K(limit_),
       K(offset_),
       K(frozen_version_),
       K(force_refresh_lc_),
       K(is_top_table_scan_),
       K(gi_above_),
       K(batch_scan_flag_),
       K(use_dist_das_),
       K(tsc_ctdef_),
       K(report_col_checksum_),
       K_(agent_vt_meta),
       K_(ddl_output_cids),
       K_(tenant_id_col_idx));
  J_OBJ_END();
  return pos;
}

int ObTableScanSpec::set_est_row_count_record(const ObIArray<ObEstRowCountRecord> &est_records)
{
  int ret = OB_SUCCESS;
  OZ(est_records_.init(est_records.count()));
  OZ(append(est_records_, est_records));
  return ret;
}

int ObTableScanSpec::set_available_index_name(const ObIArray<ObString> &idx_name,
                                              ObIAllocator &phy_alloc)
{
  int ret = OB_SUCCESS;
  OZ(available_index_name_.init(idx_name.count()));
  FOREACH_CNT_X(n, idx_name, OB_SUCC(ret)) {
    ObString name;
    OZ(ob_write_string(phy_alloc, *n, name));
    OZ(available_index_name_.push_back(name));
  }
  return ret;
}

int ObTableScanSpec::set_unstable_index_name(const ObIArray<ObString> &idx_name,
                                             ObIAllocator &phy_alloc)
{
  int ret = OB_SUCCESS;
  OZ(unstable_index_name_.init(idx_name.count()));
  FOREACH_CNT_X(n, idx_name, OB_SUCC(ret)) {
    ObString name;
    OZ(ob_write_string(phy_alloc, *n, name));
    OZ(unstable_index_name_.push_back(name));
  }
  return ret;
}

int ObTableScanSpec::set_pruned_index_name(const ObIArray<ObString> &idx_name,
                                           ObIAllocator &phy_alloc)
{
  int ret = OB_SUCCESS;
  OZ(pruned_index_name_.init(idx_name.count()));
  FOREACH_CNT_X(n, idx_name, OB_SUCC(ret)) {
    ObString name;
    OZ(ob_write_string(phy_alloc, *n, name));
    OZ(pruned_index_name_.push_back(name));
  }
  return ret;
}

int ObTableScanSpec::explain_index_selection_info(
    char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(
                "table_rows:%ld, physical_range_rows:%ld, logical_range_rows:%ld, "
                "index_back_rows:%ld, output_rows:%ld",
                table_row_count_, phy_query_range_row_count_, query_range_row_count_,
                index_back_row_count_, output_row_count_))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  if (OB_SUCC(ret) && available_index_name_.count() > 0) {
    // print available index id
    if (OB_FAIL(BUF_PRINTF(", avaiable_index_name["))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < available_index_name_.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%.*s", available_index_name_.at(i).length(),
                             available_index_name_.at(i).ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (i != available_index_name_.count() - 1) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* do nothing*/ }
      } else { /* do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF("]"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else { /* Do nothing */ }
  }

  if (OB_SUCC(ret) && pruned_index_name_.count() > 0) {
    if (OB_FAIL(BUF_PRINTF(", pruned_index_name["))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pruned_index_name_.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%.*s", pruned_index_name_.at(i).length(),
                             pruned_index_name_.at(i).ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (i != pruned_index_name_.count() - 1) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* do nothing*/ }
      } else { /* do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF("]"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else { /* Do nothing */ }
  }

  if (OB_SUCC(ret) && unstable_index_name_.count() > 0) {
    if (OB_FAIL(BUF_PRINTF(", unstable_index_name["))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < unstable_index_name_.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%.*s", unstable_index_name_.at(i).length(),
                             unstable_index_name_.at(i).ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (i != unstable_index_name_.count() - 1) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* do nothing*/ }
      } else { /* do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF("]"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else { /* Do nothing */ }
  }

  if (OB_SUCC(ret) && est_records_.count() > 0) {
    // print est row count infos
    if (OB_FAIL(BUF_PRINTF(", estimation info[table_id:%ld,", est_records_.at(0).table_id_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < est_records_.count(); ++i) {
      const ObEstRowCountRecord &record = est_records_.at(i);
      if (OB_FAIL(BUF_PRINTF(
                  " (table_type:%ld, version:%ld-%ld-%ld, logical_rc:%ld, physical_rc:%ld)%c",
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

ObTableScanOp::ObTableScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    das_ref_(eval_ctx_, exec_ctx),
    tsc_rtdef_(exec_ctx.get_allocator()),
    need_final_limit_(false),
    table_rescan_allocator_(NULL),
    input_row_cnt_(0),
    output_row_cnt_(0),
    iter_end_(false),
    iterated_rows_(0),
    got_feedback_(false),
    vt_result_converter_(nullptr),
    cur_trace_id_(nullptr),
    col_need_reshape_(),
    column_checksum_(),
    scan_task_id_(0),
    report_checksum_(false),
    in_rescan_(false),
    global_index_lookup_op_(NULL),
    spat_index_()
{
}

ObTableScanOp::~ObTableScanOp()
{
}

bool ObTableScanOp::has_das_scan_op(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_op)
{
  if (MY_SPEC.batch_scan_flag_) {
    das_op = static_cast<ObDASScanOp*>(
            das_ref_.find_das_task(tablet_loc, DAS_OP_TABLE_BATCH_SCAN));
  } else {
    das_op = static_cast<ObDASScanOp*>(
            das_ref_.find_das_task(tablet_loc, DAS_OP_TABLE_SCAN));
  }
  return das_op != nullptr;
}

int ObTableScanOp::init_das_group_range(const int64_t cur_group_idx, const int64_t group_size)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.batch_scan_flag_) {
    for (DASTaskIter task_iter = das_ref_.begin_task_iter(); !task_iter.is_end(); ++task_iter) {
      ObDASGroupScanOp *batch_op = static_cast<ObDASGroupScanOp*>(*task_iter);
      batch_op->init_group_range(cur_group_idx, group_size);
      LOG_DEBUG("init das group range", K(batch_op), K(cur_group_idx), K(group_size));
    }
  }
  return ret;
}

OB_INLINE int ObTableScanOp::create_one_das_task(ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *task_op = nullptr;
  ObDASScanOp *scan_op = nullptr;
  uint64_t table_loc_id = MY_SPEC.get_table_loc_id();
  ObDASOpType op_type = MY_SPEC.batch_scan_flag_ ? DAS_OP_TABLE_BATCH_SCAN : DAS_OP_TABLE_SCAN;
  if (OB_LIKELY(has_das_scan_op(tablet_loc, scan_op))) {
    // reuse das scan op
  } else if (OB_FAIL(das_ref_.create_das_task(tablet_loc, op_type, task_op))) {
    LOG_WARN("prepare das task failed", K(ret));
  } else {
    scan_op = static_cast<ObDASScanOp*>(task_op);
    scan_op->set_scan_ctdef(&MY_CTDEF.scan_ctdef_);
    scan_op->set_scan_rtdef(&tsc_rtdef_.scan_rtdef_);
    scan_op->set_can_part_retry(nullptr == tsc_rtdef_.scan_rtdef_.sample_info_
                                && can_partition_retry());
    scan_op->set_inner_rescan(in_rescan_);
    tsc_rtdef_.scan_rtdef_.table_loc_->is_reading_ = true;
    if (!MY_SPEC.is_index_global_ && MY_CTDEF.lookup_ctdef_ != nullptr) {
      //is local index lookup, need to set the lookup ctdef to the das scan op
      ObDASTableLoc *lookup_table_loc = tsc_rtdef_.lookup_rtdef_->table_loc_;
      ObDASTabletLoc *lookup_tablet_loc = ObDASUtils::get_related_tablet_loc(
          *tablet_loc, lookup_table_loc->loc_meta_->ref_table_id_);
      if (OB_ISNULL(lookup_tablet_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lookup tablet loc is nullptr", K(ret), KPC(tablet_loc), KPC(lookup_table_loc->loc_meta_));
      } else if (OB_FAIL(scan_op->set_lookup_ctdef(MY_CTDEF.lookup_ctdef_))) {
        LOG_WARN("set lookup ctdef failed", K(ret));
      } else if (OB_FAIL(scan_op->set_lookup_rtdef(tsc_rtdef_.lookup_rtdef_))) {
        LOG_WARN("set lookup rtdef failed", K(ret));
      } else if (OB_FAIL(scan_op->set_lookup_tablet_id(lookup_tablet_loc->tablet_id_))) {
        LOG_WARN("set lookup tablet id failed", K(ret), KPC(lookup_tablet_loc));
      } else {
        lookup_table_loc->is_reading_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cherry_pick_range_by_tablet_id(scan_op))) {
      LOG_WARN("prune query range by partition id failed", K(ret), KPC(tablet_loc));
    }
  }
  return ret;
}

int ObTableScanOp::prepare_pushdown_limit_param()
{
  int ret = OB_SUCCESS;
  if (!limit_param_.is_valid()) {
    //ignore, do nothing
  } else if (MY_SPEC.batch_scan_flag_) {
    //batch scan can not pushdown limit param to storage
    need_final_limit_ = true;
    tsc_rtdef_.scan_rtdef_.limit_param_.offset_ = 0;
    tsc_rtdef_.scan_rtdef_.limit_param_.limit_ = -1;

    if (nullptr != MY_CTDEF.lookup_ctdef_) {
      OB_ASSERT(nullptr != tsc_rtdef_.lookup_rtdef_);
      tsc_rtdef_.lookup_rtdef_->limit_param_.offset_ = 0;
      tsc_rtdef_.lookup_rtdef_->limit_param_.limit_  = -1;
    }

  } else if (tsc_rtdef_.has_lookup_limit() || das_ref_.get_das_task_cnt() > 1) {
    //for index back, need to final limit output rows in TableScan operator,
    //please see me for the reason:
    /* for multi-partition scanning, */
    /* the limit operation pushed down to the partition TSC needs to be adjusted */
    /* its rule: */
    /*                TSC(limit m, n) */
    /*                   /      \ */
    /*                  /        \ */
    /*          DAS Scan(p0)    DAS Scan(p1) */
    /*        (p0, limit m+n)  (p1, limit m+n) */
    /* each partition scans limit m+n rows of data, */
    /* and TSC operator selects the offset (m) limit (n) rows in the final result */
    need_final_limit_ = true;
    tsc_rtdef_.prepare_multi_part_limit_param();
  }
  return ret;
}

int ObTableScanOp::prepare_das_task()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx &task_exec_ctx = ctx_.get_task_exec_ctx();
  if (OB_LIKELY(!MY_SPEC.use_dist_das_)) {
    if (OB_FAIL(create_one_das_task(MY_INPUT.tablet_loc_))) {
      LOG_WARN("create one das task failed", K(ret));
    }
  } else if (OB_LIKELY(nullptr == MY_CTDEF.das_dppr_tbl_)) {
    ObDASTableLoc *table_loc = tsc_rtdef_.scan_rtdef_.table_loc_;
    for (DASTabletLocListIter node = table_loc->tablet_locs_begin();
         OB_SUCC(ret) && node != table_loc->tablet_locs_end(); ++node) {
      ObDASTabletLoc *tablet_loc = *node;
      if (OB_FAIL(create_one_das_task(tablet_loc))) {
        LOG_WARN("create one das task failed", K(ret));
      }
    }
  } else {
    // dynamic partitions
    ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
    ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session());
    const ObTableLocation &das_location = *MY_CTDEF.das_dppr_tbl_;
    ObSEArray<ObTabletID, 1> tablet_ids;
    ObSEArray<ObObjectID, 1> partition_ids;
    ObSEArray<ObObjectID, 1> first_level_part_ids;
    if (OB_FAIL(das_location.calculate_tablet_ids(ctx_,
                                                  plan_ctx->get_param_store(),
                                                  tablet_ids,
                                                  partition_ids,
                                                  first_level_part_ids,
                                                  dtc_params))) {
      LOG_WARN("calculate dynamic partitions failed", K(ret));
    } else {
      LOG_TRACE("dynamic partitions", K(tablet_ids), K(partition_ids), K(first_level_part_ids));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      ObDASTabletLoc *tablet_loc = nullptr;
      if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(*tsc_rtdef_.scan_rtdef_.table_loc_,
                                                    tablet_ids.at(i),
                                                    tablet_loc))) {
        LOG_WARN("extended tablet loc failed", K(ret));
      } else if (OB_FAIL(create_one_das_task(tablet_loc))) {
        LOG_WARN("create one das task failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanOp::prepare_all_das_tasks()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.batch_scan_flag_) {
    if (OB_SUCC(ret)) {
      if (!tsc_rtdef_.bnlj_params_.empty()) {
        tsc_rtdef_.group_size_ = tsc_rtdef_.bnlj_params_.at(0).gr_param_->count_;
        if (OB_UNLIKELY(tsc_rtdef_.group_size_ > tsc_rtdef_.max_group_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The amount of data exceeds the pre allocated memory", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("batch nlj params is empty", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (MY_SPEC.gi_above_ && !MY_INPUT.key_ranges_.empty()) {
      if (OB_FAIL(prepare_das_task())) {
        LOG_WARN("prepare das task failed", K(ret));
      }
    } else {
      int64_t group_size = MY_SPEC.batch_scan_flag_ ? tsc_rtdef_.group_size_ : 1;
      GroupRescanParamGuard grp_guard(tsc_rtdef_, GET_PHY_PLAN_CTX(ctx_)->get_param_store_for_update());
      for (int64_t i = 0; OB_SUCC(ret) && i < group_size; ++i) {
        grp_guard.switch_group_rescan_param(i);
        if (OB_FAIL(prepare_single_scan_range(i))) {
          LOG_WARN("prepare single scan range failed", K(ret));
        } else if (OB_FAIL(prepare_das_task())) {
          LOG_WARN("prepare das task failed", K(ret));
        } else {
          MY_INPUT.key_ranges_.reuse();
          MY_INPUT.ss_key_ranges_.reuse();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_das_group_range(0, tsc_rtdef_.group_size_))) {
      LOG_WARN("set group range failed", K(ret), K_(tsc_rtdef_.group_size));
    }
  }
  return ret;
}

int ObTableScanOp::init_table_scan_rtdef()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  ObDASTaskFactory &das_factory = DAS_CTX(ctx_).get_das_factory();
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = my_session->get_effective_tenant_id();
  mem_attr.label_ = "ScanDASCtx";
  das_ref_.set_mem_attr(mem_attr);
  das_ref_.set_expr_frame_info(&MY_SPEC.plan_->get_expr_frame_info());
  das_ref_.set_execute_directly(!MY_SPEC.use_dist_das_);
  set_cache_stat(plan_ctx->get_phy_plan()->stat_);
  bool is_null_value = false;
  if (OB_SUCC(ret) && NULL != MY_SPEC.limit_) {
    if (OB_FAIL(calc_expr_int_value(*MY_SPEC.limit_, limit_param_.limit_, is_null_value))) {
      LOG_WARN("fail get val", K(ret));
    } else if (limit_param_.limit_ < 0) {
      limit_param_.limit_ = 0;
    }
  }

  if (OB_SUCC(ret) && NULL != MY_SPEC.offset_ && !is_null_value) {
    if (OB_FAIL(calc_expr_int_value(*MY_SPEC.offset_, limit_param_.offset_, is_null_value))) {
      LOG_WARN("fail get val", K(ret));
    } else if (limit_param_.offset_ < 0) {
      limit_param_.offset_ = 0;
    } else if (is_null_value) {
      limit_param_.limit_ = 0;
    }
  }
  if (OB_SUCC(ret)) {
    const ObDASScanCtDef &scan_ctdef = MY_CTDEF.scan_ctdef_;
    ObDASScanRtDef &scan_rtdef = tsc_rtdef_.scan_rtdef_;
    const ObDASTableLocMeta *loc_meta = MY_CTDEF.das_dppr_tbl_ != nullptr ?
                                        &MY_CTDEF.das_dppr_tbl_->get_loc_meta() : nullptr;
    if (OB_FAIL(init_das_scan_rtdef(scan_ctdef, scan_rtdef, loc_meta))) {
      LOG_WARN("init das scan rtdef failed", K(ret));
    } else if (!MY_SPEC.use_dist_das_ && !MY_SPEC.gi_above_ && !scan_rtdef.table_loc_->empty()) {
      MY_INPUT.tablet_loc_ = scan_rtdef.table_loc_->get_first_tablet_loc();
    }
  }
  if (OB_SUCC(ret) && MY_CTDEF.lookup_ctdef_ != nullptr) {
    const ObDASScanCtDef &lookup_ctdef = *MY_CTDEF.lookup_ctdef_;
    ObDASBaseRtDef *das_rtdef = nullptr;
    if (OB_FAIL(das_factory.create_das_rtdef(DAS_OP_TABLE_SCAN, das_rtdef))) {
      LOG_WARN("create das rtdef failed", K(ret));
    } else {
      tsc_rtdef_.lookup_rtdef_ = static_cast<ObDASScanRtDef*>(das_rtdef);
      if (OB_FAIL(init_das_scan_rtdef(lookup_ctdef, *tsc_rtdef_.lookup_rtdef_, MY_CTDEF.lookup_loc_meta_))) {
        LOG_WARN("init das scan rtdef failed", K(ret), K(lookup_ctdef));
      }
    }
  }
  return ret;
}

OB_INLINE int ObTableScanOp::init_das_scan_rtdef(const ObDASScanCtDef &das_ctdef,
                                                 ObDASScanRtDef &das_rtdef,
                                                 const ObDASTableLocMeta *loc_meta)
{
  int ret = OB_SUCCESS;
  const ObTableScanCtDef &tsc_ctdef = MY_CTDEF;
  bool is_lookup = (&das_ctdef == MY_CTDEF.lookup_ctdef_);
  bool is_lookup_limit = MY_SPEC.is_index_back() &&
      !MY_CTDEF.lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  ObTaskExecutorCtx &task_exec_ctx = ctx_.get_task_exec_ctx();
  das_rtdef.timeout_ts_ = plan_ctx->get_ps_timeout_timestamp();
  das_rtdef.tx_lock_timeout_ = my_session->get_trx_lock_timeout();
  das_rtdef.scan_flag_ = MY_CTDEF.scan_flags_;
  das_rtdef.scan_flag_.is_show_seed_ = plan_ctx->get_show_seed();
  if(is_foreign_check_nested_session()) {
    das_rtdef.is_for_foreign_check_ = true;
    if (plan_ctx->get_phy_plan()->has_for_update() && ObSQLUtils::is_iter_uncommitted_row(&ctx_)) {
      das_rtdef.scan_flag_.set_iter_uncommitted_row();
    }
  }
  if (MY_SPEC.batch_scan_flag_ || is_lookup) {
    das_rtdef.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
  }
  das_rtdef.scan_flag_.is_lookup_for_4377_ = is_lookup;
  das_rtdef.need_check_output_datum_ = MY_SPEC.need_check_output_datum_;
  das_rtdef.sql_mode_ = my_session->get_sql_mode();
  das_rtdef.stmt_allocator_.set_alloc(&das_ref_.get_das_alloc());
  das_rtdef.scan_allocator_.set_alloc(&das_ref_.get_das_alloc());
  das_rtdef.eval_ctx_ = &get_eval_ctx();
  if ((is_lookup_limit && is_lookup) || (!is_lookup_limit && !is_lookup)) {
    //when is_lookup_limit = true means that the limit param should pushdown to the lookup rtdef
    //so is_lookup = true means that the das_rtdef is the lookup rtdef
    //when is_lookup_limit = false means that the limit param should pushdown to the scan rtdef
    //so is_lookup = false means that the das_rtdef is the scan rtdef
    das_rtdef.limit_param_ = limit_param_;
  }
  das_rtdef.frozen_version_ = MY_SPEC.frozen_version_;
  das_rtdef.force_refresh_lc_ = MY_SPEC.force_refresh_lc_;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(das_rtdef.init_pd_op(ctx_, das_ctdef))) {
      LOG_WARN("init pushdown storage filter failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t schema_version = task_exec_ctx.get_query_tenant_begin_schema_version();
    das_rtdef.tenant_schema_version_ = schema_version;
  }
  if (OB_SUCC(ret) && MY_SPEC.ref_table_id_ == das_ctdef.ref_table_id_) {
    //only data table scan need to set flashback query info
    if (OB_FAIL(tsc_ctdef.flashback_item_.set_flashback_query_info(eval_ctx_, das_rtdef))) {
      LOG_WARN("failed to set flashback query snapshot version", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObTableID table_loc_id = MY_SPEC.get_table_loc_id();
    das_rtdef.table_loc_ = DAS_CTX(ctx_).get_table_loc_by_id(table_loc_id, das_ctdef.ref_table_id_);
    if (OB_ISNULL(das_rtdef.table_loc_)) {
      if (OB_ISNULL(loc_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get table loc by id failed", K(ret), K(table_loc_id), K(das_ctdef.ref_table_id_),
                 K(DAS_CTX(ctx_).get_table_loc_list()));
      } else if (OB_FAIL(DAS_CTX(ctx_).extended_table_loc(*loc_meta, das_rtdef.table_loc_))) {
        LOG_WARN("extended table location failed", K(ret), KPC(loc_meta));
      }
    }
  }
  return ret;
}

int ObTableScanOp::update_output_tablet_id()
{
  int ret = OB_SUCCESS;
  if (NULL != MY_SPEC.pdml_partition_id_) {
    const ObDASTabletLoc *data_tablet_loc = nullptr;
    int64_t output_id = OB_INVALID_ID;
    if (MY_SPEC.partition_id_calc_type_ > 0) {
      // partition id for gather statistics, index scan should output index partition id
      data_tablet_loc = scan_result_.get_tablet_loc();
    } else if (MY_SPEC.should_scan_index()) {
      data_tablet_loc = ObDASUtils::get_related_tablet_loc(*scan_result_.get_tablet_loc(), MY_SPEC.ref_table_id_);
    } else {
      data_tablet_loc = scan_result_.get_tablet_loc();
    }
    if (OB_ISNULL(data_tablet_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data tablet loc is null, value of pdml partition id will not be set", K(ret),
               K(MY_SPEC.should_scan_index()), K(MY_SPEC.ref_table_id_));
    } else {
      if (MY_SPEC.partition_id_calc_type_ == 0) {
        output_id = data_tablet_loc->tablet_id_.id();
      } else if (MY_SPEC.partition_id_calc_type_ == 1) {
        output_id = data_tablet_loc->first_level_part_id_ != OB_INVALID_ID ?
            data_tablet_loc->first_level_part_id_ : data_tablet_loc->partition_id_;
      } else if (MY_SPEC.partition_id_calc_type_ == 2) {
        output_id = data_tablet_loc->partition_id_;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get invalid partition id cacl type", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (is_vectorized()) {
        const int64_t batch_size = MY_SPEC.max_batch_size_;
        ObExpr *expr = MY_SPEC.pdml_partition_id_;
        ObDatum *datums = expr->locate_datums_for_update(eval_ctx_, batch_size);
        for (int64_t i = 0; i < batch_size; i++) {
          datums[i].set_int(output_id);
        }
        expr->set_evaluated_projected(eval_ctx_);
        LOG_TRACE("find the partition id expr in pdml table scan", K(ret), KPC(expr), KPC(data_tablet_loc), K(output_id));
      } else {
        // handle PDML partition id:
        // if partition id expr in TSC output_exprs,
        // set the TSC partition id to the corresponding expr frame
        ObExpr *expr = MY_SPEC.pdml_partition_id_;
        expr->locate_datum_for_write(eval_ctx_).set_int(output_id);
        expr->set_evaluated_projected(eval_ctx_);
        LOG_TRACE("find the partition id expr in pdml table scan", K(ret), KPC(data_tablet_loc), K(output_id));
      }
    }
  }
  return ret;
}

int ObTableScanOp::prepare_scan_range()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!MY_SPEC.batch_scan_flag_)) {
    ret = prepare_single_scan_range();
  } else {
    ret = prepare_batch_scan_range();
  }
  return ret;
}

int ObTableScanOp::prepare_batch_scan_range()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int64_t batch_size = 0;
  if (OB_SUCC(ret)) {
    if (!tsc_rtdef_.bnlj_params_.empty()) {
      tsc_rtdef_.group_size_ = tsc_rtdef_.bnlj_params_.at(0).gr_param_->count_;
      if (OB_UNLIKELY(tsc_rtdef_.group_size_ > tsc_rtdef_.max_group_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The amount of data exceeds the pre allocated memory", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch nlj params is empry", K(ret));
    }
  }
  GroupRescanParamGuard grp_guard(tsc_rtdef_, GET_PHY_PLAN_CTX(ctx_)->get_param_store_for_update());
  for (int64_t i = 0; OB_SUCC(ret) && i < tsc_rtdef_.group_size_; ++i) {
    //replace real param to param store to extract scan range
    grp_guard.switch_group_rescan_param(i);
    LOG_DEBUG("replace bnlj param to extract range", K(plan_ctx->get_param_store()));
    if (OB_FAIL(prepare_single_scan_range(i))) {
      LOG_WARN("prepare single scan range failed", K(ret));
    }
  }
  LOG_DEBUG("after prepare batch scan range", K(MY_INPUT.key_ranges_), K(MY_INPUT.ss_key_ranges_));
  return ret;
}

int ObTableScanOp::build_bnlj_params()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (!MY_SPEC.batch_scan_flag_) {
    // do nothing
  } else if (tsc_rtdef_.bnlj_params_.empty()) {
    tsc_rtdef_.bnlj_params_.set_capacity(MY_CTDEF.bnlj_param_idxs_.count());
    LOG_DEBUG("prepare batch scan range", K(MY_CTDEF.bnlj_param_idxs_), K(plan_ctx->get_param_store()));
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_CTDEF.bnlj_param_idxs_.count(); ++i) {
      int64_t param_idx = MY_CTDEF.bnlj_param_idxs_.at(i);
      const ObObjParam &bnlj_param = plan_ctx->get_param_store().at(param_idx);
      if (bnlj_param.is_ext_sql_array()) {
        ObSqlArrayObj *array_obj = reinterpret_cast<ObSqlArrayObj*>(bnlj_param.get_ext());
        OZ(tsc_rtdef_.bnlj_params_.push_back(GroupRescanParamInfo(param_idx, array_obj)));
      }
    }
    if (OB_SUCC(ret) && tsc_rtdef_.bnlj_params_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch param is empty in batch rescan", K(ret),
               K(MY_CTDEF.bnlj_param_idxs_), K(plan_ctx->get_param_store()));
    }
  }
  return ret;
}

int ObTableScanOp::prepare_single_scan_range(int64_t group_idx)
{
  int ret = OB_SUCCESS;
  ObQueryRangeArray key_ranges;
  ObQueryRangeArray ss_key_ranges;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObIAllocator &range_allocator = (table_rescan_allocator_ != nullptr ?
      *table_rescan_allocator_ : ctx_.get_allocator());
  bool is_same_type = true; // use for extract equal pre_query_range
  if (OB_FAIL(single_equal_scan_check_type(plan_ctx->get_param_store(), is_same_type))) {
    LOG_WARN("failed to check type about single equal scan", K(ret));
  } else if (is_same_type && MY_CTDEF.pre_query_range_.get_is_equal_and()) {
    int64_t column_count = MY_CTDEF.pre_query_range_.get_column_count();
    size_t range_size = sizeof(ObNewRange) + sizeof(ObObj) * column_count * 2;
    void *range_buffers = static_cast<char*>(tsc_rtdef_.range_buffers_) + tsc_rtdef_.range_buffer_idx_ * range_size;
    if (tsc_rtdef_.range_buffer_idx_ < 0 || tsc_rtdef_.range_buffer_idx_ >= tsc_rtdef_.max_group_size_) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("get wrong offset of range_buffers_", K(ret));
    } else if (OB_FAIL(ObSQLUtils::extract_equal_pre_query_range(
                MY_CTDEF.pre_query_range_,
                range_buffers,
                plan_ctx->get_param_store(),
                key_ranges))) {
      LOG_WARN("failed to extract equal pre query ranges", K(ret));
    }
  } else {
    if (OB_UNLIKELY(!need_extract_range())) {
      // virtual table, do nothing
    } else if (MY_CTDEF.pre_query_range_.is_contain_geo_filters() &&
               OB_FAIL(ObSQLUtils::extract_geo_query_range(
               MY_CTDEF.pre_query_range_,
               range_allocator,
               ctx_,
               key_ranges,
               MY_INPUT.mbr_filters_,
               ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
      LOG_WARN("failed to extract pre query ranges", K(ret));
    } else if (!MY_CTDEF.pre_query_range_.is_contain_geo_filters() &&
               OB_FAIL(ObSQLUtils::extract_pre_query_range(
                MY_CTDEF.pre_query_range_,
                range_allocator,
                ctx_,
                key_ranges,
                ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
      LOG_WARN("failed to extract pre query ranges", K(ret));
    } else if (MY_CTDEF.scan_ctdef_.is_external_table_) {
      uint64_t table_loc_id = MY_SPEC.get_table_loc_id();
      ObDASTableLoc *tab_loc = DAS_CTX(ctx_).get_table_loc_by_id(table_loc_id, MY_CTDEF.scan_ctdef_.ref_table_id_);
      if (OB_ISNULL(tab_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table lock is null", K(ret));
      } else if (OB_FAIL(ObExternalTableUtils::prepare_single_scan_range(
                                                  ctx_.get_my_session()->get_effective_tenant_id(),
                                                  MY_CTDEF.scan_ctdef_.ref_table_id_,
                                                  key_ranges,
                                                  range_allocator,
                                                  key_ranges,
                           tab_loc->loc_meta_->is_external_files_on_disk_))) {
        LOG_WARN("failed to prepare single scan range for external table", K(ret));
      }
    } else if (OB_FAIL(MY_CTDEF.pre_query_range_.get_ss_tablet_ranges(range_allocator,
                                  ctx_,
                                  ss_key_ranges,
                                  ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
      LOG_WARN("failed to final extract index skip query range", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!ss_key_ranges.empty()) {
    // index skip scan, ranges from extract_pre_query_range/get_ss_tablet_ranges,
    //  prefix range and postfix range is single range
    if (1 != ss_key_ranges.count() || 1 != key_ranges.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index skip scan range", K(ret), K(key_ranges), K(ss_key_ranges));
    } else {
      key_ranges.at(0)->table_id_ = MY_CTDEF.scan_ctdef_.ref_table_id_;
      key_ranges.at(0)->group_idx_ = group_idx;
      ss_key_ranges.at(0)->table_id_ = MY_CTDEF.scan_ctdef_.ref_table_id_;
      ss_key_ranges.at(0)->group_idx_ = group_idx;
      if (OB_FAIL(MY_INPUT.key_ranges_.push_back(*key_ranges.at(0)))
          || OB_FAIL(MY_INPUT.ss_key_ranges_.push_back(*ss_key_ranges.at(0)))) {
        LOG_WARN("store key range in TSC input failed", K(ret));
      }
    }
  } else {
    ObNewRange whole_range;
    ObNewRange *key_range = NULL;
    whole_range.set_whole_range();
    whole_range.table_id_ = MY_CTDEF.scan_ctdef_.ref_table_id_;
    whole_range.group_idx_ = group_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
      key_range = key_ranges.at(i);
      key_range->table_id_ = MY_CTDEF.scan_ctdef_.ref_table_id_;
      key_range->group_idx_ = group_idx;
      if (OB_FAIL(MY_INPUT.key_ranges_.push_back(*key_range))
          || OB_FAIL(MY_INPUT.ss_key_ranges_.push_back(whole_range))) {
        LOG_WARN("store key range in TSC input failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && MY_SPEC.is_vt_mapping_) {
    OZ(vt_result_converter_->convert_key_ranges(MY_INPUT.key_ranges_));
  }
  LOG_TRACE("prepare single scan range", K(ret), K(key_ranges), K(MY_INPUT.key_ranges_),
                                         K(MY_INPUT.ss_key_ranges_));
  return ret;
}

int ObTableScanOp::single_equal_scan_check_type(const ParamStore &param_store, bool& is_same_type)
{
  int ret = OB_SUCCESS;
  is_same_type = true;
  const ObIArray<ObQueryRange::ObEqualOff>& equal_offs =
      MY_CTDEF.pre_query_range_.get_raw_equal_offs();
  for (int64_t i = 0; OB_SUCC(ret) && is_same_type && i < equal_offs.count(); ++i) {
    int64_t param_idx = equal_offs.at(i).param_idx_;
    if (equal_offs.at(i).only_pos_) {
      // do nothing
    } else if (OB_UNLIKELY(param_idx < 0 || param_idx >= param_store.count())) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("out of param store", K(ret), K(param_idx), K(param_store.count()));
    } else if (equal_offs.at(i).pos_type_ != param_store.at(param_idx).get_type()
               && !param_store.at(param_idx).is_null()) {
      is_same_type = false;
    }
  }
  return ret;
}

int ObTableScanOp::init_converter()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_vt_mapping_) {
    ObSqlCtx *sql_ctx = NULL;
    if (MY_SPEC.is_index_global_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table id is not match", K(ret), K(MY_CTDEF), K(MY_SPEC.is_index_global_));
    } else if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx())
        || OB_ISNULL(sql_ctx->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: sql ctx or schema guard is null", K(ret));
    } else {
      if (OB_NOT_NULL(vt_result_converter_)) {
        vt_result_converter_->destroy();
        vt_result_converter_->~ObVirtualTableResultConverter();
        vt_result_converter_ = nullptr;
      }
      const ObTableSchema *org_table_schema = NULL;
      const AgentVtAccessMeta &agent_vt_meta = MY_SPEC.agent_vt_meta_;
      void *buf = ctx_.get_allocator().alloc(sizeof(ObVirtualTableResultConverter));
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator", K(ret));
      } else if (FALSE_IT(vt_result_converter_ = new (buf) ObVirtualTableResultConverter)) {
      } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(
                 MTL_ID(),
                 agent_vt_meta.vt_table_id_, org_table_schema))) {
        LOG_WARN("get table schema failed", K(agent_vt_meta.vt_table_id_), K(ret));
      } else if (OB_ISNULL(org_table_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("org table schema is null", K(MTL_ID()),
            K(agent_vt_meta.vt_table_id_), K(sql_ctx->schema_guard_->get_tenant_id()), K(ret));
      } else if (OB_FAIL(reuse_table_rescan_allocator())) {
        LOG_WARN("get table allocator failed", K(ret));
      } else if (OB_FAIL(vt_result_converter_->reset_and_init(
                                          table_rescan_allocator_,
                                          GET_MY_SESSION(ctx_),
                                          &agent_vt_meta.access_row_types_,
                                          &agent_vt_meta.key_types_,
                                          &ctx_.get_allocator(),
                                          org_table_schema,
                                          &agent_vt_meta.access_column_ids_,
                                          MY_SPEC.has_tenant_id_col_,
                                          MY_SPEC.tenant_id_col_idx_
                                          ))) {
        LOG_WARN("failed to init converter", K(ret));
      }
    }
    LOG_TRACE("debug init converter", K(ret), K(MY_CTDEF));
  }
  return ret;
}

int ObTableScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  DASTableLocList &table_locs = ctx_.get_das_ctx().get_table_loc_list();
  ObSQLSessionInfo *my_session = NULL;
  cur_trace_id_ = ObCurTraceId::get();
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(ObDASUtils::check_nested_sql_mutating(MY_SPEC.ref_table_id_, ctx_, true))) {
    LOG_WARN("failed to check stmt table", K(ret), K(MY_SPEC.ref_table_id_));
  } else if (OB_FAIL(init_table_scan_rtdef())) {
    LOG_WARN("prepare scan param failed", K(ret));
  } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(init_converter())) {
    LOG_WARN("failed to init converter", K(ret));
  } else {
    if (MY_SPEC.report_col_checksum_) {
      if (PHY_TABLE_SCAN == MY_SPEC.get_type()) {
        // heap table ddl doesn't have sample scan, report checksum directly
        report_checksum_ = true;
      } else if (PHY_BLOCK_SAMPLE_SCAN == MY_SPEC.get_type() || PHY_ROW_SAMPLE_SCAN == MY_SPEC.get_type()) {
        // normal ddl need sample scan first, report_cheksum_ will be marked as true when rescan
        report_checksum_ = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_ddl_column_checksum())) {
      LOG_WARN("init ddl column checksum", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // here need add plan batch_size, because in vectorized execution,
    // left batch may greater than OB_MAX_BULK_JOIN_ROWS
    tsc_rtdef_.max_group_size_ = OB_MAX_BULK_JOIN_ROWS + MY_SPEC.plan_->get_batch_size();
    if (MY_CTDEF.pre_query_range_.get_is_equal_and()) {
      int64_t column_count = MY_CTDEF.pre_query_range_.get_column_count();
      size_t range_size = sizeof(ObNewRange) + sizeof(ObObj) * column_count * 2;
      if (!MY_SPEC.batch_scan_flag_) {
        tsc_rtdef_.range_buffers_ = ctx_.get_allocator().alloc(range_size);
      } else {
        tsc_rtdef_.range_buffers_ = ctx_.get_allocator().alloc(tsc_rtdef_.max_group_size_ * range_size);
      }
      if (OB_ISNULL(tsc_rtdef_.range_buffers_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(range_size), K(tsc_rtdef_.range_buffers_));
      } else if (!MY_SPEC.batch_scan_flag_) {
        ObNewRange *key_range = new(tsc_rtdef_.range_buffers_) ObNewRange();
      } else {
        for (int64_t i = 0; i < tsc_rtdef_.max_group_size_; ++i) {
          char *range_buffers_off = static_cast<char*>(tsc_rtdef_.range_buffers_) + i * range_size;
          ObNewRange *key_range = new(range_buffers_off) ObNewRange();
        }
      }
    }
  }
  if (OB_SUCC(ret) && MY_SPEC.is_global_index_back()) {
    if (OB_NOT_NULL(global_index_lookup_op_)) {
      global_index_lookup_op_->destroy();
      global_index_lookup_op_->~ObGlobalIndexLookupOpImpl();
      global_index_lookup_op_ = nullptr;
    }
    void *lookup_buf = ctx_.get_allocator().alloc(sizeof(ObGlobalIndexLookupOpImpl));
    if (nullptr == lookup_buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(lookup_buf));
    } else {
      global_index_lookup_op_ = new (lookup_buf) ObGlobalIndexLookupOpImpl(this);
      if (OB_FAIL(global_index_lookup_op_->open())) {
        LOG_WARN("failed to open global index lookup op", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (das_ref_.has_task()) {
    int tmp_ret = fill_storage_feedback_info();
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("fill storage feedback info failed", KR(tmp_ret));
    }
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all das task failed", KR(ret));
    }
  }
  if (MY_SPEC.is_global_index_back()) {
    int save_ret = ret;
    if (OB_ISNULL(global_index_lookup_op_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", KR(ret));
    } else if (OB_FAIL(global_index_lookup_op_->close())) {
      LOG_WARN("close global index lookup op failed", KR(ret));
    }
    ret = (OB_SUCCESS == save_ret) ? ret : save_ret;
  }
  if (OB_SUCC(ret)) {
    fill_sql_plan_monitor_info();
  }
  if (OB_SUCC(ret)) {
    iter_end_ = false;
    need_init_before_get_row_ = true;
  }
  return ret;
}

void ObTableScanOp::fill_sql_plan_monitor_info()
{
  oceanbase::common::ObDiagnoseSessionInfo *di = oceanbase::common::ObDiagnoseSessionInfo::get_local_diagnose_info();
  if (OB_LIKELY(di)) {
    // Hope to demostrate:
    // 1. how many bytes read from io (IO_READ_BYTES)
    // 2. how many bytes in total (DATA_BLOCK_READ_CNT + INDEX_BLOCK_READ_CNT) * 16K (approximately, many diff for each table)
    // 3. how many rows processed before filtering (MEMSTORE_READ_ROW_COUNT + SSSTORE_READ_ROW_COUNT)
    op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::IO_READ_BYTES;
    op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::TOTAL_READ_BYTES;
    op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::TOTAL_READ_ROW_COUNT;
    op_monitor_info_.otherstat_1_value_ = EVENT_GET(ObStatEventIds::IO_READ_BYTES, di);
    // NOTE: this is not always accurate, as block size change be change from default 16K to any value
    op_monitor_info_.otherstat_2_value_ = (EVENT_GET(ObStatEventIds::DATA_BLOCK_READ_CNT, di) + EVENT_GET(ObStatEventIds::INDEX_BLOCK_READ_CNT, di)) * 16 * 1024;
    op_monitor_info_.otherstat_3_value_ = EVENT_GET(ObStatEventIds::MEMSTORE_READ_ROW_COUNT, di) + EVENT_GET(ObStatEventIds::SSSTORE_READ_ROW_COUNT, di);
  }
}

int ObTableScanOp::do_init_before_get_row()
{
  int ret = OB_SUCCESS;
  if (need_init_before_get_row_) {
    LOG_DEBUG("do init before get row", K(MY_SPEC.use_dist_das_), K(MY_SPEC.gi_above_));
    if (OB_UNLIKELY(iter_end_)) {
      //do nothing
    } else {
      if (MY_SPEC.gi_above_) {
        ObGranuleTaskInfo info;
        if (OB_FAIL(get_access_tablet_loc(info))) {
          LOG_WARN("fail to get access partition failed", K(ret));
        } else if (OB_FAIL(reassign_task_ranges(info))) {
          LOG_WARN("assign task ranges failed", K(ret));
        }
      }
      if (OB_FAIL(ret) || OB_UNLIKELY(iter_end_)) {
        // do nothing
      } else if (OB_FAIL(prepare_all_das_tasks())) {
        LOG_WARN("prepare das task failed", K(ret));
      } else if (OB_FAIL(do_table_scan())) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to do table scan", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObTableScanOp::destroy()
{
  tsc_rtdef_.~ObTableScanRtDef();
  ObOperator::destroy();
  das_ref_.reset();
  if (OB_NOT_NULL(vt_result_converter_)) {
    vt_result_converter_->destroy();
    vt_result_converter_->~ObVirtualTableResultConverter();
    vt_result_converter_ = nullptr;
  }
  if (OB_NOT_NULL(global_index_lookup_op_)) {
    global_index_lookup_op_->destroy();
    global_index_lookup_op_->~ObGlobalIndexLookupOpImpl();
    global_index_lookup_op_ = nullptr;
  }
}

int ObTableScanOp::fill_storage_feedback_info()
{
  int ret = OB_SUCCESS;
  // fill storage feedback info for acs
  ObTableScanParam &scan_param = DAS_SCAN_OP(*das_ref_.begin_task_iter())->get_scan_param();
  bool is_index_back = scan_param.scan_flag_.index_back_;
  ObTableScanStat &table_scan_stat = GET_PHY_PLAN_CTX(ctx_)->get_table_scan_stat();
  if (MY_SPEC.should_scan_index()) {
    table_scan_stat.query_range_row_count_ = scan_param.idx_table_scan_stat_.access_row_cnt_;
    if (is_index_back) {
      table_scan_stat.indexback_row_count_ = scan_param.idx_table_scan_stat_.out_row_cnt_;
      table_scan_stat.output_row_count_ = scan_param.main_table_scan_stat_.out_row_cnt_;
    } else {
      table_scan_stat.indexback_row_count_ = -1;
      table_scan_stat.output_row_count_ = scan_param.idx_table_scan_stat_.out_row_cnt_;
    }
    LOG_DEBUG("index scan feedback info for acs",
              K(scan_param.idx_table_scan_stat_), K(table_scan_stat));
  } else {
    table_scan_stat.query_range_row_count_ = scan_param.main_table_scan_stat_.access_row_cnt_;
    table_scan_stat.indexback_row_count_ = -1;
    table_scan_stat.output_row_count_ = scan_param.main_table_scan_stat_.out_row_cnt_;
    LOG_DEBUG("table scan feedback info for acs", K(scan_param.main_table_scan_stat_), K(table_scan_stat));
  }

  // 填充计划淘汰策略所需要的反馈信息
  ObIArray<ObTableRowCount> &table_row_count_list =
      GET_PHY_PLAN_CTX(ctx_)->get_table_row_count_list();
  //仅索引回表时，存储层会将执行扫描索引数据放在idx_table_scan_stat_中;
  //对于仅扫描主表或索引表的情况, 存储层会将执行扫描索引数据放在main_table_scan_stat_中
  if (!got_feedback_) {
    got_feedback_ = true;
    if (MY_SPEC.should_scan_index() && scan_param.scan_flag_.is_index_back()) {
      if (scan_param.scan_flag_.is_need_feedback()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = table_row_count_list.push_back(ObTableRowCount(
                                                                      MY_SPEC.id_, scan_param.idx_table_scan_stat_.access_row_cnt_)))) {
          // 这里忽略插入失败时的错误码. OB的Array保证push_back失败的情况下count()仍是有效的
          // 如果一张表的信息没有被插入成功，最多
          // 只会导致后续判断计划能否淘汰时无法使用这张表的信息进行判断，从而
          // 导致某些计划无法被淘汰，相当于回退到了没有这部分淘汰策略时的逻辑
          // 这里不希望淘汰机制的错误码影响原有执行逻辑 @ banliu.zyd
          LOG_WARN("push back table_id-row_count failed", K(tmp_ret), K(MY_SPEC.ref_table_id_),
                   "access row count", scan_param.idx_table_scan_stat_.access_row_cnt_);
        }
      }
    } else {
      if (scan_param.scan_flag_.is_need_feedback()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = table_row_count_list.push_back(ObTableRowCount(
                                                                      MY_SPEC.id_, scan_param.main_table_scan_stat_.access_row_cnt_)))) {
          LOG_WARN("push back table_id-row_count failed but we won't stop execution", K(tmp_ret));
        }
      }
    }

  }
  LOG_DEBUG("table scan feed back info for buffer table",
            K(MY_CTDEF.scan_ctdef_.ref_table_id_), K(MY_SPEC.should_scan_index()),
            "is_need_feedback", scan_param.scan_flag_.is_need_feedback(),
            "idx access row count", scan_param.idx_table_scan_stat_.access_row_cnt_,
            "main access row count", scan_param.main_table_scan_stat_.access_row_cnt_);
  return ret;
}

int ObTableScanOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  in_rescan_ = true;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to exec inner rescan");
  } else if (MY_SPEC.is_global_index_back()) {
    if (OB_ISNULL(global_index_lookup_op_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments",K(ret));
    } else {
      global_index_lookup_op_->get_brs().size_ = brs_.size_ ;
      global_index_lookup_op_->get_brs().end_ = brs_.end_;
      if (OB_FAIL(global_index_lookup_op_->rescan())) {
        LOG_WARN("failed to get next batch",K(ret));
      }
    }
  } else {
    if (OB_FAIL(inner_rescan_for_tsc())) {
      LOG_WARN("failed to get next row",K(ret));
    }
  }
  return ret;
}
int ObTableScanOp::inner_rescan_for_tsc()
{
  int ret = OB_SUCCESS;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  iter_end_ = false;
  MY_INPUT.key_ranges_.reuse();
  MY_INPUT.ss_key_ranges_.reuse();
  MY_INPUT.mbr_filters_.reuse();
  if (OB_FAIL(build_bnlj_params())) {
    // At start of each round of batch rescan, NLJ will fill param_store with
    // batch parameters. After each right operator rescan, NLJ will fill
    // param_store with current rescan's parameters.
    // Therefore, we need to get and save bnlj parameters here or they will be
    // replaced by NLJ.
    LOG_WARN("build batch nlj params failed", KR(ret));
  } else if (!need_real_rescan()) {
    ret = set_batch_iter(ctx_.get_das_ctx().jump_read_group_id_);
  } else {
    if (is_virtual_table(MY_SPEC.ref_table_id_)
        || !das_ref_.is_all_local_task()
        || (MY_SPEC.use_dist_das_ && nullptr != MY_CTDEF.das_dppr_tbl_)) {
      ret = close_and_reopen();
    } else {
      ret = local_iter_rescan();
    }
  }
  return ret;
}
int ObTableScanOp::close_and_reopen()
{
  int ret = OB_SUCCESS;
  iter_end_ = false;
  if (OB_FAIL(inner_close())) {
    LOG_WARN("fail to close op", K(ret));
  } else if (OB_FAIL(reuse_table_rescan_allocator())) {
    LOG_WARN("reuse table rescan allocator failed", K(ret));
  } else {
    das_ref_.reuse();
    need_final_limit_ = false;
    //in order to avoid memory expansion caused by repeatedly creating DAS Tasks,
    //stmt allocator uses DAS allocator in the reopen process
    tsc_rtdef_.scan_rtdef_.stmt_allocator_.set_alloc(&das_ref_.get_das_alloc());
    tsc_rtdef_.scan_rtdef_.scan_allocator_.set_alloc(table_rescan_allocator_);
    MY_INPUT.key_ranges_.reuse();
    MY_INPUT.ss_key_ranges_.reuse();
    MY_INPUT.mbr_filters_.reuse();
  }
  return ret;
}

int ObTableScanOp::local_iter_rescan()
{
  int ret = OB_SUCCESS;
  ObGranuleTaskInfo info;
  if (OB_FAIL(get_access_tablet_loc(info))) {
    LOG_WARN("fail to get access partition", K(ret));
  } else if (OB_FAIL(local_iter_reuse())) {
    LOG_WARN("failed to reset query range", K(ret));
  } else if (OB_FAIL(reassign_task_ranges(info))) {
    LOG_WARN("assign task ranges failed", K(ret));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (MY_INPUT.key_ranges_.empty() &&
      OB_FAIL(prepare_scan_range())) { // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  } else {
    DASTaskIter task_iter = das_ref_.begin_task_iter();
    for (; OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
      ObDASScanOp *scan_op = DAS_SCAN_OP(*task_iter);
      if (MY_SPEC.gi_above_) {
        if (!MY_SPEC.is_index_global_ && MY_CTDEF.lookup_ctdef_ != nullptr) {
          //is local index lookup, need to set the lookup ctdef to the das scan op
          ObDASTableLoc *lookup_table_loc = tsc_rtdef_.lookup_rtdef_->table_loc_;
          ObDASTabletLoc *lookup_tablet_loc = ObDASUtils::get_related_tablet_loc(
              *MY_INPUT.tablet_loc_, MY_CTDEF.lookup_ctdef_->ref_table_id_);
          if (OB_FAIL(scan_op->set_lookup_tablet_id(lookup_tablet_loc->tablet_id_))) {
            LOG_WARN("set lookup tablet id failed", K(ret), KPC(lookup_tablet_loc));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cherry_pick_range_by_tablet_id(scan_op))) {
          LOG_WARN("prune query range by partition id failed", K(ret));
        } else if (OB_FAIL(init_das_group_range(0, tsc_rtdef_.group_size_))) {
          LOG_WARN("set group range failed", K(ret), K_(tsc_rtdef_.group_size));
        } else if (OB_FAIL(MTL(ObDataAccessService*)->rescan_das_task(das_ref_, *scan_op))) {
          LOG_WARN("rescan das task failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && das_ref_.has_task()) {
      //prepare to output row
      scan_result_ = das_ref_.begin_result_iter();
      if (OB_FAIL(update_output_tablet_id())) {
        LOG_WARN("update output row pkey failed", K(ret), KPC(scan_result_.get_tablet_loc()));
      }
    }
  }
  return ret;
}

/*
 * the following three functions are used for blocked nested loop join
 */
int ObTableScanOp::local_iter_reuse()
{
  int ret = OB_SUCCESS;
  for (DASTaskIter task_iter = das_ref_.begin_task_iter();
      !task_iter.is_end(); ++task_iter) {
    ObDASScanOp *scan_op = DAS_SCAN_OP(*task_iter);
    bool need_switch_param = (scan_op->get_tablet_loc() != MY_INPUT.tablet_loc_ &&
                                MY_INPUT.tablet_loc_ != nullptr);
    if (MY_INPUT.tablet_loc_ != nullptr) {
      scan_op->set_tablet_id(MY_INPUT.tablet_loc_->tablet_id_);
      scan_op->set_ls_id(MY_INPUT.tablet_loc_->ls_id_);
      scan_op->set_tablet_loc(MY_INPUT.tablet_loc_);
    }
    scan_op->reuse_iter();
  }
  if (OB_FAIL(reuse_table_rescan_allocator())) {
    LOG_WARN("get table allocator", K(ret));
  } else {
    tsc_rtdef_.scan_rtdef_.scan_allocator_.set_alloc(table_rescan_allocator_);
    MY_INPUT.key_ranges_.reuse();
    MY_INPUT.ss_key_ranges_.reuse();
    MY_INPUT.mbr_filters_.reuse();
  }
  return ret;
}
//TSC has its own switch iterator && bnl switch iterator
int ObTableScanOp::switch_iterator()
{
  return OB_NOT_SUPPORTED;
}

bool ObTableScanOp::need_real_rescan()
{
  bool bret = false;
  if (!MY_SPEC.batch_scan_flag_) {
    bret = true;
  } else {
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    int64_t param_idx = tsc_rtdef_.bnlj_params_.at(0).param_idx_;
    //param store has been inited by nlj, to fetch next batch result
    bret = plan_ctx->get_param_store().at(param_idx).is_ext_sql_array();
  }
  return bret;
}

int ObTableScanOp::switch_batch_iter()
{
  int ret = OB_SUCCESS;
  for (DASTaskIter task_iter = das_ref_.begin_task_iter();
       OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
    ObDASGroupScanOp *group_scan_op = DAS_GROUP_SCAN_OP(*task_iter);
    if (OB_FAIL(group_scan_op->switch_scan_group())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch batch iter failed", K(ret));
      } else {
        iter_end_ = true;
      }
    }
  }
  if (OB_SUCC(ret) && !iter_end_) {
    if (!das_ref_.has_task()) {
      iter_end_ = true;
    } else {
      //prepare to output row
      scan_result_ = das_ref_.begin_result_iter();
      if (OB_FAIL(update_output_tablet_id())) {
        LOG_WARN("update output row pkey failed", K(ret), KPC(scan_result_.get_tablet_loc()));
      }
    }
  }
  return ret;
}

int ObTableScanOp::set_batch_iter(int64_t group_id)
{
  int ret = OB_SUCCESS;
  for (DASTaskIter task_iter = das_ref_.begin_task_iter();
       OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
    ObDASGroupScanOp *group_scan_op = DAS_GROUP_SCAN_OP(*task_iter);
    if (OB_FAIL(group_scan_op->set_scan_group(group_id))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch batch iter failed", K(ret));
      } else {
        iter_end_ = true;
      }
    }
  }
  if (OB_SUCC(ret) && !iter_end_) {
    if (!das_ref_.has_task()) {
      iter_end_ = true;
    } else {
      //prepare to output row
      scan_result_ = das_ref_.begin_result_iter();
      if (OB_FAIL(update_output_tablet_id())) {
        LOG_WARN("update output row pkey failed", K(ret), KPC(scan_result_.get_tablet_loc()));
      }
    }
  }
  return ret;
}




int ObTableScanOp::get_next_row_with_das()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  lib::CompatModeGuard g(MY_SPEC.is_vt_mapping_ ? lib::Worker::CompatMode::MYSQL : lib::get_compat_mode());
  //it means multi-partition limit pushed down in DAS TSC
  //need to calc final limit row
  if (need_final_limit_ && limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
    ret = OB_ITER_END;
    LOG_DEBUG("get next row with das iter end", K(ret), K_(limit_param), K_(output_row_cnt));
  }
  while (OB_SUCC(ret) && !got_row) {
    clear_evaluated_flag();
    if (OB_FAIL(scan_result_.get_next_row())) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(scan_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          }
        } else if (OB_FAIL(update_output_tablet_id())) {
          LOG_WARN("update output row pkey failed", K(ret), K(scan_result_.get_tablet_loc()->tablet_id_));
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else {
      // We need do filter first before do the limit.
      // See the issue 47201028.
      bool filtered = false;
      if (need_final_limit_ && !MY_SPEC.filters_.empty()) {
        if (OB_FAIL(filter_row(filtered))) {
          LOG_WARN("das get_next_row filter row failed", K(ret));
        } else {
          if(filtered) {
            //Do nothing
          } else {
            ++input_row_cnt_;
          }
        }
      } else {
        ++input_row_cnt_;
      }
      if (need_final_limit_ && input_row_cnt_ <= limit_param_.offset_) {
        continue;
      } else {
        if (need_final_limit_ && !MY_SPEC.filters_.empty() && filtered) {
          //Do nothing
        } else {
          ++output_row_cnt_;
          got_row = true;
        }
      }
    }
  }
  return ret;
}

int ObTableScanOp::get_next_batch_with_das(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = capacity;
  //it means multi-partition limit pushed down in DAS TSC
  //need to calc final limit row
  lib::CompatModeGuard g(MY_SPEC.is_vt_mapping_ ? lib::Worker::CompatMode::MYSQL : lib::get_compat_mode());
  while (OB_SUCC(ret) && need_final_limit_ && input_row_cnt_ < limit_param_.offset_) {
    if (input_row_cnt_ + batch_size > limit_param_.offset_) {
      // adjust iterating count for last batch
      batch_size = limit_param_.offset_ - input_row_cnt_;
    }
    clear_evaluated_flag();
    // ObNewIterIterator::get_next_rows() may return rows too when got OB_ITER_END.
    // It's hard to use, we split it into two calls here since get_next_rows() is reentrant
    // when got OB_ITER_END.
    ret = scan_result_.get_next_rows(count, batch_size);
    if (OB_ITER_END == ret && count > 0) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        input_row_cnt_ += count;
        if (OB_FAIL(scan_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          }
        } else if (OB_FAIL(update_output_tablet_id())) {
          LOG_WARN("update output row pkey failed", K(ret), K(scan_result_.get_tablet_loc()->tablet_id_));
        }
      } else {
        LOG_WARN("get next batch from das result failed", K(ret));
      }
    } else {
      // We need do filter first before do the limit.
      // See the issue 47201028.
      if(need_final_limit_ && !MY_SPEC.filters_.empty() && count > 0) {
        bool all_filtered = false;
        if (OB_FAIL(filter_batch_rows(MY_SPEC.filters_,
                                      *brs_.skip_,
                                      count,
                                      all_filtered))) {
          LOG_WARN("filter batch failed in das get_next_batch", K(ret));
        } else if (all_filtered) {
          //Do nothing.
          brs_.skip_->reset(count);
        } else {
          int64_t skipped_rows_count = brs_.skip_->accumulate_bit_cnt(count);
          input_row_cnt_ += count - skipped_rows_count;
          brs_.skip_->reset(count);
        }
      } else {
        input_row_cnt_ += count;
      }
    }
  }
  if (OB_SUCC(ret) && need_final_limit_) {
    batch_size = capacity;
    count = 0;
    if (output_row_cnt_ >= limit_param_.limit_) {
      ret = OB_ITER_END;
      LOG_DEBUG("get next row with das iter end", K(ret), K_(limit_param), K_(output_row_cnt));
    } else if (output_row_cnt_ + batch_size > limit_param_.limit_) {
      batch_size = limit_param_.limit_ - output_row_cnt_;
    }
  }
  bool got_batch = false;
  while (OB_SUCC(ret) && !got_batch) {
    clear_evaluated_flag();
    // ObNewIterIterator::get_next_rows() may return rows too when got OB_ITER_END.
    // It's hard to use, we split it into two calls here since get_next_rows() is reentrant
    // when got OB_ITER_END.
    ret = scan_result_.get_next_rows(count, batch_size);
    if (OB_ITER_END == ret && count > 0) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(scan_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          }
        } else if (OB_FAIL(update_output_tablet_id())) {
          LOG_WARN("update output row pkey failed", K(ret), K(scan_result_.get_tablet_loc()->tablet_id_));
        }
      } else {
        LOG_WARN("get next batch from das result failed", K(ret));
      }
    } else {
      // We need do filter first before do the limit.
      // See the issue 47201028.
      if (need_final_limit_ && !MY_SPEC.filters_.empty() && count > 0) {
        bool all_filtered = false;
        if (OB_FAIL(filter_batch_rows(MY_SPEC.filters_,
                                      *brs_.skip_,
                                      count,
                                      all_filtered))) {
          LOG_WARN("filter batch failed in das get_next_batch", K(ret));
        } else if (all_filtered) {
          //Do nothing.
          brs_.skip_->reset(count);
        } else {
          int64_t skipped_rows_count = brs_.skip_->accumulate_bit_cnt(count);
          got_batch = true;
          output_row_cnt_ += (count - skipped_rows_count);
          input_row_cnt_ += (count - skipped_rows_count);
        }
      } else {
        got_batch = true;
        output_row_cnt_ += count;
        input_row_cnt_ += count;
      }
    }
  }
  return ret;
}

int ObTableScanOp::inner_get_next_row_implement()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && MY_SPEC.is_global_index_back()) {
    if (OB_ISNULL(global_index_lookup_op_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments",K(ret));
    } else if (OB_FAIL(global_index_lookup_op_->get_next_row())) {
      LOG_WARN("failed to get next batch",K(ret));
    }
  } else {
    if (OB_FAIL(inner_get_next_row_for_tsc())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row",K(ret));
      }
    }
  }
  return ret;
}
int ObTableScanOp::inner_get_next_row_for_tsc()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == limit_param_.limit_)) {
    // 涉及的partition个数为0或者limit 0，直接返回iter end
    ret = OB_ITER_END;
  } else if (OB_FAIL(do_init_before_get_row())) {
    LOG_WARN("failed to init before get row", K(ret));
  } else if (iter_end_) {
    // 保证没有数据的时候多次调用都能返回OB_ITER_END，或者空scan直接返回iter end
    ret = OB_ITER_END;
    LOG_DEBUG("inner get next row meet a iter end", K(MY_SPEC.id_), K(this), K(lbt()));
  } else if (0 == (++iterated_rows_ % CHECK_STATUS_ROWS_INTERVAL)
             && OB_FAIL(ctx_.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(get_next_row_with_das())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row from ObNewRowIterator", K(ret));
    } else {
      //set found_rows:当返回总行数不为0,且带有非0offset，才需要设置found_rows的值,
      //来修正最终设置到session内部的found_rows
      if (MY_SPEC.is_top_table_scan_ && limit_param_.offset_ > 0) {
        if (output_row_cnt_ > 0) {
          int64_t total_count = output_row_cnt_ + limit_param_.offset_;
          ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
          NG_TRACE_EXT(found_rows, OB_ID(total_count), total_count,
                       OB_ID(offset), limit_param_.offset_);
          plan_ctx->set_found_rows(total_count);
        }
      }
    }
  } else {
    NG_TRACE_TIMES_WITH_TRACE_ID(1, cur_trace_id_, get_row);
    if (MY_SPEC.is_vt_mapping_
        && OB_FAIL(vt_result_converter_->convert_output_row(eval_ctx_,
                                                            MY_CTDEF.get_das_output_exprs(),
                                                            MY_SPEC.agent_vt_meta_.access_exprs_))) {
      LOG_WARN("failed to convert output row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ExprFixedArray &storage_output = MY_CTDEF.get_das_output_exprs();
    if (!MY_SPEC.is_global_index_back()) {
      LOG_DEBUG("storage output row", "row", ROWEXPR2STR(eval_ctx_, storage_output), K(MY_CTDEF.scan_ctdef_.ref_table_id_));
    }
    if (OB_FAIL(add_ddl_column_checksum())) {
      LOG_WARN("add ddl column checksum failed", K(ret));
    }
  }
  if (OB_UNLIKELY(OB_ITER_END == ret && das_ref_.has_task())) {
//    ObIPartitionGroup *partition = NULL;
//    ObIPartitionGroupGuard *guard = NULL;
//    if (OB_ISNULL(guard)) {
//    } else if (OB_ISNULL(partition = guard->get_partition_group())) {
//    } else if (DAS_SCAN_OP->get_scan_param().main_table_scan_stat_.bf_access_cnt_ > 0) {
//      partition->feedback_scan_access_stat(DAS_SCAN_OP->get_scan_param());
//    }
    ObTableScanParam &scan_param = DAS_SCAN_OP(*das_ref_.begin_task_iter())->get_scan_param();
    ObTableScanStat &table_scan_stat = GET_PHY_PLAN_CTX(ctx_)->get_table_scan_stat();
    fill_table_scan_stat(scan_param.main_table_scan_stat_, table_scan_stat);
    if (MY_SPEC.should_scan_index() && scan_param.scan_flag_.index_back_) {
      fill_table_scan_stat(scan_param.idx_table_scan_stat_, table_scan_stat);
    }
    scan_param.main_table_scan_stat_.reset_cache_stat();
    scan_param.idx_table_scan_stat_.reset_cache_stat();
    iter_end_ = true;
    if (OB_FAIL(report_ddl_column_checksum())) {
      LOG_WARN("report checksum failed", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}
int ObTableScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && MY_SPEC.is_global_index_back()) {
    int64_t count = 0;
    if (OB_ISNULL(global_index_lookup_op_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments",K(ret));
    } else {
      global_index_lookup_op_->get_brs().size_ = brs_.size_ ;
      global_index_lookup_op_->get_brs().end_ = brs_.end_;
      if (OB_FAIL(global_index_lookup_op_->get_next_rows(count, max_row_cnt))) {
        LOG_WARN("failed to get next rows",K(ret), K(max_row_cnt));
      } else {
        brs_.size_ = global_index_lookup_op_->get_brs().size_;
        brs_.end_ = global_index_lookup_op_->get_brs().end_;
      }
    }
  } else {
    if (OB_FAIL(inner_get_next_batch_for_tsc(max_row_cnt))) {
      LOG_WARN("failed to get next row",K(ret));
    }
  }
  return ret;
}
int ObTableScanOp::inner_get_next_batch_for_tsc(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  int64_t batch_size = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (OB_UNLIKELY(0 == limit_param_.limit_)) {
    // 涉及的partition个数为0或者limit 0，直接返回iter end
    brs_.size_ = 0;
    brs_.end_ = true;
  } else if (OB_FAIL(do_init_before_get_row())) {
    LOG_WARN("failed to init before get row", K(ret));
  } else if (iter_end_) {
    // 保证没有数据的时候多次调用都能返回OB_ITER_END，或者空scan直接返回iter end
    brs_.size_ = 0;
    brs_.end_ = true;
    LOG_DEBUG("inner get next row meet a iter end", K(MY_SPEC.id_), K(this), K(lbt()));
  } else {
    access_expr_sanity_check();
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_idx(0);
    batch_info_guard.set_batch_size(batch_size);
    brs_.size_ = 0;
    brs_.end_ = false;
    if (0 == batch_size) {
      brs_.end_ = true;
    } else if (OB_FAIL(get_next_batch_with_das(brs_.size_, batch_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next batch with mode failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        brs_.end_ = true;
      }
    }
    access_expr_sanity_check();
    // TODO bin.lb: for calc_exprs_ set ObEvalInfo::cnt_ to brs_.batch_size_ if evaluated
  }

  if (OB_SUCC(ret) && brs_.end_) {
    //set found_rows:当返回总行数不为0,且带有非0offset，才需要设置found_rows的值,
    //来修正最终设置到session内部的found_rows
    iter_end_ = true;
    if (MY_SPEC.is_top_table_scan_
        && (limit_param_.offset_ > 0)) {
      if (output_row_cnt_ > 0) {
        int64_t total_count = output_row_cnt_ + limit_param_.offset_;
        ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
        NG_TRACE_EXT(found_rows, OB_ID(total_count), total_count,
                     OB_ID(offset), limit_param_.offset_);
        plan_ctx->set_found_rows(total_count);
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ExprFixedArray &storage_output = MY_CTDEF.get_das_output_exprs();
    if (!MY_SPEC.is_global_index_back()) {
      PRINT_VECTORIZED_ROWS(SQL, DEBUG, eval_ctx_, storage_output, brs_.size_, K(MY_CTDEF.scan_ctdef_.ref_table_id_));
    }
    if (OB_FAIL(add_ddl_column_checksum_batch(brs_.size_))) {
      LOG_WARN("add ddl column checksum failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && brs_.end_ && das_ref_.has_task()) {
//    ObIPartitionGroup *partition = NULL;
//    ObIPartitionGroupGuard *guard = NULL;
//    if (OB_ISNULL(guard)) {
//    } else if (OB_ISNULL(partition = guard->get_partition_group())) {
//    } else if (DAS_SCAN_OP->get_scan_param().main_table_scan_stat_.bf_access_cnt_ > 0) {
//      partition->feedback_scan_access_stat(DAS_SCAN_OP->get_scan_param());
//    }
    ObTableScanParam &scan_param = DAS_SCAN_OP(*das_ref_.begin_task_iter())->get_scan_param();
    ObTableScanStat &table_scan_stat = GET_PHY_PLAN_CTX(ctx_)->get_table_scan_stat();
    fill_table_scan_stat(scan_param.main_table_scan_stat_, table_scan_stat);
    if (MY_SPEC.should_scan_index() && scan_param.scan_flag_.index_back_) {
      fill_table_scan_stat(scan_param.idx_table_scan_stat_, table_scan_stat);
    }
    scan_param.main_table_scan_stat_.reset_cache_stat();
    scan_param.idx_table_scan_stat_.reset_cache_stat();
    if (OB_FAIL(report_ddl_column_checksum())) {
      LOG_WARN("report checksum failed", K(ret));
    }
  }

  return ret;
}

int ObTableScanOp::calc_expr_int_value(const ObExpr &expr, int64_t &retval, bool &is_null_value)
{
  int ret = OB_SUCCESS;
  is_null_value = false;
  OB_ASSERT(ob_is_int_tc(expr.datum_meta_.type_));
  ObDatum *datum = NULL;
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

OB_INLINE int ObTableScanOp::do_table_scan()
{
  int ret = OB_SUCCESS;
  need_init_before_get_row_ = false;
  lib::CompatModeGuard g(MY_SPEC.is_vt_mapping_ ? lib::Worker::CompatMode::MYSQL : lib::get_compat_mode());
  if (das_ref_.has_task()) {
    //execute with das
    LOG_DEBUG("do table scan with DAS", K(MY_SPEC.ref_table_id_), K(MY_SPEC.table_loc_id_));
    if (OB_FAIL(prepare_pushdown_limit_param())) {
      LOG_WARN("prepare pushdow limit param failed", K(ret));
    } else if (OB_FAIL(das_ref_.execute_all_task())) {
      LOG_WARN("execute all das scan task failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      //prepare to output row
      scan_result_ = das_ref_.begin_result_iter();
      if (OB_FAIL(update_output_tablet_id())) {
        LOG_WARN("update output row pkey failed", K(ret), K(scan_result_.get_tablet_loc()->tablet_id_));
      }
    }
  } else {
    iter_end_ = true;
  }
  return ret;
}

int ObTableScanOp::cherry_pick_range_by_tablet_id(ObDASScanOp *scan_op)
{
  int ret = OB_SUCCESS;
  ObIArray<ObNewRange> &scan_ranges = scan_op->get_scan_param().key_ranges_;
  ObIArray<ObNewRange> &ss_ranges = scan_op->get_scan_param().ss_key_ranges_;
  ObIArray<ObSpatialMBR> &mbr_filters = scan_op->get_scan_param().mbr_filters_;
  const ObIArray<ObNewRange> &input_ranges = MY_INPUT.key_ranges_;
  const ObIArray<ObNewRange> &input_ss_ranges = MY_INPUT.ss_key_ranges_;
  const ObIArray<ObSpatialMBR> &input_filters = MY_INPUT.mbr_filters_;
  ObDASGroupScanOp *batch_op = DAS_GROUP_SCAN_OP(scan_op);
  bool add_all = false;
  bool prune_all = true;
  if (!MY_SPEC.is_vt_mapping_ && OB_UNLIKELY(input_ranges.count() != input_ss_ranges.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ranges and skip scan postfix ranges mismatch", K(ret), K(input_ranges.count()),
                                                             K(input_ss_ranges.count()));
  } else if (ObPartitionLevel::PARTITION_LEVEL_MAX == MY_SPEC.part_level_
      || ObPartitionLevel::PARTITION_LEVEL_ZERO == MY_SPEC.part_level_
      || (input_ranges.count() <= 1)) {
    add_all = true;
  } else if (MY_SPEC.part_range_pos_.count() == 0 ||
            (ObPartitionLevel::PARTITION_LEVEL_TWO == MY_SPEC.part_level_
             && MY_SPEC.subpart_range_pos_.count() == 0)) {
    add_all = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_ranges.count(); ++i) {
    clear_evaluated_flag();
    bool can_prune = false;
    if (!add_all && OB_FAIL(can_prune_by_tablet_id(scan_op->get_tablet_id(), input_ranges.at(i), can_prune))) {
      LOG_WARN("failed to check whether can prune by tablet id", K(ret));
    } else if (add_all || !can_prune) {
      prune_all = false;
      if (OB_FAIL(scan_ranges.push_back(input_ranges.at(i)))) {
        LOG_WARN("store input range to scan param failed", K(ret));
      } else if (OB_FAIL(ss_ranges.push_back(input_ss_ranges.at(i)))) {
        LOG_WARN("store input skip scan range to scan param failed", K(ret));
      } else if (!input_ranges.at(i).is_physical_rowid_range_) {
        //do nothing
      } else if (OB_UNLIKELY(MY_SPEC.get_columns_desc().count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret));
      } else {
        ObIAllocator &range_allocator = (table_rescan_allocator_ != nullptr ?
            *table_rescan_allocator_ : ctx_.get_allocator());
        ObNewRange &scan_range = scan_ranges.at(scan_ranges.count() - 1);
        ObArrayWrap<ObColDesc> rowkey_descs(&MY_SPEC.get_columns_desc().at(0),
                                            MY_SPEC.get_rowkey_cnt());
        if (OB_FAIL(transform_physical_rowid(range_allocator,
                                             scan_op->get_tablet_id(),
                                             rowkey_descs,
                                             scan_range))) {
          LOG_WARN("transform physical rowid for range failed", K(ret), K(scan_range));
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < input_filters.count(); ++i) {
    if (OB_FAIL(mbr_filters.push_back(input_filters.at(i)))) {
      LOG_WARN("store mbr_filters failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && prune_all && !input_ranges.empty()) {
    ObNewRange false_range;
    ObNewRange whole_range;
    false_range.set_false_range();
    false_range.group_idx_ = input_ranges.at(0).group_idx_;
    whole_range.set_whole_range();
    if (OB_FAIL(scan_ranges.push_back(false_range))) {
      LOG_WARN("store false range to scan ranges failed", K(ret));
    } else if (OB_FAIL(ss_ranges.push_back(whole_range))) {
      LOG_WARN("store whole range to skip scan ranges failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("range after pruning", K(input_ranges), K(scan_ranges), K_(tsc_rtdef_.group_size),
              "tablet_id", scan_op->get_tablet_id(),
              K(input_ss_ranges), K(ss_ranges));
  }
  return ret;
}

int ObTableScanOp::can_prune_by_tablet_id(const ObTabletID &tablet_id,
                                          const ObNewRange &scan_range,
                                          bool &can_prune)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObNewRange partition_range;
  ObNewRange subpartition_range;
  ObDASTabletMapper tablet_mapper;
  can_prune = true;
  if (MY_SPEC.is_vt_mapping_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("virtual table is not partition table", K(ret));
  } else if (scan_range.is_physical_rowid_range_) {
    //scan range with physical rowid range does not support pruning range by tablet_id
    can_prune = false;
  } else if (OB_FAIL(DAS_CTX(ctx_).get_das_tablet_mapper(MY_CTDEF.scan_ctdef_.ref_table_id_, tablet_mapper))) {
    LOG_WARN("get das tablet mapper failed", K(ret), K(MY_CTDEF.scan_ctdef_.ref_table_id_));
  } else if (OB_FAIL(construct_partition_range(
              allocator, MY_SPEC.part_type_, MY_SPEC.part_range_pos_,
              scan_range, MY_SPEC.part_expr_, MY_SPEC.part_dep_cols_,
              can_prune, partition_range))) {
    LOG_WARN("failed to construct partition range", K(ret));
  } else if (can_prune && OB_FAIL(construct_partition_range(
              allocator, MY_SPEC.subpart_type_, MY_SPEC.subpart_range_pos_,
              scan_range, MY_SPEC.subpart_expr_, MY_SPEC.subpart_dep_cols_,
              can_prune, subpartition_range))) {
    LOG_WARN("failed to construct subpartition range", K(ret));
  } else if (can_prune) {
    ObSEArray<ObObjectID, 4> partition_ids;
    ObSEArray<ObObjectID, 4> subpartition_ids;
    ObSEArray<ObTabletID, 4> tablet_ids;
    if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(ObPartitionLevel::PARTITION_LEVEL_ONE,
                                                       OB_INVALID_INDEX,
                                                       partition_range,
                                                       tablet_ids,
                                                       partition_ids))) {
      LOG_WARN("failed to get partition ids", K(ret));
    } else if (partition_ids.count() == 0) {
      /*do nothing*/
    } else if (partition_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should have only one partition id", K(partition_ids), K(partition_range), K(ret));
    } else if (ObPartitionLevel::PARTITION_LEVEL_ONE == MY_SPEC.part_level_) {
      if (tablet_ids.at(0) == tablet_id) {
        can_prune = false;
      }
    } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(ObPartitionLevel::PARTITION_LEVEL_TWO,
                                                              partition_ids.at(0),
                                                              subpartition_range,
                                                              tablet_ids,
                                                              subpartition_ids))) {
      LOG_WARN("failed to get subpartition ids", K(subpartition_range), K(ret));
    } else if (subpartition_ids.count() == 0) {
      /*do nothing*/
    } else if (subpartition_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should have only one partition id", K(ret));
    } else if (tablet_ids.at(0) == tablet_id) {
      can_prune = false;
    }
  }
  return ret;
}

int ObTableScanOp::construct_partition_range(ObArenaAllocator &allocator,
                                             const ObPartitionFuncType part_type,
                                             const ObIArray<int64_t> &part_range_pos,
                                             const ObNewRange &scan_range,
                                             const ObExpr *part_expr,
                                             const ExprFixedArray &part_dep_cols,
                                             bool &can_prune,
                                             ObNewRange &part_range)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  if (is_vectorized()) {
    // batch_size_ is needed for batch result expression evaluation.
    batch_info_guard.set_batch_size(1);
    batch_info_guard.set_batch_idx(0);
  }
  if (OB_ISNULL(scan_range.start_key_.get_obj_ptr()) || OB_ISNULL(scan_range.end_key_.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(scan_range.start_key_.get_obj_ptr()),
        K(scan_range.end_key_.get_obj_ptr()), K(ret));
  } else if (OB_UNLIKELY(scan_range.start_key_.is_min_row())
      || OB_UNLIKELY(scan_range.start_key_.is_max_row())
      || OB_UNLIKELY(scan_range.end_key_.is_min_row())
      || OB_UNLIKELY(scan_range.end_key_.is_max_row())) {
    //the range contain min value or max value can not be pruned
    can_prune = false;
  } else if (OB_UNLIKELY(scan_range.start_key_.get_obj_cnt() != scan_range.end_key_.get_obj_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have the same range key count", K(scan_range.start_key_.get_obj_cnt()),
        K(scan_range.end_key_.get_obj_cnt()), K(ret));
  } else if (part_range_pos.count() > 0) {
    int64_t range_key_count = part_range_pos.count();
    ObObj *start_row_key = NULL;
    ObObj *end_row_key = NULL;
    ObObj *function_obj = NULL;
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
          sql::ObExpr *expr = part_dep_cols.at(i);
          sql::ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(start_row_key[i], expr->obj_datum_map_))) {
            LOG_WARN("convert obj to datum failed", K(ret));
          } else if (is_lob_storage(start_row_key[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(start_row_key[i], expr->obj_meta_, expr->obj_datum_map_,
                                                 get_exec_ctx().get_allocator(), datum))) {
            LOG_WARN("adjust lob datum failed", K(ret), K(i),
                     K(start_row_key[i].get_meta()), K(expr->obj_meta_));
          }else {
            expr->set_evaluated_projected(eval_ctx_);
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

int ObTableScanOp::reassign_task_ranges(ObGranuleTaskInfo &info)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.gi_above_ && !iter_end_) {
    if (OB_UNLIKELY(MY_SPEC.get_query_range().is_contain_geo_filters())) {
      MY_INPUT.key_ranges_.reuse();
      MY_INPUT.ss_key_ranges_.reuse();
      MY_INPUT.mbr_filters_.reuse();
    } else if (!MY_INPUT.get_need_extract_query_range()) {
      if (OB_FAIL(MY_INPUT.key_ranges_.assign(info.ranges_)) ||
          OB_FAIL(MY_INPUT.ss_key_ranges_.assign(info.ss_ranges_))) {
        LOG_WARN("assign the range info failed", K(ret), K(info));
      } else if (MY_SPEC.is_vt_mapping_) {
        if (OB_FAIL(vt_result_converter_->convert_key_ranges(MY_INPUT.key_ranges_))) {
          LOG_WARN("convert key ranges failed", K(ret));
        }
      }
    } else {
      // use prepare() to set key ranges if px do not extract query range
      MY_INPUT.key_ranges_.reuse();
      MY_INPUT.ss_key_ranges_.reuse();
      MY_INPUT.mbr_filters_.reuse();
      LOG_DEBUG("do prepare!!!");
    }
  }
  return ret;
}

int ObTableScanOp::get_access_tablet_loc(ObGranuleTaskInfo &info)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.gi_above_) {
    GIPrepareTaskMap *gi_prepare_map = nullptr;
    if (OB_FAIL(ctx_.get_gi_task_map(gi_prepare_map))) {
      LOG_WARN("Failed to get gi task map", K(ret));
    } else if (OB_FAIL(gi_prepare_map->get_refactored(MY_SPEC.id_, info))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("failed to get prepare gi task", K(ret), K(MY_SPEC.id_));
      } else {
        // OB_HASH_NOT_EXIST mean no more task for tsc.
        LOG_DEBUG("no prepared task info, set table scan to end",
                  K(MY_SPEC.id_), K(this), K(lbt()));
        iter_end_ = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(tsc_rtdef_.scan_rtdef_.table_loc_->get_tablet_loc_by_id(info.tablet_loc_->tablet_id_,
                                                                               MY_INPUT.tablet_loc_))) {
      //need use `get_tablet_loc_by_id` to find my px work thread's tablet_loc,
      //because the tablet_loc in SQC maybe shared with other px work thread,
      //the tablet loc maybe modify in das partition retry
      //otherwise it will get a unsafe result modified by other px work thread
      LOG_WARN("get tablet loc by id failed", K(ret), KPC(info.tablet_loc_), KPC(tsc_rtdef_.scan_rtdef_.table_loc_));
    } else {
      LOG_DEBUG("TSC consume a task", K(info), KPC(MY_INPUT.tablet_loc_), K(MY_INPUT.tablet_loc_->loc_meta_));
    }
  }
  return ret;
}

OB_INLINE void ObTableScanOp::fill_table_scan_stat(const ObTableScanStatistic &statistic,
                                                   ObTableScanStat &scan_stat) const
{
  scan_stat.bf_filter_cnt_ += statistic.bf_filter_cnt_;
  scan_stat.bf_access_cnt_ += statistic.bf_access_cnt_;
  scan_stat.fuse_row_cache_hit_cnt_ += statistic.fuse_row_cache_hit_cnt_;
  scan_stat.fuse_row_cache_miss_cnt_ += statistic.fuse_row_cache_miss_cnt_;
  scan_stat.row_cache_hit_cnt_ += statistic.row_cache_hit_cnt_;
  scan_stat.row_cache_miss_cnt_ += statistic.row_cache_miss_cnt_;
}

void ObTableScanOp::set_cache_stat(const ObPlanStat &plan_stat)
{
  const int64_t TRY_USE_CACHE_INTERVAL = 15;
  ObQueryFlag &query_flag = tsc_rtdef_.scan_rtdef_.scan_flag_;
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
  const int64_t fuse_row_cache_access_cnt =
      plan_stat.fuse_row_cache_hit_cnt_ + plan_stat.fuse_row_cache_miss_cnt_;
  if (fuse_row_cache_access_cnt > ObPlanStat::CACHE_ACCESS_THRESHOLD) {
    if (100.0 * static_cast<double>(plan_stat.fuse_row_cache_hit_cnt_) / static_cast<double>(fuse_row_cache_access_cnt) > 5) {
      query_flag.set_use_fuse_row_cache();
    } else {
      query_flag.set_not_use_fuse_row_cache();
    }
  } else {
    query_flag.set_use_fuse_row_cache();
  }
}

bool ObTableScanOp::need_init_checksum()
{
  return MY_SPEC.report_col_checksum_;
}

int ObTableScanOp::init_ddl_column_checksum()
{
  int ret = OB_SUCCESS;
  if (need_init_checksum()) {
    column_checksum_.set_allocator(&ctx_.get_allocator());
    col_need_reshape_.set_allocator(&ctx_.get_allocator());
    const ObSQLSessionInfo *session = nullptr;
    const ObIArray<ObColumnParam *> *cols = MY_CTDEF.scan_ctdef_.table_param_.get_read_info().get_columns();
    if (OB_ISNULL(session = ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid session", K(ret));
    } else if (OB_ISNULL(cols)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col param array is unexpected null", K(ret),KP(cols));
    } else if (MY_SPEC.output_.count() != MY_SPEC.ddl_output_cids_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(MY_SPEC.output_), K(MY_CTDEF.scan_ctdef_.table_param_), K(MY_SPEC.ddl_output_cids_));
    } else if (OB_FAIL(column_checksum_.init(MY_SPEC.ddl_output_cids_.count()))) {
      LOG_WARN("init column checksum array failed", K(ret));
    } else if (OB_FAIL(col_need_reshape_.init(MY_SPEC.ddl_output_cids_.count()))) {
      LOG_WARN("init column need reshape array failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.ddl_output_cids_.count(); ++i) {
        if (OB_FAIL(column_checksum_.push_back(0))) {
          LOG_WARN("push back column checksum failed", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.ddl_output_cids_.count(); ++i) {
        bool found = false;
        bool need_reshape = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < cols->count(); ++j) {
          const ObColumnParam *col_param = cols->at(j);
          if (OB_ISNULL(col_param)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid col param", K(ret));
          } else if (MY_SPEC.ddl_output_cids_.at(i) == col_param->get_column_id()) {
            found = true;
            if (col_param->get_meta_type().is_lob_storage()) {
              need_reshape = true;
            } else if (is_pad_char_to_full_length(session->get_sql_mode())) {
              need_reshape = col_param->get_meta_type().is_fixed_len_char_type();
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!found) {
          // if not found, the column is virtual generated column, in this scene,
          // if is_fixed_len_char_type() is true, need reshape
          uint64_t VIRTUAL_GEN_FIX_LEN_TAG = 1ULL << 63;
          if ((MY_SPEC.ddl_output_cids_.at(i) & VIRTUAL_GEN_FIX_LEN_TAG) >> 63) {
            need_reshape = true;
          } else {
            need_reshape = false;
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(col_need_reshape_.push_back(need_reshape))) {
          LOG_WARN("failed to push back col need reshape", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableScanOp::corrupt_obj(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_E(EventTable::EN_BUILD_GLOBAL_INDEX_WITH_CORRUPTED_DATA) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret && obj.is_fixed_len_char_type()) {
    char *ptr = obj.get_string().ptr();
    int32_t len = obj.get_string_len();
    ptr[len - 1] = ' ';
  }
  return ret;
}

int ObTableScanOp::add_ddl_column_checksum()
{
  int ret = OB_SUCCESS;
  if (report_checksum_) {
    const int64_t cnt = MY_SPEC.output_.count();
    if (OB_UNLIKELY(col_need_reshape_.count() != cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, column cnt mismatch", K(ret), K(cnt), K(col_need_reshape_.count()));
    }
    // convert datanum to obj
    ObDatum store_datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); ++i) {
      ObDatum *datum = NULL;
      const ObExpr *e = MY_SPEC.output_[i];
      if (OB_ISNULL(e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, expr is nullptr", K(ret));
      } else if (OB_FAIL(e->eval(eval_ctx_, datum))) {
        LOG_WARN("evaluate expression failed", K(ret));
      } else if (FALSE_IT(store_datum = *datum)) {
#ifdef ERRSIM
      // TODO@hanhui: fix this errsim later
      // } else if (OB_FAIL(corrupt_obj(store_datum))) {
      //   LOG_WARN("failed to corrupt obj", K(ret));
#endif
      } else if (col_need_reshape_[i] && OB_FAIL(ObDDLUtil::reshape_ddl_column_obj(store_datum, e->obj_meta_))) {
        LOG_WARN("reshape ddl column obj failed", K(ret));
      } else {
        column_checksum_[i] += store_datum.checksum(0);
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("add ddl column checksum",
                K(MY_CTDEF.get_das_output_exprs()),
                K(MY_CTDEF.get_full_acccess_cids()),
          K(MY_SPEC.output_));
    }
    clear_evaluated_flag();
  }
  return ret;
}

int ObTableScanOp::add_ddl_column_checksum_batch(const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (report_checksum_) {
    const int64_t cnt = MY_SPEC.output_.count();
    if (OB_UNLIKELY(col_need_reshape_.count() != cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, column cnt mismatch", K(ret), K(cnt), K(col_need_reshape_.count()));
    }

    ObDatum store_datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); ++i) {
      const ObExpr *e = MY_SPEC.output_[i];
      if (OB_ISNULL(e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, expr is nullptr", K(ret));
      } else if (OB_FAIL(e->eval_batch(eval_ctx_, *brs_.skip_, brs_.size_))) {
        LOG_WARN("evaluate expression failed", K(ret));
      } else {
        ObDatumVector datum_array = e->locate_expr_datumvector(eval_ctx_);
        for (int64_t j = 0; OB_SUCC(ret) && j < row_count; j++) {
          if (brs_.skip_->at(j)) {
            continue;
          } else if (FALSE_IT(store_datum = *datum_array.at(j))) {
#ifdef ERRSIM
          // TODO@hanhui: fix this errsim later
          // } else if (OB_FAIL(corrupt_obj(store_datum))) {
          //   LOG_WARN("failed to corrupt obj", K(ret));
#endif
          } else if (col_need_reshape_[i] && OB_FAIL(ObDDLUtil::reshape_ddl_column_obj(store_datum, e->obj_meta_))) {
            LOG_WARN("reshape ddl column obj failed", K(ret));
          } else {
            column_checksum_[i] += store_datum.checksum(0);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("add ddl column checksum",
                K(MY_CTDEF.get_das_output_exprs()),
                K(MY_CTDEF.get_full_acccess_cids()),
                K(MY_SPEC.output_));
    }
    clear_evaluated_flag();
  }
  return ret;
}

int ObTableScanOp::report_ddl_column_checksum()
{
  int ret = OB_SUCCESS;
  if (report_checksum_) {
    ObArray<ObDDLChecksumItem> checksum_items;
    const int64_t curr_scan_task_id = scan_task_id_++;
    const ObTabletID &tablet_id = MY_INPUT.tablet_loc_->tablet_id_;
    const uint64_t table_id = MY_CTDEF.scan_ctdef_.ref_table_id_;
    uint64_t VIRTUAL_GEN_FIXED_LEN_MASK = ~(1ULL << 63);
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.ddl_output_cids_.count(); ++i) {
      ObDDLChecksumItem item;
      item.execution_id_ = MY_SPEC.plan_->get_ddl_execution_id();
      item.tenant_id_ = MTL_ID();
      item.table_id_ = table_id;
      item.ddl_task_id_ = MY_SPEC.plan_->get_ddl_task_id();
      item.column_id_ = MY_SPEC.ddl_output_cids_.at(i) & VIRTUAL_GEN_FIXED_LEN_MASK;
      item.task_id_ = ctx_.get_px_sqc_id() << ObDDLChecksumItem::PX_SQC_ID_OFFSET | ctx_.get_px_task_id() << ObDDLChecksumItem::PX_TASK_ID_OFFSET | curr_scan_task_id;
      item.checksum_ = i < column_checksum_.count() ? column_checksum_[i] : 0;
    #ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_DATA_CHECKSUM_DDL_TASK) OB_SUCCESS;
        // set the checksum of the second column inconsistent with the report checksum of hidden table. (report_column_checksum(ObSSTable &sstable))
        if (OB_FAIL(ret) && 17 == item.column_id_) {
          item.checksum_ = i;
        }
      }
    #endif
      if (OB_FAIL(checksum_items.push_back(item))) {
        LOG_WARN("fail to push back item", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLChecksumOperator::update_checksum(checksum_items, *GCTX.sql_proxy_))) {
        LOG_WARN("fail to update checksum", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.ddl_output_cids_.count(); ++i) {
          column_checksum_[i] = 0;
        }
      }
    }
  }
  return ret;
}

int ObTableScanOp::transform_physical_rowid(ObIAllocator &allocator,
                                            const ObTabletID &scan_tablet_id,
                                            const ObArrayWrap<ObColDesc> &rowkey_descs,
                                            ObNewRange &new_range)
{
  int ret = OB_SUCCESS;
  bool start_is_phy_rowid = false;
  bool end_is_phy_rowid = false;
  ObURowIDData start_urowid_data;
  ObURowIDData end_urowid_data;
  LOG_TRACE("begin to transform physical rowid", K(new_range));
  if (OB_FAIL(check_is_physical_rowid(allocator,
                                      new_range.start_key_,
                                      start_is_phy_rowid,
                                      start_urowid_data)) ||
      OB_FAIL(check_is_physical_rowid(allocator,
                                      new_range.end_key_,
                                      end_is_phy_rowid,
                                      end_urowid_data))) {
    LOG_WARN("failed to check is physical rowid", K(ret));
  } else if (start_is_phy_rowid || end_is_phy_rowid) {
    bool is_transform_end = false;
    if (start_is_phy_rowid &&
        OB_FAIL(transform_physical_rowid_rowkey(allocator, start_urowid_data, scan_tablet_id,
                                                rowkey_descs, true, new_range, is_transform_end))) {
      LOG_WARN("failed to transform physical rowid rowkey", K(ret));
    } else if (is_transform_end) {
      /*do nothing*/
    } else if (end_is_phy_rowid &&
               OB_FAIL(transform_physical_rowid_rowkey(allocator, end_urowid_data,
                                                       scan_tablet_id, rowkey_descs, false,
                                                       new_range, is_transform_end))) {
      LOG_WARN("failed to transform physical rowid rowkey", K(ret));
    } else {/*do nothing*/}
  } else {/*do nothing*/}
  LOG_TRACE("end to transform physical rowid", K(new_range));
  return ret;
}

int ObTableScanOp::check_is_physical_rowid(ObIAllocator &allocator,
                                           ObRowkey &row_key,
                                           bool &is_physical_rowid,
                                           ObURowIDData &urowid_data)
{
  int ret = OB_SUCCESS;
  is_physical_rowid = false;
  ObObj *obj_buf = NULL;
  if (OB_UNLIKELY(!row_key.is_valid() || row_key.get_obj_cnt() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey", K(ret), K(row_key));
  } else if (row_key.is_min_row() || row_key.is_max_row() || row_key.get_obj_ptr()[0].is_null()) {
    /*do nothing*/
  } else if (OB_ISNULL(row_key.get_obj_ptr()) || OB_UNLIKELY(row_key.get_obj_cnt() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_key.get_obj_ptr()), K(row_key.get_obj_cnt()));
  } else if (OB_UNLIKELY(!ob_is_urowid(row_key.get_obj_ptr()[0].get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got invalid obj type", K(row_key.get_obj_ptr()[0]));
  } else if (row_key.get_obj_ptr()[0].get_urowid().is_physical_rowid()) {
    is_physical_rowid = true;
    urowid_data = row_key.get_obj_ptr()[0].get_urowid();
  //occur logical rowid, just convert min, because the phy rowid is max than logical rowid.
  } else if (OB_ISNULL(obj_buf = (ObObj *)allocator.alloc(sizeof(ObObj) * row_key.get_obj_cnt()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    for (int i = 0; i < row_key.get_obj_cnt(); i++) {
      new (obj_buf + i) ObObj();
      obj_buf[i].set_min_value();
    }
    row_key.assign(obj_buf, row_key.get_obj_cnt());
  }
  return ret;
}

int ObTableScanOp::transform_physical_rowid_rowkey(ObIAllocator &allocator,
                                                   const ObURowIDData &urowid_data,
                                                   const ObTabletID &scan_tablet_id,
                                                   const ObArrayWrap<ObColDesc> &rowkey_descs,
                                                   const bool is_start_key,
                                                   ObNewRange &new_range,
                                                   bool &is_transform_end)
{
  int ret = OB_SUCCESS;
  is_transform_end = false;
  ObTabletID tablet_id;
  ObObj *obj_buf = NULL;
  const int64_t rowkey_cnt = rowkey_descs.count();
  if (OB_FAIL(urowid_data.get_tablet_id_for_heap_organized_table(tablet_id))) {
    LOG_WARN("failed to get tablet id for heap organized table", K(ret));
  } else if (scan_tablet_id == tablet_id) {
    ObSEArray<ObObj, 1> pk_vals;
    if (OB_FAIL(urowid_data.get_rowkey_for_heap_organized_table(pk_vals))) {
      LOG_WARN("failed to get rowkey for heap organized table", K(ret));
    } else if (OB_UNLIKELY(pk_vals.count() != rowkey_cnt)) {
      ret = OB_INVALID_ROWID;
      LOG_WARN("invalid rowid", K(ret), K(pk_vals), K(rowkey_descs));
    } else if (OB_ISNULL(obj_buf = (ObObj *)allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(obj_buf));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
        if (!pk_vals.at(i).meta_.is_null()
            && !ObSQLUtils::is_same_type_for_compare(pk_vals.at(i).meta_,
                                                     rowkey_descs.at(i).col_type_)) {
          ret = OB_INVALID_ROWID;
          LOG_WARN("invalid rowid", K(ret), K(pk_vals.at(i).meta_), K(rowkey_descs.at(i).col_type_));
        } else {
          obj_buf[i] = pk_vals.at(i);
        }
      }
      if (OB_SUCC(ret)) {
        if (is_start_key) {
          new_range.start_key_.assign(obj_buf, rowkey_cnt);
        } else {
          new_range.end_key_.assign(obj_buf, rowkey_cnt);
        }
      }
    }
  } else {
    if (OB_ISNULL(obj_buf = (ObObj *)allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      for (int i = 0; i < rowkey_cnt; i++) {
        new (obj_buf + i) ObObj();
        if (is_start_key) {
          obj_buf[i].set_min_value();
        } else {
          obj_buf[i].set_max_value();
        }
      }
      if (is_start_key) {
        new_range.start_key_.assign(obj_buf, rowkey_cnt);
        if (scan_tablet_id < tablet_id) {
          new_range.end_key_.assign(obj_buf, rowkey_cnt);
          is_transform_end = true;
        }
      } else {
        new_range.end_key_.assign(obj_buf, rowkey_cnt);
        if (scan_tablet_id > tablet_id) {
          new_range.start_key_.assign(obj_buf, rowkey_cnt);
          is_transform_end = true;
        }
      }
    }
  }
  LOG_TRACE("transform physical rowid rowkey", K(tablet_id), K(scan_tablet_id), K(new_range));
  return ret;
}

int ObTableScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MY_SPEC.is_spatial_ddl())) {
    if (OB_FAIL(inner_get_next_spatial_index_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("spatial index ddl : get next spatial index row failed", K(ret));
      }
    }
  } else if (OB_FAIL(inner_get_next_row_implement())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next row failed", K(ret));
    }
  }
  return ret;
}

int ObTableScanOp::inner_get_next_spatial_index_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(spat_index_.spat_rows_)) {
    if (OB_FAIL(init_spatial_index_rows())) {
      LOG_WARN("init spatial row store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (spat_index_.spat_row_index_ >= spat_index_.spat_rows_->count()) {
      if (OB_FAIL(ObTableScanOp::inner_get_next_row_implement())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret), "op", op_name());
        }
      } else {
        spat_index_.spat_rows_->reuse();
        spat_index_.spat_row_index_ = 0;
        const ObExprPtrIArray &exprs = MY_SPEC.output_;
        ObExpr *expr = exprs.at(3);
        ObDatum *in_datum = NULL;
        if (OB_FAIL(expr->eval(eval_ctx_, in_datum))) {
          LOG_WARN("expression evaluate failed", K(ret));
        } else {
          ObString geo_wkb = in_datum->get_string();
          uint32_t srid = UINT32_MAX;
          omt::ObSrsCacheGuard srs_guard;
          const ObSrsItem *srs_item = NULL;
          const ObSrsBoundsItem *srs_bound = NULL;
          ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
          uint64_t tenant_id = my_session->get_effective_tenant_id();
          ObS2Cellids cellids;
          ObString mbr_val(0, static_cast<char *>(spat_index_.mbr_buffer_));

          ObArenaAllocator tmp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *in_datum,
                      expr->datum_meta_, expr->obj_meta_.has_lob_header(), geo_wkb))) {
            LOG_WARN("failed to get real geo data.", K(ret));
          } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(geo_wkb, srid))) {
            LOG_WARN("failed to get srid", K(ret), K(geo_wkb));
          } else if (srid != 0 &&
              OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
            LOG_WARN("failed to get srs guard", K(ret), K(tenant_id), K(srid));
          } else if (srid != 0 &&
              OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
            LOG_WARN("failed to get srs item", K(ret), K(tenant_id), K(srid));
          } else if (((srid == 0) || !(srs_item->is_geographical_srs())) &&
                      OB_FAIL(OTSRS_MGR->get_srs_bounds(srid, srs_item, srs_bound))) {
            LOG_WARN("failed to get srs bound", K(ret), K(srid));
          } else if (OB_FAIL(ObGeoTypeUtil::get_cellid_mbr_from_geom(geo_wkb, srs_item, srs_bound,
                                                                     cellids, mbr_val))) {
            LOG_WARN("failed to get cellid", K(ret));
          } else if (cellids.size() > SAPTIAL_INDEX_DEFAULT_ROW_COUNT) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cellid over size", K(ret), K(cellids.size()));
          } else if (OB_ISNULL(spat_index_.obj_buffer_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory for spatial index row cells", K(ret));
          } else {
            ObObj *obj_arr = reinterpret_cast<ObObj *>(spat_index_.obj_buffer_);
            uint64_t obj_idx = 0;
            for (uint64_t i = 0; OB_SUCC(ret) && i < cellids.size(); i++) {
              obj_arr[obj_idx].set_nop_value();
              obj_arr[obj_idx].set_uint64(cellids.at(i));
              obj_arr[obj_idx + 1].set_nop_value();
              obj_arr[obj_idx + 1].set_varchar(mbr_val);
              obj_arr[obj_idx + 1].set_collation_type(CS_TYPE_BINARY);
              obj_arr[obj_idx + 1].set_collation_level(CS_LEVEL_IMPLICIT);
              ObNewRow row;
              row.cells_ = &obj_arr[obj_idx];
              row.count_ = 2;
              obj_idx += 2;
              if (OB_FAIL(spat_index_.spat_rows_->push_back(row))) {
                LOG_WARN("failed to push back spatial index row", K(ret), K(row));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObNewRow &row = (*(spat_index_.spat_rows_))[spat_index_.spat_row_index_++];
      ObObj &cellid= row.get_cell(0);
      ObObj &mbr = row.get_cell(1);
      if (OB_FAIL(fill_generated_cellid_mbr(cellid, mbr))) {
        LOG_WARN("fill cellid mbr failed", K(ret), K(cellid), K(mbr));
      }
    }
  }
  return ret;
}

int ObTableScanOp::init_spatial_index_rows()
{
  int ret = OB_SUCCESS;
  void *buf = ctx_.get_allocator().alloc(sizeof(ObSpatIndexRow));
  void *mbr_buffer = ctx_.get_allocator().alloc(OB_DEFAULT_MBR_SIZE);
  void *obj_buf = ctx_.get_allocator().alloc(sizeof(ObObj) * 2 * SAPTIAL_INDEX_DEFAULT_ROW_COUNT);
  if (OB_ISNULL(buf) || OB_ISNULL(mbr_buffer) || OB_ISNULL(obj_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate spatial row store failed", K(ret), K(buf), K(mbr_buffer));
  } else {
    spat_index_.spat_rows_ = new(buf) ObSpatIndexRow();
    spat_index_.mbr_buffer_ = mbr_buffer;
    spat_index_.obj_buffer_ = obj_buf;
  }
  return ret;
}

int ObTableScanOp::fill_generated_cellid_mbr(const ObObj &cellid, const ObObj &mbr)
{
  int ret = OB_SUCCESS;
  const ObExprPtrIArray &exprs = MY_SPEC.output_;
  if (exprs.count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid exprs count", K(ret), K(exprs.count()));
  } else {
    for (uint8_t i = 0; i < 2 && OB_SUCC(ret); i++) {
      ObObjDatumMapType type = i == 0 ? OBJ_DATUM_8BYTE_DATA : OBJ_DATUM_STRING;
      const ObObj &value = i == 0 ? cellid : mbr;
      ObExpr *expr = exprs.at(i);
      ObDatum *datum = &expr->locate_datum_for_write(get_eval_ctx());
      ObEvalInfo *eval_info = &expr->get_eval_info(get_eval_ctx());
      if (OB_FAIL(datum->from_obj(value, type))) {
        LOG_WARN("fill spatial index row failed", K(ret));
      } else {
        eval_info->evaluated_ = true;
        eval_info->projected_ = true;
      }
    }
  }
  return ret;
}

ObGlobalIndexLookupOpImpl::ObGlobalIndexLookupOpImpl(ObTableScanOp *table_scan_op)
  : ObIndexLookupOpImpl(GLOBAL_INDEX, 10000 /*default_batch_row_count*/),
    table_scan_op_(table_scan_op),
    das_ref_(table_scan_op_->get_eval_ctx(), table_scan_op_->get_exec_ctx()),
    lookup_result_(),
    lookup_memctx_()
{
}

int ObGlobalIndexLookupOpImpl::open()
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = NULL;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(table_scan_op_->get_exec_ctx());
  if (OB_ISNULL(sql_ctx = table_scan_op_->get_exec_ctx().get_sql_ctx())
      || OB_ISNULL(sql_ctx->schema_guard_)
      || OB_ISNULL(get_calc_part_id_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(sql_ctx), KP(get_calc_part_id_expr()));
  } else {
    ObMemAttr mem_attr;
    mem_attr.tenant_id_ = my_session->get_effective_tenant_id();
    mem_attr.label_ = ObModIds::OB_SQL_TABLE_LOOKUP;
    das_ref_.set_mem_attr(mem_attr);
    das_ref_.set_expr_frame_info(&table_scan_op_->get_spec().plan_->get_expr_frame_info());
  }
  if (OB_SUCC(ret)) {
    table_scan_op_->das_ref_.set_lookup_iter(&lookup_result_);
  }
  if (OB_SUCC(ret) && OB_ISNULL(lookup_memctx_)) {
    lib::ContextParam param;
    param.set_mem_attr(my_session->get_effective_tenant_id(),
                      ObModIds::OB_SQL_TABLE_LOOKUP, ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(lookup_memctx_, param))) {
      LOG_WARN("create lookup mem context entity failed", K(ret));
    } else {
      table_scan_op_->tsc_rtdef_.lookup_rtdef_->scan_allocator_.set_alloc(&lookup_memctx_->get_arena_allocator());
      table_scan_op_->tsc_rtdef_.lookup_rtdef_->stmt_allocator_.set_alloc(&lookup_memctx_->get_arena_allocator());
    }
  }
  LOG_DEBUG("open table lookup", K(table_scan_op_->get_spec()));
  return ret;
}

int ObGlobalIndexLookupOpImpl::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::rescan()
{
  int ret = OB_SUCCESS;
  if (get_batch_rescan() && lookup_group_cnt_ < index_group_cnt_) {
    LOG_DEBUG("rescan in group lookup, only need to switch iterator",
              K(lookup_group_cnt_), K(index_group_cnt_));
    if (OB_FAIL(table_scan_op_->build_bnlj_params())) {
      LOG_WARN("build batch nlj params failed", K(ret));
    } else if (OB_FAIL(switch_lookup_result_iter())) {
      LOG_WARN("switch lookup result iter failed", K(ret));
    }
  } else if (OB_FAIL(das_ref_.close_all_task())) {
    LOG_WARN("failed to close all das task", K(ret));
  } else if (OB_FAIL(table_scan_op_->inner_rescan_for_tsc())) {
    LOG_WARN("rescan operator failed", K(ret));
  } else {
    reset_for_rescan();
  }
#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif
  return ret;
}

int ObGlobalIndexLookupOpImpl::get_next_row_from_index_table()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  do {
    if (OB_FAIL(table_scan_op_->inner_get_next_row_for_tsc())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child failed", K(ret));
      } else if (get_batch_rescan()) {
        //switch to next index iterator, call child's rescan
        if (OB_FAIL(table_scan_op_->inner_rescan_for_tsc())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("rescan index operator failed", K(ret));
          }
        } else {
          inc_index_group_cnt();
          LOG_DEBUG("switch to next index batch to fetch rowkey", K(get_index_group_cnt()), K(lookup_rowkey_cnt_));
        }
      }
    }  else {
      got_row = true;
    }
  } while (OB_SUCC(ret)&& !got_row) ;
  return ret;
}

int ObGlobalIndexLookupOpImpl::process_data_table_rowkey()
{
  int ret = OB_SUCCESS;
  ObObjectID partition_id = ObExprCalcPartitionId::NONE_PARTITION_ID;
  ObTabletID tablet_id;
  ObDASScanOp *das_scan_op = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;

  ObDASScanRtDef *lookup_rtdef = table_scan_op_->tsc_rtdef_.lookup_rtdef_;
  ObDASCtx &das_ctx = DAS_CTX(table_scan_op_->get_exec_ctx());
  if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(get_calc_part_id_expr(), table_scan_op_->get_eval_ctx(), partition_id, tablet_id))) {
    LOG_WARN("fail to calc part id", K(ret), KPC(get_calc_part_id_expr()));
  } else if (OB_FAIL(das_ctx.extended_tablet_loc(*lookup_rtdef->table_loc_, tablet_id, tablet_loc))) {
    LOG_WARN("pkey to tablet loc failed", K(ret));
  } else if (OB_UNLIKELY(!has_das_scan_op(tablet_loc, das_scan_op))) {
    ObDASOpType op_type = get_batch_rescan() ? DAS_OP_TABLE_BATCH_SCAN : DAS_OP_TABLE_SCAN;
    ObIDASTaskOp *tmp_op = nullptr;
    if (OB_FAIL(das_ref_.create_das_task(tablet_loc, op_type, tmp_op))) {
      LOG_WARN("prepare das task failed", K(ret));
    } else {
      das_scan_op = static_cast<ObDASScanOp*>(tmp_op);
      das_scan_op->set_scan_ctdef(get_lookup_ctdef());
      das_scan_op->set_scan_rtdef(lookup_rtdef);
      das_scan_op->set_can_part_retry(table_scan_op_->can_partition_retry());
    }
  }
  if (OB_SUCC(ret)) {
    storage::ObTableScanParam &scan_param = das_scan_op->get_scan_param();
    ObNewRange lookup_range;
    if (OB_FAIL(build_data_table_range(lookup_range))) {
      LOG_WARN("build data table range failed", K(ret), KPC(tablet_loc));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("store lookup key range failed", K(ret), K(scan_param));
    } else {
      scan_param.is_get_ = true;
    }
  }

  if (OB_SUCC(ret) && get_lookup_ctdef()->trans_info_expr_ != nullptr) {
    void *buf = nullptr;
    ObDatum *datum_ptr = nullptr;
    if (OB_FAIL(build_trans_datum(get_lookup_ctdef()->trans_info_expr_,
                                  &(table_scan_op_->get_eval_ctx()),
                                  lookup_memctx_->get_arena_allocator(),
                                  datum_ptr))) {

    } else if (OB_ISNULL(datum_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(das_scan_op->trans_info_array_.push_back(datum_ptr))) {
      LOG_WARN("fail to push back trans info array", K(ret), KPC(datum_ptr));
    }
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::process_data_table_rowkeys(const int64_t size, const ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(table_scan_op_->get_eval_ctx());
  batch_info_guard.set_batch_size(size);
  for (auto i = 0; OB_SUCC(ret) && i < size; i++)
  {
    if (skip->at(i)) {
      continue;
    }
    batch_info_guard.set_batch_idx(i);
    if (OB_FAIL(process_data_table_rowkey())) {
      LOG_WARN("Failed to process_data_table_rowkey", K(ret), K(i));
    } else {
      ++lookup_rowkey_cnt_;
    }
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::init_group_range(int64_t cur_group_idx, int64_t group_size)
{
  int ret = OB_SUCCESS;
  if (get_batch_rescan()) {
    for (DASTaskIter task_iter = das_ref_.begin_task_iter(); !task_iter.is_end(); ++task_iter) {
      ObDASGroupScanOp *group_op = static_cast<ObDASGroupScanOp*>(*task_iter);
      group_op->init_group_range(cur_group_idx, group_size);
      LOG_DEBUG("set group info",
                "scan_range", group_op->get_scan_param().key_ranges_,
                K(*group_op));
    }
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::do_index_lookup()
{
  int ret = das_ref_.execute_all_task();
  if (OB_SUCC(ret)) {
    lookup_result_ = das_ref_.begin_result_iter();
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::get_next_row_from_data_table()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  if (OB_UNLIKELY(lookup_result_.is_end())) {
    ret = OB_ITER_END;
    LOG_DEBUG("lookup task is empty", K(ret));
  }
  ObDASScanRtDef *lookup_rtdef = table_scan_op_->tsc_rtdef_.lookup_rtdef_;
  while (OB_SUCC(ret) && !got_row) {
    lookup_rtdef->p_pd_expr_op_->clear_datum_eval_flag();
    if (OB_FAIL(lookup_result_.get_next_row())) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(lookup_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task result failed", K(ret));
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else {
      got_row = true;
    }
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::get_next_rows_from_data_table(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  UNUSED(count);
  int64_t batch_size = min(capacity, table_scan_op_->get_tsc_spec().max_batch_size_);
  if (OB_FAIL(get_next_data_table_rows(brs_.size_, batch_size))) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("get next data table rows return ITER_END", K(ret));
    } else {
      LOG_WARN("look up get next row failed", K(ret));
    }
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::process_next_index_batch_for_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_lookup_row_cnt())) {
    LOG_WARN("check lookup row cnt failed", K(ret));
  } else if (need_next_index_batch()) {
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all das task failed", K(ret));
    } else {
      state_ = INDEX_SCAN;
      das_ref_.reuse();
      index_end_ = false;
      if (OB_SUCC(ret) && lookup_memctx_ != nullptr) {
        lookup_memctx_->reset_remain_one_page();
      }
    }
  } else {
    state_ = FINISHED;
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::process_next_index_batch_for_rows(int64_t &count)
{
  int ret = OB_SUCCESS;
  UNUSED(count);
  if (OB_FAIL(check_lookup_row_cnt())) {
    LOG_WARN("check lookup row cnt failed", K(ret));
  } else if (need_next_index_batch()) { // index search does not reach end, continue index scan
    state_ = INDEX_SCAN;
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all das task failed", K(ret));
    } else {
      das_ref_.reuse();
      if (OB_SUCC(ret) && lookup_memctx_ != nullptr) {
        lookup_memctx_->reset_remain_one_page();
      }
    }
  } else {
    state_ = FINISHED;
  }
  return ret;
}

bool ObGlobalIndexLookupOpImpl::need_next_index_batch() const
{
  bool bret = false;
  if (!get_batch_rescan()) {
    bret = !index_end_;
  } else if (lookup_group_cnt_ >= index_group_cnt_) {
    bret = !index_end_;
  }
  return bret;
}

int ObGlobalIndexLookupOpImpl::check_lookup_row_cnt()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(table_scan_op_->get_exec_ctx());
  if (GCONF.enable_defensive_check()
      && get_lookup_ctdef()->pd_expr_spec_.pushdown_filters_.empty()) {
    if (OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)
        && index_group_cnt_ == lookup_group_cnt_) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObString func_name = ObString::make_string("check_lookup_row_cnt");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      LOG_ERROR("Fatal Error!!! Catch a defensive error!",
                K(ret), K_(lookup_rowkey_cnt), K_(lookup_row_cnt),
                "index_group_cnt", get_index_group_cnt(),
                "lookup_group_cnt", get_lookup_group_cnt(),
                "index_table_id", table_scan_op_->get_tsc_spec().get_ref_table_id(),
                K(DAS_CTX(table_scan_op_->get_exec_ctx()).get_snapshot()),
                KPC(my_session->get_tx_desc()));
      //now to dump lookup das task info
      int64_t rownum = 0;
      for (DASTaskIter task_iter = das_ref_.begin_task_iter(); !task_iter.is_end(); ++task_iter) {
        ObDASScanOp *das_op = static_cast<ObDASScanOp*>(*task_iter);
        if (das_op->trans_info_array_.count() == das_op->get_scan_param().key_ranges_.count()) {
          for (int64_t i = 0; i < das_op->trans_info_array_.count(); i++) {
            rownum++;
            ObDatum *datum = das_op->trans_info_array_.at(i);
            LOG_ERROR("dump TableLookup DAS Task range and trans_info",
                K(rownum), KPC(datum),
                K(das_op->get_scan_param().key_ranges_.at(i)),
                K(das_op->get_tablet_id()));
          }
        } else {
          for (int64_t i = 0; i < das_op->get_scan_param().key_ranges_.count(); i++) {
            rownum++;
            LOG_ERROR("dump TableLookup DAS Task range",
                K(rownum),
                K(das_op->get_scan_param().key_ranges_.at(i)),
                K(das_op->get_tablet_id()));
          }
        }
      }
    }
  }

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_DUMP_WRITE_BUFFER);
  if (0 != simulate_error) {
    for (DASTaskIter task_iter = das_ref_.begin_task_iter(); !task_iter.is_end(); ++task_iter) {
      ObDASScanOp *das_op = static_cast<ObDASScanOp*>(*task_iter);
      for (int64_t i = 0; i < das_op->trans_info_array_.count(); i++) {
        ObDatum *datum = das_op->trans_info_array_.at(i);
        LOG_INFO("dump TableLookup DAS Task trans info", K(i),
            KPC(das_op->trans_info_array_.at(i)),
            K(das_op->get_scan_param().key_ranges_.at(i)),
            K(das_op->get_tablet_id()));
      }
    }
  }

  return ret;
}

int ObGlobalIndexLookupOpImpl::do_index_table_scan_for_rows(const int64_t max_row_cnt,
                                                        const int64_t start_group_idx,
                                                        const int64_t default_row_batch_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows* child_brs = &table_scan_op_->get_brs();
  int64_t batch_size = common::min(max_row_cnt, table_scan_op_->get_tsc_spec().max_batch_size_);
  while (OB_SUCC(ret) && lookup_rowkey_cnt_ < default_row_batch_cnt) {
    if (OB_NOT_NULL(child_brs->skip_)) {
      child_brs->skip_->reset(table_scan_op_->get_spec().max_batch_size_ > 0? table_scan_op_->get_spec().max_batch_size_ : 1);
    }
    int64_t rowkey_batch_size = min(batch_size, default_row_batch_cnt - lookup_rowkey_cnt_);
    if (OB_FAIL(table_scan_op_->inner_get_next_batch_for_tsc(rowkey_batch_size))) {
      LOG_WARN("get next row from child failed", K(ret));
    } else if (child_brs->size_ == 0 && child_brs->end_) {
      if (get_batch_rescan()) {
        if (OB_FAIL(table_scan_op_->inner_rescan_for_tsc())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("rescan index operator failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            index_end_ = true;
            break;
          }
        } else {
          inc_index_group_cnt();
          LOG_DEBUG("switch to next index batch to fetch rowkey", K(index_group_cnt_), K(lookup_rowkey_cnt_));
        }
      } else {
        // index scan is finished, go to lookup stage
        index_end_ = true;
        break;
      }
    } else {
      // critical path: no child_brs sanity check
      index_end_ = (true == child_brs->end_);
      table_scan_op_->clear_evaluated_flag();
      if (OB_FAIL(process_data_table_rowkeys(child_brs->size_, child_brs->skip_))) {
        LOG_WARN("process data table rowkey with das failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    state_ = DO_LOOKUP;
    OZ(init_group_range(start_group_idx, index_group_cnt_));
  }
  LOG_DEBUG("index scan end", KPC(child_brs), K(index_end_), K(index_group_cnt_), K(lookup_rowkey_cnt_), K(lookup_group_cnt_), K(lookup_row_cnt_));
  return ret;
}

void ObGlobalIndexLookupOpImpl::update_state_in_output_rows_state(int64_t &count)
{
  UNUSED(count);
  brs_.end_ = false;
}

void ObGlobalIndexLookupOpImpl::update_states_in_finish_state()
{
  brs_.end_ = true;
}

void ObGlobalIndexLookupOpImpl::reset_for_rescan()
{
  if (lookup_memctx_ != nullptr) {
    lookup_memctx_->reset_remain_one_page();
  }
  state_ = INDEX_SCAN;
  das_ref_.reuse();
  index_end_ = false;
  index_group_cnt_ = 1;
  lookup_group_cnt_ = 1;
}

int ObGlobalIndexLookupOpImpl::build_data_table_range(ObNewRange &lookup_range)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = table_scan_op_->get_tsc_ctdef().global_index_rowkey_exprs_.count();
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = lookup_memctx_->get_arena_allocator().alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new(buf) ObObj[rowkey_cnt];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObObj tmp_obj;
    ObExpr *expr = table_scan_op_->get_tsc_ctdef().global_index_rowkey_exprs_.at(i);
    ObDatum &col_datum = expr->locate_expr_datum(table_scan_op_->get_eval_ctx());
    if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(lookup_memctx_->get_arena_allocator(), tmp_obj, obj_ptr[i]))) {
      LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
    }
  }
  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    uint64_t ref_table_id = get_lookup_ctdef()->ref_table_id_;
    if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
    } else {
      lookup_range.group_idx_ = index_group_cnt_ - 1;
    }
    LOG_DEBUG("build data table range", K(ret), K(table_rowkey), K(lookup_range));
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::switch_lookup_result_iter()
{
  int ret = OB_SUCCESS;
  for (DASTaskIter task_iter = das_ref_.begin_task_iter();
      OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
    ObDASGroupScanOp *batch_op = static_cast<ObDASGroupScanOp*>(*task_iter);
    if (OB_FAIL(batch_op->switch_scan_group())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch batch iter failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ++lookup_group_cnt_;
    state_ = OUTPUT_ROWS;
    lookup_result_ = das_ref_.begin_result_iter();
  }
  return ret;
}

bool ObGlobalIndexLookupOpImpl::has_das_scan_op(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_op)
{
  if (get_batch_rescan()) {
    das_op = static_cast<ObDASScanOp*>(
        das_ref_.find_das_task(tablet_loc, DAS_OP_TABLE_BATCH_SCAN));
  } else {
    das_op = static_cast<ObDASScanOp*>(
        das_ref_.find_das_task(tablet_loc, DAS_OP_TABLE_SCAN));
  }
  return das_op != nullptr;
}

int ObGlobalIndexLookupOpImpl::get_next_data_table_rows(int64_t &count,int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_rows = false;
  if (OB_UNLIKELY(lookup_result_.is_end())) {
    ret = OB_ITER_END;
    LOG_DEBUG("lookup task is empty", K(ret));
  }
  while (OB_SUCC(ret) && !got_rows) {
    table_scan_op_->clear_evaluated_flag();
    ret = lookup_result_.get_next_rows(count, capacity);
    if (OB_ITER_END == ret && count > 0) {
      got_rows = true;
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(lookup_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task result failed", K(ret));
          } else {
            // do nothing, just return OB_ITER_END to notify the caller das scan
            // reach end
            LOG_DEBUG("das_ref_ reach end, stop lookup table");
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else if (count == 0) {
      if (OB_FAIL(lookup_result_.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          } else {
            // do nothing, just return OB_ITER_END to notify the caller das scan
            // reach end
            LOG_DEBUG("das_ref_ reach end, stop lookup table");
          }
      }
    } else {
      got_rows = true;
    }
  }
  if (OB_SUCC(ret) && got_rows) {
    lookup_row_cnt_ += count;
  }
  return ret;
}

int ObGlobalIndexLookupOpImpl::reset_brs()
{
  int ret = OB_SUCCESS;
  brs_.size_ = 0;
  brs_.end_ = false;
  return ret;
}

void ObGlobalIndexLookupOpImpl::destroy()
{
  state_ = FINISHED;
  index_end_ = true;
  das_ref_.reset();
  if (lookup_memctx_ != nullptr) {
    lookup_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(lookup_memctx_);
    lookup_memctx_ = nullptr;
  }
}

} // end namespace sql
} // end namespace oceanbase
