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
#include "sql/executor/ob_task_spliter.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/ob_ddl_checksum.h"
#include "observer/omt/ob_tenant_srs.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/das/iter/ob_das_iter_utils.h"
#include "share/index_usage/ob_index_usage_info_mgr.h"

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
    ret = OB_ERR_FLASHBACK_QUERY_EXP_NULL;
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
  bool is_new_query_range = scan_flags_.is_new_query_range();
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
   // abandoned fields, please remove me at next barrier version
  bool abandoned_always_false_aux_lookup = false;
  bool abandoned_always_false_text_ir = false;
  OB_UNIS_ENCODE(abandoned_always_false_aux_lookup);
  OB_UNIS_ENCODE(abandoned_always_false_text_ir);
  OB_UNIS_ENCODE(attach_spec_);
  OB_UNIS_ENCODE(flags_);
  if (is_new_query_range) {
    OB_UNIS_ENCODE(pre_range_graph_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableScanCtDef)
{
  int64_t len = 0;
  bool has_lookup = (lookup_ctdef_ != nullptr);
  bool is_new_query_range = scan_flags_.is_new_query_range();
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
   // abandoned fields, please remove me at next barrier version
  bool abandoned_always_false_aux_lookup = false;
  bool abandoned_always_false_text_ir = false;
  OB_UNIS_ADD_LEN(abandoned_always_false_aux_lookup);
  OB_UNIS_ADD_LEN(abandoned_always_false_text_ir);
  OB_UNIS_ADD_LEN(attach_spec_);
  OB_UNIS_ADD_LEN(flags_);
  if (is_new_query_range) {
    OB_UNIS_ADD_LEN(pre_range_graph_);
  }
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
  // abandoned fields, please remove me at next barrier version
  bool abandoned_always_false_aux_lookup = false;
  bool abandoned_always_false_text_ir = false;
  OB_UNIS_DECODE(abandoned_always_false_aux_lookup);
  OB_UNIS_DECODE(abandoned_always_false_text_ir);
  OB_UNIS_DECODE(attach_spec_);
  OB_UNIS_DECODE(flags_);
  bool is_new_query_range = scan_flags_.is_new_query_range();
  if (is_new_query_range) {
    OB_UNIS_DECODE(pre_range_graph_);
  }
  return ret;
}

ObDASScanCtDef *ObTableScanCtDef::get_lookup_ctdef()
{
  ObDASScanCtDef *lookup_ctdef = nullptr;
  const ObDASBaseCtDef *attach_ctdef = attach_spec_.attach_ctdef_;
  if (nullptr == attach_ctdef) {
    lookup_ctdef = lookup_ctdef_;
  } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (OB_NOT_NULL(lookup_ctdef_)) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[0]);
    }
  } else if (DAS_OP_VID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (OB_NOT_NULL(lookup_ctdef_)) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[0]);
    }
  } else if (DAS_OP_DOMAIN_ID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(attach_ctdef->children_ != nullptr);
    if (OB_NOT_NULL(lookup_ctdef_)) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[0]);
    }
  } else if (DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (DAS_OP_TABLE_SCAN == attach_ctdef->children_[1]->op_type_) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[1]);
    } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASDocIdMergeCtDef *doc_id_merge_ctdef = static_cast<ObDASDocIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == doc_id_merge_ctdef->children_cnt_ && doc_id_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(doc_id_merge_ctdef->children_[0]);
    } else if (DAS_OP_VID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASVIdMergeCtDef *vid_merge_ctdef = static_cast<ObDASVIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == vid_merge_ctdef->children_cnt_ && vid_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(vid_merge_ctdef->children_[0]);
    } else if (DAS_OP_DOMAIN_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASDomainIdMergeCtDef *domain_id_merge_ctdef = static_cast<ObDASDomainIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(domain_id_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(domain_id_merge_ctdef->children_[0]);
    }
  } else if (DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (DAS_OP_FUNC_LOOKUP == attach_ctdef->children_[1]->op_type_) {
      ObDASFuncLookupCtDef *func_lookup_ctdef = static_cast<ObDASFuncLookupCtDef *>(attach_ctdef->children_[1]);
      if (func_lookup_ctdef->has_main_table_lookup()) {
        const int64_t lookup_child_idx = func_lookup_ctdef->get_main_lookup_scan_idx();
        lookup_ctdef = static_cast<ObDASScanCtDef *>(func_lookup_ctdef->children_[lookup_child_idx]);
      }
    } else if (DAS_OP_TABLE_SCAN == attach_ctdef->children_[1]->op_type_) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[1]);
    } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASDocIdMergeCtDef *doc_id_merge_ctdef = static_cast<ObDASDocIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == doc_id_merge_ctdef->children_cnt_ && doc_id_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(doc_id_merge_ctdef->children_[0]);
    } else if (DAS_OP_VID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASVIdMergeCtDef *vid_merge_ctdef = static_cast<ObDASVIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == vid_merge_ctdef->children_cnt_ && vid_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(vid_merge_ctdef->children_[0]);
    }
  }
  return lookup_ctdef;
}

const ObDASScanCtDef *ObTableScanCtDef::get_lookup_ctdef() const
{
  ObDASScanCtDef *lookup_ctdef = nullptr;
  const ObDASBaseCtDef *attach_ctdef = attach_spec_.attach_ctdef_;
  if (nullptr == attach_ctdef) {
    lookup_ctdef = lookup_ctdef_;
  } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (OB_NOT_NULL(lookup_ctdef_)) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[0]);
    }
  } else if (DAS_OP_VID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (OB_NOT_NULL(lookup_ctdef_)) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[0]);
    }
  } else if (DAS_OP_DOMAIN_ID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(attach_ctdef->children_ != nullptr);
    if (OB_NOT_NULL(lookup_ctdef_)) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[0]);
    }
  } else if (DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (DAS_OP_TABLE_SCAN == attach_ctdef->children_[1]->op_type_) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[1]);
    } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASDocIdMergeCtDef *doc_id_merge_ctdef = static_cast<ObDASDocIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == doc_id_merge_ctdef->children_cnt_ && doc_id_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(doc_id_merge_ctdef->children_[0]);
    } else if (DAS_OP_VID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASVIdMergeCtDef *vid_merge_ctdef = static_cast<ObDASVIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == vid_merge_ctdef->children_cnt_ && vid_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(vid_merge_ctdef->children_[0]);
    } else if (DAS_OP_DOMAIN_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASDomainIdMergeCtDef *domain_id_merge_ctdef = static_cast<ObDASDomainIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(domain_id_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(domain_id_merge_ctdef->children_[0]);
    }
  } else if (DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    if (DAS_OP_FUNC_LOOKUP == attach_ctdef->children_[1]->op_type_) {
      ObDASFuncLookupCtDef *func_lookup_ctdef = static_cast<ObDASFuncLookupCtDef *>(attach_ctdef->children_[1]);
      if (func_lookup_ctdef->has_main_table_lookup()) {
        const int64_t lookup_child_idx = func_lookup_ctdef->get_main_lookup_scan_idx();
        lookup_ctdef = static_cast<ObDASScanCtDef *>(func_lookup_ctdef->children_[lookup_child_idx]);
      }
    } else if (DAS_OP_TABLE_SCAN == attach_ctdef->children_[1]->op_type_) {
      lookup_ctdef = static_cast<ObDASScanCtDef*>(attach_ctdef->children_[1]);
    } else if (DAS_OP_DOC_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASDocIdMergeCtDef *doc_id_merge_ctdef = static_cast<ObDASDocIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == doc_id_merge_ctdef->children_cnt_ && doc_id_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(doc_id_merge_ctdef->children_[0]);
    } else if (DAS_OP_VID_MERGE == attach_ctdef->children_[1]->op_type_) {
      ObDASVIdMergeCtDef *vid_merge_ctdef = static_cast<ObDASVIdMergeCtDef *>(attach_ctdef->children_[1]);
      OB_ASSERT(2 == vid_merge_ctdef->children_cnt_ && vid_merge_ctdef->children_ != nullptr);
      lookup_ctdef = static_cast<ObDASScanCtDef*>(vid_merge_ctdef->children_[0]);
    }
  }
  return lookup_ctdef;
}

ObDASScanCtDef *ObTableScanCtDef::get_rowkey_doc_ctdef()
{
  ObDASScanCtDef *rowkey_doc_ctdef = nullptr;
  const ObDASBaseCtDef *attach_ctdef = attach_spec_.attach_ctdef_;
  if (OB_NOT_NULL(attach_ctdef)) {
    /**
     * The iter tree of das scan with doc id:
     *
     * CASE 1: Partition Scan Tree                        CASE 2: Index LoopUp Tree
     *
     *                DOC_ID_MERGE_ITER                              DAS_INDEX_LOOKUP_ITER
     *                 /              \                               /                \
     *               /                  \                            /                  \
     * DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyDoc)  DAS_SCAN_ITER(IndexTable) DOC_ID_MERGE_ITER
     *                                                                                /          \
     *                                                                              /             \
     *                                                             DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyDoc)
     **/
    if (DAS_OP_DOC_ID_MERGE == attach_ctdef->op_type_) {
      OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
      rowkey_doc_ctdef = static_cast<ObDASScanCtDef *>(attach_ctdef->children_[1]);
    } else if (DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_ || DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef->op_type_) {
      OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
      if (DAS_OP_DOC_ID_MERGE == attach_ctdef->children_[1]->op_type_) {
        ObDASDocIdMergeCtDef *doc_id_merge_ctdef = static_cast<ObDASDocIdMergeCtDef *>(attach_ctdef->children_[1]);
        OB_ASSERT(2 == doc_id_merge_ctdef->children_cnt_ && doc_id_merge_ctdef->children_ != nullptr);
        rowkey_doc_ctdef = static_cast<ObDASScanCtDef *>(doc_id_merge_ctdef->children_[1]);
      }
    }
  }
  return rowkey_doc_ctdef;
}

ObDASScanCtDef *ObTableScanCtDef::get_rowkey_vid_ctdef()
{
  ObDASScanCtDef *rowkey_vid_ctdef = nullptr;
  const ObDASBaseCtDef *attach_ctdef = attach_spec_.attach_ctdef_;
  if (OB_NOT_NULL(attach_ctdef)) {
    /**
     * The iter tree of das scan with vec id:
     *
     * CASE 1: Partition Scan Tree                        CASE 2: Index LoopUp Tree
     *
     *                DOC_ID_MERGE_ITER                              DAS_INDEX_LOOKUP_ITER
     *                 /              \                               /                \
     *               /                  \                            /                  \
     * DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyVid)  DAS_SCAN_ITER(IndexTable) DOC_ID_MERGE_ITER
     *                                                                                /          \
     *                                                                              /             \
     *                                                             DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyVid)
     **/
    if (DAS_OP_VID_MERGE == attach_ctdef->op_type_) {
      OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
      rowkey_vid_ctdef = static_cast<ObDASScanCtDef *>(attach_ctdef->children_[1]);
    } else if (DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_ || DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef->op_type_) {
      OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
      if (DAS_OP_VID_MERGE == attach_ctdef->children_[1]->op_type_) {
        ObDASVIdMergeCtDef *vid_merge_ctdef = static_cast<ObDASVIdMergeCtDef *>(attach_ctdef->children_[1]);
        OB_ASSERT(2 == vid_merge_ctdef->children_cnt_ && vid_merge_ctdef->children_ != nullptr);
        rowkey_vid_ctdef = static_cast<ObDASScanCtDef *>(vid_merge_ctdef->children_[1]);
      }
    }
  }
  return rowkey_vid_ctdef;
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
    ObMemAttr attr(my_session->get_effective_tenant_id(),
                       "TableRescanCtx", ObCtxIds::DEFAULT_CTX_ID);
    param.set_mem_attr(SET_IGNORE_MEM_VERSION(attr))
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
    partition_id_calc_type_(0),
    parser_name_(),
    parser_properties_(),
    est_cost_simple_info_()
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
                    partition_id_calc_type_,
                    parser_name_,
                    parser_properties_);

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
       K_(tenant_id_col_idx),
       K_(parser_name),
       K_(parser_properties));
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
    domain_index_(),
    fts_index_(),
    output_   (nullptr),
    fold_iter_(nullptr),
    iter_tree_(nullptr),
    scan_iter_(nullptr),
    group_rescan_cnt_(0),
    group_id_(0),
    das_tasks_key_(),
    tsc_monitor_info_()
{
}

ObTableScanOp::~ObTableScanOp()
{
}

OB_INLINE int ObTableScanOp::create_one_das_task(ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  ObDASScanOp *scan_op = nullptr;
  bool reuse_das_op = false;
  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(scan_iter_->create_das_task(tablet_loc, scan_op, reuse_das_op))) {
    LOG_WARN("prepare das task failed", K(ret));
  } else if (!reuse_das_op) {
    scan_op->set_scan_ctdef(&MY_CTDEF.scan_ctdef_);
    scan_op->set_scan_rtdef(&tsc_rtdef_.scan_rtdef_);
    scan_op->set_can_part_retry(nullptr == tsc_rtdef_.scan_rtdef_.sample_info_
                                && can_partition_retry());
    scan_op->set_inner_rescan(in_rescan_);
    tsc_rtdef_.scan_rtdef_.table_loc_->is_reading_ = true;
    if (!MY_SPEC.is_index_global_ && MY_CTDEF.lookup_ctdef_ != nullptr) {
      if (OB_FAIL(pushdown_normal_lookup_to_das(*scan_op))) {
        LOG_WARN("pushdown normal lookup to das failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && MY_CTDEF.attach_spec_.attach_ctdef_ != nullptr) {
      if (OB_FAIL(pushdown_attach_task_to_das(*scan_op))) {
        LOG_WARN("pushdown attach task to das failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cherry_pick_range_by_tablet_id(scan_op))) {
      LOG_WARN("prune query range by partition id failed", K(ret), KPC(tablet_loc));
    } else if (OB_NOT_NULL(DAS_GROUP_SCAN_OP(scan_op))) {
      static_cast<ObDASGroupScanOp*>(scan_op)->init_group_range(0, tsc_rtdef_.group_size_);
    }
  }
  return ret;
}

int ObTableScanOp::pushdown_normal_lookup_to_das(ObDASScanOp &target_op)
{
  int ret = OB_SUCCESS;
  //is local index lookup, need to set the lookup ctdef to the das scan op
  ObDASTableLoc *lookup_table_loc = tsc_rtdef_.lookup_rtdef_->table_loc_;
  ObDASTabletLoc *lookup_tablet_loc = ObDASUtils::get_related_tablet_loc(
      *target_op.get_tablet_loc(), lookup_table_loc->loc_meta_->ref_table_id_);
  if (OB_ISNULL(lookup_tablet_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lookup tablet loc is nullptr", K(ret), KPC(target_op.get_tablet_loc()), KPC(lookup_table_loc->loc_meta_));
  } else if (OB_FAIL(target_op.reserve_related_buffer(1))) {
    LOG_WARN("reserve related buffer failed", K(ret));
  } else if (OB_FAIL(target_op.set_related_task_info(MY_CTDEF.lookup_ctdef_,
                                                    tsc_rtdef_.lookup_rtdef_,
                                                    lookup_tablet_loc->tablet_id_))) {
    LOG_WARN("set lookup info failed", K(ret));
  } else {
    lookup_table_loc->is_reading_ = true;
  }
  return ret;
}

int ObTableScanOp::pushdown_attach_task_to_das(ObDASScanOp &target_op)
{
  int ret = OB_SUCCESS;
  ObDASAttachRtInfo *attach_rtinfo = tsc_rtdef_.attach_rtinfo_;
  if (MY_SPEC.is_index_global_ && nullptr != MY_CTDEF.lookup_ctdef_
      && DAS_OP_DOC_ID_MERGE == MY_CTDEF.attach_spec_.attach_ctdef_->op_type_) {
    // just skip, and doc id merge will be attach into global lookup iter.
  } else if (MY_SPEC.is_index_global_ && nullptr != MY_CTDEF.lookup_ctdef_
      && DAS_OP_VID_MERGE == MY_CTDEF.attach_spec_.attach_ctdef_->op_type_) {
    // just skip, and doc id merge will be attach into global lookup iter.
  } else if (MY_SPEC.is_index_global_ && nullptr != MY_CTDEF.lookup_ctdef_
      && DAS_OP_DOMAIN_ID_MERGE == MY_CTDEF.attach_spec_.attach_ctdef_->op_type_) {
    // just skip, and domain id merge will be attach into global lookup iter.
  } else if (OB_FAIL(target_op.reserve_related_buffer(attach_rtinfo->related_scan_cnt_))) {
    LOG_WARN("reserve related buffer failed", K(ret), K(attach_rtinfo->related_scan_cnt_));
  } else if (OB_FAIL(attach_related_taskinfo(target_op, attach_rtinfo->attach_rtdef_))) {
    LOG_WARN("attach related task info failed", K(ret));
  } else {
    target_op.set_attach_ctdef(MY_CTDEF.attach_spec_.attach_ctdef_);
    target_op.set_attach_rtdef(tsc_rtdef_.attach_rtinfo_->attach_rtdef_);
  }
  return ret;
}

int ObTableScanOp::attach_related_taskinfo(ObDASScanOp &target_op, ObDASBaseRtDef *attach_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_rtdef) || OB_ISNULL(attach_rtdef->ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attach rtdef is invalid", K(ret), KP(attach_rtdef));
  } else if (attach_rtdef->op_type_ == DAS_OP_TABLE_SCAN) {
    const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(attach_rtdef->ctdef_);
    ObDASScanRtDef *scan_rtdef = static_cast<ObDASScanRtDef*>(attach_rtdef);
    ObDASTableLoc *table_loc = scan_rtdef->table_loc_;
    ObDASTabletLoc *tablet_loc = ObDASUtils::get_related_tablet_loc(
        *target_op.get_tablet_loc(), table_loc->loc_meta_->ref_table_id_);
    if (OB_ISNULL(tablet_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("related tablet loc is not found", K(ret),
               KPC(target_op.get_tablet_loc()),
               KPC(table_loc->loc_meta_));
    } else if (OB_FAIL(target_op.set_related_task_info(scan_ctdef,
                                                       scan_rtdef,
                                                       tablet_loc->tablet_id_))) {
      LOG_WARN("set attach task info failed", K(ret), KPC(tablet_loc));
    } else {
      table_loc->is_reading_ = true;
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < attach_rtdef->children_cnt_; ++i) {
      if (OB_FAIL(attach_related_taskinfo(target_op, attach_rtdef->children_[i]))) {
        LOG_WARN("recursive attach related task info failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTableScanOp::prepare_pushdown_limit_param()
{
  int ret = OB_SUCCESS;
  if (!limit_param_.is_valid()) {
    //ignore, do nothing
  } else if (MY_SPEC.batch_scan_flag_ || nullptr != tsc_rtdef_.attach_rtinfo_) {
    //batch scan can not pushdown limit param to storage
    // do final limit for TSC op with attached ops for now
    need_final_limit_ = true;
    tsc_rtdef_.scan_rtdef_.limit_param_.offset_ = 0;
    tsc_rtdef_.scan_rtdef_.limit_param_.limit_ = -1;

    if (nullptr != MY_CTDEF.lookup_ctdef_) {
      OB_ASSERT(nullptr != tsc_rtdef_.lookup_rtdef_);
      tsc_rtdef_.lookup_rtdef_->limit_param_.offset_ = 0;
      tsc_rtdef_.lookup_rtdef_->limit_param_.limit_  = -1;
    }
  } else if (tsc_rtdef_.has_lookup_limit() || (OB_NOT_NULL(scan_iter_) && scan_iter_->get_das_task_cnt() > 1)) {
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

  // NOTICE: TSC operator can not apply final limit when das need keep ordering for multi partitions,
  // consider following:
  //            TSC (limit 10), need_keep_order
  //                 /      \
  //                /        \
  //               /          \
  //           DAS SCAN     DAS SCAN
  //              p0           p1
  //          (limit 10)   (limit 10)
  // when das need keep ordering, TSC should get 10 rows from each partition, with the upper operator
  // applying merge sort and final limit.
  // However, if we apply final limit on TSC operator, it will exit after got 10 rows from p0.
  // TODO: @bingfan remove need_final_limit_ from TSC operator
  if (MY_CTDEF.is_das_keep_order_ && OB_NOT_NULL(scan_iter_) && scan_iter_->get_das_task_cnt() > 1) {
    need_final_limit_ = false;
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
    if (das_location.is_dynamic_replica_select_table() && tsc_rtdef_.dynamic_selected_tablet_id_.is_valid()) {
      if (OB_FAIL(tablet_ids.push_back(tsc_rtdef_.dynamic_selected_tablet_id_))) {
        LOG_WARN("failed to push back dynamic selected tablet id", K(ret));
      }
    } else if (OB_FAIL(das_location.calculate_tablet_ids(ctx_,
                                                         plan_ctx->get_param_store(),
                                                         tablet_ids,
                                                         partition_ids,
                                                         first_level_part_ids,
                                                         dtc_params))) {
      LOG_WARN("calculate dynamic partitions failed", K(ret));
    } else if (das_location.is_dynamic_replica_select_table()) {
      if (tablet_ids.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic selected table should have only one tablet", K(tablet_ids), K(ret));
      } else {
        tsc_rtdef_.dynamic_selected_tablet_id_ = tablet_ids.at(0);
      }
    }
    LOG_TRACE("dynamic calculate partitions", K(das_location.is_dynamic_replica_select_table()),
             K(tablet_ids), K(partition_ids), K(first_level_part_ids), K(ret), K(das_location));
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
  // get grop size of batch rescan
  int ret = OB_SUCCESS;
  if (need_perform_real_batch_rescan()) {
    tsc_rtdef_.group_size_ = tsc_rtdef_.bnlj_params_.at(0).gr_param_->count_;
    if (OB_UNLIKELY(tsc_rtdef_.group_size_ > tsc_rtdef_.max_group_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The amount of data exceeds the pre allocated memory", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (MY_SPEC.gi_above_ && !MY_INPUT.key_ranges_.empty()) {
      if (OB_FAIL(prepare_das_task())) {
        LOG_WARN("prepare das task failed", K(ret));
      }
    } else {
      int64_t group_size = (output_ == iter_tree_) ?  1: tsc_rtdef_.group_size_;
      GroupRescanParamGuard grp_guard(tsc_rtdef_, GET_PHY_PLAN_CTX(ctx_)->get_param_store_for_update());
      for (int64_t i = 0; OB_SUCC(ret) && i < group_size; ++i) {
        if (need_perform_real_batch_rescan()) {
          grp_guard.switch_group_rescan_param(i);
        }
        if (MY_CTDEF.use_index_merge_ && OB_FAIL(prepare_index_merge_scan_range(i))) {
          LOG_WARN("failed to prepare index merge scan range", K(ret));
        } else if (!MY_CTDEF.use_index_merge_ && OB_FAIL(prepare_single_scan_range(i))) {
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

  return ret;
}

int ObTableScanOp::init_attach_scan_rtdef(const ObDASBaseCtDef *attach_ctdef,
                                          ObDASBaseRtDef *&attach_rtdef)
{
  int ret = OB_SUCCESS;
  ObDASTaskFactory &das_factory = DAS_CTX(ctx_).get_das_factory();
  if (OB_ISNULL(attach_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attach ctdef is nullptr", K(ret));
  } else if (OB_FAIL(das_factory.create_das_rtdef(attach_ctdef->op_type_, attach_rtdef))) {
    LOG_WARN("create das rtdef failed", K(ret), K(attach_ctdef->op_type_));
  } else if (ObDASTaskFactory::is_attached(attach_ctdef->op_type_)) {
    attach_rtdef->ctdef_ = attach_ctdef;
    attach_rtdef->children_cnt_ = attach_ctdef->children_cnt_;
    attach_rtdef->eval_ctx_ = &eval_ctx_;
    if (attach_ctdef->children_cnt_ > 0) {
      if (OB_ISNULL(attach_rtdef->children_ = OB_NEW_ARRAY(ObDASBaseRtDef*,
                                                           &ctx_.get_allocator(),
                                                           attach_ctdef->children_cnt_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate child buf failed", K(ret), K(attach_ctdef->children_cnt_));
      }
      for (int i = 0; OB_SUCC(ret) && i < attach_ctdef->children_cnt_; ++i) {
        if (OB_FAIL(init_attach_scan_rtdef(attach_ctdef->children_[i], attach_rtdef->children_[i]))) {
          LOG_WARN("init attach scan rtdef failed", K(ret));
        }
      }
    }
  } else {
    tsc_rtdef_.attach_rtinfo_->related_scan_cnt_++;
    if (attach_ctdef == &MY_CTDEF.scan_ctdef_) {
      attach_rtdef = &tsc_rtdef_.scan_rtdef_;
    } else if (attach_ctdef == MY_CTDEF.lookup_ctdef_) {
      attach_rtdef = tsc_rtdef_.lookup_rtdef_;
    } else if (attach_ctdef->op_type_ != DAS_OP_TABLE_SCAN) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("attach ctdef type is invalid", K(ret), K(attach_ctdef->op_type_));
    } else {
      const ObDASScanCtDef *attach_scan_ctdef = static_cast<const ObDASScanCtDef*>(attach_ctdef);
      const ObDASTableLocMeta *attach_loc_meta = MY_CTDEF.attach_spec_.get_attach_loc_meta(
          MY_SPEC.table_loc_id_, attach_scan_ctdef->ref_table_id_);
      ObDASScanRtDef *attach_scan_rtdef = static_cast<ObDASScanRtDef*>(attach_rtdef);
      if (OB_FAIL(init_das_scan_rtdef(*attach_scan_ctdef, *attach_scan_rtdef, attach_loc_meta))) {
        LOG_WARN("init das scan rtdef failed", K(ret));
      }
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
      if (OB_FAIL(init_das_scan_rtdef(lookup_ctdef,
                                      *tsc_rtdef_.lookup_rtdef_,
                                      MY_CTDEF.lookup_loc_meta_))) {
        LOG_WARN("init das scan rtdef failed", K(ret), K(lookup_ctdef));
      }
    }
  }
  if (OB_SUCC(ret) && MY_CTDEF.attach_spec_.attach_ctdef_ != nullptr) {
    if (OB_ISNULL(tsc_rtdef_.attach_rtinfo_ = OB_NEWx(ObDASAttachRtInfo, &ctx_.get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate attach rtinfo failed", K(ret));
    } else if (OB_FAIL(init_attach_scan_rtdef(MY_CTDEF.attach_spec_.attach_ctdef_,
                                              tsc_rtdef_.attach_rtinfo_->attach_rtdef_))) {
      LOG_WARN("init attach scan rtdef failed", K(ret));
    } else if (tsc_rtdef_.attach_rtinfo_->pushdown_tasks_.empty()) {
      //has no pushdown task, means all attach task can be pushdown
      if (OB_FAIL(tsc_rtdef_.attach_rtinfo_->pushdown_tasks_.push_back(
          tsc_rtdef_.attach_rtinfo_->attach_rtdef_))) {
        LOG_WARN("store pushdown das rtdef failed", K(ret));
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
  bool is_lookup = (&das_ctdef == MY_CTDEF.get_lookup_ctdef());
  bool is_lookup_limit = MY_SPEC.is_index_back() &&
      !MY_CTDEF.lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  ObTaskExecutorCtx &task_exec_ctx = ctx_.get_task_exec_ctx();
  das_rtdef.ctdef_ = &das_ctdef;
  das_rtdef.timeout_ts_ = plan_ctx->get_ps_timeout_timestamp();
  das_rtdef.tx_lock_timeout_ = my_session->get_trx_lock_timeout();
  das_rtdef.scan_flag_ = MY_CTDEF.scan_flags_;
  LOG_DEBUG("scan flag", K(MY_CTDEF.scan_flags_));
  das_rtdef.scan_flag_.is_show_seed_ = plan_ctx->get_show_seed();
  das_rtdef.tsc_monitor_info_ = &tsc_monitor_info_;
  das_rtdef.scan_op_id_ = MY_SPEC.get_id();
  das_rtdef.scan_rows_size_ = MY_SPEC.rows_ * MY_SPEC.width_;
  das_rtdef.das_tasks_key_.init(das_tasks_key_);
  if(is_foreign_check_nested_session()) {
    das_rtdef.is_for_foreign_check_ = true;
    if (plan_ctx->get_phy_plan()->has_for_update() && ObSQLUtils::is_iter_uncommitted_row(&ctx_)) {
      das_rtdef.scan_flag_.set_iter_uncommitted_row();
    }
  }
  if (MY_SPEC.batch_scan_flag_) {
    // if tsc enable batch rescan, the output order of tsc is determined by group id
    if (das_rtdef.scan_flag_.scan_order_ == ObQueryFlag::Reverse) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Scan order is not supported in batch rescan", K(ret), K(das_rtdef.scan_flag_.scan_order_));
    } else {
      das_rtdef.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }
  }

  if (is_lookup) {
    das_rtdef.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
  }
  das_rtdef.scan_flag_.is_lookup_for_4377_ = is_lookup;
  das_rtdef.need_check_output_datum_ = MY_SPEC.need_check_output_datum_;
  das_rtdef.sql_mode_ = my_session->get_sql_mode();
  das_rtdef.stmt_allocator_.set_alloc(&ctx_.get_allocator());
  das_rtdef.scan_allocator_.set_alloc(&ctx_.get_allocator());
  das_rtdef.eval_ctx_ = &get_eval_ctx();
  if (nullptr != tsc_ctdef.attach_spec_.attach_ctdef_) {
    // disable limit pushdown to das iter for table scan with attached pushdown ops
  } else if ((is_lookup_limit && is_lookup) || (!is_lookup_limit && !is_lookup)) {
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
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tsc_ctdef.flashback_item_.set_flashback_query_info(eval_ctx_, das_rtdef))) {
      LOG_WARN("failed to set flashback query snapshot version", K(ret));
    } else if (share::is_oracle_mapping_real_virtual_table(MY_SPEC.ref_table_id_)
              && das_ctdef.ref_table_id_ < OB_MIN_SYS_TABLE_INDEX_ID) {
      //not index scan, keep need_scn_
    } else if (MY_SPEC.ref_table_id_ != das_ctdef.ref_table_id_) {
      //only data table scan need to set row scn flag
      das_rtdef.need_scn_ = false;
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
    if (OB_SUCC(ret) && OB_NOT_NULL(das_rtdef.table_loc_) && OB_NOT_NULL(das_rtdef.table_loc_->loc_meta_)) {
      if (das_rtdef.table_loc_->loc_meta_->select_leader_ == 0) {
        das_rtdef.scan_flag_.set_is_select_follower();
      }
    }
  }
  return ret;
}

int ObTableScanOp::prepare_scan_range()
{
  int ret = OB_SUCCESS;
  if (!need_perform_real_batch_rescan()) {
    if (MY_CTDEF.use_index_merge_ && OB_FAIL(prepare_index_merge_scan_range())) {
      LOG_WARN("failed to prepare index merge range", K(ret));
    } else if (!MY_CTDEF.use_index_merge_ && OB_FAIL(prepare_single_scan_range())) {
      LOG_WARN("failed to prepare single scan range", K(ret));
    }
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
  const GroupParamArray* group_params_above = nullptr;
  if (OB_ISNULL(group_params_above = ctx_.get_das_ctx().get_group_params())) {
    // do nothing
  } else if (tsc_rtdef_.bnlj_params_.empty()) {
    tsc_rtdef_.bnlj_params_.set_capacity(MY_CTDEF.bnlj_param_idxs_.count());
    LOG_TRACE("prepare batch scan range",K(MY_CTDEF.bnlj_param_idxs_), KPC(group_params_above));
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_CTDEF.bnlj_param_idxs_.count(); ++i) {
      int64_t param_idx = MY_CTDEF.bnlj_param_idxs_.at(i);
      uint64_t array_idx = OB_INVALID_ID;
      bool exist = false;
      if (OB_FAIL(ctx_.get_das_ctx().find_group_param_by_param_idx(param_idx, exist, array_idx))) {
        LOG_WARN("failed to find group param by param idx", K(ret), K(i), K(param_idx));
      } else if (!exist) {
        // ret = OB_ERR_UNEXPECTED;
        // LOG_WARN("failed to find group param", K(ret), K(exist), K(i), K(array_idx));
        LOG_TRACE("bnlj params is not a array", K(i), K(param_idx));
      } else {
        const GroupRescanParam &group_param = group_params_above->at(array_idx);
        OZ(tsc_rtdef_.bnlj_params_.push_back(GroupRescanParamInfo(param_idx, group_param.gr_param_)));
      }
    }
    if (OB_SUCC(ret) && !tsc_rtdef_.bnlj_params_.empty() && (OB_ISNULL(fold_iter_))) {
      if (OB_FAIL(ObDASIterUtils::create_group_fold_iter(MY_CTDEF,
                                                          tsc_rtdef_,
                                                          eval_ctx_,
                                                          ctx_,
                                                          eval_infos_,
                                                          MY_SPEC,
                                                          iter_tree_,
                                                          fold_iter_))) {
        LOG_WARN("failed to create group fold iter", K(ret), K(spec_.id_));
      }
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
  if (OB_UNLIKELY(!need_extract_range())) {
    // virtual table, do nothing
  } else if (MY_CTDEF.get_query_range_provider().is_contain_geo_filters() &&
             OB_FAIL(ObSQLUtils::extract_geo_query_range(
             MY_CTDEF.get_query_range_provider(),
             range_allocator,
             ctx_,
             key_ranges,
             MY_INPUT.mbr_filters_,
             ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
    LOG_WARN("failed to extract pre query ranges", K(ret));
  } else if (!MY_CTDEF.get_query_range_provider().is_contain_geo_filters() &&
             MY_CTDEF.get_query_range_provider().is_fast_nlj_range() &&
             OB_FAIL(MY_CTDEF.get_query_range_provider().get_fast_nlj_tablet_ranges(
              tsc_rtdef_.fast_final_nlj_range_ctx_,
              range_allocator,
              ctx_,
              plan_ctx->get_param_store(),
              locate_range_buffer(),
              key_ranges,
              ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
    LOG_WARN("failed to extract pre fast nlj query range", K(ret));
  } else if (!MY_CTDEF.get_query_range_provider().is_contain_geo_filters() &&
             !MY_CTDEF.get_query_range_provider().is_fast_nlj_range() &&
             OB_FAIL(ObSQLUtils::extract_pre_query_range(
              MY_CTDEF.get_query_range_provider(),
              range_allocator,
              ctx_,
              key_ranges,
              ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
    LOG_WARN("failed to extract pre query ranges", K(ret));
  } else if (MY_CTDEF.scan_ctdef_.is_external_table_) {
    uint64_t table_loc_id = MY_SPEC.get_table_loc_id();
    ObDASTableLoc *tab_loc = DAS_CTX(ctx_).get_table_loc_by_id(table_loc_id, MY_CTDEF.scan_ctdef_.ref_table_id_);
    const ObString &table_format_or_properties =  MY_CTDEF.scan_ctdef_.external_file_format_str_.str_;
    ObArray<int64_t> partition_ids;
    if (OB_ISNULL(tab_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table lock is null", K(ret));
    } else {
      for (DASTabletLocListIter iter = tab_loc->tablet_locs_begin(); OB_SUCC(ret)
             && iter != tab_loc->tablet_locs_end(); ++iter) {
        ret = partition_ids.push_back((*iter)->partition_id_);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObExternalTableUtils::prepare_single_scan_range(
                                                ctx_.get_my_session()->get_effective_tenant_id(),
                                                MY_CTDEF.scan_ctdef_.ref_table_id_,
                                                table_format_or_properties,
                                                partition_ids,
                                                key_ranges,
                                                range_allocator,
                                                key_ranges,
                         tab_loc->loc_meta_->is_external_files_on_disk_))) {
      LOG_WARN("failed to prepare single scan range for external table", K(ret));
    }
  } else if (OB_FAIL(MY_CTDEF.get_query_range_provider().get_ss_tablet_ranges(range_allocator,
                                ctx_,
                                ss_key_ranges,
                                ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
    LOG_WARN("failed to final extract index skip query range", K(ret));
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
                                         K(MY_INPUT.ss_key_ranges_), K(spec_.id_));
  return ret;
}

// for index merge, disable equal range optimization
int ObTableScanOp::prepare_index_merge_scan_range(int64_t group_idx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObIAllocator &range_allocator = (table_rescan_allocator_ != nullptr ?
      *table_rescan_allocator_ : ctx_.get_allocator());
  ObDASBaseRtDef *attach_rtdef = tsc_rtdef_.attach_rtinfo_->attach_rtdef_;
  if (OB_ISNULL(attach_rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KPC(attach_rtdef), K(ret));
  } else {
    ObDASBaseRtDef *index_merge_rtdef = nullptr;
    if (DAS_OP_TABLE_LOOKUP == attach_rtdef->op_type_ || DAS_OP_INDEX_PROJ_LOOKUP == attach_rtdef->op_type_) {
      index_merge_rtdef = attach_rtdef->children_[0];
    }
    if (OB_FAIL(prepare_range_for_each_index(group_idx, range_allocator, index_merge_rtdef))) {
      LOG_WARN("failed to prepare range for each index", KPC(index_merge_rtdef), K(ret));
    }
  }
  return ret;
}

int ObTableScanOp::prepare_range_for_each_index(int64_t group_idx,
                                                ObIAllocator &allocator,
                                                ObDASBaseRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rtdef) || rtdef->op_type_ != DAS_OP_INDEX_MERGE || OB_ISNULL(rtdef->ctdef_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index merge rtdef", KPC(rtdef), K(ret));
  } else {
    ObDASIndexMergeRtDef *merge_rtdef = static_cast<ObDASIndexMergeRtDef*>(rtdef);
    const ObDASIndexMergeCtDef *merge_ctdef = static_cast<const ObDASIndexMergeCtDef*>(rtdef->ctdef_);
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_rtdef->children_cnt_; ++i) {
      ObDASBaseRtDef *child_rtdef = merge_rtdef->children_[i];
      ObIndexMergeType node_type = merge_ctdef->merge_node_types_.at(i);
      if (OB_ISNULL(child_rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index merge rtdef", KPC(child_rtdef), K(ret));
      } else if (INDEX_MERGE_UNION == node_type || INDEX_MERGE_INTERSECT == node_type) {
        if (OB_FAIL(SMART_CALL(prepare_range_for_each_index(group_idx, allocator, child_rtdef)))) {
          LOG_WARN("failed to prepare range for each index", KPC(child_rtdef), K(ret));
        }
      } else if (INDEX_MERGE_SCAN == node_type) {
        ObDASScanRtDef *scan_rtdef = nullptr;
        if (child_rtdef->op_type_ == DAS_OP_TABLE_SCAN) {
          scan_rtdef = static_cast<ObDASScanRtDef*>(child_rtdef);
        } else if (child_rtdef->op_type_ == DAS_OP_SORT) {
          OB_ASSERT(child_rtdef->children_cnt_ == 1);
          scan_rtdef = static_cast<ObDASScanRtDef*>(child_rtdef->children_[0]);
        }
        if (OB_ISNULL(scan_rtdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr scan rtdef", KPC(child_rtdef), K(ret));
        } else {
          ObQueryRangeArray key_ranges;
          ObQueryRangeArray ss_key_ranges;
          const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
          const ObQueryRangeProvider &query_range_provider = scan_ctdef->get_query_range_provider();
          scan_rtdef->key_ranges_.reuse();
          scan_rtdef->ss_key_ranges_.reuse();
          scan_rtdef->mbr_filters_.reuse();
          if (OB_UNLIKELY(!query_range_provider.has_range())) {
            // virtual table, do nothing
          } else if (query_range_provider.is_contain_geo_filters() &&
                    OB_FAIL(ObSQLUtils::extract_geo_query_range(
                    query_range_provider,
                    allocator,
                    ctx_,
                    key_ranges,
                    scan_rtdef->mbr_filters_,
                    ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
            LOG_WARN("failed to extract pre query ranges", K(ret));
          } else if (!query_range_provider.is_contain_geo_filters() &&
                    OB_FAIL(ObSQLUtils::extract_pre_query_range(
                    query_range_provider,
                    allocator,
                    ctx_,
                    key_ranges,
                    ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
            LOG_WARN("failed to extract pre query ranges", K(ret));
          } else if (OB_FAIL(query_range_provider.get_ss_tablet_ranges(allocator,
                                        ctx_,
                                        ss_key_ranges,
                                        ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
            LOG_WARN("failed to final extract index skip query range", K(ret));
          } else if (!ss_key_ranges.empty()) {
            // index skip scan, ranges from extract_pre_query_range/get_ss_tablet_ranges,
            //  prefix range and postfix range is single range
            if (1 != ss_key_ranges.count() || 1 != key_ranges.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected index skip scan range", K(ret), K(key_ranges), K(ss_key_ranges));
            } else {
              key_ranges.at(0)->table_id_ = scan_ctdef->ref_table_id_;
              key_ranges.at(0)->group_idx_ = group_idx;
              ss_key_ranges.at(0)->table_id_ = scan_ctdef->ref_table_id_;
              ss_key_ranges.at(0)->group_idx_ = group_idx;
              if (OB_FAIL(scan_rtdef->key_ranges_.push_back(*key_ranges.at(0))) ||
                  OB_FAIL(scan_rtdef->ss_key_ranges_.push_back(*ss_key_ranges.at(0)))) {
                LOG_WARN("failed to push back ss key range", KPC(scan_rtdef), K(ret));
              }
            }
          } else {
            ObNewRange whole_range;
            ObNewRange *key_range = nullptr;
            whole_range.set_whole_range();
            whole_range.table_id_ = scan_ctdef->ref_table_id_;
            whole_range.group_idx_ = group_idx;
            for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
              key_range = key_ranges.at(i);
              key_range->table_id_ = scan_ctdef->ref_table_id_;
              key_range->group_idx_ = group_idx;
              if (OB_FAIL(scan_rtdef->key_ranges_.push_back(*key_range)) ||
                  OB_FAIL(scan_rtdef->ss_key_ranges_.push_back(whole_range))) {
                LOG_WARN("failed to push back key range", KPC(scan_rtdef), K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && MY_SPEC.is_vt_mapping_) {
            OZ(vt_result_converter_->convert_key_ranges(scan_rtdef->key_ranges_));
          }
        }
      }
    }
  }
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
  uint64_t timestamp = ObTimeUtility::current_time();
  int64_t thread_id = GETTID();
  das_tasks_key_.init(timestamp, thread_id, MY_SPEC.get_id());
  cur_trace_id_ = ObCurTraceId::get();
  init_scan_monitor_info();
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(ObDASUtils::check_nested_sql_mutating(MY_SPEC.ref_table_id_, ctx_, true))) {
    LOG_WARN("failed to check stmt table", K(ret), K(MY_SPEC.ref_table_id_));
  } else if (OB_UNLIKELY(!das_tasks_key_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das tasks key is invalid", K(ret));
  } else if (OB_FAIL(init_table_scan_rtdef())) {
    LOG_WARN("prepare scan param failed", K(ret));
  } else if (MY_SPEC.is_vt_mapping_ && OB_FAIL(init_converter())) {
    LOG_WARN("failed to init converter", K(ret));
  } else if (MY_SPEC.is_fts_ddl_ && OB_FAIL(fts_index_.init(MY_SPEC.is_fts_index_aux_, MY_SPEC.parser_name_,
          MY_SPEC.parser_properties_))) {
    LOG_WARN("fail to init fts index cache", K(ret));
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
    if (MY_CTDEF.get_query_range_provider().is_fast_nlj_range()) {
      int64_t column_count = MY_CTDEF.get_query_range_provider().get_column_count();
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

  // create and init iter_tree_.
  const ObTableScanSpec &spec = MY_SPEC;
  ObDASIterTreeType tree_type = spec.is_global_index_back() ? ITER_TREE_GLOBAL_LOOKUP : ITER_TREE_TABLE_SCAN;
  if (OB_SUCC(ret) && OB_FAIL(ObDASIterUtils::create_tsc_iter_tree(tree_type,
                                                                   spec.tsc_ctdef_,
                                                                   tsc_rtdef_,
                                                                   eval_ctx_,
                                                                   ctx_,
                                                                   eval_infos_,
                                                                   spec,
                                                                   can_partition_retry(),
                                                                   scan_iter_,
                                                                   iter_tree_))) {
    LOG_WARN("failed to create table scan iter tree", K(tree_type), K(ret));
  }
  output_ = iter_tree_;
  return ret;
}

int ObTableScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(scan_iter_)) {
    if (scan_iter_->has_task()) {
      int tmp_ret = fill_storage_feedback_info();
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("fill storage feedback info failed", KR(tmp_ret));
      }
    }
    if (OB_FAIL(scan_iter_->reuse())) {
      LOG_WARN("failed to reuse scan iter", K(ret));
    }
  }
  if (OB_SUCC(ret) && MY_SPEC.should_scan_index()) {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
    if (OB_NOT_NULL(session)) {
      uint64_t tenant_id = session->get_effective_tenant_id();
      uint64_t index_id = MY_CTDEF.scan_ctdef_.ref_table_id_;
      oceanbase::share::ObIndexUsageInfoMgr* mgr = MTL(oceanbase::share::ObIndexUsageInfoMgr*);
      if (OB_NOT_NULL(mgr)) {
        mgr->update(tenant_id, index_id);
      }
    }
  }

  if (OB_SUCC(ret)) {
    fts_index_.reuse();
    iter_end_ = false;
    need_init_before_get_row_ = true;
  }
  return ret;
}

int ObTableScanOp::do_init_before_get_row()
{
  int ret = OB_SUCCESS;
  if (need_init_before_get_row_) {
    LOG_DEBUG("do init before get row", K(MY_SPEC.id_), K(MY_SPEC.use_dist_das_), K(MY_SPEC.gi_above_));
    if (OB_UNLIKELY(iter_end_)) {
      LOG_DEBUG("do init before get row meet iter end", K(MY_SPEC.id_));
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
      } else {
        if (in_batch_rescan_subplan()) {
          // if the ancestor operator of TSC support batch rescan, update the group_id and batch rescan_cnt after perform a real-rescan
          group_rescan_cnt_ = ctx_.get_das_ctx().get_group_rescan_cnt();
          group_id_ = ctx_.get_das_ctx().get_skip_scan_group_id();
        }
        if (OB_FAIL(output_->set_merge_status(is_group_rescan() ? SORT_MERGE : SEQUENTIAL_MERGE))) {
          LOG_WARN("failed to set merge status for das iter", K(ret));
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
  if (OB_NOT_NULL(vt_result_converter_)) {
    vt_result_converter_->destroy();
    vt_result_converter_->~ObVirtualTableResultConverter();
    vt_result_converter_ = nullptr;
  }
  if (OB_NOT_NULL(iter_tree_)) {
    iter_tree_->release();
    iter_tree_ = nullptr;
  }
  if (OB_NOT_NULL(fold_iter_)) {
    fold_iter_->release();
    fold_iter_ = nullptr;
  }
  output_ = nullptr;
  scan_iter_ = nullptr;
  domain_index_.~ObDomainIndexCache();
  das_tasks_key_.reset();
}

void ObTableScanOp::init_scan_monitor_info()
{
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::IO_READ_BYTES;
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::SSSTORE_READ_BYTES;
  op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::SSSTORE_READ_ROW_COUNT;
  op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::MEMSTORE_READ_ROW_COUNT;
  tsc_monitor_info_.init(&(op_monitor_info_.otherstat_1_value_),
                         &(op_monitor_info_.otherstat_2_value_),
                         &(op_monitor_info_.otherstat_3_value_),
                         &(op_monitor_info_.otherstat_4_value_));
}

int ObTableScanOp::fill_storage_feedback_info()
{
  int ret = OB_SUCCESS;
  // fill storage feedback info for acs
  ObDASScanOp *scan_op = DAS_SCAN_OP(*scan_iter_->begin_task_iter());
  if (OB_ISNULL(scan_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr das scan op", K(ret));
  } else {
    ObTableScanParam &scan_param = scan_op->get_scan_param();
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
  }

  return ret;
}

int ObTableScanOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  in_rescan_ = true;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("failed to check status", K(ret));
  } else if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to exec inner rescan");
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
  bool need_real_rescan = false;
  if (OB_FAIL(build_bnlj_params())) {
    // At start of each round of batch rescan, NLJ will fill param_store with
    // batch parameters. After each right operator rescan, NLJ will fill
    // param_store with current rescan's parameters.
    // Therefore, we need to get and save bnlj parameters here or they will be
    // replaced by NLJ.
    LOG_WARN("build batch nlj params failed", KR(ret));
  } else if (OB_FAIL(check_need_real_rescan(need_real_rescan))) {
    LOG_WARN("failed to check if tsc need real rescan", K(ret));
  } else if (!need_real_rescan) {
    LOG_TRACE("[group rescan] need switch iter", K(group_rescan_cnt_), K(ctx_.get_das_ctx().get_group_rescan_cnt()),
              K(group_id_), K(ctx_.get_das_ctx().get_skip_scan_group_id()), K(spec_.id_));
    if (OB_FAIL(set_batch_iter(ctx_.get_das_ctx().get_skip_scan_group_id()))) {
      LOG_WARN("failed to switch batch iter", K(ret), K(ctx_.get_das_ctx().get_skip_scan_group_id()));
    }
    group_id_ = ctx_.get_das_ctx().get_skip_scan_group_id();
  } else {
    reset_iter_tree_for_rescan();
    LOG_TRACE("[group rescan] need perform real rescan", K(group_rescan_cnt_), K(ctx_.get_das_ctx().get_group_rescan_cnt()),
              K(group_id_), K(ctx_.get_das_ctx().get_skip_scan_group_id()), K(spec_.id_));
    if (is_virtual_table(MY_SPEC.ref_table_id_)
        || (OB_NOT_NULL(scan_iter_) && !scan_iter_->is_all_local_task())
        || (MY_SPEC.use_dist_das_ && nullptr != MY_CTDEF.das_dppr_tbl_)) {
      ret = close_and_reopen();
    } else {
      ret = local_iter_rescan();
    }
    if (OB_SUCC(ret) && need_perform_real_batch_rescan()) {
      LOG_TRACE("[group rescan] need perform real batch rescan");
      fold_iter_->init_group_range(0, tsc_rtdef_.bnlj_params_.at(0).gr_param_->count_);
    }
  }

  return ret;
}

int ObTableScanOp::close_and_reopen()
{
  int ret = OB_SUCCESS;
  iter_end_ = false;
  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(inner_close())) {
    LOG_WARN("fail to close op", K(ret));
  } else if (OB_FAIL(reuse_table_rescan_allocator())) {
    LOG_WARN("reuse table rescan allocator failed", K(ret));
  } else {
    need_final_limit_ = false;
    //in order to avoid memory expansion caused by repeatedly creating DAS Tasks,
    //stmt allocator uses DAS allocator in the reopen process
    tsc_rtdef_.scan_rtdef_.stmt_allocator_.set_alloc(scan_iter_->get_das_alloc());
    tsc_rtdef_.scan_rtdef_.scan_allocator_.set_alloc(table_rescan_allocator_);
    MY_INPUT.key_ranges_.reuse();
    MY_INPUT.ss_key_ranges_.reuse();
    MY_INPUT.mbr_filters_.reuse();

    // when not use global index, replace stmt allocator of lookup and attached table scan to index table scan
    // at each rescan to avoid memory expansion. The stmt allocator of index table scan is actually the das
    // ref reuse alloc when rescan.
    // NOTE: global index lookup task will be handled in a different das ref, thus we can not replace its stmt
    // allocator.
    if (!MY_SPEC.is_index_global_) {
      if (nullptr != tsc_rtdef_.lookup_rtdef_) {
        tsc_rtdef_.lookup_rtdef_->stmt_allocator_.set_alloc(scan_iter_->get_das_alloc());
      }
      if (nullptr != tsc_rtdef_.attach_rtinfo_) {
        if (OB_FAIL(set_stmt_allocator(tsc_rtdef_.attach_rtinfo_->attach_rtdef_, scan_iter_->get_das_alloc()))) {
          LOG_WARN("failed to set stmt allocator", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableScanOp::set_stmt_allocator(ObDASBaseRtDef *rtdef, ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rtdef) || OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(rtdef), K(alloc), K(ret));
  } else if (DAS_OP_TABLE_SCAN == rtdef->op_type_) {
    static_cast<ObDASScanRtDef*>(rtdef)->stmt_allocator_.set_alloc(alloc);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rtdef->children_cnt_; ++i) {
      if (OB_FAIL(set_stmt_allocator(rtdef->children_[i], alloc))) {
        LOG_WARN("failed to set stmt allocator", K(ret), K(rtdef));
      }
    }
  }
  return ret;
}

int ObTableScanOp::local_iter_rescan()
{
  int ret = OB_SUCCESS;
  ObGranuleTaskInfo info;
  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(get_access_tablet_loc(info))) {
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
    DASTaskIter task_iter = scan_iter_->begin_task_iter();
    for (; OB_SUCC(ret) && !task_iter.is_end(); ++task_iter) {
      ObDASScanOp *scan_op = DAS_SCAN_OP(*task_iter);
      if (OB_FAIL(cherry_pick_range_by_tablet_id(scan_op))) {
        LOG_WARN("prune query range by partition id failed", K(ret));
      } else if (OB_FAIL(scan_iter_->rescan_das_task(scan_op))) {
        LOG_WARN("rescan das task failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (in_batch_rescan_subplan()) {
      // if the ancestor operator of TSC support batch rescan, update the group_id and batch rescan_cnt after perform a real-rescan
      group_rescan_cnt_ = ctx_.get_das_ctx().get_group_rescan_cnt();
      group_id_ = ctx_.get_das_ctx().get_skip_scan_group_id();
    }
    if (OB_FAIL(output_->set_merge_status(is_group_rescan() ? SORT_MERGE : SEQUENTIAL_MERGE))) {
      LOG_WARN("failed to set merge status for das iter", K(ret));
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
  for (DASTaskIter task_iter = scan_iter_->begin_task_iter();
      !task_iter.is_end(); ++task_iter) {
    ObDASScanOp *scan_op = DAS_SCAN_OP(*task_iter);
    if (OB_ISNULL(scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr das scan op", K(ret));
    } else {
      bool need_switch_param = (scan_op->get_tablet_loc() != MY_INPUT.tablet_loc_ &&
                                  MY_INPUT.tablet_loc_ != nullptr);
      if (MY_INPUT.tablet_loc_ != nullptr) {
        scan_op->set_tablet_id(MY_INPUT.tablet_loc_->tablet_id_);
        scan_op->set_ls_id(MY_INPUT.tablet_loc_->ls_id_);
        scan_op->set_tablet_loc(MY_INPUT.tablet_loc_);
      }
      if (MY_SPEC.gi_above_) {
        if (!MY_SPEC.is_index_global_ && MY_CTDEF.lookup_ctdef_ != nullptr) {
          //is local index lookup, need to set the lookup ctdef to the das scan op
          if (OB_FAIL(pushdown_normal_lookup_to_das(*scan_op))) {
            LOG_WARN("pushdown normal lookup to das failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && MY_CTDEF.attach_spec_.attach_ctdef_ != nullptr) {
          if (OB_FAIL(pushdown_attach_task_to_das(*scan_op))) {
            LOG_WARN("pushdown attach task to das failed", K(ret));
          }
        }
      }
      scan_op->reuse_iter();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reuse_table_rescan_allocator())) {
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

int ObTableScanOp::check_need_real_rescan(bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  const GroupParamArray* group_params_above = nullptr;
  bool enable_group_rescan_test_mode = false;
  enable_group_rescan_test_mode = (OB_SUCCESS != (OB_E(EventTable::EN_DAS_GROUP_RESCAN_TEST_MODE) OB_SUCCESS));
  if (OB_ISNULL(group_params_above = ctx_.get_das_ctx().get_group_params())) {
    bret = true;
  } else if (tsc_rtdef_.bnlj_params_.empty()) {
    //batch rescan not init, need to do real rescan
    bret = true;
  } else if (OB_UNLIKELY(MY_SPEC.gi_above_ && !MY_INPUT.get_need_extract_query_range())) {
    // partition-wise rescan with no dynamic range, disable batch rescan
    bret = true;
  } else {
    // the above operator of tsc support batch group rescan
    if (group_rescan_cnt_ < ctx_.get_das_ctx().get_group_rescan_cnt()) {
      // need perform batch rescan, the output of tsc is changed to fold_iter_
      if (ctx_.get_das_ctx().get_skip_scan_group_id() > 0) {
        output_ = iter_tree_;
        LOG_TRACE("[group rescan] skip read is found");
      } else {
        output_ = fold_iter_;
      }
      bret = true;
    } else if (group_rescan_cnt_ == ctx_.get_das_ctx().get_group_rescan_cnt()) {
      if (group_id_ < ctx_.get_das_ctx().get_skip_scan_group_id()) {
        if (output_ == fold_iter_) {
          bret = false;
        } else {
          bret = true;
        }
      } else if (group_id_ == ctx_.get_das_ctx().get_skip_scan_group_id()) {
        // the sql paln like this:
        //           spf
        //         /    \
        //   tsc_1    px_partition_iterator
        //                \
        //                tsc_2
        // if enable spf batch rescan in this paln, the rescan of tsc will called by px_partition_iterator, which need perform a real rescan
        bret = true;
        output_ = iter_tree_;
        LOG_TRACE("[group rescan] gi rescan is supportted in batch rescan");
      } else {
        if (enable_group_rescan_test_mode) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the group id of tsc exceeds the group id of above operator",
                    K(ret), K(group_rescan_cnt_), K(group_id_), K(ctx_.get_das_ctx().get_skip_scan_group_id()));
        } else {
          bret = true;
          output_ = iter_tree_;
          LOG_TRACE("[group rescan] found unexpected group id", K(group_rescan_cnt_), K(group_id_), K(ctx_.get_das_ctx().get_skip_scan_group_id()));
        }
      }
    } else {
      if (enable_group_rescan_test_mode) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the batch rescan count of tsc exceeds the batch count of above operator",
                  K(ret), K(group_rescan_cnt_), K(ctx_.get_das_ctx().get_group_rescan_cnt()));

      } else {
        bret = true;
        output_ = iter_tree_;
        LOG_TRACE("[group rescan] found unexpected group rescan cnt", K(group_rescan_cnt_), K(ctx_.get_das_ctx().get_group_rescan_cnt()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // need to perform batch rescan, but we need to ensure that the number of key ranges is not too large to avoid memory expansion
    // this check is only for static partition pruning, the limit for the number of key ranges is set to 100000
    if (output_ == fold_iter_ && bret) {
      if (OB_LIKELY(nullptr == MY_CTDEF.das_dppr_tbl_)) {
        int64_t partition_cnt = 0;
        int64_t group_size = 0;
        if (OB_UNLIKELY(tsc_rtdef_.bnlj_params_.empty()) ||
            OB_ISNULL(tsc_rtdef_.bnlj_params_.at(0).gr_param_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid bnlj params", K(tsc_rtdef_.bnlj_params_), K(ret));
        } else if (OB_ISNULL(tsc_rtdef_.scan_rtdef_.table_loc_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr table loc", K(ret));
        } else if (FALSE_IT(group_size = tsc_rtdef_.bnlj_params_.at(0).gr_param_->count_)) {
        } else {
          ObDASTableLoc *table_loc = tsc_rtdef_.scan_rtdef_.table_loc_;
          for (DASTabletLocListIter node = table_loc->tablet_locs_begin();
              OB_SUCC(ret) && node != table_loc->tablet_locs_end(); ++node) {
            partition_cnt++;
          }
          if (OB_SUCC(ret)) {
            if (group_size * partition_cnt > 100000) {
              // to many key ranges, fall back to single-row rescan
              output_ = iter_tree_;
              LOG_TRACE("[group rescan] too many key ranges, fall back to single-row rescan", K(group_size), K(partition_cnt));
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObTableScanOp::reset_iter_tree_for_rescan()
{
  if (OB_NOT_NULL(fold_iter_)) {
    fold_iter_->reuse();
  }

  // we cannot simply reuse iter tree due to local iter rescan optimization.
  if (iter_tree_->get_type() == DAS_ITER_GLOBAL_LOOKUP) {
    iter_tree_->reuse();
  }
}

int ObTableScanOp::set_batch_iter(int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (!is_group_rescan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("switch group with a null fold_iter", K(ret));
  } else {
    ret = fold_iter_->set_scan_group(group_id);
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
    if (OB_FAIL(output_->get_next_row())) {
      if (OB_ITER_END == ret) {
        // do nothing.
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
    ret = output_->get_next_rows(count, batch_size);
    if (OB_ITER_END == ret && count > 0) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next batch from das result failed", K(ret));
      }
    } else {
      // We need do filter first before do the limit.
      // See the issue 47201028.
      if (!MY_SPEC.filters_.empty() && count > 0) {
        bool all_filtered = false;
        if (OB_FAIL(filter_rows(MY_SPEC.filters_,
                                *brs_.skip_,
                                count,
                                all_filtered,
                                brs_.all_rows_active_))) {
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
  } // while end

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
    ret = output_->get_next_rows(count, batch_size);
    brs_.all_rows_active_ = true;
    if (OB_ITER_END == ret && count > 0) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next batch from das result failed", K(ret));
      }
    } else {
      // We need do filter first before do the limit.
      // See the issue 47201028.
      if (need_final_limit_ && !MY_SPEC.filters_.empty() && count > 0) {
        bool all_filtered = false;
        if (OB_FAIL(filter_rows(MY_SPEC.filters_,
                                *brs_.skip_,
                                count,
                                all_filtered,
                                brs_.all_rows_active_))) {
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
  if (OB_FAIL(inner_get_next_row_for_tsc())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
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
  if (OB_UNLIKELY(OB_ITER_END == ret && OB_NOT_NULL(scan_iter_) && scan_iter_->has_task())) {
//    ObIPartitionGroup *partition = NULL;
//    ObIPartitionGroupGuard *guard = NULL;
//    if (OB_ISNULL(guard)) {
//    } else if (OB_ISNULL(partition = guard->get_partition_group())) {
//    } else if (DAS_SCAN_OP->get_scan_param().main_table_scan_stat_.bf_access_cnt_ > 0) {
//      partition->feedback_scan_access_stat(DAS_SCAN_OP->get_scan_param());
//    }
    ObDASScanOp *scan_op = DAS_SCAN_OP(*scan_iter_->begin_task_iter());
    if (OB_ISNULL(scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr das scan op", K(ret));
    } else {
      ObTableScanParam &scan_param = scan_op->get_scan_param();
      ObTableScanStat &table_scan_stat = GET_PHY_PLAN_CTX(ctx_)->get_table_scan_stat();
      fill_table_scan_stat(scan_param.main_table_scan_stat_, table_scan_stat);
      LOG_DEBUG("[ROW_CACHE_ADJUST] fill cache stat for main table", K(&scan_param), K(scan_param.main_table_scan_stat_),
                                                    K(MY_SPEC.should_scan_index()), K(MY_SPEC.is_index_back()), K(MY_SPEC.is_index_global_),
                                                    K(scan_op->is_local_task()), K(table_scan_stat));
      if (MY_SPEC.is_index_back() && !MY_SPEC.is_index_global_ && scan_op->is_local_task()) {
        ObTableScanParam *lookup_param = scan_op->get_local_lookup_param();
        if (OB_NOT_NULL(lookup_param)) {
          fill_table_scan_stat(lookup_param->main_table_scan_stat_, table_scan_stat);
        }
        LOG_DEBUG("[ROW_CACHE_ADJUST] fill cache stat for local index lookup followed by table access", KP(lookup_param), K(table_scan_stat));
      }
      scan_param.main_table_scan_stat_.reset_cache_stat();
      iter_end_ = true;
      if (OB_FAIL(report_ddl_column_checksum())) {
        LOG_WARN("report checksum failed", K(ret));
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}
ERRSIM_POINT_DEF(EN_TABLE_SCAN_RETRY_WAIT_EVENT_ERRSIM);
int ObTableScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_E(EventTable::EN_ENABLE_RANDOM_TSC) OB_SUCCESS;
  bool enable_random_output = (tmp_ret != OB_SUCCESS);

  int64_t rand_row_cnt = max_row_cnt;
  int64_t rand_append_bits = 0;
  if (enable_random_output && max_row_cnt > 1) {
    gen_rand_size_and_skip_bits(max_row_cnt, rand_row_cnt, rand_append_bits);
  }
  if (OB_UNLIKELY(EN_TABLE_SCAN_RETRY_WAIT_EVENT_ERRSIM)) {
    ret = EN_TABLE_SCAN_RETRY_WAIT_EVENT_ERRSIM;
  } else if (OB_FAIL(inner_get_next_batch_for_tsc(rand_row_cnt))) {
    LOG_WARN("failed to get next batch", K(ret));
  }

  if (OB_SUCC(ret) && MY_SPEC.is_vt_mapping_) {
    ObEvalCtx::BatchInfoScopeGuard convert_guard(eval_ctx_);
    convert_guard.set_batch_size(brs_.size_);
    for (int i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
      if (brs_.skip_->at(i)) { continue; }
      convert_guard.set_batch_idx(i);
      if (OB_FAIL(vt_result_converter_->convert_output_row(
            eval_ctx_, MY_CTDEF.get_das_output_exprs(), MY_SPEC.agent_vt_meta_.access_exprs_))) {
        LOG_WARN("convert output row failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && enable_random_output && !brs_.end_
      && brs_.skip_->accumulate_bit_cnt(brs_.size_) == 0) {
    if (OB_UNLIKELY(brs_.size_ > max_row_cnt || rand_append_bits + brs_.size_ > max_row_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected tsc output rows", K(brs_), K(rand_append_bits), K(max_row_cnt),
                K(rand_row_cnt));
    } else {
      adjust_rand_output_brs(rand_append_bits);
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
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_size(brs_.size_);
      PRINT_VECTORIZED_ROWS(SQL, DEBUG, eval_ctx_, storage_output, brs_.size_, brs_.skip_,
                            K(MY_CTDEF.scan_ctdef_.ref_table_id_));
    }
    if (OB_FAIL(add_ddl_column_checksum_batch(brs_.size_))) {
      LOG_WARN("add ddl column checksum failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && brs_.end_ && OB_NOT_NULL(scan_iter_) && scan_iter_->has_task()) {
//    ObIPartitionGroup *partition = NULL;
//    ObIPartitionGroupGuard *guard = NULL;
//    if (OB_ISNULL(guard)) {
//    } else if (OB_ISNULL(partition = guard->get_partition_group())) {
//    } else if (DAS_SCAN_OP->get_scan_param().main_table_scan_stat_.bf_access_cnt_ > 0) {
//      partition->feedback_scan_access_stat(DAS_SCAN_OP->get_scan_param());
//    }
    ObDASScanOp *scan_op = DAS_SCAN_OP(*scan_iter_->begin_task_iter());
    ObTableScanParam &scan_param = scan_op->get_scan_param();
    ObTableScanStat &table_scan_stat = GET_PHY_PLAN_CTX(ctx_)->get_table_scan_stat();
    fill_table_scan_stat(scan_param.main_table_scan_stat_, table_scan_stat);
    LOG_DEBUG("[ROW_CACHE_ADJUST] fill cache stat for main table", K(&scan_param), K(scan_param.main_table_scan_stat_),
                                                    K(MY_SPEC.should_scan_index()), K(MY_SPEC.is_index_back()), K(MY_SPEC.is_index_global_),
                                                    K(scan_op->is_local_task()), K(table_scan_stat));
    if (MY_SPEC.is_index_back() && !MY_SPEC.is_index_global_ && scan_op->is_local_task()) {
      ObTableScanParam *lookup_param = scan_op->get_local_lookup_param();
      if (OB_NOT_NULL(lookup_param)) {
        fill_table_scan_stat(lookup_param->main_table_scan_stat_, table_scan_stat);
      }
      LOG_DEBUG("[ROW_CACHE_ADJUST] fill cache stat for local index lookup followed by table access", KP(lookup_param), K(table_scan_stat));
    }
    scan_param.main_table_scan_stat_.reset_cache_stat();
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
  if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (scan_iter_->has_task()) {
    //execute with das
    LOG_DEBUG("do table scan with DAS", K(MY_SPEC.ref_table_id_), K(MY_SPEC.table_loc_id_));
    if (OB_FAIL(prepare_pushdown_limit_param())) {
      LOG_WARN("prepare pushdow limit param failed", K(ret));
    } else if (OB_FAIL(scan_iter_->do_table_scan())) {
      LOG_WARN("execute all das scan task failed", K(ret));
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
    false_range.index_ordered_idx_ = input_ranges.at(0).index_ordered_idx_;
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
          if (get_spec().use_rich_format_) {
            expr->init_vector_for_write(eval_ctx_, VEC_UNIFORM, 1);
          }
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
                                                    part_range,
                                                    allocator))) {
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
    if (OB_UNLIKELY(MY_SPEC.get_query_range_provider().is_contain_geo_filters())) {
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
    query_flag.set_use_bloomfilter_cache();
  } else {
    if (plan_stat.enable_bf_cache_) {
      query_flag.set_use_bloomfilter_cache();
    } else {
      query_flag.set_not_use_bloomfilter_cache();
    }
  }
  if (try_use_cache && !plan_stat.enable_row_cache_) {
    query_flag.set_use_row_cache();
    const int64_t row_cache_access_cnt = plan_stat.row_cache_miss_cnt_ + plan_stat.row_cache_hit_cnt_;
    tsc_rtdef_.scan_rtdef_.in_row_cache_threshold_ = row_cache_access_cnt > ObPlanStat::CACHE_ACCESS_THRESHOLD ?
        (ObPlanStat::ROW_CACHE_GROWTH_SLOPE * plan_stat.row_cache_hit_cnt_ / row_cache_access_cnt) : plan_stat.in_row_cache_threshold_;
    LOG_DEBUG("[ROW_CACHE_ADJUST] try use row cache, update row cache threshold", K(plan_stat.execute_times_), K(plan_stat.row_cache_hit_cnt_),
                    K(plan_stat.row_cache_miss_cnt_), K(plan_stat.in_row_cache_threshold_), K(tsc_rtdef_.scan_rtdef_.in_row_cache_threshold_));
  } else {
    if (plan_stat.enable_row_cache_) {
      query_flag.set_use_row_cache();
      tsc_rtdef_.scan_rtdef_.in_row_cache_threshold_ = plan_stat.in_row_cache_threshold_;
      LOG_DEBUG("[ROW_CACHE_ADJUST] update row cache threshold", K(plan_stat.cache_stat_update_times_), K(plan_stat.row_cache_hit_cnt_),
                                                                 K(plan_stat.row_cache_miss_cnt_), K(plan_stat.in_row_cache_threshold_));
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
      item.tablet_id_ = tablet_id.id();
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
      LOG_INFO("report ddl checksum table scan", K(tablet_id), K(checksum_items));
      uint64_t data_format_version = 0;
      int64_t snapshot_version = 0;
      share::ObDDLTaskStatus unused_task_status = share::ObDDLTaskStatus::PREPARE;
      if (OB_FAIL(ObDDLUtil::get_data_information(MTL_ID(), MY_SPEC.plan_->get_ddl_task_id(), data_format_version, snapshot_version, unused_task_status))) {
        LOG_WARN("get ddl cluster version failed", K(ret));
      } else if (OB_FAIL(ObDDLChecksumOperator::update_checksum(data_format_version, checksum_items, *GCTX.sql_proxy_))) {
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
  if (OB_UNLIKELY(EN_TABLE_SCAN_RETRY_WAIT_EVENT_ERRSIM)) {
    ret = EN_TABLE_SCAN_RETRY_WAIT_EVENT_ERRSIM;
  } else if (OB_UNLIKELY(MY_SPEC.is_spatial_ddl())) {
    if (OB_FAIL(inner_get_next_spatial_index_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("spatial index ddl : get next spatial index row failed", K(ret));
      }
    }
  } else if (OB_UNLIKELY(MY_SPEC.is_fts_ddl_)) {
    if (OB_FAIL(inner_get_next_fts_index_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next fts index row", K(ret));
      }
    }
  } else if (OB_UNLIKELY(MY_SPEC.is_multivalue_ddl())) {
    if (OB_FAIL(inner_get_next_multivalue_index_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("multivalue index ddl : get next multivalue index row failed", K(ret));
      }
    }
  } else if (OB_FAIL(inner_get_next_row_implement())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next row failed", K(ret));
    }
  }
  return ret;
}

int ObTableScanOp::init_multivalue_index_rows()
{
  int ret = OB_SUCCESS;
  const ObTableScanSpec& spec = get_tsc_spec();
  const ObDASScanCtDef &scan_ctdef = MY_CTDEF.scan_ctdef_;
  const storage::ObTableReadInfo& read_info = scan_ctdef.table_param_.get_read_info();

  const ObExprPtrIArray &exprs = MY_SPEC.output_;
  uint32_t data_rowkey_cnt = read_info.get_schema_rowkey_count();
  uint32_t column_count = exprs.count() - 1;


  void *buf = ctx_.get_allocator().alloc(sizeof(ObDomainIndexRow));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate spatial row store failed", K(ret), K(buf));
  } else if (OB_FAIL(extend_domain_obj_buffer(SAPTIAL_INDEX_DEFAULT_ROW_COUNT))) {
    LOG_WARN("failed to extend obobj buffer.", K(ret));
  } else {
    domain_index_.dom_rows_ = new(buf) ObDomainIndexRow();
    domain_index_.mbr_buffer_ = nullptr;
    domain_index_.rowkey_count_ = data_rowkey_cnt;
    domain_index_.column_count_ = column_count;

    int64_t multivalue_col_id = scan_ctdef.multivalue_idx_;
    for (int i = 0; i < spec.ddl_output_cids_.count(); ++i) {
      if (multivalue_col_id == spec.ddl_output_cids_.at(i)) {
        domain_index_.domain_column_idx_ = i;
        break;
      }
    }

    ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
    uint64_t tenant_id = my_session->get_effective_tenant_id();

    new (&domain_index_.alloc_) ObArenaAllocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  }

  return ret;
}

int ObTableScanOp::extend_domain_obj_buffer(uint32_t size)
{
  int ret = OB_SUCCESS;

  if (domain_index_.record_count_ < size) {
    const ObExprPtrIArray &exprs = MY_SPEC.output_;
    uint32_t column_count = exprs.count() - 1;
    if (size < SAPTIAL_INDEX_DEFAULT_ROW_COUNT) {
      size = SAPTIAL_INDEX_DEFAULT_ROW_COUNT;
    }

    void *row_buf = ctx_.get_allocator().alloc(sizeof(blocksstable::ObDatumRow) * size);
    void* docid_buf = ctx_.get_allocator().alloc(sizeof(ObDocId) * size);

    if (OB_ISNULL(row_buf) || OB_ISNULL(docid_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate spatial row store failed", K(ret), K(row_buf));
    } else if (domain_index_.rows_) {
      ctx_.get_allocator().free(domain_index_.rows_);
      domain_index_.rows_ = nullptr;
    }

    if (OB_SUCC(ret) && domain_index_.docid_buffer_) {
      ctx_.get_allocator().free(domain_index_.docid_buffer_);
      domain_index_.docid_buffer_ = nullptr;
    }

    if (OB_SUCC(ret)) {
      domain_index_.rows_ = new (row_buf) blocksstable::ObDatumRow[size];
      domain_index_.record_count_ = size;
      domain_index_.docid_buffer_ = new (docid_buf) ObDocId();
      for (uint32_t i = 0; OB_SUCC(ret) && i < size; i++) {
        if (OB_FAIL(domain_index_.rows_[i].init(column_count))) {
          LOG_WARN("init datum row failed", K(ret), K(column_count));
        }
      }
    }
  }

  return ret;
}

int ObTableScanOp::multivalue_get_pure_data(
  ObIAllocator& tmp_allocator,
  const char*& data,
  int64_t& data_len,
  uint32_t& rowkey_start,
  uint32_t& rowkey_end,
  uint32_t& record_num,
  bool& is_save_rowkey)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef &scan_ctdef = MY_CTDEF.scan_ctdef_;
  const storage::ObTableReadInfo& read_info = scan_ctdef.table_param_.get_read_info();
  const ObExprPtrIArray &exprs = MY_SPEC.output_;
  uint32_t data_rowkey_cnt = read_info.get_schema_rowkey_count();

  uint32_t column_count = exprs.count() - 1;

  ObExpr *array_expr = exprs.at(column_count);
  ObDatum *json_datum = NULL;
  ObString json_arr_data;

  if (OB_FAIL(array_expr->eval(eval_ctx_, json_datum))) {
    LOG_WARN("expression evaluate failed", K(ret));
  } else {
    ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
    uint64_t tenant_id = my_session->get_effective_tenant_id();

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *json_datum,
                                                          array_expr->datum_meta_,
                                                          array_expr->obj_meta_.has_lob_header(),
                                                          json_arr_data))) {
      LOG_WARN("failed to get real geo data.", K(ret));
    } else {
      ObJsonBin bin(json_arr_data.ptr(), json_arr_data.length());

      if (OB_FAIL(bin.reset_iter())) {
        LOG_WARN("failed to parse binary.", K(ret), K(json_arr_data));
      } else if (!ObJsonVerType::is_opaque_or_string(bin.json_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to parse binary.", K(ret), K(json_arr_data));
      } else {
        data = bin.get_data();
        data_len = bin.get_data_length();
        record_num = *reinterpret_cast<const uint32_t*>(data);
      }
    }

    uint32_t pure_data_size = 0;
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(rowkey_end = column_count - 1)) {
    } else if (FALSE_IT(rowkey_start = rowkey_end - data_rowkey_cnt)) {
    } else if (OB_FAIL(extend_domain_obj_buffer(SAPTIAL_INDEX_DEFAULT_ROW_COUNT))) {
      LOG_WARN("failed to extend obobj buffer.", K(ret));
    } else {
      ObObj tmp_objs[column_count];

      for (uint32_t j = 0; OB_SUCC(ret) && j < rowkey_end; ++j) {
        tmp_objs[j].set_nop_value();
        ObDatum *datum = nullptr;
        ObExpr *expr = exprs.at(j);

        if (j == domain_index_.domain_column_idx_) {
        } else if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
          LOG_WARN("expression evaluate failed", K(ret));
        } else if (OB_FAIL(datum->to_obj(tmp_objs[j], expr->obj_meta_))) {
          LOG_WARN("stored row to new row obj failed", K(ret), K(*datum), K(expr->obj_meta_));
        } else {
          pure_data_size += tmp_objs[j].get_serialize_size();
        }
      }

      if (OB_SUCC(ret)) {
        if (record_num < 6) {
          is_save_rowkey = true;
        } else if (pure_data_size > 48 && scan_ctdef.table_param_.is_partition_table()) {
          is_save_rowkey = false;
        } else {
          is_save_rowkey = true;
        }
      }
    }
  }
  return ret;
}

int ObTableScanOp::inner_get_next_multivalue_index_row()
{
  int ret = OB_SUCCESS;
  bool need_ignore_null = false;
  if (OB_ISNULL(domain_index_.dom_rows_)) {
    if (OB_FAIL(init_multivalue_index_rows())) {
      LOG_WARN("init spatial row store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    while (OB_SUCC(ret) && domain_index_.domain_row_index_ >= domain_index_.dom_rows_->count()) {
      need_ignore_null = false;
      if (OB_FAIL(ObTableScanOp::inner_get_next_row_implement())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret), "op", op_name());
        }
      } else {
        domain_index_.dom_rows_->reuse();
        domain_index_.domain_row_index_ = 0;
        domain_index_.alloc_.reset();

        const char* data = nullptr;
        uint32_t record_num = 0;
        int64_t data_len = 0;
        bool is_save_rowkey = false;

        const ObDASScanCtDef &scan_ctdef = MY_CTDEF.scan_ctdef_;
        int64_t multivalue_idx = domain_index_.domain_column_idx_;


        ObIndexType index_type = static_cast<ObIndexType>(scan_ctdef.multivalue_type_);
        bool is_unique_index = (index_type == ObIndexType::INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL);

        const ObExprPtrIArray &exprs = MY_SPEC.output_;
        uint32_t column_count = exprs.count() - 1;
        uint32_t rowkey_start;
        uint32_t rowkey_end;

        if (multivalue_idx < 0 || multivalue_idx > column_count - 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get multivalue idx invalid", K(ret), K(multivalue_idx));
        } else if (OB_FAIL(multivalue_get_pure_data(domain_index_.alloc_, data, data_len,
                                     rowkey_start, rowkey_end, record_num, is_save_rowkey))) {
          LOG_WARN("get pure data failed", K(ret));
        } else if (record_num == 0 && is_unique_index) {
          need_ignore_null = true;
        } else {
          uint32_t obj_idx = 0;
          bool is_none_unique_done = false;
          const storage::ObTableReadInfo& read_info = scan_ctdef.table_param_.get_read_info();
          uint32_t data_rowkey_cnt = read_info.get_schema_rowkey_count();
          ObDocId *docid_arr = reinterpret_cast<ObDocId *>(domain_index_.docid_buffer_);
          int64_t pos = sizeof(uint32_t);

          for (uint64_t i = 0; OB_SUCC(ret) && (i < record_num || !is_none_unique_done); i++) {
            domain_index_.rows_[i].reuse();
            for (uint32_t j = 0; OB_SUCC(ret) && j < column_count; ++j) {
              ObExpr *expr = exprs.at(j);
              if (j == multivalue_idx) {
                ObObj tmp_obj;
                tmp_obj.set_nop_value();
                is_none_unique_done = true;
                if (OB_FAIL(tmp_obj.deserialize(data, data_len, pos))) {
                  LOG_WARN("failed to deserialize datum.", K(ret), K(i), K(j));
                } else {
                  ObObjMeta col_type = expr->obj_meta_;
                  if (ob_is_number_or_decimal_int_tc(col_type.get_type()) || ob_is_temporal_type(col_type.get_type())) {
                    col_type.set_collation_level(CS_LEVEL_NUMERIC);
                  } else {
                    col_type.set_collation_level(CS_LEVEL_IMPLICIT);
                  }
                  if (!tmp_obj.is_null()) {
                    tmp_obj.set_meta_type(col_type);
                  }
                  if (OB_FAIL(domain_index_.rows_[i].storage_datums_[j].from_obj_enhance(tmp_obj))) {
                    LOG_WARN("failed to convert datum from obj", K(ret), K(tmp_obj));
                  }
                }
              } else {
                ObDatum *datum = nullptr;
                if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
                  LOG_WARN("expression evaluate failed", K(ret));
                } else if (rowkey_start >= j && rowkey_end < j && !is_save_rowkey) {
                  domain_index_.rows_[i].storage_datums_[j].set_null();
                } else if (j == column_count - 1) {
                  if (OB_FAIL(docid_arr[i].from_string(datum->get_string()))) {
                    LOG_WARN("fail from string get docid", K(ret));
                  } else {
                    domain_index_.rows_[i].storage_datums_[j].set_string(docid_arr[i].get_string());
                  }
                } else {
                  domain_index_.rows_[i].storage_datums_[j].shallow_copy_from_datum(*datum);
                }
              }
            }

            if (OB_SUCC(ret) && OB_FAIL(domain_index_.dom_rows_->push_back(domain_index_.rows_ + i))) {
              LOG_WARN("failed to push back spatial index row", K(ret), K(domain_index_.rows_[i]));
            }
          }
          break;
        }
      }
    }
    if (OB_SUCC(ret) && !need_ignore_null) {
      ObStorageDatum *store_datums = (*(domain_index_.dom_rows_))[domain_index_.domain_row_index_++]->storage_datums_;
      if (OB_FAIL(fill_generated_multivalue_column(store_datums))) {
        LOG_WARN("failed to fill generated column", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanOp::fill_generated_multivalue_column(ObStorageDatum* store_datums)
{
  int ret = OB_SUCCESS;

  const ObExprPtrIArray &exprs = MY_SPEC.output_;
  uint32_t count = exprs.count();


  for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
    ObExpr *expr = exprs.at(i);
    ObDatum *datum = &expr->locate_datum_for_write(get_eval_ctx());
    ObEvalInfo *eval_info = &expr->get_eval_info(get_eval_ctx());

    if (i == count - 1) {
      datum->set_null();
    } else {
      ObObjDatumMapType type = ObDatum::get_obj_datum_map_type(expr->obj_meta_.get_type());
      if (OB_FAIL(datum->from_storage_datum(store_datums[i], type))) {
        LOG_WARN("fill multivalue index row failed", K(ret));
      }
    }
    eval_info->evaluated_ = true;
    eval_info->projected_ = true;
  }

  return ret;
}

int ObTableScanOp::inner_get_next_spatial_index_row()
{
  int ret = OB_SUCCESS;
  bool need_ignore_null = false;
  if (OB_ISNULL(domain_index_.dom_rows_)) {
    if (OB_FAIL(init_spatial_index_rows())) {
      LOG_WARN("init spatial row store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (domain_index_.domain_row_index_ >= domain_index_.dom_rows_->count()) {
      if (OB_FAIL(ObTableScanOp::inner_get_next_row_implement())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret), "op", op_name());
        }
      } else {
        domain_index_.dom_rows_->reuse();
        domain_index_.domain_row_index_ = 0;
        const ObExprPtrIArray &exprs = MY_SPEC.output_;
        ObExpr *expr = exprs.at(domain_index_.geo_idx_);
        ObDatum *in_datum = NULL;
        ObString geo_wkb;
        if (OB_FAIL(expr->eval(eval_ctx_, in_datum))) {
          LOG_WARN("expression evaluate failed", K(ret));
        } else if (OB_FALSE_IT(geo_wkb = in_datum->get_string())) {
        } else if (geo_wkb.length() > 0) {
          uint32_t srid = UINT32_MAX;
          omt::ObSrsCacheGuard srs_guard;
          const ObSrsItem *srs_item = NULL;
          const ObSrsBoundsItem *srs_bound = NULL;
          ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
          uint64_t tenant_id = my_session->get_effective_tenant_id();
          ObS2Cellids cellids;
          ObString mbr_val(0, static_cast<char *>(domain_index_.mbr_buffer_));

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
          } else if (cellids.size() == 0 && mbr_val.empty()) {
            // empty geometry collection
            need_ignore_null = true;
          } else if (cellids.size() > SAPTIAL_INDEX_DEFAULT_ROW_COUNT) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cellid over size", K(ret), K(cellids.size()));
          } else if (OB_ISNULL(domain_index_.rows_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory for spatial index datum row", K(ret));
          } else {
            for (uint64_t i = 0, datum_idx = 0; OB_SUCC(ret) && i < cellids.size(); i++) {
              domain_index_.rows_[i].reuse();
              domain_index_.rows_[i].storage_datums_[datum_idx].set_uint(cellids.at(i));
              domain_index_.rows_[i].storage_datums_[datum_idx + 1].set_string(mbr_val);
              // not set_collation_type(CS_TYPE_BINARY) and set_collation_level(CS_LEVEL_IMPLICIT)
              if (OB_FAIL(domain_index_.dom_rows_->push_back(domain_index_.rows_ + i))) {
                LOG_WARN("failed to push back spatial index row", K(ret), K(domain_index_.rows_[i]));
              }
            }
          }
        } else {
          need_ignore_null = true;
        }
      }
    }
    if (OB_SUCC(ret) && !need_ignore_null) {
      ObDatumRow *row = (*(domain_index_.dom_rows_))[domain_index_.domain_row_index_++];
      ObStorageDatum &cellid = row->storage_datums_[0];
      ObStorageDatum &mbr = row->storage_datums_[1];
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
  void *buf = ctx_.get_allocator().alloc(sizeof(ObDomainIndexRow));
  void *row_buf = ctx_.get_allocator().alloc(sizeof(blocksstable::ObDatumRow) * SAPTIAL_INDEX_DEFAULT_ROW_COUNT);
  void *mbr_buffer = ctx_.get_allocator().alloc(OB_DEFAULT_MBR_SIZE);
  if (OB_ISNULL(buf) || OB_ISNULL(mbr_buffer) || OB_ISNULL(row_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate spatial row store failed", K(ret), K(buf), K(mbr_buffer));
  } else {
    domain_index_.dom_rows_ = new(buf) ObDomainIndexRow();
    domain_index_.rows_ = new(row_buf) blocksstable::ObDatumRow[SAPTIAL_INDEX_DEFAULT_ROW_COUNT];
    domain_index_.mbr_buffer_ = mbr_buffer;
    const ObExprPtrIArray &exprs = MY_SPEC.output_;
    const uint8_t spatial_expr_cnt = 3;
    uint8_t cnt = 0;
    for (uint32_t i = 0; OB_SUCC(ret) && i < SAPTIAL_INDEX_DEFAULT_ROW_COUNT; i++) {
      if (OB_FAIL(domain_index_.rows_[i].init(SAPTIAL_INDEX_DEFAULT_COL_COUNT))) {
        LOG_WARN("init datum row failed", K(ret));
      }
    }
    for (uint32_t i = 0; OB_SUCC(ret) && i < exprs.count() && cnt < spatial_expr_cnt; i++) {
      if (exprs.at(i)->type_ == T_FUN_SYS_SPATIAL_CELLID) {
        domain_index_.cell_idx_ = i;
        cnt++;
      } else if (exprs.at(i)->type_ == T_FUN_SYS_SPATIAL_MBR) {
        domain_index_.mbr_idx_ = i;
        cnt++;
      } else if (exprs.at(i)->datum_meta_.type_ == ObGeometryType) {
        domain_index_.geo_idx_ = i;
        cnt++;
      }
    }
    if (OB_FAIL(ret) || cnt != spatial_expr_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid spatial index exprs", K(ret), K(cnt));
    }
  }
  return ret;
}

int ObTableScanOp::fill_generated_cellid_mbr(const ObStorageDatum &cellid, const ObStorageDatum &mbr)
{
  int ret = OB_SUCCESS;
  const ObExprPtrIArray &exprs = MY_SPEC.output_;
  if (exprs.count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid exprs count", K(ret), K(exprs.count()));
  } else {
    for (uint8_t i = 0; i < 2 && OB_SUCC(ret); i++) {
      ObObjDatumMapType type = i == 0 ? OBJ_DATUM_8BYTE_DATA : OBJ_DATUM_STRING;
      const ObStorageDatum &value = i == 0 ? cellid : mbr;
      uint32_t idx = i == 0 ? domain_index_.cell_idx_ : domain_index_.mbr_idx_;
      ObExpr *expr = exprs.at(idx);
      ObDatum *datum = &expr->locate_datum_for_write(get_eval_ctx());
      ObEvalInfo *eval_info = &expr->get_eval_info(get_eval_ctx());
      if (OB_FAIL(datum->from_storage_datum(value, type))) {
        LOG_WARN("fill spatial index row failed", K(ret));
      } else {
        eval_info->evaluated_ = true;
        eval_info->projected_ = true;
      }
    }
  }
  return ret;
}

void ObTableScanOp::gen_rand_size_and_skip_bits(const int64_t batch_size, int64_t &rand_size,
                                                int64_t &skip_bits)
{
  rand_size = batch_size;
  skip_bits = 0;
  if (batch_size > 1) {
    std::default_random_engine rd;
    rd.seed(std::random_device()());
    std::uniform_int_distribution<int64_t> irand(0, batch_size/2);
    skip_bits = irand(rd);
    if (skip_bits <= 0) {
      skip_bits = 1;
    }
    rand_size = batch_size - skip_bits;
    LOG_TRACE("random batch size", K(rand_size), K(skip_bits));
  }
}

void ObTableScanOp::adjust_rand_output_brs(const int64_t rand_append_bits)
{
  LOG_TRACE("random output", K(brs_.size_), K(rand_append_bits));
  int64_t output_size = brs_.size_ + rand_append_bits;
  brs_.skip_->set_all(brs_.size_, output_size);
  brs_.size_ = output_size;
  brs_.all_rows_active_ = false;
}

int ObTableScanOp::inner_get_next_fts_index_row()
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *row = nullptr;
  if (OB_FAIL(fts_index_.get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row from fts index cache", K(ret));
    } else if (OB_FAIL(fetch_next_fts_index_rows())) { // need overwrite return code
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to fetch next fts index rows", K(ret));
      }
    } else if (OB_FAIL(fts_index_.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row from fts index cache", K(ret));
      }
    }
  }
  if (FAILEDx(fill_generated_fts_cols(row))) {
    LOG_WARN("fail to fill generate fts cols", K(ret), KPC(row));
  }
  return ret;
}

int ObTableScanOp::fetch_next_fts_index_rows()
{
  int ret = OB_SUCCESS;
  bool has_segment_word = false;
  while (OB_SUCC(ret) && !has_segment_word) {
    ObExpr *ft_expr = nullptr;
    ObExpr *doc_id_expr = nullptr;
    ObDatum *ft_datum = nullptr;
    ObDatum *doc_id_datum = nullptr;
    if (OB_FAIL(ObTableScanOp::inner_get_next_row_implement())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row implement", K(ret));
      }
    } else if (OB_FAIL(get_output_fts_col_expr_by_type(T_FUN_SYS_DOC_ID, doc_id_expr))) {
      LOG_WARN("fail to get doc id column expr from output", K(ret));
    } else if (OB_FAIL(get_output_fts_col_expr_by_type(T_FUN_SYS_WORD_SEGMENT, ft_expr))) {
      LOG_WARN("fail to get word segment column expr from output", K(ret));
    } else if (OB_ISNULL(ft_expr) || OB_ISNULL(doc_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpeted error, ft or doc id expr is nullptr", K(ret), KP(ft_expr), KP(doc_id_expr));
    } else if (OB_FAIL(ft_expr->eval(eval_ctx_, ft_datum))) {
      LOG_WARN("fail to evaluate fulltext expr", K(ret));
    } else if (OB_FAIL(doc_id_expr->eval(eval_ctx_, doc_id_datum))) {
      LOG_WARN("fail to evaluate doc id expr", K(ret));
    } else if (OB_ISNULL(ft_datum) || OB_ISNULL(doc_id_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpeted error, ft or doc id datum is nullptr", K(ret), KP(ft_datum), KP(doc_id_datum));
    } else {
      ObString ft = ft_datum->get_string();
      const ObString &doc_id = doc_id_datum->get_string();
      ObArenaAllocator tmp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *ft_datum, ft_expr->datum_meta_,
              ft_expr->obj_meta_.has_lob_header(), ft))) {
        LOG_WARN("fail to read real string data", K(ret));
      } else if (OB_UNLIKELY(doc_id.length() != sizeof(ObDocId)) || OB_ISNULL(doc_id.ptr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid binary document id", K(ret), K(doc_id));
      } else if (OB_FAIL(fts_index_.segment(ft_expr->obj_meta_, doc_id, ft)) && OB_ITER_END != ret) {
        LOG_WARN("fail to segment fulltext", K(ret), K(doc_id), K(ft));
      } else if (OB_ITER_END == ret) {
        has_segment_word = false;
        ret = OB_SUCCESS;
      } else {
        has_segment_word = true;
      }
    }
  }
  return ret;
}

int ObTableScanOp::fill_generated_fts_cols(blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  const ObObjDatumMapType *types = MY_SPEC.is_fts_index_aux_ ? ObFTIndexRowCache::FTS_INDEX_TYPES : ObFTIndexRowCache::FTS_DOC_WORD_TYPES;
  const ObExprOperatorType *expr_types = MY_SPEC.is_fts_index_aux_ ? ObFTIndexRowCache::FTS_INDEX_EXPR_TYPE : ObFTIndexRowCache::FTS_DOC_WORD_EXPR_TYPE;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, row is nullptr", K(ret), KP(row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < share::ObFtsIndexBuilderUtil::OB_FTS_INDEX_OR_DOC_WORD_TABLE_COL_CNT; ++i) {
      ObExpr *expr = nullptr;
      if (OB_FAIL(get_output_fts_col_expr_by_type(expr_types[i], expr))) {
        LOG_WARN("fail to get fts column expr", K(ret), K(i), K(expr_types[i]));
      } else {
        ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
        ObEvalInfo &eval_info = expr->get_eval_info(eval_ctx_);
        if (OB_FAIL(datum.from_storage_datum(row->storage_datums_[i], types[i]))) {
          LOG_WARN("fail to fill fulltext index row", K(ret), K(i), K(MY_SPEC.output_), KPC(row));
        } else {
          eval_info.evaluated_ = true;
          eval_info.projected_ = true;
        }
      }
    }
  }
  return ret;
}

int ObTableScanOp::get_output_fts_col_expr_by_type(
    const ObExprOperatorType &type,
    ObExpr *&expr)
{
  int ret = OB_SUCCESS;
  expr = nullptr;
  if (OB_UNLIKELY(T_FUN_SYS_WORD_SEGMENT != type
               && T_FUN_SYS_DOC_ID != type
               && T_FUN_SYS_WORD_COUNT != type
               && T_FUN_SYS_DOC_LENGTH != type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fts column expr type", K(ret), "type", get_type_name(type));
  } else if (T_FUN_SYS_DOC_ID == type) {
    for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(expr) && i < MY_SPEC.output_.count(); ++i) {
      ObExpr *tmp_expr = MY_SPEC.output_.at(i);
      if (OB_ISNULL(tmp_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, expr in output is nullptr", K(ret), K(i));
      } else if (T_FUN_SYS_WORD_SEGMENT == tmp_expr->type_) {
        const int64_t idx = MY_SPEC.is_fts_index_aux_ ? i+1 : i-1;
        if (OB_UNLIKELY(idx < 0 || idx >= MY_SPEC.output_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, invalid doc id idx", K(ret), K(idx), K(i), K(MY_SPEC.output_));
        } else {
          expr = MY_SPEC.output_.at(idx);
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(expr) && i < MY_SPEC.output_.count(); ++i) {
      ObExpr *tmp_expr = MY_SPEC.output_.at(i);
      if (OB_ISNULL(tmp_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, expr in output is nullptr", K(ret), K(i));
      } else if (type == tmp_expr->type_) {
        expr = tmp_expr;
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, fts column expr isn't found", K(ret), "type", get_type_name(type), K(MY_SPEC.output_));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
