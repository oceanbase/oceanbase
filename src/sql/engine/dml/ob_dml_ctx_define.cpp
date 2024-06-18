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
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/engine/dml/ob_fk_checker.h"
#include "sql/das/ob_das_utils.h"
namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER(ObTrigDMLCtDef,
                    tg_event_,
                    tg_args_,
                    trig_col_info_,
                    all_tm_points_.bit_value_,
                    old_row_exprs_,
                    new_row_exprs_,
                    rowid_old_expr_,
                    rowid_new_expr_);

OB_SERIALIZE_MEMBER(ObErrLogCtDef,
                    is_error_logging_,
                    err_log_database_name_,
                    err_log_table_name_,
                    reject_limit_,
                    err_log_values_,
                    err_log_column_names_);


OB_SERIALIZE_MEMBER(ObTriggerColumnsInfo::Flags, flags_);

OB_DEF_SERIALIZE(ObTriggerColumnsInfo)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(flags_, count_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTriggerColumnsInfo)
{
  int ret = OB_SUCCESS;
  int64_t flags_count = 0;
  OB_UNIS_DECODE(flags_count);
  if (flags_count > 0) {
    if (OB_FAIL(init(flags_count))) {
      LOG_WARN("failed to init trigger column infos", K(ret));
    } else {
      OB_UNIS_DECODE_ARRAY(flags_, flags_count);
      count_ = flags_count;
    }
  } else {
    flags_ = NULL;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTriggerColumnsInfo)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(flags_, count_);
  return len;
}

int ObTriggerColumnsInfo::init(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || 0 == count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: allocator is null", K(ret), K(count));
  } else if (OB_ISNULL(flags_ = static_cast<Flags *>(allocator_->alloc(sizeof(Flags) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    capacity_ = count;
  }
  return ret;
}

int ObTriggerColumnsInfo::set_trigger_column(
  bool is_hidden,
  bool is_update,
  bool is_gen_col,
  bool is_gen_col_dep,
  bool is_rowid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(flags_) || 0 == capacity_ || count_ >= capacity_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: flags_ is null", K(ret), K(count_), KP(flags_));
  } else {
    flags_[count_].is_hidden_ = is_hidden;
    flags_[count_].is_update_ = is_update;
    flags_[count_].is_gen_col_ = is_gen_col;
    flags_[count_].is_gen_col_dep_ = is_gen_col_dep;
    flags_[count_].is_rowid_ = is_rowid;
    ++count_;
  }
  return ret;
}

int ObTriggerColumnsInfo::set_trigger_rowid()
{
  return set_trigger_column(true, false, false, false, true);
}

OB_SERIALIZE_MEMBER(ObForeignKeyCheckerCtdef,
                    calc_part_id_expr_,
                    part_id_dep_exprs_,
                    das_scan_ctdef_,
                    loc_meta_,
                    is_part_table_,
                    tablet_id_,
                    rowkey_count_,
                    rowkey_ids_);

OB_SERIALIZE_MEMBER(ObTriggerArg, trigger_id_, trigger_events_.bit_value_, timing_points_.bit_value_);

OB_SERIALIZE_MEMBER(ObForeignKeyColumn, name_, idx_, name_idx_, obj_meta_);

// OB_SERIALIZE_MEMBER(ObForeignKeyArg,
//                     ref_action_,
//                     table_name_,
//                     columns_,
//                     database_name_,
//                     is_self_ref_,
//                     table_id_,
//                     fk_ctdef_);

OB_DEF_SERIALIZE(ObForeignKeyArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              ref_action_,
              table_name_,
              columns_,
              database_name_,
              is_self_ref_,
              table_id_
  );
  if (OB_NOT_NULL(fk_ctdef_)) {
    OB_UNIS_ENCODE(*fk_ctdef_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObForeignKeyArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              ref_action_,
              table_name_,
              columns_,
              database_name_,
              is_self_ref_,
              table_id_
  );
  if (OB_NOT_NULL(fk_ctdef_)) {
    OB_UNIS_DECODE(*fk_ctdef_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObForeignKeyArg)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              ref_action_,
              table_name_,
              columns_,
              database_name_,
              is_self_ref_,
              table_id_
  );
  if (OB_NOT_NULL(fk_ctdef_)) {
    OB_UNIS_ADD_LEN(*fk_ctdef_);
  }
  return len;
}

OB_SERIALIZE_MEMBER(ColumnContent,
                    projector_index_,
                    auto_filled_timestamp_,
                    column_name_,
                    is_nullable_,
                    is_implicit_,
                    is_predicate_column_,
                    srs_id_);

int SeRowkeyItem::init(
    const ObExprPtrIArray &row, ObEvalCtx &ctx, ObIAllocator &alloc, const int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;
  CK(rowkey_cnt <= row.count());
  if (OB_SUCC(ret)) {
    // the %alloc is arena allocator, no need to free.
    if (OB_ISNULL(datums_ = static_cast<ObDatum *>(alloc.alloc(
                    rowkey_cnt * sizeof(*datums_))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new (datums_)ObDatum[rowkey_cnt];
    }
    row_ = row.get_data();
    cnt_ = rowkey_cnt;
  }
  ObDatum *datum = NULL;
  for (int i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    if (OB_FAIL((row.at(i)->eval(ctx, datum)))) {
      LOG_WARN("evaluate expression failed", K(ret));
    } else {
      datums_[i] = *datum;
    }
  }
  return ret;
}

bool SeRowkeyItem::operator==(
    const SeRowkeyItem &other) const
{
  bool equal = true;
  if (cnt_ != other.cnt_) {
    equal = false;
  } else {
    for (int64_t i = 0; equal && i < cnt_; ++i) {
      ObExprCmpFuncType cmp_func = row_[i]->basic_funcs_->null_first_cmp_;
      // rowkey has no lob col, can ignore ret here
      int cmp_ret = 0;
      (void)cmp_func(datums_[i], other.datums_[i], cmp_ret);
      equal = cmp_ret == 0;
    }
  }
  return equal;
}

uint64_t SeRowkeyItem::hash() const
{
  uint64_t hash_val = 0;
  for (int64_t i = 0; i < cnt_; ++i) {
    ObExprHashFuncType hash_func = row_[i]->basic_funcs_->default_hash_;
    hash_func(datums_[i], hash_val, hash_val);
  }
  return hash_val;
}

int SeRowkeyItem::copy_datum_data(ObIAllocator &alloc)
{
  // the %alloc is arena allocator, no need to free.
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != datums_))  {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
      if (OB_FAIL(datums_[i].deep_copy(datums_[i], alloc))) {
        LOG_WARN("copy datum data failed", K(ret));
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDMLBaseCtDef,
                    check_cst_exprs_,
                    fk_args_,
                    trig_ctdef_,
                    column_ids_,
                    old_row_,
                    new_row_,
                    error_logging_ctdef_,
                    view_check_exprs_,
                    is_primary_index_,
                    is_heap_table_,
                    has_instead_of_trigger_,
                    trans_info_expr_);

OB_SERIALIZE_MEMBER(ObMultiInsCtDef,
                    calc_part_id_expr_,
                    hint_part_ids_,
                    loc_meta_);

OB_DEF_SERIALIZE(ObInsCtDef)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObInsCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ENCODE,
              das_ctdef_,
              column_infos_,
              has_multi_ctx);
  if (has_multi_ctx) {
    OB_UNIS_ENCODE(*multi_ctdef_);
  }
  OZ(ObDASUtils::serialize_das_ctdefs(buf, buf_len, pos, related_ctdefs_));
  OB_UNIS_ENCODE(is_single_value_);
  return ret;
}

OB_DEF_DESERIALIZE(ObInsCtDef)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObInsCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = false;
  LST_DO_CODE(OB_UNIS_DECODE,
              das_ctdef_,
              column_infos_,
              has_multi_ctx);
  if (OB_SUCC(ret) && has_multi_ctx) {
    ObDMLCtDefAllocator<ObMultiInsCtDef> multi_ctdef_allocator(alloc_);
    multi_ctdef_ = multi_ctdef_allocator.alloc();
    if (OB_ISNULL(multi_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc multi_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*multi_ctdef_);
  }
  OZ(ObDASUtils::deserialize_das_ctdefs(buf, data_len, pos,
                                        alloc_, DAS_OP_TABLE_INSERT,
                                        related_ctdefs_));
  OB_UNIS_DECODE(is_single_value_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObInsCtDef)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObInsCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              das_ctdef_,
              column_infos_,
              has_multi_ctx);
  if (has_multi_ctx) {
    OB_UNIS_ADD_LEN(*multi_ctdef_);
  }
  len += ObDASUtils::das_ctdefs_serialize_size(related_ctdefs_);
  OB_UNIS_ADD_LEN(is_single_value_);
  return len;
}

OB_SERIALIZE_MEMBER(ObMultiDelCtDef,
                    calc_part_id_expr_,
                    loc_meta_);

OB_DEF_SERIALIZE(ObDelCtDef)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObDelCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ENCODE,
              das_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              has_multi_ctx);
  if (has_multi_ctx) {
    OB_UNIS_ENCODE(*multi_ctdef_);
  }
  OZ(ObDASUtils::serialize_das_ctdefs(buf, buf_len, pos, related_ctdefs_));
  OB_UNIS_ENCODE(distinct_key_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDelCtDef)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObDelCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = false;
  LST_DO_CODE(OB_UNIS_DECODE,
              das_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              has_multi_ctx);
  if (OB_SUCC(ret) && has_multi_ctx) {
    ObDMLCtDefAllocator<ObMultiDelCtDef> multi_ctdef_allocator(alloc_);
    multi_ctdef_ = multi_ctdef_allocator.alloc();
    if (OB_ISNULL(multi_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc multi_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*multi_ctdef_);
  }
  OZ(ObDASUtils::deserialize_das_ctdefs(buf, data_len, pos,
                                        alloc_, DAS_OP_TABLE_DELETE,
                                        related_ctdefs_));
  OB_UNIS_DECODE(distinct_key_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDelCtDef)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObDelCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              das_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              has_multi_ctx);
  if (has_multi_ctx) {
    OB_UNIS_ADD_LEN(*multi_ctdef_);
  }
  len += ObDASUtils::das_ctdefs_serialize_size(related_ctdefs_);
  OB_UNIS_ADD_LEN(distinct_key_);
  return len;
}

OB_SERIALIZE_MEMBER(ObMultiLockCtDef,
                    calc_part_id_expr_,
                    partition_cnt_,
                    loc_meta_);

OB_DEF_SERIALIZE(ObLockCtDef)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObLockCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ENCODE,
              das_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              has_multi_ctx);
  if (has_multi_ctx) {
    OB_UNIS_ENCODE(*multi_ctdef_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLockCtDef)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObLockCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = false;
  LST_DO_CODE(OB_UNIS_DECODE,
              das_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              has_multi_ctx);
  if (OB_SUCC(ret) && has_multi_ctx) {
    ObDMLCtDefAllocator<ObMultiLockCtDef> multi_ctdef_allocator(alloc_);
    multi_ctdef_ = multi_ctdef_allocator.alloc();
    if (OB_ISNULL(multi_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc multi_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*multi_ctdef_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLockCtDef)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObLockCtDef,ObDMLBaseCtDef));
  bool has_multi_ctx = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              das_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              has_multi_ctx);
  if (has_multi_ctx) {
    OB_UNIS_ADD_LEN(*multi_ctdef_);
  }
  return len;
}

OB_SERIALIZE_MEMBER(ObMultiUpdCtDef,
                    calc_part_id_old_,
                    calc_part_id_new_,
                    is_enable_row_movement_,
                    loc_meta_);

OB_DEF_SERIALIZE(ObUpdCtDef)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObUpdCtDef,ObDMLBaseCtDef));
  bool has_pkey_upd = (ddel_ctdef_ != nullptr);
  bool has_lock_def = (dlock_ctdef_ != nullptr);
  bool has_multi_def = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ENCODE,
              full_row_,
              dupd_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              assign_columns_,
              distinct_key_,
              has_pkey_upd,
              has_lock_def,
              has_multi_def);
  if (has_pkey_upd) {
    OB_UNIS_ENCODE(*ddel_ctdef_);
    OB_UNIS_ENCODE(*dins_ctdef_);
  }
  if (has_lock_def) {
    OB_UNIS_ENCODE(*dlock_ctdef_);
  }
  if (has_multi_def) {
    OB_UNIS_ENCODE(*multi_ctdef_);
  }
  OZ(ObDASUtils::serialize_das_ctdefs(buf, buf_len, pos, related_upd_ctdefs_));
  OZ(ObDASUtils::serialize_das_ctdefs(buf, buf_len, pos, related_del_ctdefs_));
  OZ(ObDASUtils::serialize_das_ctdefs(buf, buf_len, pos, related_ins_ctdefs_));
  return ret;
}

OB_DEF_DESERIALIZE(ObUpdCtDef)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObUpdCtDef,ObDMLBaseCtDef));
  bool has_pkey_upd = false;
  bool has_lock_def = false;
  bool has_multi_def = false;
  LST_DO_CODE(OB_UNIS_DECODE,
              full_row_,
              dupd_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              assign_columns_,
              distinct_key_,
              has_pkey_upd,
              has_lock_def,
              has_multi_def);
  if (OB_SUCC(ret) && has_pkey_upd) {
    ObDMLCtDefAllocator<ObDASDelCtDef> ddel_allocator(alloc_);
    ObDMLCtDefAllocator<ObDASInsCtDef> dins_allocator(alloc_);
    ddel_ctdef_ = ddel_allocator.alloc();
    dins_ctdef_ = dins_allocator.alloc();
    if (OB_ISNULL(ddel_ctdef_) || OB_ISNULL(dins_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc das ctdef failed", K(ret), K(ddel_ctdef_), K(dins_ctdef_));
    }
    OB_UNIS_DECODE(*ddel_ctdef_);
    OB_UNIS_DECODE(*dins_ctdef_);
  }
  if (OB_SUCC(ret) && has_lock_def) {
    ObDMLCtDefAllocator<ObDASLockCtDef> dlock_allocator(alloc_);
    dlock_ctdef_ = dlock_allocator.alloc();
    if (OB_ISNULL(dlock_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc das lock ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*dlock_ctdef_);
  }
  if (OB_SUCC(ret) && has_multi_def) {
    ObDMLCtDefAllocator<ObMultiUpdCtDef> mctdef_allocator(alloc_);
    multi_ctdef_ = mctdef_allocator.alloc();
    if (OB_ISNULL(multi_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc multi_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*multi_ctdef_);
  }
  OZ(ObDASUtils::deserialize_das_ctdefs(buf, data_len, pos,
                                        alloc_, DAS_OP_TABLE_UPDATE,
                                        related_upd_ctdefs_));
  OZ(ObDASUtils::deserialize_das_ctdefs(buf, data_len, pos,
                                        alloc_, DAS_OP_TABLE_DELETE,
                                        related_del_ctdefs_));
  OZ(ObDASUtils::deserialize_das_ctdefs(buf, data_len, pos,
                                        alloc_, DAS_OP_TABLE_INSERT,
                                        related_ins_ctdefs_));
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObUpdCtDef)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObUpdCtDef,ObDMLBaseCtDef));
  bool has_pkey_upd = (ddel_ctdef_ != nullptr);
  bool has_lock_def = (dlock_ctdef_ != nullptr);
  bool has_multi_def = (multi_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              full_row_,
              dupd_ctdef_,
              need_check_filter_null_,
              distinct_algo_,
              assign_columns_,
              distinct_key_,
              has_pkey_upd,
              has_lock_def,
              has_multi_def);
  if (has_pkey_upd) {
    OB_UNIS_ADD_LEN(*ddel_ctdef_);
    OB_UNIS_ADD_LEN(*dins_ctdef_);
  }
  if (has_lock_def) {
    OB_UNIS_ADD_LEN(*dlock_ctdef_);
  }
  if (has_multi_def) {
    OB_UNIS_ADD_LEN(*multi_ctdef_);
  }
  len += ObDASUtils::das_ctdefs_serialize_size(related_upd_ctdefs_);
  len += ObDASUtils::das_ctdefs_serialize_size(related_del_ctdefs_);
  len += ObDASUtils::das_ctdefs_serialize_size(related_ins_ctdefs_);
  return len;
}

OB_DEF_SERIALIZE(ObMergeCtDef)
{
  int ret = OB_SUCCESS;
  bool has_insert = (ins_ctdef_ != nullptr);
  bool has_update = (upd_ctdef_ != nullptr);
  bool has_delete = (del_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ENCODE,
              has_insert,
              has_update,
              has_delete);
  if (has_insert) {
    OB_UNIS_ENCODE(*ins_ctdef_);
  }

  if (has_update) {
    OB_UNIS_ENCODE(*upd_ctdef_);
  }

  if (has_delete) {
    OB_UNIS_ENCODE(*del_ctdef_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObMergeCtDef)
{
  int ret = OB_SUCCESS;
  bool has_insert = false;
  bool has_update = false;
  bool has_delete = false;
  LST_DO_CODE(OB_UNIS_DECODE,
              has_insert,
              has_update,
              has_delete);
  if (OB_SUCC(ret) && has_insert) {
    ObDMLCtDefAllocator<ObInsCtDef> ins_ctdef_allocator(alloc_);
    ins_ctdef_ = ins_ctdef_allocator.alloc();
    if (OB_ISNULL(ins_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ins_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*ins_ctdef_);
  }

  if (OB_SUCC(ret) && has_update) {
    ObDMLCtDefAllocator<ObUpdCtDef> upd_ctdef_allocator(alloc_);
    upd_ctdef_ = upd_ctdef_allocator.alloc();
    if (OB_ISNULL(upd_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc upd_ctdef_ failed", K(ret));
    }
    OB_UNIS_DECODE(*upd_ctdef_);
  }

  if (OB_SUCC(ret) && has_delete) {
    ObDMLCtDefAllocator<ObDelCtDef> ins_ctdef_allocator(alloc_);
    del_ctdef_ = ins_ctdef_allocator.alloc();
    if (OB_ISNULL(del_ctdef_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc del_ctdef_ failed", K(ret));
    }
    OB_UNIS_DECODE(*del_ctdef_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMergeCtDef)
{
  int64_t len = 0;
  bool has_insert = (ins_ctdef_ != nullptr);
  bool has_update = (upd_ctdef_ != nullptr);
  bool has_delete = (del_ctdef_ != nullptr);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              has_insert,
              has_update,
              has_delete);
  if (has_insert) {
    OB_UNIS_ADD_LEN(*ins_ctdef_);
  }
  if (has_update) {
    OB_UNIS_ADD_LEN(*upd_ctdef_);
  }
  if (has_delete) {
    OB_UNIS_ADD_LEN(*del_ctdef_);
  }
  return len;
}

// replace into serialize and deserialize
OB_DEF_SERIALIZE(ObReplaceCtDef)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(*ins_ctdef_);
  OB_UNIS_ENCODE(*del_ctdef_);
  return ret;
}
OB_DEF_DESERIALIZE(ObReplaceCtDef)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObInsCtDef> ins_ctdef_allocator(alloc_);
  ins_ctdef_ = ins_ctdef_allocator.alloc();
  ObDMLCtDefAllocator<ObDelCtDef> del_ctdef_allocator(alloc_);
  del_ctdef_ = del_ctdef_allocator.alloc();
  if (OB_ISNULL(del_ctdef_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc del_ctdef_ failed", K(ret));
  } else if (OB_ISNULL(ins_ctdef_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ins_ctdef failed", K(ret));
  }
  OB_UNIS_DECODE(*ins_ctdef_);
  OB_UNIS_DECODE(*del_ctdef_);
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObReplaceCtDef)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(*ins_ctdef_);
  OB_UNIS_ADD_LEN(*del_ctdef_);
  return len;
}

// insert up serialize and deserialize
OB_DEF_SERIALIZE(ObInsertUpCtDef)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(*ins_ctdef_);
  OB_UNIS_ENCODE(*upd_ctdef_);
  return ret;
}

OB_DEF_DESERIALIZE(ObInsertUpCtDef)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObInsCtDef> ins_ctdef_allocator(alloc_);
  ins_ctdef_ = ins_ctdef_allocator.alloc();
  ObDMLCtDefAllocator<ObUpdCtDef> upd_ctdef_allocator(alloc_);
  upd_ctdef_ = upd_ctdef_allocator.alloc();
  if (OB_ISNULL(upd_ctdef_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc upd_ctdef failed", K(ret));
  } else if (OB_ISNULL(ins_ctdef_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ins_ctdef failed", K(ret));
  }
  OB_UNIS_DECODE(*ins_ctdef_);
  OB_UNIS_DECODE(*upd_ctdef_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObInsertUpCtDef)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(*ins_ctdef_);
  OB_UNIS_ADD_LEN(*upd_ctdef_);
  return len;
}

ObDMLBaseRtDef::~ObDMLBaseRtDef()
{
  for (int64_t i = 0; i < fk_checker_array_.count(); ++i) {
    ObForeignKeyChecker *fk_checker = fk_checker_array_.at(i);
    if (OB_NOT_NULL(fk_checker)) {
      fk_checker->reset();
      fk_checker = nullptr;
    }
  }
  fk_checker_array_.release_array();
}
}  // namespace sql
}  // namespace oceanbase
