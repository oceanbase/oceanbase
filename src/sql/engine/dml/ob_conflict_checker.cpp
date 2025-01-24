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
#include "sql/engine/dml/ob_conflict_checker.h"
#include "sql/das/ob_das_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace share::schema;
namespace sql
{

int ObTabletSnapshotMaping::assign(const ObTabletSnapshotMaping& other)
{
  int ret = OB_SUCCESS;
  this->ls_id_ = other.ls_id_;
  this->tablet_id_ = other.tablet_id_;
  if (OB_FAIL(this->snapshot_.assign(other.snapshot_))) {
    LOG_WARN("assign snapshot fail", K(ret), K(other));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRowkeyCstCtdef,
                    constraint_name_,
                    rowkey_expr_,
                    calc_exprs_,
                    rowkey_accuracys_);

OB_DEF_SERIALIZE(ObConflictCheckerCtdef)
{
  int ret = OB_SUCCESS;
  int64_t cons_info_cnt = cst_ctdefs_.count();
  OB_UNIS_ENCODE(cons_info_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < cons_info_cnt; ++i) {
    ObRowkeyCstCtdef *cst_ctdef = cst_ctdefs_.at(i);
    if (OB_ISNULL(cst_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("constraint_info is nullptr", K(ret));
    }
    OB_UNIS_ENCODE(*cst_ctdef);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(calc_part_id_expr_);
    OB_UNIS_ENCODE(part_id_dep_exprs_);
    OB_UNIS_ENCODE(das_scan_ctdef_);
    OB_UNIS_ENCODE(partition_cnt_);
    OB_UNIS_ENCODE(data_table_rowkey_expr_);
    OB_UNIS_ENCODE(table_column_exprs_);
    OB_UNIS_ENCODE(use_dist_das_);
    OB_UNIS_ENCODE(rowkey_count_);
    OB_UNIS_ENCODE(attach_spec_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObConflictCheckerCtdef)
{
  int ret = OB_SUCCESS;
  int64_t cons_info_cnt = 0;
  OB_UNIS_DECODE(cons_info_cnt);
  ObDMLCtDefAllocator<ObRowkeyCstCtdef> cons_info_allocator(alloc_);
  OZ(cst_ctdefs_.init(cons_info_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < cons_info_cnt; ++i) {
    ObRowkeyCstCtdef *cst_ctdef = cons_info_allocator.alloc();
    if (OB_ISNULL(cst_ctdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc constraint_info failed", K(ret));
    }
    OB_UNIS_DECODE(*cst_ctdef);
    OZ(cst_ctdefs_.push_back(cst_ctdef));
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(calc_part_id_expr_);
    OB_UNIS_DECODE(part_id_dep_exprs_);
    OB_UNIS_DECODE(das_scan_ctdef_);
    OB_UNIS_DECODE(partition_cnt_);
    OB_UNIS_DECODE(data_table_rowkey_expr_);
    OB_UNIS_DECODE(table_column_exprs_);
    OB_UNIS_DECODE(use_dist_das_);
    OB_UNIS_DECODE(rowkey_count_);
    OB_UNIS_DECODE(attach_spec_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObConflictCheckerCtdef)
{
  int64_t len = 0;
  int64_t cons_info_cnt = cst_ctdefs_.count();
  OB_UNIS_ADD_LEN(cons_info_cnt);
  for (int64_t i = 0; i < cons_info_cnt; ++i) {
    ObRowkeyCstCtdef *cts_ctdef = cst_ctdefs_.at(i);
    if (cts_ctdef != nullptr) {
      OB_UNIS_ADD_LEN(*cts_ctdef);
    }
  }
  OB_UNIS_ADD_LEN(calc_part_id_expr_);
  OB_UNIS_ADD_LEN(part_id_dep_exprs_);
  OB_UNIS_ADD_LEN(das_scan_ctdef_);
  OB_UNIS_ADD_LEN(partition_cnt_);
  OB_UNIS_ADD_LEN(data_table_rowkey_expr_);
  OB_UNIS_ADD_LEN(table_column_exprs_);
  OB_UNIS_ADD_LEN(use_dist_das_);
  OB_UNIS_ADD_LEN(rowkey_count_);
  OB_UNIS_ADD_LEN(attach_spec_);
  return len;
}

bool ObTabletSnapshotMaping::operator==(const ObTabletSnapshotMaping &other) const
{
  return ls_id_ == other.ls_id_ && tablet_id_ == other.tablet_id_;
}


bool ObConflictValue::operator==(const ObConflictValue &other) const
{
  return current_datum_row_ == other.current_datum_row_;
}

int ObConflictRowMapCtx::reuse()
{
  int ret = OB_SUCCESS;
  if (conflict_map_.created()) {
    OZ(conflict_map_.reuse());
  }
  return ret;
}

int ObConflictRowMapCtx::destroy()
{
  int ret = OB_SUCCESS;
  if (conflict_map_.created()) {
    OZ(conflict_map_.destroy());
  }
  return ret;
}

int ObConflictRowMapCtx::init_conflict_map(int64_t replace_row_cnt, int64_t rowkey_cnt, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator used to init conflict map is null", K(ret));
  } else {
    allocator_ = allocator;
  }
  if (OB_SUCC(ret) && !conflict_map_.created()) {
    ObObj *objs = NULL;
    int64_t bucket_num = 0;
    bucket_num = replace_row_cnt < MAX_ROW_BATCH_SIZE ? replace_row_cnt : MAX_ROW_BATCH_SIZE;
    // map 没创建的场景下才需要创建, 这里可能被重复调用
    if (NULL == (rowkey_ = static_cast<ObRowkey*>(allocator_->alloc(sizeof(ObRowkey))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (NULL == (objs = static_cast<ObObj*>(allocator_->alloc(sizeof(ObObj)* rowkey_cnt)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_FAIL(conflict_map_.create(bucket_num, ObModIds::OB_HASH_BUCKET))) {
      LOG_WARN("fail to create conflict map", K(ret));
    } else {
      rowkey_->assign(objs, rowkey_cnt);
    }
  }
  return ret;
}

ObConflictChecker::ObConflictChecker(common::ObIAllocator &allocator,
                                     ObEvalCtx &eval_ctx,
                                     const ObConflictCheckerCtdef &checker_ctdef)
  : eval_ctx_(eval_ctx),
    checker_ctdef_(checker_ctdef),
    das_scan_rtdef_(),
    attach_rtinfo_(nullptr),
    allocator_(allocator),
    das_ref_(eval_ctx, eval_ctx.exec_ctx_),
    local_tablet_loc_(nullptr),
    table_loc_(nullptr),
    tmp_mem_ctx_(),
    snapshot_maping_(),
    conflict_range_dist_ctx_(nullptr)
{
}

int ObConflictChecker::create_rowkey_check_hashset(int64_t replace_row_cnt)
{
  int ret = OB_SUCCESS;
  ConflictRangeDistCtx *conflict_range_dist_ctx = nullptr;
  if (OB_ISNULL(conflict_range_dist_ctx_)) {
    void *buf = allocator_.alloc(sizeof(ConflictRangeDistCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), "size", sizeof(SeRowkeyDistCtx));
    } else {
      conflict_range_dist_ctx = new (buf) ConflictRangeDistCtx();
      const int64_t max_bucket_num = replace_row_cnt > MAX_ROWKEY_CHECKER_DISTINCT_BUCKET_NUM ?
          MAX_ROWKEY_CHECKER_DISTINCT_BUCKET_NUM : replace_row_cnt;
      if (OB_FAIL(conflict_range_dist_ctx->create(max_bucket_num,
                                          "DmlConflictDisBu",
                                          "DmlConflictDisNo",
                                          MTL_ID()))) {
        LOG_WARN("create rowkey distinct context failed", K(ret), "rows", replace_row_cnt, K(max_bucket_num));
      } else {
        conflict_range_dist_ctx_ = conflict_range_dist_ctx;
      }
    }
  }
  return ret;
}

//初始conflict_map
int ObConflictChecker::create_conflict_map(int64_t replace_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t constraint_cnt = checker_ctdef_.cst_ctdefs_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < constraint_cnt; ++i) {
    ObRowkeyCstCtdef *rowkey_cst_ctdef = checker_ctdef_.cst_ctdefs_.at(i);
    int64_t rowkey_cnt = rowkey_cst_ctdef->rowkey_expr_.count();
    // 在init_conflict_map 函数内保证了map不会被重复created
    if (OB_FAIL(conflict_map_array_.at(i).init_conflict_map(replace_row_cnt, rowkey_cnt, &allocator_))) {
      LOG_WARN("fail to init conflict_map", K(ret), K(rowkey_cnt));
    }
  }
  return ret;
}

//初始化map array， map创建hash_bucket将会在延后
int ObConflictChecker::init_conflict_checker(const ObExprFrameInfo *expr_frame_info,
                                             ObDASTableLoc *table_loc,
                                             bool use_partition_gts_opt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = eval_ctx_.exec_ctx_.get_my_session();
  int64_t constraint_cnt = checker_ctdef_.cst_ctdefs_.count();
  table_loc_ = table_loc;
  OZ(conflict_map_array_.allocate_array(allocator_, constraint_cnt), constraint_cnt);
  if (OB_SUCC(ret)) {
    ObMemAttr mem_attr;
    mem_attr.tenant_id_ = session->get_effective_tenant_id();
    mem_attr.label_ = "SqlConflictCkr";
    das_ref_.set_expr_frame_info(expr_frame_info);
    // 这里需要注意
    das_ref_.set_execute_directly(!checker_ctdef_.use_dist_das_);
    das_ref_.set_mem_attr(mem_attr);
    das_ref_.set_do_gts_opt(use_partition_gts_opt);
  }
  OZ(init_das_scan_rtdef());
  return ret;
}

int ObConflictChecker::get_tmp_string_buffer(ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = nullptr;
  if (OB_ISNULL(tmp_mem_ctx_)) {
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), "ConflictRowkey", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(tmp_mem_ctx_, param))) {
      LOG_WARN("create conflict rowkey checker context entity failed", K(ret));
    }
  } else {
    tmp_mem_ctx_->reset_remain_one_page();
  }
  if (OB_SUCC(ret)) {
    allocator = &tmp_mem_ctx_->get_allocator();
  }
  return ret;
}

int ObConflictChecker::build_rowkey(ObRowkey *&rowkey,
                                    ObRowkeyCstCtdef *rowkey_info)
{
  int ret = OB_SUCCESS;
  ObObj *objs = NULL;
  int64_t rowkey_cnt = rowkey_info->rowkey_expr_.count();
  ObIAllocator &alloc = das_ref_.get_das_alloc();
  ObIAllocator *tmp_string_buffer = nullptr;

  if (NULL == (rowkey = static_cast<ObRowkey*>(alloc.alloc(sizeof(ObRowkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (NULL ==
      (objs = static_cast<ObObj*>(das_ref_.get_das_alloc().alloc(sizeof(ObObj)* rowkey_cnt)))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(rowkey_info->calc_exprs_, eval_ctx_))) {
    LOG_WARN("fail to clear rowkey flag", K(ret), K(rowkey_info->calc_exprs_));
  } else if (OB_FAIL(get_tmp_string_buffer(tmp_string_buffer))) {
    LOG_WARN("get tmp string buffer failed", K(ret));
  }

  for (int i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObDatum *datum = NULL;
    ObObj tmp_obj;
    const ObObjMeta &col_obj_meta = rowkey_info->rowkey_expr_.at(i)->obj_meta_;
    ObExpr *expr = rowkey_info->rowkey_expr_.at(i);
    const ObAccuracy *col_accuracy = nullptr;

    if (rowkey_info->rowkey_accuracys_.count() == rowkey_cnt) {
      //To maintain compatibility with older versions,
      //reshape_storage_value is only performed when rowkey_accuracys is not empty.
      col_accuracy = &rowkey_info->rowkey_accuracys_.at(i);
    }

    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i));
    } else if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
      LOG_WARN("expr eval fail", K(ret), K(i), KPC(expr));
    }
    // 这里对datum要做浅拷贝，因为此时的datum来自于从存储层table_scan出来的数据,
    // 读后续行的时候会覆盖前边的数据，所以这里一定要做深拷贝
    else if (OB_FAIL(datum->to_obj(tmp_obj, col_obj_meta))) {
      LOG_WARN("datum to obj fail", K(ret), K(i), KPC(expr), KPC(datum));
    } else if (col_accuracy != nullptr &&
        OB_FAIL(ObDASUtils::reshape_storage_value(col_obj_meta,
                                                  *col_accuracy,
                                                  *tmp_string_buffer,
                                                  tmp_obj))) {
      LOG_WARN("reshape storage value failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(alloc, tmp_obj, objs[i]))) {
      LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
    }
  }

  if (OB_SUCC(ret)) {
    rowkey->assign(objs, rowkey_cnt);
    LOG_DEBUG("succeed to build rowkey", KPC(rowkey));
  }

  return ret;
}

int ObConflictChecker::build_tmp_rowkey(ObRowkey *rowkey, ObRowkeyCstCtdef *rowkey_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rowkey is null", K(ret));
  } else if (OB_ISNULL(rowkey_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rowkey_info is null", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObObj *obj_ptr = rowkey->get_obj_ptr();
    ObIAllocator *tmp_string_buffer = nullptr;

    if (OB_ISNULL(obj_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("obj_ptr is null", K(ret));
    } else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(rowkey_info->calc_exprs_, eval_ctx_))) {
      LOG_WARN("clear eval flag failed", K(ret), K(rowkey_info->calc_exprs_));
    } else if (OB_FAIL(get_tmp_string_buffer(tmp_string_buffer))) {
      LOG_WARN("get tmp string buffer failed", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < rowkey_info->rowkey_expr_.count(); ++i) {
      ObDatum *datum = NULL;
      const ObObjMeta &col_obj_meta = rowkey_info->rowkey_expr_.at(i)->obj_meta_;
      ObExpr *expr = rowkey_info->rowkey_expr_.at(i);
      const ObAccuracy *col_accuracy = nullptr;

      if (rowkey_info->rowkey_accuracys_.count() == rowkey_info->rowkey_expr_.count()) {
        //To maintain compatibility with older versions,
        //reshape_storage_value is only performed when rowkey_accuracys is not empty.
        col_accuracy = &rowkey_info->rowkey_accuracys_.at(i);
      }
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(i));
      } else if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
        LOG_WARN("expr eval fail", K(ret), K(i), KPC(expr));
      } else if (OB_FAIL(datum->to_obj(obj_ptr[i], col_obj_meta))) {
        LOG_WARN("datum to obj fail", K(ret), K(i), KPC(expr), KPC(datum));
      } else if (col_accuracy != nullptr &&
          OB_FAIL(ObDASUtils::reshape_storage_value(col_obj_meta,
                                                    *col_accuracy,
                                                    *tmp_string_buffer,
                                                    obj_ptr[i]))) {
        LOG_WARN("reshape storage value failed", K(ret));
      } else {
        LOG_DEBUG("succ to build tmp rowkey obj", K(i), K(obj_ptr[i]));
      }
    }
  }

  return ret;
}

// 用回表拉回的数据来构建map
int ObConflictChecker::build_base_conflict_map(
    int64_t replace_row_cnt,
    const ObChunkDatumStore::StoredRow *conflict_row)
{
  int ret = OB_SUCCESS;
  bool is_duplicated = false;
  if (replace_row_cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace_row_cnt is unexpected", K(ret), K(replace_row_cnt));
  } else if (OB_ISNULL(conflict_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict_row is unexpected", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && !is_duplicated && i < checker_ctdef_.cst_ctdefs_.count(); ++i) {
    ObRowkey *rowkey = nullptr;
    if (OB_FAIL(build_rowkey(rowkey, checker_ctdef_.cst_ctdefs_.at(i)))) {
      LOG_WARN("fail to build rowkey", K(ret));
    } else if (OB_ISNULL(rowkey)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey is null", K(ret));
    } else {
      ObConflictValue constraint_value;
      constraint_value.baseline_datum_row_ = conflict_row;
      constraint_value.current_datum_row_ = conflict_row;
      constraint_value.new_row_source_ = ObNewRowSource::FROM_SCAN;
      if (OB_FAIL(conflict_map_array_.at(i).conflict_map_.set_refactored(*rowkey, constraint_value))) {
        if (OB_HASH_EXIST == ret && 0 == i) {
          // 回表查出来的结果很可能有相同主表行
          // create table t1 (c1 int primary key, c2 int unique) partition by hash(c1) ....;
          // insert into t1 values(1,1),(2,2),(3,3),(4,4);
          // replace into t1 values(1,1);
          // 此时主表和索引表冲突的行都是主表中(1,1)行
          // 所以这里跳过duplicated row
          is_duplicated = true;
          ret = OB_SUCCESS;
        } else {
          // 如果主键不相同，但是后边有唯一性索引相同，绝对的不符合预期
          LOG_WARN("set constraint key failed", K(ret), K(i));
        }
      }
      LOG_DEBUG("build base line conflict infos", K(ret), K(i),
                KPC(rowkey), K(constraint_value), K(is_duplicated),
                "conflict row", ROWEXPR2STR(eval_ctx_, checker_ctdef_.table_column_exprs_));
    }
  }
  return ret;
}

// 检查当前的主键是否冲突
int ObConflictChecker::check_duplicate_rowkey(const ObChunkDatumStore::StoredRow *replace_row,
                                              ObIArray<ObConflictValue> &constraint_values,
                                              bool is_insert_up)
{
  int ret = OB_SUCCESS;
  bool is_break = false;
  if (OB_FAIL(to_expr(replace_row))) {
    LOG_WARN("do to_expr failed", K(ret), KPC(replace_row));
  } else {
    LOG_DEBUG("check one row whether duplicated in hash_map", K(ret),
              "check row", ROWEXPR2STR(eval_ctx_, checker_ctdef_.table_column_exprs_));
  }
  int64_t N = checker_ctdef_.cst_ctdefs_.count();
  for (int64_t i = 0; OB_SUCC(ret) && !is_break && i < N; ++i) {
    ObRowkeyCstCtdef *rowkey_cst_ctdef = checker_ctdef_.cst_ctdefs_.at(i);
    ObRowkey *constraint_key = nullptr;
    ObConflictValue constraint_value;
    ObConflictRowMapCtx &map_ctx = conflict_map_array_.at(i);
    constraint_key = map_ctx.rowkey_;
    if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(rowkey_cst_ctdef->calc_exprs_, eval_ctx_))) {
      LOG_WARN("fail to clear constraint eval flag", K(ret), K(rowkey_cst_ctdef->calc_exprs_));
    } else if (OB_FAIL(build_tmp_rowkey(constraint_key, rowkey_cst_ctdef))) {
      LOG_WARN("fail to build_tmp_rowkey", K(ret));
    } else if (OB_FAIL(map_ctx.conflict_map_.get_refactored(*constraint_key, constraint_value))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get duplicated row from constraint contexts failed", K(ret), KPC(constraint_key));
      } else {
        ret = OB_SUCCESS;
      }
    }

    // todo @kaizhan.dkz 这里需要检查是否需要update_incremental_row_检查，
    // insert_up才需要，replace 暂时用不到
    if (OB_SUCC(ret) && constraint_value.current_datum_row_ != NULL) {
      if (OB_FAIL(add_var_to_array_no_dup(constraint_values, constraint_value))) {
        LOG_WARN("add constraint value no duplicate failed", K(ret));
      } else if (is_insert_up) {
        //is_insert_up = true:
        // only need one duplicated row(compatible with MySQL insert_up)
        //is_insert_up = false:
        // need all duplicated row(compatible with MySQL replace)
        is_break = true;
      }
    }
    LOG_DEBUG("check duplicate rowkey", K(ret), K(is_insert_up),
              K(constraint_key), K(constraint_value),
              "check row", ROWEXPR2STR(eval_ctx_, checker_ctdef_.table_column_exprs_));
  }

  return ret;
}

// 从hash map中删除冲突base行
int ObConflictChecker::delete_old_row(const ObChunkDatumStore::StoredRow *replace_row,
                                      ObNewRowSource from)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(to_expr(replace_row))) {
    LOG_WARN("do to_expr failed", K(ret), KPC(replace_row));
  } else {
    LOG_DEBUG("delete one row from hash_map", K(ret),
              "delete row", ROWEXPR2STR(eval_ctx_, checker_ctdef_.table_column_exprs_));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < checker_ctdef_.cst_ctdefs_.count(); ++i) {
    ObRowkey *constraint_key;
    ObConflictValue *constraint_value = NULL;
    ObRowkeyCstCtdef *rowkey_cst_ctdef = checker_ctdef_.cst_ctdefs_.at(i);
    ObConflictRowMapCtx &map_ctx = conflict_map_array_.at(i);
    constraint_key = map_ctx.rowkey_;
    if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(rowkey_cst_ctdef->calc_exprs_, eval_ctx_))) {
      LOG_WARN("fail to clear constraint_info eval flag", K(ret));
    } else if (OB_FAIL(build_tmp_rowkey(constraint_key, rowkey_cst_ctdef))) {
      LOG_WARN("fail to build_tmp_rowkey", K(ret));
    } else {
      constraint_value = const_cast<ObConflictValue*>(map_ctx.conflict_map_.get(*constraint_key));
      if (OB_ISNULL(constraint_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constraint_value is unexpected", K(ret));
      } else {
        constraint_value->current_datum_row_ = NULL;
        constraint_value->new_row_source_ = from;
      }
    }
  }
  return ret;
}

// 插入新行到hash map中
int ObConflictChecker::insert_new_row(const ObChunkDatumStore::StoredRow *new_row,
                                      ObNewRowSource from)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(to_expr(new_row))) {
    LOG_WARN("do to_expr failed", K(ret), KPC(new_row));
  } else {
    LOG_DEBUG("insert one row to hash_map", K(ret),
              "insert row", ROWEXPR2STR(eval_ctx_, checker_ctdef_.table_column_exprs_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < checker_ctdef_.cst_ctdefs_.count(); ++i) {
    ObRowkey *constraint_key = nullptr;
    ObConflictValue *constraint_value = NULL;
    ObRowkeyCstCtdef *rowkey_cst_ctdef = checker_ctdef_.cst_ctdefs_.at(i);
    ObConflictRowMapCtx &map_ctx = conflict_map_array_.at(i);
    ObRowkey *insert_rowkey = nullptr;
    constraint_key = map_ctx.rowkey_;
    if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(rowkey_cst_ctdef->calc_exprs_, eval_ctx_))) {
      LOG_WARN("fail to clear eval flag", K(ret));
    } else if (OB_FAIL(build_tmp_rowkey(constraint_key, rowkey_cst_ctdef))) {
      LOG_WARN("fail to build tmp rowkey", K(ret), KPC(rowkey_cst_ctdef));
    } else if (FALSE_IT(constraint_value =
                        const_cast<ObConflictValue*>(map_ctx.conflict_map_.get(*constraint_key)))) {

    } else if (constraint_value != NULL) {
      if (OB_NOT_NULL(constraint_value->current_datum_row_)) {
        // insert up 在update之后的行仍然是存在唯一约束冲突，在这里模拟存储层报错
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        char rowkey_buffer[OB_TMP_BUF_SIZE_256];
        if (OB_SUCCESS != (extract_rowkey_info(rowkey_cst_ctdef,
                                               rowkey_buffer,
                                               OB_TMP_BUF_SIZE_256))) {
          LOG_WARN("extract rowkey info failed", K(ret), KPC(rowkey_cst_ctdef), K(new_row));
        } else {
          const ObString &constraint_name = rowkey_cst_ctdef->constraint_name_;
          LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer,
                       constraint_name.length(), constraint_name.ptr());
        }
      } else {
        // map中命中，说明base行被删除了，然后插入新行
        constraint_value->current_datum_row_ = new_row;
        constraint_value->new_row_source_ = from;
        LOG_DEBUG("add one row to hash map and current_datum_row_ is null",
                  K(i), KPC(constraint_key), KPC(constraint_value));
      }
    } else {
      // 真实插入新行到map
      ObConflictValue new_constraint_value;
      new_constraint_value.current_datum_row_ = new_row;
      new_constraint_value.new_row_source_ = from;
      if (OB_FAIL(build_rowkey(insert_rowkey, rowkey_cst_ctdef))) {
        LOG_WARN("fail to build insert_rowkey rowkey", K(ret), K(i), KPC(rowkey_cst_ctdef));
      } else if (OB_ISNULL(insert_rowkey)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert rowkey is null", K(ret), K(i), KPC(rowkey_cst_ctdef));
      } else if (OB_FAIL(map_ctx.conflict_map_.set_refactored(*insert_rowkey, new_constraint_value))) {
        LOG_WARN("insert to map failed", K(ret), K(i), KPC(insert_rowkey));
      } else {
        LOG_DEBUG("real add one row to hash_map", K(i), KPC(insert_rowkey), K(new_constraint_value));
      }
    }
  }
  return ret;
}

int ObConflictChecker::update_row(const ObChunkDatumStore::StoredRow *new_row,
                                  const ObChunkDatumStore::StoredRow *old_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_old_row(old_row, ObNewRowSource::FROM_INSERT))) {
    LOG_WARN("fail to delete old_row", K(ret), KPC(old_row));
  } else if (OB_FAIL(insert_new_row(new_row, ObNewRowSource::FROM_UPDATE))) {
    LOG_WARN("fail to insert new_row", K(ret), KPC(new_row));
  }
  return ret;
}

int ObConflictChecker::lock_row(const ObChunkDatumStore::StoredRow *lock_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(to_expr(lock_row))) {
    LOG_WARN("do to_expr failed", K(ret), KPC(lock_row));
  } else {
    LOG_DEBUG("lock one row from hash_map", K(ret),
              "lock row", ROWEXPR2STR(eval_ctx_, checker_ctdef_.table_column_exprs_));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < checker_ctdef_.cst_ctdefs_.count(); ++i) {
    ObRowkey *constraint_key;
    ObConflictValue *constraint_value = NULL;
    ObRowkeyCstCtdef *rowkey_cst_ctdef = checker_ctdef_.cst_ctdefs_.at(i);
    ObConflictRowMapCtx &map_ctx = conflict_map_array_.at(i);
    constraint_key = map_ctx.rowkey_;
    if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(rowkey_cst_ctdef->calc_exprs_, eval_ctx_))) {
      LOG_WARN("fail to clear constraint_info eval flag", K(ret));
    } else if (OB_FAIL(build_tmp_rowkey(constraint_key, rowkey_cst_ctdef))) {
      LOG_WARN("fail to build_tmp_rowkey", K(ret));
    } else {
      // get one duplicate row, this row must current_datum_row_ == baseline_datum_row_ and
      // new_row_source_ is FROM_SCAN, can do row lock
      constraint_value = const_cast<ObConflictValue*>(map_ctx.conflict_map_.get(*constraint_key));
      if (OB_ISNULL(constraint_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constraint_value is unexpected", K(ret));
      } else if (constraint_value->new_row_source_ == ObNewRowSource::FROM_SCAN) {
        if (constraint_value->current_datum_row_ != constraint_value->baseline_datum_row_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("curr_datum_row is not equal with baseline_datum_row", K(ret), K(constraint_value));
        } else if (OB_ISNULL(constraint_value->baseline_datum_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("baseline_datum_row is null", K(ret), K(constraint_value));
        } else {
          constraint_value->new_row_source_ = ObNewRowSource::NEED_DO_LOCK;
        }
      } else {
        // create table t1 (c1 int primary key, c2 int unique, c3 int);
        // insert into t1 values(1,1,1);
        // insert into t1 values(1,1,1),(2,2,2) on duplicate key update c1 = 2, c2 = 2, c3 = 2;
        // (1,1,1) will do update --> (2,2,2), then insert (2,2,2) will do update, but row not changed
        // (2,2,2) in hash_map don't do lock, but do ins_upd_new_row
      }
    }
  }
  return ret;
}

int ObConflictChecker::convert_exprs_to_stored_row(const ObExprPtrIArray &exprs,
                                                   ObChunkDatumStore::StoredRow *&new_row)
{
  return ObChunkDatumStore::StoredRow::build(new_row, exprs, eval_ctx_, das_ref_.get_das_alloc());
}

int ObConflictChecker::close()
{
  int ret = OB_SUCCESS;
  // close回表的task
  if (das_ref_.has_task()) {
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all das task failed", K(ret));
    }
  }
  return ret;
}

int ObConflictChecker::reuse()
{
  int ret = OB_SUCCESS;
  if (das_ref_.has_task()) {
    if (OB_FAIL(das_ref_.close_all_task())) {
      LOG_WARN("close all insert das task failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    das_ref_.reuse();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < conflict_map_array_.count(); ++i) {
    if (OB_FAIL(conflict_map_array_.at(i).reuse())) {
      LOG_WARN("fail to reuse conflict_map", K(ret), K(i));
    }
  }
  if (tmp_mem_ctx_ != nullptr) {
    tmp_mem_ctx_->reset_remain_one_page();
  }
  snapshot_maping_.reuse();
  if (conflict_range_dist_ctx_ != nullptr) {
    conflict_range_dist_ctx_->reuse();
  }
  return ret;
}

int ObConflictChecker::destroy()
{
  int ret = OB_SUCCESS;
  // 在这里析构 conflict_map_array_和das_scan_rtdef_
  for (int64_t i = 0; OB_SUCC(ret) && i < conflict_map_array_.count(); ++i) {
    if (OB_FAIL(conflict_map_array_.at(i).destroy())) {
      LOG_WARN("fail to destroy conflict_map", K(ret), K(i));
    }
  }
  das_ref_.reset();
  das_scan_rtdef_.~ObDASScanRtDef();
  if (tmp_mem_ctx_ != nullptr) {
    DESTROY_CONTEXT(tmp_mem_ctx_);
    tmp_mem_ctx_ = nullptr;
  }
  snapshot_maping_.reset();
  if (conflict_range_dist_ctx_ != nullptr) {
    conflict_range_dist_ctx_->destroy();
    conflict_range_dist_ctx_ = nullptr;
  }
  return ret;
}

int ObConflictChecker::add_lookup_range_no_dup(storage::ObTableScanParam &scan_param,
                                               ObNewRange &lookup_range,
                                               common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(conflict_range_dist_ctx_)) {
    ObConflictRange conflict_range;
    conflict_range.init_conflict_range(lookup_range.get_start_key(), tablet_id);
    ret = conflict_range_dist_ctx_->exist_refactored(conflict_range);
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("print find unique", K(lookup_range), K(tablet_id));
    } else if (OB_HASH_NOT_EXIST == ret) {
      //step3: if not exist, deep copy data and add ObRowkey to hash set
      //step3.1: Init the buffer of ObObj Array
      ret = OB_SUCCESS;
      if (OB_FAIL(conflict_range_dist_ctx_->set_refactored(conflict_range))) {
        LOG_WARN("set rowkey item failed", K(ret));
      } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
        LOG_WARN("push_back lookup_range failed", K(ret), K(lookup_range));
      } else {
        LOG_TRACE("print add lookup_range succ", K(conflict_range));
      }
    } else {
      LOG_WARN("check if rowkey item exists failed", K(ret));
    }
  } else if (OB_FAIL(add_var_to_array_no_dup(scan_param.key_ranges_, lookup_range))) {
    LOG_WARN("store lookup key range failed", K(ret), K(lookup_range), K(scan_param));
  }
  return ret;
}

// todo @kaizhan.dkz 向主表执行回表操作，返回主表中冲突的行
int ObConflictChecker::build_primary_table_lookup_das_task()
{
  int ret = OB_SUCCESS;
  ObDASScanOp *das_scan_op = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObNewRange lookup_range;

  // data_table_rowkey_expr_ 是column_ref expr
  if (OB_FAIL(calc_lookup_tablet_loc(tablet_loc))) {
    LOG_WARN("calc lookup pkey fail", K(ret));
  } else if (OB_FAIL(get_das_scan_op(tablet_loc, das_scan_op))) {
    LOG_WARN("get_das_scan_op failed", K(ret), K(tablet_loc));
  } else if (OB_ISNULL(das_scan_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das_scan_op should be not null", K(ret));
  } else {
    storage::ObTableScanParam &scan_param = das_scan_op->get_scan_param();
    ObRowkey table_rowkey;
    if (OB_FAIL(build_data_table_range(lookup_range, table_rowkey))) {
      LOG_WARN("build data table range failed", K(ret), KPC(tablet_loc));
    } else if (OB_FAIL(add_lookup_range_no_dup(scan_param, lookup_range, tablet_loc->tablet_id_))) {
      LOG_WARN("store lookup key range failed", K(ret), K(table_rowkey), K(scan_param));
    } else {
      LOG_TRACE("after build conflict rowkey", K(scan_param.tablet_id_),
                K(scan_param.key_ranges_.count()), K(lookup_range), K(table_rowkey));
    }
  }
  return ret;
}

int ObConflictChecker::to_expr(const ObChunkDatumStore::StoredRow *replace_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replace_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace_row is null", K(ret));
  } else if (OB_FAIL(replace_row->to_expr(checker_ctdef_.table_column_exprs_,
                                          eval_ctx_,
                                          checker_ctdef_.table_column_exprs_.count()))) {
    LOG_WARN("fail to do to_expr", K(ret), KPC(replace_row),
             K(checker_ctdef_.table_column_exprs_.count()));
  }
  return ret;
}

int ObConflictChecker::calc_lookup_tablet_loc(ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  ObExpr *part_id_expr = NULL;
  ObTabletID tablet_id;
  ObObjectID partition_id = OB_INVALID_ID;
  tablet_loc = nullptr;
  if (checker_ctdef_.use_dist_das_) {
    // data_table_rowkey_expr_ 是column_ref expr
    if (OB_ISNULL(part_id_expr = checker_ctdef_.calc_part_id_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("calc_part_id_expr_ is null", K(ret));
    } // 清除回表使用的分区计算表达式的eval flag
      else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(checker_ctdef_.part_id_dep_exprs_, eval_ctx_))) {
      LOG_WARN("fail to clear rowkey flag", K(ret), K(checker_ctdef_.part_id_dep_exprs_));
    } else if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(part_id_expr, eval_ctx_, partition_id, tablet_id))) {
      LOG_WARN("fail to calc part id", K(ret), KPC(part_id_expr));
    } else if (OB_FAIL(DAS_CTX(das_ref_.get_exec_ctx()).extended_tablet_loc(*table_loc_, tablet_id, tablet_loc))) {
      LOG_WARN("extended tablet loc failed", K(ret));
    }
  } else {
    tablet_loc = local_tablet_loc_;
  }
  return ret;
}

// get当前行对应的scan_op
int ObConflictChecker::get_das_scan_op(ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_scan_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!das_ref_.has_das_op(tablet_loc, das_scan_op))) {
    if (OB_FAIL(das_ref_.prepare_das_task(tablet_loc, das_scan_op))) {
      LOG_WARN("prepare das task failed", K(ret));
    } else {
      das_scan_op->set_scan_ctdef(&checker_ctdef_.das_scan_ctdef_);
      das_scan_op->set_scan_rtdef(&das_scan_rtdef_);
      table_loc_->is_reading_ = true; //mark the table location with reading action
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(attach_rtinfo_)) {
      if (OB_FAIL(das_scan_op->reserve_related_buffer(attach_rtinfo_->related_scan_cnt_))) {
        LOG_WARN("fail to reserve related buffer", K(ret), K(attach_rtinfo_->related_scan_cnt_));
      } else if (OB_FAIL(attach_related_taskinfo(*das_scan_op, attach_rtinfo_->attach_rtdef_))) {
        LOG_WARN("fail to attach related task info", K(ret));
      } else {
        das_scan_op->set_attach_ctdef(checker_ctdef_.attach_spec_.attach_ctdef_);
        das_scan_op->set_attach_rtdef(attach_rtinfo_->attach_rtdef_);
      }
    }
  }
  return ret;
}

int ObConflictChecker::build_data_table_range(ObNewRange &lookup_range, ObRowkey &table_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  int64_t rowkey_cnt = checker_ctdef_.rowkey_count_;
  if (OB_ISNULL(buf = das_ref_.get_das_alloc().alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new(buf) ObObj[rowkey_cnt];
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObObj tmp_obj;
    ObExpr *expr = checker_ctdef_.data_table_rowkey_expr_.at(i);
    ObDatum *col_datum = nullptr;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in rowkey is nullptr", K(ret), K(i));
    } else if (OB_FAIL(expr->eval(eval_ctx_, col_datum))) {
      LOG_WARN("failed to evaluate expr in rowkey", K(ret), K(i));
    } else if (OB_ISNULL(col_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("evaluated column datum in rowkey is nullptr", K(ret), K(i));
    } else if (OB_FAIL(col_datum->to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    }
    // 这里需要做深拷贝
    else if (OB_FAIL(ob_write_obj(das_ref_.get_das_alloc(), tmp_obj, obj_ptr[i]))) {
      LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
    }
  }
  if (OB_SUCC(ret)) {
    table_rowkey.assign(obj_ptr, rowkey_cnt);
    uint64_t ref_table_id = checker_ctdef_.das_scan_ctdef_.ref_table_id_;
    if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
    } else {
      LOG_DEBUG("after build range", K(table_rowkey), K(lookup_range));
    }
  }
  return ret;
}

// lookup from primary table
int ObConflictChecker::do_lookup_and_build_base_map(int64_t replace_row_cnt)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, start_fetch_conflict_row);
  const ExprFixedArray &storage_output = checker_ctdef_.das_scan_ctdef_.pd_expr_spec_.access_exprs_;
  if (OB_FAIL(post_all_das_scan_tasks())) {
    LOG_WARN("execute all delete das task failed", K(ret));
  } else {
    DASOpResultIter result_iter = das_ref_.begin_result_iter();
    if (OB_FAIL(create_conflict_map(replace_row_cnt))) {
      LOG_WARN("create conflict map failed", K(ret));
    }
    ObChunkDatumStore::StoredRow *conflict_row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(get_next_row_from_data_table(result_iter, conflict_row))) {
      if (OB_FAIL(build_base_conflict_map(replace_row_cnt, conflict_row))) {
        LOG_WARN("build conflict map failed", K(ret));
      }
    }
    ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  }
  return ret;
}

int ObConflictChecker::get_next_row_from_data_table(DASOpResultIter &result_iter,
                                                    ObChunkDatumStore::StoredRow *&conflict_row)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  const ExprFixedArray &storage_output = checker_ctdef_.table_column_exprs_;
  while (OB_SUCC(ret) && !got_row) {
    das_scan_rtdef_.p_pd_expr_op_->clear_datum_eval_flag();
    if (OB_FAIL(result_iter.get_next_row())) {
      if (OB_ITER_END == ret) {
        if (OB_FAIL(result_iter.next_result())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fetch next task failed", K(ret));
          }
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else if (OB_FAIL(convert_exprs_to_stored_row(storage_output, conflict_row))) {
      LOG_WARN("convert expr to stored row failed", K(ret),
               "lookup one row", ROWEXPR2STR(eval_ctx_, storage_output));
    } else {
      got_row = true;
      // 这里打印回表拿到的行信息
      LOG_DEBUG("success to get row from data_table", K(ret), K(storage_output),
                "lookup one row", ROWEXPR2STR(eval_ctx_, storage_output));
    }
  }
  return ret;
}


// 主表的ObRowkeyCstCtdef就是主表主键的表达式
// unique索引表的主键组成: unique column + shadow_pk
int ObConflictChecker::extract_rowkey_info(const ObRowkeyCstCtdef *constraint_info,
                                           char *buf,
                                           int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObDatum *r_datum = NULL;
  ObObj r_obj;
  int64_t unique_key_cnt = 0;
  int64_t pos = 0;
  ObSQLSessionInfo *session = eval_ctx_.exec_ctx_.get_my_session();
  const ObRowkeyCstCtdef *primary_cst_info =checker_ctdef_.cst_ctdefs_.at(0);
  int64_t primary_rowkey_cnt = primary_cst_info->rowkey_expr_.count();
  if (constraint_info == primary_cst_info) {
    unique_key_cnt = primary_rowkey_cnt;
  } else {
    // unique索引表需要排除shadow_pk
    // shadow_pk的位置顺序在主键最后边
    unique_key_cnt = constraint_info->rowkey_expr_.count() - primary_rowkey_cnt;
  }

  if (unique_key_cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unique_key_cnt is unexpected", K(unique_key_cnt), K(primary_rowkey_cnt), K(constraint_info));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_cnt; i++) {
    ObExpr *expr =  constraint_info->rowkey_expr_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(expr->eval(eval_ctx_, r_datum))) {
      LOG_WARN("expr eval failed", K(ret), K(expr));
    } else if (OB_ISNULL(r_datum)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("after eval r_datum is null", K(ret), K(expr));
    } else if (OB_FAIL(r_datum->to_obj(r_obj, expr->obj_meta_))) {
      LOG_WARN("r_datum to obj failed", K(ret), KPC(r_datum));
    } else if (OB_FAIL(r_obj.print_plain_str_literal(buf, buf_len - 1, pos, session->get_timezone_info()))) {
      LOG_WARN("print obj failed", K(ret), K(r_obj));
    } else if (i == unique_key_cnt - 1) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf,  buf_len - 1, pos, "-"))) {
      LOG_WARN("print - failed", K(ret), K(pos));
    }
  }

  if (OB_SUCC(ret)) {
    ObString rowkey_info(buf);
    LOG_DEBUG("after extract rowkey info", K(rowkey_info));
  }
  return ret;
}
//这个函数类似于shuffle_final_data，只不过这里只是返回主表的hash map的指针，外层函数迭代map，将行分别插入对应的das task
int ObConflictChecker::get_primary_table_map(ObConflictRowMap *&primary_map)
{
  int ret = OB_SUCCESS;
  primary_map = NULL;
  CK(conflict_map_array_.count() > 0)
  if (conflict_map_array_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("conflict map count must greater than 0", K(ret));
  } else {
    primary_map = &conflict_map_array_.at(0).conflict_map_;
  }
  return ret;
}

int ObConflictChecker::init_das_scan_rtdef()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = eval_ctx_.exec_ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo *my_session = eval_ctx_.exec_ctx_.get_my_session();
  ObTaskExecutorCtx &task_exec_ctx = eval_ctx_.exec_ctx_.get_task_exec_ctx();
  das_scan_rtdef_.timeout_ts_ = plan_ctx->get_ps_timeout_timestamp();
  das_scan_rtdef_.sql_mode_ = my_session->get_sql_mode();
  das_scan_rtdef_.stmt_allocator_.set_alloc(&das_ref_.get_das_alloc());
  das_scan_rtdef_.scan_allocator_.set_alloc(&das_ref_.get_das_alloc());
  ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true // read_latest
                        );
  das_scan_rtdef_.scan_flag_.flag_ = query_flag.flag_;
  int64_t schema_version = task_exec_ctx.get_query_tenant_begin_schema_version();
  das_scan_rtdef_.tenant_schema_version_ = schema_version;
  das_scan_rtdef_.eval_ctx_ = &eval_ctx_;
  das_scan_rtdef_.ctdef_ = &checker_ctdef_.das_scan_ctdef_;
  das_scan_rtdef_.table_loc_ = table_loc_;
  if (OB_FAIL(das_scan_rtdef_.init_pd_op(eval_ctx_.exec_ctx_, checker_ctdef_.das_scan_ctdef_))) {
    LOG_WARN("init pushdown storage filter failed", K(ret));
  } else if (nullptr != checker_ctdef_.attach_spec_.attach_ctdef_) {
    if (OB_ISNULL(attach_rtinfo_ = OB_NEWx(ObDASAttachRtInfo, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate das attach info", K(ret));
    } else if (OB_FAIL(init_attach_scan_rtdef(checker_ctdef_.attach_spec_.attach_ctdef_, attach_rtinfo_->attach_rtdef_))) {
      LOG_WARN("fail to init attach scan rtdef", K(ret), KPC(checker_ctdef_.attach_spec_.attach_ctdef_));
    }
  }
  return ret;
}

int ObConflictChecker::init_attach_scan_rtdef(const ObDASBaseCtDef *attach_ctdef,
                                              ObDASBaseRtDef *&attach_rtdef)
{
  int ret = OB_SUCCESS;
  ObExecContext &ctx = eval_ctx_.exec_ctx_;
  ObDASTaskFactory &das_factory = DAS_CTX(ctx).get_das_factory();
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
                                                           &ctx.get_allocator(),
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
    attach_rtinfo_->related_scan_cnt_++;
    if (attach_ctdef == &checker_ctdef_.das_scan_ctdef_) {
      attach_rtdef = &das_scan_rtdef_;
    } else if (attach_ctdef->op_type_ != DAS_OP_TABLE_SCAN) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("attach ctdef type is invalid", K(ret), K(attach_ctdef->op_type_));
    } else {
      ObPhysicalPlanCtx *plan_ctx = eval_ctx_.exec_ctx_.get_physical_plan_ctx();
      ObSQLSessionInfo *my_session = eval_ctx_.exec_ctx_.get_my_session();
      ObTaskExecutorCtx &task_exec_ctx = eval_ctx_.exec_ctx_.get_task_exec_ctx();
      const ObDASScanCtDef *attach_scan_ctdef = static_cast<const ObDASScanCtDef*>(attach_ctdef);
      const ObDASTableLocMeta *attach_loc_meta = checker_ctdef_.attach_spec_.get_attach_loc_meta(
          table_loc_->get_table_location_key(), attach_scan_ctdef->ref_table_id_);
      ObDASScanRtDef *attach_scan_rtdef = static_cast<ObDASScanRtDef*>(attach_rtdef);
      attach_scan_rtdef->timeout_ts_ = plan_ctx->get_ps_timeout_timestamp();
      attach_scan_rtdef->sql_mode_ = my_session->get_sql_mode();
      attach_scan_rtdef->stmt_allocator_.set_alloc(&das_ref_.get_das_alloc());
      attach_scan_rtdef->scan_allocator_.set_alloc(&das_ref_.get_das_alloc());
      ObQueryFlag query_flag(ObQueryFlag::Forward/*scan_order*/, false/*daily_merge*/, false/*optimize*/,
                            false/*sys scan*/, false/*full_row*/, false/*index_back*/, false/*query_stat*/,
                            ObQueryFlag::MysqlMode/*sql_mode*/, true/*read_latest*/);
      attach_scan_rtdef->scan_flag_.flag_ = query_flag.flag_;
      attach_scan_rtdef->tenant_schema_version_ = task_exec_ctx.get_query_tenant_begin_schema_version();
      attach_scan_rtdef->eval_ctx_ = &eval_ctx_;
      attach_scan_rtdef->ctdef_ = attach_ctdef;
      attach_scan_rtdef->table_loc_ = DAS_CTX(ctx).get_table_loc_by_id(table_loc_->get_table_location_key(),
          attach_scan_ctdef->ref_table_id_);
      if (OB_FAIL(attach_scan_rtdef->init_pd_op(eval_ctx_.exec_ctx_, *attach_scan_ctdef))) {
        LOG_WARN("init pushdown storage filter failed", K(ret));
      } else if (OB_ISNULL(attach_scan_rtdef->table_loc_)) {
        if (OB_ISNULL(attach_loc_meta)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get table loc by id failed", K(ret), K(table_loc_->get_table_location_key()),
              K(attach_scan_ctdef->ref_table_id_),K(DAS_CTX(ctx).get_table_loc_list()));
        } else if (OB_FAIL(DAS_CTX(ctx).extended_table_loc(*attach_loc_meta, attach_scan_rtdef->table_loc_))) {
          LOG_WARN("extended table location failed", K(ret), KPC(attach_loc_meta));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(attach_scan_rtdef->table_loc_)
          && OB_NOT_NULL(attach_scan_rtdef->table_loc_->loc_meta_)) {
        if (attach_scan_rtdef->table_loc_->loc_meta_->select_leader_ == 0) {
          attach_scan_rtdef->scan_flag_.set_is_select_follower();
        }
      }
    }
  }
  return ret;
}

int ObConflictChecker::attach_related_taskinfo(ObDASScanOp &target_op, ObDASBaseRtDef *attach_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_rtdef) || OB_ISNULL(attach_rtdef->ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attach rtdef is invalid", K(ret), KP(attach_rtdef), KP(attach_rtdef->ctdef_));
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

int ObConflictChecker::collect_all_snapshot(transaction::ObTxReadSnapshot &snapshot, const ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  ObTabletSnapshotMaping tablet_snapshot_maping;
  tablet_snapshot_maping.tablet_id_ = tablet_loc->tablet_id_;
  tablet_snapshot_maping.ls_id_ = tablet_loc->ls_id_;
  if (OB_ISNULL(tablet_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (FALSE_IT(tablet_snapshot_maping.tablet_id_ = tablet_loc->tablet_id_)) {
    // do nothing
  } else if (FALSE_IT(tablet_snapshot_maping.ls_id_ = tablet_loc->ls_id_)) {
    // do nothing
  } else if (OB_FAIL(tablet_snapshot_maping.snapshot_.assign(snapshot))) {
    LOG_WARN("fail to assign snapshot", K(ret), K(snapshot));
  } else if (OB_FAIL(has_exist_in_array(snapshot_maping_, tablet_snapshot_maping))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected duplicate snapshot_version", K(ret), K(tablet_snapshot_maping), K(snapshot_maping_));
  } else if (OB_FAIL(snapshot_maping_.push_back(tablet_snapshot_maping))) {
    LOG_WARN("fail to push back snapshot", K(ret), K(tablet_snapshot_maping));
  } else {
    LOG_TRACE("collect snaoshot after try_insert", K(tablet_snapshot_maping));
  }
  return ret;
}

int ObConflictChecker::get_snapshot_by_ids(ObTabletID tablet_id, share::ObLSID ls_id, bool &founded, transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  founded = false;
  for (int64_t i = 0; OB_SUCC(ret) && !founded && i < snapshot_maping_.count(); i++) {
    ObTabletSnapshotMaping &snapshot_maping = snapshot_maping_.at(i);
    if (snapshot_maping.tablet_id_ == tablet_id && snapshot_maping.ls_id_ == ls_id) {
      if (OB_FAIL(snapshot.assign(snapshot_maping.snapshot_))) {
        LOG_WARN("fail to assign snapshot", K(ret));
      } else {
        founded = true;
      }
    }
  }

  return ret;
}

int ObConflictChecker::post_all_das_scan_tasks()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_partition_snapshot_for_das_task(das_ref_))) {
    LOG_WARN("fail to set partition snapshot", K(ret));
  } else if (OB_FAIL(das_ref_.execute_all_task())) {
    LOG_WARN("execute all delete das task failed", K(ret));
  }
  return ret;
}

int ObConflictChecker::set_partition_snapshot_for_das_task(ObDASRef &das_ref)
{
  int ret = OB_SUCCESS;
  if (!das_ref.is_do_gts_opt()) {
    // do nothing
  } else {
    DASTaskIter task_iter = das_ref.begin_task_iter();
    while (OB_SUCC(ret) && !task_iter.is_end()) {
      bool founded = false;
      transaction::ObTxReadSnapshot *snapshot = nullptr;;
      ObIDASTaskOp *das_op = *task_iter;
      if (OB_ISNULL(das_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_ISNULL(snapshot = das_op->get_das_gts_opt_info().get_specify_snapshot())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), KPC(snapshot));
      } else if (OB_FAIL(get_snapshot_by_ids(das_op->get_tablet_id(), das_op->get_ls_id(), founded, *snapshot))) {
        LOG_WARN("fail to get snapshot by ids", K(das_op->get_tablet_id()), K(das_op->get_ls_id()), K(snapshot_maping_));
      } else if (!founded) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to found snapshot", K(ret), K(das_op->get_tablet_id()), K(das_op->get_type()), K(das_op->get_ls_id()));
      } else if (!snapshot->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("snapshot is invalid", K(ret), K(das_op->get_tablet_id()), K(das_op->get_type()));
      } else {
        LOG_TRACE("set specify snapshot for current das_task", KPC(snapshot),
            K(das_op->get_tablet_id()), K(das_op->get_ls_id()), K(das_op->get_type()));
      }
      ++task_iter;
    }
  }
  return ret;
}

int ObConflictRange::assign(const ObConflictRange &conflict_range)
{
  int ret = OB_SUCCESS;
  if (!conflict_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(conflict_range));
  } else {
    tablet_id_ = conflict_range.tablet_id_;
    rowkey_ = conflict_range.rowkey_;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
