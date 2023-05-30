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

#ifndef OBDEV_SRC_SQL_ENGINE_DML_OB_CONFLICT_ROW_CHECKER_H_
#define OBDEV_SRC_SQL_ENGINE_DML_OB_CONFLICT_ROW_CHECKER_H_
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"

namespace oceanbase
{
namespace sql
{
class ObTableModifyOp;
struct ObDASScanCtDef;
struct ObDASScanRtDef;

struct ObRowkeyCstCtdef
{
  OB_UNIS_VERSION_V(1);
public:
  ObRowkeyCstCtdef(common::ObIAllocator &alloc)
    :  constraint_name_(),
       rowkey_expr_(alloc),
       calc_exprs_(alloc),
       rowkey_accuracys_(alloc)
  {
  }
  virtual ~ObRowkeyCstCtdef() = default;
  TO_STRING_KV(K_(constraint_name),
               K_(rowkey_expr),
               K_(rowkey_accuracys),
               K_(calc_exprs));
  ObString constraint_name_;  // 冲突时打印表名用
  ExprFixedArray rowkey_expr_; // 索引表的主键
  ExprFixedArray calc_exprs_; // 计算逐渐信息依赖的表达式
  AccuracyFixedArray rowkey_accuracys_;
};

enum ObNewRowSource
{
  //can not adjust the order of DASOpType, append OpType at the last
  FROM_SCAN = 0,
  FROM_INSERT,
  FROM_UPDATE,
  NEED_DO_LOCK
};

struct ObConflictValue
{
  ObConflictValue()
    : baseline_datum_row_(NULL),
      current_datum_row_(NULL),
      new_row_source_(ObNewRowSource::FROM_SCAN)
  {}
  //在进行check_duplicate_rowkey时, 会将冲突的行去重(add_var_to_array_no_dup)
  //需要使用该操作符
  bool operator==(const ObConflictValue &other) const;
  TO_STRING_KV(KPC_(baseline_datum_row), KPC_(current_datum_row), K_(new_row_source));
  const ObChunkDatumStore::StoredRow *baseline_datum_row_;
  const ObChunkDatumStore::StoredRow *current_datum_row_;
  ObNewRowSource new_row_source_;
};

typedef common::hash::ObHashMap<ObRowkey,
    ObConflictValue, common::hash::NoPthreadDefendMode> ObConflictRowMap;
class ObConflictRowMapCtx
{
public:
  ObConflictRowMapCtx()
    : conflict_map_(),
      rowkey_(NULL),
      allocator_(nullptr)
  {
  }
  ~ObConflictRowMapCtx() {};

  int init_conflict_map(int64_t bucket_num, int64_t obj_cnt, common::ObIAllocator *allocator);
  int reuse();
  int destroy();

public:
  static const int64_t MAX_ROW_BATCH_SIZE = 500;
  ObConflictRowMap conflict_map_;
  ObRowkey *rowkey_; // 临时的ObRowkey,用于map的compare，循环使用
  common::ObIAllocator *allocator_; // allocator用来创建hash map
};

typedef common::ObFixedArray<ObRowkeyCstCtdef *, common::ObIAllocator> ObRowkeyCstCtdefArray;
class ObConflictCheckerCtdef
{
  OB_UNIS_VERSION_V(1);
public:
  ObConflictCheckerCtdef(common::ObIAllocator &alloc)
    : cst_ctdefs_(alloc),
      calc_part_id_expr_(NULL),
      part_id_dep_exprs_(alloc),
      das_scan_ctdef_(alloc),
      partition_cnt_(0),
      data_table_rowkey_expr_(alloc),
      table_column_exprs_(alloc),
      use_dist_das_(false),
      rowkey_count_(0),
      alloc_(alloc)
  {}
  virtual ~ObConflictCheckerCtdef() = default;
  TO_STRING_KV(K_(cst_ctdefs), K_(das_scan_ctdef), KPC_(calc_part_id_expr));
  // must constraint_infos_.count() == conflict_map_array_.count()
  // constraint_infos_ 用于生成ObConflictRowMap的key
  ObRowkeyCstCtdefArray cst_ctdefs_;
  ObExpr *calc_part_id_expr_; // 非dist计划时是NULL
  // calc_part_id_expr_计算所依赖的表达式，用于clear_eval_flag
  ExprFixedArray part_id_dep_exprs_;
  // 回表查询，为了结构统一，主表也需要一次回表，第一次的insert都返回主表的主键
  ObDASScanCtDef das_scan_ctdef_;
  int64_t partition_cnt_;
  // 主表的主键, 用于构建回表时的scan_range
  ExprFixedArray data_table_rowkey_expr_;
  // 主表的all_columns
  ExprFixedArray table_column_exprs_;
  bool use_dist_das_;
  int64_t rowkey_count_;
  common::ObIAllocator &alloc_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConflictCheckerCtdef);
};

class ObConflictChecker
{
public:
  ObConflictChecker(common::ObIAllocator &allocator,
                    ObEvalCtx &eval_ctx,
                    const ObConflictCheckerCtdef &checker_ctdef);
  ~ObConflictChecker() {};

  //初始conflict_checker
  int init_conflict_checker(const ObExprFrameInfo *expr_frame_info,
                            ObDASTableLoc *table_loc);
  void set_local_tablet_loc(ObDASTabletLoc *tablet_loc) { local_tablet_loc_ = tablet_loc; }

  //初始conflict_map
  int create_conflict_map(int64_t replace_row_cnt);

  // 检查当前的主键是否冲突
  int check_duplicate_rowkey(const ObChunkDatumStore::StoredRow *replace_row,
                             ObIArray<ObConflictValue> &constraint_values,
                             bool is_insert_up);

  // 从hash map中删除冲突行
  int delete_old_row(const ObChunkDatumStore::StoredRow *replace_row,
                     ObNewRowSource from);

  // 插入新行到hash map中
  int insert_new_row(const ObChunkDatumStore::StoredRow *new_row,
                     ObNewRowSource from);

  int update_row(const ObChunkDatumStore::StoredRow *new_row,
                 const ObChunkDatumStore::StoredRow *old_row);

  int lock_row(const ObChunkDatumStore::StoredRow *lock_row);

  int convert_exprs_to_stored_row(const ObExprPtrIArray &exprs,
                                  ObChunkDatumStore::StoredRow *&new_row);

  // todo @kaizhan.dkz 这里可以把 char *buf 和int64_t buf_len 替换成ObString
  int extract_rowkey_info(const ObRowkeyCstCtdef *constraint_info,
                          char *buf,
                          int64_t buf_len);

  //这个函数类似于shuffle_final_data，只不过这里只是返回主表的hash map的指针，外层函数迭代map，将行分别插入对应的das task
  int get_primary_table_map(ObConflictRowMap *&primary_map);

  // 将回表拉回的数据来构建map
  int build_base_conflict_map(
      int64_t replace_row_cnt,
      const ObChunkDatumStore::StoredRow *conflict_row);

  // 向主表做回表，根据冲突行的主键，查询出所有对应主表的冲突行, 构建冲突map
  int do_lookup_and_build_base_map(int64_t replace_row_cnt);

  // todo @kaizhan.dkz 构建回表的das scan task
  int build_primary_table_lookup_das_task();

  //会被算子的inner_close函数调用
  int close();

  int reuse();

  int destroy();

private:
  int to_expr(const ObChunkDatumStore::StoredRow *replace_row);
  int calc_lookup_tablet_loc(ObDASTabletLoc *&tablet_loc);

  // get当前行对应的scan_op
  int get_das_scan_op(ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_scan_op);

  // 构建回表的range信息
  int build_data_table_range(ObNewRange &lookup_range);

  // --------------------------
  int get_next_row_from_data_table(DASOpResultIter &result_iter,
                                   ObChunkDatumStore::StoredRow *&conflict_row);

  // 构建ob_rowkey
  int build_rowkey(ObRowkey *&rowkey, ObRowkeyCstCtdef *rowkey_cst_ctdef);

  // 构建ob_rowkey
  int build_tmp_rowkey(ObRowkey *rowkey, ObRowkeyCstCtdef *rowkey_info);

  int init_das_scan_rtdef();

  int get_tmp_string_buffer(common::ObIAllocator *&allocator);
public:
  common::ObArrayWrap<ObConflictRowMapCtx> conflict_map_array_;
  ObEvalCtx &eval_ctx_; // 用于表达式的计算
  const ObConflictCheckerCtdef &checker_ctdef_;
  ObDASScanRtDef das_scan_rtdef_;
  // allocator用来创建hash map, 是ObExecContext内部的allocator 这个不能被reuse
  common::ObIAllocator &allocator_;
  // das_scan回表用
  // This das_ref must be careful with reuse and reset,
  // because its internal allocator is used in many places
  ObDASRef das_ref_;
  ObDASTabletLoc *local_tablet_loc_;
  ObDASTableLoc *table_loc_;
  lib::MemoryContext tmp_mem_ctx_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_DML_OB_CONFLICT_ROW_CHECKER_H_ */
