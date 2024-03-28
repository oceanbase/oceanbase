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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_MERGE_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_MERGE_ITER_H_
#include "sql/das/ob_das_utils.h"
#include "sql/das/iter/ob_das_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASMergeIterParam : public ObDASIterParam
{
public:
  ObFixedArray<ObEvalInfo*, ObIAllocator> *eval_infos_;
  bool need_update_partition_id_;
  ObExpr *pdml_partition_id_;
  int64_t partition_id_calc_type_;
  bool should_scan_index_;
  common::ObTableID ref_table_id_;
  bool is_vectorized_;
  const ObExprFrameInfo *frame_info_;
  bool execute_das_directly_;

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid() && eval_infos_ != nullptr && frame_info_ != nullptr;
  }
};

class ObDASMergeIter : public ObDASIter
{
public:
  ObDASMergeIter()
    : wild_datum_info_(),
      merge_type_(SEQUENTIAL_MERGE),
      eval_infos_(nullptr),
      need_update_partition_id_(false),
      pdml_partition_id_(nullptr),
      partition_id_calc_type_(0),
      should_scan_index_(false),
      ref_table_id_(),
      is_vectorized_(false),
      das_ref_(nullptr),
      iter_alloc_(nullptr),
      das_tasks_arr_(),
      get_next_row_(nullptr),
      get_next_rows_(nullptr),
      seq_task_idx_(OB_INVALID_INDEX),
      group_id_idx_(OB_INVALID_INDEX),
      merge_state_arr_(),
      store_rows_(nullptr)
  {}
  virtual ~ObDASMergeIter() {}

  virtual int set_merge_status(MergeType merge_type) override;
  MergeType get_merge_type() const { return merge_type_; }
  void set_global_lookup_iter(ObDASMergeIter *global_lookup_iter);
  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter, K_(merge_type), K_(ref_table_id));

  /********* DAS REF BEGIN *********/
  common::ObIAllocator *get_das_alloc();
  int create_das_task(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&scan_op, bool &reuse_op);
  bool has_task() const;
  int32_t get_das_task_cnt() const;
  DASTaskIter begin_task_iter();
  bool is_all_local_task() const;
  int rescan_das_task(ObDASScanOp *scan_op);
  // do_table_scan() need be called before get_next_row(s).
  int do_table_scan();
  /********* DAS REF END *********/

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

  void reset_datum_ptr(ObDASScanOp *scan_op, int64_t &capacity);
  void reset_wild_datum_ptr();
  void update_wild_datum_ptr(int64_t rows_count);
  void clear_evaluated_flag();
  int update_output_tablet_id(ObIDASTaskOp *output_das_task);

private:
  int get_next_seq_row();
  int get_next_seq_rows(int64_t &count, int64_t capacity);
  int get_next_sorted_row();
  int get_next_sorted_rows(int64_t &count, int64_t capacity);
  void compare(int64_t cur_idx, int64_t &output_idx);

private:

  struct WildDatumPtrInfo
  {
    WildDatumPtrInfo()
      : exprs_(nullptr),
        max_output_rows_(0),
        global_lookup_iter_(nullptr)
    { }
    const ObExprPtrIArray *exprs_;
    int64_t max_output_rows_;
    // global index scan and its lookup maybe share some expr,
    // so remote lookup task change its datum ptr,
    // and also lead index scan touch the wild datum ptr
    // so need to associate the result iterator of scan and lookup
    // resetting the index scan result datum ptr will also reset the lookup result datum ptr
    ObDASMergeIter *global_lookup_iter_;
  };

  WildDatumPtrInfo wild_datum_info_;
  MergeType merge_type_;
  ObFixedArray<ObEvalInfo*, ObIAllocator> *eval_infos_;
  bool need_update_partition_id_;
  ObExpr *pdml_partition_id_;
  int64_t partition_id_calc_type_;
  bool should_scan_index_;
  common::ObTableID ref_table_id_;
  bool is_vectorized_;
  ObDASRef *das_ref_;
  char das_ref_buf_[sizeof(ObDASRef)];
  common::ObArenaAllocator *iter_alloc_;
  char iter_alloc_buf_[sizeof(common::ObArenaAllocator)];
  typedef common::ObSEArray<ObIDASTaskOp*, 16> DasTaskArray;
  DasTaskArray das_tasks_arr_;
  int (ObDASMergeIter::*get_next_row_)();
  int (ObDASMergeIter::*get_next_rows_)(int64_t&, int64_t);

  /********* SEQUENTIAL MERGE BEGIN *********/
  int64_t seq_task_idx_;
  /********* SEQUENTIAL MERGE END *********/

  /********* SORT MERGE BEGIN *********/
  struct MergeState
  {
    bool row_store_have_data_;
    bool das_task_iter_end_;
    MergeState()
     : row_store_have_data_(false),
       das_task_iter_end_(false)
    {}
    void reset()
    {
      row_store_have_data_ = false;
      das_task_iter_end_ = false;
    }
    TO_STRING_KV(K_(row_store_have_data),
                 K_(das_task_iter_end));
  };
  typedef common::ObSEArray<MergeState, 16> MergeStateArray;
  typedef ObChunkDatumStore::LastStoredRow LastDASStoreRow;
  int64_t group_id_idx_;
  MergeStateArray merge_state_arr_;
  LastDASStoreRow *store_rows_;
  /********* SORT MERGE END *********/
};

}//end namespace sql
}//end namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_MERGE_ITER_H_ */
