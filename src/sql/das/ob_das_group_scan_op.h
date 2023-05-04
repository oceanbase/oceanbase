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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_GROUP_SCAN_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_GROUP_SCAN_OP_H_
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_group_scan_iter.h"
namespace oceanbase
{
namespace sql
{
class ObGroupLookupOp : public ObLocalIndexLookupOp
{
public:
  ObGroupLookupOp() : ObLocalIndexLookupOp(ObNewRowIterator::IterType::ObGroupLookupOp),
                      group_iter_()
  {
    lookup_iter_ = &group_iter_;
  }
  virtual ~ObGroupLookupOp();
  virtual void reset() override
  {
    ObLocalIndexLookupOp::reset();
    index_group_cnt_ = 1;
    lookup_group_cnt_ = 1;
  }
  virtual int64_t get_index_group_cnt() const override  { return index_group_cnt_; }
  virtual void set_index_group_cnt(int64_t group_cnt_) override {index_group_cnt_ = group_cnt_;}
  virtual void inc_index_group_cnt() override { ++index_group_cnt_; }
  virtual int64_t get_lookup_group_cnt() const  override  { return lookup_group_cnt_; }
  virtual void inc_lookup_group_cnt() override { ++lookup_group_cnt_; }
  virtual int switch_rowkey_scan_group() override
  {
    return static_cast<ObGroupScanIter *>(rowkey_iter_)->switch_scan_group();
  }
  virtual int set_rowkey_scan_group(int64_t group_id) override
  {
    return static_cast<ObGroupScanIter *>(rowkey_iter_)->set_scan_group(group_id);
  }
  virtual ObNewRowIterator *&get_lookup_storage_iter() override;
  int init_group_range(int64_t cur_group_idx, int64_t group_size) override;
  virtual bool need_next_index_batch() const override;
  virtual int init_group_scan_iter(int64_t cur_group_idx,
                                   int64_t group_size,
                                   ObExpr *group_id_expr);
  virtual int switch_lookup_scan_group() override;
  virtual int set_lookup_scan_group(int64_t group_id) override;

  int revert_iter();
public:
  ObGroupScanIter group_iter_;
};

class ObDASGroupScanOp : public ObDASScanOp
{
  OB_UNIS_VERSION(1);
public:
  ObDASGroupScanOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASGroupScanOp();
  int open_op() override;
  int release_op() override;
  virtual int rescan() override;
  virtual int switch_scan_group() override;
  virtual int set_scan_group(int64_t group_id) override;
  int64_t get_cur_group_idx() const { return iter_.get_cur_group_idx(); }
  void init_group_range(int64_t cur_group_idx, int64_t group_size);
  virtual ObLocalIndexLookupOp *get_lookup_op() override
  { return group_lookup_op_; }
  ObNewRowIterator *get_storage_scan_iter() override;
  int do_local_index_lookup() override;
  int decode_task_result(ObIDASTaskResult *task_result) override;
  int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override;
  void set_is_exec_remote(bool v) { is_exec_remote_ = v; }
  virtual bool need_all_output() override { return is_exec_remote_; }
  TO_STRING_KV(K(iter_), KP(group_lookup_op_), K(group_size_), K(cur_group_idx_));
private:
  common::ObNewRowIterator *&get_scan_result() { return result_; }
  ObNewRowIterator *get_output_result_iter() override
  {
    return result_iter_;
  };
private:
  // can't set group_lookup_op_ to ObDASScanOp::result_
  // because may circular dependencies:
  // ObDASGroupScanOp::result_ -> ObGroupLookupOp
  // ObGroupLookupOp::rowkey_iter_ -> ObDASGroupScanOp::ObGroupScanIter
  // ObDASGroupScanOp::ObGroupScanIter -> ObDASGroupScanOp::result_
  ObGroupLookupOp *group_lookup_op_;
  ObGroupScanIter iter_;
  // for normal group scan:
  //   local das:
  //      result_iter_ is &group_scan_iter_
  //   remote das:
  //      local server: result_iter_ is &group_scan_iter_ and
  //                    the input of  group_scan_iter_ is ObDASScanResult
  //      remote server: result_iter_ is storage_iter
  // for local index lookup group scan:
  //   local das:
  //      result_iter_ is group_lookup_op_
  //   remote das:
  //      local server: result_iter_ is group_scan_iter of group_lookup_op_ and
  //                    the input of this group_scan_iter is ObGroupScanIter
  //      remote server: result_iter_ is group_lookup_op_ and indicate the group_lookup_op is exec remote
  //                     which will not need switch iter when lookup, and output the result of all group
  ObNewRowIterator *result_iter_;
  bool is_exec_remote_;
  int64_t cur_group_idx_;
  int64_t group_size_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_BATCH_SCAN_OP_H_ */
