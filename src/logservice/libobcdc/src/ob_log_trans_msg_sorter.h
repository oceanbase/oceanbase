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
 *
 * Binlog Record Sorter Module. Sort binlog record for user
 * Module Input: TransCtx
 */

#ifndef OCEANBASE_LIBOBCDC_BINLOG_SORTER_
#define OCEANBASE_LIBOBCDC_BINLOG_SORTER_

#include "lib/thread/ob_simple_thread_pool.h"  // ObSimpleThreadPool

#include "ob_log_trans_ctx.h"                  // TransCtx, TransID
#include "ob_log_binlog_record.h"              // ObLogBR
#include "ob_log_trans_stat_mgr.h"


namespace oceanbase
{
namespace libobcdc
{

class IObLogTransMsgSorter
{
public:
  virtual ~IObLogTransMsgSorter() {}
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual int submit(TransCtx *trans) = 0;
  virtual void mark_stop_flag() = 0;
  virtual int get_task_count(int64_t &task_count) = 0;
  virtual const transaction::ObTransID &get_cur_sort_trans_id() const = 0;
};

/**
 * @brief Binlog Record Sorter, output br by order and put br to TransCtx,
 * use fixed-size thread pool(ObSimpleThreadPool) to handle task
 * use a thread to handle participants feedback function(if need sort by seq_no)
 */
class IObLogErrHandler;
typedef ObSimpleThreadPool TransMsgSorterThread;
class ObLogTransMsgSorter: public IObLogTransMsgSorter, public TransMsgSorterThread
{
public:
  ObLogTransMsgSorter();
  virtual ~ObLogTransMsgSorter();

public:
  int start();
  void stop();
  int submit(TransCtx *trans);
  // OBSimpleThraedPool guaraentee task to be handled is not null
  virtual void handle(void *data) override;
  int get_task_count(int64_t &task_count);
  void mark_stop_flag();

public:
  int init(const bool enable_sort_by_seq_no,
           const int64_t thread_num,
           const int64_t task_limit,
           IObLogTransStatMgr &trans_stat_mgr,
           IObLogErrHandler *err_handler);
  void destroy();
  OB_INLINE const transaction::ObTransID &get_cur_sort_trans_id() const
  { return cur_sort_trans_id_; }

private:
  struct StmtSequerenceCompFunc
  {
    // stmt compare function for br sorter, sort by seq_no to get min_seq_no in heap, only to sort DmlStmtTask
    bool operator()(const DmlStmtTask *task1, const DmlStmtTask *task2)
    {
        return task1->get_row_seq_no() > task2->get_row_seq_no();
    }
  };

private:
  // function thread, points to sort_br_by_seq_no_if is_sort_by_br_seq_no_ = true
  // otherwise points to sort_br_by_part_order_
  int (ObLogTransMsgSorter::*br_sort_func_)(TransCtx &trans);

private:
  // output br by partition order
  int sort_br_by_part_order_(TransCtx &trans);
  // output br by seq_no
  int sort_br_by_seq_no_(TransCtx &trans);
  // output all br of part trans
  int handle_partition_br_(TransCtx &trans, PartTransTask &part_trans_task);
  // get next valid br of specified part trans and statistic feedback info
  int next_stmt_contains_valid_br_(PartTransTask &part_trans_task, DmlStmtTask *&dml_stmt_task);
  // feedback br output static info for PartTransTask
  // note: this can safely called by sorter because PartTransTask won't be revert before sorter mark TransCtx is sorted
  //       should change this function if the guarantee above is changed otherwise will core or unexpected behave
  void feedback_br_output_info_(PartTransTask &part_trans_task);

private:
  bool                      is_inited_;
  int64_t                   thread_num_;
  int64_t                   task_limit_;
  int64_t                   total_task_count_;
  IObLogTransStatMgr        *trans_stat_mgr_;
  bool                      enable_sort_by_seq_no_;
  transaction::ObTransID    cur_sort_trans_id_;
  IObLogErrHandler          *err_handler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTransMsgSorter);
};

} // end namespace libobcdc
} // end namespace oceanbase
#endif // end OCEANBASE_LIBOBCDC_BINLOG_SORTER_
