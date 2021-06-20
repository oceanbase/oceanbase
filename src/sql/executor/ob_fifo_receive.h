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

#ifndef OCEANBASE_SQL_EXECUTOR_FIFO_RECEIVE_
#define OCEANBASE_SQL_EXECUTOR_FIFO_RECEIVE_

#include "sql/engine/sort/ob_base_sort.h"
#include "sql/executor/ob_receive.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_distributed_scheduler.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObTaskInfo;
class ObPhyOperator;

class ObTaskResultIter {
public:
  enum IterType {
    IT_ROOT,
    IT_DISTRIBUTED,
  };

public:
  explicit ObTaskResultIter(IterType iter_tyep);
  virtual ~ObTaskResultIter();
  virtual int get_next_task_result(ObTaskResult& task_result) = 0;
  virtual int check_status() = 0;
  inline IterType get_iter_type()
  {
    return iter_type_;
  }

protected:
  IterType iter_type_;
};

class ObRootTaskResultIter : public ObTaskResultIter {
public:
  ObRootTaskResultIter(ObExecContext& exec_ctx, uint64_t exec_id, uint64_t child_op_id, int64_t ts_timeout);
  virtual ~ObRootTaskResultIter();
  virtual int get_next_task_result(ObTaskResult& task_result);
  virtual int check_status();
  int init();

private:
  ObExecContext& exec_ctx_;
  uint64_t exec_id_;
  ObDistributedSchedulerManager::ObDistributedSchedulerHolder scheduler_holder_;
  ObDistributedScheduler* scheduler_;
  uint64_t child_op_id_;
  int64_t ts_timeout_;
};

class ObDistributedTaskResultIter : public ObTaskResultIter {
public:
  explicit ObDistributedTaskResultIter(const ObIArray<ObTaskResultBuf>& task_results);
  virtual ~ObDistributedTaskResultIter();
  virtual int get_next_task_result(ObTaskResult& task_result);
  virtual int check_status();

private:
  const ObIArray<ObTaskResultBuf>& task_results_;
  int64_t cur_idx_;
};

// class ObRootReceiveInput : public ObIPhyOperatorInput
//{
//  OB_UNIS_VERSION_V(1);
// public:
//  ObRootReceiveInput();
//  virtual ~ObRootReceiveInput();
//  virtual ObPhyOperatorType get_phy_op_type() const  { return PHY_ROOT_RECEIVE; }
//  virtual int init(ObExecContext &exec_ctx, ObTaskInfo &task_info, ObPhyOperator &phy_op);
//  inline uint64_t get_child_op_id() const { return child_op_id_; }
// private:
//  uint64_t child_op_id_;
//};

class ObDistributedReceiveInput : public ObIPhyOperatorInput {
  OB_UNIS_VERSION_V(1);

public:
  ObDistributedReceiveInput();
  virtual ~ObDistributedReceiveInput();
  virtual void reset() override;
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_DISTRIBUTED_RECEIVE;
  }
  virtual int init(ObExecContext& exec_ctx, ObTaskInfo& task_info, const ObPhyOperator& phy_op);
  inline const common::ObIArray<ObTaskResultBuf>& get_child_task_results() const
  {
    return child_task_results_;
  }
  inline void set_child_job_id(uint64_t child_job_id)
  {
    child_job_id_ = child_job_id;
  }

private:
  common::ObSEArray<ObTaskResultBuf, 8> child_task_results_;
  uint64_t child_job_id_;  // need serialized, but only used for init child_task_results.
};

class ObIDataSource {
public:
  ObIDataSource();
  virtual ~ObIDataSource();

  int get_next_row(ObNewRow& row);

  virtual int open(ObExecContext& exec_ctx, const ObTaskResult& task_result);
  virtual int close() = 0;
  TO_STRING_KV(K_(inited), K_(cur_scanner));

protected:
  virtual int fetch_next_scanner() = 0;

  common::ObScanner cur_scanner_;
  common::ObRowStore::Iterator row_iter_;
  ObExecContext* exec_ctx_;
  ObSliceID slice_id_;
  common::ObAddr peer_;
  bool use_small_result_;
  bool inited_;
};

// Fetch interm result using stream RPC (occupy one peer thread).
// Only for compatibility, can be removed after all cluster upgrade to 2.1.0
class ObStreamDataSource : public ObIDataSource {
public:
  ObStreamDataSource();
  ~ObStreamDataSource();
  virtual int close() override;

private:
  virtual int fetch_next_scanner() override;
  /*
   * ObExecutorRpcImpl::task_fetch_interm_result() => FetchIntermResultStreamHandle
   * FetchIntermResultStreamHandle::get_result() => ObIntermResultItem
   * ObIntermResultItem::assign_to_scanner() => ObScanner
   * ObScanner::begin() => ObRowStore::Iterator
   * ObRowStore::Iterator::get_next_row() => ObNewRow
   */
  FetchIntermResultStreamHandle stream_handler_;
  bool stream_opened_;
};

// Fetch interm result using normal RPC, specify interm item index every time.
class ObSpecifyDataSource : public ObIDataSource {
public:
  ObSpecifyDataSource();
  ~ObSpecifyDataSource();

  virtual int close() override;

private:
  virtual int fetch_next_scanner() override;

  int64_t fetch_index_;
  ObFetchIntermResultItemRes interm_result_item_;
};

class ObAsyncReceive : public ObReceive {
protected:
  class ObAsyncReceiveCtx : public ObReceiveCtx {
    friend class ObAsyncReceive;
    friend class ObFifoReceiveV2;

  public:
    explicit ObAsyncReceiveCtx(ObExecContext& exec_ctx);
    virtual ~ObAsyncReceiveCtx();
    virtual void destroy();
    int init_root_iter(uint64_t child_op_id);
    int init_distributed_iter(const ObIArray<ObTaskResultBuf>& task_results);

  protected:
    ObTaskResultIter* task_result_iter_;
    int64_t found_rows_;
    int64_t affected_rows_;
    int64_t matched_rows_;
    int64_t duplicated_rows_;
    bool iter_end_;
  };

public:
  explicit ObAsyncReceive(common::ObIAllocator& alloc);
  virtual ~ObAsyncReceive();
  void set_in_root_job(bool in_root_job)
  {
    in_root_job_ = in_root_job;
  }
  bool is_in_root_job() const
  {
    return in_root_job_;
  }

protected:
  virtual int create_operator_input(ObExecContext& exec_ctx) const;
  virtual int create_op_ctx(ObExecContext& exec_ctx, ObAsyncReceiveCtx*& op_ctx) const = 0;
  virtual int init_op_ctx(ObExecContext& exec_ctx) const;
  virtual int inner_close(ObExecContext& exec_ctx) const;
  virtual int rescan(ObExecContext& ctx) const;
  virtual int get_next_task_result(ObAsyncReceiveCtx& op_ctx, ObTaskResult& task_result) const;

  int create_data_source(ObExecContext& exec_ctx, ObIDataSource*& data_source) const;

protected:
  bool in_root_job_;
};

class ObSerialReceive : public ObAsyncReceive {
protected:
  class ObSerialReceiveCtx : public ObAsyncReceiveCtx {
    friend class ObSerialReceive;

  public:
    explicit ObSerialReceiveCtx(ObExecContext& exec_ctx);
    virtual ~ObSerialReceiveCtx();
    virtual void destroy();

  protected:
    ObIDataSource* data_source_;
  };

public:
  explicit ObSerialReceive(common::ObIAllocator& alloc);
  virtual ~ObSerialReceive();

protected:
  virtual int inner_open(ObExecContext& exec_ctx) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& exec_ctx) const;
};

class ObParallelReceive : public ObAsyncReceive {
protected:
  class ObParallelReceiveCtx : public ObAsyncReceiveCtx {
    friend class ObParallelReceive;
    friend class ObMergeSortReceive;

  public:
    explicit ObParallelReceiveCtx(ObExecContext& exec_ctx);
    virtual ~ObParallelReceiveCtx();
    virtual void destroy();

  protected:
    common::ObSEArray<ObIDataSource*, 8> data_sources_;
    common::ObSEArray<common::ObNewRow, 8> child_rows_;
    common::ObSEArray<int64_t, 8> row_idxs_;
  };

public:
  explicit ObParallelReceive(common::ObIAllocator& alloc);
  virtual ~ObParallelReceive();

protected:
  virtual int inner_open(ObExecContext& exec_ctx) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const = 0;
  virtual int inner_close(ObExecContext& exec_ctx) const;
  int create_new_row(ObExecContext& exec_ctx, const ObNewRow& cur_row, ObNewRow& row) const;
};

class ObFifoReceiveV2 : public ObSerialReceive {
  typedef ObSerialReceiveCtx ObFifoReceiveCtx;

public:
  explicit ObFifoReceiveV2(common::ObIAllocator& alloc);
  virtual ~ObFifoReceiveV2();

protected:
  virtual int create_op_ctx(ObExecContext& exec_ctx, ObAsyncReceiveCtx*& op_ctx) const;
};

class ObTaskOrderReceive : public ObSerialReceive {
  struct ObTaskComparer {
  public:
    ObTaskComparer();
    virtual ~ObTaskComparer();
    bool operator()(const ObTaskResult& task1, const ObTaskResult& task2);
  };
  class ObTaskOrderReceiveCtx : public ObSerialReceiveCtx {
    friend class ObTaskOrderReceive;

  public:
    explicit ObTaskOrderReceiveCtx(ObExecContext& exec_ctx);
    virtual ~ObTaskOrderReceiveCtx();
    virtual void destroy();

  protected:
    common::ObSEArray<ObTaskResult, 8> task_results_;
    ObTaskComparer task_comparer_;
    uint64_t cur_task_id_;
  };

public:
  explicit ObTaskOrderReceive(common::ObIAllocator& alloc);
  virtual ~ObTaskOrderReceive();

protected:
  virtual int create_op_ctx(ObExecContext& exec_ctx, ObAsyncReceiveCtx*& op_ctx) const;
  virtual int get_next_task_result(ObAsyncReceiveCtx& op_ctx, ObTaskResult& task_result) const;

private:
  int get_next_task_result_root(ObAsyncReceiveCtx& op_ctx, ObTaskResult& task_result) const;
  int get_next_task_result_distributed(ObAsyncReceiveCtx& op_ctx, ObTaskResult& task_result) const;
};

class ObMergeSortReceive : public ObParallelReceive, public ObSortableTrait {
  OB_UNIS_VERSION_V(1);

private:
  struct ObRowComparer {
  public:
    ObRowComparer();
    virtual ~ObRowComparer();
    void init(const common::ObIArray<ObSortColumn>& columns, const common::ObIArray<ObNewRow>& rows);
    bool operator()(int64_t row_idx1, int64_t row_idx2);
    int get_ret() const
    {
      return ret_;
    }

  private:
    const common::ObIArray<ObSortColumn>* columns_;
    const common::ObIArray<ObNewRow>* rows_;
    int ret_;
  };
  class ObMergeSortReceiveCtx : public ObParallelReceiveCtx {
    friend class ObMergeSortReceive;

  public:
    explicit ObMergeSortReceiveCtx(ObExecContext& exec_ctx);
    virtual ~ObMergeSortReceiveCtx();

  private:
    ObRowComparer row_comparer_;
    int64_t last_row_idx_;
  };

public:
  explicit ObMergeSortReceive(common::ObIAllocator& alloc);
  virtual ~ObMergeSortReceive();

protected:
  virtual int create_op_ctx(ObExecContext& exec_ctx, ObAsyncReceiveCtx*& op_ctx) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
};

class ObFifoReceiveInput : public ObReceiveInput {
  friend class ObFifoReceive;
  OB_UNIS_VERSION_V(1);

public:
  ObFifoReceiveInput(){};
  virtual ~ObFifoReceiveInput()
  {}
  virtual void reset() override
  {}
  virtual ObPhyOperatorType get_phy_op_type() const;
};

class ObDistributedScheduler;
class ObFifoReceive : public ObReceive, public ObSortableTrait {
private:
  struct MergeRowComparer;
  struct MergeSortHandle {
    MergeSortHandle()
        : stream_handler_(common::ObModIds::OB_SQL_EXECUTOR),
          merge_id_(0),
          row_iter_(),
          curhandler_scanner_(common::ObModIds::OB_SQL_EXECUTOR_MERGE_SORT_SCANNER)
    {}
    ~MergeSortHandle()
    {}
    virtual void destroy()
    {
      stream_handler_.~FetchIntermResultStreamHandle();
      curhandler_scanner_.~ObScanner();
    }
    void reset()
    {
      stream_handler_.reset();
      merge_id_ = 0;
      curhandler_scanner_.reset();
    }
    TO_STRING_KV("merge id", merge_id_);
    FetchIntermResultStreamHandle stream_handler_;
    int64_t merge_id_;  // pos in merge_handles
    common::ObRowStore::Iterator row_iter_;
    common::ObScanner curhandler_scanner_;
  };

  struct ObSortRow {
    ObSortRow() : row_(NULL), pos_(0)
    {}
    TO_STRING_KV("pos", pos_);
    common::ObNewRow* row_;
    int64_t pos_;
  };

  class ObFifoReceiveCtx : public ObReceiveCtx {
    friend class ObFifoReceive;

  public:
    explicit ObFifoReceiveCtx(ObExecContext& ctx);
    virtual ~ObFifoReceiveCtx();
    virtual void destroy()
    {
      stream_handler_.~FetchIntermResultStreamHandle();
      old_stream_handler_.~FetchResultStreamHandle();
      cur_scanner_.~ObScanner();
      for (int64_t i = 0; i < merge_handles_.count(); ++i) {
        if (NULL != merge_handles_.at(i)) {
          merge_handles_.at(i)->~MergeSortHandle();
        }
      }
      merge_handles_.destroy();
      heap_sort_rows_.destroy();
      ObReceiveCtx::destroy();
    }

  private:
    int64_t found_rows_;
    FetchIntermResultStreamHandle stream_handler_;
    FetchResultStreamHandle old_stream_handler_;
    common::ObScanner cur_scanner_;
    common::ObRowStore::Iterator cur_scanner_iter_;
    ObDistributedSchedulerManager::ObDistributedSchedulerHolder scheduler_holder_;
    bool iter_has_started_;
    bool iter_end_;
    common::ObArenaAllocator allocator_;
    common::ObList<ObTaskResult, common::ObArenaAllocator> waiting_finish_tasks_;
    uint64_t last_pull_task_id_;
    int64_t affected_row_;
    common::ObNewRow* row_i_;  // cur row from merge sort
    common::ObNewRow child_row_;
    MergeSortHandle* cur_merge_handle_;
    common::ObSEArray<MergeSortHandle*, 8> merge_handles_;
    common::ObSEArray<ObSortRow, 8> heap_sort_rows_;
  };

public:
  explicit ObFifoReceive(common::ObIAllocator& aloc);
  virtual ~ObFifoReceive();
  virtual int create_operator_input(ObExecContext& ctx) const;
  virtual int rescan(ObExecContext& ctx) const;

private:
  /* functions */
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& ctx) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;

  int fetch_more_result(
      ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const;
  int new_fetch_more_result(
      ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const;
  int old_fetch_more_result(
      ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const;
  int check_schedule_status(ObDistributedScheduler* scheduler) const;

  int deal_with_insert(ObExecContext& ctx) const;

  // for merge sort
  int merge_sort_result(
      ObExecContext& ctx, ObFifoReceiveCtx* fifo_receive_ctx, const ObFifoReceiveInput* fifo_receive_input) const;
  int deal_with_task(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx, MergeSortHandle& sort_handler,
      ObTaskResult& task_result, bool& result_scanner_is_empty, const ObFifoReceiveInput& fifo_receive_input) const;
  int fetch_a_new_scanner(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx, MergeSortHandle& sort_handler,
      const ObFifoReceiveInput& fifo_receive_input) const;
  int partition_order_fetch_task(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx,
      ObDistributedScheduler* scheduler, int64_t timeout_timestamp, ObTaskLocation& task_loc,
      const ObFifoReceiveInput& fifo_receive_input) const;
  int create_new_cur_row(ObExecContext& ctx, const common::ObNewRow& cur_row, common::ObNewRow*& row_i) const;
  int first_get_row(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx) const;
  int get_row_from_heap(ObFifoReceiveCtx& fifo_receive_ctx) const;
  int more_get_scanner(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx, MergeSortHandle& sort_handler,
      const ObFifoReceiveInput& fifo_receive_input) const;
  int get_row_from_scanner(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx, MergeSortHandle& sort_handler,
      const ObFifoReceiveInput& fifo_receive_input) const;
  int init_row_heap(ObExecContext& ctx, ObFifoReceiveCtx& fifo_receive_ctx) const;
  int pop_a_row_from_heap(ObFifoReceiveCtx& fifo_receive_ctx, ObSortRow& row) const;
  int push_a_row_into_heap(ObFifoReceiveCtx& fifo_receive_ctx, ObSortRow& row) const;
  TO_STRING_KV(N_ORDER_BY, sort_columns_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFifoReceive);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_FIFO_RECEIVE_ */
//// end of header file
