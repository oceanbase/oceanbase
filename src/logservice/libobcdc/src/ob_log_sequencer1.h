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

#ifndef OCEANBASE_LIBOBCDC_SEQUENCER_H__
#define OCEANBASE_LIBOBCDC_SEQUENCER_H__

#include <queue>                                    // std::priority_queue
#include <vector>                                   // std::vector
#include "lib/allocator/ob_allocator.h"             // ObIAllocator
#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "lib/thread/thread_pool.h"                 // lib::ThreadPool
#include "lib/lock/ob_small_spin_lock.h"            // ObByteLock

#include "ob_log_trans_ctx.h"                       // TransCtx
#include "ob_log_part_trans_task.h"                 // PartTransTask
#include "ob_log_trans_redo_dispatcher.h"           // ObLogRedoDispatcher
#include "ob_log_trans_msg_sorter.h"                // ObLogTransMsgSorter
#include "ob_log_schema_incremental_replay.h"       // ObLogSchemaIncReplay

namespace oceanbase
{
namespace common
{
class ObString;
}

namespace libobcdc
{
class ObLogConfig;
/////////////////////////////////////////////////////////////////////////////////////////
// IObLogSequencer

class IObLogSequencer
{
public:
  enum
  {
    MAX_SEQUENCER_NUM = 64,
    GET_SCHEMA_TIMEOUT = 1 * 1000 * 1000,
  };

  struct SeqStatInfo
  {
    SeqStatInfo() { reset(); }
    ~SeqStatInfo() { reset(); }

    void reset()
    {
      total_part_trans_task_count_ = 0;
      ddl_part_trans_task_count_ = 0;
      dml_part_trans_task_count_ = 0;
      hb_part_trans_task_count_ = 0;
      queue_part_trans_task_count_ = 0;
      ready_trans_count_ = 0;
      sequenced_trans_count_ = 0;
    }
    int64_t total_part_trans_task_count_ CACHE_ALIGNED;
    int64_t ddl_part_trans_task_count_ CACHE_ALIGNED;
    int64_t dml_part_trans_task_count_ CACHE_ALIGNED;
    int64_t hb_part_trans_task_count_ CACHE_ALIGNED;
    int64_t queue_part_trans_task_count_ CACHE_ALIGNED;
    int64_t ready_trans_count_ CACHE_ALIGNED;
    int64_t sequenced_trans_count_ CACHE_ALIGNED;
  };

public:
  virtual ~IObLogSequencer() {}
  virtual void configure(const ObLogConfig &config) = 0;

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  // ObLogFetcherDispatcher single-threaded call
  // ObLogSysLsTaskHandler
  // 1. DDL/DML partitioning transaction task
  // 2. Global heartbeat
  virtual int push(PartTransTask *task, volatile bool &stop_flag) = 0;
  virtual void get_task_count(SeqStatInfo &stat_info) = 0;
  virtual int64_t get_thread_num() const = 0;
};


/////////////////////////////////////////////////////////////////////////////////////////

class IObLogTransCtxMgr;
class IObLogTransStatMgr;
class IObLogCommitter;
class IObLogErrHandler;
class ObLogTenant;

typedef common::ObMQThread<IObLogSequencer::MAX_SEQUENCER_NUM, IObLogSequencer> SequencerThread;

// ObClockGenerator
class ObLogSequencer : public IObLogSequencer, public SequencerThread, public lib::ThreadPool
{
public:
  ObLogSequencer();
  virtual ~ObLogSequencer();

public:
  static bool g_print_participant_not_serve_info;
  void configure(const ObLogConfig &config);

public:
  int start();
  void stop();
  void mark_stop_flag() {
    SequencerThread::mark_stop_flag();
    lib::ThreadPool::stop();
  }
  int push(PartTransTask *task, volatile bool &stop_flag);
  void get_task_count(SeqStatInfo &stat_info);
  int64_t get_thread_num() const { return SequencerThread::get_thread_num(); }
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  int init(
      const int64_t thread_num,
      const int64_t queue_size,
      IObLogTransCtxMgr &trans_ctx_mgr,
      IObLogTransStatMgr &trans_stat_mgr,
      IObLogCommitter &trans_committer,
      IObLogTransRedoDispatcher &redo_dispatcher,
      IObLogTransMsgSorter &br_sorter,
      IObLogErrHandler &err_handler);
  void destroy();

private:
  static const int64_t PRINT_SEQ_INFO_INTERVAL = 10 * _SEC_;
  static const int64_t DATA_OP_TIMEOUT = 1 * _SEC_;
  static const int64_t WAIT_TIMEOUT = 10 * _SEC_;
  typedef libobcdc::TransCtxSortElement TrxSortElem;
  typedef libobcdc::TransCtxSortElement::TransCtxCmp TrxCmp;
  typedef std::priority_queue<TrxSortElem, std::vector<TrxSortElem>, TrxCmp> ReadyTransQueue;
  typedef ObFixedQueue<TransCtx> SeqTransQueue;

private:
  void run1() final;
  // try push trans into seq_trans_queue which trans_version is less than checkpoint
  int push_ready_trans_to_seq_queue_();
  int handle_trans_in_seq_queue_();
  int handle_sequenced_trans_(
      TransCtx *trans_ctx,
      volatile bool &stop_flag);
  int handle_global_hb_part_trans_task_(PartTransTask &part_trans_task,
      volatile bool &stop_flag);
  int handle_part_trans_task_(PartTransTask &part_trans_task,
      volatile bool &stop_flag);

  // First prepare the transaction context
  // If it is confirmed that the partitioned transaction is not serviced, the transaction context returned is empty
  // Note: if a partitioned transaction is returned as being in service, this does not mean that the partitioned transaction is necessarily in service,
  // and the final confirmation of whether the partitioned transaction is in service will have to wait until it is added to the list of participants
  int prepare_trans_ctx_(PartTransTask &part_trans_task,
      bool &is_part_trans_served,
      TransCtx *&trans_ctx,
      volatile bool &stop_flag);
  int handle_not_served_trans_(PartTransTask &part_trans_task, volatile bool &stop_flag);
  int handle_participants_ready_trans_(const bool is_dml_trans,
      TransCtx *trans_ctx,
      volatile bool &stop_flag);
  // Once the participants are gathered, the entire DML transaction is processed
  // TODO
  int handle_dml_trans_(ObLogTenant &tenant, TransCtx &trans_ctx, volatile bool &stop_flag);
  // need sort_partition = 1. TODO: consider always sort participants
  int handle_ddl_trans_(ObLogTenant &tenant, TransCtx &trans_ctx, volatile bool &stop_flag);
  int handle_multi_data_source_info_(ObLogTenant &tenant, TransCtx &trans_ctx, volatile bool &stop_flag);
  int handle_ddl_multi_data_source_info_(
      PartTransTask &part_trans_task,
      ObLogTenant &tenant,
      TransCtx &trans_ctx);
  // wait reader/parser module empty
  int wait_until_parser_done_(
      const char *caller,
      volatile bool &stop_flag);
  // wait reader/parser/formatter module empty
  int wait_until_formatter_done_(volatile bool &stop_flag);
  int recycle_resources_after_trans_ready_(TransCtx &trans_ctx, ObLogTenant &tenant, volatile bool &stop_flag);
  int push_task_into_br_sorter_(TransCtx &trans_ctx, volatile bool &stop_flag);
  int push_task_into_redo_dispatcher_(TransCtx &trans_ctx, volatile bool &stop_flag);
  int push_task_into_committer_(PartTransTask *task,
      const int64_t task_count,
      volatile bool &stop_flag,
      ObLogTenant *tenant);
  void do_stat_for_part_trans_task_count_(
      PartTransTask &part_trans_task,
      const int64_t task_count,
      const bool is_sub_stat);

  // TODO add
  // 1. statistics on transaction tps and rps (rps before and after Formatter filtering)
  // 2. count tenant rps information
  int do_trans_stat_(const uint64_t tenant_id, const int64_t total_stmt_cnt);

private:
  bool                      inited_;
  uint64_t                  round_value_;
  uint64_t                  heartbeat_round_value_;

  IObLogTransCtxMgr         *trans_ctx_mgr_;
  IObLogTransStatMgr        *trans_stat_mgr_;
  IObLogCommitter           *trans_committer_;
  IObLogTransRedoDispatcher *redo_dispatcher_;
  IObLogTransMsgSorter      *msg_sorter_;
  IObLogErrHandler          *err_handler_;
  ObLogSchemaIncReplay      schema_inc_replay_;

  int64_t                   global_checkpoint_ CACHE_ALIGNED;
  int64_t                   last_global_checkpoint_ CACHE_ALIGNED;
  uint64_t                  global_seq_ CACHE_ALIGNED;
  uint64_t                  br_committer_queue_seq_ CACHE_ALIGNED;
  // Store assembled distributed transactions
  ReadyTransQueue           ready_trans_queue_;
  common::ObByteLock        trans_queue_lock_;
  // Store transactions that can be outputted (trans commit_version greater than the global checkpoint).
  SeqTransQueue             seq_trans_queue_;
  common::ObCond            checkpoint_cond_;
  common::ObCond            ready_queue_cond_;
  common::ObCond            seq_queue_cond_;

  // Counting the number of partitioned tasks owned by Sequencer
  int64_t                   total_part_trans_task_count_ CACHE_ALIGNED;
  int64_t                   ddl_part_trans_task_count_ CACHE_ALIGNED;
  int64_t                   dml_part_trans_task_count_ CACHE_ALIGNED;
  int64_t                   hb_part_trans_task_count_ CACHE_ALIGNED;
  // Counting the number of partitioned tasks owned by the Sequencer queue
  int64_t                   queue_part_trans_task_count_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSequencer);
};


} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_SEQUENCER_H__ */
