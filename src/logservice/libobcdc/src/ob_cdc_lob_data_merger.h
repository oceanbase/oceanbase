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

#ifndef OCEANBASE_LIBOBCDC_LOB_DATA_MERGER_H_
#define OCEANBASE_LIBOBCDC_LOB_DATA_MERGER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "ob_cdc_lob_ctx.h"    // ObLobDataOutRowCtxList

namespace oceanbase
{
namespace libobcdc
{
class IObCDCLobDataMerger
{
public:
  enum
  {
    MAX_PARSER_NUM = 32
  };

public:
  virtual ~IObCDCLobDataMerger() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;

  virtual int push(ObLobDataOutRowCtxList &task, volatile bool &stop_flag) = 0;
  virtual void get_task_count(int64_t &lob_data_list_task_count) const = 0;
  virtual void print_stat_info() = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////

class IObLogErrHandler;

typedef common::ObMQThread<IObCDCLobDataMerger::MAX_PARSER_NUM> LobDataMergerThread;

class ObCDCLobDataMerger : public IObCDCLobDataMerger, public LobDataMergerThread
{
public:
  ObCDCLobDataMerger();
  virtual ~ObCDCLobDataMerger();

public:
  int start();
  void stop();
  void mark_stop_flag() { LobDataMergerThread::mark_stop_flag(); }
  int push(ObLobDataOutRowCtxList &task, volatile bool &stop_flag);
  void get_task_count(int64_t &lob_data_list_task_count) const
  {
    lob_data_list_task_count = ATOMIC_LOAD(&lob_data_list_task_count_);
  }
  void print_stat_info() {}
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  int init(
      const int64_t thread_num,
      const int64_t queue_size,
      IObLogErrHandler &err_handler);
  void destroy();

private:
  int push_task_(
      ObLobDataOutRowCtxList &task,
      volatile bool &stop_flag);
  int push_lob_column_(
      ObIAllocator &allocator,
      ObLobDataOutRowCtxList &task,
      ObLobDataGetCtx &lob_data_get_ctx,
      volatile bool &stop_flag);
  int check_empty_outrow_lob_col_(
      ObLobDataGetCtx &lob_data_get_ctx,
      uint32_t seq_no_cnt,
      uint32_t del_seq_no_cnt,
      bool &is_update_outrow_lob_from_empty_to_empty);
  int get_lob_col_fra_ctx_list_(
      const bool is_new_col,
      const transaction::ObTxSEQ &seq_no_start,
      const uint32_t seq_no_cnt,
      ObIAllocator &allocator,
      ObLobDataGetCtx &lob_data_get_ctx,
      LobColumnFragmentCtxList &lob_col_fra_ctx_list);
  int push_lob_col_fra_ctx_list_(
      LobColumnFragmentCtxList &lob_col_fra_ctx_list,
      volatile bool &stop_flag);
  int handle_task_(
      LobColumnFragmentCtx &task,
      const int64_t thread_index,
      volatile bool &stop_flag);
  int handle_when_outrow_log_fragment_progress_done_(
      LobColumnFragmentCtx &task,
      ObLobDataGetCtx &lob_data_get_ctx,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  int try_to_push_task_into_formatter_(
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  int merge_fragments_(
      LobColumnFragmentCtx &task,
      ObLobDataGetCtx &lob_data_get_ctx,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      ObString &data);
  int after_fragment_progress_done_(
      ObLobDataGetCtx &lob_data_get_ctx,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  // ext info log handle
  int handle_ext_info_log_(
      ObLobDataGetCtx &lob_data_get_ctx,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      const ObString &src_data,
      ObString &format_data);
  int handle_json_diff_ext_info_log_(
      ObIAllocator &allocator,
      const char *buf, uint64_t len, int64_t pos,
      ObString &format_data);

  bool is_in_stop_status(volatile bool stop_flag) const { return stop_flag || LobDataMergerThread::is_stoped(); }
  // TODO
  void print_task_count_();

private:
  bool                      is_inited_;
  // Used to ensure that tasks are evenly distributed to threads
  uint64_t                  round_value_;
  // ObLobDataOutRowCtxList
  int64_t                   lob_data_list_task_count_ CACHE_ALIGNED;

  IObLogErrHandler          *err_handler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCLobDataMerger);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
