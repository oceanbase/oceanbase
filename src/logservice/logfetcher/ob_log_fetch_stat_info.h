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
 * Fetching log statistics
 */

#ifndef OCEANBASE_LOG_FETCHER_FETCH_STAT_INFO_H__
#define OCEANBASE_LOG_FETCHER_FETCH_STAT_INFO_H__

#include "lib/utility/ob_print_utils.h"       // TO_STRING_KV

namespace oceanbase
{
namespace logfetcher
{

// Read transaction log statistics
struct TransStatInfo
{
  // Decoding transaction log header time
  int64_t decode_header_time_;

  // ********** REDO **********
  int64_t redo_cnt_;
  int64_t redo_size_;
  int64_t read_redo_time_;
  // Read redo subprocess time: decode and parse
  int64_t read_redo_decode_time_;
  int64_t read_redo_parse_time_;

  // ********** PREPARE **********
  int64_t prepare_cnt_;
  int64_t prepare_size_;
  int64_t prepare_with_redo_cnt_;
  int64_t read_prepare_time_;
  // Read prepare log subprocess time: decode and parse
  int64_t read_prepare_decode_time_;        // Decode prepare log time
  int64_t read_prepare_parse_time_;         // Parse prepare log time

  // ********** COMMIT **********
  int64_t commit_cnt_;
  int64_t commit_size_;
  int64_t commit_with_prepare_cnt_;
  int64_t participant_cnt_;                 // 每个事务的参与者数量
  int64_t read_commit_time_;
  // Read commmi log subprocess time: decode and parse
  int64_t read_commit_decode_time_;
  int64_t read_commit_parse_time_;

  // ********** SP TRANS REDO **********
  int64_t sp_redo_cnt_;
  int64_t sp_redo_size_;
  int64_t read_sp_redo_time_;
  // Read sp redo subprocess time
  int64_t read_sp_redo_decode_time_;
  int64_t read_sp_redo_parse_time_;

  // ********** SP TRANS COMMIT **********
  int64_t sp_commit_cnt_;
  int64_t sp_commit_size_;
  int64_t sp_commit_with_redo_cnt_;
  int64_t read_sp_commit_time_;
  // Read sp commit subprocess time
  int64_t read_sp_commit_decode_time_;
  int64_t read_sp_commit_parse_time_;

  // ********** CLEAR **********
  int64_t clear_cnt_;
  int64_t clear_size_;

  TransStatInfo() { reset(); }
  void reset();
  void update(const TransStatInfo &tsi);
  TransStatInfo operator - (const TransStatInfo &tsi) const;
  void do_stat(const int64_t rpc_cnt);

  int64_t get_total_time() const
  {
    return decode_header_time_ + get_decode_time() + get_parse_time();
  }

  int64_t get_decode_time() const
  {
    return read_redo_decode_time_ + read_prepare_decode_time_ + read_commit_decode_time_ +
        read_sp_redo_decode_time_ + read_sp_commit_decode_time_;
  }

  int64_t get_parse_time() const
  {
    return read_redo_parse_time_ + read_prepare_parse_time_ + read_commit_parse_time_ +
        read_sp_redo_parse_time_ + read_sp_commit_parse_time_;
  }

  int64_t get_log_cnt() const
  {
    return redo_cnt_ + prepare_cnt_ - prepare_with_redo_cnt_ + commit_cnt_ -
        commit_with_prepare_cnt_ + sp_redo_cnt_ + sp_commit_cnt_ + clear_cnt_;
  }

  int64_t get_log_size() const
  {
    return redo_size_ + prepare_size_ + commit_size_ + sp_redo_size_ + sp_commit_size_ +
        clear_size_;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;
};

///////////////////////////////// FetchStatInfo /////////////////////////////////

// Fetch log overall process statistics
struct FetchStatInfo
{
  int64_t fetch_log_cnt_;           // Number of log entries
  int64_t fetch_log_size_;          // Fetch log size

  ///////////////// RPC相关统计项 ////////////////////
  int64_t fetch_log_rpc_cnt_;       // Number of fetch log rpc

  int64_t single_rpc_cnt_;          // Number of rpc that stop immediately after execution
  int64_t reach_upper_limit_rpc_cnt_; // Number of rpc that reach upper limit
  int64_t reach_max_log_id_rpc_cnt_;  // Number of rpc that reach max log id

  int64_t no_log_rpc_cnt_;            // Number of rpc that without log

  int64_t reach_max_result_rpc_cnt_;  // Number of rpc that reach max result

  // Total time of fetch log RPC: including network, observer processing, asynchronous callback processing
  int64_t fetch_log_rpc_time_;

  // Network time libobcdc to observer
  int64_t fetch_log_rpc_to_svr_net_time_;

  // observer queuing time
  int64_t fetch_log_rpc_svr_queue_time_;

  // observer progressing time
  int64_t fetch_log_rpc_svr_process_time_;

  // RPC local callback processing time
  int64_t fetch_log_rpc_callback_time_;

  // Total log processing time
  int64_t handle_rpc_time_;

  // Processing log flow: time to read logs
  int64_t handle_rpc_read_log_time_;

  // Processing log flow: flush partition transaction operation time
  int64_t handle_rpc_flush_time_;

  // Deserialize log entry time in the read log process
  int64_t read_log_decode_log_entry_time_;

  // Transaction Resolution Statistics
  TransStatInfo tsi_;

  FetchStatInfo() { reset(); }
  void reset();

  // Logs are fetched or RPC is executed
  bool is_valid()
  {
    return fetch_log_cnt_ > 0 || fetch_log_rpc_cnt_ > 0;
  }

  // Update statistical information
  void update(const FetchStatInfo &fsi);
  FetchStatInfo operator - (const FetchStatInfo &fsi) const;

  TO_STRING_KV(K_(fetch_log_cnt),
      K_(fetch_log_size),
      K_(fetch_log_rpc_cnt),
      K_(single_rpc_cnt),
      K_(reach_upper_limit_rpc_cnt),
      K_(reach_max_log_id_rpc_cnt),
      K_(no_log_rpc_cnt),
      K_(reach_max_result_rpc_cnt),
      K_(fetch_log_rpc_time),
      K_(fetch_log_rpc_to_svr_net_time),
      K_(fetch_log_rpc_svr_queue_time),
      K_(fetch_log_rpc_svr_process_time),
      K_(fetch_log_rpc_callback_time),
      K_(handle_rpc_time),
      K_(handle_rpc_read_log_time),
      K_(handle_rpc_flush_time),
      K_(read_log_decode_log_entry_time),
      K_(tsi));
};

///////////////////////////////// FetchStatInfoPrinter /////////////////////////////////

// FetchStatInfo Printers
struct FetchStatInfoPrinter
{
  FetchStatInfoPrinter(const FetchStatInfo &cur_stat_info,
      const FetchStatInfo &last_stat_info,
      const double delta_second);

  int64_t to_string(char* buf, const int64_t buf_len) const;

  FetchStatInfo delta_fsi_;
  const double delta_second_;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchStatInfoPrinter);
};

}
}

#endif
