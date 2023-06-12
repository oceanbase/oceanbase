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

#include "ob_log_fetch_stat_info.h"

#include "lib/utility/ob_print_utils.h"   // databuff_printf
#include "ob_log_utils.h"                 // SIZE_TO_STR

using namespace oceanbase::common;

namespace oceanbase
{
namespace logfetcher
{

///////////////////////////// TransStatInfo ///////////////////////////
void TransStatInfo::reset()
{
  decode_header_time_ = 0;

  redo_cnt_ = 0;
  redo_size_ = 0;
  read_redo_time_ = 0;
  read_redo_decode_time_ = 0;
  read_redo_parse_time_ = 0;

  prepare_cnt_ = 0;
  prepare_size_ = 0;
  prepare_with_redo_cnt_ = 0;
  read_prepare_time_ = 0;
  read_prepare_decode_time_ = 0;
  read_prepare_parse_time_ = 0;

  commit_cnt_ = 0;
  commit_size_ = 0;
  commit_with_prepare_cnt_ = 0;
  participant_cnt_ = 0;
  read_commit_time_ = 0;
  read_commit_decode_time_ = 0;
  read_commit_parse_time_ = 0;

  sp_redo_cnt_ = 0;
  sp_redo_size_ = 0;
  read_sp_redo_time_ = 0;
  read_sp_redo_decode_time_ = 0;
  read_sp_redo_parse_time_ = 0;

  sp_commit_cnt_ = 0;
  sp_commit_size_ = 0;
  sp_commit_with_redo_cnt_ = 0;
  read_sp_commit_time_ = 0;
  read_sp_commit_decode_time_ = 0;
  read_sp_commit_parse_time_ = 0;

  clear_cnt_ = 0;
  clear_size_ = 0;
}

void TransStatInfo::update(const TransStatInfo &tsi)
{
  decode_header_time_ += tsi.decode_header_time_;

  redo_cnt_ += tsi.redo_cnt_;
  redo_size_ += tsi.redo_size_;
  read_redo_time_ += tsi.read_redo_time_;
  read_redo_decode_time_ += tsi.read_redo_decode_time_;
  read_redo_parse_time_ += tsi.read_redo_parse_time_;

  prepare_cnt_ += tsi.prepare_cnt_;
  prepare_size_ += tsi.prepare_size_;
  prepare_with_redo_cnt_ += tsi.prepare_with_redo_cnt_;
  read_prepare_time_ += tsi.read_prepare_time_;
  read_prepare_decode_time_ += tsi.read_prepare_decode_time_;
  read_prepare_parse_time_ += tsi.read_prepare_parse_time_;

  commit_cnt_ += tsi.commit_cnt_;
  commit_size_ += tsi.commit_size_;
  commit_with_prepare_cnt_ += tsi.commit_with_prepare_cnt_;
  read_commit_time_ += tsi.read_commit_time_;
  read_commit_decode_time_ += tsi.read_commit_decode_time_;
  read_commit_parse_time_ += tsi.read_commit_parse_time_;

  sp_redo_cnt_ += tsi.sp_redo_cnt_;
  sp_redo_size_ += tsi.sp_redo_size_;
  read_sp_redo_time_ += tsi.read_sp_redo_time_;
  read_sp_redo_decode_time_ += tsi.read_sp_redo_decode_time_;
  read_sp_redo_parse_time_ += tsi.read_sp_redo_parse_time_;

  sp_commit_cnt_ += tsi.sp_commit_cnt_;
  sp_commit_size_ += tsi.sp_commit_size_;
  sp_commit_with_redo_cnt_ += tsi.sp_commit_with_redo_cnt_;
  read_sp_commit_time_ += tsi.read_sp_commit_time_;
  read_sp_commit_decode_time_ += tsi.read_sp_commit_decode_time_;
  read_sp_commit_parse_time_ += tsi.read_sp_commit_parse_time_;

  clear_cnt_ += tsi.clear_cnt_;
  clear_size_ += tsi.clear_size_;
}

TransStatInfo TransStatInfo::operator - (const TransStatInfo &tsi) const
{
  TransStatInfo ret_tsi;

  ret_tsi.decode_header_time_ = decode_header_time_ - tsi.decode_header_time_;

  ret_tsi.redo_cnt_ = redo_cnt_ - tsi.redo_cnt_;
  ret_tsi.redo_size_ = redo_size_ - tsi.redo_size_;
  ret_tsi.read_redo_time_ = read_redo_time_ - tsi.read_redo_time_;
  ret_tsi.read_redo_decode_time_ = read_redo_decode_time_ - tsi.read_redo_decode_time_;
  ret_tsi.read_redo_parse_time_ = read_redo_parse_time_ - tsi.read_redo_parse_time_;

  ret_tsi.prepare_cnt_ = prepare_cnt_ - tsi.prepare_cnt_;
  ret_tsi.prepare_size_ = prepare_size_ - tsi.prepare_size_;
  ret_tsi.prepare_with_redo_cnt_ = prepare_with_redo_cnt_ - tsi.prepare_with_redo_cnt_;
  ret_tsi.read_prepare_time_ = read_prepare_time_ - tsi.read_prepare_time_;
  ret_tsi.read_prepare_decode_time_ = read_prepare_decode_time_ - tsi.read_prepare_decode_time_;
  ret_tsi.read_prepare_parse_time_ = read_prepare_parse_time_ - tsi.read_prepare_parse_time_;

  ret_tsi.commit_cnt_ = commit_cnt_ - tsi.commit_cnt_;
  ret_tsi.commit_size_ = commit_size_ - tsi.commit_size_;
  ret_tsi.commit_with_prepare_cnt_ = commit_with_prepare_cnt_ - tsi.commit_with_prepare_cnt_;
  ret_tsi.read_commit_time_ = read_commit_time_ - tsi.read_commit_time_;
  ret_tsi.read_commit_decode_time_ = read_commit_decode_time_ - tsi.read_commit_decode_time_;
  ret_tsi.read_commit_parse_time_ = read_commit_parse_time_ - tsi.read_commit_parse_time_;

  ret_tsi.sp_redo_cnt_ = sp_redo_cnt_ - tsi.sp_redo_cnt_;
  ret_tsi.sp_redo_size_ = sp_redo_size_ - tsi.sp_redo_size_;
  ret_tsi.read_sp_redo_time_ = read_sp_redo_time_ - tsi.read_sp_redo_time_;
  ret_tsi.read_sp_redo_decode_time_ = read_sp_redo_decode_time_ - tsi.read_sp_redo_decode_time_;
  ret_tsi.read_sp_redo_parse_time_ = read_sp_redo_parse_time_ - tsi.read_sp_redo_parse_time_;

  ret_tsi.sp_commit_cnt_ = sp_commit_cnt_ - tsi.sp_commit_cnt_;
  ret_tsi.sp_commit_size_ = sp_commit_size_ - tsi.sp_commit_size_;
  ret_tsi.sp_commit_with_redo_cnt_ = sp_commit_with_redo_cnt_ - tsi.sp_commit_with_redo_cnt_;
  ret_tsi.read_sp_commit_time_ = read_sp_commit_time_ - tsi.read_sp_commit_time_;
  ret_tsi.read_sp_commit_decode_time_ = read_sp_commit_decode_time_ - tsi.read_sp_commit_decode_time_;
  ret_tsi.read_sp_commit_parse_time_ = read_sp_commit_parse_time_ - tsi.read_sp_commit_parse_time_;

  ret_tsi.clear_cnt_ = clear_cnt_ - tsi.clear_cnt_;
  ret_tsi.clear_size_ = clear_size_ - tsi.clear_size_;

  return ret_tsi;
}

void TransStatInfo::do_stat(const int64_t rpc_cnt)
{
  if (rpc_cnt <= 0) {
    reset();
  } else {
    decode_header_time_ /= rpc_cnt;

    redo_cnt_ /= rpc_cnt;
    redo_size_ /= rpc_cnt;
    read_redo_time_ /= rpc_cnt;
    read_redo_decode_time_ /= rpc_cnt;
    read_redo_parse_time_ /= rpc_cnt;

    prepare_cnt_ /= rpc_cnt;
    prepare_size_ /= rpc_cnt;
    prepare_with_redo_cnt_ /= rpc_cnt;
    read_prepare_time_ /= rpc_cnt;
    read_prepare_decode_time_ /= rpc_cnt;
    read_prepare_parse_time_ /= rpc_cnt;

    commit_cnt_ /= rpc_cnt;
    commit_size_ /= rpc_cnt;
    commit_with_prepare_cnt_ /= rpc_cnt;
    read_commit_time_ /= rpc_cnt;
    read_commit_decode_time_ /= rpc_cnt;
    read_commit_parse_time_ /= rpc_cnt;

    sp_redo_cnt_ /= rpc_cnt;
    sp_redo_size_ /= rpc_cnt;
    read_sp_redo_time_ /= rpc_cnt;
    read_sp_redo_decode_time_ /= rpc_cnt;
    read_sp_redo_parse_time_ /= rpc_cnt;

    sp_commit_cnt_ /= rpc_cnt;
    sp_commit_size_ /= rpc_cnt;
    sp_commit_with_redo_cnt_ /= rpc_cnt;
    read_sp_commit_time_ /= rpc_cnt;
    read_sp_commit_decode_time_ /= rpc_cnt;
    read_sp_commit_parse_time_ /= rpc_cnt;

    clear_cnt_ /= rpc_cnt;
    clear_size_ /= rpc_cnt;
  }
}

int64_t TransStatInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    int64_t total_log_cnt = get_log_cnt();
    int64_t total_size = get_log_size();

    (void)common::databuff_printf(buf, buf_len, pos, "trans_count=%ld", total_log_cnt);

    if (total_log_cnt <= 0) {
      (void)common::databuff_printf(buf, buf_len, pos, " ");
    } else {
      (void)common::databuff_printf(buf, buf_len, pos, "(");

      // redo / prepare / commit
      if (commit_cnt_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos,
            "redo=%ld,prepare=%ld(with_redo=%ld),commit=%ld(with_prepare=%ld),",
            redo_cnt_, prepare_cnt_, prepare_with_redo_cnt_,
            commit_cnt_, commit_with_prepare_cnt_);
      }

      // sp redo / sp commit
      if (sp_commit_cnt_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos, "sp_redo=%ld,sp_commit=%ld,",
            sp_redo_cnt_, sp_commit_cnt_);
      }

      // clear
      if (clear_cnt_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos, "clear=%ld,", clear_cnt_);
      }

      pos--;
      (void)common::databuff_printf(buf, buf_len, pos, ") ");
    }

    (void)common::databuff_printf(buf, buf_len, pos, "trans_size=%s", SIZE_TO_STR(total_size));

    if (total_log_cnt <= 0) {
      (void)common::databuff_printf(buf, buf_len, pos, " ");
    } else {
      (void)common::databuff_printf(buf, buf_len, pos, "(");

      // redo / prepare / commit
      if (commit_cnt_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos, "redo=%s,prepare=%s,commit=%s,",
            SIZE_TO_STR(redo_size_), SIZE_TO_STR(prepare_size_), SIZE_TO_STR(commit_size_));
      }

      // sp redo / sp commit
      if (sp_commit_cnt_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos, "sp_redo=%s,sp_commit=%s,",
            SIZE_TO_STR(sp_redo_size_), SIZE_TO_STR(sp_commit_size_));
      }

      // clear
      if (clear_cnt_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos, "clear=%s,", SIZE_TO_STR(clear_size_));
      }

      pos--;
      (void)common::databuff_printf(buf, buf_len, pos, ") ");
    }

    if (commit_cnt_ > 0) {
      if (read_redo_time_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos,
            "redo_time=%ld(decode=%ld,parse=%ld) ",
            read_redo_time_, read_redo_decode_time_, read_redo_parse_time_);
      }

      if (read_prepare_time_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos,
            "prepare_time=%ld(decode=%ld,parse=%ld) ",
            read_prepare_time_, read_prepare_decode_time_, read_prepare_parse_time_);
      }

      if (read_commit_time_ > 0) {
        (void)common::databuff_printf(buf, buf_len, pos,
            "commit_time=%ld(decode=%ld,parse=%ld) ",
            read_commit_time_, read_commit_decode_time_, read_commit_parse_time_);
      }
    }

    if (sp_commit_cnt_ > 0) {
      (void)common::databuff_printf(buf, buf_len, pos,
          "sp_redo_time=%ld(decode=%ld,parse=%ld) ",
          read_sp_redo_time_, read_sp_redo_decode_time_, read_sp_redo_parse_time_);

      (void)common::databuff_printf(buf, buf_len, pos,
          "sp_commit_time=%ld(decode=%ld,parse=%ld)",
          read_sp_commit_time_, read_sp_commit_decode_time_, read_sp_commit_parse_time_);
    }
  }

  return pos;
}

///////////////////////////////////// FetchStatInfo /////////////////////////////////////

void FetchStatInfo::reset()
{
  fetch_log_cnt_ = 0;
  fetch_log_size_ = 0;
  fetch_log_rpc_cnt_ = 0;
  single_rpc_cnt_ = 0;
  reach_upper_limit_rpc_cnt_ = 0;
  reach_max_log_id_rpc_cnt_ = 0;
  no_log_rpc_cnt_ = 0;
  reach_max_result_rpc_cnt_ = 0;
  fetch_log_rpc_time_ = 0;
  fetch_log_rpc_to_svr_net_time_ = 0;
  fetch_log_rpc_svr_queue_time_ = 0;
  fetch_log_rpc_svr_process_time_ = 0;
  fetch_log_rpc_callback_time_ = 0;
  handle_rpc_time_ = 0;
  handle_rpc_read_log_time_ = 0;
  handle_rpc_flush_time_ = 0;
  read_log_decode_log_entry_time_ = 0;

  tsi_.reset();
}

void FetchStatInfo::update(const FetchStatInfo &fsi)
{
  fetch_log_cnt_ += fsi.fetch_log_cnt_;
  fetch_log_size_ += fsi.fetch_log_size_;
  fetch_log_rpc_cnt_ += fsi.fetch_log_rpc_cnt_;
  single_rpc_cnt_ += fsi.single_rpc_cnt_;
  reach_upper_limit_rpc_cnt_ += fsi.reach_upper_limit_rpc_cnt_;
  reach_max_log_id_rpc_cnt_ += fsi.reach_max_log_id_rpc_cnt_;
  no_log_rpc_cnt_ += fsi.no_log_rpc_cnt_;
  reach_max_result_rpc_cnt_ += fsi.reach_max_result_rpc_cnt_;
  fetch_log_rpc_time_ += fsi.fetch_log_rpc_time_;
  fetch_log_rpc_to_svr_net_time_ += fsi.fetch_log_rpc_to_svr_net_time_;
  fetch_log_rpc_svr_queue_time_ += fsi.fetch_log_rpc_svr_queue_time_;
  fetch_log_rpc_svr_process_time_ += fsi.fetch_log_rpc_svr_process_time_;
  fetch_log_rpc_callback_time_ += fsi.fetch_log_rpc_callback_time_;
  handle_rpc_time_ += fsi.handle_rpc_time_;
  handle_rpc_read_log_time_ += fsi.handle_rpc_read_log_time_;
  handle_rpc_flush_time_ += fsi.handle_rpc_flush_time_;
  read_log_decode_log_entry_time_ += fsi.read_log_decode_log_entry_time_;

  tsi_.update(fsi.tsi_);
}

FetchStatInfo FetchStatInfo::operator - (const FetchStatInfo &fsi) const
{
  FetchStatInfo ret_fsi;

  ret_fsi.fetch_log_cnt_ = fetch_log_cnt_ - fsi.fetch_log_cnt_;
  ret_fsi.fetch_log_size_ = fetch_log_size_ - fsi.fetch_log_size_;
  ret_fsi.fetch_log_rpc_cnt_ = fetch_log_rpc_cnt_ - fsi.fetch_log_rpc_cnt_;
  ret_fsi.single_rpc_cnt_ = single_rpc_cnt_ - fsi.single_rpc_cnt_;
  ret_fsi.reach_upper_limit_rpc_cnt_ = reach_upper_limit_rpc_cnt_ - fsi.reach_upper_limit_rpc_cnt_;
  ret_fsi.reach_max_log_id_rpc_cnt_ = reach_max_log_id_rpc_cnt_ - fsi.reach_max_log_id_rpc_cnt_;
  ret_fsi.no_log_rpc_cnt_ = no_log_rpc_cnt_ - fsi.no_log_rpc_cnt_;
  ret_fsi.reach_max_result_rpc_cnt_ = reach_max_result_rpc_cnt_ - fsi.reach_max_result_rpc_cnt_;
  ret_fsi.fetch_log_rpc_time_ = fetch_log_rpc_time_ - fsi.fetch_log_rpc_time_;
  ret_fsi.fetch_log_rpc_to_svr_net_time_ = fetch_log_rpc_to_svr_net_time_ - fsi.fetch_log_rpc_to_svr_net_time_;
  ret_fsi.fetch_log_rpc_svr_queue_time_ = fetch_log_rpc_svr_queue_time_ - fsi.fetch_log_rpc_svr_queue_time_;
  ret_fsi.fetch_log_rpc_svr_process_time_ = fetch_log_rpc_svr_process_time_ - fsi.fetch_log_rpc_svr_process_time_;
  ret_fsi.fetch_log_rpc_callback_time_ = fetch_log_rpc_callback_time_ - fsi.fetch_log_rpc_callback_time_;
  ret_fsi.handle_rpc_time_ = handle_rpc_time_ - fsi.handle_rpc_time_;
  ret_fsi.handle_rpc_read_log_time_ = handle_rpc_read_log_time_ - fsi.handle_rpc_read_log_time_;
  ret_fsi.handle_rpc_flush_time_ = handle_rpc_flush_time_ - fsi.handle_rpc_flush_time_;
  ret_fsi.read_log_decode_log_entry_time_ = read_log_decode_log_entry_time_ - fsi.read_log_decode_log_entry_time_;

  ret_fsi.tsi_ = tsi_ - fsi.tsi_;

  return ret_fsi;
}

///////////////////////////////// FetchStatInfoPrinter /////////////////////////////////

FetchStatInfoPrinter::FetchStatInfoPrinter(const FetchStatInfo &cur_stat_info,
    const FetchStatInfo &last_stat_info,
    const double delta_second) :
    delta_fsi_(cur_stat_info - last_stat_info),
    delta_second_(delta_second)
{
}

int64_t FetchStatInfoPrinter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (delta_second_ > 0) {
    int64_t log_cnt = delta_fsi_.fetch_log_cnt_;
    int64_t log_size = delta_fsi_.fetch_log_size_;
    int64_t rpc_cnt = delta_fsi_.fetch_log_rpc_cnt_;
    int64_t single_rpc_cnt = delta_fsi_.single_rpc_cnt_;
    int64_t reach_upper_limit_rpc_cnt = delta_fsi_.reach_upper_limit_rpc_cnt_;
    int64_t reach_max_log_id_rpc_cnt = delta_fsi_.reach_max_log_id_rpc_cnt_;
    int64_t no_log_rpc_cnt = delta_fsi_.no_log_rpc_cnt_;
    int64_t reach_max_result_rpc_cnt = delta_fsi_.reach_max_result_rpc_cnt_;
    int64_t rpc_time = delta_fsi_.fetch_log_rpc_time_;
    int64_t svr_queue_time = delta_fsi_.fetch_log_rpc_svr_queue_time_;
    int64_t svr_process_time = delta_fsi_.fetch_log_rpc_svr_process_time_;
    int64_t callback_time = delta_fsi_.fetch_log_rpc_callback_time_;

    // Network time from libobcdc to server
    int64_t l2s_net_time = delta_fsi_.fetch_log_rpc_to_svr_net_time_;

    // The network time from server to libobcdc is calculated and is inaccurate
    // including: observer's outgoing packet queue, libobcdc's incoming packet queue, outgoing packet encoding, incoming packet decoding, and network time
    int64_t s2l_net_time = rpc_time - svr_queue_time - svr_process_time - callback_time -
        l2s_net_time;

    // Total asynchronous processing RPC time
    int64_t handle_rpc_time = delta_fsi_.handle_rpc_time_;

    // Parsing log time
    int64_t read_log_time = delta_fsi_.handle_rpc_read_log_time_;

    // Deserialization log entry time
    int64_t decode_log_entry_time = delta_fsi_.read_log_decode_log_entry_time_;

    // Output Transaction Task Time
    int64_t flush_time = delta_fsi_.handle_rpc_flush_time_;

    // Calculate transaction statistics difference
    TransStatInfo tsi = delta_fsi_.tsi_;

    // Each statistic item is divided by the number of RPC to obtain statistics per RPC
    tsi.do_stat(rpc_cnt);

    int64_t traffic = static_cast<int64_t>(static_cast<double>(log_size) / delta_second_);
    int64_t rpc_cnt_per_sec = static_cast<int64_t>(static_cast<double>(rpc_cnt) / delta_second_);
    int64_t single_rpc_cnt_per_sec =
        static_cast<int64_t>(static_cast<double>(single_rpc_cnt) / delta_second_);
    int64_t reach_upper_limit_rpc_cnt_per_sec =
        static_cast<int64_t>(static_cast<double>(reach_upper_limit_rpc_cnt) / delta_second_);
    int64_t reach_max_log_id_rpc_cnt_per_sec =
        static_cast<int64_t>(static_cast<double>(reach_max_log_id_rpc_cnt) / delta_second_);
    int64_t no_log_rpc_cnt_per_sec =
        static_cast<int64_t>(static_cast<double>(no_log_rpc_cnt) / delta_second_);
    int64_t reach_max_result_rpc_cnt_per_sec =
        static_cast<int64_t>(static_cast<double>(reach_max_result_rpc_cnt) / delta_second_);
    int64_t log_size_per_rpc = rpc_cnt <= 0 ? 0 : log_size / rpc_cnt;
    int64_t log_cnt_per_rpc = rpc_cnt <= 0 ? 0 : log_cnt / rpc_cnt;
    int64_t rpc_time_per_rpc = rpc_cnt <= 0 ? 0 : rpc_time / rpc_cnt;
    int64_t svr_process_time_per_rpc = rpc_cnt <= 0 ? 0 : svr_process_time / rpc_cnt;
    int64_t svr_queue_time_per_rpc = rpc_cnt <= 0 ? 0 : svr_queue_time / rpc_cnt;
    int64_t callback_time_per_rpc = rpc_cnt <= 0 ? 0 : callback_time / rpc_cnt;
    int64_t l2s_net_time_per_rpc = rpc_cnt <= 0 ? 0 : l2s_net_time / rpc_cnt;
    int64_t s2l_net_time_per_rpc = rpc_cnt <= 0 ? 0 : s2l_net_time / rpc_cnt;
    int64_t handle_rpc_time_per_rpc = rpc_cnt <= 0 ? 0 : handle_rpc_time / rpc_cnt;
    int64_t read_log_time_per_rpc = rpc_cnt <= 0 ? 0 : read_log_time / rpc_cnt;
    int64_t decode_log_entry_time_per_rpc = (rpc_cnt <= 0 ? 0 : decode_log_entry_time / rpc_cnt);
    int64_t flush_time_per_rpc = (rpc_cnt <= 0 ? 0 : flush_time / rpc_cnt);


    (void)databuff_printf(buf, buf_len, pos,
        "traffic=%s/sec log_size=%ld size/rpc=%s log_cnt/rpc=%ld "
        "rpc_cnt=%ld(%ld/sec) single_rpc=%ld(%ld/sec)"
        "(upper_limit=%ld(%ld/sec),max_log=%ld(%ld/sec),no_log=%ld(%ld/sec),max_result=%ld(%ld/sec)) "
        "rpc_time=%ld svr_time=(queue=%ld,process=%ld) net_time=(l2s=%ld,s2l=%ld) cb_time=%ld "
        "handle_rpc_time=%ld flush_time=%ld read_log_time=%ld(log_entry=%ld,trans=%ld) %s",
        SIZE_TO_STR(traffic), log_size, SIZE_TO_STR(log_size_per_rpc), log_cnt_per_rpc,
        rpc_cnt, rpc_cnt_per_sec, single_rpc_cnt, single_rpc_cnt_per_sec,
        reach_upper_limit_rpc_cnt, reach_upper_limit_rpc_cnt_per_sec,
        reach_max_log_id_rpc_cnt, reach_max_log_id_rpc_cnt_per_sec,
        no_log_rpc_cnt, no_log_rpc_cnt_per_sec,
        reach_max_result_rpc_cnt, reach_max_result_rpc_cnt_per_sec,
        rpc_time_per_rpc, svr_queue_time_per_rpc, svr_process_time_per_rpc,
        l2s_net_time_per_rpc, s2l_net_time_per_rpc, callback_time_per_rpc,
        handle_rpc_time_per_rpc, flush_time_per_rpc, read_log_time_per_rpc,
        decode_log_entry_time_per_rpc, tsi.get_total_time(), to_cstring(tsi));
  }

  return pos;
}

}
}
