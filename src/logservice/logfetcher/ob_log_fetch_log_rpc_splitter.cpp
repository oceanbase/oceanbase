/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * Fetching log-related RPC implementation
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetch_log_rpc_splitter.h"

namespace oceanbase
{
namespace logfetcher
{

IFetchLogRpcSplitter::~IFetchLogRpcSplitter() {}

ObLogFetchLogRpcSplitter::ObLogFetchLogRpcSplitter(const int32_t max_sub_rpc_cnt):
    max_sub_rpc_cnt_(max_sub_rpc_cnt),
    cur_pos_(0)
{
  for (int i = 0; i < STAT_WINDOW_SIZE; i++) {
    valid_rpc_cnt_window_[i] = max_sub_rpc_cnt;
  }
}

void ObLogFetchLogRpcSplitter::reset()
{
  max_sub_rpc_cnt_ = 0;
  cur_pos_ = 0;
  for (int i = 0; i < STAT_WINDOW_SIZE; i++) {
    valid_rpc_cnt_window_[i] = 0;
  }
}

int ObLogFetchLogRpcSplitter::split(RawLogFileRpcRequest &rpc_req)
{
  int ret = OB_SUCCESS;

  if (max_sub_rpc_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("max_sub_rpc_cnt is less or equal than 0, unexpected", KPC(this));
  } else {
    const palf::LSN &start_lsn = rpc_req.start_lsn_;
    const share::ObLSID &ls_id = rpc_req.ls_id_;
    const int64_t progress = rpc_req.progress_;
    int32_t max_valid_rpc_cnt_in_window = 0;
    for (int i = 0; i < STAT_WINDOW_SIZE; i++) {
      max_valid_rpc_cnt_in_window = max(max_valid_rpc_cnt_in_window, valid_rpc_cnt_window_[i]);
    }

    if (max_valid_rpc_cnt_in_window <= 0 || max_valid_rpc_cnt_in_window > max_sub_rpc_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("max_valid_rpc_cnt_in_window is invalid", K(max_valid_rpc_cnt_in_window), K(max_sub_rpc_cnt_));
    } else if (! start_lsn.is_valid() || ! ls_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid start lsn in rawlogfile rpc request", K(rpc_req));
    } else {
      // align start_lsn to 4K
      const palf::LSN aligned_start_lsn = palf::LSN(start_lsn.val_ & ~((1 << 12) - 1));
      const palf::block_id_t file_id = palf::lsn_2_block(aligned_start_lsn, palf::PALF_BLOCK_SIZE);
      const palf::LSN next_file_start_lsn = palf::LSN((file_id + 1) * palf::PALF_BLOCK_SIZE);
      const uint64_t fetch_buffer_len = obrpc::ObCdcFetchRawLogResp::FETCH_BUF_LEN;
      const palf::offset_t fetch_log_size = next_file_start_lsn - aligned_start_lsn;
      //min(ceil(fetch_log_size/fetch_buffer_len), last_valid_rpc_cnt_)
      const int32_t rpc_req_cnt = min(static_cast<int32_t>((fetch_log_size+fetch_buffer_len-1) / fetch_buffer_len),
          static_cast<int32_t>(max_valid_rpc_cnt_in_window));
      palf::LSN cur_start_lsn = aligned_start_lsn;
      for (int i = 0; OB_SUCC(ret) && i < rpc_req_cnt; i++) {
        RawLogDataRpcRequest *req = nullptr;
        int64_t req_size = min(next_file_start_lsn - cur_start_lsn, fetch_buffer_len);
        if (OB_FAIL(rpc_req.alloc_sub_rpc_request(req))) {
          LOG_WARN("failed to alloc sub rpc request", K(rpc_req));
        } else if (OB_ISNULL(req)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null sub rpc request", KP(req));
        } else if (OB_FAIL(req->prepare(ls_id, cur_start_lsn, req_size, progress,
            file_id, i, rpc_req_cnt))) {
          LOG_ERROR("failed to prepare sub rpc request", K(ls_id), K(cur_start_lsn), K(req_size),
            K(progress));
        } else if (OB_FAIL(rpc_req.mark_sub_rpc_req_busy(req))) {
          LOG_ERROR("failed to mark sub rpc request busy", KPC(req));
        } else {
          cur_start_lsn = cur_start_lsn + req_size;
          LOG_TRACE("prepare sub rpc req succ", K(start_lsn), K(aligned_start_lsn),
              K(ls_id), K(file_id), K(next_file_start_lsn), K(fetch_buffer_len), K(rpc_req_cnt),
              KPC(this));
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(req)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(rpc_req.free_sub_rpc_request(req))) {
            LOG_ERROR_RET(tmp_ret, "failed to free sub rpc request");
          }
        }
      }

      if (OB_SUCC(ret)) {
        ATOMIC_STORE(&rpc_req.sub_rpc_req_count_, rpc_req_cnt);
        if (OB_FAIL(rpc_req.free_requests_in_free_list())) {
          LOG_WARN("failed to free requests in free list", K(rpc_req));
        }
      }
    }

  }

  return ret;
}

int ObLogFetchLogRpcSplitter::update_stat(const int32_t cur_valid_rpc_cnt)
{
  int ret = OB_SUCCESS;

  if (cur_valid_rpc_cnt > max_sub_rpc_cnt_ || cur_valid_rpc_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t cur_index = ((cur_pos_ % STAT_WINDOW_SIZE) + STAT_WINDOW_SIZE) % STAT_WINDOW_SIZE;
    valid_rpc_cnt_window_[cur_index] = cur_valid_rpc_cnt;
    cur_pos_++;
  }

  return ret;
}

}
}