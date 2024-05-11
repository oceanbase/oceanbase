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

#ifndef OCEANBASE_LOG_FETCH_LOG_RPC_SPLITTER_H_
#define OCEANBASE_LOG_FETCH_LOG_RPC_SPLITTER_H_

#include "ob_log_fetch_log_rpc_req.h"

namespace oceanbase
{

namespace logfetcher
{

class IFetchLogRpcSplitter
{
public:
  virtual ~IFetchLogRpcSplitter() = 0;
  virtual int split(RawLogFileRpcRequest &rpc_rep) = 0;
  virtual int update_stat(const int32_t cur_valid_rpc_cnt) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObLogFetchLogRpcSplitter: public IFetchLogRpcSplitter
{
  static constexpr int64_t STAT_WINDOW_SIZE = 4;
public:
  virtual int split(RawLogFileRpcRequest &rpc_rep);
  virtual int update_stat(const int32_t cur_valid_rpc_cnt);
public:
  explicit ObLogFetchLogRpcSplitter(const int32_t max_sub_rpc_cnt);
  ~ObLogFetchLogRpcSplitter() { destroy(); }
  void reset();
  void destroy() { reset(); }
  VIRTUAL_TO_STRING_KV(
    K(max_sub_rpc_cnt_)
  );
private:
  // must be the power of 2, and less than 64MB / 4K
  // related to the buffer size of rpc response
  int32_t max_sub_rpc_cnt_;
  int64_t cur_pos_;
  int32_t valid_rpc_cnt_window_[STAT_WINDOW_SIZE];
};

}

}

#endif