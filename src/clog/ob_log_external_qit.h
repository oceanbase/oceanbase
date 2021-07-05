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

#ifndef OCEANBASE_CLOG_OB_LOG_EXTERNAL_QIT_
#define OCEANBASE_CLOG_OB_LOG_EXTERNAL_QIT_

#include "lib/time/ob_time_utility.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace extlog {

// Quit In Time
class ObExtRpcQit {
public:
  enum ExtRpcType { RPC_INVALID_TYPE = 0, RPC_LOCATE_BY_TS = 1, RPC_LOCATE_BY_ID = 2, RPC_TYPE_MAX = 3 };

public:
  ObExtRpcQit() : is_inited_(false), type_(RPC_INVALID_TYPE), deadline_(0)
  {}
  ~ObExtRpcQit()
  {}
  int init(const ExtRpcType& type, const int64_t deadline);

  // check enough_time_to_handle when receive the req from rpc queue
  inline int enough_time_to_handle(bool& enough_to_handle) const
  {
    return enough_time(get_handle_time(), enough_to_handle);
  }
  // check should_hurry_quit when start to perform a time-consuming operation
  int should_hurry_quit(bool& is_hurry_quit) const;

  TO_STRING_KV(K(is_inited_), K(type_), K(deadline_));

private:
  inline int64_t cur_ts() const
  {
    return common::ObTimeUtility::current_time();
  }
  bool is_valid_rpc_type(const ExtRpcType& type) const
  {
    return type >= RPC_LOCATE_BY_TS && type <= RPC_LOCATE_BY_ID;
  }
  int64_t get_handle_time() const;
  int enough_time(const int64_t reserved_interval, bool& is_enough) const;

public:
  static const int64_t MILLI_SECOND = 1000;
  static const int64_t SECOND = 1000 * 1000;
  static const int64_t HANDLE_TIME_INVALD = 0;
  static const int64_t HANDLE_TIME_TS = 1 * SECOND;
  static const int64_t HANDLE_TIME_ID = 1 * SECOND;
  static const int64_t HURRY_QUIT_RESERVED = 100 * MILLI_SECOND;

private:
  bool is_inited_;
  ExtRpcType type_;
  int64_t deadline_;
  DISALLOW_COPY_AND_ASSIGN(ObExtRpcQit);
};

}  // namespace extlog
}  // namespace oceanbase

#endif
