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
 */

#ifndef OCEANBASE_CDC_REQ_STRUCT_H_
#define OCEANBASE_CDC_REQ_STRUCT_H_

#include <cstdint>
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
#include "logservice/logfetcher/ob_log_fetcher_user.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace obrpc
{

enum class FeedbackType
{
  INVALID_FEEDBACK = -1,
  LAGGED_FOLLOWER = 0,         // lagged follower
  LOG_NOT_IN_THIS_SERVER = 1,  // this server does not server this log
  LS_OFFLINED = 2,             // LS offlined
  ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF = 3,   // Reach Max LSN in archive log but cannot switch
                                // to palf because ls not exists in current server
};

enum ObCdcClientType: uint8_t
{
  CLIENT_TYPE_UNKNOWN = 0,
  CLIENT_TYPE_CDC = 1,
  CLIENT_TYPE_STANDBY = 2,
};

class ObCdcRpcTestFlag {
public:
  // rpc request flag bit
  static const int8_t OBCDC_RPC_FETCH_ARCHIVE = 1;
  static const int8_t OBCDC_RPC_TEST_SWITCH_MODE = 1 << 1;

public:
  static bool is_fetch_archive_only(int8_t flag) {
    return flag & OBCDC_RPC_FETCH_ARCHIVE;
  }
  static bool is_test_switch_mode(int8_t flag) {
    return flag & OBCDC_RPC_TEST_SWITCH_MODE;
  }
};

class ObCdcRpcId {
public:
  OB_UNIS_VERSION(1);
public:
  ObCdcRpcId(): client_pid_(0), client_addr_()  {}
  ~ObCdcRpcId() = default;
  int init(const uint64_t pid, const ObAddr &addr);

  void reset() {
    client_pid_ = 0;
    client_addr_.reset();
  }

  bool operator==(const ObCdcRpcId &that) const {
    return client_pid_ == that.client_pid_ &&
           client_addr_ == that.client_addr_;
  }

  bool operator!=(const ObCdcRpcId &that) const {
    return !(*this == that);
  }

  ObCdcRpcId &operator=(const ObCdcRpcId &that) {
    client_pid_ = that.client_pid_;
    client_addr_ = that.client_addr_;
    return *this;
  }

  void set_addr(ObAddr &addr) { client_addr_ = addr; }
  const ObAddr& get_addr() const { return client_addr_; }

  void set_pid(uint64_t pid) { client_pid_ = pid; }
  uint64_t get_pid() const { return client_pid_; }

  TO_STRING_KV(K_(client_addr), K_(client_pid));
private:
  uint64_t client_pid_;
  ObAddr client_addr_;
};

enum ObCdcFetchLogProtocolType
{
  UnknownProto = -1,
  LogGroupEntryProto = 0,
  RawLogDataProto = 1,
};

ObCdcFetchLogProtocolType get_fetch_log_protocol_type(const ObString &proto_type_str);

const char *fetch_log_protocol_type_str(const ObCdcFetchLogProtocolType type);

inline bool is_v1_fetch_log_protocol(const ObCdcFetchLogProtocolType type) {
  return ObCdcFetchLogProtocolType::LogGroupEntryProto == type;
}

inline bool is_v2_fetch_log_protocol(const ObCdcFetchLogProtocolType type) {
  return ObCdcFetchLogProtocolType::RawLogDataProto == type;
}

const char *cdc_client_type_str(const ObCdcClientType type);

const char *feedback_type_str(const FeedbackType feedback);

ObCdcClientType get_client_type_from_user_type(const logfetcher::LogFetcherUser user_type);

}
}

#endif