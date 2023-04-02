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

#ifndef OCEANBASE_OBSERVER_OB_INTERRUPT_RPC_H_
#define OCEANBASE_OBSERVER_OB_INTERRUPT_RPC_H_

#include "lib/ob_errno.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obrpc
{
struct ObInterruptStackInfo
{
  static constexpr int64_t BUF1_SIZE = 128;
public:
  OB_UNIS_VERSION(1);
public:
  ObInterruptStackInfo() : pos1_(0)
  {
    buf1_[0] = '\0';
  }
  ObInterruptStackInfo(const ObInterruptStackInfo &other)
  {
    *this = other;
  }
  ObInterruptStackInfo(int64_t from_tid, const common::ObAddr &from_svr_addr, const char *extra_msg)
      : pos1_(0)
  {
    set_info(from_tid, from_svr_addr, extra_msg);
  }
  // As long as the buff of buf_ is enough, this interface can be continuously expanded, or a new set interface can be added
  void set_info(int64_t from_tid, const common::ObAddr &from_svr_addr, const char *extra_msg)
  {
    char svr_buf[common::MAX_IP_ADDR_LENGTH];
    (void)from_svr_addr.to_string(svr_buf, common::MAX_IP_ADDR_LENGTH);
    (void) common::databuff_printf(
        buf1_, BUF1_SIZE, pos1_,
        "tid:%ld,from:%s,%s",
        from_tid, svr_buf, extra_msg);
  }
  void reset()
  {
    pos1_ = 0;
    buf1_[0] = '\0';
  }
  TO_STRING_KV("msg", buf1_);
private:
  char buf1_[BUF1_SIZE]; // Allow to piggyback text messages up to 128 letters long and end with 0
  int64_t pos1_; // writable position of buf1_
  // NOTE:
  // If you need to expand this structure, please continue to add buf2_, buf3_, etc., do not modify the length of buf1_
  // Otherwise there will be version compatibility issues
  // char buf2_[BUF1_SIZE]; // Allow to piggyback text messages up to 128 letters long and end with 0
  // int64_t pos2_; // actual use length of buf1_
};

};

namespace common
{

// In order to make the diagnosis of interrupts simpler and clearer,
// Need to add the interrupt number, interrupt source, and auxiliary copy in the interrupt information
struct ObInterruptCode
{
public:
  OB_UNIS_VERSION(1);
public:
  ObInterruptCode() : code_(0), info_()
  {
  }
  ObInterruptCode(int code) : code_(code), info_()
  {
  }
  ObInterruptCode(int code, const obrpc::ObInterruptStackInfo &info)
      : code_(code), info_(info)
  {
  }
  ObInterruptCode(int code, int64_t from_tid, const common::ObAddr &from_svr_addr, const char *extra_msg)
      : code_(code), info_(from_tid, from_svr_addr, extra_msg)
  {
  }
  void reset()
  {
    code_ = 0;
    info_.reset();
  }
  int code_; // Interrupt number
  obrpc::ObInterruptStackInfo info_;
  TO_STRING_KV(K_(code), K_(info));
};

}; // ns common

namespace obrpc
{
struct ObInterruptMessage
{
  OB_UNIS_VERSION(1);

public:
  ObInterruptMessage()
      : first_(0), last_(0), code_(0), info_() {};
  ObInterruptMessage(uint64_t first, uint64_t last, int code) :
      first_(first), last_(last), code_(code), info_() {};
  ObInterruptMessage(uint64_t first, uint64_t last, common::ObInterruptCode &code) :
      first_(first), last_(last), code_(code.code_), info_(code.info_) {};
  TO_STRING_KV(K_(first), K_(last), K_(code), K_(info));
  uint64_t first_;
  uint64_t last_;
  // For compatibility, code_ and info_ are not combined into ObInterruptCode
  int code_;
  ObInterruptStackInfo info_;
};

class ObInterruptRpcProxy
    : public ObRpcProxy
{

public:
  DEFINE_TO(ObInterruptRpcProxy);
  RPC_AP(PR1 remote_interrupt_call, OB_REMOTE_INTERRUPT_CALL, (ObInterruptMessage));
};

class ObInterruptProcessor
    : public ObRpcProcessor<ObInterruptRpcProxy::ObRpc<OB_REMOTE_INTERRUPT_CALL>>
{
protected:
  int process();
};
} // namespace obrpc

} // namespace oceanbase

#endif
