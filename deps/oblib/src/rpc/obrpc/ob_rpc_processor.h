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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_

#include "lib/runtime.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/frame/ob_req_processor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "rpc/obrpc/ob_rpc_processor_base.h"

namespace oceanbase
{
namespace obrpc
{

template <class T>
class ObRpcProcessor : public ObRpcProcessorBase
{
public:
  static constexpr ObRpcPacketCode PCODE = T::PCODE;
public:
  ObRpcProcessor() {}
  virtual ~ObRpcProcessor() {}
  virtual int check_timeout()
  {
    return m_check_timeout();
  }
protected:
  virtual int process() = 0;
  virtual int preprocess_arg() { return common::OB_SUCCESS; }
protected:
  int decode_base(const char *buf, const int64_t len, int64_t &pos)
  {
    return common::serialization::decode(buf, len, pos, arg_);
  }
  int m_get_pcode() { return PCODE; }
  int encode_base(char *buf, const int64_t len, int64_t &pos)
  {
    return common::serialization::encode(buf, len, pos, result_);
  }
  int64_t m_get_encoded_length()
  {
    return common::serialization::encoded_length(result_);
  }
protected:
  typename T::Request arg_;
  typename T::Response result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcProcessor);
}; // end of class ObRpcProcessor

} // end of namespace observer
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_
