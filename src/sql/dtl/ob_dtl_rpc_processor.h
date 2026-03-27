/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DTL_RPC_PROCESSOR_H
#define OB_DTL_RPC_PROCESSOR_H

#include "rpc/obrpc/ob_rpc_processor.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "ob_dtl_interm_result_manager.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlLinkedBuffer;
class ObDtlChannel;

class ObDtlSendMessageP
    : public obrpc::ObRpcProcessor< obrpc::ObDtlRpcProxy::ObRpc<obrpc::OB_DTL_SEND> >
{
public:
  virtual int process() final;
  static int process_msg(ObDtlRpcDataResponse &response, ObDtlSendArgs &args);
  static int process_interm_result(ObDtlSendArgs &arg);
  static int process_interm_result_inner(ObDtlLinkedBuffer &buffer,
                                        ObDTLIntermResultKey &key,
                                        int64_t start_pos,
                                        int64_t length,
                                        int64_t rows,
                                        bool is_eof,
                                        bool append_whole_block);
private:
  static int process_px_bloom_filter_data(ObDtlLinkedBuffer *&buffer);
};

class ObDtlBCSendMessageP
    : public obrpc::ObRpcProcessor< obrpc::ObDtlRpcProxy::ObRpc<obrpc::OB_DTL_BC_SEND> >
{
public:
  virtual int process() final;
};

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_RPC_PROCESSOR_H */
