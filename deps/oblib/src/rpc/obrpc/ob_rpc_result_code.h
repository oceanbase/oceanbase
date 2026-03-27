/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_RESULT_CODE_
#define OCEANBASE_RPC_OBRPC_OB_RPC_RESULT_CODE_

#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_warning_buffer.h"
namespace oceanbase
{
namespace obrpc
{

struct ObRpcResultCode
{
  OB_UNIS_VERSION(1);

public:
  ObRpcResultCode() : rcode_(0)
  {
    msg_[0] = '\0';
    warnings_.reset();
  }

  TO_STRING_KV("code", rcode_, "msg", msg_, K_(warnings));

  void reset()
  {
    rcode_ = 0;
    msg_[0] = '\0';
    warnings_.reset();
  }

  int32_t rcode_;
  char msg_[common::OB_MAX_ERROR_MSG_LEN];
  common::ObSEArray<common::ObWarningBuffer::WarningItem, 1> warnings_;
};

} // end of namespace obrpc
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_RESULT_CODE_
