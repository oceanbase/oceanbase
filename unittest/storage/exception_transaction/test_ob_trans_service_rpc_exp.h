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

#ifndef OB_TEST_OB_TEST_ERROR_TRANSACTION_RPC_EXP_H_
#define OB_TEST_OB_TEST_ERROR_TRANSACTION_RPC_EXP_H_

#include <stdint.h>
#include "storage/transaction/ob_trans_msg_type.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace unittest {
class ObTransMsgException {
public:
  ObTransMsgException()
      : inited_(false),
        request_abort_(false),
        response_abort_(false),
        start_request_abort_time_(0),
        end_request_abort_time_(0),
        start_response_abort_time_(0),
        end_response_abort_time_(0)
  {}
  virtual ~ObTransMsgException()
  {}

  int init(const bool request_abort, const int64_t start_request_abort_time, const int64_t end_request_abort_time,
      const bool response_abort, const int64_t start_response_abort_time, const int64_t end_response_abort_time);

  bool is_valid() const;

  void reset()
  {
    inited_ = false;
    request_abort_ = false;
    response_abort_ = false;
    msg_type_ = -1;
    start_request_abort_time_ = 0;
    end_request_abort_time_ = 0;
    start_response_abort_time_ = 0;
    end_response_abort_time_ = 0;
  }
  ObTransMsgException& operator=(const ObTransMsgException& trans_msg_exp);
  bool is_request_abort() const
  {
    return request_abort_;
  }
  bool is_response_abort() const
  {
    return response_abort_;
  }
  void check_rpc_exp(const int64_t msg_type, const int64_t cur_ts, bool& abort);

  TO_STRING_KV(K_(request_abort), K_(start_request_abort_time), K_(end_request_abort_time), K_(response_abort),
      K_(start_response_abort_time), K_(end_response_abort_time));

public:
  bool inited_;
  bool request_abort_;
  bool response_abort_;
  int msg_type_;
  int64_t start_request_abort_time_;
  int64_t end_request_abort_time_;
  int64_t start_response_abort_time_;
  int64_t end_response_abort_time_;
};

class ObTransRpcExecption {
public:
  ObTransRpcExecption()
  {
    reset();
  }
  virtual ~ObTransRpcExecption()
  {}
  void reset();
  ObTransRpcExecption& operator=(const ObTransRpcExecption& rpc_exp);
  ObTransMsgException* get_msg_exception(const int64_t msg_type);
  int set_msg_exception(const int64_t msg_type, const bool request_abort, const int64_t start_request_abort_time,
      const int64_t end_request_abort_time, const bool response_abort, const int64_t start_response_abort_time,
      const int64_t end_response_abort_time);

public:
  ObTransMsgException commit_msg_exp_;
  ObTransMsgException abort_msg_exp_;
  ObTransMsgException stmt_create_ctx_msg_exp_;
  ObTransMsgException stmt_rollback_msg_exp_;
  ObTransMsgException prepare_2pc_msg_exp_;
  ObTransMsgException commit_2pc_msg_exp_;
  ObTransMsgException abort_2pc_msg_exp_;
  ObTransMsgException clear_2pc_msg_exp_;
  ObTransMsgException error_resp_msg_exp_;
};

}  // namespace unittest
}  // namespace oceanbase
#endif
