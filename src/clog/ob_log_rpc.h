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

#ifndef OCEANBASE_CLOG_OB_LOG_RPC_
#define OCEANBASE_CLOG_OB_LOG_RPC_
#include "share/ob_define.h"

namespace oceanbase {
namespace common {
class ObAddr;
};
namespace clog {
enum {
  CLOG_PCODE = 21,
};

class ObILogRpc {
public:
  ObILogRpc()
  {}
  virtual ~ObILogRpc()
  {}
  virtual int post(const common::ObAddr& server, int pcode, const char* data, int64_t len) = 0;
};

};  // end namespace clog
};  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_RPC_
