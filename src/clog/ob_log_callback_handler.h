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

#ifndef OCEANBASE_CLOG_OB_LOG_CALLBACK_HANDLER_
#define OCEANBASE_CLOG_OB_LOG_CALLBACK_HANDLER_

#include <cstdlib>
#include "common/ob_partition_key.h"
#include "ob_log_callback_engine.h"

namespace oceanbase {
namespace share {
class ObIPSCb;
}
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObIMembershipCallback;
class ObIRoleChangeCb;
class ObLogCallbackHandler {
public:
  ObLogCallbackHandler() : is_inited_(false), partition_service_(NULL), callback_engine_(NULL)
  {}
  ~ObLogCallbackHandler()
  {}

public:
  int init(storage::ObPartitionService* partition_service, ObILogCallbackEngine* callback_engine);

public:
  void handle(void* task);
  void handle_pop_task_(const common::ObPartitionKey& partition_key);

private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  ObILogCallbackEngine* callback_engine_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_CALLBACK_HANDLER_
