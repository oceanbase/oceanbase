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

#ifndef OCEANBASE_CLOG_MOCK_OB_SUBMIT_LOG_CB_H_
#define OCEANBASE_CLOG_MOCK_OB_SUBMIT_LOG_CB_H_
#include "clog/ob_i_submit_log_cb.h"

namespace oceanbase {
namespace clog {
class MockObSubmitLogCb : public ObISubmitLogCb {
public:
  MockObSubmitLogCb()
  {}
  virtual ~MockObSubmitLogCb()
  {}

public:
  int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed)
  {
    UNUSED(partition_key);
    UNUSED(log_type);
    UNUSED(log_id);
    UNUSED(version);
    UNUSED(batch_committed);
    UNUSED(batch_last_succeed);
    return common::OB_SUCCESS;
  }
  int on_finished(const common::ObPartitionKey& partition_key, const uint64_t log_id)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(partition_key);
    UNUSED(log_id);
    return ret;
  }
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_MOCK_OB_SUBMIT_LOG_CB_H_
