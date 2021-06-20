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

#ifndef OCEANBASE_UNITTEST_CLOG_MOCK_OB_LOG_REPLAY_ENGINE_WRAPPER_H_
#define OOCEANBASE_UNITTEST_CLOG_MOCK_OB_LOG_REPLAY_ENGINE_WRAPPER_H_

#include "clog/ob_log_replay_engine_wrapper.h"
#include "storage/replayengine/ob_i_log_replay_engine.h"

namespace oceanbase {
namespace clog {
class MockObLogReplayEngineWrapper : public ObLogReplayEngineWrapper {
public:
  MockObLogReplayEngineWrapper()
  {}
  virtual ~MockObLogReplayEngineWrapper()
  {}

public:
  int init(replayengine::ObILogReplayEngine* log_replay_engine)
  {
    UNUSED(log_replay_engine);
    return OB_SUCCESS;
  }

public:
  int submit_replay_task(const common::ObPartitionKey& partition_key, const ObLogEntry& log_entry)
  {
    UNUSED(partition_key);
    UNUSED(log_entry);
    return OB_SUCCESS;
  }
  bool is_replay_finished(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  int is_tenant_out_of_memory(const common::ObPartitionKey& partition_key, bool& is_out_of_mem)
  {
    UNUSED(partition_key);
    UNUSED(is_out_of_mem);
    return OB_SUCCESS;
  }
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_CLOG_MOCK_OB_LOG_REPLAY_ENGINE_WRAPPER_H_
