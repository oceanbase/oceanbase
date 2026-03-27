/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_MOCK_TRANS_REPLAY_SERVICE_H_
#define OCEANBASE_UNITTEST_MOCK_TRANS_REPLAY_SERVICE_H_

#include "share/ob_define.h"
#include "storage/ob_i_trans_replay_service.h"
#include "storage/ob_trans_log_mock.h"

namespace oceanbase
{
namespace unittest
{
class MockTransReplayService: public transaction::ObITransReplayService
{
public:
  MockTransReplayService() {}
  virtual ~MockTransReplayService() {}
public:
  virtual int submit_trans_log(transaction::ObTransLog *trans_log)
  {
    TRANS_LOG(INFO, "submit_trans_log", "log_type", trans_log->get_type());
    return common::OB_SUCCESS;
  }
  virtual void finish_replay()
  {
    TRANS_LOG(INFO, "finish replay");
  }
};

} // namespace unittest
} // namespace oceanbase

#endif // OCEANBASE_TRANSACTION_MOCK_TRANS_REPLAY_SERVICE_H_
