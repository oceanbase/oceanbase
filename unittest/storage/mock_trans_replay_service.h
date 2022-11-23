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
