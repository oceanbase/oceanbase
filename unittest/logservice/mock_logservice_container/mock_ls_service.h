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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_LOG_STREAM_SERVICE_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_LOG_STREAM_SERVICE_
#include "storage/tx_storage/ob_ls_service.h"
namespace oceanbase
{
namespace logservice
{
class MockLS : public storage::ObLS
{
public:
  int replay(const palf::LSN &lsn,
             const int64_t &log_timestamp,
             const int64_t &log_size,
             const char *log_buf)
  {
    REPLAY_LOG(INFO, "replay log", K(lsn), K(log_timestamp), K(log_size), K(log_size));
    return OB_SUCCESS;
  }
};

class MockLSMap : public storage::ObLSMap
{
public:
  void revert_ls(ObLS *ls)
  {
    // do nothing
  }
};

class MockLSService : public storage::ObLSService
{
public:
  int get_ls(const share::ObLSID &ls_id,
             ObLSHandle &handle)
  {
    handle.set_ls(map_, ls_);
    return OB_SUCCESS;
  }
private:
  MockLS ls_;
  MockLSMap map_;
};
}
}
#endif //OCEANBASE_UNITTEST_LOGSERVICE_MOCK_LOG_STREAM_SERVICE_
