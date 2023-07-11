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

#ifndef OCEANBASE_LOGSERVICE_OB_LS_ADAPTER_H_
#define OCEANBASE_LOGSERVICE_OB_LS_ADAPTER_H_

#include <stdint.h>
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{
class ObLSService;
}
namespace logservice
{
class ObLogReplayTask;
class ObLSAdapter
{
public:
  ObLSAdapter();
  ~ObLSAdapter();
  int init(storage::ObLSService *ls_service_);
  void destroy();
public:
  virtual int replay(ObLogReplayTask *replay_task);
  virtual int wait_append_sync(const share::ObLSID &ls_id);
private:
const int64_t MAX_SINGLE_REPLAY_WARNING_TIME_THRESOLD = 100 * 1000; //100ms
  const int64_t MAX_SINGLE_REPLAY_ERROR_TIME_THRESOLD = 2 * 1000 * 1000; //2s 单条日志回放执行时间超过此值报error
  const int64_t MAX_SINGLE_RETRY_WARNING_TIME_THRESOLD = 5 * 1000 * 1000; //5s 单条日志回放重试超过此值报error
  bool is_inited_;
  storage::ObLSService *ls_service_;
};

} // logservice
} // oceanbase

#endif
