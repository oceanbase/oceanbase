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

#ifndef OCEANBASE_LOGSERVICE_OB_REPLAY_HANDLER_
#define OCEANBASE_LOGSERVICE_OB_REPLAY_HANDLER_

#include "lib/lock/ob_tc_rwlock.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/lsn.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
}
namespace share
{
class SCN;
}
namespace logservice
{
class ObLogService;
class ObReplayHandler
{
public:
  ObReplayHandler(storage::ObLS *ls_);
  ~ObReplayHandler();
  void reset();
public:
  int register_handler(const ObLogBaseType &type,
                       ObIReplaySubHandler *handler);
  void unregister_handler(const ObLogBaseType &type);

  int replay(const ObLogBaseType &type,
             const void *buffer,
             const int64_t nbytes,
             const palf::LSN &lsn,
             const share::SCN &scn);
private:
  typedef common::RWLock::WLockGuard WLockGuard;
  typedef common::RWLock::RLockGuard RLockGuard;
private:
  storage::ObLS *ls_;
  common::RWLock lock_;
  ObIReplaySubHandler *handlers_[ObLogBaseType::MAX_LOG_BASE_TYPE];
private:
  DISALLOW_COPY_AND_ASSIGN(ObReplayHandler);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_REPLAY_HANDLER_
