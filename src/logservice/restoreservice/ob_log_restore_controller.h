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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_CONTROLLER_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_CONTROLLER_H_

#include "lib/utility/ob_macro_utils.h"
#include <cstdint>
namespace oceanbase
{
namespace logservice
{
class ObLogService;
// Only support single thread get and update log restore quota
class ObLogRestoreController
{
  const int64_t LOG_RESTORE_CONTROL_REFRESH_INTERVAL = 1000 * 1000L;  // 1s
public:
  ObLogRestoreController();
  ~ObLogRestoreController();
public:
  int init(const uint64_t tenant_id, ObLogService *log_service);
  void destroy();
  int update_quota();
  int get_quota(const int64_t size, bool &succ);
private:
  bool need_update_() const;
private:
  bool inited_;
  uint64_t tenant_id_;
  ObLogService *log_service_;
  int64_t available_capacity_;
  int64_t last_refresh_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreController);
};
} // namespace logservice
} // namespace oceanbase
#endif
