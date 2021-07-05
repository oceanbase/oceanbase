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

#ifndef OCEANBASE_ROOTSERVER_OB_FREEZE_INFO_UPDATER_H
#define OCEANBASE_ROOTSERVER_OB_FREEZE_INFO_UPDATER_H
#include "rootserver/ob_rs_reentrant_thread.h"

namespace oceanbase {
namespace rootserver {
class ObFreezeInfoManager;
class ObFreezeInfoUpdater : public ObRsReentrantThread {
public:
  ObFreezeInfoUpdater();
  ~ObFreezeInfoUpdater();
  int init(ObFreezeInfoManager& freeze_info_manager);
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int64_t get_schedule_interval() const;

private:
  int try_gc_snapshot();
  int try_update_major_schema_version();
  int try_reload_freeze_info();
  int try_broadcast_freeze_info();
  int calc_weakread_timestamp();
  int process_invalid_schema_version();

private:
  static const int64_t TRY_UPDATER_INTERVAL_US = 1000 * 1000;  // 1s
  bool inited_;
  int64_t last_gc_timestamp_;
  ObFreezeInfoManager* freeze_info_manager_;
};
}  // namespace rootserver
}  // namespace oceanbase
#endif
