// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_LOGSERVICE_OB_ACTIVE_KEEP_ALIVE_H_
#define OCEANBASE_LOGSERVICE_OB_ACTIVE_KEEP_ALIVE_H_

#include "lib/literals/ob_literals.h"
#include "lib/net/ob_addr.h"
#include "lib/task/ob_timer_service.h"
#include "logservice/common_util/ob_log_active_keep_alive.h"

namespace oceanbase
{
namespace logservice
{

class ObActiveKeepAlive : public common::ObTimerTask
{
public:
  ObActiveKeepAlive();
  ~ObActiveKeepAlive();
  int init(const int tg_id, const common::ObAddr &self_addr);
  void runTimerTask() override;
  TO_STRING_KV(K_(is_inited), K_(tg_id), K_(active_keep_alive_worker), K_(self_addr));
private:
  const static constexpr int64_t ACTIVE_KEEP_ALIVE_ROUND_INTERVAL = 10_s;
private:
  void destroy_();
private:
  bool is_inited_;
  int64_t tg_id_;
  ObLogActiveKeepAlive active_keep_alive_worker_;
  common::ObAddr self_addr_;
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_ACTIVE_KEEP_ALIVE_H_
