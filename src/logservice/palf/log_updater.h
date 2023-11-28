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

#ifndef OCEANBASE_PALF_LOG_UPDATER_
#define OCEANBASE_PALF_LOG_UPDATER_
#include "lib/task/ob_timer.h"
namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;
class LogUpdater : public common::ObTimerTask
{
public:
  LogUpdater();
  virtual ~LogUpdater();
  int init(IPalfEnvImpl *palf_env_impl);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask();
private:
  IPalfEnvImpl *palf_env_impl_;
  int tg_id_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase
#endif
