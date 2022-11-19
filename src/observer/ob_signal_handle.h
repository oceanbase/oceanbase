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

#ifndef OCEANBASE_OBSERVER_OB_SIGNAL_HANDLE_
#define OCEANBASE_OBSERVER_OB_SIGNAL_HANDLE_

#include <signal.h>
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace observer
{
class ObSignalHandle: public share::ObThreadPool
{
public:
  ObSignalHandle() {}
  ~ObSignalHandle() {
    stop();
    wait();
  }
  virtual void run1();
  //should be called in main thread. Change signal mask to block these signals.
  static int change_signal_mask();
  //add signals to signal set.
  static int add_signums_to_set(sigset_t &sig_set);
  //deal signals. Called in the signal handle thread.
  static int deal_signals(int signum);
};

}
}

#endif
