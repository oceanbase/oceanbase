/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
