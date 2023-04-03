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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_THREAD_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_RS_THREAD_CHECKER_H_

// ObRsReentrantThread
#include "rootserver/ob_rs_reentrant_thread.h"

namespace oceanbase
{
namespace rootserver
{
/// thread checker thread.
class ObRsThreadChecker : public ObRsReentrantThread
{
public:
  ObRsThreadChecker();
  virtual ~ObRsThreadChecker();

  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }

  int init();
  void check_loop();
  int destroy();

private:
  bool inited_;
#ifdef ERRSIM     //for obtest
  static const int64_t CHECK_TIMEVAL_US = 1000LL * 1000LL;
#else
  static const int64_t CHECK_TIMEVAL_US = 60LL * 1000LL * 1000LL;
#endif
  DISALLOW_COPY_AND_ASSIGN(ObRsThreadChecker);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_EMPTY_SERVER_CHECKER_H_
