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

#ifndef OCEANBASE_ELECTION_OB_GC_THREAD_H_
#define OCEANBASE_ELECTION_OB_GC_THREAD_H_

#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace election {
class ObElectionGroupMgr;

class ObElectionGCThread : public share::ObThreadPool {
public:
  ObElectionGCThread();
  ~ObElectionGCThread();

public:
  int init(ObElectionGroupMgr* eg_mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();

private:
  int exec_gc_loop_();

private:
  static const int32_t ELECT_GC_INTERVAL = 60 * 1000 * 1000;

private:
  ObElectionGroupMgr* eg_mgr_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObElectionGCThread);
};
}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_GC_THREAD_H_
