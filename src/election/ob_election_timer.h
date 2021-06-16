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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_TIMER_
#define OCEANBASE_ELECTION_OB_ELECTION_TIMER_

#include "ob_election_time_def.h"
#include <stdint.h>
#include <sys/time.h>
#include "lib/utility/ob_print_utils.h"
#include "common/ob_member_list.h"
#include "storage/transaction/ob_time_wheel.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
}

namespace election {
class ObIElectionTimerP;
class ObElectionTimer;
class ObIElectionRpc;

// election base task
class ObElectionTask : public common::ObTimeWheelTask {
public:
  ObElectionTask()
  {
    reset();
  }
  virtual ~ObElectionTask()
  {}
  virtual void runTimerTask() = 0;
  int init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc);
  void reset();

public:
  void set_run_time_expect(const int64_t ts)
  {
    run_time_expect_ = ts;
  }
  int64_t get_run_time_expect() const
  {
    return run_time_expect_;
  }
  void set_registered(const bool flag)
  {
    registered_ = flag;
  }
  bool is_registered() const
  {
    return registered_;
  }
  // do not care return code
  void set_running(const bool is_running)
  {
    (void)ATOMIC_STORE(&is_running_, is_running);
  }
  bool is_running() const
  {
    return ATOMIC_LOAD(&is_running_);
  }
  virtual void begin_run()
  {
    set_running(true);
  }
  virtual uint64_t hash() const;
  bool is_valid() const;

public:
  virtual int64_t to_string(char* buf, const int64_t buf_len) const;

protected:
  bool is_inited_;
  bool is_running_;
  ObIElectionTimerP* e_;
  ObElectionTimer* timer_;
  ObIElectionRpc* rpc_;
  int64_t run_time_expect_;
  bool registered_;
};

// election GT1 timer task
class ObElectionGT1Task : public ObElectionTask {
public:
  ObElectionGT1Task()
  {
    reset();
  }
  virtual ~ObElectionGT1Task()
  {}
  virtual void runTimerTask();
  int init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc);
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  int64_t candidate_index_;
};

// election GT2 timer task
class ObElectionGT2Task : public ObElectionTask {
public:
  ObElectionGT2Task()
  {
    reset();
  }
  virtual ~ObElectionGT2Task()
  {}
  virtual void runTimerTask();
  int init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc);
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;
};

// election GT3 timer task
class ObElectionGT3Task : public ObElectionTask {
public:
  ObElectionGT3Task()
  {
    reset();
  }
  virtual ~ObElectionGT3Task()
  {}
  virtual void runTimerTask();
  int init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc);
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;
};

// election GT4 timer task
class ObElectionGT4Task : public ObElectionTask {
public:
  ObElectionGT4Task()
  {
    reset();
  }
  virtual ~ObElectionGT4Task()
  {}
  virtual void runTimerTask();
  int init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc);
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;
};

// election ChangeLeader Timer task
class ObElectionChangeLeaderTask : public ObElectionTask {
public:
  ObElectionChangeLeaderTask()
  {
    reset();
  }
  virtual ~ObElectionChangeLeaderTask()
  {}
  virtual void runTimerTask();
  int init(ObIElectionTimerP* e, ObElectionTimer* timer, ObIElectionRpc* rpc);
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;
};

// election timer
class ObElectionTimer {
public:
  ObElectionTimer();
  ~ObElectionTimer();
  int init(ObIElectionTimerP* e, common::ObTimeWheel* tw, ObIElectionRpc* rpc);
  void destroy();
  void reset();
  int start(const int64_t start);
  int stop();
  int try_stop();  // don't wait
public:
  int register_gt1_once(const int64_t run_time_expect, int64_t& delay);
  int register_gt2_once(const int64_t run_time_expect, int64_t& delay);
  int register_gt3_once(const int64_t run_time_expect, int64_t& delay);
  int register_gt4_once(const int64_t run_time_expect, int64_t& delay);
  int register_change_leader_once(const int64_t run_time_expect, int64_t& delay);

private:
  template <typename GT>
  int register_gt_once_(GT& gt, const int64_t run_time_expect, int64_t& delay);
  template <typename GT>
  int unregister_gt_(GT& gt);

private:
  bool is_inited_;
  bool is_running_;
  ObIElectionTimerP* e_;
  common::ObTimeWheel* tw_;
  ObIElectionRpc* rpc_;
  ObElectionGT1Task gt1_;
  ObElectionGT2Task gt2_;
  ObElectionGT3Task gt3_;
  ObElectionGT4Task gt4_;
  ObElectionChangeLeaderTask change_leader_t_;
};
}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_TIMER_
