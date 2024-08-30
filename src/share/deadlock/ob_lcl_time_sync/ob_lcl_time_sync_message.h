#ifndef OCEANBASE_LCL_TIME_SYNC_MESSAGE_H
#define OCEANBASE_LCL_TIME_SYNC_MESSAGE_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace share {
namespace detector {

class ObDeadLockTimeSyncArg {
  OB_UNIS_VERSION(1);

public:
  ObDeadLockTimeSyncArg() = default;
  ~ObDeadLockTimeSyncArg() = default;
  int to_string(char *, const int64_t) const { return 0; }

private:
};

class ObDeadLockTimeSyncResp {
  OB_UNIS_VERSION(1);

public:
  int assign(const ObDeadLockTimeSyncResp &other);
  bool is_valid() const { return true; }
  void set_leader_current_time(uint64_t leader_current_time) {
    leader_current_time_ = leader_current_time;
  }
  int64_t get_leader_current_time() const { return leader_current_time_; }
  TO_STRING_KV(K_(leader_current_time));

private:
  uint64_t leader_current_time_;
};
} // namespace detector
} // namespace share
} // namespace oceanbase

#endif