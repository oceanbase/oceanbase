#include "ob_lcl_time_sync_message.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase {
namespace share {
namespace detector {

using namespace common;

OB_SERIALIZE_MEMBER(ObDeadLockTimeSyncArg);
OB_SERIALIZE_MEMBER(ObDeadLockTimeSyncResp, leader_current_time_);

int ObDeadLockTimeSyncResp::assign(const ObDeadLockTimeSyncResp &other) {
  int ret = OB_SUCCESS;
  leader_current_time_ = other.get_leader_current_time();
  return ret;
}

} // namespace detector
} // namespace share
} // namespace oceanbase