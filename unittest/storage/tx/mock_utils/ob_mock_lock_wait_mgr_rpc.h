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

#ifndef OCEANBASE_TRANSACTION_TEST_OB_MOCK_LOCK_WAIT_MGR_RPC_
#define OCEANBASE_TRANSACTION_TEST_OB_MOCK_LOCK_WAIT_MGR_RPC_
#include "msg_bus.h"
#include "storage/lock_wait_mgr/ob_lock_wait_mgr_rpc.h"
#include "storage/tx/ob_location_adapter.h"
#include "share/ob_define.h"
namespace oceanbase
{
using namespace transaction;
namespace lockwaitmgr
{
class ObFakeLockWaitMgrRpc : public ObILockWaitMgrRpc
{
public:
  ObFakeLockWaitMgrRpc(MsgBus *msg_bus,
                       const ObAddr &my_addr,
                       ObLockWaitMgr *lwm)
    : addr_(my_addr), msg_bus_(msg_bus), lwm_(lwm), errsim_msg_type_(-1) {}
  int post_msg(const ObAddr &server, ObLockWaitMgrMsg &msg)
  {
    int ret = OB_SUCCESS;
    if (msg.get_msg_type() == errsim_msg_type_) {
      TRANS_LOG(INFO, "errsim loss msg", K(msg), K(server));
    } else if(msg_bus_ == NULL) {
      // do nothing
    } else {
      int64_t size = msg.get_serialize_size() + 1 /*for msg category*/ + sizeof(int16_t) /* for tx_msg.type_ */;
      char *buf = (char*)ob_malloc(size, ObNewModIds::TEST);
      buf[0] = 3; // 2 LockWaitMgr msg
      int64_t pos = 1;
      // 暂定1000避免msg type冲突
      int16_t msg_type = msg.get_msg_type();
      OZ(serialization::encode(buf, size, pos, msg_type));
      OZ(msg.serialize(buf, size, pos));
      ObString msg_str(size, buf);
      TRANS_LOG(INFO, "post_msg", "msg_ptr", OB_P(buf), K(msg), "receiver", server);
      OZ(msg_bus_->send_msg(msg_str, addr_, server));
    }

    return ret;
  }
  void inject_msg_errsim(int msg_type)
  {
    errsim_msg_type_ = msg_type;
  }
  void reset_msg_errsim()
  {
    errsim_msg_type_ = -1;
  }
private:
  ObAddr addr_;
  MsgBus *msg_bus_;
  ObLockWaitMgr *lwm_;
  int errsim_msg_type_;
};

} // memtable
} // oceanabse

#endif // OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_LOCK_WAIT_MGR_RPC