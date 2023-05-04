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

#ifndef OCEANBASE_TRANSACTION_TEST_MSG_BUS_DEFINE_
#define OCEANBASE_TRANSACTION_TEST_MSG_BUS_DEFINE_

#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"
namespace oceanbase {
namespace common {
class MsgEndPoint {
public:
  virtual int recv_msg(const ObAddr &sender, ObString &msg) = 0;
  virtual int sync_recv_msg(const ObAddr &sender, ObString &msg, ObString &resp) = 0;
};
class MsgBus {
struct ObAddrPair {
  ObAddrPair() = default;
  ObAddrPair(const ObAddr &src, const ObAddr &dst)
    : src_(src), dst_(dst) {}
  bool operator==(const ObAddrPair &r) const
  {
    return r.src_ == src_ && r.dst_ == dst_;
  }
  ObAddr src_;
  ObAddr dst_;
  int64_t hash() const { return src_.hash() ^ dst_.hash(); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
};
public:
  MsgBus() {
    int ret = OB_SUCCESS;
    OZ(route_table_.create(16, ObModIds::TEST));
    OZ(fail_links_.create(16));
    if (OB_FAIL(ret)) abort();
  }
  int regist(const ObAddr &addr, MsgEndPoint &endpoint)
  {
    int ret = OB_SUCCESS;
    OZ(route_table_.set_refactored(addr, &endpoint));
    return ret;
  }
  int unregist(const ObAddr &addr) {
    int ret = OB_SUCCESS;
    MsgEndPoint *unused = NULL;
    ret = route_table_.erase_refactored(addr, &unused);
    return ret;
  }
  int send_msg(ObString &msg, const ObAddr &sender, const ObAddr &recv)
  {
    int ret = OB_SUCCESS;
    MsgEndPoint *e = NULL;
    OZ(route_table_.get_refactored(recv, e));
    if (OB_SUCC(ret)) {
      ObAddrPair pair(sender, recv);
      if (OB_HASH_EXIST == fail_links_.exist_refactored(pair)) {
        TRANS_LOG(WARN, "[msg bus] link failure, msg lost", K(sender), K(recv));
      } else {
        OZ(e->recv_msg(sender, msg));
      }
    }
    return ret;
  }
  int sync_send_msg(ObString &msg, const ObAddr &sender, const ObAddr &recv, ObString &resp)
  {
    int ret = OB_SUCCESS;
    MsgEndPoint *e = NULL;
    OZ(route_table_.get_refactored(recv, e));
    if (OB_SUCC(ret)) {
      ObAddrPair pair(sender, recv);
      if (OB_HASH_EXIST == fail_links_.exist_refactored(pair)) {
        TRANS_LOG(WARN, "[msg bus] link failure, msg lost", K(sender), K(recv));
      } else {
        OZ(e->sync_recv_msg(sender, msg, resp));
      }
    }
    return ret;
  }
  int inject_link_failure(const ObAddr &from, const ObAddr &to)
  {
    int ret = OB_SUCCESS;
    ObAddrPair pair(from, to);
    OZ(fail_links_.set_refactored(pair));
    return ret;
  }
  int repair_link_failure(const ObAddr &from, const ObAddr &to)
  {
    int ret = OB_SUCCESS;
    ObAddrPair pair(from, to);
    OZ(fail_links_.erase_refactored(pair));
    return ret;
  }
private:
  common::hash::ObHashSet<ObAddrPair> fail_links_;
  common::hash::ObHashMap<ObAddr, MsgEndPoint*> route_table_;
};
} // common
} // oceanbase
#endif //OCEANBASE_TRANSACTION_TEST_MSG_BUS_DEFINE_
