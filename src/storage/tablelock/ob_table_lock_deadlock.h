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

#ifndef OCEANBASE_STORAGE_TABLELOCK_DEADLOCK_H_
#define OCEANBASE_STORAGE_TABLELOCK_DEADLOCK_H_

#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "share/ob_ls_id.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
}

namespace share
{
namespace detector
{
class ObDetectorUserReportInfo;
class ObDependencyResource;
}
}

namespace transaction
{
namespace tablelock
{
const static int DEFAULT_LOCK_DEADLOCK_BUFFER_LENGTH = 128;

struct ObTransLockPartID
{
  OB_UNIS_VERSION(1);
public:
  ObTransLockPartID()
    : trans_id_(),
      lock_id_()
  {}
  ~ObTransLockPartID()
  {
    reset();
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&trans_id_, sizeof(trans_id_), hash_val);
    hash_val = murmurhash(&lock_id_, sizeof(lock_id_), hash_val);
    return hash_val;
  }
  bool is_valid() const
  {
    return trans_id_.is_valid() && lock_id_.is_valid();
  }
  void reset()
  {
    trans_id_.reset();
    lock_id_.reset();
  }
  TO_STRING_KV(K_(trans_id), K_(lock_id));
public:
  ObTransID trans_id_;
  ObLockID lock_id_;
};

class ObTxLockPartOnDetectOp
{
public:
  ObTxLockPartOnDetectOp() {}
  ~ObTxLockPartOnDetectOp() { reset(); }
  int operator()(
      const common::ObIArray<share::detector::ObDetectorInnerReportInfo> &info,
      const int64_t self_idx);
  int set(const ObTransLockPartID &lock_part_id,
          const share::ObLSID &ls_id);
  void reset();
private:
  ObTransLockPartID lock_part_id_;
  share::ObLSID ls_id_;
};

class ObTxLockPartCollectCallBack
{
public:
  ObTxLockPartCollectCallBack(const ObTransLockPartID &tx_lock_part_id)
    : tx_lock_part_id_(tx_lock_part_id) {}
  int operator()(share::detector::ObDetectorUserReportInfo &info);
private:
  ObTransLockPartID tx_lock_part_id_;
};

class ObTransLockPartBlockCallBack
{
  OB_UNIS_VERSION(1);
public:
  ObTransLockPartBlockCallBack()
    : ls_id_(),
      lock_op_()
  {}
  ~ObTransLockPartBlockCallBack()
  {
    reset();
  }
  // @param [out] depend_res, a dependency list.
  // @param [out] need_remove, shall the callback be removed.
  int operator()(common::ObIArray<share::detector::ObDependencyResource> &depend_res,
                 bool &need_remove);
  int init(const share::ObLSID &ls_id,
           const ObTableLockOp &lock_op);
  bool is_valid() const
  {
    return ls_id_.is_valid() && lock_op_.is_valid();
  }
  void reset()
  {
    ls_id_.reset();
    // lock_op_.reset();
  }
  TO_STRING_KV(K_(ls_id), K_(lock_op));
private:
  // we need ls id to tell which ls we will try to find the obj lock
  share::ObLSID ls_id_;
  // contain the info of which trans want to lock which obj with which lock mode.
  // we can get all the trans that will block this operation.
  ObTableLockOp lock_op_;
};

class ObTableLockDeadlockDetectorHelper
{
public:
  // register a trans lock part as the deadlock node.
  // this deadlock node will be killed if deadlock occur.
  // @param [in] tx_lock_part_id, which trans lock part will be registered.
  // @param [in] guard, contain the dead lock ctx used for kill trans lock part.
  //                    all the trans lock part of one trans parent should use
  //                    the same ObTxLockPartDeadLockCtx.
  static int register_trans_lock_part(const ObTransLockPartID &tx_lock_part_id,
                                      const share::ObLSID &ls_id,
                                      const share::detector::ObDetectorPriority priority);
  // unregister a trans lock part
  // @param [in] tx_lock_part_id, which trans lock part will be removed.
  static int unregister_trans_lock_part(const ObTransLockPartID &tx_lock_part_id);
  // add a dependency of parent_trans_id -> tx_lock_part_id, and
  // register parent_trans_id into deadlock detector at remote.
  // @param [in] tx_lock_part_id, specified the one who has conflict with others.
  // @param [in] parent_trans_id, the parent trans of tx_lock_part_id.
  static int add_parent(const ObTransLockPartID &tx_lock_part_id,
                        const common::ObAddr &parent_addr,
                        const ObTransID &parent_trans_id);
  // add the block dependency of trans lock part.
  // @param [in] tx_lock_part_id, whose dependency will be registered.
  // @param [in] ls_id, which ls is the lock belong to.
  // @param [in] lock_op, the lock operation tx_lock_part_id wants to do.
  static int block(const ObTransLockPartID &tx_lock_part_id,
                   const share::ObLSID &ls_id,
                   const ObTableLockOp &lock_op);
};

}
}
}
#endif
