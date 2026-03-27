/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef STROAGE_TX_DEADLOCK_ADAPTER_OB_TRANS_DETECTOR_KEY_H
#define STROAGE_TX_DEADLOCK_ADAPTER_OB_TRANS_DETECTOR_KEY_H
#include "lib/ob_errno.h"
#include "share/ob_define.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace transaction
{

enum class ObDeadlockKeyType
{
  DEFAULT = 0, // normal key
  REMOTE_EXEC_SIDE = 1 // execution side dectector in remote execution wait relationship
};

class ObTransDeadlockDetectorKey
{
  OB_UNIS_VERSION_V(1);
public:
  ObTransDeadlockDetectorKey(const ObDeadlockKeyType type = ObDeadlockKeyType::DEFAULT,
                             const ObTransID trans_id = ObTransID())
    : type_(type), trans_id_(trans_id) {}
  bool is_valid() const { return trans_id_.is_valid(); }
  ObTransID get_trans_id() const { return trans_id_; }
  TO_STRING_KV(K_(type), K_(trans_id));
private:
  ObDeadlockKeyType type_;
  ObTransID trans_id_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, ObTransDeadlockDetectorKey, type_, trans_id_);

} // transaction
} // oceanabase
#endif