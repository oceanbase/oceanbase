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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_STATUS_
#define OCEANBASE_TRANSACTION_OB_TRANS_STATUS_

#include <stdint.h>
#include "lib/lock/ob_spin_lock.h"
#include "common/ob_partition_key.h"
#include "ob_trans_define.h"

namespace oceanbase {
namespace transaction {

class ObTransStatus {
public:
  ObTransStatus() : trans_id_(), status_(common::OB_TRANS_UNKNOWN)
  {}
  ObTransStatus(const ObTransID& trans_id, const int status) : trans_id_(trans_id), status_(status)
  {}
  ~ObTransStatus()
  {}

public:
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  int get_status() const
  {
    return status_;
  }
  TO_STRING_KV(K_(trans_id), K_(status));

private:
  ObTransID trans_id_;
  int status_;
};

class ObTransStatusMgr {
public:
  ObTransStatusMgr()
  {}
  ~ObTransStatusMgr()
  {}
  int set_status(const ObTransID& trans_id, const ObTransStatus& status);
  int get_status(const ObTransID& trans_id, ObTransStatus& status);

private:
  static const int64_t MAX_TRANS_STATUS = 1 * 1024 * 1024;
  static const int64_t MAX_LOCK = 64 * 1024;

private:
  common::ObSpinLock lock_[MAX_LOCK];
  ObTransStatus status_array_[MAX_TRANS_STATUS];
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_STATUS_
