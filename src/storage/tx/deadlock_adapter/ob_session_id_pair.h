/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef STROAGE_TX_DEADLOCK_ADAPTER_OB_SESSION_ID_PAIR_H
#define STROAGE_TX_DEADLOCK_ADAPTER_OB_SESSION_ID_PAIR_H
#include "lib/ob_errno.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace transaction
{

struct SessionIDPair {
  OB_UNIS_VERSION(1);
public:
  SessionIDPair() : sess_id_(0), assoc_sess_id_(0) {}
  SessionIDPair(const uint32_t sess_id, const uint32_t assoc_sess_id)
  : sess_id_(sess_id),
  assoc_sess_id_(assoc_sess_id) {}
  uint32_t get_valid_sess_id() const {
    uint32_t valid_sess_id = assoc_sess_id_;
    if (valid_sess_id == 0) {
      valid_sess_id = sess_id_;
      if (valid_sess_id == 0) {
        DETECT_LOG_RET(WARN, OB_ERR_UNEXPECTED, "get_vald_sess_id is 0", K(*this));
      }
    }
    return valid_sess_id;
  }
  bool is_valid() const {
    return assoc_sess_id_ != 0 || sess_id_ != 0;
  }
  TO_STRING_KV(K_(sess_id), K_(assoc_sess_id));
  uint32_t sess_id_;
  uint32_t assoc_sess_id_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, SessionIDPair, sess_id_, assoc_sess_id_);

}
}
#endif