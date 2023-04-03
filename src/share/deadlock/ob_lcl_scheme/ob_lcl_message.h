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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_MESSAGE_H
#define OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_MESSAGE_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "share/deadlock/ob_deadlock_parameters.h"
#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "lib/container/ob_array.h"
#include "ob_lcl_utils.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

class ObLCLMessage
{
  OB_UNIS_VERSION(1);
public:
  ObLCLMessage() : lclv_(INVALID_VALUE) {};
  int set_args(const common::ObAddr &dest_addr,
               const UserBinaryKey &dest_key,
               const common::ObAddr &src_addr,
               const UserBinaryKey &src_key,
               const uint64_t &lclv,
               const ObLCLLabel &label,
               const int64_t ts);
  int merge(const ObLCLMessage &rhs);
  bool is_valid() const;
  const common::ObAddr &get_addr() const { return dest_addr_; }
  const UserBinaryKey &get_user_key() const { return dest_key_; }
  uint64_t get_lclv() const { return lclv_; }
  const ObLCLLabel &get_label() const { return label_; }
  int64_t get_send_ts() const { return  send_ts_; }
  TO_STRING_KV(K_(dest_addr), K_(dest_key), K_(src_addr), K_(src_key), K_(lclv), K_(label), K_(send_ts));
private:
  common::ObAddr dest_addr_;
  UserBinaryKey dest_key_;
  common::ObAddr src_addr_;
  UserBinaryKey src_key_;
  uint64_t lclv_;
  ObLCLLabel label_;
  int64_t send_ts_;
};

}// namespace detector
}// namespace share
}// namespace oceanbase
#endif
