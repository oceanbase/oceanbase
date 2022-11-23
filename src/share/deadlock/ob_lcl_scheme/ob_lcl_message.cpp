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

#include "ob_lcl_message.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

using namespace common;

OB_SERIALIZE_MEMBER(ObLCLMessage, dest_addr_, dest_key_, src_addr_, src_key_, lclv_, label_, send_ts_);

int ObLCLMessage::merge(const ObLCLMessage &rhs)
{
  int ret = OB_SUCCESS;
  if (dest_addr_ != rhs.dest_addr_ ||
      dest_key_ != rhs.dest_key_) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    if (lclv_ < rhs.lclv_) {
      lclv_ = rhs.lclv_;
      label_ = rhs.label_;
      src_key_ = rhs.src_key_;
    } else if (lclv_ == rhs.lclv_ && rhs.label_ < label_) {
      label_ = rhs.label_;
      src_key_ = rhs.src_key_;
    }
  }
  return ret;
}

bool ObLCLMessage::is_valid() const
{
  return dest_addr_.is_valid() &&
         dest_key_.is_valid() &&
         INVALID_VALUE != lclv_ &&
         label_.is_valid();
}

int ObLCLMessage::set_args(const common::ObAddr &dest_addr,
                           const UserBinaryKey &dest_key,
                           const common::ObAddr &src_addr,
                           const UserBinaryKey &src_key,
                           const uint64_t &lclv,
                           const ObLCLLabel &label,
                           const int64_t ts) {
  int ret = OB_SUCCESS;
  if (!dest_addr.is_valid() ||
      !dest_key.is_valid() ||
      !src_addr.is_valid() ||
      !src_key.is_valid() ||
      INVALID_VALUE == lclv ||
      !label.is_valid() ||
      INVALID_VALUE == ts) {
    ret = OB_INVALID_ARGUMENT;
  }
  dest_addr_ = dest_addr;
  dest_key_ = dest_key;
  src_addr_ = src_addr;
  src_key_ = src_key;
  lclv_ = lclv;
  label_ = label;
  send_ts_ = ts;
  return ret;
}

}
}
}
