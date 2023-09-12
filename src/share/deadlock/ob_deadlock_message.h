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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_COLLECT_INFO_MESSAGE_H
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_COLLECT_INFO_MESSAGE_H

#include "ob_deadlock_detector_common_define.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

class ObDeadLockCollectInfoMessage
{
  OB_UNIS_VERSION(1);
public:
  ObDeadLockCollectInfoMessage() = default;
  ~ObDeadLockCollectInfoMessage() = default;
  ObDeadLockCollectInfoMessage &operator=(const ObDeadLockCollectInfoMessage &) = delete;
  int assign(const ObDeadLockCollectInfoMessage &rhs);
  int set_dest_key(const UserBinaryKey &dest_key);
  int set_args(const UserBinaryKey &dest_key,
               const common::ObSArray<ObDetectorInnerReportInfo> &collected_info);
  int append(const ObDetectorInnerReportInfo &info);
  bool is_valid() const;
  const UserBinaryKey &get_dest_key() const;
  const common::ObSArray<ObDetectorInnerReportInfo> &get_collected_info() const;
  TO_STRING_KV(K_(dest_key), K_(collected_info));
private:
  UserBinaryKey dest_key_;
  common::ObSArray<ObDetectorInnerReportInfo> collected_info_;
};

class ObDeadLockNotifyParentMessage
{
OB_UNIS_VERSION(1);
public:
  ObDeadLockNotifyParentMessage() = default;
  ~ObDeadLockNotifyParentMessage() = default;
  int set_args(const common::ObAddr &parent_addr,
               const UserBinaryKey &dest_key,
               const common::ObAddr &src_addr,
               const UserBinaryKey &src_key);
  const UserBinaryKey &get_parent_key() const { return parent_key_; }
  const common::ObAddr &get_src_addr() const { return src_addr_; }
  const UserBinaryKey &get_src_key() const { return src_key_; }
  bool is_valid() const;
  TO_STRING_KV(K_(parent_addr), K_(parent_key), K_(src_addr), K_(src_key));
private:
  common::ObAddr parent_addr_;
  UserBinaryKey parent_key_;
  common::ObAddr src_addr_;
  UserBinaryKey src_key_;
};

}
}
}

#endif