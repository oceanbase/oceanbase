/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef __SHARE_OB_TIME_TRAVEL_INFO_H__
#define __SHARE_OB_TIME_TRAVEL_INFO_H__

#include "lib/string/ob_string.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace share
{

struct ObTimeTravelInfo
{
  enum class TimeTravelType
  {
    NONE = 0,        // 无时间旅行查询
    SNAPSHOT_ID = 1, // 通过快照ID查询
    TIMESTAMP = 2,   // 通过时间戳查询
    BRANCH_TAG = 3   // 通过分支或标签查询
  };

  TimeTravelType type_;
  int64_t snapshot_id_;         // 快照ID值
  int64_t timestamp_ms_;        // 时间戳（毫秒）
  ObString branch_or_tag_name_; // 分支或标签名

  ObTimeTravelInfo()
      : type_(TimeTravelType::NONE), snapshot_id_(OB_INVALID_ID), timestamp_ms_(0),
        branch_or_tag_name_()
  {
  }

  TO_STRING_KV(K_(type), K_(snapshot_id), K_(timestamp_ms), K_(branch_or_tag_name));
};

} // namespace share
} // namespace oceanbase

#endif // __SHARE_OB_TIME_TRAVEL_INFO_H__
