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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_BASE_HEADER_
#define OCEANBASE_LOGSERVICE_OB_LOG_BASE_HEADER_

#include "lib/ob_define.h"
#include <stdint.h>                           // int64_t...
#include <string.h>                           // strncmp...
#include "ob_log_base_type.h"

namespace oceanbase
{
namespace logservice
{
// ObReplayBarrierType为follower回放日志时的barrier类型, 分为以下三类
// 1. STRICT_BARRIER:
//    此日志能回放的前提条件为比此日志的log ts小的日志都已回放完,
//    并且在此日志的回放完成之前, 比此日志的log ts大的日志都不会回放.
// 2. PRE_BARRIER:
//    此日志能回放的前提条件为比此日志的log ts小的日志都已回放完,
//    但此日志的回放完成之前, 比此日志的log ts大的日志可以回放.
// 3. NO_NEED_BARRIER:
//    此日志无任何特殊回放条件, 也不会对其他日志的回放有任何影响
enum ObReplayBarrierType
{
  INVALID_BARRIER = 0,
  STRICT_BARRIER = 1,
  PRE_BARRIER = 2,
  NO_NEED_BARRIER = 3,
};

class ObLogBaseHeader {
public:
  ObLogBaseHeader();
  ObLogBaseHeader(const ObLogBaseType log_type,
                  const enum ObReplayBarrierType replay_barrier_type);
  ObLogBaseHeader(const ObLogBaseType log_type,
                  const enum ObReplayBarrierType replay_barrier_type,
                  const int64_t replay_hint);
  ~ObLogBaseHeader();
public:
  void reset();
  bool is_valid() const;
  bool need_pre_replay_barrier() const;
  bool need_post_replay_barrier() const;
  ObLogBaseType get_log_type() const;
  int64_t get_replay_hint() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV("version", version_,
                "log_type", log_type_,
                "flag", flag_,
                "replay_hint", replay_hint_);
private:
  static const int16_t BASE_HEADER_VERSION = 1;
  static const uint32_t NEED_POST_REPLAY_BARRIER_FLAG = (1 << 31);
  static const uint32_t NEED_PRE_REPLAY_BARRIER_FLAG = (1 << 30);
  int16_t version_;
  int16_t log_type_;
  int32_t flag_;
  int64_t replay_hint_;
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_BASE_HEADER_
