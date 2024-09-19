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

#ifndef OCEANBASE_OBSERVER_FEEDBACK_PROXY_INFO_H
#define OCEANBASE_OBSERVER_FEEDBACK_PROXY_INFO_H

#include <cstdint>
#include "deps/oblib/src/lib/utility/serialization.h"
#include "deps/oblib/src/lib/utility/ob_proto_trans_util.h"
#include "deps/oblib/src/lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace observer
{
enum ObFeedbackProxyInfoType
{
  IS_LOCK_SESSION = 0,
  FEEDBACK_PROXY_MAX_TYPE
};

// EXAMPLE:
// We return a info which sub_type is IS_LOCK_SESSION (which eqauls to 1)
// and sub_type_value is '1' (which is 31 in hex), the buffer of extra_kv
// should like this:
// | type | type_len  | sub_type | sub_type_len | sub_type_value |
// | E903 | 0700 0000 |   0000   |   0100 0000  |       31       |
// ATTENTION: it's in little-endian order, so the sub_type_len is 1 in fact.
template <typename T>
class ObFeedbackProxyInfo
{
public:
  static const int64_t KEY_PLACEHOLDER_LENGTH = 2;
  static const int64_t VALUE_LENGTH_PLACEHOLDER_LENGTH = 4;

public:
  ObFeedbackProxyInfo(const ObFeedbackProxyInfoType &key, const T &value) : key_(key), value_(value){};

  void set_value(const T &value) { value_ = value; }
  int64_t get_serialize_size() const
  {
    int64_t val_length = serialization::encoded_length(value_);
    return KEY_PLACEHOLDER_LENGTH + VALUE_LENGTH_PLACEHOLDER_LENGTH + val_length;
  }

  int serialize(char *buf, int64_t len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    int64_t val_len = 0;
    int64_t org_pos = pos;  // used for debug

    int16_t type = static_cast<int16_t>(key_);

    if (pos + KEY_PLACEHOLDER_LENGTH + VALUE_LENGTH_PLACEHOLDER_LENGTH > len) {
      ret = OB_SIZE_OVERFLOW;
      SERVER_LOG(WARN, "buffer size overflow", K(ret), K(pos), K(len), KPC(this));
    } else if (OB_FAIL(serialization::encode(buf + KEY_PLACEHOLDER_LENGTH + VALUE_LENGTH_PLACEHOLDER_LENGTH, len, val_len, value_))) {
      // 1. skip type and len, serialize value firstly
      SERVER_LOG(WARN, "serialize value in ObFeedbackProxyInfo failed", K(ret), K(pos), K(len), KPC(this));
    } else if (OB_FAIL(ObProtoTransUtil::store_type_and_len(buf, len, pos, type, val_len))) {
      // 2. fill type and len
      SERVER_LOG(WARN, "store_type_and_len failed", K(ret), K(pos), K(len), KPC(this));
    } else {
      // 3. compute the actual pos
      pos += val_len;
    }
    SERVER_LOG(DEBUG,
               "ObFeedbackProxyInfo::serialize ",
               K(ret),
               KPHEX(buf + org_pos, pos - org_pos),
               K(len),
               K(org_pos),
               K(pos),
               KPC(this));
    return ret;
  }

  TO_STRING_KV(K_(key), K_(value));

private:
  ObFeedbackProxyInfoType key_;
  // value_ should implement the methods serialize(buf, len ,pos) and get_serialize_size()
  T value_;
};

typedef ObFeedbackProxyInfo<char> ObIsLockSessionInfo;
}  // namespace observer
}  // namespace oceanbase
#endif
