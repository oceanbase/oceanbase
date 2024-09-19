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

#ifndef OCEANBASE_OBSERVER_FEEDBACK_PROXY_UTILS_H
#define OCEANBASE_OBSERVER_FEEDBACK_PROXY_UTILS_H

#include "observer/mysql/ob_feedback_proxy_info.h"
#include "deps/oblib/src/rpc/obmysql/obp20_extra_info.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
template <typename T>
class ObIArray;
}  // namespace common
namespace obmysql
{
class Obp20Encoder;
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
// EXAMPLE:
// We return a info which sub_type is IS_LOCK_SESSION (which eqauls to 1)
// and sub_type_value is '1' (which is 31 in hex), the buffer of extra_kv
// should like this:
// | type | type_len  | sub_type | sub_type_len | sub_type_value |
// | E903 | 0700 0000 |   0000   |   0100 0000  |       31       |
// ATTENTION: it's in little-endian order, so the sub_type_len is 1 in fact.
class ObFeedbackProxyUtils
{
public:
  static const int64_t KEY_PLACEHOLDER_LENGTH = 2;
  static const int64_t VALUE_LENGTH_PLACEHOLDER_LENGTH = 4;
  static const int64_t MAX_FEEDBACK_INFO_LENGTH = 0xFFFFFFFF;

public:
  static int append_feedback_proxy_info(common::ObIAllocator &allocator,
                                        common::ObIArray<obmysql::Obp20Encoder *> *extra_info_ecds,
                                        sql::ObSQLSessionInfo &sess);
  // This interface will append feedback_proxy_info into extra_info_ecds directly,
  // and not depend on variables on the session. So we do not need to check or set
  // the flag 'is_need_feedback_proxy_info' on the session.
  template <typename T>
  static int append_feedback_proxy_info(common::ObIAllocator &allocator,
                                        common::ObIArray<obmysql::Obp20Encoder *> *extra_info_ecds,
                                        const ObFeedbackProxyInfoType &key,
                                        const T &value)
  {
    int ret = OB_SUCCESS;
    int64_t len = 0;  // it's the length of feedback_proxy_info in fact
    int64_t pos = 0;
    char *buf = nullptr;
    void *ecd_buf = nullptr;
    obmysql::Obp20FeedbackProxyInfoEncoder *fb_proxy_info_ecd = nullptr;

    len = get_serialize_size(value);
    if (len == 0) {
      // no new feedback_proxy_info needs to be sent, do nothing
    } else if (OB_UNLIKELY(len < 0 || len > MAX_FEEDBACK_INFO_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "invalid buffer length", K(ret), K(len));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc memory buffer", K(ret), K(len));
    } else if (OB_FAIL(serialize_(key, value, buf, len, pos))) {
      SERVER_LOG(WARN, "serialize feedback_proxy_info failed", K(ret), K(len));
    } else if (OB_ISNULL(ecd_buf = allocator.alloc(sizeof(obmysql::Obp20FeedbackProxyInfoEncoder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc memory for ecd", K(ret), K(sizeof(obmysql::Obp20FeedbackProxyInfoEncoder)));
    } else if (FALSE_IT(fb_proxy_info_ecd = new (ecd_buf) obmysql::Obp20FeedbackProxyInfoEncoder())) {
    } else if (FALSE_IT(fb_proxy_info_ecd->feedback_proxy_info_.assign(buf, pos))) {  // transfer lifecycle to feedback_proxy_info
    } else if (OB_FAIL(extra_info_ecds->push_back(fb_proxy_info_ecd))) {
      fb_proxy_info_ecd->reset();  // recycle the memory of feedback_proxy_info
      SERVER_LOG(WARN, "failed to add extra info kv", K(ret), K(fb_proxy_info_ecd));
    } else {
      SERVER_LOG(INFO,
                 "append_feedback_proxy_info successfully",
                 K(ObHexStringWrap(fb_proxy_info_ecd->feedback_proxy_info_)),
                 K(len),
                 K(pos));
    }
    // any error means append feedback info failed, we should recycle the memory
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(buf)) {
        allocator.free(buf);
      }
      if (OB_NOT_NULL(ecd_buf)) {
        allocator.free(ecd_buf);
      }
    }
    return ret;
  }

private:
  static int64_t get_serialize_size_(sql::ObSQLSessionInfo &sess);
  static int serialize_(sql::ObSQLSessionInfo &sess, char *buf, int64_t len, int64_t &pos);
  template <typename T>
  static int64_t get_serialize_size_(const T &value)
  {
    int64_t val_length = serialization::encoded_length(value);
    return KEY_PLACEHOLDER_LENGTH + VALUE_LENGTH_PLACEHOLDER_LENGTH + val_length;
  }
  template <typename T>
  static int serialize_(const ObFeedbackProxyInfoType &key, const T &value, char *buf, int64_t len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t val_len = 0;
    int64_t org_pos = pos;  // used for debug

    int16_t type = static_cast<int16_t>(key);

    // reserved buffer for type and len
    if (pos + KEY_PLACEHOLDER_LENGTH + VALUE_LENGTH_PLACEHOLDER_LENGTH > len) {
      ret = OB_SIZE_OVERFLOW;
      SERVER_LOG(WARN, "buffer size overflow", K(ret), K(pos), K(len), K(type), K(value));
    } else if (OB_FAIL(serialization::encode(buf + KEY_PLACEHOLDER_LENGTH + VALUE_LENGTH_PLACEHOLDER_LENGTH, len, val_len, value))) {
      // 1. skip type and len, serialize value firstly
      SERVER_LOG(WARN, "serialize value in ObFeedbackProxyInfo failed", K(ret), K(pos), K(len), K(type), K(value));
    } else if (OB_FAIL(ObProtoTransUtil::store_type_and_len(buf, len, pos, type, val_len))) {
      // 2. fill type and len
      SERVER_LOG(WARN, "store_type_and_len failed", K(ret), K(pos), K(len), K(key), K(value));
    } else {
      // 3. compute the actual pos
      pos += val_len;
    }
    SERVER_LOG(DEBUG,
               "ObFeedbackProxyUtils::serialize_ ",
               K(ret),
               KPHEX(buf + org_pos, pos - org_pos),
               K(len),
               K(org_pos),
               K(pos),
               K(type),
               K(value));
    return ret;
  }

private:
  static ObIsLockSessionInfo is_lock_session;
};
}  // namespace observer
}  // namespace oceanbase
#endif
