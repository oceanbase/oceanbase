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

#define USING_LOG_PREFIX SQL

#include "observer/mysql/ob_feedback_proxy_utils.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace observer
{
ObIsLockSessionInfo ObFeedbackProxyUtils::is_lock_session(ObFeedbackProxyInfoType::IS_LOCK_SESSION, '1');

int ObFeedbackProxyUtils::append_feedback_proxy_info(common::ObIAllocator &allocator,
                                                     ObIArray<obmysql::Obp20Encoder *> *extra_info_ecds,
                                                     sql::ObSQLSessionInfo &sess)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;  // it's the length of feedback_proxy_info in fact
  int64_t pos = 0;
  char *buf = nullptr;
  void *ecd_buf = nullptr;
  obmysql::Obp20FeedbackProxyInfoEncoder *fb_proxy_info_ecd = nullptr;

  if (sess.is_need_send_feedback_proxy_info()) {
    len = get_serialize_size_(sess);
    LOG_DEBUG("begin to feedback proxy info", K(sess.get_sessid()), K(len));
    if (len == 0) {
      // no new feedback_proxy_info needs to be sent, do nothing
    } else if (OB_UNLIKELY(len < 0 || len > MAX_FEEDBACK_INFO_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid buffer length", K(ret), K(len));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory buffer", K(ret), K(len));
    } else if (OB_FAIL(serialize_(sess, buf, len, pos))) {
      LOG_WARN("serialize feedback_proxy_info failed", K(ret), K(len));
    } else if (OB_ISNULL(ecd_buf = allocator.alloc(sizeof(obmysql::Obp20FeedbackProxyInfoEncoder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory for ecd", K(ret), K(sizeof(obmysql::Obp20FeedbackProxyInfoEncoder)));
    } else if (FALSE_IT(fb_proxy_info_ecd = new (ecd_buf) obmysql::Obp20FeedbackProxyInfoEncoder())) {
    } else if (FALSE_IT(fb_proxy_info_ecd->feedback_proxy_info_.assign(buf, pos))) {
    } else if (OB_FAIL(extra_info_ecds->push_back(fb_proxy_info_ecd))) {
      fb_proxy_info_ecd->reset();
      LOG_WARN("failed to add extra info kv", K(ret), K(fb_proxy_info_ecd));
    } else {
      LOG_INFO("append_feedback_proxy_info successfully",
                K(sess.get_sessid()),
                K(ObHexStringWrap(fb_proxy_info_ecd->feedback_proxy_info_)),
                K(len),
                K(pos));
      // only respond once
      sess.set_need_send_feedback_proxy_info(false);
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
  }
  return ret;
}

int64_t ObFeedbackProxyUtils::get_serialize_size_(sql::ObSQLSessionInfo &sess)
{
  int64_t size = 0;
  size += is_lock_session.get_serialize_size();
  // add other information here...
  return size;
}

int ObFeedbackProxyUtils::serialize_(sql::ObSQLSessionInfo &sess, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!sess.is_lock_session()) {
    is_lock_session.set_value('0');
  } else {
    is_lock_session.set_value('1');
  }
  if (OB_FAIL(is_lock_session.serialize(buf, len, pos))) {
    LOG_WARN("serialize is_lock_session failed", K(ret), K(is_lock_session));
  }
  // add other information here...
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
