/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "observer/mysql/ob_feedback_proxy_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace observer
{
ObIsLockSessionInfo ObFeedbackProxyUtils::is_lock_session(ObFeedbackProxyInfoType::IS_LOCK_SESSION, '1');

void ObFeedbackProxyUtils::refresh_temp_table_feedback_state_(sql::ObSQLSessionInfo &sess)
{
  // Mark a session when it tracks a GTT session tablet and is eligible for
  // non-forced routing. Refresh the flag after statement execution and only
  // feedback when the effective value changes.
  const uint64_t data_version = sess.get_min_data_version_of_init_sess();
  if ((data_version >= MOCK_DATA_VERSION_4_4_2_1 && data_version < DATA_VERSION_4_5_0_0)
      || data_version >= DATA_VERSION_4_6_1_0) {
    const bool is_used = sess.get_gtt_tablet_info_map().has_session_tablet();
    // Keep a marked session sticky while any session tablet remains. Changes to
    // the routing configuration only affect sessions that are not marked.
    bool flag = is_used && sess.is_temporary_table_session();
    if (is_used && !flag) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(sess.get_effective_tenant_id()));
      const bool need_strong_routing = tenant_config.is_valid()
          ? !tenant_config->_enable_gtt_non_forced_routing
          : true;
      const bool strong_routing = INVALID_SESSID != sess.get_client_sid() ? need_strong_routing : true;
      flag = is_used && !strong_routing;
    }
    sess.mark_session_temp_table_used(flag);
  }
}

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

  refresh_temp_table_feedback_state_(sess);
  if (sess.is_need_send_feedback_proxy_info()) {
    len = get_serialize_size_(sess);
    LOG_DEBUG("begin to feedback proxy info", K(sess.get_server_sid()), K(len));
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
                K(sess.get_server_sid()),
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
  const ObIsTemporaryTableSessionInfo is_temporary_table_session(
      ObFeedbackProxyInfoType::IS_TEMPORARY_TABLE_SESSION, '1');
  size += is_lock_session.get_serialize_size();
  size += is_temporary_table_session.get_serialize_size();
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
  const ObIsTemporaryTableSessionInfo is_temporary_table_session(
      ObFeedbackProxyInfoType::IS_TEMPORARY_TABLE_SESSION,
      sess.is_temporary_table_session() ? '1' : '0');
  if (FAILEDx(is_temporary_table_session.serialize(buf, len, pos))) {
    LOG_WARN("serialize is_temporary_table_session failed", K(ret), K(is_temporary_table_session));
  }
  // add other information here...
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
