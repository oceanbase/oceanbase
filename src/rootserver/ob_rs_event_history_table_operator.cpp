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

#include "ob_rs_event_history_table_operator.h"
#include "share/config/ob_server_config.h"
namespace oceanbase {
namespace rootserver {
using namespace common;
using namespace share;

int ObRsEventHistoryTableOperator::init(common::ObMySQLProxy& proxy, const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObEventHistoryTableOperator::init(proxy))) {
  } else {
    const bool is_rs_event = true;
    set_addr(self_addr, is_rs_event);
    set_event_table(share::OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME);
  }
  return ret;
}

ObRsEventHistoryTableOperator& ObRsEventHistoryTableOperator::get_instance()
{
  static ObRsEventHistoryTableOperator instance;
  return instance;
}

int ObRsEventHistoryTableOperator::async_delete()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObSqlString sql;
    const bool is_delete = true;
    if (OB_SUCCESS == ret) {
      const int64_t rs_delete_timestap = now - GCONF.ob_event_history_recycle_interval;
      if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE gmt_create < usec_to_time(%ld) LIMIT 1024",
              share::OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME,
              rs_delete_timestap))) {
        SHARE_LOG(WARN, "assign_fmt failed", K(ret));
      } else if (OB_FAIL(add_task(sql, is_delete))) {
        SHARE_LOG(WARN, "add_task failed", K(sql), K(is_delete), K(ret));
      }
    }
  }
  return ret;
}
}  // end namespace rootserver
}  // end namespace oceanbase
