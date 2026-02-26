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
#ifdef DEF_INNER_SQL_WAIT
DEF_INNER_SQL_WAIT(NULL_INNER_SQL, 0)

//***************  start of seq ***************
DEF_INNER_SQL_WAIT(SEQUENCE_SAVE, 10001)
DEF_INNER_SQL_WAIT(SEQUENCE_LOAD, 10002)
//***************  end of seq   ***************

//***************  start of wr ***************
DEF_INNER_SQL_WAIT(WR_TAKE_SNAPSHOT, 20001)
DEF_INNER_SQL_WAIT(WR_DEL_SNAPSHOT, 20002)
//***************  end of wr   ***************

//***************  start of tx ***************
DEF_INNER_SQL_WAIT(TX_UPDATE_WEAK_READ_VERSION, 30001)
DEF_INNER_SQL_WAIT(TX_GET_WEAK_READ_VERSION_RANGE, 30002)
//***************  end of tx   ***************

//***************  start of rs ***************
DEF_INNER_SQL_WAIT(RS_GET_ARBITRATION_MEMBER, 40001)
DEF_INNER_SQL_WAIT(RS_CHECK_SYS_VIEW_EXPANSION, 40002)
DEF_INNER_SQL_WAIT(RS_GET_SERVICE_EPOCH, 40003)
DEF_INNER_SQL_WAIT(RS_LOAD_PURE_TENANT_INFO, 40004)
DEF_INNER_SQL_WAIT(RS_CREATE_INDEX_BUILD_REPLICA, 40005)
DEF_INNER_SQL_WAIT(RS_GET_TENANT_ARBITRATION_SERVICE_STATUS, 40006)
DEF_INNER_SQL_WAIT(RS_GET_META_TENANT_INFO, 40007)
//***************  end of rs   ***************

//***************  start of log ***************
DEF_INNER_SQL_WAIT(LOG_GET_ALL_LS_STATUS_BY_ORDER, 50001)
DEF_INNER_SQL_WAIT(LOG_GET_BLACK_LIST_LS_INFO, 50002)
DEF_INNER_SQL_WAIT(LOG_UPDATE_LS_RECOVERY_STAT, 50003)
DEF_INNER_SQL_WAIT(LOG_GET_LS_RECOVERY_STAT, 50004)
DEF_INNER_SQL_WAIT(LOG_GET_LS_PRIMARY_ZONE_INFO, 50005)
DEF_INNER_SQL_WAIT(LOG_GET_TENANT_RECOVERY_STAT, 50006)
//***************  end of log   ***************


//***************  start of observer ***************
DEF_INNER_SQL_WAIT(OMT_FETCH_ALL_SRS, 60001)
//***************  end of observer   ***************

//***************  start of sql ***************
DEF_INNER_SQL_WAIT(SQL_DYNAMIC_SAMPLING_ESTIMATE_ROWCOUNT, 70001)
//***************  end of sql   ***************

//***************  start of location ***************
DEF_INNER_SQL_WAIT(RENEW_TABLET_LOCATION, 80001)  // 获取location
DEF_INNER_SQL_WAIT(GET_TABLET_LOCATION, 80002)  // 刷新location
//***************  end of location   ***************

//***************  start of schema ***************
DEF_INNER_SQL_WAIT(WAIT_REFRESH_SCHEMA, 90001)  // 同步等待schema刷新到指定版本
DEF_INNER_SQL_WAIT(ASYNC_REFRESH_SCHEMA, 90002)  // 刷新schema任务
DEF_INNER_SQL_WAIT(REFRESH_SCHEMA, 90003)  // 后台刷新schema任务
//***************  end of schema   ***************


#endif // DEF_INNER_SQL_WAIT

#ifndef _OB_INNER_SQL_WAIT_TYPE_H_
#define _OB_INNER_SQL_WAIT_TYPE_H_
namespace oceanbase
{
namespace common
{
enum ObInnerSqlWaitTypeId : int64_t
  {
#define DEF_INNER_SQL_WAIT(def_name, enum_id) def_name = enum_id,
#include "lib/wait_event/ob_inner_sql_wait_type.h"
#undef DEF_INNER_SQL_WAIT
  };
static inline const char* inner_sql_action_name(ObInnerSqlWaitTypeId id)
{
  static const char* const NULL_INNER_SQL = "UNDEFINED";
  switch( id )
  {
#define DEF_INNER_SQL_WAIT(def_name, enum_id)                                   \
      case (ObInnerSqlWaitTypeId::def_name) : return #def_name;
#include "lib/wait_event/ob_inner_sql_wait_type.h"
#undef DEF_INNER_SQL_WAIT
  default: return NULL_INNER_SQL;
  }
}
} // end common
} // end oceanbase

#endif /* _OB_INNER_SQL_WAIT_TYPE_H_ */
