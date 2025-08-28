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
  /**
   * @description:
   * Use the DEF_MODULE macro in this file to define MODULE. Use the DEF_EVENT macro to define EVENT.
   * Each MODULE can define multiple EVENTS of the same type.
   *
   * ******  NOTICE  *********
   * To ensure compatibility, the already defined MODULE and EVENT cannot be modified.
   * Only MODULE, EVENT, and EVENT fields can be added.
   *
   *    DEF_MODULE(MODULE, MODULE_STR)
   *      @param[in] MODULE The label of MODULE
   *      @param[in] MODULE_STR The corresponding string of MODULE
   *
   *    DEF_EVENT(MODULE, EVENT, EVENT_STR, NAME1 ...)
   *      @param[in] MODULE The label of MODULE
   *      @param[in] EVENT The label of EVENT
   *      @param[in] EVENT_STR The corresponding string of EVENT
   *      @param[in] NAME1 The field name of the key information of EVENT,
   *                       up to 6 key information fields are supported
   *
   * Use the TENANT_EVENT macro to record the EVENT of the tenant,
   * and fill in the field values in the order defined by DEF_EVENT.
   *    TENANT_EVENT(tenant_id, MODULE, EVENT, event_timestamp, user_ret, cost_us, VALUE1 ...)
   *      @param[in] tenant_id The tenant ID of the ordinary tenant
   *      @param[in] MODULE The label of MODULE
   *      @param[in] EVENT The label of EVENT
   *      @param[in] event_timestamp the timestamp when the event occurs
   *      @param[in] user_ret return code
   *      @param[in] cost_us cost time
   *      @param[in] VALUE1 The value of the field of the key information of EVENT,
   *                        fill in the value of the field according to the order defined by DEF_EVENT
   * The EVENT recorded by TENANT_EVENT will be stored in the inner table
   * (__all_tenant_event_history) under the META tenant space corresponding to tenant_id,
   *  and displayed through the views CDB_OB_TENANT_EVENT_HISTORY/DBA_OB_TENANT_EVENT_HISTORY.
   *
   * ******  NOTICE  *********
   * TENANT_EVENT is inserted into the table asynchronously, gmt_create is the input arg event_timestamp
   * The EVENTS recorded in the inner table (__all_tenant_event_history) under the META tenant space
   * will not be cleared until the tenant is deleted.
   * Therefore, in order to prevent too many EVENTS, the interval between recording EVENTS should not be too frequent
   */
#ifdef DEF_MODULE
#ifdef DEF_EVENT
  /**
   * @description:
   * Log events related to tenant role change
   * failover to primary/switchover to primary/switchover to standby
   */
  class TENANT_ROLE_CHANGE {
    public:
      DEF_MODULE(TENANT_ROLE_CHANGE, "TENANT ROLE CHANGE");
      DEF_EVENT(TENANT_ROLE_CHANGE, SWITCHOVER_TO_PRIMARY_START, "SWITCHOVER TO PRIMARY START",
                STMT_STR,
                TENANT_INFO);
      DEF_EVENT(TENANT_ROLE_CHANGE, SWITCHOVER_TO_PRIMARY_END, "SWITCHOVER TO PRIMARY END",
                STMT_STR,
                TENANT_INFO,
                SWITCHOVER_SCN,
                COST_DETAIL,
                ALL_LS);
      DEF_EVENT(TENANT_ROLE_CHANGE, SWITCHOVER_TO_STANDBY_START, "SWITCHOVER TO STANDBY START",
                STMT_STR,
                TENANT_INFO);
      DEF_EVENT(TENANT_ROLE_CHANGE, SWITCHOVER_TO_STANDBY_END, "SWITCHOVER TO STANDBY END",
                STMT_STR,
                TENANT_INFO,
                SWITCHOVER_SCN,
                COST_DETAIL,
                ALL_LS);
      DEF_EVENT(TENANT_ROLE_CHANGE, FAILOVER_TO_PRIMARY_START, "FAILOVER TO PRIMARY START",
                STMT_STR,
                TENANT_INFO);
      DEF_EVENT(TENANT_ROLE_CHANGE, FAILOVER_TO_PRIMARY_END, "FAILOVER TO PRIMARY END",
                STMT_STR,
                TENANT_INFO,
                FAILOVER_SCN,
                COST_DETAIL,
                ALL_LS);
      DEF_EVENT(TENANT_ROLE_CHANGE, WAIT_LOG_SYNC, "WAIT LOG SYNC",
                IS_SYS_LS_SYNCED,
                IS_ALL_LS_SYNCED,
                NON_SYNC_INFO);
  };

  class SERVICE_NAME {
    public:
      DEF_MODULE(SERVICE_NAME, "SERVICE NAME");

      DEF_EVENT(SERVICE_NAME, CREATE_SERVICE, "CREATE SERVICE",
                SERVICE_NAME_STRING,
                CREATED_SERVICE_NAME);

      DEF_EVENT(SERVICE_NAME, DELETE_SERVICE, "DELETE SERVICE",
                DELETED_SERVICE_NAME);

      DEF_EVENT(SERVICE_NAME, START_SERVICE, "START SERVICE",
                SERVICE_NAME_BEFORE,
                SERVICE_NAME_AFTER);

      DEF_EVENT(SERVICE_NAME, STOP_SERVICE, "STOP SERVICE",
                SERVICE_NAME_BEFORE,
                SERVICE_NAME_AFTER);

      DEF_EVENT(SERVICE_NAME, KILL_CONNECTIONS_OF_SERVICE_NAME, "KILL CONNECTIONS OF SERVICE NAME",
                SERVICE_NAME,
                KILLED_CONNECTIONS_COUNT,
                KILLED_CONNECTIONS_LIST);

      DEF_EVENT(SERVICE_NAME, BROADCAST_SERVICE_NAME, "BROADCAST SERVICE NAME",
                EPOCH,
                TARGET_SERVICE_NAME_ID,
                SERVICE_NAME_LIST,
                SERVICE_NAME_COMMAND_TYPE,
                TARGET_SERVERS_LIST,
                SUCCESS_SERVERS_LIST);
  };

  class DBMS_PARTITION {
    public:
      DEF_MODULE(DBMS_PARTITION, "DBMS_PARTITION");

      DEF_EVENT(DBMS_PARTITION, MANAGE_DYNAMIC_PARTITION, "MANAGE_DYNAMIC_PARTITION",
                SUCCESS_TABLE_ID_LIST,
                FAILED_TABLE_ID_LIST);
  };
#endif
#endif
////////////////////////////////////////////////////////////////
#ifndef _OB_TENANT_EVENT_DEF_H
#define _OB_TENANT_EVENT_DEF_H 1
#include <stdint.h>
#include "rootserver/ob_tenant_event_history_table_operator.h" // TENANT_EVENT_ADD
namespace oceanbase
{
namespace tenant_event
{
#define DEF_MODULE(MODULE, MODULE_STR) \
  static constexpr const char* const MODULE##_NAME = #MODULE; \
  static constexpr const char* const MODULE##_STR = MODULE_STR; 
#define DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  static constexpr const char* const EVENT##_NAME = #EVENT; \
  static constexpr const char* const EVENT##_STR = EVENT_STR; 
#define OneArguments(MODULE)
#define TwoArguments(MODULE, EVENT)
#define ThreeArguments(MODULE, EVENT, EVENT_STR) \
  DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  template<> \
  static void MODULE##_##EVENT##_func(const uint64_t tenant_id, const char * const module, const char * const event, \
                               const int64_t event_timestamp, const int user_ret, const int64_t cost_us) \
  { \
    TENANT_EVENT_ADD(tenant_id, module, event, event_timestamp, user_ret, cost_us); \
    return ;\
  }
#define FourArguments(MODULE, EVENT, EVENT_STR, NAME1) \
  DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  template<typename T1> \
  static void MODULE##_##EVENT##_func(const uint64_t tenant_id, const char * const module, const char * const event, \
                               const int64_t event_timestamp, const int user_ret, const int64_t cost_us, \
                               const T1 &value1) \
  { \
    TENANT_EVENT_ADD(tenant_id, module, event, event_timestamp, user_ret, cost_us, #NAME1, value1); \
    return ;\
  }
#define FiveArguments(MODULE, EVENT, EVENT_STR, NAME1, NAME2) \
  DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  template<typename T1, typename T2> \
  static void MODULE##_##EVENT##_func(const uint64_t tenant_id, const char * const module, const char * const event, \
                               const int64_t event_timestamp, const int user_ret, const int64_t cost_us, \
                               const T1 &value1, const T2 &value2) \
  { \
    TENANT_EVENT_ADD(tenant_id, module, event, event_timestamp, user_ret, cost_us, #NAME1, value1, #NAME2, value2); \
    return ;\
  }
#define SixArguments(MODULE, EVENT, EVENT_STR, NAME1, NAME2, NAME3) \
  DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  template<typename T1, typename T2, typename T3> \
  static void MODULE##_##EVENT##_func(const uint64_t tenant_id, const char * const module, const char * const event, \
                               const int64_t event_timestamp, const int user_ret, const int64_t cost_us, \
                               const T1 &value1, const T2 &value2, \
                               const T3 &value3) \
  { \
    TENANT_EVENT_ADD(tenant_id, module, event, event_timestamp, user_ret, cost_us, #NAME1, value1, #NAME2, value2, \
                     #NAME3, value3); \
    return ;\
  }
#define SevenArguments(MODULE, EVENT, EVENT_STR, NAME1, NAME2, NAME3, NAME4) \
  DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  template<typename T1, typename T2, typename T3, typename T4> \
  static void MODULE##_##EVENT##_func(const uint64_t tenant_id, const char * const module, const char * const event, \
                               const int64_t event_timestamp, const int user_ret, const int64_t cost_us, \
                               const T1 &value1, const T2 &value2, \
                               const T3 &value3, const T4 &value4) \
  { \
    TENANT_EVENT_ADD(tenant_id, module, event, event_timestamp, user_ret, cost_us, #NAME1, value1, #NAME2, value2, \
                     #NAME3, value3, #NAME4, value4); \
    return ;\
  }
#define EightArguments(MODULE, EVENT, EVENT_STR, NAME1, NAME2, NAME3, NAME4, NAME5) \
  DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  template<typename T1, typename T2, typename T3, typename T4, \
      typename T5> \
  static void MODULE##_##EVENT##_func(const uint64_t tenant_id, const char * const module, const char * const event, \
                               const int64_t event_timestamp, const int user_ret, const int64_t cost_us, \
                               const T1 &value1, const T2 &value2, \
                               const T3 &value3, const T4 &value4, \
                               const T5 &value5) \
  { \
    TENANT_EVENT_ADD(tenant_id, module, event, event_timestamp, user_ret, cost_us, #NAME1, value1, #NAME2, value2, \
                     #NAME3, value3, #NAME4, value4, #NAME5, value5); \
    return ;\
  }
#define NineArguments(MODULE, EVENT, EVENT_STR, NAME1, NAME2, NAME3, NAME4, NAME5, NAME6) \
  DEF_EVENT_COMMON(EVENT, EVENT_STR) \
  template<typename T1, typename T2, typename T3, typename T4, \
      typename T5, typename T6> \
  static void MODULE##_##EVENT##_func(const uint64_t tenant_id, const char * const module, const char * const event, \
                               const int64_t event_timestamp, const int user_ret, const int64_t cost_us, \
                               const T1 &value1, const T2 &value2, \
                               const T3 &value3, const T4 &value4, \
                               const T5 &value5, const T6 &value6) \
  { \
    TENANT_EVENT_ADD(tenant_id, module, event, event_timestamp, user_ret, cost_us, #NAME1, value1, #NAME2, value2, \
                     #NAME3, value3, #NAME4, value4, #NAME5, value5, #NAME6, value6); \
    return ;\
  }
 
#define GetMacro(_1, _2, _3, _4, _5, _6, _7, _8, _9, NAME, ...) NAME
#define DEF_EVENT(...) \
  GetMacro(__VA_ARGS__, NineArguments, EightArguments, SevenArguments, SixArguments, FiveArguments, FourArguments, ThreeArguments, TwoArguments, OneArgument, ...)(__VA_ARGS__)

#define TENANT_EVENT(tenant_id, MODULE, EVENT, event_timestamp, user_ret, cost_us, args...) \
  MODULE::MODULE##_##EVENT##_func(tenant_id, MODULE::MODULE##_STR, MODULE::EVENT##_STR, event_timestamp, user_ret, cost_us, args)

#include "ob_tenant_event_def.h"
#undef DEF_MODULE
#undef DEF_EVENT
#undef DEF_EVENT_COMMON
#undef OneArguments
#undef TwoArguments
#undef ThreeArguments
#undef FourArguments
#undef FiveArguments
#undef SixArguments
#undef SevenArguments
#undef EightArguments
#undef NineArguments
#undef GetMacro
} // end namespace tenant_event
} // end namespace oceanbase
#endif /* _OB_TENANT_EVENT_DEF_H */