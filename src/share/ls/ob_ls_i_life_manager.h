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

#ifndef OCEANBASE_SHARE_OB_LS_I_LIFE_MANAGER_H_
#define OCEANBASE_SHARE_OB_LS_I_LIFE_MANAGER_H_

#include "share/ob_share_util.h"
#include "share/ob_tenant_switchover_status.h"//ObTenantSwitchoverStatus
#include "common/ob_timeout_ctx.h"
#include "share/config/ob_server_config.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/scn.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObMySQLProxy;
class ObISQLClient;
class ObString;
namespace sqlclient
{
class ObMySQLResult;
}

}
namespace share
{
class ObLSID;
struct ObLSStatusInfo;

#define ALL_LS_EVENT_ADD(tenant_id, ls_id, event, ret, sql, args...)\
  do {\
    const int64_t MAX_VALUE_LENGTH = 512; \
    char VALUE[MAX_VALUE_LENGTH] = {""}; \
    int64_t pos = 0; \
    common::ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();\
    common::databuff_print_kv(VALUE, MAX_VALUE_LENGTH, pos, ##args, KPC(trace_id)); \
    ROOTSERVICE_EVENT_ADD("LS", event, "tenant_id", tenant_id, "ls_id", ls_id,\
        "ret", ret, "sql", ObHexEscapeSqlStr(sql.string()),\
        "", NULL, "", NULL, ObHexEscapeSqlStr(VALUE));\
  } while (0)

#define LS_EVENT_ADD(tenant_id, ls_id, event, ret, paxos_cnt, success_cnt, args...)\
  do {\
    const int64_t MAX_VALUE_LENGTH = 512; \
    char VALUE[MAX_VALUE_LENGTH] = {""}; \
    int64_t pos = 0; \
    common::ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();\
    common::databuff_print_kv(VALUE, MAX_VALUE_LENGTH, pos, ##args, KPC(trace_id)); \
    ROOTSERVICE_EVENT_ADD("LS", event, "tenant_id", tenant_id, "ls_id", ls_id,\
        "ret", ret, "success_cnt", success_cnt, "paxos_cnt", paxos_cnt, "", NULL, ObHexEscapeSqlStr(VALUE));\
  } while (0)


enum ObLSStatus
{
  OB_LS_EMPTY = -1,
  OB_LS_CREATING = 0,
  OB_LS_CREATED,
  OB_LS_NORMAL,
  OB_LS_DROPPING,
  OB_LS_TENANT_DROPPING,
  OB_LS_WAIT_OFFLINE,
  OB_LS_CREATE_ABORT,
  OB_LS_PRE_TENANT_DROPPING,//only for sys ls
  OB_LS_DROPPED,//for __all_ls
  OB_LS_MAX_STATUS,
};

/**
 * @description:
 *    In order to let switchover switch the accessmode of all LS correctly,
 *    when creating, deleting, and updating LS status,
 *    it needs to be mutually exclusive with switchover status of __all_tenant_info
 */

/*
 *log stream lifetime description:
 If an inner_table needs to be aware of the creation and deletion of the log stream; 
 or it needs to be an atomic transaction with the insertion and deletion of __all_ls_status.
 The operation of this internal table needs to inherit this class, implement the following two methods,
 and register it in the class of ObLSLifeAgentManager.
 The specific implementation can refer to ObLSRecoveryStatOperator
 * */
class ObLSLifeIAgent
{
public:
  ObLSLifeIAgent() {}
  virtual ~ObLSLifeIAgent () {} 
  //create new ls
  virtual int create_new_ls(const ObLSStatusInfo &ls_info,
                            const SCN &create_scn,
                            const common::ObString &zone_priority,
                            const share::ObTenantSwitchoverStatus &working_sw_status,
                            ObMySQLTransaction &trans) = 0;
  //drop ls
  virtual int drop_ls(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) = 0;
  //set ls to offline
  virtual int set_ls_offline(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const share::ObLSStatus &ls_status,
                      const SCN &drop_scn,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) = 0;
  //update ls primary zone
  virtual int update_ls_primary_zone(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const common::ObZone &primary_zone,
      const common::ObString &zone_priority,
      ObMySQLTransaction &trans) = 0;

  /*
   *description: The table related to the log stream status needs to be reported to the meta tenant or system tenant.
   *param[in]: the ls's tenant_id
   *return : need to operator's tenant_id
   * */
  static uint64_t get_exec_tenant_id(const uint64_t tenant_id)
  {
    return get_private_table_exec_tenant_id(tenant_id);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSLifeIAgent);
};

/*
 *description:
 * Many inner_tables related to log streams require read and write operations. 
 * Most of them are written in units of log streams and scanned in units of log streams or full tables.
 * Therefore, a template class that provides read and write is perfected.
 * To use this template class, you need to implement the interface: get_exec_tenant_id and the fill_cell.
 * For specific implementation, please refer to ObLSStatusOperator*/
class ObLSTemplateOperator
{
public:
  ObLSTemplateOperator() {}
  virtual ~ObLSTemplateOperator() {}

  /*
   * description: write to the inner_table by ls, it will check affected_row must equal to one,
   *              the interface need operator has get_exec_tenant_id
   * param[in]: tenant_id of ls
   * param[in]: the exec_write sql
   * param[in]: the inner_table to operator
   * param[in]: client*/
  template <typename TableOperator>
  int exec_write(const uint64_t &tenant_id, const common::ObSqlString &sql,
                 TableOperator *table_operator, ObISQLClient &client,
                 const bool ignore_row = false);
  /*
   * description: read the inner_table.the interface need operator has get_exec_tenant_id and fill_cell
   * param[in]: tenant_id of ls
   * param[in]: the exec_read sql
   * param[in]: client
   * param[in]: the inner_table to operator
   * param[in]: the result array of inner_table*/

  template <typename TableOperator, typename LS_Result>
  int exec_read(const uint64_t &tenant_id,
                       const common::ObSqlString &sql, ObISQLClient &client,
                       TableOperator *table_operator,
                       common::ObIArray<LS_Result> &res);
  template<typename T>
  static int hex_str_to_type(const common::ObString &str, T &list);

  template<typename T>
  static int type_to_hex_str(const T &list,
    common::ObIAllocator &allocator,
    common::ObString &hex_str);
 private:
  DISALLOW_COPY_AND_ASSIGN(ObLSTemplateOperator);
};

template <typename TableOperator, typename LS_Result>
int ObLSTemplateOperator::exec_read(const uint64_t &tenant_id,
                        const common::ObSqlString &sql, ObISQLClient &client,
                        TableOperator *table_operator, common::ObIArray<LS_Result> &ls_res)
{
  int ret = OB_SUCCESS;
  ls_res.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_ISNULL(table_operator)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), KP(table_operator));
  } else {
    ObTimeoutCtx ctx;
    const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
    uint64_t exec_tenant_id = table_operator->get_exec_tenant_id(tenant_id);
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to get exec tenant id", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
      SHARE_LOG(WARN, "failed to set default timeout ctx", KR(ret), K(default_timeout));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(client.read(res, exec_tenant_id, sql.ptr()))) {
          SHARE_LOG(WARN, "failed to read", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "failed to get sql result", KR(ret));
        } else {
           LS_Result single_res;
           while (OB_SUCC(ret) && OB_SUCC(result->next())) {
             single_res.reset();
             if (OB_FAIL(table_operator->fill_cell(result, single_res))) {
               SHARE_LOG(WARN, "failed to read cell from result", KR(ret), K(sql));
             } else if (OB_FAIL(ls_res.push_back(single_res))) {
               SHARE_LOG(WARN, "failed to get cell", KR(ret), K(single_res));
             }
           }  // end while
           if (OB_ITER_END == ret) {
             ret = OB_SUCCESS;
           } else if (OB_FAIL(ret)) {
             SHARE_LOG(WARN, "failed to get ls", KR(ret));
           } else {
             ret = OB_ERR_UNEXPECTED;
             SHARE_LOG(WARN, "ret can not be success", KR(ret));
           }
        }  // end heap var 
      }
    }//end else
  }
  return ret;
}

template <typename TableOperator>
int ObLSTemplateOperator::exec_write(const uint64_t &tenant_id,
                                      const common::ObSqlString &sql,
                                      TableOperator *table_operator, 
                                      ObISQLClient &client,
                                      const bool ignore_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql.empty() || OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(table_operator)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KR(ret), K(sql), K(tenant_id), KP(table_operator));
  } else {
    int64_t affected_rows = 0;
    ObTimeoutCtx ctx;
    const int64_t timestamp = ObTimeUtility::current_time();
    const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
    uint64_t exec_tenant_id = table_operator->get_exec_tenant_id(tenant_id);
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to get exec tenant id", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx,
                                                            default_timeout))) {
      SHARE_LOG(WARN, "failed to set default timeout ctx", KR(ret),
               K(default_timeout));
    } else if (OB_FAIL(
                   client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      SHARE_LOG(WARN, "failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    } else if (!is_single_row(affected_rows) && !ignore_row) {
      ret = OB_NEED_RETRY;
      SHARE_LOG(WARN, "expected one row, may need retry", KR(ret), K(affected_rows),
               K(sql), K(ignore_row));
    }
  }
  return ret;
}

template<typename T>
int ObLSTemplateOperator::hex_str_to_type(
    const common::ObString &str,
    T &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  char *deserialize_buf = NULL;
  const int64_t str_size = str.length();
  const int64_t deserialize_size = str.length() / 2 + 1;
  int64_t deserialize_pos = 0;
  ObArenaAllocator allocator("HexValue");
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "str is empty", KR(ret));
  } else if (OB_ISNULL(deserialize_buf = static_cast<char*>(allocator.alloc(deserialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "fail to alloc memory", KR(ret), K(deserialize_size));
  } else if (OB_FAIL(hex_to_cstr(str.ptr(), str_size, deserialize_buf, deserialize_size))) {
    SHARE_LOG(WARN, "fail to get cstr from hex", KR(ret), K(str_size), K(deserialize_size), K(str));
  } else if (OB_FAIL(list.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
    SHARE_LOG(WARN, "fail to deserialize set member list arg", KR(ret), K(deserialize_pos), K(deserialize_size),
             K(str));
  } else if (OB_UNLIKELY(deserialize_pos > deserialize_size)) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "deserialize error", KR(ret), K(deserialize_pos), K(deserialize_size));
  }
  return ret;
}

template<typename T>
int ObLSTemplateOperator::type_to_hex_str(
    const T &list,
    common::ObIAllocator &allocator,
    common::ObString &hex_str)
{
  int ret = OB_SUCCESS;
  char *serialize_buf = NULL;
  const int64_t serialize_size = list.get_serialize_size();
  int64_t serialize_pos = 0;
  char *hex_buf = NULL;
  const int64_t hex_size = 2 * serialize_size;
  int64_t hex_pos = 0;
  if (OB_UNLIKELY(!list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "list is invalid", KR(ret), K(list));
  } else if (OB_UNLIKELY(hex_size > OB_MAX_LONGTEXT_LENGTH + 1)) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "format str is too long", KR(ret), K(hex_size), K(list));
  } else if (OB_ISNULL(serialize_buf = static_cast<char *>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "fail to alloc buf", KR(ret), K(serialize_size));
  } else if (OB_FAIL(list.serialize(serialize_buf, serialize_size, serialize_pos))) {
    SHARE_LOG(WARN, "failed to serialize set list arg", KR(ret), K(list), K(serialize_size), K(serialize_pos));
  } else if (OB_UNLIKELY(serialize_pos > serialize_size)) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "serialize error", KR(ret), K(serialize_pos), K(serialize_size));
  } else if (OB_ISNULL(hex_buf = static_cast<char*>(allocator.alloc(hex_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "fail to alloc memory", KR(ret), K(hex_size));
  } else if (OB_FAIL(hex_print(serialize_buf, serialize_pos, hex_buf, hex_size, hex_pos))) {
    SHARE_LOG(WARN, "fail to print hex", KR(ret), K(serialize_pos), K(hex_size), K(serialize_buf));
  } else if (OB_UNLIKELY(hex_pos > hex_size)) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "encode error", KR(ret), K(hex_pos), K(hex_size));
  } else {
    hex_str.assign_ptr(hex_buf, static_cast<int32_t>(hex_pos));
  }
  return ret;
}

#define DEFINE_IN_TRANS_FUC(func_name, ...)\
int func_name ##_in_trans(__VA_ARGS__, ObMySQLTransaction &trans);\
int func_name(__VA_ARGS__);

#define DEFINE_IN_TRANS_FUC1(func_name, ...)\
static int func_name ##_in_trans(__VA_ARGS__, ObMySQLTransaction &trans);\
static int func_name(__VA_ARGS__, ObISQLClient *proxy);

#define TAKE_IN_TRANS(func_name, proxy, exec_tenant_id, ...)\
do {\
  ObMySQLTransaction trans; \
  if (FAILEDx(trans.start(proxy, exec_tenant_id))) {\
    SHARE_LOG(WARN, "failed to start trans", KR(ret), K(exec_tenant_id));\
  } else if (OB_FAIL(func_name##_in_trans(__VA_ARGS__, trans))) {\
    SHARE_LOG(WARN, "failed to do it in trans", KR(ret));\
  }\
  if (trans.is_started()) {\
    int tmp_ret = OB_SUCCESS;\
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {\
      SHARE_LOG(WARN, "failed to commit trans", KR(ret), KR(tmp_ret));\
      ret = OB_SUCC(ret) ? tmp_ret : ret;\
    }\
  }\
} while (0)

#define START_TRANSACTION(proxy, exec_tenant_id)                          \
  ObMySQLTransaction trans;                                               \
  if (FAILEDx(trans.start(proxy, exec_tenant_id))) {                      \
    SHARE_LOG(WARN, "failed to start trans", KR(ret), K(exec_tenant_id)); \
  }
#define END_TRANSACTION(trans)\
  if (trans.is_started()) {\
    int tmp_ret = OB_SUCCESS;\
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {\
      ret = OB_SUCC(ret) ? tmp_ret : ret;\
      SHARE_LOG(WARN, "failed to end trans", KR(ret), K(tmp_ret));\
    }\
  }
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_I_LIFE_MANAGER_H_ */
