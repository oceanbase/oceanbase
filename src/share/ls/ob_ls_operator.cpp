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

#define USING_LOG_PREFIX SHARE

#include "ob_ls_operator.h"
#include "share/ob_errno.h"
#include "share/ob_share_util.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_fixed_length_string.h"//ObFixedLengthString
#include "common/ob_timeout_ctx.h"
#include "lib/utility/ob_unify_serialize.h" //OB_SERIALIZE_MEMBER
#include "observer/ob_inner_sql_connection.h"//ObInnerSQLConnection
#include "observer/ob_inner_sql_connection_pool.h"//ObInnerSQLConnectionPool
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" //ObTenantSnapshotUtil
#include "share/rc/ob_tenant_base.h"//MTL_WITH_CHECK_TENANT
#include "storage/tx/ob_trans_define.h"//MonotonicTs
#include "storage/tx/ob_ts_mgr.h"//GET_GTS
#include "storage/tx/ob_trans_service.h"
#include "storage/tablelock/ob_lock_utils.h" // ObLSObjLockUtil
#include "share/ob_max_id_fetcher.h"//ObMaxIdFetcher
#include "share/ob_global_stat_proxy.h"//get gc
#include "logservice/palf/log_define.h"//SCN
#include "share/scn.h"//SCN
#include "share/ls/ob_ls_status_operator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
namespace oceanbase
{
using namespace transaction;
using namespace palf;
using namespace transaction::tablelock;

namespace share
{
#define APPEND_LS_EXIST_STATUS()\
  if (OB_SUCC(ret) && only_existing_ls) {\
    if (OB_FAIL(sql.append_fmt(" and status != '%s' and status != '%s'",\
            ls_status_to_str(OB_LS_DROPPED), ls_status_to_str(OB_LS_CREATE_ABORT)))) {\
      LOG_WARN("failed to append", KR(ret), K(sql));\
    }\
  }
static const char* LS_FLAG_ARRAY[] = { ""/*NORMAL*/, "DUPLICATE", "BLOCK_TABLET_IN" };
const char* LS_STATUS_ARRAY[] =
{
  "CREATING",
  "CREATED",
  "NORMAL",
  "DROPPING",
  "TENANT_DROPPING",
  "WAIT_OFFLINE",
  "CREATE_ABORT",
  "PRE_TENANT_DROPPING",
  "DROPPED"
};

ObLSStatus str_to_ls_status(const ObString &status_str)
{
  ObLSStatus ret_status = OB_LS_EMPTY;
  if (status_str.empty()) {
    ret_status = OB_LS_EMPTY;
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(LS_STATUS_ARRAY); i++) {
      if (0 == status_str.case_compare(LS_STATUS_ARRAY[i])) {
        ret_status = static_cast<ObLSStatus>(i);
        break;
      }
    }
  }
  return ret_status;
}

const char* ls_status_to_str(const ObLSStatus &status)
{
  const char* str = "UNKNOWN";
  STATIC_ASSERT(ARRAYSIZEOF(LS_STATUS_ARRAY) == OB_LS_MAX_STATUS,
                "status string array size mismatch with enum ObLSStatus");


  if (OB_UNLIKELY(OB_LS_EMPTY == status
        || status >= OB_LS_MAX_STATUS)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid log stream status", K(status));
  } else {
    str = LS_STATUS_ARRAY[status];
  }
  return str;
}
int ObLSFlag::assign(const ObLSFlag &ls_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_flag.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_flag));
  } else {
    flag_ = ls_flag.flag_;
  }
  return ret;
}

//maybe empty, DUPLICATE, BLOCK_TABLET_IN, DUPLICATE|BLOCK_TABLET_IN
int ObLSFlag::flag_to_str(ObLSFlagStr &str) const
{
  STATIC_ASSERT(ARRAYSIZEOF(LS_FLAG_ARRAY) == (int64_t)MAX_FLAG,
                "ls flag string array size mismatch enum LSFlag count");
  int ret = OB_SUCCESS;
  str.reset();
  int64_t pos = 0;
  const int64_t len = str.capacity();
  if (is_normal_flag()) {
    if (OB_FAIL(databuff_printf(str.ptr(), len, pos, "%s", LS_FLAG_ARRAY[0]))) {
      LOG_WARN("failed to databuf printf", KR(ret), K(len), K(pos));
    }
  } else {
    bool need_append_split_char = false;
    for (int64_t i = 1; i < ARRAYSIZEOF(LS_FLAG_ARRAY) && OB_SUCC(ret); i++) {
      int64_t flag_bit_to_compare = 1 << (i - 1);
      if (flag_ & flag_bit_to_compare) {
        if (need_append_split_char && OB_FAIL(databuff_printf(str.ptr(), len, pos, "|"))) {
          LOG_WARN("failed to databuff print", KR(ret), K(pos), K(len));
        } else if (OB_FAIL(databuff_printf(str.ptr(), len, pos, "%s", LS_FLAG_ARRAY[i]))) {
          LOG_WARN("failed to databuff print", KR(ret), K(pos), K(len));
        } else {
          need_append_split_char = true;
        }
      }
    }
  }
  return ret;
}

int ObLSFlag::str_to_flag(const common::ObString &sql)
{
  int ret = OB_SUCCESS;
  reset();
  ObLSFlagStr sql_copy;
  if (OB_UNLIKELY(sql.empty())) {
    flag_ = NORMAL_FLAG;
  } else if (sql.length() >= FLAG_STR_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql is larger than expected", KR(ret), K(sql));
  } else if (OB_FAIL(sql_copy.assign(sql))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else {
    char *data_str = NULL;
    char *save_ptr = NULL;
    while (OB_SUCC(ret)) {
      data_str = strtok_r((NULL == data_str ? sql_copy.ptr() : NULL), "|", &save_ptr);
      if (NULL != data_str) {
        bool found = false;
        for (int64_t i = 1; i < ARRAYSIZEOF(LS_FLAG_ARRAY); i++) {
          int64_t flag_bit_to_compare = 1 << (i - 1);
          if (0 == ObString(data_str).case_compare(LS_FLAG_ARRAY[i])) {
            if (flag_ & flag_bit_to_compare) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("can not double ls flag", KR(ret), K(data_str), K(save_ptr));
            } else {
              flag_ |= flag_bit_to_compare;
              found = true;
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected flag string", KR(ret), K(sql));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSFlag, flag_);
////////////ObLSAttr
bool ObLSAttr::is_valid() const
{
  return id_.is_valid()
         && (OB_INVALID_ID != ls_group_id_ || SYS_LS == id_)
         && is_valid_status_in_ls(status_)
         && OB_LS_OP_INVALID_TYPE != operation_type_
         && create_scn_.is_valid() && create_scn_ > SCN::min_scn();
}

int ObLSAttr::init(const ObLSID &id,
           const uint64_t ls_group_id,
           const ObLSFlag &flag,
           const ObLSStatus &status,
           const ObLSOperationType &type,
           const SCN &create_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid()
                  || !is_valid_status_in_ls(status)
                  || !create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(id), K(ls_group_id), K(status),
                                 K(create_scn));
  } else {
    id_ = id;
    ls_group_id_ = ls_group_id;
    flag_ = flag;
    status_ = status;
    operation_type_ = type;
    create_scn_ = create_scn;
  }
  return ret;
}


int ObLSAttr::assign(const ObLSAttr &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
   id_ = other.id_;
    ls_group_id_ = other.ls_group_id_;
    flag_ = other.flag_;
    status_ = other.status_;
    operation_type_ = other.operation_type_;
    create_scn_ = other.create_scn_;
  }
  return ret;
}
void ObLSAttr::reset()
{
  id_.reset();
  ls_group_id_ = OB_INVALID_ID;
  status_ = OB_LS_EMPTY;
  flag_.reset();
  operation_type_ = OB_LS_OP_INVALID_TYPE;
  create_scn_.reset();
}

OB_SERIALIZE_MEMBER(ObLSAttr, id_, ls_group_id_, flag_compatible_, status_, operation_type_, create_scn_, flag_);

//////////////ObLSAttrOperator
bool ObLSAttrOperator::is_valid() const
{
    return OB_INVALID_TENANT_ID != tenant_id_
           && OB_NOT_NULL(proxy_);
}
//1. start trans, and select sys_ls for update except sys_ls,
//   update sys ls or insert sys ls no need select for update
//2. if sys_ls create, no need to start sub trans. sys_ls is insert into __all_ls
//   after sys ls create success during create tenant
//3. do sub_trans and end.

int ObLSAttrOperator::operator_ls_(
    const ObLSAttr &ls_attr, const common::ObSqlString &sql,
    const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_attr.is_valid()
                   || sql.empty()
                   || !working_sw_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation is invalid", KR(ret), K(ls_attr), K(sql), K(working_sw_status));
  } else {
    ObMySQLTransaction trans;
    const bool for_update = true;
    ObLSAttr sys_ls_attr;
    ObLSAttr duplicate_ls_attr;
    bool skip_sub_trans = false;
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(trans.start(proxy_, tenant_id_))) {
      LOG_WARN("failed to start transaction", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(operator_ls_in_trans_(ls_attr, sql,
        working_sw_status, trans))) {
      LOG_WARN("failed to operator ls in trans", KR(ret), K(ls_attr), K(sql));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObLSAttrOperator::operator_ls_in_trans_(
    const ObLSAttr &ls_attr,
    const common::ObSqlString &sql,
    const ObTenantSwitchoverStatus &working_sw_status,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::MODIFY_LS);
  if (OB_UNLIKELY(!ls_attr.is_valid()
                   || sql.empty()
                   || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation is invalid", KR(ret), K(ls_attr), K(sql),
             "is_started", trans.is_started());
  } else {
    const bool for_update = true;
    ObLSAttr sys_ls_attr;
    bool skip_sub_trans = false;
    ObAllTenantInfo tenant_info;
    ObLSAttr duplicate_ls_attr;
    if (ls_attr.get_ls_id().is_sys_ls()) {
      if (OB_LS_NORMAL == ls_attr.get_ls_status()) {
        skip_sub_trans = true;
      }
    /* Lock SYS_LS in all_ls table ensures that only one thread can update ls table,
       and in order to ensure that switchover can change all LS's access mode,
       only update ls table on normal switchover status */
    } else if (OB_FAIL(get_ls_attr(SYS_LS, for_update, trans, sys_ls_attr))) {
      LOG_WARN("failed to load sys ls status", KR(ret));
    } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(tenant_id_, case_to_check))) {
      LOG_WARN("fail to check whether tenant is cloning", KR(ret), K_(tenant_id), K(case_to_check));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, proxy_, false /* for_update */, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K_(tenant_id));
    } else if (working_sw_status != tenant_info.get_switchover_status()) {
      ret = OB_NEED_RETRY;
      LOG_WARN("tenant not in specified switchover status", K_(tenant_id), K(working_sw_status),
               K(tenant_info));
    } else if (OB_LS_OP_CREATE_PRE == ls_attr.get_ls_operation_type()) {
      if (OB_LS_NORMAL != sys_ls_attr.get_ls_status()) {
        //for sys ls, need insert_ls, but ls_status is normal
        //delete ls may in ls is creating, use operation_type
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("ls_status not expected while create ls", KR(ret), K(ls_attr),
          K(sys_ls_attr));
      } else if (ls_attr.get_ls_flag().is_duplicate_ls()
                 && OB_FAIL(get_duplicate_ls_attr(false/*for_update*/, trans, duplicate_ls_attr))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // good, duplicate ls not exist
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get duplicate ls info", KR(ret), K(ls_attr));
        }
      } else if (duplicate_ls_attr.get_ls_flag().is_duplicate_ls()) {
        ret = OB_LS_EXIST;
        LOG_WARN("duplicate ls already exist", KR(ret), K(duplicate_ls_attr), K(ls_attr));
      }
    }
    if (FAILEDx(exec_write(tenant_id_, sql, this, trans))) {
      LOG_WARN("failed to exec write", KR(ret), K(tenant_id_), K(sql));
    } else if (!skip_sub_trans && OB_FAIL(process_sub_trans_(ls_attr, trans))) {
      LOG_WARN("failed to process sub trans", KR(ret), K(ls_attr));
    }
  }
  return ret;
}

int ObLSAttrOperator::insert_ls(
    const ObLSAttr &ls_attr,
    const ObTenantSwitchoverStatus &working_sw_status,
    ObMySQLTransaction *trans)
{
  int ret = OB_SUCCESS;
  ObLSFlagStr flag_str;
  common::ObSqlString sql;
  if (OB_UNLIKELY(!ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation is invalid", KR(ret), K(ls_attr));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else if (OB_FAIL(ls_attr.get_ls_flag().flag_to_str(flag_str))) {
    LOG_WARN("fail to convert flag to string", KR(ret), K(ls_attr));
  } else {
    if (FAILEDx(sql.assign_fmt(
            "insert into %s (ls_id, ls_group_id, status, flag, create_scn) values(%ld, "
            "%ld, '%s', '%s', '%lu')",
            OB_ALL_LS_TNAME, ls_attr.get_ls_id().id(), ls_attr.get_ls_group_id(),
            ls_status_to_str(ls_attr.get_ls_status()), flag_str.ptr(),
            ls_attr.get_create_scn().get_val_for_inner_table_field()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(ls_attr), K(sql));
    } else if (OB_ISNULL(trans)) {
      if (OB_FAIL(operator_ls_(ls_attr, sql, working_sw_status))) {
        LOG_WARN("failed to operator ls", KR(ret), K(ls_attr), K(sql));
      }
    } else {
      if (OB_FAIL(operator_ls_in_trans_(ls_attr, sql, working_sw_status, *trans))) {
        LOG_WARN("failed to operator ls", KR(ret), K(ls_attr), K(sql));
      }
    }
  }
  LOG_INFO("[LS_OPERATOR] insert ls", KR(ret), K(ls_attr), K(sql));
  ALL_LS_EVENT_ADD(tenant_id_, ls_attr.get_ls_id(), "insert_ls", ret, sql);
  return ret;
}

int ObLSAttrOperator::delete_ls(
    const ObLSID &ls_id, const share::ObLSStatus &old_status,
    const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !is_valid_status_in_ls(old_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation is invalid", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else if (ls_id.is_sys_ls() && OB_LS_CREATING == old_status) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys ls can not create abort", KR(ret), K(old_status), K(ls_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, compat_version))) {
      LOG_WARN("get min data_version failed", K(ret), K(tenant_id_));
  } else if (compat_version >= DATA_VERSION_4_2_0_0) {
    share::ObLSStatus new_status = OB_LS_CREATING == old_status ?
                    OB_LS_CREATE_ABORT : OB_LS_DROPPED;
    if (OB_FAIL(update_ls_status(ls_id, old_status, new_status, working_sw_status))) {
      LOG_WARN("failed to update ls status", KR(ret), K(ls_id), K(old_status), K(new_status), K(working_sw_status));
    }
  } else {
    ObSqlString sql;
    ObLSAttr ls_attr;
    if (OB_FAIL(get_ls_attr(ls_id, false/*for_update*/, *proxy_, ls_attr))) {
      LOG_WARN("failed to get ls attr", KR(ret), K(ls_id));
    } else if (ls_attr.get_ls_status() != old_status) {
      ret = OB_EAGAIN;
      LOG_WARN("status not match", KR(ret), K(ls_attr), K(old_status));
    } else if (OB_FAIL(sql.assign_fmt("delete from %s where ls_id = %ld and status = '%s'",
            OB_ALL_LS_TNAME, ls_id.id(), ls_status_to_str(old_status)))) {
      LOG_WARN("failed to assign sql", KR(ret), K(ls_id), K(sql));
    } else {
      ObLSAttr new_ls_attr;
      ObLSOperationType operation_type = OB_LS_CREATING == old_status
                                             ? OB_LS_OP_CREATE_ABORT
                                             : OB_LS_OP_DROP_END;
      if (OB_FAIL(new_ls_attr.init(ls_id, ls_attr.get_ls_group_id(),
                                   ls_attr.get_ls_flag(), ls_attr.get_ls_status(),
                                   operation_type, SCN::base_scn()))) {
        LOG_WARN("failed to init new ls attr", KR(ret), K(ls_id), K(ls_attr), K(operation_type));
      } else if (OB_FAIL(operator_ls_(new_ls_attr, sql, working_sw_status))) {
        LOG_WARN("failed to operator ls", KR(ret), K(new_ls_attr), K(sql));
      }
      LOG_INFO("[LS_OPERATOR] delete ls", KR(ret), K(ls_id), K(old_status));
      ALL_LS_EVENT_ADD(tenant_id_, ls_id, "delete_ls", ret, sql);
    }
  }

  return ret;
}


int ObLSAttrOperator::process_sub_trans_(const ObLSAttr &ls_attr, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation is invalid", KR(ret), K(ls_attr));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else {
    //create sys ls no need to start sub trans
    //other ls operation must execute on tenant's leader of sys_ls
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    if (OB_ISNULL(conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection or trans service is null", KR(ret), KP(conn), K(tenant_id_));
    } else if (OB_UNLIKELY(tenant_id_ != MTL_ID())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not in the right tenant id", KR(ret), K(tenant_id_), "thread tenant id", MTL_ID());
    } else {
      transaction::ObTransService *txs = MTL(transaction::ObTransService*);//no need check nullable
      transaction::ObTxDesc* trans_desc = conn->get_session().get_tx_desc();
      const int64_t expire_ts = conn->get_exec_start_timestamp() + GCONF.internal_sql_execute_timeout;
      const int64_t length = ls_attr.get_serialize_size();
      char *buf = NULL;
      int64_t pos = 0;
      ObArenaAllocator allocator("LSOperator");
      if (OB_ISNULL(trans_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get trans desc", KR(ret));
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buf", KR(ret), K(length));
      } else if (OB_FAIL(ls_attr.serialize(buf, length, pos))) {
        LOG_WARN("failed to serialize set member list arg", KR(ret), K(ls_attr),
                 K(length), K(pos));
      } else if (OB_UNLIKELY(pos > length)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("serialize error", KR(ret), K(pos), K(length));
      } else if (OB_FAIL(txs->register_mds_into_tx(*trans_desc,
                                                   SYS_LS,
                                                   transaction::ObTxDataSourceType::LS_TABLE,
                                                   buf,
                                                   length))) {
        LOG_WARN("failed to register tx data", KR(ret), KPC(trans_desc), K(expire_ts));
      }
      LOG_INFO("process sub trans", KR(ret), K(ls_attr));
    }
  }
  return ret;
}

int ObLSAttrOperator::update_ls_status(const ObLSID &id,
                                        const share::ObLSStatus &old_status,
                                        const share::ObLSStatus &new_status,
                                        const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid()
                  || !is_valid_status_in_ls(old_status)
                  || !is_valid_status_in_ls(new_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(id), K(new_status), K(old_status));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(proxy_, tenant_id_))) {
      LOG_WARN("failed to start transaction", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(update_ls_status_in_trans(id, old_status, new_status, working_sw_status, trans))) {
      LOG_WARN("failed to update ls status in trans", KR(ret), K(id), K(old_status), K(new_status));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObLSAttrOperator::update_ls_status_in_trans(const ObLSID &id,
                                        const share::ObLSStatus &old_status,
                                        const share::ObLSStatus &new_status,
                                        const ObTenantSwitchoverStatus &working_sw_status,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid()
                  || !is_valid_status_in_ls(old_status)
                  || !is_valid_status_in_ls(new_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(id), K(new_status), K(old_status));
  } else {
    common::ObSqlString sql;
    ObLSAttr ls_attr;
    if (OB_FAIL(get_ls_attr(id, false/*for_update*/, trans, ls_attr))) {
      LOG_WARN("failed to get ls attr", KR(ret), K(id));
    } else if (ls_attr.get_ls_status() != old_status) {
      ret = OB_EAGAIN;
      LOG_WARN("status not match", KR(ret), K(ls_attr), K(old_status));
    } else if (OB_FAIL(sql.assign_fmt(
            "UPDATE %s set status = '%s' where ls_id = %ld and status = '%s'",
            OB_ALL_LS_TNAME, ls_status_to_str(new_status),
            id.id(), ls_status_to_str(old_status)))) {
      LOG_WARN("failed to assign sql", KR(ret), K(id), K(new_status), K(old_status), K(sql));
    } else {
      ObLSAttr new_ls_attr;
      ObLSOperationType operation_type = get_ls_operation_by_status(new_status);
      if (OB_FAIL(new_ls_attr.init(id, ls_attr.get_ls_group_id(),
                                   ls_attr.get_ls_flag(), new_status,
                                   operation_type, SCN::base_scn()))) {
        LOG_WARN("failed to init new ls attr", KR(ret), K(id), K(ls_attr),
                 K(operation_type));
      } else if (OB_FAIL(operator_ls_in_trans_(new_ls_attr, sql,
                                      working_sw_status, trans))) {
        LOG_WARN("failed to operator ls", KR(ret), K(new_ls_attr), K(sql));
      }
      LOG_INFO("[LS_OPERATOR] update ls status", KR(ret), K(ls_attr), K(new_ls_attr));
    }
    ALL_LS_EVENT_ADD(tenant_id_, id, "update_ls_status", ret, sql);
  }
  return ret;
}

int ObLSAttrOperator::get_ls_attr(const ObLSID &id,
    const bool for_update, common::ObISQLClient &client,
    ObLSAttr &ls_attr,
    const bool only_existing_ls)
{
  int ret = OB_SUCCESS;
  ls_attr.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(id));
  } else {
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("select * from %s where ls_id = %ld",
               OB_ALL_LS_TNAME, id.id()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else if (for_update && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("failed to append sql", KR(ret), K(sql), K(for_update));
    } else {
      ObLSAttrArray ls_array;
      if (OB_FAIL(exec_read(tenant_id_, sql, client, this, ls_array))) {
        LOG_WARN("failed to get ls array", KR(ret), K(tenant_id_), K(sql));
      } else if (0 == ls_array.count()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("failed to get ls array", KR(ret), K(id), K(sql));
      } else if (OB_UNLIKELY(1 != ls_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("more than one ls is unexpected", KR(ret), K(ls_array), K(sql));
      } else if (only_existing_ls && ls_array.at(0).ls_is_dropped_create_abort()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_INFO("ls is dropped or create abort", KR(ret), K(ls_array));
      } else if (OB_FAIL(ls_attr.assign(ls_array.at(0)))) {
        LOG_WARN("failed to assign ls attr", KR(ret), K(ls_array));
      }
    }
  }

  return ret;
}

int ObLSAttrOperator::get_pre_tenant_dropping_ora_rowscn(share::SCN &pre_tenant_dropping_ora_rowscn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  pre_tenant_dropping_ora_rowscn.set_invalid();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(sql.assign_fmt(
              "SELECT ORA_ROWSCN FROM %s WHERE ls_id = %ld and status = '%s'",
              OB_ALL_LS_TNAME, SYS_LS.id(), ls_status_to_str(OB_LS_PRE_TENANT_DROPPING)))) {
    LOG_WARN("assign sql failed", KR(ret));
  } else if (OB_FAIL(ObShareUtil::get_ora_rowscn(*GCTX.sql_proxy_, tenant_id_, sql, pre_tenant_dropping_ora_rowscn))) {
    LOG_WARN("fail to get target_data_version_ora_rowscn", KR(ret), K(pre_tenant_dropping_ora_rowscn), K(sql));
  }
  return ret;
}

int ObLSAttrOperator::get_duplicate_ls_attr(const bool for_update,
                                            common::ObISQLClient &client,
                                            ObLSAttr &ls_attr,
                                            bool only_existing_ls)
{
  int ret = OB_SUCCESS;
  ls_attr.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else {
    common::ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("select * from %s where flag like \"%%%s%%\"",
               OB_ALL_LS_TNAME, LS_FLAG_ARRAY[ObLSFlag::DUPLICATE_FLAG]))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else if (for_update && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("failed to append sql", KR(ret), K(sql), K(for_update));
    } else {
      ObLSAttrArray ls_array;
      if (OB_FAIL(exec_read(tenant_id_, sql, client, this, ls_array))) {
        LOG_WARN("failed to get ls array", KR(ret), K(tenant_id_), K(sql));
      } else if (0 == ls_array.count()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("failed to ls array", KR(ret), K_(tenant_id));
      } else if (OB_UNLIKELY(1 != ls_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("more than one ls is unexpected", KR(ret), K(ls_array), K(sql));
      } else if (only_existing_ls && ls_array.at(0).ls_is_dropped_create_abort()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_INFO("ls is dropped or create abort", KR(ret), K(ls_array));
      } else if (OB_FAIL(ls_attr.assign(ls_array.at(0)))) {
        LOG_WARN("failed to assign ls attr", KR(ret), K(ls_array));
      }
    }
  }
  return ret;
}

int ObLSAttrOperator::get_all_ls_by_order(
    ObLSAttrIArray &ls_operation_array,
    bool only_existing_ls)
{
  int ret = OB_SUCCESS;
  ls_operation_array.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                   "select * from %s where 1=1", OB_ALL_LS_TNAME))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else {
      APPEND_LS_EXIST_STATUS()
      if (FAILEDx(sql.append(" order by ls_id"))) {
        LOG_WARN("failed to append", KR(ret), K(sql));
      }
    }
    if (FAILEDx(exec_read(tenant_id_, sql, *proxy_, this, ls_operation_array))) {
      LOG_WARN("failed to construct ls attr", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObLSAttrOperator::load_all_ls_and_snapshot(
    const SCN &read_scn, ObLSAttrIArray &ls_array, bool only_existing_ls)
{
  int ret = OB_SUCCESS;
  ls_array.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else {
    ObSqlString sql;
    if (OB_UNLIKELY(!read_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read scn is invalid", KR(ret), K(read_scn));
    } else if (OB_FAIL(sql.assign_fmt("select * from %s as of snapshot %lu where 1=1",
            OB_ALL_LS_TNAME, read_scn.get_val_for_inner_table_field()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql), K(read_scn));
    } else {
      APPEND_LS_EXIST_STATUS()
    }
    if (FAILEDx(exec_read(tenant_id_, sql, *proxy_, this, ls_array))) {
      LOG_WARN("failed to construct ls attr", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObLSAttrOperator::fill_cell(common::sqlclient::ObMySQLResult *result, ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ls_attr.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    ObString status_str;
    int64_t id_value = OB_INVALID_ID;
    uint64_t ls_group_id = OB_INVALID_ID;
    uint64_t create_scn_val = OB_INVALID_SCN_VAL;
    ObString flag_str;
    ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
    SCN create_scn;
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", id_value, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_group_id", ls_group_id, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", status_str);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "flag", flag_str);
    EXTRACT_UINT_FIELD_MYSQL(*result, "create_scn", create_scn_val, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "flag", flag_str);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get result", KR(ret), K(ls_group_id), K(status_str),
               K(id_value), K(flag_str));
    } else if (OB_FAIL(create_scn.convert_for_inner_table_field(create_scn_val))) {
      LOG_WARN("failed to convert create_scn", KR(ret), K(ls_group_id), K(status_str),
               K(id_value), K(create_scn));
    } else if (OB_FAIL(flag.str_to_flag(flag_str))) {
      LOG_WARN("failed to convert flag", KR(ret), K(flag_str));
    } else {
      ObLSID ls_id(id_value);
      ObLSStatus status = str_to_ls_status(status_str);
      ObLSOperationType type = get_ls_operation_by_status(status);
      if (OB_FAIL(ls_attr.init(
              ls_id, ls_group_id, flag,
              status, type, create_scn))) {
        LOG_WARN("failed to init ls operation", KR(ret), K(ls_group_id),
                 K(ls_id), K(status_str), K(status), K(type), K(create_scn));
      }
    }
  }
  return ret;
}


//get ls operation type by ls_status of __all_ls
//can not get OB_LS_OP_CREATE_ABORT OR OB_LS_DROP_END
ObLSOperationType ObLSAttrOperator::get_ls_operation_by_status(const ObLSStatus &ls_status)
{
  ObLSOperationType type_ret = OB_LS_OP_INVALID_TYPE;
  if (OB_UNLIKELY(!is_valid_status_in_ls(ls_status))) {
    type_ret = OB_LS_OP_INVALID_TYPE;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "status is invalid", K(ls_status));
  } else if (OB_LS_CREATING == ls_status) {
    type_ret = OB_LS_OP_CREATE_PRE;
  } else if (OB_LS_NORMAL == ls_status) {
    type_ret = OB_LS_OP_CREATE_END;
  } else if (OB_LS_TENANT_DROPPING == ls_status) {
    type_ret = OB_LS_OP_TENANT_DROP;
  } else if (OB_LS_PRE_TENANT_DROPPING == ls_status) {
    type_ret = OB_LS_OP_TENANT_DROP_PRE;
  } else if (OB_LS_DROPPING == ls_status) {
    type_ret = OB_LS_OP_DROP_PRE;
  } else if (OB_LS_DROPPED == ls_status) {
    type_ret = OB_LS_OP_DROP_END;
  } else if (OB_LS_CREATE_ABORT == ls_status) {
    type_ret = OB_LS_OP_CREATE_ABORT;
  } else {
    type_ret = OB_LS_OP_INVALID_TYPE;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "status is invalid", K(ls_status));
  }
  return type_ret;
}
#undef OPERATOR_LS

int ObLSAttrOperator::get_tenant_gts(const uint64_t &tenant_id, SCN &gts_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else {
    ret = OB_EAGAIN;
    const MonotonicTs stc = MonotonicTs::current_time();
    MonotonicTs unused_ts(0);
    const int64_t start_time = ObTimeUtility::fast_current_time();
    const int64_t TIMEOUT = GCONF.rpc_timeout;
    while (OB_EAGAIN == ret) {
      if (ObTimeUtility::fast_current_time() - start_time > TIMEOUT) {
        ret = OB_TIMEOUT;
        LOG_WARN("stmt is timeout", KR(ret), K(start_time), K(TIMEOUT));
      } else if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, stc, NULL,
                                           gts_scn, unused_ts))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to get gts", KR(ret), K(tenant_id));
        } else {
          // waiting 10ms
          ob_usleep(10L * 1000L);
        }
      }
    }
  }
  LOG_INFO("[LS_OPERATOR] get tenant gts", KR(ret), K(tenant_id), K(gts_scn));
  return ret;
}

int ObLSAttrOperator::alter_ls_group_in_trans(const ObLSAttr &ls_info,
                              const uint64_t new_ls_group_id,
                              common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObLSAttr lock_ls_attr;
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::MODIFY_LS);
  if (OB_UNLIKELY(!ls_info.is_valid()
                  || OB_INVALID_ID == new_ls_group_id
                  || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(ls_info),
             K(new_ls_group_id), "trans_started", trans.is_started());
  } else if (OB_FAIL(get_ls_attr(ls_info.get_ls_id(), true/*for_update*/, trans, lock_ls_attr))) {
    LOG_WARN("failed to lock ls status for clone conflict check", KR(ret), K(ls_info));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(tenant_id_, case_to_check))) {
    LOG_WARN("fail to check whether tenant is cloning", KR(ret), K_(tenant_id), K(case_to_check));
  } else if (OB_FAIL(sql.assign_fmt(
          "UPDATE %s set ls_group_id = %lu where ls_id = %ld and ls_group_id = %lu",
          OB_ALL_LS_TNAME, new_ls_group_id, ls_info.get_ls_id().id(), ls_info.get_ls_group_id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(new_ls_group_id), K(ls_info), K(sql));
  } else if (OB_FAIL(exec_write(tenant_id_, sql, this, trans))) {
    LOG_WARN("failed to exec write", KR(ret), K(tenant_id_), K(sql));
  } else {
    ObLSAttr new_ls_info;
    if (OB_FAIL(new_ls_info.init(ls_info.get_ls_id(), new_ls_group_id, ls_info.get_ls_flag(), ls_info.get_ls_status(),
            OB_LS_OP_ALTER_LS_GROUP, ls_info.get_create_scn()))) {
      LOG_WARN("failed to init new ls info", KR(ret), K(ls_info), K(new_ls_group_id));
    } else if (OB_FAIL(process_sub_trans_(new_ls_info, trans))) {
      LOG_WARN("failed to process sub trans", KR(ret), K(new_ls_info));
    }
  }
  ALL_LS_EVENT_ADD(tenant_id_, ls_info.get_ls_id(), "alter_ls_group", ret, sql);
  return ret;
}

int ObLSAttrOperator::update_ls_flag_in_trans(const ObLSID &id,
    const ObLSFlag &old_flag, const ObLSFlag &new_flag,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObLSFlagStr old_flag_str;
  ObLSFlagStr new_flag_str;
  ObLSAttr lock_ls_attr;
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::MODIFY_LS);

  if (OB_UNLIKELY(!id.is_valid()
                  || !new_flag.is_valid()
                  || !old_flag.is_valid()
                  || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(id), K(old_flag),
             K(new_flag), "trans_started", trans.is_started());
  } else if (OB_FAIL(ObLSObjLockUtil::lock_ls_in_trans(
      trans,
      tenant_id_,
      id,
      EXCLUSIVE))) {
    LOG_WARN("lock ls in trans failed", KR(ret), K_(tenant_id), K(id));
  } else if (OB_FAIL(get_ls_attr(id, true/*for_update*/, trans, lock_ls_attr))) {
    LOG_WARN("failed to lock ls status for clone conflict check", KR(ret), K(id));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(tenant_id_, case_to_check))) {
    LOG_WARN("fail to check whether tenant is cloning", KR(ret), K_(tenant_id), K(case_to_check));
  } else {
    DEBUG_SYNC(AFTER_LOCK_LS_AND_BEFORE_CHANGE_LS_FLAG);
    if (OB_FAIL(new_flag.flag_to_str(new_flag_str))) {
      LOG_WARN("failed to flag to str", KR(ret), K(new_flag));
    } else if (OB_FAIL(old_flag.flag_to_str(old_flag_str))) {
      LOG_WARN("failed to flag to str", KR(ret), K(old_flag));
    } else if (OB_FAIL(sql.assign_fmt(
            "UPDATE %s set flag = '%s' where ls_id = %ld and flag = '%s'",
            OB_ALL_LS_TNAME, new_flag_str.ptr(), id.id(), old_flag_str.ptr()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(id), K(new_flag_str),
              K(old_flag_str), K(sql));
    } else if (OB_FAIL(exec_write(tenant_id_, sql, this, trans))) {
      LOG_WARN("failed to exec write", KR(ret), K(tenant_id_), K(sql));
    }
  }
  ALL_LS_EVENT_ADD(tenant_id_, id, "update_ls_flag", ret, sql);
  return ret;
}

int ObLSAttrOperator::get_random_normal_user_ls(
    ObLSID &ls_id,
    const ObLSFlag &flag)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  ObLSFlagStr flag_str;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id_) || !flag.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K_(tenant_id), K(flag));
  } else if (OB_FAIL(flag.flag_to_str(flag_str))) {
    LOG_WARN("flag to str failed", KR(ret), K(flag), K_(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      int64_t int_ls_id = 0;
      common::sqlclient::ObMySQLResult *res;
      if (OB_FAIL(sql.assign_fmt(
          "select ls_id from %s where ls_id > %ld and status = '%s' and flag = '%s' order by rand() limit 1",
          OB_ALL_LS_TNAME,
          ObLSID::MIN_USER_LS_ID,
          ls_status_to_str(OB_LS_NORMAL),
          flag_str.ptr()))) {
        LOG_WARN("failed to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(proxy_->read(result, tenant_id_, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("normal user ls not found", KR(ret), K_(tenant_id), K(sql));
        } else {
          LOG_WARN("next failed", KR(ret), K_(tenant_id), K(sql));
        }
      } else if (OB_FAIL(res->get_int("ls_id", int_ls_id))) {
        LOG_WARN("get int failed", KR(ret), K_(tenant_id), K(sql));
      } else {
        ls_id = int_ls_id;
      }
    }
  }

  return ret;
}

int ObLSAttrOperator::get_all_ls_by_order(const bool lock_sys_ls,
    ObLSAttrIArray &ls_operation_array,
    bool only_existing_ls)
{
  int ret = OB_SUCCESS;
  ls_operation_array.reset();
  ObMySQLTransaction trans;
  ObLSAttr sys_ls_attr;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else if (OB_FAIL(trans.start(proxy_, tenant_id_))) {
    LOG_WARN("failed to start transaction", KR(ret), K_(tenant_id));
    /* to get accurate LS list need lock SYS_LS */
  } else if (lock_sys_ls && OB_FAIL(get_ls_attr(SYS_LS, true /* for_update */, trans, sys_ls_attr))) {
    LOG_WARN("failed to load sys ls status", KR(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "select * from %s where 1=1", OB_ALL_LS_TNAME))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else {
      APPEND_LS_EXIST_STATUS()
      if (FAILEDx(sql.append(" order by ls_id"))) {
        LOG_WARN("failed to append", KR(ret), K(sql));
      }
    }
    if (FAILEDx(exec_read(tenant_id_, sql, *proxy_, this, ls_operation_array))) {
      LOG_WARN("failed to construct ls attr", KR(ret), K(sql));
    }
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

}
}

