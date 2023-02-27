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
#include "common/ob_timeout_ctx.h"
#include "lib/utility/ob_unify_serialize.h" //OB_SERIALIZE_MEMBER
#include "observer/ob_inner_sql_connection.h"//ObInnerSQLConnection
#include "observer/ob_inner_sql_connection_pool.h"//ObInnerSQLConnectionPool
#include "share/rc/ob_tenant_base.h"//MTL_WITH_CHECK_TENANT
#include "storage/tx/ob_trans_define.h"//MonotonicTs
#include "storage/tx/ob_ts_mgr.h"//GET_GTS
#include "storage/tx/ob_trans_service.h"
#include "share/ob_max_id_fetcher.h"//ObMaxIdFetcher
#include "share/ob_global_stat_proxy.h"//get gc
#include "logservice/palf/log_define.h"//SCN
#include "share/scn.h"//SCN

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
using namespace transaction;
using namespace palf;
namespace share
{
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
  flag_ = OB_LS_FLAG_NORMAL;
  operation_type_ = OB_LS_OP_INVALID_TYPE;
  create_scn_.reset();
}

OB_SERIALIZE_MEMBER(ObLSAttr, id_, ls_group_id_, flag_, status_, operation_type_, create_scn_);

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
    const uint64_t target_max_ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_attr.is_valid()
                   || sql.empty()
                   || OB_INVALID_ID == target_max_ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation is invalid", KR(ret), K(ls_attr), K(sql),
        K(target_max_ls_group_id));
  } else {
    ObMySQLTransaction trans;
    const bool for_update = true;
    ObLSAttr sys_ls_attr;
    bool skip_sub_trans = false;
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(trans.start(proxy_, tenant_id_))) {
      LOG_WARN("failed to start transaction", KR(ret), K(tenant_id_));
    } else if (ls_attr.get_ls_id().is_sys_ls()) {
      if (OB_LS_NORMAL == ls_attr.get_ls_status()) {
        skip_sub_trans = true;
      }
    /* Lock SYS_LS in all_ls table ensures that only one thread can update ls table,
       and in order to ensure that switchover can change all LS's access mode,
       only update ls table on normal switchover status */
    } else if (OB_FAIL(get_ls_attr(SYS_LS, for_update, trans, sys_ls_attr))) {
      LOG_WARN("failed to load sys ls status", KR(ret));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, proxy_, false /* for_update */, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K_(tenant_id));
    } else if (OB_LS_OP_CREATE_PRE == ls_attr.get_ls_operation_type()) {
      if (OB_LS_NORMAL != sys_ls_attr.get_ls_status()) {
        //for sys ls, need insert_ls, but ls_status is normal
        //delete ls may in ls is creating, use operation_type 
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("ls_status not expected while create ls", KR(ret), K(ls_attr),
          K(sys_ls_attr));
      } else {
        //check ls create not concurrency
        uint64_t max_ls_id = 0;
        uint64_t max_ls_group_id = 0;
        if (OB_FAIL(ObMaxIdFetcher::fetch_max_id(trans, tenant_id_,
                share::OB_MAX_USED_LS_GROUP_ID_TYPE, max_ls_group_id))) {
          LOG_WARN("failed to fetch max id", KR(ret), K(tenant_id_));
        } else if (max_ls_group_id != target_max_ls_group_id) {
          ret = OB_NEED_RETRY;
          LOG_WARN("max ls group id not equal, ls may be concurrency created", KR(ret),
              K(target_max_ls_group_id), K(max_ls_group_id));
        } else if (OB_FAIL(ObMaxIdFetcher::fetch_max_id(trans, tenant_id_,
                share::OB_MAX_USED_LS_ID_TYPE, max_ls_id))) {
          LOG_WARN("failed to fetch max id", KR(ret), K(tenant_id_));
        } else if (max_ls_id != ls_attr.get_ls_id().id()) {
          ret = OB_NEED_RETRY;
          LOG_WARN("max ls id not equal, ls may be concurrency created", KR(ret),
              K(max_ls_id), K(ls_attr));
        }
      }
    }
    if (FAILEDx(exec_write(tenant_id_, sql, this, trans))) {
      LOG_WARN("failed to exec write", KR(ret), K(tenant_id_), K(sql));
    } else if (!skip_sub_trans && OB_FAIL(process_sub_trans_(ls_attr, trans))) {
      LOG_WARN("failed to process sub trans", KR(ret), K(ls_attr));
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

int ObLSAttrOperator::insert_ls(
    const ObLSAttr &ls_attr,
    const uint64_t max_ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_attr.is_valid() || OB_INVALID_ID == max_ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operation is invalid", KR(ret), K(ls_attr), K(max_ls_group_id));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else {
    ObSqlString sql;
    if (FAILEDx(sql.assign_fmt(
            "insert into %s (ls_id, ls_group_id, status, flag, create_scn) values(%ld, "
            "%ld, '%s', '%s', '%lu')",
            OB_ALL_LS_TNAME, ls_attr.get_ls_id().id(), ls_attr.get_ls_group_id(),
            ObLSStatusOperator::ls_status_to_str(ls_attr.get_ls_status()), "",
            ls_attr.get_create_scn().get_val_for_inner_table_field()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(ls_attr), K(sql));
    } else if (OB_FAIL(operator_ls_(ls_attr, sql, max_ls_group_id))) {
      LOG_WARN("failed to operator ls", KR(ret), K(ls_attr), K(sql));
    }
  }
  LOG_INFO("[LS_OPERATOR] insert ls", KR(ret), K(ls_attr));
  return ret;
}

int ObLSAttrOperator::delete_ls(
    const ObLSID &ls_id, const share::ObLSStatus &old_status)
{
  int ret = OB_SUCCESS;
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
  } else {
    ObSqlString sql;
    ObLSAttr ls_attr;
    if (OB_FAIL(get_ls_attr(ls_id, false/*for_update*/, *proxy_, ls_attr))) {
      LOG_WARN("failed to get ls attr", KR(ret), K(ls_id));
    } else if (ls_attr.get_ls_status() != old_status) {
      ret = OB_EAGAIN;
      LOG_WARN("status not match", KR(ret), K(ls_attr), K(old_status));
    } else if (OB_FAIL(sql.assign_fmt("delete from %s where ls_id = %ld and status = '%s'",
            OB_ALL_LS_TNAME, ls_id.id(), ObLSStatusOperator::ls_status_to_str(old_status)))) {
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
      } else if (OB_FAIL(operator_ls_(new_ls_attr, sql, new_ls_attr.get_ls_group_id()))) {
        LOG_WARN("failed to operator ls", KR(ret), K(new_ls_attr), K(sql));
      }
      LOG_INFO("[LS_OPERATOR] delete ls", KR(ret), K(ls_id), K(old_status));
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
      } else if (OB_FAIL(txs->register_mds_into_tx(
                     *trans_desc, SYS_LS,
                     transaction::ObTxDataSourceType::LS_TABLE, buf, length))) {
        LOG_WARN("failed to register tx data", KR(ret), KPC(trans_desc), K(expire_ts));
      }
      LOG_INFO("process sub trans", KR(ret), K(ls_attr));
    }
  }
  return ret;
}

int ObLSAttrOperator::update_ls_status(const ObLSID &id,
                                        const share::ObLSStatus &old_status,
                                        const share::ObLSStatus &new_status)
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
    if (OB_FAIL(get_ls_attr(id, false/*for_update*/, *proxy_, ls_attr))) {
      LOG_WARN("failed to get ls attr", KR(ret), K(id));
    } else if (ls_attr.get_ls_status() != old_status) {
      ret = OB_EAGAIN;
      LOG_WARN("status not match", KR(ret), K(ls_attr), K(old_status));
    } else if (OB_FAIL(sql.assign_fmt(
            "UPDATE %s set status = '%s' where ls_id = %ld and status = '%s'",
            OB_ALL_LS_TNAME, ObLSStatusOperator::ls_status_to_str(new_status),
            id.id(), ObLSStatusOperator::ls_status_to_str(old_status)))) {
      LOG_WARN("failed to assign sql", KR(ret), K(id), K(new_status), K(old_status), K(sql));
    } else {
      ObLSAttr new_ls_attr;
      ObLSOperationType operation_type = get_ls_operation_by_status(new_status);
      if (OB_FAIL(new_ls_attr.init(id, ls_attr.get_ls_group_id(),
                                   ls_attr.get_ls_flag(), new_status,
                                   operation_type, SCN::base_scn()))) {
        LOG_WARN("failed to init new ls attr", KR(ret), K(id), K(ls_attr),
                 K(operation_type));
      } else if (OB_FAIL(operator_ls_(new_ls_attr, sql, new_ls_attr.get_ls_group_id()))) {
        LOG_WARN("failed to operator ls", KR(ret), K(new_ls_attr), K(sql));
      }
      LOG_INFO("[LS_OPERATOR] update ls status", KR(ret), K(ls_attr), K(new_ls_attr));
    }
  }
  return ret;
}

int ObLSAttrOperator::get_ls_attr(const ObLSID &id, const bool for_update, common::ObISQLClient &client, ObLSAttr &ls_attr)
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
        LOG_WARN("failed to ls array", KR(ret), K(id));
      } else if (OB_UNLIKELY(1 != ls_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("more than one ls is unexpected", KR(ret), K(ls_array), K(sql));
      } else if (OB_FAIL(ls_attr.assign(ls_array.at(0)))) {
        LOG_WARN("failed to assign ls attr", KR(ret), K(ls_array));
      }
    }
  }

  return ret;
}

int ObLSAttrOperator::get_all_ls_by_order(
    ObLSAttrIArray &ls_operation_array)
{
  int ret = OB_SUCCESS;
  ls_operation_array.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                   "select * from %s order by ls_id",
                   OB_ALL_LS_TNAME))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else if (OB_FAIL(exec_read(tenant_id_, sql, *proxy_, this, ls_operation_array))) {
      LOG_WARN("failed to construct ls attr", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObLSAttrOperator::load_all_ls_and_snapshot(
    SCN &read_scn, ObLSAttrIArray &ls_array)
{
  int ret = OB_SUCCESS;
  ls_array.reset();
  read_scn.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), "operation", *this);
  } else {
    if (OB_FAIL(get_tenant_gts(tenant_id_, read_scn))) {
      LOG_WARN("failed to get tenant gts", KR(ret), K(tenant_id_));
    } else {
      ObSqlString sql;
      if (OB_UNLIKELY(!read_scn.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read scn is invalid", KR(ret), K(read_scn));
      } else if (OB_FAIL(sql.assign_fmt("select * from %s as of snapshot %lu",
              OB_ALL_LS_TNAME, read_scn.get_val_for_inner_table_field()))) {
        LOG_WARN("failed to assign sql", KR(ret), K(sql), K(read_scn));
      } else if (OB_FAIL(exec_read(tenant_id_, sql, *proxy_, this, ls_array))) {
        LOG_WARN("failed to construct ls attr", KR(ret), K(sql));
      }
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
    ObLSFlag flag;  // TODO no used
    SCN create_scn;
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", id_value, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_group_id", ls_group_id, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", status_str);
    EXTRACT_UINT_FIELD_MYSQL(*result, "create_scn", create_scn_val, uint64_t);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get result", KR(ret), K(ls_group_id), K(status_str),
               K(id_value));
    } else if (OB_FAIL(create_scn.convert_for_inner_table_field(create_scn_val))) {
      LOG_WARN("failed to convert create_scn", KR(ret), K(ls_group_id), K(status_str),
               K(id_value), K(create_scn));
    } else {
      ObLSID ls_id(id_value);
      ObLSStatus status = ObLSStatusOperator::str_to_ls_status(status_str);
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

}
}

