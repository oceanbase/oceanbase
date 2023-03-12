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

#ifndef OCEANBASE_SHARE_OB_LS_OPERATOR_H_
#define OCEANBASE_SHARE_OB_LS_OPERATOR_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "share/ls/ob_ls_status_operator.h" //ObLSStatus
#include "share/ls/ob_ls_i_life_manager.h"//ObLSTemplateOperator
#include "logservice/palf/log_define.h"//SCN
#include "share/scn.h"//SCN


namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObString;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class SCN;
//TODO for duplicate ls
enum ObLSFlag
{
  OB_LS_FLAG_NORMAL = 0,
};
enum ObLSOperationType
{
  OB_LS_OP_INVALID_TYPE = -1,
  OB_LS_OP_CREATE_PRE,
  OB_LS_OP_CREATE_END,
  OB_LS_OP_CREATE_ABORT,
  OB_LS_OP_DROP_PRE,
  OB_LS_OP_TENANT_DROP_PRE,
  OB_LS_OP_DROP_END,
  OB_LS_OP_TENANT_DROP,
};
#define IS_LS_OPERATION(OPERATION_TYPE, OPERATION) \
static bool is_ls_##OPERATION##_op(const ObLSOperationType type) { \
    return OB_LS_OP_##OPERATION_TYPE == type;\
  };

IS_LS_OPERATION(CREATE_PRE, create_pre)
IS_LS_OPERATION(CREATE_END, create_end)
IS_LS_OPERATION(DROP_PRE, drop_pre)
IS_LS_OPERATION(TENANT_DROP_PRE, tenant_drop_pre)
IS_LS_OPERATION(DROP_END, drop_end)
IS_LS_OPERATION(CREATE_ABORT, create_abort)
IS_LS_OPERATION(TENANT_DROP, tenant_drop)

struct ObLSAttr
{
  OB_UNIS_VERSION(1);
 public:
  ObLSAttr()
      : id_(),
        ls_group_id_(OB_INVALID_ID),
        flag_(OB_LS_FLAG_NORMAL),
        status_(OB_LS_EMPTY),
        operation_type_(OB_LS_OP_INVALID_TYPE)
  { create_scn_.set_min();}
  virtual ~ObLSAttr() {}
  bool is_valid() const;
  int init(const ObLSID &id,
           const uint64_t ls_group_id,
           const ObLSFlag &flag,
           const ObLSStatus &status,
           const ObLSOperationType &type,
           const SCN &create_scn);
  void reset();
  int assign(const ObLSAttr &other);
  bool ls_is_creating() const
  {
    return ls_is_creating_status(status_);
  }
  bool ls_is_dropping() const
  {
    return ls_is_dropping_status(status_);
  }
  bool ls_is_tenant_dropping() const
  {
    return ls_is_tenant_dropping_status(status_);
  }
  bool ls_is_normal() const
  {
    return ls_is_normal_status(status_);
  }
  ObLSOperationType get_ls_operatin_type() const
  {
    return operation_type_;
  }
  ObLSID get_ls_id() const 
  {
    return id_;
  }
  uint64_t get_ls_group_id() const
  {
    return ls_group_id_;
  }

  ObLSStatus get_ls_status() const
  {
    return status_;
  }
  
  ObLSOperationType get_ls_operation_type() const
  {
    return operation_type_;
  }
  
  ObLSFlag get_ls_flag() const
  {
    return flag_;
  }
  SCN get_create_scn() const
  {
    return create_scn_;
  }

  TO_STRING_KV(K_(id), K_(ls_group_id), K_(flag), K_(status), K(operation_type_),
               K_(create_scn));
private:
  ObLSID id_;
  uint64_t ls_group_id_;
  ObLSFlag flag_;
  ObLSStatus status_;
  ObLSOperationType operation_type_;
  SCN create_scn_;
};

typedef common::ObArray<ObLSAttr> ObLSAttrArray;
typedef common::ObIArray<ObLSAttr> ObLSAttrIArray;

/*
 * description: the operation of __all_ls*/
class ObLSAttrOperator : public ObLSTemplateOperator
{
public:
  ObLSAttrOperator(const uint64_t tenant_id,
                   common::ObMySQLProxy *proxy) :
                      tenant_id_(tenant_id), proxy_(proxy) {};
  virtual ~ObLSAttrOperator(){}

  TO_STRING_KV(K_(tenant_id), KP_(proxy));

  uint64_t get_exec_tenant_id(const uint64_t tenant_id)
  {
    return tenant_id;
  }
  int fill_cell(common::sqlclient::ObMySQLResult *result, ObLSAttr &ls_attr);
public:
  bool is_valid() const;
  int get_all_ls_by_order(
      ObLSAttrIArray &ls_array);
  /**
   * @description:
   *    get ls list from all_ls table,
   *    if want to get accurate LS list, set lock_sys_ls to true to lock SYS LS in __all_ls table
   *    to make sure mutual exclusion with load balancing thread
   * @param[in] lock_sys_ls whether lock SYS LS in __all_ls table
   * @param[out] ls_operation_array ls list
   * @return return code
   */
  int get_all_ls_by_order(const bool lock_sys_ls, ObLSAttrIArray &ls_operation_array);
  int insert_ls(const ObLSAttr &ls_attr, const uint64_t max_ls_group_id,
                const ObTenantSwitchoverStatus &working_sw_status);
  //prevent the concurrency of create and drop ls
  int delete_ls(const ObLSID &id,
                const share::ObLSStatus &old_status,
                const ObTenantSwitchoverStatus &working_sw_status);
  int update_ls_status(const ObLSID &id, const share::ObLSStatus &old_status, const share::ObLSStatus &new_status,
                       const ObTenantSwitchoverStatus &working_sw_status);
  static ObLSOperationType get_ls_operation_by_status(const ObLSStatus &ls_status);
  int get_ls_attr(const ObLSID &id, const bool for_update, common::ObISQLClient &client, ObLSAttr &ls_attr);
  /*
   * description: get all ls with snapshot 
   * @param[out] read_scn:the snapshot of read_version
   * @param[out] ObLSAttrIArray ls_info in __all_ls
   * */
  int load_all_ls_and_snapshot(share::SCN &read_scn, ObLSAttrIArray &ls_array);
  static int get_tenant_gts(const uint64_t &tenant_id, SCN &gts_scn);

private:
  int process_sub_trans_(const ObLSAttr &ls_attr, ObMySQLTransaction &trans);
  int operator_ls_(const ObLSAttr &ls_attr, const common::ObSqlString &sql, const uint64_t max_ls_group_id,
                   const ObTenantSwitchoverStatus &working_sw_status);
private:
  uint64_t tenant_id_;
  common::ObMySQLProxy *proxy_;
};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_OPERATOR_H_ */
