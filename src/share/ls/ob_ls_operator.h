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
ObLSStatus str_to_ls_status(const ObString &status_str);
const char* ls_status_to_str(const ObLSStatus &status);

inline bool ls_is_empty_status(const ObLSStatus &status)
{
  return OB_LS_EMPTY == status;
}

inline bool ls_is_invalid_status(const ObLSStatus &status)
{
  return OB_LS_MAX_STATUS == status
         || ls_is_empty_status(status);
}

inline bool ls_is_creating_status(const ObLSStatus &status)
{
  return OB_LS_CREATING == status;
}

inline bool ls_is_created_status(const ObLSStatus &status)
{
  return OB_LS_CREATED == status;
}

inline bool ls_is_normal_status(const ObLSStatus &status)
{
  return OB_LS_NORMAL == status;
}

inline bool ls_is_tenant_dropping_status(const ObLSStatus &status)
{
  return OB_LS_TENANT_DROPPING == status;
}

inline bool ls_is_dropping_status(const ObLSStatus &status)
{
  return OB_LS_DROPPING == status;
}

inline bool ls_is_wait_offline_status(const ObLSStatus &status)
{
  return OB_LS_WAIT_OFFLINE == status;
}
inline bool ls_is_create_abort_status(const ObLSStatus &status)
{
  return OB_LS_CREATE_ABORT == status;
}

inline bool ls_need_create_abort_status(const ObLSStatus &status)
{
  return OB_LS_CREATING == status || OB_LS_CREATED == status;
}

inline bool ls_is_pre_tenant_dropping_status(const ObLSStatus &status)
{
  return OB_LS_PRE_TENANT_DROPPING == status;
}

inline bool ls_is_dropped_status(const ObLSStatus &status)
{
  return OB_LS_DROPPED == status;
}

inline bool is_valid_status_in_ls(const ObLSStatus &status)
{
  return OB_LS_CREATING == status || OB_LS_NORMAL == status
         || OB_LS_DROPPING == status || OB_LS_TENANT_DROPPING == status
         || OB_LS_PRE_TENANT_DROPPING == status
         || OB_LS_DROPPED == status
         || OB_LS_CREATE_ABORT == status;
}
//maybe empty, DUPLICATE, BLOCK_TABLET_IN, DUPLICATE|BLOCK_TABLET_IN
static const int64_t FLAG_STR_LENGTH = 100;
typedef common::ObFixedLengthString<FLAG_STR_LENGTH> ObLSFlagStr;
class SCN;
//TODO for duplicate ls
enum ObLSFlagForCompatible
{
  OB_LS_FLAG_NORMAL = 0,
};
class ObLSFlag
{
public:
  OB_UNIS_VERSION(1);
public:
  enum LSFlag
  {
    INVALID_TYPE = -1,
    NORMAL_FLAG = 0,
    //If the low 0 bit is 1, it means that this is duplicate ls
    DUPLICATE_FLAG = 1,
    //If the low 1 bit is 1, it means that this is block tablet in
    BLOCK_TABLET_IN_FLAG = 2,
    MAX_FLAG
  };
  ObLSFlag() : flag_(NORMAL_FLAG) {}
  ObLSFlag(const int64_t flag) : flag_(flag) {}
  ~ObLSFlag() {}
  void reset() {flag_ = NORMAL_FLAG;}
  int assign(const ObLSFlag &ls_flag);
  bool operator==(const ObLSFlag &other) const
  {
    return flag_ == other.flag_;
  }
  bool is_valid() const { return flag_ >= 0; }
  void set_block_tablet_in() { flag_ |= BLOCK_TABLET_IN_FLAG; }
  void clear_block_tablet_in() { flag_ &= (~BLOCK_TABLET_IN_FLAG); }
  bool is_normal_flag() const { return NORMAL_FLAG == flag_; }
  bool is_block_tablet_in() const {return flag_ & BLOCK_TABLET_IN_FLAG;}
  void set_duplicate() { flag_ |= DUPLICATE_FLAG; }
  bool is_duplicate_ls() const { return flag_ & DUPLICATE_FLAG; }
  int flag_to_str(ObLSFlagStr &str) const;
  int str_to_flag(const common::ObString &sql);
  int64_t get_flag_value() const { return flag_; }
  TO_STRING_KV(K_(flag), "is_duplicate", is_duplicate_ls(), "is_block_tablet_in", is_block_tablet_in());

private:
  int64_t flag_;
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
  OB_LS_OP_ALTER_LS_GROUP,
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
IS_LS_OPERATION(ALTER_LS_GROUP, alter_ls_group)

struct ObLSAttr
{
  OB_UNIS_VERSION(1);
 public:
  ObLSAttr()
      : id_(),
        ls_group_id_(OB_INVALID_ID),
        flag_compatible_(OB_LS_FLAG_NORMAL),
        flag_(ObLSFlag::NORMAL_FLAG),
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
  bool ls_is_pre_tenant_dropping() const
  {
    return ls_is_pre_tenant_dropping_status(status_);
  }

  bool ls_is_dropped_create_abort() const
  {
    return ls_is_dropped_status(status_)
           || ls_is_create_abort_status(status_);
  }
  bool ls_is_normal() const
  {
    return ls_is_normal_status(status_);
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
  ObLSFlagForCompatible flag_compatible_;//no use, only for compatiable
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

  // get duplicate ls status info
  // @params[in]  for_update, whether to lock line
  // @params[in]  client, sql client to use
  // @params[out] ls_attr, the result
  // @params[in] only_existing_ls : Mark whether to get the LS that has been deleted or create_abort
  int get_duplicate_ls_attr(
      const bool for_update,
      common::ObISQLClient &client,
      ObLSAttr &ls_attr,
      bool only_existing_ls = true);
  /**
   * @description: get ls list from all_ls table
   * @param[out] ls_operation_array ls list
   * @params[in] only_existing_ls : Mark whether to get the LS that has been deleted or create_abort
   * */
  int get_all_ls_by_order(
      ObLSAttrIArray &ls_array,
      bool only_existing_ls = true);
  /**
   * @description:
   *    get ls list from all_ls table,
   *    if want to get accurate LS list, set lock_sys_ls to true to lock SYS LS in __all_ls table
   *    to make sure mutual exclusion with load balancing thread
   * @param[in] lock_sys_ls whether lock SYS LS in __all_ls table
   * @param[out] ls_operation_array ls list
   * @params[in] only_existing_ls : Mark whether to get the LS that has been deleted or create_abort
   * @return return code
   */
  int get_all_ls_by_order(const bool lock_sys_ls,
                          ObLSAttrIArray &ls_operation_array,
                          bool only_existing_ls = true);
  int insert_ls(const ObLSAttr &ls_attr,
                const ObTenantSwitchoverStatus &working_sw_status,
                ObMySQLTransaction *trans = NULL);
  //prevent the concurrency of create and drop ls
  int delete_ls(const ObLSID &id,
                const share::ObLSStatus &old_status,
                const ObTenantSwitchoverStatus &working_sw_status);
  int update_ls_status(const ObLSID &id, const share::ObLSStatus &old_status, const share::ObLSStatus &new_status,
                       const ObTenantSwitchoverStatus &working_sw_status);
  int update_ls_status_in_trans(const ObLSID &id, const share::ObLSStatus &old_status, const share::ObLSStatus &new_status,
                       const ObTenantSwitchoverStatus &working_sw_status,
                       common::ObMySQLTransaction &trans);
  static ObLSOperationType get_ls_operation_by_status(const ObLSStatus &ls_status);
  int get_ls_attr(const ObLSID &id, const bool for_update, common::ObISQLClient &client,
      ObLSAttr &ls_attr, bool only_existing_ls = true);
  int get_pre_tenant_dropping_ora_rowscn(share::SCN &pre_tenant_dropping_ora_rowscn);
  /*
   * description: get all ls with snapshot 
   * @param[in] read_scn:the snapshot of read_version
   * @param[out] ObLSAttrIArray ls_info in __all_ls
   * @params[in] only_existing_ls : Mark whether to get the LS that has been deleted or create_abort
   * */
  int load_all_ls_and_snapshot(const share::SCN &read_scn, ObLSAttrIArray &ls_array, bool only_existing_ls = true);
  static int get_tenant_gts(const uint64_t &tenant_id, SCN &gts_scn);
  int alter_ls_group_in_trans(const ObLSAttr &ls_info,
                              const uint64_t new_ls_group_id,
                              common::ObMySQLTransaction &trans);
  int update_ls_flag_in_trans(const ObLSID &id, const ObLSFlag &old_flag, const ObLSFlag &new_flag,
                    common::ObMySQLTransaction &trans);

  /*
   * description: get random user ls in normal status with specific flag which default is normal
   *
   * @param[out] ls_id: ls_id of user ls
   * @param[in] flag:   ls flag (default normal)
   * */
  int get_random_normal_user_ls(ObLSID &ls_id, const ObLSFlag &flag = ObLSFlag());
private:
  int process_sub_trans_(const ObLSAttr &ls_attr, ObMySQLTransaction &trans);
  int operator_ls_(const ObLSAttr &ls_attr, const common::ObSqlString &sql,
                   const ObTenantSwitchoverStatus &working_sw_status);
  int operator_ls_in_trans_(const ObLSAttr &ls_attr, const common::ObSqlString &sql,
                   const ObTenantSwitchoverStatus &working_sw_status,
                   ObMySQLTransaction &trans);
private:
  uint64_t tenant_id_;
  common::ObMySQLProxy *proxy_;
};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_OPERATOR_H_ */
