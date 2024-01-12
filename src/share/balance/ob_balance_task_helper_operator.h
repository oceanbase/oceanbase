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

#ifndef OCEANBASE_SHARE_OB_LS_BALANCE_TASK_HELPER_OPERATOR_H_
#define OCEANBASE_SHARE_OB_LS_BALANCE_TASK_HELPER_OPERATOR_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ob_common_id.h"// ObCommonID
#include "share/scn.h"//SCN
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "lib/string/ob_sql_string.h"//ObSqlString

namespace oceanbase
{

namespace common
{
class ObISQLClient;
class ObString;
namespace sqlclient
{
class ObMySQLResult;
}
}

namespace share
{
class ObDMLSqlSplicer;
class ObBalanceTaskHelperOp
{
public:
  OB_UNIS_VERSION(1);
public:
  static const int64_t LS_BALANCE_TASK_OP_INVALID = -1;
  static const int64_t LS_BALANCE_TASK_OP_ALTER = 0;
  static const int64_t LS_BALANCE_TASK_OP_TRANSFER_BEGIN = 1;
  static const int64_t LS_BALANCE_TASK_OP_TRANSFER_END = 2;
  static const int64_t LS_BALANCE_TASK_OP_MAX = 3;
  ObBalanceTaskHelperOp(const int64_t value = LS_BALANCE_TASK_OP_INVALID) : val_(value) {}
  ObBalanceTaskHelperOp(const ObString &str);
  ~ObBalanceTaskHelperOp() {reset(); }

public:
  void reset() { val_ = LS_BALANCE_TASK_OP_INVALID; }
  bool is_valid() const { return val_ > LS_BALANCE_TASK_OP_INVALID && val_ < LS_BALANCE_TASK_OP_MAX; }
  const char* to_str() const;

  // assignment
  ObBalanceTaskHelperOp &operator=(const int64_t value) { val_ = value; return *this; }

  // compare operator
  bool operator == (const ObBalanceTaskHelperOp &other) const { return val_ == other.val_; }
  bool operator != (const ObBalanceTaskHelperOp &other) const { return val_ != other.val_; }

  // ObBalanceTaskHelperOp attribute interface
  bool is_ls_alter() const { return LS_BALANCE_TASK_OP_ALTER == val_; }
  bool is_transfer_begin() const { return LS_BALANCE_TASK_OP_TRANSFER_BEGIN == val_; }
  bool is_transfer_end() const { return LS_BALANCE_TASK_OP_TRANSFER_END == val_; }

  TO_STRING_KV(K_(val), "ls_balance_task_op", to_str());
private:
  int64_t val_;
};

struct ObBalanceTaskHelperMeta
{
public:
  OB_UNIS_VERSION(1);
public:
  ObBalanceTaskHelperMeta() { reset(); }
  ~ObBalanceTaskHelperMeta() {}
  void reset();
  int init(const ObBalanceTaskHelperOp &task_op,
           const share::ObLSID &src_ls,
           const share::ObLSID &dest_ls,
           const uint64_t ls_group_id);
  bool is_valid() const;
  int assign(const ObBalanceTaskHelperMeta &other);
  TO_STRING_KV(K_(task_op), K_(src_ls),
               K_(dest_ls), K_(ls_group_id));
#define Property_declare_var(variable_type, variable_name) \
 private:                                                  \
  variable_type variable_name##_;                          \
                                                           \
 public:                                                   \
  variable_type get_##variable_name() const { return variable_name##_; }

  Property_declare_var(ObBalanceTaskHelperOp, task_op)
  Property_declare_var(share::ObLSID, src_ls)
  Property_declare_var(share::ObLSID, dest_ls)
  Property_declare_var(uint64_t, ls_group_id)
#undef Property_declare_var
};

struct ObBalanceTaskHelper
{
public:
  ObBalanceTaskHelper() { reset(); }
  ~ObBalanceTaskHelper() {}
  void reset();
  int init(const share::SCN &operation_scn,
           const uint64_t tenant_id,
           const ObBalanceTaskHelperMeta &balance_task_helper_meta);
  int init(const uint64_t tenant_id,
           const share::SCN &operation_scn,
           const ObBalanceTaskHelperOp &task_op,
           const share::ObLSID &src_ls,
           const share::ObLSID &dest_ls,
           const uint64_t ls_group_id);
  bool is_valid() const;
  int assign(const ObBalanceTaskHelper &other);
  TO_STRING_KV(K_(balance_task_helper_meta), K_(operation_scn));
  share::SCN get_operation_scn() const
  {
    return operation_scn_;
  }
  ObBalanceTaskHelperOp get_task_op() const
  {
    return balance_task_helper_meta_.get_task_op();
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_src_ls() const
  {
    return balance_task_helper_meta_.get_src_ls();
  }
  share::ObLSID get_dest_ls() const
  {
    return balance_task_helper_meta_.get_dest_ls();
  }
  uint64_t get_ls_group_id() const
  {
    return balance_task_helper_meta_.get_ls_group_id();
  }
private:
  share::SCN operation_scn_;
  uint64_t tenant_id_;
  ObBalanceTaskHelperMeta balance_task_helper_meta_;
};

class ObBalanceTaskHelperTableOperator
{
public:
  /**
   * @description: insert new ls_task to __all_ls_balance_task_operator
   * @param[in] ls_balance_task : a valid ls balance task
   * @param[in] client: sql client or trans
   * @return OB_SUCCESS if success, otherwise failed
   */
  static int insert_ls_balance_task(const ObBalanceTaskHelper &ls_balance_task,
                     ObISQLClient &client);
  /**
   * @description: get task which operation scn less than max_scn
   * @param[in] tenant_id : user_tenant_id
   * @param[in] client : sql client or trans
   * @param[in] max_operation_scn : max_scn
   * @param[out] ls_balance_tasks : ls_balance_task's operation_scn less than max_scn
   * @return :
   *  OB_SUCCESS : get valid ls_balance_task array
   *  OB_ENTRY_NOT_EXIST : empty
   *  OTHER : fail
   */
  static int load_tasks_order_by_scn(const uint64_t tenant_id,
                      ObISQLClient &client,
                      const share::SCN &max_operation_scn,
                      ObIArray<ObBalanceTaskHelper> &ls_balance_task);
  /**
   * @description: remove task of operation_scn
   * @param[in] tenant_id : user_tenant_id
   * @param[in] op_scn : task's operation_scn
   * @param[in] client : sql client or trans
   * @return:
   *   OB_SUCCESS : remove_success;
   *   OB_ENTRY_NOT_EXIST : the task not exist
   *   OTHER : fail
   */
  static int remove_task(const uint64_t tenant_id, const share::SCN &op_scn,
                        ObISQLClient &client);
  static int fill_dml_spliter(share::ObDMLSqlSplicer &dml,
                              const ObBalanceTaskHelper &ls_balance_task);
 /**
   * @description: get transfer end task while has transfer begin
   * @param[in] tenant_id : user_tenant_id
   * @param[in] op_scn : transfer begin operation scn, transfer end 's scn is larger than this
   * @param[in] src_ls : transfer_begin's source ls
   * @param[in] desc_ls : transfer_begin's destination ls
   * @param[in] client : sql client or trans
   * @param[out] ls_balance_task : ls_balance_task of min operation_scn
   * @return :
   *  OB_SUCCESS : get a valid ls_balance_task
   *  OB_ENTRY_NOT_EXIST : empty
   *  OTHER : fail
   */
  static int try_find_transfer_end(const uint64_t tenant_id,
                                   const share::SCN &op_scn,
                                   const ObLSID &src_ls,
                                   const ObLSID &dest_ls,
                                   ObISQLClient &client,
                                   ObBalanceTaskHelper &ls_balance_task);
private:
  static int exec_get_rows_(const common::ObSqlString &sql,
                             const uint64_t tenant_id,
                             ObISQLClient &client,
                             ObIArray<ObBalanceTaskHelper> &ls_balance_tasks);
  static int exec_get_single_row_(const common::ObSqlString &sql,
      const uint64_t tenant_id, ObISQLClient &client,
      ObBalanceTaskHelper &ls_balance_task);
};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_BALANCE_TASK_HELPER_OPERATOR_H_ */
