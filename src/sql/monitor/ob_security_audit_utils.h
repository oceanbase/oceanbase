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

#ifndef OCEANBASE_SQL_SECURITY_AUDIT_UTILS_H
#define OCEANBASE_SQL_SECURITY_AUDIT_UTILS_H

#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "sql/monitor/ob_audit_action_type.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
#ifndef OB_BUILD_AUDIT_SECURITY
namespace sql
{
enum class ObAuditTrailType{
  INVALID = 0,
  NONE,
};

ObAuditTrailType get_audit_trail_type_from_string(const common::ObString &string);

struct ObAuditUnit
{
  ObAuditUnit() {}
  ~ObAuditUnit() {}
  TO_STRING_KV(K(""));
};

class ObSecurityAuditUtils final
{
public:
  static int check_allow_audit(ObSQLSessionInfo &session, ObAuditTrailType &at_type);
  static int get_audit_file_name(char *buf,
                                 const int64_t buf_len,
                                 int64_t &pos);
};
}
#else
namespace share
{
namespace schema
{
enum ObSAuditOperationType : uint64_t;
enum ObSAuditType : uint64_t;
enum class ObObjectType;
struct ObObjectStruct;
class ObSchemaGetterGuard;
}
}
namespace sql
{
class ObStmt;
class ObPhysicalPlan;
class ObSQLSessionInfo;
class ObSecurityAuditData;
class ObResultSet;

enum class ObAuditTrailType{
  INVALID = 0,
  NONE,
  OS,
  DB,
  DB_EXTENDED,
};

ObAuditTrailType get_audit_trail_type_from_string(const common::ObString &string);

// 定义可以被审计的一个元素
// 记录一个对象和对其进行的操作，如 (SELECT, TABLE T1)
// 或者一个没有对应对象的操作，如 (LOGIN, NULL)
struct ObAuditUnit
{
  ObAuditUnit() : stmt_type_(sql::stmt::T_NONE),
                  obj_type_(share::schema::ObObjectType::INVALID),
                  obj_id_(common::OB_INVALID_ID),
                  obj_idx_(-1),
                  stmt_operation_type_(share::schema::AUDIT_OP_INVALID),
                  obj_operation_type_(share::schema::AUDIT_OP_INVALID) {}
  ObAuditUnit(
    sql::stmt::StmtType stmt_type,
    share::schema::ObObjectType obj_type,
    uint64_t obj_id,
    int64_t obj_idx,
    share::schema::ObSAuditOperationType stmt_operation_type,
    share::schema::ObSAuditOperationType obj_operation_type)
    : stmt_type_(stmt_type), obj_type_(obj_type), obj_id_(obj_id), obj_idx_(obj_idx),
      stmt_operation_type_(stmt_operation_type), obj_operation_type_(obj_operation_type) {}

  ~ObAuditUnit() {};

  sql::stmt::StmtType stmt_type_;
  share::schema::ObObjectType obj_type_;
  uint64_t obj_id_;
  int64_t obj_idx_;
  share::schema::ObSAuditOperationType stmt_operation_type_;
  share::schema::ObSAuditOperationType obj_operation_type_;
  TO_STRING_KV(K_(stmt_type), K_(obj_type), K_(obj_id), K_(obj_idx), K_(stmt_operation_type),
               K_(obj_operation_type));
};

class ObSecurityAuditUtils final
{
public:
  struct AuditDataParam {
    AuditDataParam() : audit_id_(0), audit_type_(0),
                       operation_type_(0), action_id_(0),
                       db_id_(0), action_sql_(), comment_text_(), return_code_(0),
                       sys_privilege_(0), grant_option_(0),
                       audit_option_(share::schema::AUDIT_OP_INVALID), grantee_(),
                       stmt_type_(stmt::StmtType::T_NONE), stmt_(NULL)
    {
      MEMSET(client_ip_buf_, 0, sizeof(client_ip_buf_));
      MEMSET(server_ip_buf_, 0, sizeof(server_ip_buf_));
      MEMSET(trans_id_buf_, 0, sizeof(trans_id_buf_));
      MEMSET(auth_privileges_buf_, 0, sizeof(auth_privileges_buf_));
    }
    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV(K_(client_ip_buf),
           K_(server_ip_buf),
           K_(trans_id_buf),
           K_(auth_privileges_buf),
           K_(audit_id),
           K_(audit_type),
           K_(operation_type),
           K_(db_id),
           K_(action_sql),
           K_(comment_text),
           K_(return_code),
           K_(sys_privilege),
           K_(grant_option),
           K_(audit_option),
           K_(grantee),
           K_(stmt_type),
           KP_(stmt));
      J_OBJ_END();
      return pos;
    }

    char client_ip_buf_[common::OB_IP_STR_BUFF];
    char server_ip_buf_[common::OB_IP_STR_BUFF];
    char trans_id_buf_[common::OB_MAX_TRANS_ID_BUFFER_SIZE];
    char auth_privileges_buf_[common::MAX_COLUMN_PRIVILEGE_LENGTH];
    uint64_t audit_id_;
    uint64_t audit_type_;
    uint64_t operation_type_;
    int64_t action_id_;
    uint64_t db_id_;
    common::ObString action_sql_;
    common::ObString comment_text_;
    int return_code_;
    uint64_t sys_privilege_;
    uint64_t grant_option_;
    share::schema::ObSAuditOperationType audit_option_;
    common::ObString grantee_;
    stmt::StmtType stmt_type_;
    const sql::ObStmt *stmt_;
  };

  struct AuditActionTypeTransform
  {
  public:
    AuditActionTypeTransform()
      : audit_action_type_from_stmt_()
    {
      for (int j = 0; j < ARRAYSIZEOF(audit_action_type_from_stmt_); j++) {
        audit_action_type_from_stmt_[j] = audit::ACTION_TYPE_UNKNOWN;
      }
      #define OB_STMT_TYPE_DEF(stmt_type, priv_check_func, id, action_type) audit_action_type_from_stmt_[stmt::stmt_type] = audit::action_type;
      #include "sql/resolver/ob_stmt_type.h"
      #undef OB_STMT_TYPE_DEF
    }
    audit::AuditActionType audit_action_type_from_stmt_[stmt::T_MAX + 1];
  };

  typedef int (*ObCheckAllowAuditFunc) (ObSQLSessionInfo &session,
                                        share::schema::ObSchemaGetterGuard *schema_guard,
                                        const ObAuditUnit &audit_unit,
                                        AuditDataParam &filled_param,
                                        bool &is_allow_audit);

public:
  ObSecurityAuditUtils() {}

  static int handle_security_audit(ObSQLSessionInfo &session,
                                   const stmt::StmtType stmt_type,
                                   const common::ObString &action_sql,
                                   const common::ObString &comment_text,
                                   const int return_code);
  static int handle_security_audit(ObResultSet &result,
                                   share::schema::ObSchemaGetterGuard *schema_guard,
                                   const sql::ObStmt *stmt,
                                   const common::ObString &comment_text,
                                                                 const int return_code);
  static int check_allow_audit(ObSQLSessionInfo &session, bool &allow_audit);
  static int check_allow_audit(ObSQLSessionInfo &session, ObAuditTrailType &at_type);
  static int do_security_audit_record(ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      const sql::ObStmt *stmt,
                                      ObResultSet *result,
                                      AuditDataParam &filled_param,
                                      const ObAuditTrailType at_type);
  static int gen_audit_records(ObSQLSessionInfo &session,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               AuditDataParam &param,
                               const sql::ObStmt *stmt,
                               const ObAuditUnit &audit_unit,
                               const ObAuditTrailType at_type);
  static int gen_single_audit_record(ObSQLSessionInfo &session,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     const AuditDataParam &param,
                                     const sql::ObStmt *stmt,
                                     const ObAuditUnit &audit_unit,
                                     const ObAuditTrailType at_type);
  static int fill_audit_data(ObSQLSessionInfo &session,
                             const AuditDataParam &param,
                             const sql::ObStmt *stmt,
                             ObSecurityAuditData &security_audit_data);
  static int fill_audit_obj_name(const AuditDataParam &param,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 const sql::ObStmt *stmt,
                                 const ObAuditUnit &audit_unit,
                                 ObSecurityAuditData &saudit_data);
  static int fill_audit_obj_name_from_stmt(const AuditDataParam &param,
                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                           const sql::ObStmt *stmt,
                                           const ObAuditUnit &audit_unit,
                                           ObSecurityAuditData &saudit_data);
  static int record_audit_data(const ObAuditTrailType at_type,
                               ObSecurityAuditData &security_audit_data);
  static int record_audit_data_into_table(ObSecurityAuditData &security_audit_data,
                                          const bool need_record_sql);

  static int get_action_sql(const stmt::StmtType stmt_type, const sql::ObStmt *stmt,
      ObSQLSessionInfo &session, common::ObString &action_sql);
  static int get_audit_units(const stmt::StmtType stmt_type,
                             const sql::ObStmt *basic_stmt,
                             common::ObIArray<ObAuditUnit> &audit_units);
  static int get_audit_units_in_subquery(const sql::ObDMLStmt *basic_stmt,
                                         common::ObIArray<ObAuditUnit> &audit_units);
  static int get_stmt_operation_type_from_stmt(const stmt::StmtType stmt_type,
      const sql::ObStmt *stmt, share::schema::ObSAuditOperationType &operation_type);
  static int get_object_operation_type_from_stmt(const stmt::StmtType stmt_type,
      sql::ObStmt *stmt,
      share::schema::ObSAuditOperationType &operation_type,
      common::ObIArray<share::schema::ObObjectStruct> &object_ids);
  static int get_dml_objects(const sql::ObDMLStmt *stmt,
      common::ObIArray<share::schema::ObObjectStruct> &object_ids);
  static int get_uniq_sequences_in_dml(const sql::ObDMLStmt *stmt,
      common::hash::ObHashSet<uint64_t> &object_ids);
  // 下面一系列函数各种检查一种场景的审计规则，符合ObCheckAllowAuditFunc接口
  // 检查是否命中指定用户的语句审计规则
  static int check_allow_stmt_operation(ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      const ObAuditUnit &audit_unit,
                                      AuditDataParam &filled_param,
                                      bool &is_allow_audit);
  // 检查是否命中不指定用户的语句审计规则
  static int check_allow_stmt_all_user_operation(ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      const ObAuditUnit &audit_unit,
                                      AuditDataParam &filled_param,
                                      bool &is_allow_audit);
  // 检查是否命中not exist规则，此类规则根据返回码判断，和其他语句审计规则根据stmt_type判断的逻辑不同
  static int check_allow_not_exist_operation(ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      const ObAuditUnit &audit_unit,
                                      AuditDataParam &filled_param,
                                      bool &is_allow_audit);
  // 检查是否命中对象审计规则
  static int check_allow_obj_operation(ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      const ObAuditUnit &audit_unit,
                                      AuditDataParam &filled_param,
                                      bool &is_allow_audit);
  // 新增审计规则时，如果这条语句自身满足新增的规则，则也需要被审计
  static int check_allow_self_audit_operation(ObSQLSessionInfo &session,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      const ObAuditUnit &audit_unit,
                                      AuditDataParam &filled_param,
                                      bool &is_allow_audit);
  static share::schema::ObObjectType get_object_type_from_audit_type(
      const share::schema::ObSAuditType type);
  static share::schema::ObSAuditType get_audit_type_from_object_type(
      const share::schema::ObObjectType type);
  static bool is_not_exist_errno(const int errcode);
  static int print_obj_privs_to_buff(char *buf,
                                     const int64_t buf_len,
                                     int64_t &pos,
                                     const stmt::StmtType stmt_type,
                                     const share::ObRawObjPrivArray &obj_priv_array,
                                     const uint64_t grant_option);
  static int print_admin_option_to_buff(char *buf,
                                        const int64_t buf_len,
                                        int64_t &pos,
                                        const stmt::StmtType stmt_type,
                                        uint64_t option);

  static int get_audit_file_name(char *buf,
                                 const int64_t buf_len,
                                 int64_t &pos);
  static int get_action_type_from_stmt_type(const stmt::StmtType stmt_type,
                                            const sql::ObStmt *stmt,
                                            audit::AuditActionType &action_type);
  static common::ObString get_action_type_string(const audit::AuditActionType action_type);
  static const common::ObString audit_action_type_string[];

private:
  static const char* priv_names[];
  static const ObCheckAllowAuditFunc check_allow_audit_funcs_[];
  static const int check_allow_funcs_nums_;
};
} //namespace sql
#endif
} //namespace oceanbase
#endif


