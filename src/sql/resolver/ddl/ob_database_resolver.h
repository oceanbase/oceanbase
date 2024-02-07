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

#ifndef OCEANBASE_SQL_OB_DATABASE_RESOLVER_
#define OCEANBASE_SQL_OB_DATABASE_RESOLVER_

#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt.h"
#include "lib/charset/ob_charset.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
namespace sql
{
/*关于为什么使用模版而不用继承，请看ddl/ob_tenant_resolver.h的解释*/
template <class T>
class ObDatabaseResolver
{
public:
  ObDatabaseResolver() :
    alter_option_bitset_(),
    collation_already_set_(false)
  {
  }
  ~ObDatabaseResolver() {};
private:
  DISALLOW_COPY_AND_ASSIGN(ObDatabaseResolver);
public:
  static int resolve_primary_zone(T *stmt, ParseNode *node);
  int resolve_database_options(T *stmt, ParseNode *node, ObSQLSessionInfo *session_info);
  const common::ObBitSet<> &get_alter_option_bitset() const { return alter_option_bitset_; };
private:
  int resolve_database_option(T *stmt, ParseNode *node, ObSQLSessionInfo *session_info);
  int resolve_zone_list(T *stmt, ParseNode *node) const;
private:
  common::ObBitSet<> alter_option_bitset_;
  // 一条create/alter database语句中可能出现多次charset/collate，用于标记是否已出现过的flag
  bool collation_already_set_;
};

template <class T>
int ObDatabaseResolver<T>::resolve_database_options(T *stmt, ParseNode *node, ObSQLSessionInfo *session_info)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(node)) {
    ret = common::OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(stmt), K(node));
  } else if (OB_UNLIKELY(T_DATABASE_OPTION_LIST != node->type_)
             || OB_UNLIKELY(0 > node->num_child_)
             || OB_ISNULL(node->children_)) {
    ret = common::OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid node info", K(node->type_), K(node->num_child_), K(node->children_));
  } else {
    ParseNode *option_node = NULL;
    int32_t num = node->num_child_;
    for (int32_t i = 0; ret == common::OB_SUCCESS && i < num; i++) {
      option_node = node->children_[i];
      if (OB_FAIL(resolve_database_option(stmt, option_node, session_info))) {
        OB_LOG(WARN, "resolve database option failed", K(ret));
      }
    }
  }
  return ret;
}

template <class T>
int ObDatabaseResolver<T>::resolve_primary_zone(T *stmt, ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid primary_zone argument", K(ret), K(node));
  } else if (node->type_ == T_DEFAULT) {
    // do nothing
  } else if (T_RANDOM == node->type_) {
    if (OB_FAIL(stmt->set_primary_zone(common::ObString(common::OB_RANDOM_PRIMARY_ZONE)))) {
      SQL_RESV_LOG(WARN, "fail to set primary zone", K(ret));
    }
  } else {
    common::ObString primary_zone;
    primary_zone.assign_ptr(const_cast<char *>(node->str_value_),
                            static_cast<int32_t>(node->str_len_));
    if (OB_UNLIKELY(primary_zone.empty())) {
      ret = OB_OP_NOT_ALLOW;
      SQL_RESV_LOG(WARN, "set primary_zone empty is not allowed now", K(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set primary_zone empty");
    } else if (OB_FAIL(stmt->set_primary_zone(primary_zone))) {
      SQL_RESV_LOG(WARN, "fail to set primary zone", K(ret));
    }
  }
  return ret;
}

template <class T>
int ObDatabaseResolver<T>::resolve_database_option(T *stmt, ParseNode *node, ObSQLSessionInfo *session_info)
{
  int ret = common::OB_SUCCESS;
  ParseNode *option_node = node;
  if (OB_ISNULL(stmt)) {
    ret = common::OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(stmt), K(node));
  } else if (OB_ISNULL(option_node)) {
    //nothing to do
  } else {
    switch (option_node->type_) {
      case T_REPLICA_NUM: {
        int32_t replica_num = static_cast<int32_t>(option_node->value_);
        if (replica_num <= 0 || replica_num > common::OB_TABLET_MAX_REPLICA_COUNT) {
          ret = common::OB_NOT_SUPPORTED;
          OB_LOG(WARN, "Invalid replica_num", K(replica_num));
        } else {
          if (stmt::T_ALTER_DATABASE == stmt->get_stmt_type()) {
            if (OB_FAIL(alter_option_bitset_.add_member(
                    obrpc::ObAlterDatabaseArg::REPLICA_NUM))) {
              OB_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_PRIMARY_ZONE: {
        if (NULL == option_node->children_ || option_node->num_child_ != 1) {
          ret = common::OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid primary_zone argument", K(ret), "num_child", option_node->num_child_); 
        } else if (OB_FAIL(ObDatabaseResolver<T>::resolve_primary_zone(stmt, option_node->children_[0]))) {
          SQL_RESV_LOG(WARN, "failed to resolve primary zone", K(ret));
        } else if (stmt::T_ALTER_DATABASE == stmt->get_stmt_type()) {
          if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObAlterDatabaseArg::PRIMARY_ZONE))) {
            SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_CHARSET:
      case T_COLLATION: {
        common::ObCharsetType charset_type = common::CHARSET_INVALID;
        common::ObCollationType collation_type = common::CS_TYPE_INVALID;
        if (T_CHARSET == option_node->type_) {
          common::ObString charset(option_node->str_len_, option_node->str_value_);
          charset_type = common::ObCharset::charset_type(charset.trim());
          collation_type = common::ObCharset::get_default_collation(charset_type);
          if (OB_UNLIKELY(common::CHARSET_INVALID == charset_type)) {
            ret = common::OB_ERR_UNKNOWN_CHARSET;
            LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
          } else if (OB_UNLIKELY(common::CS_TYPE_INVALID == collation_type)) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "all valid charset types should have default collation type",
                            K(ret), K(charset_type), K(collation_type));
          } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(charset_type,
                                                                            session_info->get_effective_tenant_id()))) {
            OB_LOG(WARN, "failed to check charset data version valid", K(ret));
          } else if (OB_FAIL(sql::ObSQLUtils::is_collation_data_version_valid(collation_type,
                                                                              session_info->get_effective_tenant_id()))) {
            OB_LOG(WARN, "failed to check collation data version valid", K(ret));
          } else if (OB_UNLIKELY(collation_already_set_
                              && stmt->get_charset_type() != charset_type)) {
            // mysql执行下面这条sql时会报错，为了行为与mysql一致，resolve时即检查collation/charset不一致的问题
            // create database db charset utf8 charset utf16; 
            ret = OB_ERR_CONFLICTING_DECLARATIONS;
            SQL_RESV_LOG(WARN, "charsets mismatch", K(stmt->get_charset_type()), K(charset_type));
            const char *charset_name1 = ObCharset::charset_name(stmt->get_charset_type());
            const char *charset_name2 = ObCharset::charset_name(charset_type);
            LOG_USER_ERROR(OB_ERR_CONFLICTING_DECLARATIONS, charset_name1, charset_name2);
          }
        } else {
          common::ObString collation(option_node->str_len_, option_node->str_value_);
          collation_type = common::ObCharset::collation_type(collation.trim());
          charset_type = ObCharset::charset_type_by_coll(collation_type);
          if (OB_UNLIKELY(common::CS_TYPE_INVALID == collation_type)) {
            ret = common::OB_ERR_UNKNOWN_COLLATION;
            LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
          } else if (OB_UNLIKELY(common::CHARSET_INVALID == charset_type)) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "all valid collation types should have corresponding charset type",
                            K(ret), K(charset_type), K(collation_type));
          } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(charset_type,
                                                                            session_info->get_effective_tenant_id()))) {
            OB_LOG(WARN, "failed to check charset data version valid", K(ret));
          } else if (OB_FAIL(sql::ObSQLUtils::is_collation_data_version_valid(collation_type,
                                                                              session_info->get_effective_tenant_id()))) {
            OB_LOG(WARN, "failed to check collation data version valid", K(ret));
          } else if (OB_UNLIKELY(collation_already_set_
                              && stmt->get_charset_type() != charset_type)) {
            ret = OB_ERR_COLLATION_MISMATCH;
            SQL_RESV_LOG(WARN, "charset and collation mismatch",
                          K(stmt->get_charset_type()), K(charset_type));
          }
        }
        if (OB_SUCC(ret)) {
          stmt->set_charset_type(charset_type);
          stmt->set_collation_type(collation_type);
          collation_already_set_ = true;
          if (stmt::T_ALTER_DATABASE == stmt->get_stmt_type()) {
            if (OB_FAIL(alter_option_bitset_.add_member(
                    obrpc::ObAlterDatabaseArg::COLLATION_TYPE))) {
              OB_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_READ_ONLY: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = common::OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "invalid option node for read_only", K(option_node),
                 K(option_node->children_[0]));
        } else if (T_ON == option_node->children_[0]->type_) {
          stmt->set_read_only(true);
        } else if (T_OFF == option_node->children_[0]->type_) {
          stmt->set_read_only(false);
        } else {
          ret = common::OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "unknown read only options", K(ret));
        }
        if (common::OB_SUCCESS == ret && stmt->get_stmt_type() == stmt::T_ALTER_DATABASE) {
          if (OB_FAIL(alter_option_bitset_.add_member(
                  obrpc::ObAlterDatabaseArg::READ_ONLY))) {
            OB_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_DEFAULT_TABLEGROUP: {
        common::ObString tablegroup_name(option_node->str_len_, option_node->str_value_);
        if (OB_FAIL(stmt->set_default_tablegroup_name(tablegroup_name))) {
          OB_LOG(WARN, "failed to set default tablegroup name", K(ret));
        }

        if (common::OB_SUCCESS == ret && stmt->get_stmt_type() == stmt::T_ALTER_DATABASE) {
          if (OB_FAIL(alter_option_bitset_.add_member(
                  obrpc::ObAlterDatabaseArg::DEFAULT_TABLEGROUP))) {
            OB_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_DATABASE_ID: {
        if (stmt::T_CREATE_DATABASE != stmt->get_stmt_type()) {
          ret = common::OB_ERR_PARSE_SQL;
          SQL_RESV_LOG(WARN, "database id can be set in create database", K(ret));
        } else {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            uint64_t database_id = static_cast<uint64_t>(option_node->children_[0]->value_);
            stmt->set_database_id(database_id);
          }
        }
        break;
      }
      default: {
        OB_LOG(WARN, "invalid type of parse node", K(option_node));
        break;
      }
    }
  }
  return ret;
}

template <class T>
int ObDatabaseResolver<T>::resolve_zone_list(T *stmt, ParseNode *node) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(node) || T_ZONE_LIST != node->type_ || OB_ISNULL(node->children_)) {
    ret = common::OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(stmt), K(node));
  } else {
    for (int32_t i = 0; ret == common::OB_SUCCESS && i < node->num_child_; i++) {
      ParseNode *elem = node->children_[i];
      if (OB_ISNULL(elem)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        OB_LOG(WARN, "Wrong zone", K(node));
      } else {
        if (OB_LIKELY(T_VARCHAR == elem->type_)) {
          common::ObSqlString buf;
          if (OB_FAIL(buf.append(elem->str_value_, elem->str_len_))) {
            OB_LOG(WARN, "fail to assign str value to buf", K(ret));
          } else {
            ret = stmt->add_zone(buf.ptr());
          }
        } else {
          ret = common::OB_ERR_PARSER_SYNTAX;
          OB_LOG(WARN, "Wrong zone");
          break;
        }
      }
    }
  }
  return ret;
}
}  // namespace sql
} //namespace oceanbase

#endif
