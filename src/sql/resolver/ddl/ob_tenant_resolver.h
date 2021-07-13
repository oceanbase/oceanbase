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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_TENANT_RESOLVER_H
#define OCEANBASE_SQL_RESOLVER_DDL_OB_TENANT_RESOLVER_H

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_modify_tenant_stmt.h"
#include "sql/resolver/ob_stmt.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase {
namespace sql {
template <class T>
class ObTenantResolver {
public:
  ObTenantResolver()
      : charset_type_(common::CHARSET_INVALID),
        collation_type_(common::CS_TYPE_INVALID),
        alter_option_bitset_(),
        modify_read_only_(false)
  {}
  ~ObTenantResolver()
  {}

public:
  int resolve_tenant_options(T* stmt, ParseNode* node, ObSQLSessionInfo* session_info, common::ObIAllocator& allocator);
  const common::ObBitSet<>& get_alter_option_bitset() const
  {
    return alter_option_bitset_;
  };
  bool is_modify_read_only() const
  {
    return modify_read_only_;
  }

private:
  int resolve_tenant_option(T* stmt, ParseNode* node, ObSQLSessionInfo* session_info, common::ObIAllocator& allocator);
  int resolve_zone_list(T* stmt, ParseNode* node) const;
  int resolve_resource_pool_list(T* stmt, ParseNode* node) const;

private:
  common::ObCharsetType charset_type_;
  common::ObCollationType collation_type_;
  common::ObBitSet<> alter_option_bitset_;
  bool modify_read_only_;  // used in ob_modify_tenant_resolver.cpp
  DISALLOW_COPY_AND_ASSIGN(ObTenantResolver);
};

template <class T>
int ObTenantResolver<T>::resolve_tenant_options(
    T* stmt, ParseNode* node, ObSQLSessionInfo* session_info, common::ObIAllocator& allocator)
{
  int ret = common::OB_SUCCESS;
  if (node) {
    if (OB_UNLIKELY(T_TENANT_OPTION_LIST != node->type_ || 0 > node->num_child_)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid node", K(ret));
    } else {
      ParseNode* option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
        option_node = node->children_[i];
        if (OB_FAIL(resolve_tenant_option(stmt, option_node, session_info, allocator))) {
          SQL_LOG(WARN, "resolve tenant option failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    stmt->set_collation_type(collation_type_);
    stmt->set_charset_type(charset_type_);
  }

  return ret;
}

template <class T>
int ObTenantResolver<T>::resolve_tenant_option(
    T* stmt, ParseNode* node, ObSQLSessionInfo* session_info, common::ObIAllocator& allocator)
{
  int ret = common::OB_SUCCESS;
  ParseNode* option_node = node;
  if (OB_ISNULL(stmt)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_LOG(ERROR, "null ptr", K(ret));
  } else if (option_node) {
    switch (option_node->type_) {
      case T_REPLICA_NUM: {
        int64_t replica_num = option_node->children_[0]->value_;
        if (OB_UNLIKELY(replica_num <= 0) || OB_UNLIKELY(replica_num > common::OB_TABLET_MAX_REPLICA_COUNT)) {
          ret = common::OB_INVALID_ARGUMENT;
          SQL_LOG(WARN, "invalid replica_num", K(ret), K(replica_num));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "replica_num");
        } else {
          if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::REPLICA_NUM))) {
              SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_REWRITE_MERGE_VERSION: {
        int64_t merge_version = option_node->children_[0]->value_;
        if (OB_UNLIKELY(merge_version <= 0)) {
          ret = common::OB_NOT_SUPPORTED;
          SQL_LOG(WARN, "Invalid rewrite merge version", K(merge_version));
        } else {
          stmt->set_rewrite_merge_version(merge_version);
          if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::REWRITE_MERGE_VERSION))) {
              SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_STORAGE_FORMAT_VERSION: {
        int64_t storage_format_version = option_node->children_[0]->value_;
        if (OB_UNLIKELY(storage_format_version < 0)) {
          ret = common::OB_NOT_SUPPORTED;
          SQL_LOG(WARN, "Invalid storage format version", K(storage_format_version), K(ret));
        } else {
          stmt->set_storage_format_version(storage_format_version);
          if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::STORAGE_FORMAT_VERSION))) {
              SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_STORAGE_FORMAT_WORK_VERSION: {
        int64_t storage_format_work_version = option_node->children_[0]->value_;
        if (OB_UNLIKELY(storage_format_work_version < 0)) {
          ret = common::OB_NOT_SUPPORTED;
          SQL_LOG(WARN, "Invalid storage format work version", K(storage_format_work_version), K(ret));
        } else {
          stmt->set_storage_format_work_version(storage_format_work_version);
          if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::STORAGE_FORMAT_WORK_VERSION))) {
              SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }

      case T_CHARSET: {
        common::ObString node_val(option_node->str_len_, option_node->str_value_);
        common::ObString charset = node_val.trim();
        common::ObCharsetType charset_type = common::ObCharset::charset_type(charset);
        if (common::CHARSET_INVALID == charset_type) {
          ret = common::OB_ERR_UNKNOWN_CHARSET;
          LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
        } else {
          charset_type_ = charset_type;
          if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            ret = OB_NOT_SUPPORTED;
            SQL_LOG(WARN, "tenant can't change charset", K(ret));
          }
        }
        break;
      }
      case T_COLLATION: {
        common::ObString node_val(option_node->str_len_, option_node->str_value_);
        common::ObString collation = node_val.trim();
        common::ObCollationType collation_type = common::ObCharset::collation_type(collation);
        if (common::CS_TYPE_INVALID == collation_type) {
          ret = common::OB_ERR_UNKNOWN_COLLATION;
          LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
        } else {
          collation_type_ = collation_type;
          if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            ret = OB_NOT_SUPPORTED;
            SQL_LOG(WARN, "tenant can't change collation", K(ret));
          }
        }
        break;
      }
      case T_PRIMARY_ZONE: {
        if (option_node->children_[0]->type_ == T_DEFAULT) {
          ret = OB_OP_NOT_ALLOW;
          SQL_LOG(WARN, "set tenant primary_zone DEFAULT is not allowed now", K(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set tenant primary_zone DEFAULT");
        } else if (T_RANDOM == option_node->children_[0]->type_) {
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000) {
            ret = OB_OP_NOT_ALLOW;
            SQL_RESV_LOG(WARN, "set primary_zone RANDOM is not allowed now", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set primary_zone RANDOM");
          } else {
            stmt->set_primary_zone(common::ObString(common::OB_RANDOM_PRIMARY_ZONE));
          }
        } else if (T_OP_GET_USER_VAR == option_node->children_[0]->type_) {
          ObObj var_value;
          if (OB_FAIL(ObResolverUtils::get_user_var_value(option_node->children_[0], session_info, var_value))) {
            SQL_RESV_LOG(WARN, "failed to get user var value", K(ret));
          } else if (!var_value.is_string_type()) {
            ret = OB_OP_NOT_ALLOW;
            SQL_RESV_LOG(WARN, "user variable not string type", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "user variable not string type");
          } else if (0 == var_value.get_string_len()) {
            ret = OB_OP_NOT_ALLOW;
            SQL_RESV_LOG(WARN, "set primary_zone empty is not allowed now", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set primary_zone empty");
          } else {
            stmt->set_primary_zone(var_value.get_string());
          }
        } else {
          common::ObString primary_zone;
          primary_zone.assign_ptr(const_cast<char*>(option_node->children_[0]->str_value_),
              static_cast<int32_t>(option_node->children_[0]->str_len_));
          if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2000 && primary_zone.empty()) {
            ret = OB_OP_NOT_ALLOW;
            SQL_RESV_LOG(WARN, "set primary_zone empty is not allowed now", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set primary_zone empty");
          } else {
            stmt->set_primary_zone(primary_zone);
          }
        }
        if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
          if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::PRIMARY_ZONE))) {
            SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_ZONE_LIST: {
        /* zone_list = (xxx,xxx,xxx) */
        ret = resolve_zone_list(stmt, option_node);
        if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
          if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::ZONE_LIST))) {
            SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_TENANT_RESOURCE_POOL_LIST: {
        /* resource_pool_list = (xxx,xxx,xxx) */
        ret = resolve_resource_pool_list(stmt, option_node);
        if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
          if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST))) {
            SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_READ_ONLY: {
        modify_read_only_ = true;
        if (T_ON == option_node->children_[0]->type_) {
          stmt->set_read_only(true);
        } else if (T_OFF == option_node->children_[0]->type_) {
          stmt->set_read_only(false);
        } else {
          ret = common::OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unknown read only options", K(ret));
        }
        if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
          if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::READ_ONLY))) {
            SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_COMMENT: {
        int64_t comment_length = option_node->children_[0]->str_len_;
        const char* comment_ptr = option_node->children_[0]->str_value_;
        common::ObString comment(comment_length, comment_ptr);
        ObCollationType client_cs_type = session_info->get_local_collation_connection();
        if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(
                allocator, comment, comment, client_cs_type, CS_TYPE_UTF8MB4_BIN))) {
          SQL_LOG(WARN, "fail to convert comment to utf8", K(ret));
        } else if (OB_UNLIKELY(comment.length() > common::MAX_TENANT_COMMENT_LENGTH)) {
          ret = common::OB_ERR_TOO_LONG_TENANT_COMMENT;
          LOG_USER_ERROR(OB_ERR_TOO_LONG_TENANT_COMMENT, common::MAX_TENANT_COMMENT_LENGTH);
        } else if (OB_FAIL(stmt->set_comment(comment))) {
          SQL_LOG(WARN, "fail to set comment", K(ret), K(comment));
        }
        if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
          if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::COMMENT))) {
            SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_LOCALITY: {
        if (NULL == option_node->children_ || option_node->num_child_ != 2) {
          ret = common::OB_INVALID_ARGUMENT;
          SQL_LOG(WARN, "invalid locality argument", K(ret), "num_child", option_node->num_child_);
        } else if (option_node->children_[0]->type_ == T_DEFAULT) {
          ret = OB_OP_NOT_ALLOW;
          SQL_LOG(WARN, "set tenant locality DEFAULT is not allowed now", K(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set tenant locality DEFAULT");
        } else {
          int64_t locality_length = option_node->children_[0]->str_len_;
          const char* locality_str = option_node->children_[0]->str_value_;
          common::ObString locality(locality_length, locality_str);
          if (OB_UNLIKELY(locality_length > common::MAX_LOCALITY_LENGTH)) {
            ret = common::OB_ERR_TOO_LONG_IDENT;
            LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, locality.length(), locality.ptr());
          } else if (0 == locality_length) {
            ret = OB_OP_NOT_ALLOW;
            SQL_RESV_LOG(WARN, "set locality empty is not allowed now", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set locality empty");
          } else if (OB_FAIL(stmt->set_locality(locality))) {
            SQL_LOG(WARN, "fail to set locality", K(ret), K(locality));
          }
          if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::LOCALITY))) {
              SQL_LOG(WARN, "fail to add locality member to bitset!", K(ret));
            } else if (nullptr == option_node->children_[1]) {
              // not force alter locality
            } else if (option_node->children_[1]->type_ != T_FORCE) {
              ret = common::OB_ERR_UNEXPECTED;
              SQL_LOG(ERROR, "invalid node", K(ret));
            } else if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::FORCE_LOCALITY))) {
              SQL_LOG(WARN, "fail to add force locality member to bitset", K(ret));
            }
          }
        }
        break;
      }
      case T_LOGONLY_REPLICA_NUM: {
        int64_t logonly_replica_num = option_node->children_[0]->value_;
        if (OB_UNLIKELY(logonly_replica_num <= 0) ||
            OB_UNLIKELY(logonly_replica_num > common::OB_TABLET_MAX_REPLICA_COUNT)) {
          ret = common::OB_INVALID_ARGUMENT;
          SQL_LOG(WARN, "invalid logonly_replica_num", K(ret), K(logonly_replica_num));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "logonly_replica_num");
        } else {
          if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
            if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::LOGONLY_REPLICA_NUM))) {
              SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_DEFAULT_TABLEGROUP: {
        if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
          common::ObString tablegroup_name(option_node->str_len_, option_node->str_value_);
          if (OB_FAIL(stmt->set_default_tablegroup_name(tablegroup_name))) {
            OB_LOG(WARN, "failed to set default tablegroup name", K(ret));
          } else if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::DEFAULT_TABLEGROUP))) {
            OB_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        } else {
          ret = OB_OP_NOT_ALLOW;
          SQL_LOG(WARN, "create tenant set default_tablegroup not allowed");
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set defalut_tablegroup");
        }
        break;
      }
      case T_PROGRESSIVE_MERGE_NUM: {
        if (stmt->get_stmt_type() == stmt::T_MODIFY_TENANT) {
          int64_t progressive_merge_num = option_node->children_[0]->value_;
          if (progressive_merge_num < 0 || progressive_merge_num > ObDDLResolver::MAX_PROGRESSIVE_MERGE_NUM) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "progressive_merge_num");
          } else if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::PROGRESSIVE_MERGE_NUM))) {
            SQL_LOG(WARN, "fail to add member", K(ret));
          } else {
            reinterpret_cast<ObModifyTenantStmt*>(stmt)->set_progressive_merge_num(progressive_merge_num);
          }
        }
        break;
      }
      default: {
        /* won't be here */
        ret = common::OB_ERR_UNEXPECTED;
        SQL_LOG(ERROR, "code should not reach here", K(ret));
        break;
      }
    }
  }
  return ret;
}

template <class T>
int ObTenantResolver<T>::resolve_zone_list(T* stmt, ParseNode* node) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(T_ZONE_LIST != node->type_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_LOG(ERROR, "invalid node", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode* elem = node->children_[i];
    if (OB_ISNULL(elem)) {
      ret = common::OB_ERR_PARSER_SYNTAX;
      SQL_LOG(WARN, "Wrong zone");
    } else {
      if (OB_LIKELY(T_VARCHAR == elem->type_)) {
        common::ObSqlString buf;
        if (OB_FAIL(buf.append(elem->str_value_, elem->str_len_))) {
          SQL_LOG(WARN, "fail to assigb str value to buf", K(ret));
        } else {
          ret = stmt->add_zone(buf.ptr());
        }
      } else {
        ret = common::OB_ERR_PARSER_SYNTAX;
        SQL_LOG(WARN, "Wrong zone");
      }
    }
  }
  return ret;
}

template <class T>
int ObTenantResolver<T>::resolve_resource_pool_list(T* stmt, ParseNode* node) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(T_TENANT_RESOURCE_POOL_LIST != node->type_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_LOG(ERROR, "invalid node", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode* elem = node->children_[i];
    if (OB_ISNULL(elem)) {
      ret = common::OB_ERR_PARSER_SYNTAX;
      SQL_LOG(WARN, "Wrong resource pool");
    } else {
      if (OB_LIKELY(T_VARCHAR == elem->type_)) {
        common::ObString resource_pool(static_cast<int32_t>(elem->str_len_), const_cast<char*>(elem->str_value_));
        ret = stmt->add_resource_pool(resource_pool);
      } else {
        ret = common::OB_ERR_PARSER_SYNTAX;
        SQL_LOG(WARN, "Wrong resource pool");
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_RESOLVER_DDL_OB_TENANT_RESOLVER_H
