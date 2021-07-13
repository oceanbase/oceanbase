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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_RESOURCE_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_RESOURCE_RESOLVER_

#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "share/config/ob_config_helper.h"
#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/cmd/ob_resource_stmt.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "sql/resolver/ob_stmt.h"

namespace oceanbase {
namespace sql {

template <class T>
class ObResourcePoolOptionResolver {
public:
  ObResourcePoolOptionResolver(){};
  ~ObResourcePoolOptionResolver(){};

private:
  DISALLOW_COPY_AND_ASSIGN(ObResourcePoolOptionResolver);

public:
  int resolve_options(T* stmt, ParseNode* node) const;

private:
  int resolve_option(T* stmt, ParseNode* node) const;
  int resolve_zone_list(T* stmt, ParseNode* node) const;
  int resolve_unit_num_option(T* stmt, ParseNode* node) const;
  int resolve_unit_id_list(T* stmt, ParseNode* node) const;
};

template <class T>
int ObResourcePoolOptionResolver<T>::resolve_options(T* stmt, ParseNode* node) const
{
  int ret = common::OB_SUCCESS;
  if (node) {
    if (OB_UNLIKELY(T_RESOURCE_POOL_OPTION_LIST != node->type_) || OB_UNLIKELY(0 > node->num_child_)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(ERROR, "invalid param", K(ret));
    } else {
      ParseNode* option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
        option_node = node->children_[i];
        if (OB_FAIL(resolve_option(stmt, option_node))) {
          SQL_RESV_LOG(WARN, "resolve resource option failed", K(ret));
          break;
        }
      }
    }
  }
  return ret;
}

template <class T>
int ObResourcePoolOptionResolver<T>::resolve_option(T* stmt, ParseNode* option_node) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "null ptr", K(ret));
  } else if (option_node) {
    switch (option_node->type_) {
      case T_UNIT_NUM: {
        if (OB_FAIL(resolve_unit_num_option(stmt, option_node))) {
          SQL_RESV_LOG(WARN, "resolve unit num option failed", K(ret));
        }
        break;
      }
      case T_ZONE_LIST: {
        if (OB_FAIL(resolve_zone_list(stmt, option_node))) {
          SQL_RESV_LOG(WARN, "resolve zone list failed", K(ret));
        }
        break;
      }
      case T_UNIT: {
        char* unit_name = const_cast<char*>(option_node->children_[0]->str_value_);
        common::ObString unit;
        unit.assign_ptr(unit_name, static_cast<int32_t>(option_node->children_[0]->str_len_));
        stmt->set_unit(unit);
        break;
      }
      case T_REPLICA_TYPE: {
        ObReplicaType type;
        if (option_node->num_child_ != 1 || OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid replica type option_node", K(ret), K(option_node));
        } else if (OB_FAIL(ObAlterSystemResolverUtil::resolve_replica_type(option_node->children_[0], type))) {
          SQL_RESV_LOG(WARN, "fail to resove repilca type", K(ret));
        } else {
          stmt->set_replica_type(type);
        }
        break;
      }
      default: {
        /* won't be here */
        ret = common::OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(ERROR, "unexprected option", "type", option_node->type_, K(ret));
        break;
      }
    }
  }
  return ret;
}

template <class T>
int ObResourcePoolOptionResolver<T>::resolve_unit_id_list(T* stmt, ParseNode* node) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(stmt));
  } else if (NULL == node) {
    // may be null node
  } else if (OB_UNLIKELY(T_UNIT_ID_LIST != node->type_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "invalid param", K(ret));
  } else if (stmt::T_ALTER_RESOURCE_POOL != stmt->get_cmd_type()) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "invalid cmd type", "cmd_type", stmt->get_cmd_type());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      uint64_t unit_id = node->children_[i]->value_;
      if (OB_FAIL(stmt->fill_delete_unit_id(unit_id))) {
        SQL_RESV_LOG(WARN, "fail to add unit id", K(ret));
      }
    }
  }
  return ret;
}

template <class T>
int ObResourcePoolOptionResolver<T>::resolve_unit_num_option(T* stmt, ParseNode* node) const
{
  using namespace common;
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(T_UNIT_NUM != node->type_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "invalid param", K(ret));
  } else if (node->num_child_ < 1 || node->num_child_ > 2) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "invalid node", K(node));
  } else {
    int32_t unit_num = static_cast<int32_t>(node->children_[0]->value_);
    stmt->set_unit_num(unit_num);
    if (0 == unit_num) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "unit_num, can't be zero");
    } else if (1 == node->num_child_) {
      // do nothing.
    } else if (stmt::T_ALTER_RESOURCE_POOL != stmt->get_cmd_type()) {
      if (2 == node->num_child_) {
        ret = common::OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create/drop resource pool with specific unit id");
      }
    } else if (OB_FAIL(resolve_unit_id_list(stmt, node->children_[1]))) {
      SQL_RESV_LOG(WARN, "fail to resolve unit_id_list", K(ret));
    } else {
    }  // over, no more to do
  }
  return ret;
}

template <class T>
int ObResourcePoolOptionResolver<T>::resolve_zone_list(T* stmt, ParseNode* node) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(T_ZONE_LIST != node->type_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "invalid param", K(ret));
  }

  for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode* elem = node->children_[i];
    if (OB_ISNULL(elem)) {
      ret = common::OB_ERR_PARSER_SYNTAX;
      SQL_RESV_LOG(WARN, "Wrong zone");
    } else {
      if (OB_LIKELY(T_VARCHAR == elem->type_)) {
        common::ObString zone_str(elem->str_len_, elem->str_value_);
        if (OB_FAIL(stmt->add_zone(zone_str))) {
          SQL_RESV_LOG(WARN, "stmt add_zone failed", K(zone_str), K(ret));
        }
      } else {
        ret = common::OB_ERR_PARSER_SYNTAX;
        SQL_RESV_LOG(WARN, "Wrong zone");
        break;
      }
    }
  }
  return ret;
}

// use template to reuse for alter unit.
template <class T>
class ObResourceUnitOptionResolver {
public:
  ObResourceUnitOptionResolver(){};
  ~ObResourceUnitOptionResolver(){};

private:
  DISALLOW_COPY_AND_ASSIGN(ObResourceUnitOptionResolver);

public:
  int resolve_options(T* stmt, ParseNode* node) const;

private:
  int resolve_option(T* stmt, ParseNode* node) const;
};

template <class T>
int ObResourceUnitOptionResolver<T>::resolve_options(T* stmt, ParseNode* node) const
{
  int ret = common::OB_SUCCESS;
  if (node) {
    if (OB_UNLIKELY(node->type_ != T_RESOURCE_UNIT_OPTION_LIST) || OB_UNLIKELY(node->num_child_ < 0)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid node", K(ret));
    } else {
      ParseNode* option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
        option_node = node->children_[i];
        if (OB_FAIL(resolve_option(stmt, option_node))) {
          SQL_RESV_LOG(WARN, "resolve resource option failed", K(ret));
        }
      }
    }
  }
  return ret;
}

template <class T>
int ObResourceUnitOptionResolver<T>::resolve_option(T* stmt, ParseNode* option_node) const
{
  using namespace common;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid stmt", K(ret));
  } else if (OB_LIKELY(NULL != option_node)) {
    if (OB_UNLIKELY(1 != option_node->num_child_ || NULL == option_node->children_[0])) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(ERROR, "invalid node", K(ret));
    } else {
      ParseNode* child = option_node->children_[0];
      // resolve conf value (e.g KB, MB)
      int64_t unit_num = 0;
      double cpu = 0;
      if (T_INT == child->type_) {
        unit_num = static_cast<int64_t>(child->value_);
        if (OB_UNLIKELY(0 == unit_num)) {
          ret = common::OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "param, the param can't be zero");
        }
      } else if (T_VARCHAR == child->type_) {
        bool valid = false;
        common::ObSqlString buf;
        if (OB_FAIL(buf.append(child->str_value_, child->str_len_))) {
          SQL_RESV_LOG(WARN, "fail to assign child str", K(ret));
        } else {
          // create resource unit unit_test4 max_cpu '1', max_iops '128'
          // equal to
          // create resource unit unit_test4 max_cpu 1, max_iops 128
          // for backward compatibility.
          //
          // create resource unit unit_test4 max_memory '1', max_disk_size '128'
          // equal to
          // create resource unit unit_test4 max_memory 1048576, max_disk_size 134217728
          // for backward compatibility, because the unit of string value like '128' is MB
          // in oberver with old version.
          if (T_MIN_CPU == option_node->type_ || T_MAX_CPU == option_node->type_ || T_MIN_IOPS == option_node->type_ ||
              T_MAX_IOPS == option_node->type_ || T_MAX_SESSION_NUM == option_node->type_) {
            //  '3' = 3, '3k' = 3000, '3m' = 3000000
            //  not support: 3kb, 3mb, 3gb, 3g, 3t, 3tb etc
            unit_num = common::ObConfigReadableIntParser::get(buf.ptr(), valid);
          } else {
            // '3' = '3mb' = 3*1024*1024
            unit_num = common::ObConfigCapacityParser::get(buf.ptr(), valid);
          }
          if (!valid) {
            ret = common::OB_ERR_PARSE_SQL;
          } else if (OB_UNLIKELY(0 == unit_num)) {
            ret = common::OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "param, the param can't be zero");
          }
        }
      } else if (T_NUMBER == child->type_) {
        int err = 0;
        char* endptr = NULL;
        cpu = common::ObCharset::strntod(child->str_value_, static_cast<int32_t>(child->str_len_), &endptr, &err);
        if (OB_UNLIKELY(EOVERFLOW == err)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
        } else if (OB_UNLIKELY(0 == cpu)) {
          ret = common::OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "param, the param can't be zero");
        }
      } else {
        ret = common::OB_ERR_PARSE_SQL;
      }

      if (OB_SUCC(ret)) {
        switch (option_node->type_) {
          case T_MIN_MEMORY: {
            stmt->set_min_memory(unit_num);
            break;
          }
          case T_MIN_IOPS: {
            stmt->set_min_iops(unit_num);
            break;
          }
          case T_MIN_CPU: {
            if (T_NUMBER == child->type_) {
              stmt->set_min_cpu(cpu);
            } else {
              stmt->set_min_cpu(static_cast<double>(unit_num));
            }
            break;
          }
          case T_MAX_MEMORY: {
            stmt->set_max_memory(unit_num);
            break;
          }
          case T_MAX_DISK_SIZE: {
            stmt->set_max_disk_size(unit_num);
            break;
          }
          case T_MAX_CPU: {
            if (T_NUMBER == child->type_) {
              stmt->set_max_cpu(cpu);
            } else {
              stmt->set_max_cpu(static_cast<double>(unit_num));
            }
            break;
          }
          case T_MAX_IOPS: {
            stmt->set_max_iops(unit_num);
            break;
          }
          case T_MAX_SESSION_NUM: {
            stmt->set_max_session_num(unit_num);
            break;
          }
          default: {
            /* won't be here */
            ret = common::OB_ERR_UNEXPECTED;
            break;
          }
        }  // switch
      }    // if
    }
  }
  return ret;
}

/**************************************************
 *            Resource Pool Resolver
 **************************************************/
class ObCreateResourcePoolResolver : public ObCMDResolver {
public:
  explicit ObCreateResourcePoolResolver(ObResolverParams& params);
  virtual ~ObCreateResourcePoolResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateResourcePoolResolver);
  // function members

private:
  // data members
};

class ObSplitResourcePoolResolver : public ObCMDResolver {
public:
  explicit ObSplitResourcePoolResolver(ObResolverParams& params);
  virtual ~ObSplitResourcePoolResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  int resolve_split_pool_list(ObSplitResourcePoolStmt* stmt, const ParseNode& parse_node);
  int resolve_corresponding_zone_list(ObSplitResourcePoolStmt* stmt, const ParseNode& parse_node);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSplitResourcePoolResolver);
  // function members

private:
  // data members
};

class ObMergeResourcePoolResolver : public ObCMDResolver {
public:
  explicit ObMergeResourcePoolResolver(ObResolverParams& params);
  virtual ~ObMergeResourcePoolResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  int resolve_old_merge_pool_list(ObMergeResourcePoolStmt* stmt, const ParseNode& parse_node);
  int resolve_new_merge_pool_list(ObMergeResourcePoolStmt* stmt, const ParseNode& parse_node);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeResourcePoolResolver);
  // function members

private:
  // data members
};

class ObAlterResourcePoolResolver : public ObCMDResolver {
public:
  explicit ObAlterResourcePoolResolver(ObResolverParams& params);
  virtual ~ObAlterResourcePoolResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourcePoolResolver);
  // function members

private:
  // data members
};

class ObDropResourcePoolResolver : public ObCMDResolver {
public:
  explicit ObDropResourcePoolResolver(ObResolverParams& params);
  virtual ~ObDropResourcePoolResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropResourcePoolResolver);
  // function members

private:
  // data members
};

/**************************************************
 *            Resource Unit Resolver
 **************************************************/
class ObCreateResourceUnitResolver : public ObCMDResolver {
public:
  explicit ObCreateResourceUnitResolver(ObResolverParams& params);
  virtual ~ObCreateResourceUnitResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateResourceUnitResolver);
  // function members

private:
  // data members
};

class ObAlterResourceUnitResolver : public ObCMDResolver {
public:
  explicit ObAlterResourceUnitResolver(ObResolverParams& params);
  virtual ~ObAlterResourceUnitResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourceUnitResolver);
  // function members
};

class ObDropResourceUnitResolver : public ObCMDResolver {
public:
  explicit ObDropResourceUnitResolver(ObResolverParams& params);
  virtual ~ObDropResourceUnitResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropResourceUnitResolver);
  // function members

private:
  // data members
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_CREATE_RESOURCE_RESOLVER_H */
