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

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/config/ob_config_helper.h"
#include "share/unit/ob_unit_resource.h"          // ObUnitResource
#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/cmd/ob_resource_stmt.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "sql/resolver/ob_stmt.h"

namespace oceanbase
{
namespace sql
{

template <class T>
class ObResourcePoolOptionResolver
{
public:
  ObResourcePoolOptionResolver() {};
  ~ObResourcePoolOptionResolver() {};
private:
  DISALLOW_COPY_AND_ASSIGN(ObResourcePoolOptionResolver);
public:
  int resolve_options(T *stmt, ParseNode *node) const;
private:
  int resolve_option(T *stmt, ParseNode *node) const;
  int resolve_zone_list(T *stmt, ParseNode *node) const;
  int resolve_unit_num_option(T *stmt, ParseNode *node) const;
  int resolve_unit_id_list(T *stmt, ParseNode *node) const;
};

template <class T>
int ObResourcePoolOptionResolver<T>::resolve_options(T *stmt, ParseNode *node) const
{
  int ret = common::OB_SUCCESS;
  if (node) {
    if (OB_UNLIKELY(T_RESOURCE_POOL_OPTION_LIST != node->type_)
        || OB_UNLIKELY(0 > node->num_child_)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(ERROR, "invalid param", K(ret));
    } else {
      ParseNode *option_node = NULL;
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
int ObResourcePoolOptionResolver<T>::resolve_option(T *stmt, ParseNode *option_node) const
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
        char *unit_name = const_cast<char *>(option_node->children_[0]->str_value_);
        common::ObString unit;
        unit.assign_ptr(unit_name, static_cast<int32_t>(option_node->children_[0]->str_len_));
        stmt->set_unit(unit);
        break;
      }
      case T_REPLICA_TYPE: {
        ObReplicaType type;
        if (option_node->num_child_ != 1
            || OB_ISNULL(option_node->children_[0])) {
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

template<class T>
int ObResourcePoolOptionResolver<T>::resolve_unit_id_list(T *stmt, ParseNode *node) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == stmt)) {
    ret = common::OB_INVALID_ARGUMENT;
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

template<class T>
int ObResourcePoolOptionResolver<T>::resolve_unit_num_option(T *stmt, ParseNode *node) const
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
      // 仅有一个child，没有delete unit num，无需继续解析
    } else if (stmt::T_ALTER_RESOURCE_POOL != stmt->get_cmd_type()) {
      if (2 == node->num_child_) {
        ret = common::OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create/drop resource pool with specific unit id");
      }
    } else if (OB_FAIL(resolve_unit_id_list(stmt, node->children_[1]))) {
      SQL_RESV_LOG(WARN, "fail to resolve unit_id_list", K(ret));
    } else {} // over, no more to do
  }
  return ret;
}

template<class T>
int ObResourcePoolOptionResolver<T>::resolve_zone_list(T *stmt, ParseNode *node) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(T_ZONE_LIST != node->type_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "invalid param", K(ret));
  }

  for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode *elem = node->children_[i];
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

// 做成模板的原因是：如果将来有Alter Unit的需求, 改动代码量更小
template <class T>
class ObResourceUnitOptionResolver : public share::ObUnitResource
{
public:
  ObResourceUnitOptionResolver() : share::ObUnitResource() {};
  ~ObResourceUnitOptionResolver() {};
private:
  DISALLOW_COPY_AND_ASSIGN(ObResourceUnitOptionResolver);
public:
  int resolve_options(ParseNode *node, share::ObUnitResource &ur);
  const char *get_resource_unit_option_type_str(const ObItemType &type) const
  {
    const char *name = "UNKNOWN";
    switch (type) {
      case T_MIN_CPU:
        name = "MIN_CPU";
        break;
      case T_MAX_CPU:
        name = "MAX_CPU";
        break;
      case T_MEMORY_SIZE:
        name = "MEMORY_SIZE";
        break;
      case T_LOG_DISK_SIZE:
        name = "LOG_DISK_SIZE";
        break;
      case T_MAX_IOPS:
        name = "MAX_IOPS";
        break;
      case T_MIN_IOPS:
        name = "MIN_IOPS";
        break;
      case T_IOPS_WEIGHT:
        name = "IOPS_WEIGHT";
        break;
      default:
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid item type for RESOURCE UNIT", K(type));
        name = "UNKNOWN";
        break;
    }
    return name;
  }
private:
  int resolve_option_(ParseNode *node, share::ObUnitResource &ur);
  void print_invalid_argument_user_error_(const ObItemType type, const char *str) const;
  template <class ValueT>
  int check_value_(const ValueT parse_value, const ObItemType type) const;
  int resolve_number_(
      const ObItemType &option_type,
      ParseNode *child,
      double &parse_double_value) const;
  int resolve_varchar_(ParseNode *child, const ObItemType &type, int64_t &parse_int_value) const;
};

template <class T>
int ObResourceUnitOptionResolver<T>::resolve_options(ParseNode *node, share::ObUnitResource &ur)
{
  int ret = common::OB_SUCCESS;
  // first reset all resources
  share::ObUnitResource::reset();

  if (node) {
    if (OB_UNLIKELY(node->type_ != T_RESOURCE_UNIT_OPTION_LIST)
        || OB_UNLIKELY(node->num_child_ < 0)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid node", K(ret));
    } else {
      ParseNode *option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
        option_node = node->children_[i];
        // resolve every resource options
        if (OB_FAIL(resolve_option_(option_node, ur))) {
          SQL_RESV_LOG(WARN, "resolve resource option failed", KR(ret));
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    // construct user specified share::ObUnitResource
    ur = *this;
  }

  // at last, reset anyway
  ObUnitResource::reset();
  return ret;
}

template <class T>
void ObResourceUnitOptionResolver<T>::print_invalid_argument_user_error_(const ObItemType type,
    const char *str) const
{
  std::string err_msg = get_resource_unit_option_type_str(type);
  err_msg += str;
  LOG_USER_ERROR(OB_INVALID_ARGUMENT, err_msg.c_str());
}

template <class T>
template <class ValueT>
int ObResourceUnitOptionResolver<T>::check_value_(const ValueT value,
    const ObItemType type) const
{
  int ret = OB_SUCCESS;
  if (T_IOPS_WEIGHT != type) {
    if (OB_UNLIKELY(value <= 0)) {
      // iops weight is allowed to set zero
      print_invalid_argument_user_error_(type, ", value should be positive");
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("param can't be zero", KR(ret), K(type), K(value));
    }
  } else if (OB_UNLIKELY(value < 0)) {
    print_invalid_argument_user_error_(type, ", value can not be negative");
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("param can not be negative", KR(ret), K(type), K(value));
  } else {
    // succ
  }
  return ret;
}

template <class T>
int ObResourceUnitOptionResolver<T>::resolve_number_(
    const ObItemType &option_type,
    ParseNode *child,
    double &parse_double_value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", KR(ret));
  } else {
    int err = 0;
    char *endptr = NULL;
    parse_double_value = common::ObCharset::strntod(child->str_value_,
        static_cast<int32_t>(child->str_len_), &endptr, &err);
    if (OB_UNLIKELY(EOVERFLOW == err)) {
      ret = common::OB_DATA_OUT_OF_RANGE;
      LOG_WARN("fail to convert to double value", KR(ret), K(child->str_value_),
          K(child->str_len_));
    }
  }
  return ret;
}

template <class T>
int ObResourceUnitOptionResolver<T>::resolve_varchar_(ParseNode *child, const ObItemType &type, int64_t &parse_int_value) const
{
  int ret = OB_SUCCESS;
  bool valid = false;
  common::ObSqlString buf;
  if (OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", KR(ret), KP(child));
  } else if (OB_FAIL(buf.append(child->str_value_, child->str_len_))) {
    SQL_RESV_LOG(WARN, "fail to assign child str", KR(ret), K(child->str_value_), K(child->str_len_));
  } else {
    //
    // bugfix:
    //
    // create resource unit unit_test4 max_cpu '1', max_iops '128'
    // 等价于
    // create resource unit unit_test4 max_cpu 1, max_iops 128
    // 因为按照大家公认的理解，它们的单位是“个”
    //
    // create resource unit unit_test4 memory_size '1', log_disk_size '128'
    // 等价于
    // create resource unit unit_test4 memory_size 1048576, max_iops 134217728
    // 因为“按照 OceanBase 历史标准” 它们的默认单位为 mb
    //
    // 之所以没有在语法层禁止 min_cpu 等用 varchar 表示，是 backward compatibility
    // 考虑。万一已经有项目在 iops 等值上用了 '912312' 这种 varchar 表示，
    // 我们不能让他报错。
    if (T_MIN_CPU == type ||
        T_MAX_CPU == type ||
        T_MIN_IOPS == type ||
        T_MAX_IOPS == type ||
        T_IOPS_WEIGHT == type) {
      //  '3' = 3, '3k' = 3000, '3m' = 3000000
      //  not support: 3kb, 3mb, 3gb, 3g, 3t, 3tb etc
      parse_int_value = common::ObConfigReadableIntParser::get(buf.ptr(), valid);
    } else if (T_MEMORY_SIZE == type ||
               T_LOG_DISK_SIZE == type) {
      // '3' = '3mb' = 3*1024*1024
      parse_int_value = common::ObConfigCapacityParser::get(buf.ptr(), valid);
    } else {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid option node type", KR(ret), K(type), K(buf));
    }

    if (OB_FAIL(ret)) {
    } else if (!valid) {
      print_invalid_argument_user_error_(type, ", parse int value error");
      ret = common::OB_ERR_PARSE_SQL;
      LOG_WARN("parse varchar value to int fail", KR(ret), K(type), K(buf), K(valid));
    }
  }
  return ret;
}

template <class T>
int ObResourceUnitOptionResolver<T>::resolve_option_(ParseNode *option_node, share::ObUnitResource &ur)
{
  using namespace common;
  int ret = common::OB_SUCCESS;

  // default value means not changed
  double parse_double_value = 0;
  int64_t parse_int_value = 0;

  if (OB_LIKELY(NULL != option_node)) {
    if (OB_UNLIKELY(1 != option_node->num_child_ || NULL == option_node->children_[0])) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(ERROR, "invalid node", KR(ret), K(option_node->num_child_));
    } else {
      ParseNode *child = option_node->children_[0];
      ObItemType option_type = option_node->type_;

      if (T_NUMBER == child->type_) {
        // handle NUMBER value only for CPU
        if (OB_FAIL(resolve_number_(option_node->type_, child, parse_double_value))) {
          LOG_WARN("resolve number value fail", KR(ret), K(option_node->type_));
        } else if (OB_FAIL(check_value_(parse_double_value, option_type))) {
          LOG_WARN("fail to check double value", KR(ret), K(parse_double_value));
        } else if (T_MIN_CPU == option_type) {
          min_cpu_ = parse_double_value;
        } else if (T_MAX_CPU == option_type) {
          max_cpu_ = parse_double_value;
        } else {
          print_invalid_argument_user_error_(option_type, ", value can not be 'number' type");
          ret = common::OB_INVALID_ARGUMENT;
          LOG_WARN("resource unit option should not be number type", KR(ret), K(option_type),
              K(child->str_value_), K(child->str_len_));
        }
      } else {
        if (T_INT == child->type_) {
          parse_int_value = static_cast<int64_t>(child->value_);
        } else if (T_VARCHAR == child->type_) {
          if (OB_FAIL(resolve_varchar_(child, option_type, parse_int_value))) {
            LOG_WARN("fail to resolve varchar value", KR(ret));
          }
        } else {
          ret = common::OB_ERR_PARSE_SQL;
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_value_(parse_int_value, option_type))) {
          LOG_WARN("fail to check int value", KR(ret), K(parse_int_value));
        } else if (T_MIN_CPU == option_type) {
          min_cpu_ = static_cast<double>(parse_int_value);
        } else if (T_MAX_CPU == option_type) {
          max_cpu_ = static_cast<double>(parse_int_value);
        } else if (T_MEMORY_SIZE == option_type) {
          memory_size_ = parse_int_value;
        } else if (T_LOG_DISK_SIZE == option_type) {
          log_disk_size_ = parse_int_value;
        } else if (T_MIN_IOPS == option_type) {
          min_iops_ = parse_int_value;
        } else if (T_MAX_IOPS == option_type) {
          max_iops_ = parse_int_value;
        } else if (T_IOPS_WEIGHT == option_type) {
          iops_weight_ = parse_int_value;
        } else {
          /* won't be here */
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("unknown resource unit option, unexprected", KR(ret), K(option_type),
              K(parse_int_value));
        }
      }
    }
  }
  return ret;
}

/**************************************************
 *            Resource Pool Resolver
 **************************************************/
class ObCreateResourcePoolResolver : public ObCMDResolver
{
public:
  explicit ObCreateResourcePoolResolver(ObResolverParams &params);
  virtual ~ObCreateResourcePoolResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateResourcePoolResolver);
  // function members

private:
  // data members
};

class ObSplitResourcePoolResolver : public ObCMDResolver
{
public:
  explicit ObSplitResourcePoolResolver(ObResolverParams &params);
  virtual ~ObSplitResourcePoolResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_split_pool_list(ObSplitResourcePoolStmt *stmt, const ParseNode &parse_node);
  int resolve_corresponding_zone_list(ObSplitResourcePoolStmt *stmt, const ParseNode &parse_node);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSplitResourcePoolResolver);
  // function members

private:
  // data members
};

class ObMergeResourcePoolResolver : public ObCMDResolver
{
public:
  explicit ObMergeResourcePoolResolver(ObResolverParams &params);
  virtual ~ObMergeResourcePoolResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_old_merge_pool_list(ObMergeResourcePoolStmt *stmt, const ParseNode &parse_node);
  int resolve_new_merge_pool_list(ObMergeResourcePoolStmt *stmt, const ParseNode &parse_node);
private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeResourcePoolResolver);
  //function members

private:
  //data members
};

class ObAlterResourceTenantResolver : public ObCMDResolver
{
public:
  explicit ObAlterResourceTenantResolver(ObResolverParams &params);
  virtual ~ObAlterResourceTenantResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_resource_tenant_name(ObAlterResourceTenantStmt *stmt, const ParseNode *parse_node);
  int resolve_new_unit_num(ObAlterResourceTenantStmt *stmt, const ParseNode *parse_node);
  int resolve_unit_group_id_list(ObAlterResourceTenantStmt *stmt, const ParseNode *parse_node);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourceTenantResolver);
  // function members

private:
  // data members
};

class ObAlterResourcePoolResolver : public ObCMDResolver
{
public:
  explicit ObAlterResourcePoolResolver(ObResolverParams &params);
  virtual ~ObAlterResourcePoolResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourcePoolResolver);
  // function members

private:
  // data members
};

class ObDropResourcePoolResolver : public ObCMDResolver
{
public:
  explicit ObDropResourcePoolResolver(ObResolverParams &params);
  virtual ~ObDropResourcePoolResolver();

  virtual int resolve(const ParseNode &parse_tree);
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
class ObCreateResourceUnitResolver : public ObCMDResolver
{
public:
  explicit ObCreateResourceUnitResolver(ObResolverParams &params);
  virtual ~ObCreateResourceUnitResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateResourceUnitResolver);
  // function members

private:
  // data members
};

class ObAlterResourceUnitResolver : public ObCMDResolver
{
public:
  explicit ObAlterResourceUnitResolver(ObResolverParams &params);
  virtual ~ObAlterResourceUnitResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterResourceUnitResolver);
  // function members
};

class ObDropResourceUnitResolver : public ObCMDResolver
{
public:
  explicit ObDropResourceUnitResolver(ObResolverParams &params);
  virtual ~ObDropResourceUnitResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropResourceUnitResolver);
  // function members

private:
  // data members
};


} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_CREATE_RESOURCE_RESOLVER_H */
