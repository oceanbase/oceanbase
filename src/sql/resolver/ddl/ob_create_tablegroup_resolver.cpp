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

#define USING_LOG_PREFIX SERVER
#include "sql/resolver/ddl/ob_create_tablegroup_resolver.h"
#include "sql/resolver/ddl/ob_tablegroup_resolver.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

ObCreateTablegroupResolver::ObCreateTablegroupResolver(ObResolverParams &params)
    : ObTableGroupResolver(params)
{
}

ObCreateTablegroupResolver::~ObCreateTablegroupResolver()
{
}

int ObCreateTablegroupResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateTablegroupStmt *create_tablegroup_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(node) ||
      T_CREATE_TABLEGROUP != node->type_ ||
      OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null or parser is error", K(ret));
  } else {
    uint64_t compat_version = OB_INVALID_VERSION;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("get min data_version failed", K(ret), K(tenant_id));
    } else if (compat_version < DATA_VERSION_4_2_0_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can not create tablegroup while observer is upgrading", KR(ret), K(tenant_id));
    }
  }
  ObString tablegroup_name;
  if (OB_SUCC(ret)) {
    if (NULL == (create_tablegroup_stmt = create_stmt<ObCreateTablegroupStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create create_tablegroup_stmt", K(ret));
    } else {
      stmt_ = create_tablegroup_stmt;
    }

    if (OB_SUCC(ret)) {
      if (NULL != node->children_[IF_NOT_EXIST]) {
        if (T_IF_NOT_EXISTS == node->children_[IF_NOT_EXIST]->type_) {
          create_tablegroup_stmt->set_if_not_exists(true);
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "node type is not T_IF_NOT_EXISTS", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(node->children_[TG_NAME]) || T_IDENT != node->children_[TG_NAME]->type_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "node is null or node type is not T_IDENT", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (static_cast<int32_t>(node->children_[TG_NAME]->str_len_)
            >= OB_MAX_TABLEGROUP_NAME_LENGTH) {
          ret = OB_ERR_TOO_LONG_IDENT;
          LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(node->children_[TG_NAME]->str_len_),
                         node->children_[TG_NAME]->str_value_);
        } else {
          tablegroup_name.assign_ptr(node->children_[TG_NAME]->str_value_,
                                     static_cast<int32_t>(node->children_[TG_NAME]->str_len_));
          if (OB_FAIL(create_tablegroup_stmt->set_tablegroup_name(tablegroup_name))) {
            SQL_RESV_LOG(WARN, "set tablegroup name failed", K(ret));
          } else {
            create_tablegroup_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    //nothing todo
  } else if (OB_ISNULL(node->children_[TABLEGROUP_OPTION])) {
    //nothing todo
  } else {
    if (OB_FAIL(resolve_tablegroup_option(create_tablegroup_stmt,
                                          node->children_[TABLEGROUP_OPTION]))) {
      LOG_WARN("fail to resolve tabelgroup option", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    //nothing todo
  } else if (OB_NOT_NULL(node->children_[PARTITION_OPTION])) {
    //ignore partition after 4.2
    LOG_USER_WARN(OB_NOT_SUPPORTED, "create tablegroup with partition");
  }
  // set default sharding attribute
  if (OB_SUCC(ret)) {
    ObTablegroupSchema &tablegroup_schema = create_tablegroup_stmt->get_create_tablegroup_arg().tablegroup_schema_;
    ObString sharding_default(OB_PARTITION_SHARDING_ADAPTIVE);
    if (tablegroup_schema.get_sharding().empty()) {
      if (OB_FAIL(tablegroup_schema.set_sharding(sharding_default))) {
        LOG_WARN("set_default_sharding fail", K(ret));
      }
    }
  }
  SQL_RESV_LOG(INFO, "resolve create tablegroup finish", K(ret), K(tablegroup_name));
  return ret;
}
