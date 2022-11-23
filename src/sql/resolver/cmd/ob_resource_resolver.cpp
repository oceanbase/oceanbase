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

#define USING_LOG_PREFIX SQL_RESV

#include "ob_resource_resolver.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObCreateResourcePoolResolver::ObCreateResourcePoolResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObCreateResourcePoolResolver::~ObCreateResourcePoolResolver()
{
}

ObSplitResourcePoolResolver::ObSplitResourcePoolResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObSplitResourcePoolResolver::~ObSplitResourcePoolResolver()
{
}

ObMergeResourcePoolResolver::ObMergeResourcePoolResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObMergeResourcePoolResolver::~ObMergeResourcePoolResolver()
{
}

ObAlterResourcePoolResolver::ObAlterResourcePoolResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObAlterResourcePoolResolver::~ObAlterResourcePoolResolver()
{
}

ObDropResourcePoolResolver::ObDropResourcePoolResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObDropResourcePoolResolver::~ObDropResourcePoolResolver()
{
}

ObAlterResourceTenantResolver::ObAlterResourceTenantResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObAlterResourceTenantResolver::~ObAlterResourceTenantResolver()
{
}

ObCreateResourceUnitResolver::ObCreateResourceUnitResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObCreateResourceUnitResolver::~ObCreateResourceUnitResolver()
{
}

ObAlterResourceUnitResolver::ObAlterResourceUnitResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObAlterResourceUnitResolver::~ObAlterResourceUnitResolver()
{
}

ObDropResourceUnitResolver::ObDropResourceUnitResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObDropResourceUnitResolver::~ObDropResourceUnitResolver()
{
}

int ObCreateResourcePoolResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateResourcePoolStmt *mystmt = NULL;

  if (OB_ISNULL(node)
      || T_CREATE_RESOURCE_POOL != node->type_
      || OB_UNLIKELY(3 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid parse node", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCreateResourcePoolStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create select stmt", K(ret));
    } else {
      stmt_ = mystmt;
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      if (OB_UNLIKELY(T_IF_NOT_EXISTS != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", "type", node->children_[0]->type_);
      } else {
        mystmt->set_if_not_exist(true);
      }
    } else {
      mystmt->set_if_not_exist(false);
    }
  }

  if (OB_SUCC(ret)) {
    ObString res_pool_name;
    if (OB_ISNULL(node->children_[1]) || OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", "child", node->children_[1]);
    } else {
      res_pool_name.assign_ptr((char *)(node->children_[1]->str_value_),
                               static_cast<int32_t>(node->children_[1]->str_len_));
      mystmt->set_resource_pool_name(res_pool_name);
    }
  }

  /* options */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[2]) {
      if (T_RESOURCE_POOL_OPTION_LIST != node->children_[2]->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node type", "child", node->children_[2]->type_);
      } else {
        ObResourcePoolOptionResolver<ObCreateResourcePoolStmt> opt_resolver;
        ret = opt_resolver.resolve_options(mystmt, node->children_[2]);
      }
    }
  }

  return ret;
}

int ObSplitResourcePoolResolver::resolve_split_pool_list(
    ObSplitResourcePoolStmt *stmt,
    const ParseNode &parse_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(T_RESOURCE_POOL_LIST != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret), "node_type", parse_node.type_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_node.num_child_; ++i) {
      ParseNode *element = parse_node.children_[i];
      if (OB_UNLIKELY(NULL == element)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else if (OB_UNLIKELY(T_VARCHAR != element->type_)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else {
        common::ObString pool_str(element->str_len_, element->str_value_);
        if (OB_FAIL(stmt->add_split_pool(pool_str))) {
          LOG_WARN("fail to add split pool", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSplitResourcePoolResolver::resolve_corresponding_zone_list(
    ObSplitResourcePoolStmt *stmt,
    const ParseNode &parse_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(T_ZONE_LIST != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret), "node_type", parse_node.type_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_node.num_child_; ++i) {
      ParseNode *element = parse_node.children_[i];
      if (OB_UNLIKELY(NULL == element)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else if (OB_UNLIKELY(T_VARCHAR != element->type_)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else {
        common::ObString zone_str(element->str_len_, element->str_value_);
        if (OB_FAIL(stmt->add_corresponding_zone(zone_str))) {
          LOG_WARN("fail to add corresponding zone", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSplitResourcePoolResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObSplitResourcePoolStmt *mystmt = NULL;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_SPLIT_RESOURCE_POOL != node->type_)
      || OB_UNLIKELY(3 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", K(node));
  } else if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObSplitResourcePoolStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create split resource pool stmt", K(ret));
  } else {
    stmt_ = mystmt;
  }
  // resource pool name
  if (OB_SUCC(ret)) {
    ObString resource_pool_name;
    if (OB_UNLIKELY(NULL == node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node child 0 ptr is null", K(ret));
    } else if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node", K(ret), "node_type", node->children_[0]->type_);
    } else {
      resource_pool_name.assign_ptr((char *)(node->children_[0]->str_value_),
                                    static_cast<int32_t>(node->children_[0]->str_len_));
      mystmt->set_resource_pool_name(resource_pool_name);
    }
  }
  // split into new resource pools
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node child 1 ptr is null", K(ret));
    } else if (OB_UNLIKELY(T_RESOURCE_POOL_LIST != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node", K(ret), "node_type", node->children_[1]->type_);
    } else if (OB_FAIL(resolve_split_pool_list(mystmt, *node->children_[1]))) {
      LOG_WARN("fail to resolve split pool list", K(ret));
    }
  }
  // corresponding zones
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == node->children_[2])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node child 2 ptr is null", K(ret));
    } else if (OB_UNLIKELY(T_ZONE_LIST != node->children_[2]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node", K(ret), "node_type", node->children_[2]->type_);
    } else if (OB_FAIL(resolve_corresponding_zone_list(mystmt, *node->children_[2]))) {
      LOG_WARN("fail to resolve corresponding zone list", K(ret));
    }
  }
  return ret;
}

int ObAlterResourceTenantResolver::resolve_resource_tenant_name(
    ObAlterResourceTenantStmt *stmt,
    const ParseNode *parse_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(stmt));
  } else if (OB_UNLIKELY(T_IDENT != parse_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node type", KR(ret), "node_type", parse_node->type_);
  } else {
    ObString tenant_name;
    tenant_name.assign_ptr((char *)(parse_node->str_value_),
                           static_cast<int32_t>(parse_node->str_len_));
    stmt->set_tenant_name(tenant_name);
  }
  return ret;
}

int ObAlterResourceTenantResolver::resolve_new_unit_num(
    ObAlterResourceTenantStmt *stmt,
    const ParseNode *parse_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(stmt));
  } else if (OB_UNLIKELY(T_INT != parse_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node type", KR(ret));
  } else {
    const int64_t unit_num = static_cast<int64_t>(parse_node->value_);
    stmt->set_unit_num(unit_num);
  }
  return ret;
}

int ObAlterResourceTenantResolver::resolve_unit_group_id_list(
    ObAlterResourceTenantStmt *stmt,
    const ParseNode *parse_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(stmt));
  } else if (OB_UNLIKELY(T_UNIT_GROUP_ID_LIST != parse_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node type", KR(ret));
  } else if (OB_UNLIKELY(parse_node->num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num unexpected", KR(ret), "children_num", parse_node->num_child_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_node->num_child_; ++i) {
      uint64_t unit_group_id = parse_node->children_[i]->value_;
      if (OB_FAIL(stmt->fill_unit_group_id(unit_group_id))) {
        LOG_WARN("fail to fill delete unit group id", KR(ret));
      }
    }
  }
  return ret;
}

int ObAlterResourceTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = &parse_tree;
  ObAlterResourceTenantStmt *mystmt = NULL;

  if (OB_UNLIKELY(nullptr == node
                  || T_ALTER_RESOURCE_TENANT != node->type_
                  || 3 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", K(node));
  } else if (OB_UNLIKELY(nullptr == (mystmt = create_stmt<ObAlterResourceTenantStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create alter resource pool stmt", KR(ret));
  } else {
    stmt_ = mystmt;
  }

  // 1. resolve tenant name
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node child 0 ptr is null", KR(ret));
    } else if (OB_FAIL(resolve_resource_tenant_name(mystmt, node->children_[0]))) {
      LOG_WARN("fail to resolve resource tenant name", KR(ret));
    }
  }
  // 2. resolve new unit num
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node child 1 ptr is null", KR(ret));
    } else if (OB_FAIL(resolve_new_unit_num(mystmt, node->children_[1]))) {
      LOG_WARN("fail to resolve new unit num", KR(ret));
    }
  }
  // 3. resolve unit_group id
  if (OB_SUCC(ret)) {
    if (nullptr == node->children_[2]) {
      // bypass, unit_group id is not specified
    } else if (OB_FAIL(resolve_unit_group_id_list(mystmt, node->children_[2]))) {
      LOG_WARN("fail to resolve unit group id", KR(ret));
    }
  }
  return ret;
}

int ObMergeResourcePoolResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObMergeResourcePoolStmt *mystmt = NULL;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_MERGE_RESOURCE_POOL != node->type_)
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", K(node));
  } else if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObMergeResourcePoolStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create merge resource pool stmt", K(ret));
  } else {
    stmt_ = mystmt;
  }

  //to be merged resource pool
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node child 0 ptr is null", K(ret));
    } else if (OB_UNLIKELY(T_RESOURCE_POOL_LIST != node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node", K(ret), "node_type", node->children_[0]->type_);
    } else if (OB_FAIL(resolve_old_merge_pool_list(mystmt, *node->children_[0]))) {
      LOG_WARN("fail to resolve old merge pool list", K(ret));
    }
  }
  //merge new resource pool
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node child 1 ptr is null", K(ret));
    } else if (OB_UNLIKELY(T_RESOURCE_POOL_LIST != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node", K(ret), "node_type", node->children_[1]->type_);
    } else if (OB_FAIL(resolve_new_merge_pool_list(mystmt, *node->children_[1]))) {
      LOG_WARN("fail to resolve new merge pool list", K(ret));
    }
  }
  return ret;
}

int ObMergeResourcePoolResolver::resolve_old_merge_pool_list(
    ObMergeResourcePoolStmt *stmt,
    const ParseNode &parse_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(T_RESOURCE_POOL_LIST != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret), "node_type", parse_node.type_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_node.num_child_; ++i) {
      ParseNode *element = parse_node.children_[i];
      if (OB_UNLIKELY(NULL == element)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else if (OB_UNLIKELY(T_VARCHAR != element->type_)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else {
        common::ObString pool_str(element->str_len_, element->str_value_);
        if (OB_FAIL(stmt->add_old_pool(pool_str))) {
          LOG_WARN("fail to add old pool", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMergeResourcePoolResolver::resolve_new_merge_pool_list(
    ObMergeResourcePoolStmt *stmt,
    const ParseNode &parse_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(T_RESOURCE_POOL_LIST != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret), "node_type", parse_node.type_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_node.num_child_; ++i) {
      ParseNode *element = parse_node.children_[i];
      if (OB_UNLIKELY(NULL == element)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else if (OB_UNLIKELY(T_VARCHAR != element->type_)) {
        ret = common::OB_ERR_PARSER_SYNTAX;
        LOG_WARN("unexpected zone node", K(ret));
      } else {
        common::ObString pool_str(element->str_len_, element->str_value_);
        if (OB_FAIL(stmt->add_new_pool(pool_str))) {
          LOG_WARN("fail to add new pool", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterResourcePoolResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObAlterResourcePoolStmt *mystmt = NULL;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_ALTER_RESOURCE_POOL != node->type_)
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", K(node));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObAlterResourcePoolStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create select stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* res pool name */
  if (OB_SUCC(ret)) {
    ObString res_pool_name;
    if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", K(ret));
    } else {
      res_pool_name.assign_ptr((char *)(node->children_[0]->str_value_),
                               static_cast<int32_t>(node->children_[0]->str_len_));
      mystmt->set_resource_pool_name(res_pool_name);
    }
  }

  /* options */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[1]) {
      if (OB_UNLIKELY(T_RESOURCE_POOL_OPTION_LIST != node->children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", K(ret));
      } else {
        ObResourcePoolOptionResolver<ObAlterResourcePoolStmt> opt_resolver;
        ret = opt_resolver.resolve_options(mystmt, node->children_[1]);
      }
    }
  }

  return ret;
}

int ObDropResourcePoolResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObDropResourcePoolStmt *mystmt = NULL;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_DROP_RESOURCE_POOL != node->type_)
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", K(node));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObDropResourcePoolStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* resource pool name */
  if (OB_SUCC(ret)) {
    if (node->children_[0] != NULL) {
      if (OB_UNLIKELY(T_IF_EXISTS != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", K(ret));
      } else {
        mystmt->set_if_exist(true);
      }
    } else {
      mystmt->set_if_exist(false);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", K(ret));
    } else {
      ObString res_pool_name;
      res_pool_name.assign_ptr((char *)(node->children_[1]->str_value_),
                               static_cast<int32_t>(node->children_[1]->str_len_));
      mystmt->set_resource_pool_name(res_pool_name);
    }
  }

  return ret;
}

int ObCreateResourceUnitResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateResourceUnitStmt *mystmt = NULL;
  ObUnitResource ur;
  bool if_not_exist = false;
  ObString res_unit_name;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_CREATE_RESOURCE_UNIT != node->type_)
      || OB_UNLIKELY(3 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", K(node));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCreateResourceUnitStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create select stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* res unit name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      if (OB_UNLIKELY(T_IF_NOT_EXISTS != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", K(ret));
      } else {
        if_not_exist = true;
      }
    } else {
      if_not_exist = false;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node");
    } else {
      res_unit_name.assign_ptr((char *)(node->children_[1]->str_value_),
                               static_cast<int32_t>(node->children_[1]->str_len_));
    }
  }

  /* options */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[2]) {
      if (OB_UNLIKELY(T_RESOURCE_UNIT_OPTION_LIST != node->children_[2]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", K(ret));
      } else {
        ObResourceUnitOptionResolver<ObCreateResourceUnitStmt> opt_resolver;
        ret = opt_resolver.resolve_options(node->children_[2], ur);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mystmt->init(res_unit_name, ur, if_not_exist))) {
      LOG_WARN("init ObCreateResourceUnitStmt fail", KR(ret), KPC(mystmt), K(res_unit_name),
          K(ur), K(if_not_exist));
    }
  }

  return ret;
}

int ObAlterResourceUnitResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObAlterResourceUnitStmt *mystmt = NULL;
  ObString res_unit_name;
  ObUnitResource ur;

  if (OB_ISNULL(node)
      || T_ALTER_RESOURCE_UNIT != node->type_
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid parse node", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObAlterResourceUnitStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create alter resource unit stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* res unit name */
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", K(ret));
    } else {
      res_unit_name.assign_ptr((char *)(node->children_[0]->str_value_),
                               static_cast<int32_t>(node->children_[0]->str_len_));
    }
  }

  /* options */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[1]) {
      if (OB_UNLIKELY(T_RESOURCE_UNIT_OPTION_LIST != node->children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", K(ret));
      } else {
        ObResourceUnitOptionResolver<ObAlterResourceUnitStmt> opt_resolver;
        ret = opt_resolver.resolve_options(node->children_[1], ur);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mystmt->init(res_unit_name, ur))) {
      LOG_WARN("init ObAlterResourceUnitStmt fail", KR(ret), KPC(mystmt), K(res_unit_name),
          K(ur));
    }
  }

  return ret;
}

int ObDropResourceUnitResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObDropResourceUnitStmt *mystmt = NULL;

  if (OB_ISNULL(node)
      || T_DROP_RESOURCE_UNIT != node->type_
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid parse node", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObDropResourceUnitStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  if (OB_SUCC(ret)) {
    if (node->children_[0] != NULL) {
      if (OB_UNLIKELY(T_IF_EXISTS != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", K(ret));
      } else {
        mystmt->set_if_exist(true);
      }
    } else {
      mystmt->set_if_exist(false);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", K(ret));
    } else {
      ObString res_unit_name;
      res_unit_name.assign_ptr((char *)(node->children_[1]->str_value_),
                               static_cast<int32_t>(node->children_[1]->str_len_));
      mystmt->set_resource_unit_name(res_unit_name);
    }
  }

  return ret;
}

