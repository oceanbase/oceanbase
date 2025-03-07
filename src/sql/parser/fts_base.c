/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "fts_base.h"

static int fts_node_create(FtsParserResult *arg, FtsNode **node)
{
  int ret = FTS_OK;
  *node = (FtsNode *)parse_malloc(sizeof(FtsNode), arg->malloc_pool_);
  if (NULL == *node) {
    ret = FTS_ERROR_MEMORY;
  }
  return ret;
}

int fts_create_node_list(
  void *arg,
  FtsNode *expr,
  FtsNode **list)
{
  int ret = FTS_OK;
  if (NULL == expr) {
  } else {
    ret = fts_node_create((FtsParserResult *)arg, list);
    if (FTS_OK == ret) {
      (*list)->type = FTS_NODE_LIST;
      (*list)->list.head = (*list)->list.tail = expr;
    }
  }
  return ret;
}

int fts_string_create(FtsParserResult *arg, const char *str, uint32_t len, FtsString **ast_str)
{
  int ret = FTS_OK;
  if (len <= 0) {
    ret = FTS_ERROR_OTHER;
  } else {
    *ast_str = (FtsString *)parse_malloc(sizeof(FtsString), arg->malloc_pool_);
    if (NULL == *ast_str) {
      ret = FTS_ERROR_MEMORY;
    } else {
      (*ast_str)->str_ = (char *)(parse_malloc(len + 1, arg->malloc_pool_));
      if (NULL == (*ast_str)->str_) {
        ret = FTS_ERROR_MEMORY;
      } else {
        (*ast_str)->len_ = len;
        memcpy((*ast_str)->str_, str, len);
        (*ast_str)->str_[len] = '\0';
      }
    }
  }
  return ret;
}

int fts_create_node_term(void *arg, const FtsString *term, FtsNode **node)
{
  int ret = FTS_OK;
  ret = fts_node_create((FtsParserResult *)arg, node);
  if (FTS_OK == ret) {
    (*node)->type = FTS_NODE_TERM;
    (*node)->term.str_ = term->str_;
    (*node)->term.len_ = term->len_;
  }
  return ret;
}

int fts_create_node_oper(
  void *arg,
  enum FtsOperType oper,
  FtsNode **node)
{
  int ret = FTS_OK;
  ret = fts_node_create((FtsParserResult *)arg, node);
  if (FTS_OK == ret) {
    (*node)->type = FTS_NODE_OPER;
    (*node)->oper = oper;
  }
  return ret;
}

int fts_add_node(
    FtsNode *node, /*!< in: list instance */
    FtsNode *elem) /*!< in: node to add to list */
{
  int ret = FTS_OK;
  if (NULL == elem) {
    // do nothing
  } else if (node->type != FTS_NODE_LIST && node->type != FTS_NODE_SUBEXP_LIST) {
    ret = FTS_ERROR_OTHER;
  } else if (NULL == node->list.head) {
    if (node->list.tail) {
      ret = FTS_ERROR_OTHER;
    } else {
      node->list.head = node->list.tail = elem;
    }
  } else {
    if (NULL == node->list.tail) {
      ret = FTS_ERROR_OTHER;
    } else {
      node->list.tail->next = elem;
      node->list.tail = elem;
    }
  }
  return ret;
}

int fts_create_node_subexp_list(void *arg, FtsNode *expr, FtsNode **node)
{
  int ret = FTS_OK;
  ret = fts_node_create((FtsParserResult *)arg, node);

  if (FTS_OK == ret) {
    (*node)->type = FTS_NODE_SUBEXP_LIST;
    (*node)->list.head = (*node)->list.tail = expr;
  }
  return ret;
}
