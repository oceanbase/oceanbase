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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/engine/ob_exec_context.h"
#include "ob_das_text_retrieval_eval_node.h"

namespace oceanbase
{
namespace sql
{
int ObFtsEvalNode::print_node(ObFtsEvalNode *node, const ObArray<ObString> &tokens, int level)
{
  int ret = OB_SUCCESS;
  if (!node) {
  } else if (node->leaf_node_) {
    LOG_WARN("fts compute leaf node:", K(level), K(node->postion_), K(tokens.at(node->postion_)));
  } else {
    for (int i = 0; i < node->child_nodes_.count(); i++) {
      LOG_WARN("fts compute children node:", K(level), K(node->child_flags_[i]));
      print_node(node->child_nodes_[i], tokens, level + 1);
    }
  }
  return ret;
}

int ObFtsEvalNode::fts_boolean_eval(ObFtsEvalNode *node, const common::ObIArray<double> &relevences, double &result)
{
  int ret = OB_SUCCESS;
  if (node->leaf_node_) {
    if (OB_UNLIKELY(relevences.count() <= node->postion_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected relevence size", K(ret), K(relevences.count()), K(node->postion_));
    } else {
      result = relevences.at(node->postion_);
    }
  } else {
    result = 0;
    int32_t size = node->child_nodes_.count();
    bool maybe = false;
    bool must = true;
    bool must_not = false;
    for (size_t i = 0; OB_SUCC(ret) && i < size; i++) {
      double relevence = 0;
      if (OB_FAIL(fts_boolean_eval(node->child_nodes_[i], relevences, relevence))) {
        LOG_WARN("failed to get relevence", K(ret));
      } else {
        switch (node->child_flags_[i])
        {
        case AND:
          must = relevence > 0 ? must : false;
          result = relevence <= 0 ? result : result + relevence;
          break;
        case NOT:
          must_not = relevence > 0 ? true : must_not;
          break;
        case OR:
          maybe = relevence <= 0 ? maybe : true;
          result = relevence <= 0 ? result : result + relevence;
          break;
        case NO_OPERATOR:
          result = relevence <= 0 ? result : result + relevence;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected fts compute flag", K(node->child_flags_[i]));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (maybe) {
        // do nothing
      } else if (must_not) {
        result = -1;
      } else if (must) {
        // do nothing
      } else {
        result = -1;
      }
    }
  }
  return ret;
}

int ObFtsEvalNode::fts_boolean_node_create(ObFtsEvalNode *&parant_node, const FtsNode *cur_node, ObIAllocator &allocator, ObArray<ObString> &tokens, hash::ObHashMap<ObString, int32_t> &tokens_map) // TODO: tokens maybe repeat
{
  int ret = OB_SUCCESS;
  ObFtsEvalNode *node = nullptr;
  if (cur_node == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fts compute node");
  } else if (parant_node != nullptr) {
    FtsNode *head = cur_node->list.head;
    FtsNode *tail = cur_node->list.tail;
    if (FTS_NODE_OPER != head->type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected fts compute flag", K(head->type));
    } else {
      FtsComputeFlag flag;
      switch (head->oper)
      {
      case FTS_IGNORE:
        flag = NOT;
        break;
      case FTS_EXIST:
        flag = AND;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fts compute flag", K(head->oper));
        break;
      }
      if (OB_FAIL(parant_node->child_flags_.push_back(flag))) {
        LOG_WARN("failed to append flag", K(ret));
      } else if (OB_FAIL(fts_boolean_node_create(node, tail, allocator, tokens, tokens_map))) {
        LOG_WARN("failed to create fts compute node", K(ret));
      } else if (OB_FAIL(parant_node->child_nodes_.push_back(node))) {
        LOG_WARN("failed to append node", K(ret));
      } else {
        node = nullptr;
      }
    }
  } else if (FTS_NODE_SUBEXP_LIST == cur_node->type || FTS_NODE_LIST == cur_node->type) {
    void *buf = nullptr;
    ObFtsEvalNode *re_node = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObFtsEvalNode)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      re_node = new(buf) ObFtsEvalNode();
      re_node->leaf_node_ = false;
      re_node->parant_node_ = nullptr;
    }
    FtsNode *head = cur_node->list.head;
    FtsNode *tail = cur_node->list.tail;
    FtsNode *feak_head = static_cast<FtsNode *>(allocator.alloc(sizeof(FtsNode)));
    feak_head->next = head;
    if (OB_SUCC(ret) && (OB_ISNULL(head) || OB_ISNULL(tail))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected fts compute node");
    }
    while (OB_SUCC(ret) && feak_head != tail) {
      feak_head = feak_head->next;
      if (FTS_NODE_TERM == feak_head->type) {
        if (OB_FAIL(re_node->child_flags_.push_back(NO_OPERATOR))) {
          LOG_WARN("failed to append flag", K(ret));
        } else if (OB_FAIL(fts_boolean_node_create(node, feak_head, allocator, tokens, tokens_map))) {
          LOG_WARN("failed to create fts compute node", K(ret));
        } else if (OB_FAIL(re_node->child_nodes_.push_back(node))) {
          LOG_WARN("failed to append node", K(ret));
        } else {
          node = nullptr;
        }
      } else if (FTS_NODE_OPER == feak_head->type) {
        FtsComputeFlag flag;
        switch (feak_head->oper)
        {
        case FTS_IGNORE:
          flag = NOT;
          break;
        case FTS_EXIST:
          flag = AND;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected fts compute flag", K(cur_node->oper));
          break;
        }
        if (OB_FAIL(re_node->child_flags_.push_back(flag))) {
          LOG_WARN("failed to append flag", K(ret));
        } else {
          feak_head = feak_head->next;
          if (OB_SUCC(ret) && feak_head != tail) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected fts compute flag", K(feak_head->type));
          } else if (FTS_NODE_LIST == feak_head->type) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected fts compute node type", K(feak_head->type));
          } else if (FTS_NODE_TERM == feak_head->type || FTS_NODE_SUBEXP_LIST == feak_head->type) {
            if (OB_FAIL(fts_boolean_node_create(node, feak_head, allocator, tokens, tokens_map))) {
              LOG_WARN("failed to create fts compute node", K(ret));
            } else if (OB_FAIL(re_node->child_nodes_.push_back(node))) {
              LOG_WARN("failed to append node", K(ret));
            } else {
              node = nullptr;
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected fts compute flag", K(feak_head->type));
          }
        }
      } else if (FTS_NODE_SUBEXP_LIST == feak_head->type || FTS_NODE_SUBEXP_LIST == cur_node->type) {
        if (OB_FAIL(fts_boolean_node_create(node, feak_head, allocator, tokens, tokens_map))) {
          LOG_WARN("failed to create fts compute node", K(ret));
        } else if (OB_FAIL(re_node->child_flags_.push_back(OR))) {
          LOG_WARN("failed to append flag", K(ret));
        } else if (OB_FAIL(re_node->child_nodes_.push_back(node))) {
          LOG_WARN("failed to append node", K(ret));
        } else {
          node = nullptr;
        }
      } else if (FTS_NODE_LIST == feak_head->type) {
        if (OB_FAIL(fts_boolean_node_create(re_node, feak_head, allocator, tokens, tokens_map))) {
          LOG_WARN("failed to create fts compute node", K(ret));
        } else {
          node = nullptr;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fts compute flag", K(feak_head->type));
      }
    }
    parant_node = re_node;
  } else if (cur_node->type == FTS_NODE_TERM) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObFtsEvalNode)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      node = new(buf) ObFtsEvalNode();
    }
    ObString tmp_string(cur_node->term.len_, cur_node->term.str_);
    ObString token_string;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(allocator, tmp_string, token_string))) {
      LOG_WARN("failed to deep copy query token", K(ret));
    } else {
      int32_t token_idx = 0;
      int32_t map_size = tokens_map.size();
      if (OB_FAIL(tokens_map.get_refactored(token_string, token_idx))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get relevance", K(ret), K(token_idx));
        } else if (OB_FAIL(tokens_map.set_refactored(token_string, map_size, 1/*overwrite*/))) {
          LOG_WARN("failed to push data", K(ret), K(token_string));
        } else if (OB_FAIL(tokens.push_back(token_string))) {
          LOG_WARN("failed to append query token", K(ret));
        } else {
          token_idx = map_size;
          ret = OB_SUCCESS;
        }
      }
      node->postion_ = token_idx;
      node->leaf_node_ = true;
      parant_node = node;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fts compute flag", K(cur_node->type));
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
