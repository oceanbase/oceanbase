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

#ifndef OB_DAS_TEXT_RETRIEVAL_EVAL_NODE_H_
#define OB_DAS_TEXT_RETRIEVAL_EVAL_NODE_H_

#include "ob_das_iter.h"
#include "sql/parser/fts_base.h"
#include "sql/parser/fts_parse.h"

namespace oceanbase
{
namespace sql
{
struct ObFtsEvalNode
{
public:
  enum FtsComputeFlag
  {
    AND = 0, // +
    NOT = 1, // -
    OR = 2,  // ()
    NO_OPERATOR = 3,
    MORE = 4, // >
    LESS = 5, // <
    NEGATIVE = 6, // ～
    INVALID = 7,
  };
  ObFtsEvalNode() : leaf_node_(false), postion_(-1) {}
  ~ObFtsEvalNode() {}

  void release() {
    parant_node_ = nullptr;
    for (int64_t i = 0; i < child_nodes_.count(); i++) {
      if (child_nodes_.at(i) != nullptr) {
        child_nodes_.at(i)->release();
      }
    }
    child_nodes_.reset();
    child_flags_.reset();
    leaf_node_ = false;
    postion_ = -1;
  }

  VIRTUAL_TO_STRING_KV(K_(leaf_node), K_(postion));
public:
  static int print_node(ObFtsEvalNode *node, const ObArray<ObString> &tokens, int level);
  static int fts_boolean_eval(ObFtsEvalNode *node, const common::ObIArray<double> &relevences, double &result);
  static int fts_boolean_node_create(ObFtsEvalNode *&parant_node, const FtsNode *cur_node, ObIAllocator &allocator, ObArray<ObString> &tokens, hash::ObHashMap<ObString, int32_t> &tokens_map);
  ObFtsEvalNode *parant_node_;
  ObArray<ObFtsEvalNode *> child_nodes_;
  ObArray<FtsComputeFlag> child_flags_;
  bool leaf_node_;
  int32_t postion_;  // the position of the cur node in the tokens array
};

} // namespace sql
} // namespace oceanbase

#endif // OB_DAS_TEXT_RETRIEVAL_EVAL_NODE_H_
