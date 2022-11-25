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

#include "test_mock_pl_stmt.h"

using namespace oceanbase::pl;

namespace test
{
ObPLStmt *TestPLStmtMockService::make_stmt(ObPLStmtType type, ObPLStmtBlock *block)
{
  ObPLStmt *stmt = stmt_factory_.allocate(type, block);
  ++cur_loc_.line_;
  stmt->set_location(cur_loc_);
  if (NULL != block) {
    block->add_stmt(stmt);
  }
  return stmt;
}

ObPLStmtBlock *TestPLStmtMockService::make_block(ObPLBlockNS *pre_ns,
                                                 ObPLSymbolTable *symbol_table,
                                                 ObPLLabelTable *label_table,
                                                 ObPLConditionTable *condition_table,
                                                 ObPLCursorTable *cursor_table,
                                                 common::ObIArray<ObRawExpr*> *exprs,
                                                 ObPLExternalNS *external_ns)
{
  ObPLStmtBlock *block = static_cast<ObPLStmtBlock*>(allocator_.alloc(sizeof(ObPLStmtBlock)));
  block = new(block)ObPLStmtBlock(allocator_, pre_ns, symbol_table, label_table, condition_table, cursor_table, NULL, exprs, external_ns);
  return block;
}

}

