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

#ifndef OCEANBASE_UNITTEST_PL_OB_MOCK_PL_STMT_H_
#define OCEANBASE_UNITTEST_PL_OB_MOCK_PL_STMT_H_

#include "../../src/pl/ob_pl_code_generator.h"

using namespace oceanbase;

namespace test
{
class TestPLStmtMockService
{
public:
  TestPLStmtMockService(common::ObIAllocator &allocator) :
    allocator_(allocator),
    expr_factory_(allocator_),
    stmt_factory_(allocator_),
    cur_loc_() {}
  virtual ~TestPLStmtMockService() {}

public:
  pl::ObPLStmt *make_stmt(pl::ObPLStmtType type, pl::ObPLStmtBlock *block);
  pl::ObPLStmtBlock *make_block(pl::ObPLBlockNS *pre_ns,
                                pl::ObPLSymbolTable *symbol_table,
                                pl::ObPLLabelTable *label_table,
                                pl::ObPLConditionTable *condition_table,
                                pl::ObPLCursorTable *cursor_table,
                                common::ObIArray<ObRawExpr*> *exprs,
                                pl::ObPLExternalNS *external_ns);

public:
  common::ObIAllocator &allocator_;
  sql::ObRawExprFactory expr_factory_;
  pl::ObPLStmtFactory stmt_factory_;
  pl::SourceLocation cur_loc_;
};

}



#endif /* OCEANBASE_UNITTEST_PL_OB_MOCK_PL_STMT_H_ */
