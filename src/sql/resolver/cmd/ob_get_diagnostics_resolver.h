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

#ifndef _OB_GET_DIAGNOSTICS_RESOLVER_H
#define _OB_GET_DIAGNOSTICS_RESOLVER_H
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/cmd/ob_get_diagnostics_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObGetDiagnosticsResolver : public ObStmtResolver
{
public:
    explicit ObGetDiagnosticsResolver(ObResolverParams &params);
    virtual ~ObGetDiagnosticsResolver();
    virtual int resolve(const ParseNode &parse_tree);
    int set_diagnostics_type(ObGetDiagnosticsStmt *diagnostics_stmt, const bool &is_current, const bool &is_cond);
private:
    class ObSqlStrGenerator;
    DISALLOW_COPY_AND_ASSIGN(ObGetDiagnosticsResolver);
};


}
}
#endif
