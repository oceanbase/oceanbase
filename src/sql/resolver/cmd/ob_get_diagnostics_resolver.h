/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
