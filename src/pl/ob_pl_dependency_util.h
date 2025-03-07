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

#ifndef OCEANBASE_SRC_PL_OB_PL_DEPENDENCY_MANAGER_H_
#define OCEANBASE_SRC_PL_OB_PL_DEPENDENCY_MANAGER_H_

#include "share/ob_define.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "pl/ob_pl_stmt.h"

namespace oceanbase
{

namespace pl
{

class ObPLDependencyUtil
{
public:
  ObPLDependencyUtil() {}
  virtual ~ObPLDependencyUtil() {}

  static int get_synonym_object(uint64_t tenant_id,
                                uint64_t &owner_id,
                                ObString &object_name,
                                bool &exist,
                                sql::ObSQLSessionInfo &session_info,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                ObIArray<share::schema::ObSchemaObjVersion> *deps);
  static int collect_synonym_deps(uint64_t tenant_id,
                                  ObSynonymChecker &synonym_checker,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  ObIArray<ObSchemaObjVersion> *deps);
  static int add_dependency_objects(const ObPLDependencyTable *dep_tbl,
                                    const ObIArray<ObSchemaObjVersion> &dependency_objects);
  static int add_dependency_objects(ObPLDependencyTable &dep_tbl,
                                    const ObPLResolveCtx &resolve_ctx,
                                    const ObPLDataType &type);
  static int add_dependency_object_impl(const ObPLDependencyTable *dep_tbl,
                                        const share::schema::ObSchemaObjVersion &obj_version);
  static int add_dependency_object_impl(ObPLDependencyTable &dep_tbl,
                             const share::schema::ObSchemaObjVersion &obj_version);
};

class ObPLDependencyGuard
{
public:
  ObPLDependencyGuard(const ObPLExternalNS *src_external_ns, const ObPLExternalNS *dst_external_ns)
  : old_external_ns_(nullptr), old_dependency_table_(nullptr) {
    if (OB_NOT_NULL(src_external_ns)
    && OB_NOT_NULL(dst_external_ns)
    && dst_external_ns->get_dependency_table() != src_external_ns->get_dependency_table()) {
      old_external_ns_ = const_cast<ObPLExternalNS *>(dst_external_ns);
      old_dependency_table_ = old_external_ns_->get_dependency_table();
      old_external_ns_->set_dependency_table(const_cast<ObPLExternalNS *>(src_external_ns)->get_dependency_table());
    }
  }
  ~ObPLDependencyGuard() {
    if (OB_NOT_NULL(old_external_ns_)) {
      old_external_ns_->set_dependency_table(old_dependency_table_);
    }
  }
private:
  ObPLExternalNS *old_external_ns_;
  ObPLDependencyTable *old_dependency_table_;
};

}
}

#endif