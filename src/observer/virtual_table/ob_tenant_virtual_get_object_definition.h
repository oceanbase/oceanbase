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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_GET_OBJECT_DEFINITION_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_GET_OBJECT_DEFINITION_

#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace observer
{
class ObGetObjectDefinition : public common::ObVirtualTableScannerIterator
{
  enum GetDDLObjectType
  {
    T_GET_DDL_TABLE,            // table, view, index
    T_GET_DDL_PROCEDURE,        // procedure, function
    T_GET_DDL_PACKAGE,          // package, package spec, package body
    T_GET_DDL_CONSTRAINT,       // check/pk constraint
    T_GET_DDL_REF_CONSTRAINT,   // foreign key constraint
    T_GET_DDL_TABLESPACE,       // table space
    T_GET_DDL_SEQUENCE,         // sequence
    T_GET_DDL_TRIGGER,          // trigger
    T_GET_DDL_USER,             // user
    T_GET_DDL_SYNONYM,          // synonym
    T_GET_DDL_TYPE,             // user defined type
    T_GET_DDL_TYPE_SPEC,        // user defined type spec
    T_GET_DDL_TYPE_BODY,        // user defined type body
    T_GET_DDL_ROLE,
    T_GET_DDL_MAX
  };
  static const char *ObjectTypeName[T_GET_DDL_MAX];
public:
  ObGetObjectDefinition();
  virtual ~ObGetObjectDefinition();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int get_object_type_and_name(GetDDLObjectType &object_type,
                              ObString &object_name,
                              ObString &ob_schema,
                              ObString &version,
                              ObString &model,
                              ObString &transform);
  int get_ddl_creation_str(ObString &ddl_str,
                          GetDDLObjectType object_type,
                          const ObString &object_name,
                          const ObString &db_name);
  OB_INLINE int print_error_log(GetDDLObjectType object_type,
                                const common::ObString &db_name,
                                const common::ObString &object_name);
  int fill_row_cells(ObString &ddl_str,
                    int64_t object_type,
                    ObString &object_name,
                    ObString &ob_schema,
                    ObString &version,
                    ObString &model,
                    ObString &transform);

  int get_table_definition(ObString &ddl_str, const uint64_t table_id);
  int get_procedure_definition(ObString &ddl_str, const uint64_t table_id);
  int get_constraint_definition(ObString &ddl_str, const ObString &constraint_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type);
  int get_foreign_key_definition(ObString &ddl_str, const ObString &foreign_key_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type);
  int get_tablespace_definition(ObString &ddl_str, const ObString &tablespace_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type);
  int get_sequence_definition(ObString &ddl_str, const ObString &sequence_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type);
  int get_trigger_definition(ObString &ddl_str, const ObString &trigger_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type);
  int get_user_definition(ObString &ddl_str, const ObString &user_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type,
                                bool is_role);
  int get_synonym_definition(ObString &ddl_str, const ObString &synonym_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type);
  int get_udt_definition(ObString &ddl_str, const ObString &udt_name,
                                const ObString &db_name,
                                GetDDLObjectType object_type);
  int get_database_id(uint64_t tenant_id, const ObString db_name,
                      uint64_t &database_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetObjectDefinition);
};
}// observer
}// oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_GET_OBJECT_DEFINITION_ */
