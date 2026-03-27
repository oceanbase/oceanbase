/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_PARAMETERS_TABLE_H_
#define OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_PARAMETERS_TABLE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_routine_info.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObInformationParametersTable : public common::ObVirtualTableScannerIterator
{
private:
  enum InformationParametersTableColumns {
    SPECIFIC_CATALOG = 16,
    SPECIFIC_SCHEMA,
    SPECIFIC_NAME,
    ORDINAL_POSITION,
    PARAMETER_MODE,
    PARAMETER_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    CHARACTER_OCTET_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    DATETIME_PRECISION,
    CHARACTER_SET_NAME,
    COLLATION_NAME,
    DTD_IDENTIFIER,
    ROUTINE_TYPE
  };
public:
  ObInformationParametersTable();
  virtual ~ObInformationParametersTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

private:
  int fill_row_cells(const share::schema::ObRoutineInfo *routine_info, const share::schema::ObRoutineParam *param_info, common::ObObj *&cells);

private:
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObInformationParametersTable);
};
}
}

#endif /* OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_PROC_TABLE_H_ */
