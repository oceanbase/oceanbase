/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_RESTORE_JOB_H_
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_RESTORE_JOB_H_

#include "observer/virtual_table/ob_iterate_private_virtual_table.h"

namespace oceanbase
{
namespace observer
{

// Redacts monitoring output for tenant and table restore jobs and their history.
class ObAllVirtualRestoreJob : public ObIteratePrivateVirtualTable
{
public:
  ObAllVirtualRestoreJob() = default;
  virtual ~ObAllVirtualRestoreJob() = default;
  virtual int try_convert_row(
      const common::ObNewRow *input_row,
      common::ObNewRow *&row) override;

private:
  virtual int construct_columns(
      const uint64_t exec_tenant_id,
      common::ObSqlString &sql) override;
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_RESTORE_JOB_H_
