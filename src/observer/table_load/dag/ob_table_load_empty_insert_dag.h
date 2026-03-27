/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage/ddl/ob_ddl_independent_dag.h"

namespace oceanbase
{
namespace observer
{

using ObTableLoadEmptyInsertDagInitParam = storage::ObDDLIndependentDagInitParam;

class ObTableLoadEmptyInsertDag final : public storage::ObDDLIndependentDag
{
public:
  ObTableLoadEmptyInsertDag() = default;
  virtual ~ObTableLoadEmptyInsertDag() = default;

private:
  int init_by_param(const share::ObIDagInitParam *param) override;
  bool is_scan_finished() override { return true; }
};

} // namespace observer
} // namespace oceanbase
