/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
