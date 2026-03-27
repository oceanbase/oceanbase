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
class ObTableLoadStoreCtx;

class ObTableLoadDag final : public storage::ObDDLIndependentDag
{
public:
  ObTableLoadDag();
  virtual ~ObTableLoadDag();
  int init(ObTableLoadStoreCtx *store_ctx);
  int start();
  int check_status();
  void clear_task();
  INHERIT_TO_STRING_KV("ObDDLIndependentDag", ObDDLIndependentDag, KP_(store_ctx));

private:
  int init_by_param(const share::ObIDagInitParam *param) override { return OB_SUCCESS; }
  bool is_scan_finished() override { return true; }
  bool use_tablet_mode() const override { return true; }
  class StartDagProcessor;
  class StartDagCallback;

public:
  ObTableLoadStoreCtx *store_ctx_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
