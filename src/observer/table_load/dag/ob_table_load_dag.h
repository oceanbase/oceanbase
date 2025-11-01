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
