/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_PROVIDER_IMPORTER_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_PROVIDER_IMPORTER_H_

#include "share/table/ob_redis_importer.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace table
{

class ObAiProviderImporter
{
public:
  ObAiProviderImporter(uint64_t tenant_id, sql::ObExecContext &exec_ctx)
    : tenant_id_(tenant_id), exec_ctx_(exec_ctx), affected_rows_(0) {}
  int exec_op(ObModuleDataArg::ObInfoOpType op);

private:
  int check_basic_info(bool &need_import);
  int import_ai_provider_info();
  int check_ai_provider_info();

  uint64_t tenant_id_;
  sql::ObExecContext &exec_ctx_;
  int64_t affected_rows_;
};

} // namespace table
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_PROVIDER_IMPORTER_H_
