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

#define USING_LOG_PREFIX SERVER
#include "ob_table_multi_batch_service.h"
#include "ob_table_batch_service.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;

int ObTableMultiBatchService::execute(ObTableMultiBatchCtx &ctx,
                                      const ObTableMultiBatchRequest &request,
                                      ObTableMultiBatchResult &result)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableBatchOperation> &batch_ops = request.get_ops();
  const int64_t count = batch_ops.count();

  if (!request.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTableMultiBatchRequest is invalid", K(ret), K(request));
  } else if (result.empty() && OB_FAIL(result.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate results", K(ret), K(count));
  } else {
    ObIArray<ObTableBatchOperationResult> &batch_results = result.get_results();
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      // change ops and result each loop
      const ObIArray<ObTableOperation> &ops = batch_ops.at(i).get_table_operations();
      ObIArray<ObTableOperationResult> &results = batch_results.at(i);
      ctx.tablet_ids_.reset();
      if (OB_FAIL(ctx.tablet_ids_.push_back(request.get_tablet_ids().at(i)))) {
        LOG_WARN("fail to push back tablet id", K(ret), K(ctx));
      } else if (results.empty() && OB_FAIL(ObTableBatchService::prepare_results(ops,
                                                                                 ctx.entity_factory_,
                                                                                 results))) {
        LOG_WARN("fail to prepare results", K(ret), K(ops));
      } else if (OB_FAIL(ObTableBatchService::execute(ctx, ops, results))) {
        LOG_WARN("fail to execute batch operation", K(ret), K(ctx));
      }
    }
  }

  return ret;
}
