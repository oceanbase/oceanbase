/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TABLE_MULTI_BATCH_SERVICE_H
#define _OB_TABLE_MULTI_BATCH_SERVICE_H

#include "ob_table_multi_batch_common.h"

namespace oceanbase
{
namespace table
{

class ObTableMultiBatchService
{
public:
  static int execute(ObTableMultiBatchCtx &ctx,
                     const ObTableMultiBatchRequest &request,
                     ObTableMultiBatchResult &result);
private:
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_MULTI_BATCH_SERVICE_H */
