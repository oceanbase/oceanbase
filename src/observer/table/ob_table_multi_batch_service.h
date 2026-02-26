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
