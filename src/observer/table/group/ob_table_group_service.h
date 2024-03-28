/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_GROUP_SERVICE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_GROUP_SERVICE_H_

#include "ob_table_tenant_group.h"
#include "ob_table_group_execute.h"
#include "observer/table/ob_table_batch_service.h"

namespace oceanbase
{

namespace table
{

class ObTableGroupService final
{
public:
  static int process(const ObTableGroupCtx &ctx, ObTableGroupCommitSingleOp *op);
  static int process_trigger();
private:
  static int process_one_by_one(ObTableGroupCommitOps &group);
  static int process_failed_group();
  static int process_other_group();
  static int check_legality(const ObTableGroupCtx &ctx);
  static int start_trans(ObTableBatchCtx &batch_ctx);
  static int end_trans(const ObTableBatchCtx &batch_ctx,
                       ObTableGroupCommitEndTransCb *cb,
                       bool is_rollback);
  static int init_table_ctx(ObTableGroupCommitOps &group, ObTableCtx &tb_ctx);
  static int init_batch_ctx(ObTableGroupCommitOps &group, ObTableBatchCtx &batch_ctx);
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_SERVICE_H_ */
