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

#include <unordered_map>
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
  static int process(ObTableGroupCtx &ctx, ObITableOp *op, bool is_direct_execute = false);
  static int process_trigger();
  static int process_one_by_one(ObTableGroup &group);
private:
  static int add_and_try_to_get_batch(ObITableOp *op, ObITableGroupValue *group, ObIArray<ObITableOp *> &ops);
  static int execute_batch(ObTableGroupCtx &ctx,
                           ObIArray<ObITableOp *> &ops,
                           bool is_direct_execute,
                          bool add_fail_group);
  static int process_failed_group();
  static int process_other_group();
  static int process_expired_group();
  static int check_legality(const ObTableGroupCtx &ctx, const ObITableGroupKey *key, const ObITableOp *op);
  static int start_trans(ObTableBatchCtx &batch_ctx);
  static int end_trans(const ObTableBatchCtx &batch_ctx,
                       ObTableGroupCommitEndTransCb *cb,
                       bool is_rollback);
  static int init_table_ctx(ObTableGroup &group, ObTableCtx &tb_ctx);
  static int init_batch_ctx(ObTableGroup &group, ObTableBatchCtx &batch_ctx);
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_SERVICE_H_ */
