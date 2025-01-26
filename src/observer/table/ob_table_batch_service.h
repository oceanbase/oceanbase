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

#ifndef _OB_TABLE_BATCH_SERVICE_H
#define _OB_TABLE_BATCH_SERVICE_H

#include "ob_table_batch_common.h"

namespace oceanbase
{
namespace table
{

class ObTableBatchService
{
public:
  static int execute(ObTableBatchCtx &ctx,
                     const common::ObIArray<ObTableOperation> &ops,
                     common::ObIArray<ObTableOperationResult> &results);
  static int prepare_results(const common::ObIArray<ObTableOperation> &ops,
                             ObITableEntityFactory &entity_factory,
                             common::ObIArray<ObTableOperationResult> &results);
  static int aggregate_one_result(common::ObIArray<ObTableOperationResult> &results);
  static int process_get(common::ObIAllocator &allocator,
                         ObTableCtx &tb_ctx,
                         ObTableOperationResult &result);
private:
  static int multi_execute_internal(ObTableBatchCtx &ctx,
                                    ObTableApiSpec &spec,
                                    const common::ObIArray<ObTableOperation> &ops,
                                    common::ObIArray<ObTableOperationResult> &results);
  static int multi_get(ObTableBatchCtx &ctx,
                       const common::ObIArray<ObTableOperation> &ops,
                       common::ObIArray<ObTableOperationResult> &results);
  static int multi_get_fuse_key_range(ObTableBatchCtx &ctx, ObTableApiSpec &spec,
                                      const common::ObIArray<ObTableOperation> &ops,
                                      common::ObIArray<ObTableOperationResult> &results);
  static int multi_op_in_executor(ObTableBatchCtx &ctx,
                                  ObTableApiSpec &pec,
                                  const common::ObIArray<ObTableOperation> &ops,
                                  common::ObIArray<ObTableOperationResult> &results);
  static int multi_insert(ObTableBatchCtx &ctx,
                          const common::ObIArray<ObTableOperation> &ops,
                          common::ObIArray<ObTableOperationResult> &results);
  static int multi_delete(ObTableBatchCtx &ctx,
                          const common::ObIArray<ObTableOperation> &ops,
                          common::ObIArray<ObTableOperationResult> &results);
  static int multi_replace(ObTableBatchCtx &ctx,
                           const common::ObIArray<ObTableOperation> &ops,
                           common::ObIArray<ObTableOperationResult> &results);
  static int multi_put(ObTableBatchCtx &ctx,
                       const common::ObIArray<ObTableOperation> &ops,
                       common::ObIArray<ObTableOperationResult> &results);
  static int htable_delete(ObTableBatchCtx &ctx,
                           const common::ObIArray<ObTableOperation> &ops,
                           common::ObIArray<ObTableOperationResult> &results);
  static int htable_put(ObTableBatchCtx &ctx,
                        const common::ObIArray<ObTableOperation> &ops,
                        common::ObIArray<ObTableOperationResult> &results);
  static int htable_mutate_row(ObTableBatchCtx &ctx,
                               const common::ObIArray<ObTableOperation> &ops,
                               common::ObIArray<ObTableOperationResult> &results);
  static int batch_execute(ObTableBatchCtx &ctx,
                           const common::ObIArray<ObTableOperation> &ops,
                           common::ObIArray<ObTableOperationResult> &results);
  static int process_insert(ObTableCtx &tb_ctx,
                            ObTableOperationResult &result);
  static int process_delete(ObTableCtx &tb_ctx,
                            ObTableOperationResult &result);
  static int process_update(ObTableCtx &tb_ctx,
                            ObTableOperationResult &result);
  static int process_replace(ObTableCtx &tb_ctx,
                             ObTableOperationResult &result);
  static int process_insert_up(ObTableCtx &tb_ctx,
                               ObTableOperationResult &result);
  static int process_put(ObTableCtx &tb_ctx,
                         ObTableOperationResult &result);
  static int process_increment_or_append(ObTableCtx &tb_ctx,
                                         ObTableOperationResult &result);
  static int process_htable_delete(const ObTableOperation &op,
                                   ObTableBatchCtx &ctx,
                                   common::ObIArray<ObTableOperationResult> &results);
  static int process_htable_put(const ObTableOperation &op,
                                ObTableBatchCtx &ctx,
                                common::ObIArray<ObTableOperationResult> &results);
  static int init_table_ctx(ObTableCtx &tb_ctx,
                            const ObTableOperation &op,
                            const ObTableBatchCtx &batch_ctx,
                            ObTabletID tablet_id);
  static int check_arg2(bool returning_rowkey,
                        bool returning_affected_entity);
  static int adjust_entities(ObTableBatchCtx &ctx, const common::ObIArray<ObTableOperation> &ops);
  static int get_result_index(const ObNewRow &row,
                              const ObIArray<ObTableOperation> &ops,
                              const ObIArray<uint64_t> &rowkey_ids,
                              ObObj *rowkey_cells,
                              ObIArray<int64_t> &indexs);
  static int check_legality(ObTableBatchCtx &ctx,
                            const common::ObIArray<ObTableOperation> &ops,
                            common::ObIArray<ObTableOperationResult> &results);
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_SERVICE_H */
