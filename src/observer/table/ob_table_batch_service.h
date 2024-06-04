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

#include "ob_table_context.h"
#include "share/table/ob_table_rpc_struct.h"
#include "ob_table_trans_utils.h"
#include "ob_table_executor.h"
#include "ob_table_audit.h"

namespace oceanbase
{
namespace table
{

struct ObTableBatchCtx
{
public:
  explicit ObTableBatchCtx(common::ObIAllocator &allocator, ObTableAuditCtx &audit_ctx)
      : allocator_(allocator),
        tb_ctx_(allocator_),
        audit_ctx_(audit_ctx)
  {
    reset();
  }
  virtual ~ObTableBatchCtx() {}
  TO_STRING_KV(K_(tb_ctx),
               KPC_(trans_param),
               KPC_(ops),
               KPC_(results),
               K_(table_id),
               K_(tablet_id),
               K_(ls_id),
               K_(is_atomic),
               K_(is_readonly),
               K_(is_same_type),
               K_(is_same_properties_names),
               K_(use_put),
               K_(returning_affected_entity),
               K_(returning_rowkey),
               K_(return_one_result),
               K_(entity_type),
               K_(consistency_level),
               KPC_(result_entity),
               KPC_(credential));
public:
  int check_legality();
  void reset()
  {
    stat_event_type_ = nullptr;
    trans_param_ = nullptr;
    ops_ = nullptr;
    results_ = nullptr;
    table_id_ = common::OB_INVALID_ID;
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    is_atomic_ = true;
    is_readonly_ = false;
    is_same_type_ = false;
    is_same_properties_names_ = false;
    use_put_ = false;
    returning_affected_entity_ = false;
    returning_rowkey_ = false;
    return_one_result_ = false;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    consistency_level_ = ObTableConsistencyLevel::EVENTUAL;
    entity_factory_ = nullptr;
    result_entity_ = nullptr;
    credential_ = nullptr;
    tb_ctx_.reset();
  }
public:
  common::ObIAllocator &allocator_;
  ObTableCtx tb_ctx_;
  int32_t *stat_event_type_;
  ObTableTransParam *trans_param_;
  const ObIArray<ObTableOperation> *ops_;
  ObIArray<ObTableOperationResult> *results_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  bool is_atomic_;
  bool is_readonly_;
  bool is_same_type_;
  bool is_same_properties_names_;
  bool use_put_;
  bool returning_affected_entity_;
  bool returning_rowkey_;
  bool return_one_result_;
  ObTableEntityType entity_type_;
  ObTableConsistencyLevel consistency_level_;
  ObITableEntityFactory *entity_factory_;
  ObITableEntity *result_entity_;
  ObTableApiCredential *credential_;
  ObTableAuditCtx &audit_ctx_;
};

class ObTableBatchService
{
public:
  static int execute(ObTableBatchCtx &ctx);
private:
  static int multi_get(ObTableBatchCtx &ctx);
  static int multi_get_fuse_key_range(ObTableBatchCtx &ctx, ObTableApiSpec &spec);
  static int multi_op_in_executor(ObTableBatchCtx &ctx, ObTableApiSpec &pec);
  static int multi_insert(ObTableBatchCtx &ctx);
  static int multi_delete(ObTableBatchCtx &ctx);
  static int multi_replace(ObTableBatchCtx &ctx);
  static int multi_put(ObTableBatchCtx &ctx);
  static int htable_delete(ObTableBatchCtx &ctx);
  static int htable_put(ObTableBatchCtx &ctx);
  static int htable_mutate_row(ObTableBatchCtx &ctx);
  static int batch_execute(ObTableBatchCtx &ctx);
  static int process_get(common::ObIAllocator &allocator,
                         ObTableCtx &tb_ctx,
                         ObTableOperationResult &result);
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
                                   ObTableBatchCtx &ctx);
  static int process_htable_put(const ObTableOperation &op,
                                ObTableBatchCtx &ctx);
  static int init_table_ctx(ObTableCtx &tb_ctx,
                            const ObTableOperation &op,
                            const ObTableBatchCtx &batch_ctx);
  static int check_arg2(bool returning_rowkey,
                        bool returning_affected_entity);
  static int adjust_entities(ObTableBatchCtx &ctx);
  static int get_result_index(const common::ObNewRow &row,
                              const common::ObIArray<ObTableOperation> &ops,
                              ObIArray<int64_t> &indexs);
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_SERVICE_H */
