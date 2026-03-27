/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TABLE_MULTI_BATCH_COMMON_H
#define _OB_TABLE_MULTI_BATCH_COMMON_H

#include "ob_table_batch_common.h"

namespace oceanbase
{
namespace table
{

class ObTableMultiBatchRequest
{
public:
  ObTableMultiBatchRequest()
  {
    ops_.set_attr(ObMemAttr(MTL_ID(), "BReqOps"));
    tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "BReqTbtIds"));
  }
  TO_STRING_KV(K_(ops), K_(tablet_ids));
  bool is_valid() const;
  OB_INLINE common::ObIArray<ObTableBatchOperation>& get_ops() { return ops_; }
  OB_INLINE const common::ObIArray<ObTableBatchOperation>& get_ops() const { return ops_; }
  OB_INLINE common::ObIArray<ObTabletID>& get_tablet_ids() { return tablet_ids_; }
  OB_INLINE const common::ObIArray<ObTabletID>& get_tablet_ids() const { return tablet_ids_; }
private:
  common::ObSEArray<ObTableBatchOperation, ObTableBatchOperation::COMMON_BATCH_SIZE> ops_;
  common::ObSEArray<ObTabletID, ObTableBatchOperation::COMMON_BATCH_SIZE> tablet_ids_;
};

class ObTableMultiBatchResult
{
public:
  ObTableMultiBatchResult(common::ObIAllocator &allocator)
      : results_(allocator)
  {}
  TO_STRING_KV(K_(results));
  OB_INLINE common::ObIArray<ObTableBatchOperationResult>& get_results() { return results_; }
  OB_INLINE const common::ObIArray<ObTableBatchOperationResult>& get_results() const { return results_; }
  OB_INLINE bool empty() const { return results_.empty(); }
  OB_INLINE int prepare_allocate(int64_t size) { return results_.prepare_allocate(size); }
private:
  common::ObFixedArray<ObTableBatchOperationResult, common::ObIAllocator> results_;
};

struct ObTableMultiBatchCtx : public ObTableBatchCtx
{
public:
  ObTableMultiBatchCtx(common::ObIAllocator &allocator,
                       ObTableAuditCtx &audit_ctx,
                       ObITableEntityFactory &entity_factory)
      : ObTableBatchCtx(allocator, audit_ctx),
        entity_factory_(entity_factory)
  {}
public:
  ObITableEntityFactory &entity_factory_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_MULTI_BATCH_COMMON_H */
