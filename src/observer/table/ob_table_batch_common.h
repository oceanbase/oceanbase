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

#ifndef _OB_TABLE_BATCH_COMMON_H
#define _OB_TABLE_BATCH_COMMON_H

#include "ob_table_context.h"
#include "share/table/ob_table_rpc_struct.h"
#include "ob_table_trans_utils.h"
#include "ob_table_executor.h"
#include "ob_table_audit.h"

namespace oceanbase
{
namespace table
{


struct ObTableQueryBatchCtx
{
  explicit ObTableQueryBatchCtx()
      : table_id_(OB_INVALID_ID)
  {}
  virtual ~ObTableQueryBatchCtx() {}

  uint64_t table_id_;
};

struct ObTableBatchCtx : public ObTableQueryBatchCtx
{
public:
  explicit ObTableBatchCtx(common::ObIAllocator &allocator, ObTableAuditCtx &audit_ctx)
      : ObTableQueryBatchCtx(),
        allocator_(allocator),
        tb_ctx_(allocator),
        audit_ctx_(audit_ctx)
  {
    reset();
    tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "BCtxTbts"));
  }
  virtual ~ObTableBatchCtx() {}
  TO_STRING_KV(K_(tb_ctx),
               K_(table_id),
               KPC_(trans_param),
               K_(is_atomic),
               K_(is_readonly),
               K_(is_same_type),
               K_(is_same_properties_names),
               K_(use_put),
               K_(returning_affected_entity),
               K_(returning_rowkey),
               K_(consistency_level),
               K_(tablet_ids),
               KPC_(credential));
public:
  int check_legality();
  void reset()
  {
    trans_param_ = nullptr;
    is_atomic_ = true;
    is_readonly_ = false;
    is_same_type_ = false;
    is_same_properties_names_ = false;
    use_put_ = false;
    returning_affected_entity_ = false;
    table_id_ = common::OB_INVALID_ID;
    tablet_ids_.reset();
    returning_rowkey_ = false;
    consistency_level_ = ObTableConsistencyLevel::EVENTUAL;
    credential_ = nullptr;
    tb_ctx_.reset();
  }
public:
  common::ObIAllocator &allocator_;
  table::ObTableCtx tb_ctx_;
  ObTableTransParam *trans_param_;
  bool is_atomic_;
  bool is_readonly_;
  bool is_same_type_;
  bool is_same_properties_names_;
  bool use_put_;
  bool returning_affected_entity_;
  bool returning_rowkey_;
  common::ObSEArray<ObTabletID, 16> tablet_ids_;
  ObTableConsistencyLevel consistency_level_;
  ObTableApiCredential *credential_;
  ObTableAuditCtx &audit_ctx_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_COMMON_H */
