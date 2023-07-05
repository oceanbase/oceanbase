/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_PX_OB_PX_SSTABLE_INSERT_OP_H
#define OCEANBASE_SQL_PX_OB_PX_SSTABLE_INSERT_OP_H

#include "sql/engine/pdml/static/ob_px_multi_part_insert_op.h"
#include "share/ob_tablet_autoincrement_param.h"

namespace oceanbase
{
namespace storage
{
class ObSSTableInsertRowIterator;
}

namespace sql
{
class ObPxMultiPartSSTableInsertOpInput : public ObPxMultiPartModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartSSTableInsertOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxMultiPartModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObPxMultiPartModifyOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertOpInput);
};

class ObPxMultiPartSSTableInsertSpec : public ObPxMultiPartInsertSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartSSTableInsertSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObPxMultiPartInsertSpec(alloc, type), flashback_query_expr_(nullptr),
      regenerate_heap_table_pk_(false)
  {}
  int get_snapshot_version(ObEvalCtx &eval_ctx, int64_t &snapshot_version) const;
public:
  ObExpr *flashback_query_expr_;
  bool regenerate_heap_table_pk_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertSpec);
};

class ObPxMultiPartSSTableInsertOp : public ObPxMultiPartInsertOp
{
public:
  ObPxMultiPartSSTableInsertOp(ObExecContext &exec_ctx,
                               const ObOpSpec &spec,
                               ObOpInput *input)
    : ObPxMultiPartInsertOp(exec_ctx, spec, input),
      allocator_("SSTABLE_INS"),
      tablet_store_map_(),
      tablet_seq_caches_(),
      curr_tablet_store_iter_(),
      curr_tablet_idx_(-1),
      count_rows_finish_(false),
      curr_part_idx_(0)
  {}
  virtual ~ObPxMultiPartSSTableInsertOp() { destroy(); }
  const ObPxMultiPartSSTableInsertSpec &get_spec() const;
  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  int get_next_row_with_cache();
  int get_tablet_id_from_row(const ObExprPtrIArray &row,
                             const int64_t part_id_idx,
                             common::ObTabletID &tablet_id);
private:
  int get_all_rows_and_count();
  int create_tablet_store(common::ObTabletID &tablet_id, ObChunkDatumStore *&tablet_store);
  bool need_count_rows() const { return MY_SPEC.regenerate_heap_table_pk_ && !count_rows_finish_; }      
  int get_next_tablet_id(common::ObTabletID &tablet_id);
private:
  friend class storage::ObSSTableInsertRowIterator;
  static const uint64_t MAP_HASH_BUCKET_NUM = 1543L;
  static const uint64_t TABLET_STORE_MEM_LIMIT = 2 * 1024 * 1024; // 2M
  typedef common::hash::ObHashMap<common::ObTabletID, ObChunkDatumStore*, common::hash::NoPthreadDefendMode> TabletStoreMap;
  common::ObArenaAllocator allocator_;
  TabletStoreMap tablet_store_map_;
  ObArray<share::ObTabletCacheInterval> tablet_seq_caches_;
  ObChunkDatumStore::Iterator curr_tablet_store_iter_;
  int64_t curr_tablet_idx_;
  bool count_rows_finish_;
  int64_t curr_part_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertOp);
};

}// end namespace sql
}// end namespace oceanbase


#endif//OCEANBASE_SQL_PX_OB_PX_SSTABLE_INSERT_OP_H
