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
typedef std::pair<share::ObLSID, common::ObTabletID> LSTabletIDPair;
struct ObInsertMonitor;
struct ObTabletSliceParam;
class ObDirectLoadMgrAgent;
class ObColumnClusteredDag;
class ObISliceWriter;
class ObHeapCsSliceWriter;
struct ObDDLAutoincParam;
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

class ObPxMultiPartSSTableInsertVecOpInput : public ObPxMultiPartSSTableInsertOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartSSTableInsertVecOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxMultiPartSSTableInsertOpInput(ctx, spec)
  {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertVecOpInput);
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
  int64_t ddl_slice_id_idx_; // record idx of exprs for ddl slice id
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertSpec);
};

class ObPxMultiPartSSTableInsertVecSpec : public ObPxMultiPartSSTableInsertSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartSSTableInsertVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObPxMultiPartSSTableInsertSpec(alloc, type)
  {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertVecSpec);
};

class ObPxMultiPartSSTableInsertOp : public ObPxMultiPartInsertOp
{
public:
  ObPxMultiPartSSTableInsertOp(ObExecContext &exec_ctx,
                               const ObOpSpec &spec,
                               ObOpInput *input)
    : ObPxMultiPartInsertOp(exec_ctx, spec, input),
      allocator_("SSTABLE_INS"),
      is_all_partition_finished_(false),
      is_partitioned_table_(false),
      is_vec_gen_vid_(false),
      tablet_id_expr_(nullptr),
      slice_info_expr_(nullptr),
      tablet_autoinc_expr_(nullptr),
      tablet_autoinc_column_idx_(-1),
      ddl_dag_(nullptr),
      need_idempotent_tablet_autoinc_(false),
      need_idempotent_table_autoinc_(false),
      need_idempotent_doc_id_(false)
  {}
  virtual ~ObPxMultiPartSSTableInsertOp() { destroy(); }
  const ObPxMultiPartSSTableInsertSpec &get_spec() const;
  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
protected:
  int get_next_row_from_child(ObInsertMonitor *insert_monitor);
  int get_next_batch_from_child(const int64_t max_batch_size, const ObBatchRows *&brs, ObInsertMonitor *insert_monitor);
  int get_tablet_info_from_row(
      const ObExprPtrIArray &row,
      common::ObTabletID &tablet_id,
      storage::ObTabletSliceParam *tablet_slice_param = nullptr);
  int eval_current_row(const int64_t rowkey_column_count, blocksstable::ObDatumRow &current_row);
  int eval_current_row(ObIArray<ObDatum *> &datums);
  int eval_current_batch(ObIArray<ObIVector *> &vectors, const ObBatchRows &brs);
  int sync_table_level_autoinc_value();
  bool is_heap_plan() const { return MY_SPEC.regenerate_heap_table_pk_ || is_vec_gen_vid_; }
  int write_heap_slice_by_row();
  int write_heap_slice_by_batch();
  int write_ordered_slice_by_row();
  int write_ordered_slice_by_batch();
  int finish_dag();
  bool need_autoinc_by_row();
  int sync_tablet_doc_id(ObISliceWriter *slice_writer);
  int init_table_autoinc_param(const ObTabletID &tablet_id, const int64_t slice_idx, ObDDLAutoincParam &autoinc_param);
  int init_tablet_autoinc_param(const ObTabletID &tablet_id, const int64_t slice_idx, ObDDLAutoincParam &autoinc_param);
  int locate_exprs();
  int check_need_idempotence();
  int get_or_create_heap_writer(const ObTabletID &tablet_id, const bool is_append_batch, ObISliceWriter *&slice_writer);
  int generate_tablet_active_rows(const ObIVector *tablet_id_vector, const ObBatchRows &brs,
                                  hash::ObHashMap<ObTabletID, ObHeapCsSliceWriter *, hash::NoPthreadDefendMode> &slice_writer_map);
  int switch_slice_if_need(const ObTabletID &tablet_id, const int64_t slice_idx, const bool is_append_batch,
                           ObISliceWriter *&slice_writer, ObDDLAutoincParam *autoinc_param = nullptr);
  int get_continue_slice(const ObIVector *tablet_id_vector, const ObIVector *slice_info_vector_, const ObBatchRows &brs,
                         ObTabletID &tablet_id, int64_t &slice_idx, int64_t &offset, int64_t &row_count);

protected:
  static const uint64_t MAP_HASH_BUCKET_NUM = 1543L;
  common::ObArenaAllocator allocator_;
  bool is_all_partition_finished_;
  bool is_partitioned_table_;
  // vector index
  bool is_vec_gen_vid_;

  ObTabletID non_partitioned_tablet_id_;
  ObExpr *tablet_id_expr_; // valid when partitioned table
  ObExpr *slice_info_expr_; // valid when ordered tablet and idempotent ddl
  ObExpr *tablet_autoinc_expr_; // valid when heap plan
  int64_t tablet_autoinc_column_idx_;
  storage::ObColumnClusteredDag *ddl_dag_;
  // for heap plan, direct write tablet
  typedef common::hash::ObHashMap<common::ObTabletID, ObISliceWriter *, common::hash::NoPthreadDefendMode> TabletWriterMap;
  TabletWriterMap heap_tablet_writer_map_;
  bool need_idempotent_tablet_autoinc_;
  bool need_idempotent_table_autoinc_;
  bool need_idempotent_doc_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertOp);
};

class ObPxMultiPartSSTableInsertVecOp : public ObPxMultiPartSSTableInsertOp
{
public:
  ObPxMultiPartSSTableInsertVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObPxMultiPartSSTableInsertOp(exec_ctx, spec, input)
  {}

  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertVecOp);
};

}// end namespace sql
}// end namespace oceanbase


#endif//OCEANBASE_SQL_PX_OB_PX_SSTABLE_INSERT_OP_H
