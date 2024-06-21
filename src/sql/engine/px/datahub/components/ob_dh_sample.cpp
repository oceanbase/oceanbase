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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/ob_px_coord_op.h"
#include "sql/engine/px/exchange/ob_px_dist_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit_op.h"
#include "sql/engine/px/ob_px_coord_op.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;


ObDynamicSamplePieceMsg::ObDynamicSamplePieceMsg()
  : expect_range_count_(), tablet_ids_(),
    sample_type_(NOT_INIT_SAMPLE_TYPE), part_ranges_(),
    row_stores_(), arena_(), spin_lock_(common::ObLatchIds::SQL_DYN_SAMPLE_MSG_LOCK)
{

}

void ObDynamicSamplePieceMsg::reset()
{
  tablet_ids_.reset();
  for (int i = 0; i < row_stores_.count(); ++i) {
    if (OB_NOT_NULL(row_stores_.at(i))) {
      row_stores_.at(i)->reset();
    }
  }
  part_ranges_.reset();
  row_stores_.reset();
  arena_.reset();
}

bool ObDynamicSamplePieceMsg::is_valid() const
{
  return expect_range_count_ > 0 &&
         tablet_ids_.count() > 0;
}

OB_DEF_SERIALIZE(ObDynamicSamplePieceMsg)
{
  int ret = OB_SUCCESS;
  ret = ObDatahubPieceMsg::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    int64_t store_count = row_stores_.count();
    LST_DO_CODE(OB_UNIS_ENCODE, expect_range_count_, tablet_ids_,
        sample_type_, part_ranges_, store_count);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_stores_.count(); ++i) {
    ObChunkDatumStore *cur_store = row_stores_.at(i);
    bool is_null = nullptr == cur_store;
    OB_UNIS_ENCODE(is_null);
    if (!is_null) {
      OB_UNIS_ENCODE(*cur_store);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDynamicSamplePieceMsg)
{
  int ret = OB_SUCCESS;
  ret = ObDatahubPieceMsg::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    int64_t store_count = 0;
    LST_DO_CODE(OB_UNIS_DECODE, expect_range_count_, tablet_ids_,
        sample_type_, part_ranges_, store_count);
    for (int64_t i = 0; OB_SUCC(ret) && i < store_count; ++i) {
      bool is_null = false;
      OB_UNIS_DECODE(is_null);
      if (is_null) {
        if (OB_FAIL(row_stores_.push_back(nullptr))) {
          LOG_WARN("push back null row store failed", K(ret));
        }
      } else {
        void *tmp_buf = arena_.alloc(sizeof(ObChunkDatumStore));
        if (OB_ISNULL(tmp_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          ObChunkDatumStore *tmp_store = new (tmp_buf) ObChunkDatumStore("DYN_SAMPLE_CTX");
          if (OB_FAIL(tmp_store->deserialize(buf, data_len, pos))) {
            LOG_WARN("deserialize datum store failed", K(ret), K(i));
          } else if (OB_FAIL(row_stores_.push_back(tmp_store))) {
            LOG_WARN("push back datum store failed", K(ret), K(i));
          }
          if (OB_FAIL(ret) && nullptr != tmp_store) {
            tmp_store->~ObChunkDatumStore();
            // The corresponding memory does not need to be manually released;
            // it will be automatically freed upon the destruction of arena_.
          }
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDynamicSamplePieceMsg)
{
  int64_t len = 0;
  len += ObDatahubPieceMsg::get_serialize_size();
  int64_t store_count = row_stores_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, expect_range_count_, tablet_ids_,
      sample_type_, part_ranges_, store_count);
  for (int64_t i = 0; i < row_stores_.count(); ++i) {
    ObChunkDatumStore *cur_store = row_stores_.at(i);
    bool is_null = nullptr == cur_store;
    OB_UNIS_ADD_LEN(is_null);
    if (!is_null) {
      OB_UNIS_ADD_LEN(*cur_store);
    }
  }
  return len;
}

int ObDynamicSamplePieceMsg::merge_piece_msg(int64_t task_count,
    ObDynamicSamplePieceMsg &piece_msg, bool &is_finish)
{
  int ret = OB_SUCCESS;
  if (piece_msg.is_row_sample()) {
    CK(piece_msg.row_stores_.count() == row_stores_.count());
    ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
    for (int i = 0; OB_SUCC(ret) && i < piece_msg.row_stores_.count(); ++i) {
      if (OB_ISNULL(piece_msg.row_stores_.at(i))) {
        continue;
      } else if (OB_FAIL(row_stores_.at(i)->append_datum_store(*piece_msg.row_stores_.at(i)))) {
        LOG_WARN("append sample store failed", K(ret));
      }
    }
  } else if (piece_msg.is_object_sample()) {
    ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
    for (int i = 0; i < piece_msg.part_ranges_.count() && OB_SUCC(ret); ++i) {
      OZ(part_ranges_.push_back(piece_msg.part_ranges_.at(i)));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sample type", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (task_count == ATOMIC_AAF(&piece_count_, 1)) {
      is_finish = true;
    } else {
      is_finish = false;
    }
  }
  return ret;
}


ObDynamicSampleWholeMsg::ObDynamicSampleWholeMsg()
  : part_ranges_()
{

}

void ObDynamicSampleWholeMsg::reset()
{
  part_ranges_.reset();
  assign_allocator_.reset();
}

int ObDynamicSampleWholeMsg::assign(const ObDynamicSampleWholeMsg &other, common::ObIAllocator *allocator/* = NULL */)
{
  int ret = OB_SUCCESS;
  if (nullptr == allocator) {
    allocator = &assign_allocator_;
  }
  if (OB_FAIL(part_ranges_.reserve(other.part_ranges_.count()))) {
    LOG_WARN("reserve partition ranges failed", K(ret), K(other.part_ranges_.count()));
  }
  char *buf = NULL;
  int64_t size = 0;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.part_ranges_.count(); ++i) {
    const ObPxTabletRange &cur_part_range = other.part_ranges_.at(i);
    ObPxTabletRange tmp_part_range;
    if (OB_FAIL(tmp_part_range.deep_copy_from<true>(cur_part_range, *allocator, buf, size, pos))) {
      LOG_WARN("deep copy partition range failed", K(ret), K(cur_part_range), K(i));
    } else if (OB_FAIL(part_ranges_.push_back(tmp_part_range))) {
      LOG_WARN("push back partition range failed", K(ret), K(tmp_part_range), K(i));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDynamicSampleWholeMsg, ObDatahubWholeMsg), part_ranges_);


ObDynamicSamplePieceMsgCtx::ObDynamicSamplePieceMsgCtx(
    uint64_t op_id,
    int64_t task_cnt,
    int64_t timeout_ts,
    int64_t tenant_id,
    ObExecContext &exec_ctx,
    ObPxCoordOp &coord,
    const ObDynamicSamplePieceMsgCtx::SortDef &sort_def)
  : ObPieceMsgCtx(op_id, task_cnt, timeout_ts),
    is_inited_(false),
    tenant_id_(tenant_id),
    received_(0),
    succ_count_(0),
    tablet_ids_(),
    expect_range_count_(0),
    sort_impl_(op_monitor_info_),
    exec_ctx_(exec_ctx),
    last_store_row_(),
    coord_(coord),
    sort_def_(sort_def),
    mutex_(common::ObLatchIds::SQL_DYN_SAMPLE_MSG_LOCK)
{

}

int ObDynamicSamplePieceMsgCtx::alloc_piece_msg_ctx(const ObDynamicSamplePieceMsg &pkt,
                                                    ObPxCoordInfo &coord_info,
                                                    ObExecContext &ctx,
                                                    int64_t task_cnt,
                                                    ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  SortDef sort_def;
  ObOperatorKit *op_kit = ctx.get_operator_kit(pkt.op_id_);
  if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret),
        KP(ctx.get_my_session()), K(ctx.get_physical_plan_ctx()));
  } else if (NULL == op_kit || NULL == op_kit->spec_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("spec is NULL", K(ret), K(pkt.op_id_));
  } else {
    if (PHY_PX_DIST_TRANSMIT == op_kit->spec_->type_) {
      const ObPxDistTransmitSpec *spec = static_cast<const ObPxDistTransmitSpec *>(op_kit->spec_);
      sort_def.exprs_ = &spec->dist_exprs_;
      sort_def.collations_ = &spec->sort_collations_;
      sort_def.cmp_funs_ = &spec->sort_cmp_funs_;
    } else if (PHY_PX_REPART_TRANSMIT == op_kit->spec_->type_) {
      const ObPxRepartTransmitSpec *spec = static_cast<const ObPxRepartTransmitSpec *>(op_kit->spec_);
      sort_def.exprs_ = &spec->dist_exprs_;
      sort_def.collations_ = &spec->sort_collations_;
      sort_def.cmp_funs_ = &spec->sort_cmp_funs_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected spec type", K(ret), K(pkt.op_id_), K(op_kit->spec_->type_));
    }
  }

  if (OB_SUCC(ret)) {
    void *buf = ctx.get_allocator().alloc(sizeof(ObDynamicSamplePieceMsgCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      msg_ctx = new (buf) ObDynamicSamplePieceMsgCtx(
          pkt.op_id_,
          task_cnt,
          ctx.get_physical_plan_ctx()->get_timeout_timestamp(),
          ctx.get_my_session()->get_effective_tenant_id(),
          ctx,
          coord_info.coord_,
          sort_def);
    }
  }
  return ret;
}

int ObDynamicSamplePieceMsgCtx::init(const ObIArray<uint64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(tablet_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_ids));
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("assign partition ids failed", K(ret), K(tablet_ids));
  } else if (OB_FAIL(sort_impl_.init(
          tenant_id_,
          sort_def_.collations_,
          sort_def_.cmp_funs_,
          &coord_.get_eval_ctx(),
          &exec_ctx_,
          false/*in_local_order*/,
          true/*need_rewind*/))) {
    LOG_WARN("init sort instance failed", K(ret));
  } else {
    sort_impl_.set_io_event_observer(&coord_.get_io_event_observer());
    char *buf = (char *)exec_ctx_.get_allocator().alloc(tablet_ids.count() * sizeof(ObChunkDatumStore));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(tablet_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      ObChunkDatumStore *sample_store = new (buf + i * sizeof(ObChunkDatumStore)) ObChunkDatumStore("DYN_SAMPLE_CTX");
      if (OB_FAIL(sample_store->init(0, tenant_id_, ObCtxIds::DEFAULT_CTX_ID,
          "DYN_SAMPLE_CTX", false/*enable dump*/))) {
        LOG_WARN("init sample chunk store failed", K(ret), K(i));
      } else if (OB_FAIL(sample_stores_.push_back(sample_store))) {
        LOG_WARN("push back sample store failed", K(ret), K(i));
      }
      if (OB_FAIL(ret) && nullptr != sample_store) {
        sample_store->~ObChunkDatumStore();
      }
    }
    if (OB_FAIL(ret) && nullptr != buf) {
      destroy_sample_stores();
      exec_ctx_.get_allocator().free(buf);
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObDynamicSamplePieceMsgCtx::destroy()
{
  is_inited_ = false;
  received_ = 0;
  succ_count_ = 0;
  tablet_ids_.reset();
  void *buf = sample_stores_.empty() ? nullptr : reinterpret_cast<void *>(sample_stores_.at(0));
  destroy_sample_stores();
  if (nullptr != buf) {
    exec_ctx_.get_allocator().free(buf);
  }
  sort_impl_.reset();
  arena_.reset();
}

void ObDynamicSamplePieceMsgCtx::destroy_sample_stores()
{
  for (int64_t i = 0; i < sample_stores_.count(); ++i) {
    ObChunkDatumStore *sample_store = sample_stores_.at(i);
    if (nullptr != sample_store) {
      sample_store->~ObChunkDatumStore();
    }
  }
  sample_stores_.reset();
}

int ObDynamicSamplePieceMsgCtx::process_piece(const ObDynamicSamplePieceMsg &piece)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (piece.tablet_ids_.count() != tablet_ids_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(piece), K(tablet_ids_.count()));
  } else {
    expect_range_count_ = piece.expect_range_count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < piece.tablet_ids_.count(); ++i) {
      if (OB_UNLIKELY(piece.tablet_ids_.at(i) != tablet_ids_.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition id not equal", K(ret), K(i),
            "piece_tablet_id", piece.tablet_ids_.at(i), "ctx_tablet_id", tablet_ids_.at(i));
      } else if (piece.is_row_sample()) {
        const ObChunkDatumStore *cur_sample_store = piece.row_stores_.at(i);
        if (nullptr == cur_sample_store) {
        // do nothing
        } else if (OB_FAIL(sample_stores_.at(i)->append_datum_store(*cur_sample_store))) {
          LOG_WARN("append sample store failed", K(ret));
        }
      } else if (piece.is_object_sample()) {
        if (OB_FAIL(append_object_sample_data(piece, i, sample_stores_.at(i)))) {
          LOG_WARN("fail to append object sample data", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected way", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    succ_count_ += piece.piece_count_;
  }
  return ret;
}

int ObDynamicSamplePieceMsgCtx::append_object_sample_data(
    const ObDynamicSamplePieceMsg &piece,
    int64_t idx,
    ObChunkDatumStore* sample_store)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *store_row = last_store_row_.get_store_row();
  CK(idx < piece.tablet_ids_.count());
  // part_ranges cnt可小于、大于、等于tablet_id cnt.
  for (int m = 0; m < piece.part_ranges_.count() && OB_SUCC(ret); ++m) {
    if (piece.tablet_ids_.at(idx) == piece.part_ranges_.at(m).tablet_id_) {
      if (OB_ISNULL(store_row)) {
        if (OB_FAIL(last_store_row_.init(arena_, piece.part_ranges_.at(m).get_range_col_cnt()))) {
          LOG_WARN("failed to init last store row", K(ret));
        } else {
          store_row = last_store_row_.get_store_row();
        }
      }
      CK(OB_NOT_NULL(store_row));
      if (OB_SUCC(ret)) {
        ObDatum *cells = store_row->cells();
        for (int i = 0; i < piece.part_ranges_.at(m).range_cut_.count() && OB_SUCC(ret); ++i) {
          last_store_row_.reuse();
          for (int j = 0; j < piece.part_ranges_.at(m).range_cut_.at(i).count() && OB_SUCC(ret); ++j) {
            cells[j] = piece.part_ranges_.at(m).range_cut_.at(i).at(j);
            store_row->row_size_ += cells[j].len_;
          }
          OZ(sample_store->add_row(last_store_row_));
        }
      }
    }
  }
  return ret;
}

int ObDynamicSamplePieceMsgCtx::build_whole_msg(ObDynamicSampleWholeMsg &whole_msg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (task_cnt_ != received_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("piece not full", K(ret));
  } else if (task_cnt_ != succ_count_) {
    ret = OB_PARTIAL_FAILED;
    LOG_WARN("partial failed", K(ret));
  } else {
    ObPxTabletRange partition_range;
    // Both pkey range and range shuffle will use the sampling function
    // and the range of pkey range required to be segmented is aggregated by parititon.
    // In order to be compatible with these two situations,
    // So mock only one partition during range shuffle.
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids_.count(); ++i) {
      partition_range.tablet_id_ = tablet_ids_.at(i);
      if (OB_FAIL(split_range(sample_stores_.at(i), expect_range_count_, partition_range.range_cut_))) {
        LOG_WARN("cut range failed", K(ret), K(i), K(tablet_ids_.at(i)), K(expect_range_count_));
      } else if (OB_FAIL(whole_msg.part_ranges_.push_back(partition_range))) {
        LOG_WARN("push back sample range cut failed", K(ret), K(partition_range));
      }
    }
  }
  return ret;
}

int ObDynamicSamplePieceMsgCtx::split_range(
    const ObChunkDatumStore *sample_store,
    const int64_t expect_range_count,
    ObPxTabletRange::RangeCut &range_cut)
{
  int ret = OB_SUCCESS;
  range_cut.reset();
  if (nullptr == sample_store || 0 == sample_store->get_row_cnt() || 1 == expect_range_count) {
    // do nothing if no samples or only need one range
  } else if (OB_FAIL(sort_row_store(const_cast<ObChunkDatumStore &>(*sample_store)))) {
    LOG_WARN("sort row store failed", K(ret));
  } else {
    bool sort_iter_end = false;
    int64_t tmp_row_count = 0;
    int64_t tmp_key_count = 1; // expect_key_count = expect_range_count - 1
    const int64_t step = max(1, sample_store->get_row_cnt() / expect_range_count);
    ObPxTabletRange::DatumKey copied_key;
    if (OB_FAIL(copied_key.reserve(sort_def_.exprs_->count()))) {
      LOG_WARN("reserve datum key failed", K(ret), K(sort_def_.exprs_->count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_def_.exprs_->count(); ++i) {
      ObExpr *expr = sort_def_.exprs_->at(i);
      if (coord_.get_spec().use_rich_format_ &&
          !is_uniform_format(expr->get_format(coord_.get_eval_ctx()))) {
        if (OB_FAIL(expr->init_vector(coord_.get_eval_ctx(),
                          expr->is_const_expr() ? VEC_UNIFORM_CONST : VEC_UNIFORM,
                          coord_.get_eval_ctx().get_batch_size()))) {
          LOG_WARN("expr init vector failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(copied_key.push_back(ObDatum()))) {
          LOG_WARN("push back empty datum failed", K(ret), K(i));
        }
      }
    }
    while (OB_SUCC(ret) && !sort_iter_end && tmp_key_count < expect_range_count) {
      if (OB_FAIL(sort_impl_.get_next_row(*sort_def_.exprs_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("sort instance get next row failed", K(ret));
        } else {
          sort_iter_end = true;
          ret = OB_SUCCESS;
        }
      } else {
        ++tmp_row_count;
        if (tmp_row_count % step == 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < sort_def_.exprs_->count(); ++i) {
            ObDatum *cur_datum = nullptr;
            if (OB_FAIL(sort_def_.exprs_->at(i)->eval(coord_.get_eval_ctx(), cur_datum))) {
              LOG_WARN("eval expr to datum failed", K(ret), K(i));
            } else if (OB_ISNULL(cur_datum)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("current datum is null", K(ret), K(i));
            } else if (OB_FAIL(copied_key.at(i).deep_copy(*cur_datum, exec_ctx_.get_allocator()))) {
              LOG_WARN("deep copy datum failed", K(ret), K(i), K(*cur_datum));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(range_cut.push_back(copied_key))) {
              LOG_WARN("push back rowkey failed", K(ret), K(copied_key));
            } else {
              ++tmp_key_count;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDynamicSamplePieceMsgCtx::sort_row_store(ObChunkDatumStore &row_store)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    sort_impl_.reuse();
    // sort row store
    ObChunkDatumStore::Iterator row_iter;
    if (OB_FAIL(row_store.begin(row_iter))) {
      LOG_WARN("init row iterator failed", K(ret));
    } else {
      const ObChunkDatumStore::StoredRow *sr = nullptr;
      while (OB_SUCC(ret) && row_iter.has_next()) {
        if (OB_FAIL(row_iter.get_next_row(sr))) {
          LOG_WARN("get next row failed", K(ret));
        } else if (OB_FAIL(sort_impl_.add_stored_row(*sr))) {
          LOG_WARN("add stored row failed", K(ret));
        } else {
          LOG_DEBUG("sort row store", K(*sr));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sort_impl_.sort())) {
          LOG_WARN("sort failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDynamicSamplePieceMsgCtx::on_message(
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObDynamicSamplePieceMsg &piece)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!piece.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(piece));
  } else if (received_ >= task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not receive any more pkt. already get all pkt expected", K(piece), K(*this));
  } else if (piece.piece_count_ > task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(piece));
  } else if (OB_UNLIKELY(!is_inited_) && OB_FAIL(init(piece.tablet_ids_))) {
    LOG_WARN("init dynamic sample context failed", K(ret));
  } else if (OB_FAIL(process_piece(piece))) {
    LOG_WARN("process piece message failed", K(ret), K(piece));
  }
  received_ += piece.piece_count_;
  LOG_DEBUG("process a sample picece msg", K(piece), "all_got", received_, "expected", task_cnt_);

  // send whole message when all piece received
  if (OB_SUCC(ret) && received_ == task_cnt_) {
    if (OB_FAIL(send_whole_msg(sqcs))) {
      LOG_WARN("fail to send whole msg", K(ret));
    }
    IGNORE_RETURN reset_resource();
  }
  return ret;
}

int ObDynamicSamplePieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDynamicSampleWholeMsg, whole) {
    whole.op_id_ = op_id_;
    if (OB_FAIL(build_whole_msg(whole))) {
      LOG_WARN("build sample whole message failed", K(ret), K(*this));
    }
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      dtl::ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
      if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expected", K(ret));
      } else if (OB_FAIL(ch->send(whole, timeout_ts_))) {
        LOG_WARN("fail push data to channel", K(ret));
      } else if (OB_FAIL(ch->flush(true, false))) {
        LOG_WARN("fail flush dtl data", K(ret));
      } else {
        LOG_TRACE("dispatched sample whole msg",
                  K(idx), K(cnt), K(whole), K(*ch));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
      LOG_WARN("failed to wait response", K(ret));
    }
  }
  return ret;
}

void ObDynamicSamplePieceMsgCtx::reset_resource()
{
  received_ = 0;
}

int ObDynamicSamplePieceMsgListener::on_message(
    ObDynamicSamplePieceMsgCtx &ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObDynamicSamplePieceMsg &piece)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!piece.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(piece));
  } else if (piece.op_id_ != ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(piece), K(ctx));
  } else if (OB_FAIL(ctx.on_message(sqcs, piece))) {
    LOG_WARN("dynamic sample context process piece message failed", K(ret));
  }
  return ret;
}
