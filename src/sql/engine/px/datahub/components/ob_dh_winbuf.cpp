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
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

#define UNLIMITED_MEM 0

int ObWinbufPieceMsgListener::on_message(
    ObWinbufPieceMsgCtx &ctx,
    common::ObIArray<ObPxSqcMeta *> &sqcs,
    const ObWinbufPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  if (pkt.op_id_ != ctx.op_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(pkt), K(ctx));
  } else if (ctx.received_ >= ctx.task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not receive any more pkt. already get all pkt expected",
             K(pkt), K(ctx));
  } else if (pkt.is_end_) {
    /*do nothing*/
  } else if (pkt.is_datum_) {
    if (!ctx.whole_msg_.datum_store_.is_inited() && OB_FAIL(ctx.whole_msg_.datum_store_.init(
        UNLIMITED_MEM, ctx.tenant_id_, common::ObCtxIds::WORK_AREA, "PXDhWinbuf", false))) {
      LOG_WARN("fail to init row store", K(ret));
    } else if (OB_FAIL(ctx.whole_msg_.datum_store_.add_row(*pkt.datum_row_))) {
      LOG_WARN("fail to add row", K(ret));
    }
  } else {
    if (!ctx.whole_msg_.row_store_.is_inited() && OB_FAIL(ctx.whole_msg_.row_store_.init(
         UNLIMITED_MEM, ctx.tenant_id_, common::ObCtxIds::WORK_AREA, "PXDhWinbuf", false))) {
      LOG_WARN("fail to init row store", K(ret));
    } else if (OB_FAIL(ctx.whole_msg_.row_store_.add_row(pkt.row_))) {
      LOG_WARN("fail to add row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
     ctx.received_++;
    LOG_TRACE("got a win buf picece msg", "all_got", ctx.received_, "expected", ctx.task_cnt_);
  }
  // 已经收到所有 piece，发送 sqc  个 whole
  // 各个 sqc 广播给各自 task
  if (OB_SUCC(ret) && ctx.received_ == ctx.task_cnt_) {
    if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
      LOG_WARN("fail to send whole msg", K(ret));
    }
    IGNORE_RETURN ctx.reset_resource();
  }
  return ret;
}

int ObWinbufPieceMsgCtx::alloc_piece_msg_ctx(const ObWinbufPieceMsg &pkt,
                                             ObPxCoordInfo &,
                                             ObExecContext &ctx,
                                             int64_t task_cnt,
                                             ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_my_session()) ||
      OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null or physical plan ctx is null", K(ret));
  } else {
    void *buf = ctx.get_allocator().alloc(sizeof(ObWinbufPieceMsgCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      msg_ctx = new (buf) ObWinbufPieceMsgCtx(pkt.op_id_, task_cnt,
          ctx.get_physical_plan_ctx()->get_timeout_timestamp(),
          ctx.get_my_session()->get_effective_tenant_id());
    }
  }
  return ret;
}

int ObWinbufPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  whole_msg_.is_datum_ = true;
  whole_msg_.op_id_ = op_id_;
  whole_msg_.is_empty_ = (!whole_msg_.datum_store_.is_inited());
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    dtl::ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expected", K(ret));
    } else if (OB_FAIL(ch->send(whole_msg_, timeout_ts_))) {
      LOG_WARN("fail push data to channel", K(ret));
    } else if (OB_FAIL(ch->flush(true, false))) {
      LOG_WARN("fail flush dtl data", K(ret));
    } else {
      LOG_DEBUG("dispatched winbuf whole msg",
                  K(idx), K(cnt), K(whole_msg_), K(*ch));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
    LOG_WARN("failed to wait response", K(ret));
  }
  return ret;
}

void ObWinbufPieceMsgCtx::reset_resource()
{
  whole_msg_.reset();
  received_ = 0;
}

namespace ob_dh_winbuf {

template <typename T, typename B>
void pointer2off(T *&pointer, B *base)
{
  pointer = reinterpret_cast<T *>(
      reinterpret_cast<const char *>(pointer) - reinterpret_cast<const char *>(base));
}

template <typename T, typename B>
void off2pointer(T *&pointer, B *base)
{
  pointer = reinterpret_cast<T *>(
      reinterpret_cast<intptr_t>(pointer) + reinterpret_cast<char *>(base));
}

}

OB_DEF_SERIALIZE(ObWinbufPieceMsg)
{
  int ret = OB_SUCCESS;
  ret = ObDatahubPieceMsg::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    if (is_datum_) {
       LST_DO_CODE(OB_UNIS_ENCODE,
                 is_end_,
                 is_datum_,
                 col_count_,
                 row_size_,
                 payload_len_);
      if (OB_SUCC(ret)) {
        if (row_size_ > 0) {
          if (OB_ISNULL(datum_row_) || row_size_ != datum_row_->row_size_
              || row_size_ < sizeof(ObChunkDatumStore::StoredRow) ||
                 payload_len_ < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("datum row is null or size is unexpected", K(ret),
                K(row_size_), K(datum_row_->row_size_), K(payload_len_));
          } else {
            MEMCPY(buf + pos, datum_row_->payload_, payload_len_);
            common::ObDatum *cells = reinterpret_cast<common::ObDatum *>(buf + pos);
            for (int i = 0; i < datum_row_->cnt_; ++i) {
              ob_dh_winbuf::pointer2off(*(const char **)&cells[i].ptr_, datum_row_->payload_);
            }
            pos += payload_len_;
          }
        }
      }
    } else {
      LST_DO_CODE(OB_UNIS_ENCODE,
                  is_end_,
                  is_datum_,
                  col_count_,
                  row_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObWinbufPieceMsg)
{
  int ret = OB_SUCCESS;
  ret = ObDatahubPieceMsg::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
                is_end_,
                is_datum_,
                col_count_);
    if (is_datum_) {
      OB_UNIS_DECODE(row_size_);
      OB_UNIS_DECODE(payload_len_);
      if (OB_SUCC(ret) && row_size_ > 0) {
        char *datum_row_ptr = NULL;
        if (OB_ISNULL(datum_row_ptr = static_cast<char *>(deseria_allocator_.alloc(
              row_size_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret), K(row_size_));
        } else {
          datum_row_ = new(datum_row_ptr) ObChunkDatumStore::StoredRow();
          datum_row_->row_size_ = row_size_;
          datum_row_->cnt_ = col_count_;
          if (pos + payload_len_ > data_len) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("the size is overflow", K(ret));
          } else {
            MEMCPY(datum_row_->payload_, buf + pos, payload_len_);
            common::ObDatum *cells = reinterpret_cast<common::ObDatum *>(datum_row_->payload_);
            for (int64_t i = 0; i < datum_row_->cnt_; ++i) {
              ob_dh_winbuf::off2pointer(*(const char **)&cells[i].ptr_, datum_row_->payload_);
            }
            pos += payload_len_;
          }
        }
      }
    } else {
      if (col_count_ > 0 &&
          OB_FAIL(ob_create_row(deseria_allocator_, col_count_, row_))) {
        LOG_WARN("fail to create row", K(ret));
      } else {
        LST_DO_CODE(OB_UNIS_DECODE, row_);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWinbufPieceMsg)
{
  int64_t len = 0;
  len += ObDatahubPieceMsg::get_serialize_size();
  if (is_datum_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                is_end_,
                is_datum_,
                col_count_,
                row_size_,
                payload_len_);
    len += payload_len_ > 0 ? payload_len_ : 0;
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                is_end_,
                is_datum_,
                col_count_,
                row_);
  }

  return len;
}

OB_DEF_DESERIALIZE(ObWinbufWholeMsg)
{
  int ret = OB_SUCCESS;
  ret = ObDatahubWholeMsg::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
                is_empty_,
                is_datum_)
    if (is_datum_ && !is_empty_) {
      LST_DO_CODE(OB_UNIS_DECODE, datum_store_);
    } else if (!is_empty_){
      LST_DO_CODE(OB_UNIS_DECODE, row_store_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObWinbufWholeMsg)
{
  int ret = OB_SUCCESS;
  ret = ObDatahubWholeMsg::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE,
                is_empty_,
                is_datum_);
    if (is_datum_ && !is_empty_) {
      LST_DO_CODE(OB_UNIS_ENCODE, datum_store_);
    } else if (!is_empty_) {
      LST_DO_CODE(OB_UNIS_ENCODE, row_store_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWinbufWholeMsg)
{
  int64_t len = 0;
  len += ObDatahubWholeMsg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              is_empty_,
              is_datum_);
  if (is_datum_ && !is_empty_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, datum_store_);
  } else if (!is_empty_){
    LST_DO_CODE(OB_UNIS_ADD_LEN, row_store_);
  }
  return len;
}

int ObWinbufWholeMsg::assign(const ObWinbufWholeMsg &other, common::ObIAllocator *allocator)
 {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    allocator = &assign_allocator_;
  }
  ready_state_ = other.ready_state_;
  is_datum_ = other.is_datum_;
  is_empty_ = other.is_empty_;
  if (!is_empty_) {
    int64_t ser_len = 0;
    void *ser_ptr = NULL;
    int64_t ser_pos = 0;
    int64_t des_pos = 0;
    if (is_datum_) {
      datum_store_.reset();
      int64_t mem_limit = other.datum_store_.get_mem_limit();
      uint64_t tenant_id = other.datum_store_.get_tenant_id();
      int64_t ctx_id = other.datum_store_.get_mem_ctx_id();
      const char *label = other.datum_store_.get_label();
      if (OB_FAIL(datum_store_.init(mem_limit, tenant_id, ctx_id, label, false))) {
        LOG_WARN("init datum store failed", K(ret));
      } else if (OB_FAIL(datum_store_.append_datum_store(other.datum_store_))) {
        LOG_WARN("append store failed", K(ret));
      }
    } else {
      ser_len = other.row_store_.get_serialize_size();
      if (OB_ISNULL(ser_ptr = allocator->alloc(ser_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail alloc memory", K(ser_len), KP(ser_ptr), K(ret));
      } else if (OB_FAIL(other.row_store_.serialize(static_cast<char *>(ser_ptr),
            ser_len, ser_pos))) {
        LOG_WARN("fail serialzie init task arg", KP(ser_ptr), K(ser_len), K(ser_pos), K(ret));
      } else if (OB_FAIL(row_store_.deserialize(static_cast<const char *>(ser_ptr),
           ser_pos, des_pos))) {
        LOG_WARN("fail des task arg", KP(ser_ptr), K(ser_pos), K(des_pos), K(ret));
      } else if (ser_pos != des_pos) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("data_len and pos mismatch", K(ser_len), K(ser_pos), K(des_pos), K(ret));
      }
    }
  }
  return ret;
}

// ================ SPWinFuncPXPieceMsg/SPWinFuncPXWholeMsg
int SPWinFuncPXPieceMsgCtx::alloc_piece_msg_ctx(const SPWinFuncPXPieceMsg &pkt,
                                                ObPxCoordInfo &coord_info, ObExecContext &ctx,
                                                int64_t task_cnt, ObPieceMsgCtx *&msg_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(ctx.get_physical_plan_ctx())
      || OB_ISNULL(ctx.get_physical_plan_ctx()->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or physical plan ctx is null", K(ret), K(ctx.get_my_session()),
             K(ctx.get_physical_plan_ctx()), K(ctx.get_physical_plan_ctx()->get_phy_plan()));
  } else {
    uint64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
    common::ObMemAttr mem_attr(tenant_id, ObModIds::OB_SQL_WINDOW_LOCAL, ObCtxIds::WORK_AREA);
    void *buf = ctx.get_allocator().alloc(sizeof(SPWinFuncPXPieceMsgCtx));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      msg_ctx = new (buf) SPWinFuncPXPieceMsgCtx(
        pkt.op_id_, task_cnt, ctx.get_physical_plan_ctx()->get_timeout_timestamp(), tenant_id,
        ctx.get_physical_plan_ctx()->get_phy_plan()->get_batch_size(), mem_attr);
      LOG_DEBUG("init msg ctx", K(pkt.op_id_), K(task_cnt), K(tenant_id),
                K(ctx.get_physical_plan_ctx()->get_phy_plan()->get_batch_size()));
    }
  }
  return ret;
}

void SPWinFuncPXPieceMsgCtx::reset_resource()
{
  whole_msg_.reset();
  received_ = 0;
}

int SPWinFuncPXPieceMsgCtx::send_whole_msg(common::ObIArray<ObPxSqcMeta *> &sqcs)
{
  int ret = OB_SUCCESS;
  whole_msg_.op_id_ = op_id_;
  whole_msg_.is_empty_ = (!whole_msg_.row_store_.is_inited());
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    dtl::ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null channel", K(ret), K(ch));
    } else if (OB_FAIL(ch->send(whole_msg_, timeout_ts_))) {
      LOG_WARN("send data failed", K(ret));
    } else if (OB_FAIL(ch->flush(true, false))) {
      LOG_WARN("flush dtl data failed", K(ret));
    } else {
      LOG_DEBUG("dispatched sp_winfunc_px_whole_msg", K(idx), K(cnt), K(whole_msg_), K(*ch));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
    LOG_WARN("failed to wait response", K(ret));
  }
  return ret;
}

int SPWinFuncPXPieceMsgListener::on_message(SPWinFuncPXPieceMsgCtx &ctx,
                                            common::ObIArray<ObPxSqcMeta *> &sqcs,
                                            const SPWinFuncPXPieceMsg &pkt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pkt.op_id_ != ctx.op_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece msg", K(ret), K(pkt), K(ctx));
  } else if (ctx.received_ >= ctx.task_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already got all pkts, expect no more", K(ret));
  } else if (pkt.is_empty_) {
    // do nothing
  } else {
    if (!ctx.whole_msg_.row_store_.is_inited()) {
      // init row_store
      common::ObMemAttr attr(ctx.tenant_id_, ObModIds::OB_SQL_WINDOW_LOCAL, ObCtxIds::WORK_AREA);
      if (OB_FAIL(
            ctx.whole_msg_.row_meta_.deep_copy(pkt.row_meta_, &ctx.whole_msg_.assign_allocator_))) {
        LOG_WARN("deep copy row meta failed", K(ret));
      } else if (OB_FAIL(ctx.whole_msg_.row_store_.init(ctx.whole_msg_.row_meta_,
                                                        ctx.max_batch_size_, attr, 0, false,
                                                        NONE_COMPRESSOR))) {
        LOG_WARN("init row store failed", K(ret));
      }
    }
    ObCompactRow *sr = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx.whole_msg_.row_store_.add_row(pkt.row_, sr))) {
      LOG_WARN("add row failed", K(ret));
    } else {
    }
  }
  if (OB_SUCC(ret)) {
    ctx.received_++;
    LOG_TRACE("got a peice msg", "all_got", ctx.received_, "expected", ctx.task_cnt_,
             K(ctx.whole_msg_), K(pkt.thread_id_));
    if (ctx.received_ == ctx.task_cnt_) {
      if (OB_FAIL(ctx.send_whole_msg(sqcs))) {
        LOG_WARN("send whole msg failed", K(ret));
      } else {
        IGNORE_RETURN ctx.reset_resource();
      }
    }
  }
  return ret;
}

int SPWinFuncPXWholeMsg::assign(const SPWinFuncPXWholeMsg &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    allocator = &assign_allocator_;
  }
  row_meta_.set_allocator(allocator);
  is_empty_ = other.is_empty_;
  if (OB_FAIL(row_meta_.deep_copy(other.row_meta_, allocator))) {
    LOG_WARN("deep copy row meta failed", K(ret));
  } else if (!is_empty_) {
    // deep copy row store by ser/deser
    int64_t ser_len = 0, ser_pos = 0, des_pos = 0;
    void *ser_buf = nullptr;
    ser_len = other.row_store_.get_serialize_size();
    row_store_.reset();
    const int64_t constexpr read_batch = 256;
    int64_t read_rows = 0;
    const ObCompactRow *stored_rows[read_batch] = {nullptr};
    ObTempRowStore::Iterator other_it;
    ObTempRowStore &copying_store = const_cast<ObTempRowStore &>(other.row_store_);
    if (OB_FAIL(row_store_.init(other.row_store_.get_row_meta(),
                                other.row_store_.get_max_batch_size(),
                                other.row_store_.get_mem_attr(), 0, false, NONE_COMPRESSOR))) {
      LOG_WARN("init temp row store failed", K(ret));
    } else if (OB_FAIL(other_it.init(&copying_store))) {
      LOG_WARN("init row store iterator failed", K(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(other_it.get_next_batch(read_batch, read_rows, stored_rows))) {
        if (OB_LIKELY(ret == OB_ITER_END)) {
        } else {
          LOG_WARN("get batch rows failed", K(ret));
        }
      } else if (OB_UNLIKELY(read_rows == 0)) {
        ret = OB_ITER_END;
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        break;
      }
      ObCompactRow *sr_row = nullptr;
      for (int i = 0; OB_SUCC(ret) && i < read_rows; i++) {
        if (OB_FAIL(row_store_.add_row(stored_rows[i], sr_row))) {
          LOG_WARN("add row failed", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(SPWinFuncPXPieceMsg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDatahubPieceMsg::serialize(buf, buf_len, pos))) {
    LOG_WARN("datahub ser failed", K(ret));
  } else {
    OB_UNIS_ENCODE(is_empty_);
    OB_UNIS_ENCODE(row_meta_);
    if (OB_SUCC(ret) && !is_empty_) {
      OB_UNIS_ENCODE(row_->get_row_size());
      if (OB_SUCC(ret)) {
        MEMCPY(buf + pos, row_, row_->get_row_size());
        pos += row_->get_row_size();
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(SPWinFuncPXPieceMsg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDatahubPieceMsg::deserialize(buf, data_len, pos))) {
    LOG_WARN("datahub deser failed", K(ret));
  } else {
    OB_UNIS_DECODE(is_empty_);
    OB_UNIS_DECODE(row_meta_);
    if (OB_SUCC(ret) && !is_empty_) {
      int64_t row_size = 0;
      void *row_buf = nullptr;
      OB_UNIS_DECODE(row_size);
      if (OB_UNLIKELY(row_size <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row size", K(ret));
      } else {
        if (OB_ISNULL(row_buf = deserial_allocator_.alloc(row_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(row_buf, buf + pos, row_size);
          row_ = reinterpret_cast<ObCompactRow *>(row_buf);
          row_->set_row_size(row_size);
          pos += row_size;
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(SPWinFuncPXPieceMsg)
{
  int64_t len = 0;
  len += ObDatahubPieceMsg::get_serialize_size();
  OB_UNIS_ADD_LEN(is_empty_);
  OB_UNIS_ADD_LEN(row_meta_);
  if (!is_empty_) {
    OB_UNIS_ADD_LEN(row_->get_row_size());
    len += row_->get_row_size();
  }
  return len;
}

OB_DEF_SERIALIZE(SPWinFuncPXWholeMsg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDatahubWholeMsg::serialize(buf, buf_len, pos))) {
    LOG_WARN("whole msg ser failed", K(ret));
  } else {
    OB_UNIS_ENCODE(is_empty_);
    OB_UNIS_ENCODE(row_meta_);
    if (!is_empty_) {
      OB_UNIS_ENCODE(row_store_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(SPWinFuncPXWholeMsg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDatahubWholeMsg::deserialize(buf, data_len, pos))) {
    LOG_WARN("whole msg deser failed", K(ret));
  } else {
    OB_UNIS_DECODE(is_empty_);
    OB_UNIS_DECODE(row_meta_);
    if (!is_empty_) {
      OB_UNIS_DECODE(row_store_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(SPWinFuncPXWholeMsg)
{
  int64_t len = ObDatahubWholeMsg::get_serialize_size();
  OB_UNIS_ADD_LEN(is_empty_);
  OB_UNIS_ADD_LEN(row_meta_);
  if(!is_empty_) {
    OB_UNIS_ADD_LEN(row_store_);
  }
  return len;
}