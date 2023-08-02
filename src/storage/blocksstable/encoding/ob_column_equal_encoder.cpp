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

#define USING_LOG_PREFIX STORAGE

#include "ob_column_equal_encoder.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_integer_array.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

const ObColumnHeader::Type ObColumnEqualEncoder::type_;

ObColumnEqualEncoder::ObColumnEqualEncoder()
  : ref_col_idx_(-1), exc_row_ids_(), store_class_(ObMaxSC), ref_ctx_(NULL)
{
  exc_row_ids_.set_attr(ObMemAttr(MTL_ID(), "ColEqEncoder"));
}

ObColumnEqualEncoder::~ObColumnEqualEncoder()
{
}

int ObColumnEqualEncoder::init(
    const ObColumnEncodingCtx &ctx,
    const int64_t column_idx,
    const ObConstDatumRowArray &rows)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnEncoder::init(ctx, column_idx, rows))) {
    LOG_WARN("ObIColumnEncoder init failed", K(ret), K(ctx), K(column_idx));
  } else {
    column_header_.type_ = type_;
    store_class_ = get_store_class_map()[ob_obj_type_class(column_type_.get_type())];
    is_inited_ = true;
  }
  return ret;
}

int ObColumnEqualEncoder::set_ref_col_idx(const int64_t ref_col_idx,
                                          const ObColumnEncodingCtx &ref_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ref_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(ref_col_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ref_col_idx", K(ref_col_idx), K(ret));
  } else {
    ref_col_idx_ = ref_col_idx;
    ref_ctx_ = &ref_ctx;
  }
  return ret;
}

int64_t ObColumnEqualEncoder::get_ref_col_idx() const
{
  return ref_col_idx_;
}

void ObColumnEqualEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  ref_col_idx_ = -1;
  exc_row_ids_.reuse();
  store_class_ = ObMaxSC;
  ref_ctx_ = NULL;
  base_meta_writer_.reset();
}

int ObColumnEqualEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_type_ != ctx_->encoding_ctx_->col_descs_->at(ref_col_idx_).col_type_)) {
    suitable = false;
  } else {
    suitable = true;
    // in exception meta, fix_data_cnt_ is uint8_t,
    // to avoid overflow, we have to limit the max excepction count
    const int64_t max_exc_cnt = std::min(MAX_EXC_CNT, rows_->count() * EXC_THRESHOLD_PCT / 100 + 1);
    bool has_lob_header = is_lob_storage(column_type_.get_type());
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        column_type_.get_type(), column_type_.get_collation_type(), column_type_.get_scale(),
        lib::is_oracle_mode(), has_lob_header);
    ObCmpFunc cmp_func;
    cmp_func.cmp_func_ = lib::is_oracle_mode()
        ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;

    // get all exception row_ids
    for (int64_t row_id = 0; row_id < rows_->count() && OB_SUCC(ret)
         && exc_row_ids_.count() <= max_exc_cnt; ++row_id) {
      const ObDatum &datum = ctx_->col_datums_->at(row_id);
      const ObDatum &ref_datum = ref_ctx_->col_datums_->at(row_id);
      bool equal = false;
      if (OB_FAIL(is_datum_equal(datum, ref_datum, cmp_func, equal))) {
        LOG_WARN("cmp datum failed", K(ret), K(row_id));
      } else if (!equal && OB_FAIL(exc_row_ids_.push_back(row_id))) {
        LOG_WARN("push_back failed", K(ret), K(row_id));
      }
    }

    // init exception meta writer
    if (OB_SUCC(ret)) {
      if (exc_row_ids_.count() > max_exc_cnt) {
        suitable = false;
      } else if (exc_row_ids_.count() > 0) {
        switch (store_class_) {
          case ObIntSC:
          case ObUIntSC: {
            ObIntBitMapMetaWriter *meta_writer =
                static_cast<ObIntBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->init(&exc_row_ids_, ctx_->col_datums_, column_type_))) {
              LOG_WARN("init meta writer failed", K(ret));
            } else if (OB_FAIL(meta_writer->traverse_exc(suitable))) {
              LOG_WARN("meta writer traverse failed", K(ret));
            }
            break;
          }
          case ObNumberSC: {
            ObNumberBitMapMetaWriter *meta_writer =
                static_cast<ObNumberBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->init(&exc_row_ids_, ctx_->col_datums_, column_type_))) {
              LOG_WARN("init meta writer failed", K(ret));
            } else if (OB_FAIL(meta_writer->traverse_exc(suitable))) {
              LOG_WARN("meta writer traverse failed", K(ret));
            }
            break;
          }
          case ObStringSC:
          case ObTextSC:
          case ObJsonSC:
          case ObGeometrySC: {
            ObStringBitMapMetaWriter *meta_writer =
                static_cast<ObStringBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->init(&exc_row_ids_, ctx_->col_datums_, column_type_))) {
              LOG_WARN("init meta writer failed", K(ret));
            } else if (OB_FAIL(meta_writer->traverse_exc(suitable))) {
              LOG_WARN("meta writer traverse failed", K(ret));
            }
            break;
          }
          case ObOTimestampSC: {
            ObOTimestampBitMapMetaWriter *meta_writer =
                static_cast<ObOTimestampBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->init(&exc_row_ids_, ctx_->col_datums_, column_type_))) {
              LOG_WARN("init meta writer failed", K(ret));
            } else if (OB_FAIL(meta_writer->traverse_exc(suitable))) {
              LOG_WARN("meta writer traverse failed", K(ret));
            }
            break;
          }
          case ObIntervalSC: {
            ObIntervalBitMapMetaWriter *meta_writer =
                static_cast<ObIntervalBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->init(&exc_row_ids_, ctx_->col_datums_, column_type_))) {
              LOG_WARN("init meta writer failed", K(ret));
            } else if (OB_FAIL(meta_writer->traverse_exc(suitable))) {
              LOG_WARN("meta writer traverse failed", K(ret));
            }
            break;
          }
          default:
            ret = OB_INNER_STAT_ERROR;
            LOG_WARN("not supported store class", K(ret), K_(store_class), K_(column_type));
        }
      }
      if (OB_SUCC(ret)) {
        desc_.need_data_store_ = false;
        if (base_meta_writer_.is_bit_packing()) {
          column_header_.set_bit_packing_attr();
        }
        if (ctx_->encoding_ctx_->major_working_cluster_version_ > CLUSTER_VERSION_2276) {
          desc_.has_null_ = 0 != ctx_->null_cnt_;
          desc_.has_nope_ = 0 != ctx_->nope_cnt_;
        }
      }
    }
  }

  return ret;
}

int64_t ObColumnEqualEncoder::calc_size() const
{
  int64_t size = sizeof(ObColumnEqualMetaHeader);
  if (0 < exc_row_ids_.count()) {
    size += base_meta_writer_.size();
  }
  LOG_DEBUG("column equal size", K(size), K(column_index_));
  return size;
}

int ObColumnEqualEncoder::store_meta(ObBufferWriter &writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char *buf = writer.current();
    const int64_t size = calc_size();
    if (OB_FAIL(writer.advance_zero(size))) {
      LOG_WARN("advance failed", K(ret), K(size));
    } else {
      ObColumnEqualMetaHeader *header = reinterpret_cast<ObColumnEqualMetaHeader *>(buf);
      header->version_ = ObColumnEqualMetaHeader::OB_COLUMN_EQUAL_META_HEADER_V1;
      header->ref_col_idx_ = static_cast<uint16_t>(ref_col_idx_);
    }

    if (OB_SUCC(ret) && exc_row_ids_.count() > 0 ) {
      buf += sizeof(ObColumnEqualMetaHeader);
      // advance extra 8 bytes for safety bit packing
      if (OB_FAIL(writer.advance_zero(sizeof(uint64_t)))) {
        LOG_WARN("advance failed", K(ret));
      } else {
        switch (store_class_) {
          case ObIntSC:
          case ObUIntSC: {
            ObIntBitMapMetaWriter *meta_writer =
                static_cast<ObIntBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->write(buf))) {
              LOG_WARN("write meta failed", K(ret), KP(buf));
            }
            break;
          }
          case ObNumberSC: {
            ObNumberBitMapMetaWriter *meta_writer =
                static_cast<ObNumberBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->write(buf))) {
              LOG_WARN("write meta failed", K(ret), KP(buf));
            }
            break;
          }
          case ObStringSC:
          case ObTextSC:
          case ObJsonSC:
          case ObGeometrySC: {
            ObStringBitMapMetaWriter *meta_writer =
                static_cast<ObStringBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->write(buf))) {
              LOG_WARN("write meta failed", K(ret), KP(buf));
            }
            break;
          }
          case ObOTimestampSC: {
            ObOTimestampBitMapMetaWriter *meta_writer =
                static_cast<ObOTimestampBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->write(buf))) {
              LOG_WARN("write meta failed", K(ret), KP(buf));
            }
            break;
          }
          case ObIntervalSC: {
            ObIntervalBitMapMetaWriter *meta_writer =
                static_cast<ObIntervalBitMapMetaWriter *>(&base_meta_writer_);
            if (OB_FAIL(meta_writer->write(buf))) {
              LOG_WARN("write meta failed", K(ret), KP(buf));
            }
            break;
          }
          default:
            ret = OB_INNER_STAT_ERROR;
            LOG_WARN("not supported store class", K(ret), K_(store_class), K_(column_type));
        }
      }
      if (OB_SUCC(ret)) {
        // revert extra bytes
        if (OB_FAIL(writer.backward(sizeof(uint64_t)))) {
          LOG_ERROR("backword failed", K(ret));
        }
      }
    }
  }
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase

