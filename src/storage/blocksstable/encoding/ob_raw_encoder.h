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

#ifndef OCEANBASE_ENCODING_OB_RAW_ENCODER_H_
#define OCEANBASE_ENCODING_OB_RAW_ENCODER_H_

#include "ob_icolumn_encoder.h"
#include "ob_encoding_util.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{

class ObRawEncoder : public ObIColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::RAW;

  struct ValueGetter
  {
    explicit ValueGetter(const int64_t len)
    {
      mask_ = ObBitStream::get_mask(len);
    }

    inline int operator()(const int64_t, const ObDatum &datum, uint64_t &v)
    {
      v = datum.get_uint64() & mask_;
      return common::OB_SUCCESS;
    }

    uint64_t mask_;
  };

  ObRawEncoder();
  virtual ~ObRawEncoder();

  virtual int init(const ObColumnEncodingCtx &ctx,
      const int64_t column_index, const ObConstDatumRowArray &rows) override;
  virtual void reuse() override;

  virtual int set_data_pos(const int64_t offset, const int64_t length) override;
  virtual int get_var_length(const int64_t row_id, int64_t &length) override;
  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(
      const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len) override;

  virtual int traverse(bool &suitable) override;
  int traverse(const bool force_var_store, bool &suitable);
  virtual int64_t calc_size() const override;

  virtual ObColumnHeader::Type get_type() const override { return type_; }

  virtual int store_fix_data(ObBufferWriter &buf_writer) override;

private:
  struct DatumDataSetter;

private:
  ObObjTypeStoreClass store_class_;
  int64_t type_store_size_;
  int64_t null_cnt_;
  int64_t nope_cnt_;
  int64_t fix_data_size_; // -1 for var data store
  // max value for integer store type
  uint64_t max_integer_;
  int64_t var_data_size_; // for calculate size
};

OB_INLINE int ObRawEncoder::store_data(
    const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  const ObDatum &datum = rows_->at(row_id).get_datum(column_index_);
  const ObStoredExtValue ext_val = get_stored_ext_value(datum);
  if (STORED_NOT_EXT != ext_val) {
    if (OB_FAIL(bs.set(column_header_.extend_value_index_,
        extend_value_bit_, static_cast<int64_t>(ext_val)))) {
      STORAGE_LOG(WARN,"store extend value bit failed",
          K(ret), K_(column_header), K_(extend_value_bit), K(ext_val), K_(column_index));
    }
  } else if (!column_header_.is_fix_length() || column_header_.length_ > 0) { // need row value store
    switch (store_class_) {
      case ObIntSC:
      case ObUIntSC: {
        MEMCPY(buf, datum.ptr_, len);
        break;
      }
      case ObNumberSC:
      case ObStringSC:
      case ObTextSC: 
      case ObJsonSC:
      case ObGeometrySC:
      case ObOTimestampSC:
      case ObIntervalSC: {
        MEMCPY(buf, datum.ptr_, datum.len_);
        break;
      }
      default:
        ret = common::OB_INNER_STAT_ERROR;
        STORAGE_LOG(WARN,"not supported store class",
            K(ret), K_(store_class), K_(column_type), K(datum));
    }
  }
  return ret;
}


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_RAW_ENCODER_H_
