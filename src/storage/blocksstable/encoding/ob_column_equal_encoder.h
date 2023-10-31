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

#ifndef OCEANBASE_ENCODING_OB_COLUMN_EQUAL_ENCODER_H_
#define OCEANBASE_ENCODING_OB_COLUMN_EQUAL_ENCODER_H_

#include "ob_encoding_bitset.h"
//#include "ob_icolumn_encoder.h"
//#include "ob_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnEqualMetaHeader
{
  static constexpr uint8_t OB_COLUMN_EQUAL_META_HEADER_V1 = 0;
  ObColumnEqualMetaHeader() : version_(OB_COLUMN_EQUAL_META_HEADER_V1), ref_col_idx_(0) {}

  void reset()
  {
    ref_col_idx_ = 0;
  }
  uint8_t version_;
  uint16_t ref_col_idx_;
  char payload_[0];

  TO_STRING_KV(K_(ref_col_idx));
}__attribute__((packed));

class ObColumnEqualEncoder : public ObSpanColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::COLUMN_EQUAL;

  ObColumnEqualEncoder();
  virtual ~ObColumnEqualEncoder();

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_idx,
      const ObConstDatumRowArray &rows) override;
  virtual int set_ref_col_idx(const int64_t ref_col_idx,
                              const ObColumnEncodingCtx &ref_ctx) override;
  virtual int64_t get_ref_col_idx() const override;
  virtual void reuse() override;

  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual int get_encoding_store_meta_need_space(int64_t &need_size) const override;

  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(const int64_t row_id, ObBitStream &bs,
                         char *buf, const int64_t len) override
  {
    UNUSEDx(row_id, bs, buf, len);
    return common::OB_NOT_SUPPORTED;
  }
  virtual ObColumnHeader::Type get_type() const override { return type_; }
  virtual int store_fix_data(ObBufferWriter &buf_writer) override
  {
    UNUSED(buf_writer);
    return common::OB_NOT_SUPPORTED;
  }
private:
  OB_INLINE int is_datum_equal(
      const common::ObDatum &left,
      const common::ObDatum &right,
      const common::ObCmpFunc &cmp_func,
      bool &equal) const;

  int64_t ref_col_idx_;
  common::ObArray<int64_t> exc_row_ids_;
  ObObjTypeStoreClass store_class_;
  ObBitMapMetaBaseWriter base_meta_writer_;
  const ObColumnEncodingCtx *ref_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObColumnEqualEncoder);
};

OB_INLINE int ObColumnEqualEncoder::is_datum_equal(
      const common::ObDatum &left,
      const common::ObDatum &right,
      const common::ObCmpFunc &cmp_func,
      bool &equal) const
{
  int ret = OB_SUCCESS;
  equal = false;
  const bool is_int_type = ObIntSC == store_class_ || ObUIntSC == store_class_;
  const bool need_binary_equal =
      store_class_ != ObNumberSC && store_class_ != ObOTimestampSC && store_class_ != ObIntervalSC;
  ObStoredExtValue left_ext = get_stored_ext_value(left);
  ObStoredExtValue right_ext = get_stored_ext_value(right);
  if (left_ext != right_ext) {
    equal = false;
  } else if (left.is_null() || left.is_nop()) {
    equal = true;
  } else if (is_int_type) {
    equal = (left.get_uint64() == right.get_uint64());
  } else if (need_binary_equal) {
    equal = ObDatum::binary_equal(left, right);
  } else {
    int cmp_ret = 0;
    ret = cmp_func.cmp_func_(left, right, cmp_ret);
    equal = (0 == cmp_ret);
  }
  return ret;
}


}//end namespace blocksstable
}//end namespace oceanbase

#endif //OCEANBASE_ENCODING_OB_COLUMN_DIFF_ENCODER_H_
