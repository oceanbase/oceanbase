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

#ifndef OCEANBASE_ENCODING_OB_INTEGER_BASE_DIFF_ENCODER_H_
#define OCEANBASE_ENCODING_OB_INTEGER_BASE_DIFF_ENCODER_H_


#include "ob_icolumn_encoder.h"
#include "ob_encoding_util.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObIntegerBaseDiffHeader
{
  static constexpr uint8_t OB_INTEGER_BASE_DIFF_HEADER_V1 = 0;
  uint8_t version_;
  uint8_t length_;

  ObIntegerBaseDiffHeader() : version_(OB_INTEGER_BASE_DIFF_HEADER_V1), length_(0)
  {
  }

  TO_STRING_KV(K_(length));
} __attribute__((packed));

class ObIntegerBaseDiffEncoder : public ObIColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::INTEGER_BASE_DIFF;

  ObIntegerBaseDiffEncoder();
  virtual ~ObIntegerBaseDiffEncoder() {}

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_index,
      const ObConstDatumRowArray &rows) override;

  virtual void reuse() override;
  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(
      const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len) override
  {
    UNUSEDx(row_id, bs, buf, len);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual ObColumnHeader::Type get_type() const { return type_; }
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;
private:
  class ObIIntegerData
  {
  public:
    virtual ~ObIIntegerData() {}

    virtual uint64_t max_delta() const = 0;
    virtual uint64_t max_unsign_value() const = 0;
    virtual uint64_t delta(const common::ObDatum &datum) const = 0;
    virtual uint64_t base() const = 0;
  };

  template <typename T>
  class ObIntegerData : public ObIIntegerData
  {
  public:
    explicit ObIntegerData(ObIntegerBaseDiffEncoder &encoder);
    OB_INLINE void traverse_cell(const common::ObDatum &datum);
    virtual uint64_t max_delta() const override;
    virtual uint64_t max_unsign_value() const override;
    virtual uint64_t delta(const common::ObDatum &datum) const override;
    virtual uint64_t base() const override
    { return *reinterpret_cast<const uint64_t *>(&min_); }
    void reuse();
  private:
    OB_INLINE uint64_t cast_to_uint64(const uint64_t v) const { return v; }
  private:
    T min_;
    T max_;
    ObIntegerBaseDiffEncoder &encoder_;
  };

public:
  struct DeltaGetter
  {
    explicit DeltaGetter(ObIIntegerData *integer_data) : integer_data_(integer_data) {}
    inline int operator()(const int64_t, const common::ObDatum &datum, uint64_t &v)
    {
      v = integer_data_->delta(datum);
      return common::OB_SUCCESS;
    }

    ObIIntegerData *integer_data_;
  };

  struct FixDataSetter
  {
    explicit FixDataSetter(ObIIntegerData *integer_data) : integer_data_(integer_data) {}
    inline int operator()(
        const int64_t,
        const common::ObDatum &datum,
        char *buf,
        const int64_t len) const
    {
      // performance critical, do not check parameters
      uint64_t v = integer_data_->delta(datum);
      MEMCPY(buf, &v, len);
      return common::OB_SUCCESS;
    }

    ObIIntegerData *integer_data_;
  };

private:
  template <typename T>
  int traverse_cells(T &integer_data);

private:
  ObObjTypeStoreClass store_class_;
  int64_t type_store_size_;
  uint64_t mask_;
  uint64_t reverse_mask_;
  ObIIntegerData *integer_data_;
  ObIntegerData<int64_t> int64_data_;
  ObIntegerData<uint64_t> uint64_data_;
  // is null before write meta
  ObIntegerBaseDiffHeader *header_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_INTEGER_BASE_DIFF_ENCODER_H_
