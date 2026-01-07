/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_block_sstable_struct.h"
#include "ob_icolumn_decoder.h"
#include "ob_integer_array.h"
#include "ob_dict_decoder.h"
#include "ob_encoding_bitset.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

struct ObColumnEqualMetaHeader
{
  ObColumnEqualMetaHeader() : ref_col_idx_(0) {}

  void reset()
  {
    ref_col_idx_ = 0;
  }

  uint8_t ref_col_idx_;
  char payload_[0];

  TO_STRING_KV(K_(ref_col_idx));
}__attribute__((packed));

class ObColumnEqualDecoder : public ObSpanColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::COLUMN_EQUAL;
  ObColumnEqualDecoder();
  virtual ~ObColumnEqualDecoder();

  OB_INLINE int init(const common::ObObjMeta &obj_meta,
      const ObMicroBlockHeaderV2 &micro_block_header,
      const ObColumnHeader &column_header, const char *block_data);
  void reset();
  OB_INLINE void reuse();
  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  virtual int get_ref_col_idx(int64_t &ref_col_idx) const override;

  virtual ObColumnHeader::Type get_type() const override { return type_; }

  virtual bool can_vectorized() const override { return false; }

protected:
  inline bool has_exc(const ObColumnDecoderCtx &ctx) const
  { return ctx.col_header_->length_ > sizeof(ObColumnEqualMetaHeader); }
private:
  bool inited_;
  const ObColumnEqualMetaHeader *meta_header_;
};

OB_INLINE int ObColumnEqualDecoder::init(const common::ObObjMeta &obj_meta,
    const ObMicroBlockHeaderV2 &micro_block_header,
    const ObColumnHeader &column_header,
    const char *block_data)
{
  UNUSEDx(obj_meta, micro_block_header, column_header);
  int ret = common::OB_SUCCESS;
  // performance critical, don't check params
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta_header_ = reinterpret_cast<const ObColumnEqualMetaHeader *>
        (block_data + column_header.offset_);
    inited_ = true;
  }
  return ret;
}

OB_INLINE void ObColumnEqualDecoder::reuse()
{
  inited_ = false;
  /*
  ref_decoder_ = NULL;
  obj_meta_.reset();
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  opt_.reset();
  meta_reader_.reuse();
  */
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
