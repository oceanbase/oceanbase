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

#ifndef OB_PG_META_CHECKPOINT_WRITER_H_
#define OB_PG_META_CHECKPOINT_WRITER_H_

#include "storage/ob_pg_meta_block_writer.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace storage {

class ObPGMetaItem : public ObIPGMetaItem {
public:
  ObPGMetaItem();
  virtual ~ObPGMetaItem() = default;
  virtual int serialize(const char*& buf, int64_t& buf_len) override;
  virtual int16_t get_item_type() const override
  {
    return PG_META;
  }
  void set_serialize_buf(const char* buf, const int64_t buf_len);
  bool is_valid() const
  {
    return nullptr != buf_;
  }
  TO_STRING_KV(KP_(buf), K_(buf_len));

private:
  const char* buf_;
  int64_t buf_len_;
};

// sstable meta and pg meta
class ObPGMetaCheckpointWriter final {
public:
  ObPGMetaCheckpointWriter();
  ~ObPGMetaCheckpointWriter() = default;
  int init(ObPGMetaItem& pg_meta, ObPGMetaItemWriter& writer);
  int write_checkpoint();
  void reset();

private:
  bool is_inited_;
  ObPGMetaItemWriter* writer_;
  ObPGMetaItem pg_meta_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_META_CHECKPOINT_WRITER_H_
