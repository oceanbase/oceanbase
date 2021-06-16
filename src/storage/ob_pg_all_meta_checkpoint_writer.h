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

#ifndef OB_PG_ALL_META_CHECKPOINT_WRITER_H_
#define OB_PG_ALL_META_CHECKPOINT_WRITER_H_

#include "storage/ob_pg_macro_meta_checkpoint_writer.h"
#include "storage/ob_pg_meta_checkpoint_writer.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace storage {

struct ObPGCheckpointInfo final {
public:
  ObPGCheckpointInfo();
  ~ObPGCheckpointInfo() = default;
  bool is_valid() const
  {
    return nullptr != pg_meta_buf_;
  }
  int assign(const ObPGCheckpointInfo& other);
  void reset();
  TO_STRING_KV(K_(tables_handle), KP_(pg_meta_buf), K_(pg_meta_buf_len));
  ObTablesHandle tables_handle_;
  const char* pg_meta_buf_;
  int64_t pg_meta_buf_len_;
};

class ObPGAllMetaCheckpointWriter final {
public:
  ObPGAllMetaCheckpointWriter();
  ~ObPGAllMetaCheckpointWriter() = default;
  int init(ObPGCheckpointInfo& pg_checkpoint_info, blocksstable::ObStorageFile* file,
      ObPGMetaItemWriter& macro_meta_writer, ObPGMetaItemWriter& pg_meta_writer);
  int write_checkpoint();
  void reset();

private:
  bool is_inited_;
  ObPGMacroMetaCheckpointWriter macro_meta_writer_;
  ObPGMetaCheckpointWriter pg_meta_writer_;
  ObPGCheckpointInfo pg_checkpoint_info_;
  blocksstable::ObStorageFile* file_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_ALL_META_CHECKPOINT_WRITER_H_
