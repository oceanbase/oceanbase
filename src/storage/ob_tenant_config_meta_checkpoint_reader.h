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

#ifndef _OB_TENANT_CONFIG_META_CHECKPOINT_READER_H
#define _OB_TENANT_CONFIG_META_CHECKPOINT_READER_H

#include "storage/ob_pg_meta_block_reader.h"

namespace oceanbase {
namespace storage {

class ObTenantConfigMetaCheckpointReader final {
public:
  ObTenantConfigMetaCheckpointReader();
  ~ObTenantConfigMetaCheckpointReader() = default;
  int init(const blocksstable::MacroBlockId& entry_block, blocksstable::ObStorageFileHandle& file_handle);
  int read_checkpoint();
  common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();

private:
  bool is_inited_;
  ObPGMetaItemReader reader_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // _OB_TENANT_CONFIG_META_CHECKPOINT_READER_H
