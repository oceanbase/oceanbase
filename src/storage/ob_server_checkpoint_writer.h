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

#ifndef _OB_SERVER_CHECKPOINT_WRITER_H
#define _OB_SERVER_CHECKPOINT_WRITER_H

#include "storage/ob_server_pg_meta_checkpoint_writer.h"
#include "storage/ob_tenant_file_super_block_checkpoint_writer.h"
#include "storage/ob_tenant_config_meta_checkpoint_writer.h"

namespace oceanbase {
namespace storage {

class ObServerCheckpointWriter final {
public:
  static ObServerCheckpointWriter& get_instance();
  int init();
  int write_checkpoint(const common::ObLogCursor& checkpoint);
  int write_file_checkpoint(const ObTenantFileKey& file_key);
  int set_meta_block_list(const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  void reset();

private:
  ObServerCheckpointWriter();
  ~ObServerCheckpointWriter() = default;
  int update_tenant_file_super_block(common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry>& hash_map);

private:
  static const int64_t MAX_SERVER_META_MACRO_CNT = 10L;
  static const int64_t FILE_CNT_PER_SERVER = 10000L;
  ObServerPGMetaCheckpointWriter meta_writer_;
  ObTenantFileSuperBlockCheckpointWriter tenant_file_super_block_writer_;
  ObTenantConfigMetaCheckpointWriter tenant_config_meta_writer_;
  ObMetaBlockListHandle meta_block_handle_;
  ObBucketLock checkpoint_lock_;
  common::TCRWLock meta_lock_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  //_OB_SERVER_CHECKPOINT_WRITER_H
