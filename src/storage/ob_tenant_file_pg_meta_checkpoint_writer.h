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

#ifndef OB_TENANT_FILE_PG_META_CHECKPOINT_WRITER_H_
#define OB_TENANT_FILE_PG_META_CHECKPOINT_WRITER_H_

#include "storage/ob_partition_meta_redo_module.h"
#include "storage/ob_tenant_file_struct.h"
#include "storage/ob_server_pg_meta_checkpoint_writer.h"

namespace oceanbase {
namespace storage {

class ObTenantFilePGMetaCheckpointWriter final {
public:
  ObTenantFilePGMetaCheckpointWriter();
  ~ObTenantFilePGMetaCheckpointWriter() = default;
  int init(ObPartitionMetaRedoModule& partition_meta_mgr, ObBaseFileMgr& pg_super_block_mgr,
      const ObTenantFileKey& tenant_file_key);
  int write_checkpoint();
  int get_file_checkpoint_entry(ObTenantFileCheckpointEntry& entry);
  void reset();

private:
  ObPartitionMetaRedoModule* partition_meta_mgr_;
  ObBaseFileMgr* server_file_mgr_;
  ObTenantFileKey tenant_file_key_;
  ObServerPGMetaCheckpointWriter writer_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_TENANT_FILE_PG_META_CHECKPOINT_WRITER_H_
