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

#ifndef OB_SERVER_PG_META_CHECKPOINT_WRITER_H_
#define OB_SERVER_PG_META_CHECKPOINT_WRITER_H_

#include "storage/ob_partition_service.h"
#include "storage/ob_tenant_file_struct.h"
#include "storage/ob_pg_meta_block_writer.h"

namespace oceanbase {
namespace storage {

struct ObPGTenantFileComparator final {
public:
  ObPGTenantFileComparator(int& ret) : ret_(ret)
  {}
  ~ObPGTenantFileComparator() = default;
  bool operator()(ObIPartitionGroup* left, ObIPartitionGroup* right);
  int ret_;
};

class ObServerPGMetaCheckpointWriter final {
public:
  ObServerPGMetaCheckpointWriter();
  ~ObServerPGMetaCheckpointWriter() = default;
  int init(ObPartitionMetaRedoModule& partition_meta_service, ObBaseFileMgr& server_file_mgr,
      const ObTenantFileKey& filter_tenant_file_key);
  int write_checkpoint();
  void reset();
  common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry>& get_file_checkpoint_map()
  {
    return file_checkpoint_map_;
  }

private:
  int write_tenant_file_checkpoint(const ObTenantFileKey& file_key, common::ObIArray<ObIPartitionGroup*>& pg_array);

private:
  static const int64_t MAX_FILE_CNT_PER_SERVER = 10000L;
  ObPartitionMetaRedoModule* partition_meta_mgr_;
  ObBaseFileMgr* server_file_mgr_;
  ObTenantFileFilter file_filter_;
  ObPGMetaItemWriter macro_meta_writer_;
  ObPGMetaItemWriter pg_meta_writer_;
  common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry> file_checkpoint_map_;
  ObIPartitionArrayGuard guard_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_SERVER_PG_META_CHECKPOINT_WRITER_H_
