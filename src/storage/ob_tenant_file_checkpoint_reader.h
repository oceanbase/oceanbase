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

#ifndef OB_TENANT_FILE_CHECKPOINT_READER_H_
#define OB_TENANT_FILE_CHECKPOINT_READER_H_

#include "storage/ob_tenant_file_pg_meta_checkpoint_reader.h"

namespace oceanbase {
namespace storage {

class ObTenantFileCheckpointReader final {
public:
  ObTenantFileCheckpointReader();
  ~ObTenantFileCheckpointReader() = default;
  int read_checkpoint(const ObTenantFileKey& file_key, const ObTenantFileSuperBlock& super_block,
      ObBaseFileMgr& file_mgr, ObPartitionMetaRedoModule& pg_mgr);

private:
  ObTenantFilePGMetaCheckpointReader pg_meta_reader_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_TENANT_FILE_CHECKPOINT_READER_H_
