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

#ifndef OB_TENANT_FILE_CHECKPOINT_WRITER_H_
#define OB_TENANT_FILE_CHECKPOINT_WRITER_H_

#include "storage/ob_tenant_file_pg_meta_checkpoint_writer.h"

namespace oceanbase {
namespace storage {

class ObTenantFileCheckpointWriter final {
public:
  ObTenantFileCheckpointWriter();
  ~ObTenantFileCheckpointWriter() = default;
  int init(
      ObPartitionMetaRedoModule& pg_meta_mgr, ObBaseFileMgr& server_file_mgr, const ObTenantFileKey& tenant_file_key);
  int write_checkpoint();
  void reset();

private:
  ObTenantFilePGMetaCheckpointWriter pg_meta_writer_;
  ObTenantFileKey tenant_file_key_;
  ObBaseFileMgr* file_mgr_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_TENANT_FILE_CHECKPOINT_WRITER_H_
