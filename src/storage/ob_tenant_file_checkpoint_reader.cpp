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

#define USING_LOG_PREFIX STORAGE

#include "ob_tenant_file_checkpoint_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantFileCheckpointReader::ObTenantFileCheckpointReader() : pg_meta_reader_()
{}

int ObTenantFileCheckpointReader::read_checkpoint(const ObTenantFileKey& file_key,
    const ObTenantFileSuperBlock& tenant_file_super_block, ObBaseFileMgr& file_mgr, ObPartitionMetaRedoModule& pg_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_key.is_valid() || !tenant_file_super_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key), K(tenant_file_super_block));
  } else if (OB_FAIL(pg_meta_reader_.read_checkpoint(file_key, tenant_file_super_block, file_mgr, pg_mgr))) {
    LOG_WARN("fail to read checkpoint", K(ret));
  }
  return ret;
}
