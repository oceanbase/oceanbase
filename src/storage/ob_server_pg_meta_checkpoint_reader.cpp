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

#include "storage/ob_server_pg_meta_checkpoint_reader.h"
#include "storage/ob_server_pg_meta_checkpoint_writer.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_tenant_file_pg_meta_checkpoint_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObServerPGMetaCheckpointReader::ObServerPGMetaCheckpointReader()
{}

int ObServerPGMetaCheckpointReader::read_checkpoint(ObBaseFileMgr& file_mgr, ObPartitionMetaRedoModule& pg_mgr)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObArray<ObTenantFileInfo*> tenant_file_infos;
  ObStorageFilesHandle files_handle;
  if (OB_FAIL(file_mgr.get_all_tenant_file_infos(allocator, tenant_file_infos))) {
    LOG_WARN("fail to get all tenant file infos", K(ret));
  } else {
    ObTenantFilePGMetaCheckpointReader reader;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_file_infos.count(); ++i) {
      ObTenantFileInfo& file_info = *tenant_file_infos.at(i);
      if (file_info.is_normal_status()) {
        bool from_svr_ckpt = false;
        if (OB_FAIL(file_mgr.is_from_svr_ckpt(file_info.tenant_key_, from_svr_ckpt))) {
          LOG_WARN("check is from svr ckpt fail", K(ret), K(file_info));
        } else if (!from_svr_ckpt) {
          FLOG_INFO("skip load file ckpt", K(file_info), K(from_svr_ckpt));
        } else if (OB_FAIL(reader.read_checkpoint(
                       file_info.tenant_key_, file_info.tenant_file_super_block_, file_mgr, pg_mgr))) {
          LOG_WARN("fail to read checkpoint", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; i < tenant_file_infos.count(); ++i) {
    if (nullptr != tenant_file_infos.at(i)) {
      tenant_file_infos.at(i)->~ObTenantFileInfo();
      tenant_file_infos.at(i) = nullptr;
    }
  }
  tenant_file_infos.reset();
  return ret;
}
