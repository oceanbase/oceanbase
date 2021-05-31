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

#include "ob_server_pg_meta_checkpoint_writer.h"
#include "lib/container/ob_array_iterator.h"
#include "ob_pg_all_meta_checkpoint_writer.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

bool ObPGTenantFileComparator::operator()(ObIPartitionGroup* left, ObIPartitionGroup* right)
{
  bool bret = false;
  if (OB_SUCCESS != ret_) {
    LOG_WARN("ObPGTenantFileComparator compare failed", K(ret_));
  } else if (OB_UNLIKELY(nullptr == left || nullptr == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret_), KP(left), KP(right));
  } else {
    ObStorageFile* left_file = left->get_storage_file();
    ObStorageFile* right_file = right->get_storage_file();
    if (OB_UNLIKELY(nullptr == left_file || nullptr == right_file)) {
      ret_ = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret_), KP(left_file), KP(right_file));
    } else {
      const int64_t left_tenant_id = left_file->get_tenant_id();
      const int64_t right_tenant_id = right_file->get_tenant_id();
      if (left_tenant_id == right_tenant_id) {
        bret = left_file->get_file_id() < right_file->get_file_id();
      } else {
        bret = left_tenant_id < right_tenant_id;
      }
    }
  }
  return bret;
}

ObServerPGMetaCheckpointWriter::ObServerPGMetaCheckpointWriter()
    : partition_meta_mgr_(nullptr),
      server_file_mgr_(nullptr),
      file_filter_(),
      macro_meta_writer_(),
      pg_meta_writer_(),
      is_inited_(false)
{}

int ObServerPGMetaCheckpointWriter::init(ObPartitionMetaRedoModule& partition_meta_mgr, ObBaseFileMgr& server_file_mgr,
    const ObTenantFileKey& filter_tenant_file_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerPGMetaCheckpointWriter has not been inited", K(ret));
  } else if (OB_FAIL(file_checkpoint_map_.create(
                 MAX_FILE_CNT_PER_SERVER, ObModIds::OB_TENANT_FILE_MGR, ObModIds::OB_TENANT_FILE_MGR))) {
    LOG_WARN("fail to create file checkpoint entry map", K(ret));
  } else {
    partition_meta_mgr_ = &partition_meta_mgr;
    server_file_mgr_ = &server_file_mgr;
    file_filter_.set_filter_tenant_file_key(filter_tenant_file_key);
    is_inited_ = true;
  }
  return ret;
}

void ObServerPGMetaCheckpointWriter::reset()
{
  partition_meta_mgr_ = nullptr;
  server_file_mgr_ = nullptr;
  file_filter_.reset();
  macro_meta_writer_.reset();
  pg_meta_writer_.reset();
  file_checkpoint_map_.destroy();
  guard_.reuse();
  is_inited_ = false;
}

int ObServerPGMetaCheckpointWriter::write_checkpoint()
{
  int ret = OB_SUCCESS;
  ObArray<ObIPartitionGroup*> pg_array;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerPGMetaCheckpointWriter has not been inited", K(ret));
  } else if (OB_FAIL(partition_meta_mgr_->get_all_partitions(guard_))) {
    LOG_WARN("fail to get all partitions", K(ret));
  } else if (guard_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < guard_.count(); ++i) {
      ObIPartitionGroup* pg = guard_.at(i);
      if (OB_FAIL(pg_array.push_back(pg))) {
        LOG_WARN("fail to push back pg", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObPGTenantFileComparator comparator(ret);
      std::sort(pg_array.begin(), pg_array.end(), comparator);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to sort pg array", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObIPartitionGroup*> tenant_file_pg_array;
      ObTenantFileKey last_tenant_file_key(
          pg_array.at(0)->get_storage_file()->get_tenant_id(), pg_array.at(0)->get_storage_file()->get_file_id());
      ObTenantFileKey curr_tenant_file_key;
      int64_t tenant_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < pg_array.count(); ++i) {
        const int64_t curr_tenant_id = pg_array.at(i)->get_storage_file()->get_tenant_id();
        const int64_t curr_file_id = pg_array.at(i)->get_storage_file()->get_file_id();
        curr_tenant_file_key.tenant_id_ = curr_tenant_id;
        curr_tenant_file_key.file_id_ = curr_file_id;
        if (last_tenant_file_key != curr_tenant_file_key) {
          if (tenant_file_pg_array.count() > 0) {
            ++tenant_count;
            if (OB_FAIL(write_tenant_file_checkpoint(last_tenant_file_key, tenant_file_pg_array))) {
              LOG_WARN("fail to write tenant file checkpoint", K(ret));
            } else {
              LOG_INFO("end write tenant file checkpoint", K(last_tenant_file_key), K(tenant_count));
            }
#ifdef ERRSIM
            if (OB_SUCC(ret)) {
              ret = E(EventTable::EN_SERVER_PG_META_WRITE_HALF_FAILED) OB_SUCCESS;
              if (OB_FAIL(ret)) {
                STORAGE_LOG(INFO, "sim error write checkpoint", K(i), K(ret));
                if (tenant_count >= 1) {
                  STORAGE_LOG(WARN, "server pg meta write checkpoint half fail", K(ret));
                } else {
                  ret = OB_SUCCESS;
                }
              }
            }
#endif
          }

          if (OB_SUCC(ret)) {
            last_tenant_file_key = curr_tenant_file_key;
            tenant_file_pg_array.reuse();
            if (OB_FAIL(tenant_file_pg_array.push_back(pg_array.at(i)))) {
              LOG_WARN("fail to push back tenant file pg array", K(ret));
            }
          }
        } else {
          if (OB_FAIL(tenant_file_pg_array.push_back(pg_array.at(i)))) {
            LOG_WARN("fail to push back pg array", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && tenant_file_pg_array.count() > 0) {
        if (OB_FAIL(write_tenant_file_checkpoint(last_tenant_file_key, tenant_file_pg_array))) {
          LOG_WARN("fail to write tenant file checkpoint", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerPGMetaCheckpointWriter::write_tenant_file_checkpoint(
    const ObTenantFileKey& file_key, common::ObIArray<ObIPartitionGroup*>& pg_array)
{
  int ret = OB_SUCCESS;
  ObStorageFile* file = nullptr;
  bool is_filtered = false;
  macro_meta_writer_.reset();
  pg_meta_writer_.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerPGMetaCheckpointWriter has not been inited", K(ret));
  } else if (OB_FAIL(file_filter_.is_filtered(file_key, is_filtered))) {
    LOG_WARN("fail to check tenant file is filtered", K(ret), K(file_key));
  } else if (is_filtered) {
    // do nothing
  } else if (OB_UNLIKELY(pg_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_array.count()));
  } else if (OB_ISNULL(file = pg_array.at(0)->get_storage_file())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, file must not be null", K(ret));
  } else if (OB_FAIL(macro_meta_writer_.init(pg_array.at(0)->get_storage_file_handle()))) {
    LOG_WARN("fail to init writer", K(ret));
  } else if (OB_FAIL(pg_meta_writer_.init(pg_array.at(0)->get_storage_file_handle()))) {
    LOG_WARN("fail to init writer", K(ret));
  } else {
    ObPGCheckpointInfo checkpoint_info;
    ObPGMetaItem pg_meta;
    ObPGAllMetaCheckpointWriter pg_checkpoint_writer;
    ObArenaAllocator allocator(ObModIds::OB_CHECKPOINT);
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_array.count(); ++i) {
      pg_checkpoint_writer.reset();
      allocator.reuse();
      ObIPartitionGroup* pg = pg_array.at(i);
      if (OB_ISNULL(pg)) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, pg must not be null", K(ret));
      } else if (OB_FAIL(pg->get_checkpoint_info(allocator, checkpoint_info))) {
        LOG_WARN("fail to get pg checkpoint info", K(ret));
      } else if (OB_FAIL(pg_checkpoint_writer.init(checkpoint_info, file, macro_meta_writer_, pg_meta_writer_))) {
        LOG_WARN("fail to init pg checkpoint writer", K(ret));
      } else if (OB_FAIL(pg_checkpoint_writer.write_checkpoint())) {
        LOG_WARN("fail to write checkpoint", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObSuperBlockMetaEntry macro_meta_entry;
      ObSuperBlockMetaEntry pg_meta_entry;
      ObTenantFileInfo org_file_info;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(macro_meta_writer_.close())) {
        LOG_WARN("fail to close macro meta writer", K(ret));
      } else if (OB_FAIL(pg_meta_writer_.close())) {
        LOG_WARN("fail to close pg meta writer", K(ret));
      } else if (OB_FAIL(macro_meta_writer_.get_entry_block_index(macro_meta_entry.macro_block_id_))) {
        LOG_WARN("fail to get macro meta entry block index", K(ret));
      } else if (OB_FAIL(pg_meta_writer_.get_entry_block_index(pg_meta_entry.macro_block_id_))) {
        LOG_WARN("fail to get entry block index", K(ret));
      } else if (OB_FAIL(server_file_mgr_->get_tenant_file_info(file_key, org_file_info))) {
        LOG_WARN("fail to get tenant file info", K(ret), K(file_key));
      } else {
        ObTenantFileSuperBlock tenant_file_super_block = org_file_info.tenant_file_super_block_;
        ObArray<MacroBlockId> meta_block_list;
        tenant_file_super_block.macro_meta_entry_ = macro_meta_entry;
        tenant_file_super_block.pg_meta_entry_ = pg_meta_entry;
        ObIArray<MacroBlockId>& macro_meta_block_list = macro_meta_writer_.get_meta_block_list();
        ObIArray<MacroBlockId>& pg_meta_block_list = pg_meta_writer_.get_meta_block_list();
        ObTenantFileKey file_key(file->get_tenant_id(), file->get_file_id());
        for (int64_t i = 0; OB_SUCC(ret) && i < macro_meta_block_list.count(); ++i) {
          if (OB_FAIL(meta_block_list.push_back(macro_meta_block_list.at(i)))) {
            LOG_WARN("fail to push back meta block", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < pg_meta_block_list.count(); ++i) {
          if (OB_FAIL(meta_block_list.push_back(pg_meta_block_list.at(i)))) {
            LOG_WARN("fail to push back meta block", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObTenantFileCheckpointEntry file_entry;
          file_entry.tenant_file_key_ = file_key;
          file_entry.super_block_ = tenant_file_super_block;
          file_entry.meta_block_handle_.set_storage_file(pg_array.at(0)->get_storage_file_handle().get_storage_file());
          if (OB_FAIL(file_entry.meta_block_handle_.add_macro_blocks(meta_block_list, true /*switch handle*/))) {
            LOG_WARN("fail to add macro blocks", K(ret));
          } else if (OB_FAIL(file_checkpoint_map_.set_refactored(file_key, file_entry))) {
            LOG_WARN("fail to set refactored file checkpoint entry map", K(ret));
          } else {
            LOG_INFO("end write tenant file checkpoint", K(file_key), K(tenant_file_super_block));
          }
        }
      }
    }
  }
  return ret;
}
