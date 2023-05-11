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

#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_writer.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sql/das/ob_das_id_service.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObTenantStorageCheckpointWriter::ObTenantStorageCheckpointWriter()
  : is_inited_(false),
    allocator_(),
    tablet_item_addr_info_arr_(),
    ls_id_set_(),
    ls_item_writer_(),
    tablet_item_writer_()
{
}

int ObTenantStorageCheckpointWriter::init()
{
  int ret = OB_SUCCESS;
  const int64_t MEM_LIMIT = 128 << 20;  // 128M
  const char *MEM_LABEL = "TenantStorageCheckpointWriter";

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantStorageCheckpointWriter init twice", K(ret));
  } else if (OB_FAIL(allocator_.init(
               common::OB_MALLOC_NORMAL_BLOCK_SIZE, MEM_LABEL, MTL_ID(), MEM_LIMIT))) {
    LOG_WARN("fail to init allocator_", K(ret));
  } else if (OB_FAIL(ls_id_set_.create(128))) {
    LOG_WARN("fail to crete ls id set", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantStorageCheckpointWriter::reset()
{
  is_inited_ = false;
  allocator_.reset();
  tablet_item_addr_info_arr_.reset();
  ls_id_set_.clear();
  ls_item_writer_.reset();
  tablet_item_writer_.reset();
}

int ObTenantStorageCheckpointWriter::write_checkpoint(ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not inited", K(ret));
  } else if (OB_FAIL(write_ls_checkpoint(super_block.ls_meta_entry_))) {
    LOG_WARN("fail to write_ls_checkpoint", K(ret));
  } else if (OB_FAIL(write_tablet_checkpoint(super_block.replay_start_point_,
                                             super_block.tablet_meta_entry_))) {
    LOG_WARN("fail to write_tablet_checkpoint", K(ret));
  } else if (OB_FAIL(write_ls_dup_table_checkpoint(super_block.ls_dup_table_entry_))) {
    LOG_WARN("fail to write dup_table ls checkpoint", K(ret));
  } else if (OB_FAIL(THE_IO_DEVICE->fsync_block())) {
    LOG_WARN("fail to fsync_block", K(ret));
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::write_ls_checkpoint(blocksstable::MacroBlockId &entry_block)
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObLSMeta ls_meta;
  int64_t count = 0;

  ls_item_writer_.reset();
  if (OB_FAIL(ls_id_set_.clear())) {
    LOG_WARN("fail to clear ls id set", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream iter", K(ret));
  } else if (OB_FAIL(ls_item_writer_.init(false /*no need addr*/))) {
    LOG_WARN("failed to init logs tream item writer", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next log stream", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObLSLockGuard lock_ls(ls);
        if (OB_FAIL(ls->get_ls_meta(ls_meta))) {
          LOG_WARN("fail to get_ls_meta", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        count++;
        buf_len = ls_meta.get_serialize_size();
        pos = 0;
        if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_FAIL(ls_meta.serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize", K(ret));
        } else if (OB_FAIL(ls_item_writer_.write_item(buf, buf_len, nullptr))) {
          LOG_WARN("fail to write log stream item", K(ret));
        } else if (OB_FAIL(ls_id_set_.set_refactored(ls_meta.ls_id_))) {
          LOG_WARN("fail to set ls id", K(ret), K(ls_meta));
        }

        if (OB_LIKELY(nullptr != buf)) {
          allocator_.free(buf);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ls_item_writer_.close())) {
        LOG_WARN("fail to close log stream item writer", K(ret));
      } else if (OB_FAIL(ls_item_writer_.get_entry_block(entry_block))) {
        LOG_WARN("fail to get entry block", K(ret));
      }
    }

  }

  LOG_INFO("write ls checkpoint finish", K(ret), K(count), K(entry_block));

  return ret;
}

int ObTenantStorageCheckpointWriter::write_ls_dup_table_checkpoint(blocksstable::MacroBlockId &entry_block)
{
  int ret = OB_SUCCESS;

  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  int64_t count = 0;

  transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta dup_ls_meta;

  ls_item_writer_.reset();
  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream iter", K(ret));
  } else if (OB_FAIL(ls_item_writer_.init(false /*no need addr*/))) {
    LOG_WARN("failed to init logs tream item writer", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next log stream", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ls->get_dup_table_ls_meta(dup_ls_meta))) {
          LOG_WARN("fail to get_ls_meta", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        count++;
        buf_len = dup_ls_meta.get_serialize_size();
        pos = 0;
        if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_FAIL(dup_ls_meta.serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize", K(ret));
        } else if (OB_FAIL(ls_item_writer_.write_item(buf, buf_len, nullptr))) {
          LOG_WARN("fail to write log stream item", K(ret));
        }

        if (OB_LIKELY(nullptr != buf)) {
          allocator_.free(buf);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ls_item_writer_.close())) {
        LOG_WARN("fail to close log stream item writer", K(ret));
      } else if (OB_FAIL(ls_item_writer_.get_entry_block(entry_block))) {
        LOG_WARN("fail to get entry block", K(ret));
      }
    }

  }

  LOG_INFO("write ls dup_table checkpoint finish", K(ret), K(count), K(entry_block));
  return ret;
}

int ObTenantStorageCheckpointWriter::write_tablet_checkpoint(
  const common::ObLogCursor &cursor, blocksstable::MacroBlockId &entry_block)
{
  int ret = OB_SUCCESS;

  ObMetaDiskAddr addr;
  int64_t item_idx = -1;
  TabletItemAddrInfo addr_info;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSTabletIterator tablet_iter;
  ObTabletMapKey tablet_key;

  tablet_item_writer_.reset();
  if (OB_FAIL(tablet_item_writer_.init(true /*need addr*/))) {
    LOG_WARN("failed to init tablet item writer", K(ret));
  }

  for (hash::ObHashSet<share::ObLSID>::const_iterator it = ls_id_set_.begin();
      OB_SUCC(ret) && it != ls_id_set_.end(); it++) {
    if (OB_FAIL(MTL(ObLSService *)->get_ls(it->first, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      if (OB_LS_NOT_EXIST == ret) { // ls maybe delete when write tablet checkpoint
        ret = OB_SUCCESS;
        continue;
      } else {
        LOG_WARN("fail to get ls", K(ret), "ls_id", it->first);
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is nullptr", K(ret), "ls_id", it->first);
    } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
      LOG_WARN("fail to build ls tablet iter", K(ret), "ls_id", it->first);
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_iter.get_next_tablet_addr(tablet_key, addr))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next item", K(ret));
        }
      } else if (addr.is_file() && (addr.file_id() > cursor.file_id_ ||
          (addr.file_id() == cursor.file_id_ && addr.offset() >= cursor.offset_))) {
        // no need copy if addr exceeds the replay cursor
        LOG_INFO("skip copy tablet", K(cursor), K(addr), K(tablet_key));
      } else if (addr.is_memory()) {
        FLOG_INFO("skip MEM type", K(ret), K(tablet_key), K(addr));
      } else if (OB_FAIL(copy_one_tablet_item(tablet_item_writer_, addr, &item_idx))) {
        LOG_WARN("fail to copy_one_tablet_item", K(ret), K(tablet_key), K(addr));
      } else {
        addr_info.tablet_key_ = tablet_key;
        addr_info.item_idx_ = item_idx;
        addr_info.old_addr_ = addr;
        if (OB_FAIL(tablet_item_addr_info_arr_.push_back(addr_info))) {
          LOG_WARN("fail to push back addr info", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_item_writer_.close())) {
      LOG_WARN("fail to close tablet item writer", K(ret));
    } else if (OB_FAIL(tablet_item_writer_.get_entry_block(entry_block))) {
      LOG_WARN("fail to get entry block", K(ret));
    }
  }

  const int64_t tablet_count = tablet_item_addr_info_arr_.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; i++) {
    TabletItemAddrInfo &addr_info = tablet_item_addr_info_arr_.at(i);
    if (OB_FAIL(
          tablet_item_writer_.get_item_disk_addr(addr_info.item_idx_, addr_info.new_addr_))) {
      LOG_WARN("fail to get tablet item disk addr", K(ret), K(i), K(addr_info));
    }
  }

  FLOG_INFO("write tablet checkpoint finish", K(ret), K(tablet_count), K(entry_block));

  return ret;
}

// copy tablet item from old addr to new checkpoint
int ObTenantStorageCheckpointWriter::copy_one_tablet_item(
  ObLinkedMacroBlockItemWriter &tablet_item_writer, const ObMetaDiskAddr &addr, int64_t *item_idx)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  char *item_buf = nullptr;
  int64_t item_buf_len = 0;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;

  if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(addr.size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    if (addr.is_none()) {
      ret = OB_NEED_RETRY;  // may be just write tablet slog and not update the addr yet
      LOG_WARN("addr is none", K(ret));
    } else if (addr.is_file()) {
      if (OB_FAIL(read_tablet_from_slog(addr, buf, pos))) {
        LOG_WARN("fail to read_tablet_from_slog", K(ret), K(addr));
      } else {
        item_buf = buf + pos;
        item_buf_len = addr.size() - pos;
      }
    } else if (addr.is_block()) {
      if (OB_FAIL(slog_handler->read_tablet_checkpoint_by_addr(addr, buf, buf_len))) {
        LOG_WARN("fail to read_tablet_checkpoint_by_addr", K(ret), K(addr));
      } else {
        item_buf = buf;
        item_buf_len = buf_len;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid addr type", K(ret), K(addr));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_item_writer.write_item(item_buf, item_buf_len, item_idx))) {
    LOG_WARN("fail to write tablet item", K(ret));
  }

  if (OB_LIKELY(nullptr != buf)) {
    allocator_.free(buf);
  }

  return ret;
}

int ObTenantStorageCheckpointWriter::read_tablet_from_slog(
  const ObMetaDiskAddr &addr, char *buf, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObStorageLogReader::read_log(MTL(ObStorageLogger *)->get_dir(),
      addr, addr.size(), buf, pos, MTL_ID()))) {
    LOG_WARN("fail to read slog", K(ret));
  }

  return ret;
}

int ObTenantStorageCheckpointWriter::get_ls_block_list(common::ObIArray<MacroBlockId> *&block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not inited", K(ret));
  } else {
    ObIArray<MacroBlockId> &ls_block_list = ls_item_writer_.get_meta_block_list();
    block_list = &ls_block_list;
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::get_tablet_block_list(
  common::ObIArray<MacroBlockId> *&block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not inited", K(ret));
  } else {
    ObIArray<MacroBlockId> &tablet_block_list = tablet_item_writer_.get_meta_block_list();
    block_list = &tablet_block_list;
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::update_tablet_meta_addr()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not init", K(ret));
  }
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_item_addr_info_arr_.count(); i++) {
    const TabletItemAddrInfo &addr_info = tablet_item_addr_info_arr_.at(i);
    if (OB_FAIL(t3m->compare_and_swap_tablet_pure_address_without_object(
        addr_info.tablet_key_, addr_info.old_addr_, addr_info.new_addr_))) {
      if (OB_NOT_THE_OBJECT == ret) {  // addr may be change, ignore
        LOG_INFO("tablet addr changed when update_tablet_meta_addr", K(ret), K(addr_info));
        ret = OB_SUCCESS;
      } else if (OB_ENTRY_NOT_EXIST == ret) {  // may be deleted ignore
        LOG_INFO("tablet not exist when update_tablet_meta_addr", K(ret), K(addr_info));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to update_tablet_meta_addr", K(ret), K(addr_info));
        break;
      }
    }
  }

  return ret;
}

}  // namespace storage
}  // end namespace oceanbase
