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
#include "ob_backup_other_blocks_mgr.h"
#include "storage/backup/ob_backup_ctx.h"

namespace oceanbase
{
namespace backup
{

// ObBackupOtherBlockIdIterator

ObBackupOtherBlockIdIterator::ObBackupOtherBlockIdIterator()
  : is_inited_(false),
    tablet_id_(),
    sstable_ptr_(NULL),
    sst_meta_hdl_(),
    id_iterator_()
{
}

ObBackupOtherBlockIdIterator::~ObBackupOtherBlockIdIterator()
{
}

int ObBackupOtherBlockIdIterator::init(const common::ObTabletID &tablet_id, const blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup other blocks iterator init twice", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    sstable_ptr_ = &sstable;
    if (OB_FAIL(get_sstable_meta_handle_())) {
      LOG_WARN("failed to get sstable meta handle", K(ret));
    } else if (OB_FAIL(prepare_macro_id_iterator_())) {
      LOG_WARN("failed to prepare macro id iterator", K(ret));
    } else {
      is_inited_ = true;
      LOG_INFO("prepare backup other block id iterator", K(tablet_id));
    }
  }
  return ret;
}

int ObBackupOtherBlockIdIterator::get_sstable_meta_handle_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sstable_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable ptr should not be nul", K(ret), KP_(sstable_ptr));
  } else if (OB_FAIL(sstable_ptr_->get_meta(sst_meta_hdl_))) {
    LOG_WARN("failed to get meta", K(ret), KP_(sstable_ptr), K_(tablet_id));
  }
  return ret;
}

int ObBackupOtherBlockIdIterator::prepare_macro_id_iterator_()
{
  int ret = OB_SUCCESS;
  id_iterator_.reset();
  if (OB_UNLIKELY(!sst_meta_hdl_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable meta handle should not be invalid", K(ret), K(sst_meta_hdl_));
  } else if (OB_FAIL(sst_meta_hdl_.get_sstable_meta().get_macro_info().get_other_block_iter(id_iterator_))) {
    LOG_WARN("failed to get other block iter", K(ret), K_(tablet_id), KPC_(sstable_ptr));
  }
  return ret;
}

int ObBackupOtherBlockIdIterator::get_next_id(blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup other block id iterator do not init", K(ret));
  } else if (OB_FAIL(id_iterator_.get_next_macro_id(macro_id))) {
    LOG_WARN("failed to get next macro id", K(ret));
  } else {
    LOG_INFO("get next macro id", K(macro_id));
  }
  return ret;
}

// ObBackupOtherBlocksMgr

ObBackupOtherBlocksMgr::ObBackupOtherBlocksMgr()
  : is_inited_(false),
    mutex_(),
    tenant_id_(OB_INVALID_ID),
    tablet_id_(),
    table_key_(),
    total_other_block_count_(0),
    list_(),
    idx_(0)
{
}

ObBackupOtherBlocksMgr::~ObBackupOtherBlocksMgr()
{
}

int ObBackupOtherBlocksMgr::init(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const storage::ObITable::TableKey &table_key, const blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  int64_t total_other_block_count = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup other blocks mgr init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(tablet_id), K(table_key));
  } else if (!GCTX.is_shared_storage_mode() && !table_key.is_ddl_dump_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only ddl dump in ss mode allowed here", K(ret), K(table_key));
  } else if (OB_FAIL(get_total_other_block_count_(tablet_id, sstable, total_other_block_count))) {
    LOG_WARN("failed to get total other block count", K(ret), K(tablet_id), K(sstable));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
    table_key_ = table_key;
    total_other_block_count_ = total_other_block_count;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupOtherBlocksMgr::wait(ObLSBackupCtx *ls_backup_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup other blocks mgr do not init", K(ret));
  } else {
    static const int64_t WAIT_SLEEP_TIME = 10_ms;
    while (OB_SUCC(ret)) {
      const int64_t cur_list_count = get_list_count_();
      if (OB_ISNULL(ls_backup_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls backup ctx should not be null", K(ret));
      } else if (OB_SUCCESS != ls_backup_ctx->get_result_code()) {
        ret = OB_CANCELED;
        LOG_INFO("ls backup ctx already failed", K(ret), K(ret));
        break;
      } else if (total_other_block_count_ < cur_list_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur list count should not be greater than total block count",
            K(ret), K_(total_other_block_count), K(cur_list_count));
      } else if (total_other_block_count_ > cur_list_count) {
        sleep(WAIT_SLEEP_TIME);
        continue;
      } else {
        LOG_INFO("other block all has been backed up", K_(tablet_id), K_(tablet_id), K_(table_key), K_(total_other_block_count));
        break;
      }
    }
  }
  return ret;
}

int ObBackupOtherBlocksMgr::add_item(const ObBackupLinkedItem &link_item)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup other blocks mgr do not init", K(ret));
  } else if (!link_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(link_item));
  } else if (OB_FAIL(list_.push_back(link_item))) {
    LOG_WARN("failed to push back", K(ret), K(link_item));
  } else {
    LOG_INFO("add item", K(link_item));
  }
  return ret;
}

int ObBackupOtherBlocksMgr::get_next_item(ObBackupLinkedItem &link_item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup other blocks mgr do not init", K(ret));
  } else if (total_other_block_count_ != list_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("other block missing", K(ret), K_(total_other_block_count), "list_count", list_.count());
  } else if (idx_ >= list_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("reach end", K(ret), K(idx_), K_(list));
  } else {
    link_item = list_.at(idx_);
    idx_++;
  }
  return ret;
}

int ObBackupOtherBlocksMgr::get_total_other_block_count_(const common::ObTabletID &tablet_id,
    const blocksstable::ObSSTable &sstable, int64_t &total_count)
{
  int ret = OB_SUCCESS;
  total_count = 0;
  ObBackupOtherBlockIdIterator iter;
  if (OB_FAIL(iter.init(tablet_id, sstable))) {
    LOG_WARN("failed to init backup other block id iterator", K(ret));
  } else {
    MacroBlockId macro_id;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_id(macro_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next id", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        total_count++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get total other block count", K(total_count), K(tablet_id), K(sstable));
  }
  return ret;
}

int64_t ObBackupOtherBlocksMgr::get_list_count_() const
{
  ObMutexGuard guard(mutex_);
  return list_.count();
}

}
}
