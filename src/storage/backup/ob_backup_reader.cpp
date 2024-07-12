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

#include "storage/backup/ob_backup_reader.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/random/ob_random.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/ls/ob_ls.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tablet/ob_mds_schema_helper.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObLSTabletIdReader */

ObLSTabletIdReader::ObLSTabletIdReader()
    : is_inited_(false), backup_dest_(), tenant_id_(OB_INVALID_ID), backup_set_desc_(), store_()
{}

ObLSTabletIdReader::~ObLSTabletIdReader()
{}

int ObLSTabletIdReader::init(const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
    const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!backup_dest.is_valid() || OB_INVALID_ID == tenant_id || !backup_set_desc.is_valid() ||
             !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(tenant_id), K(backup_set_desc), K(ls_id));
  } else if (OB_FAIL(store_.init(backup_dest, backup_set_desc))) {
    LOG_WARN("failed to init extern info mgr", K(ret), K(backup_dest), K(tenant_id), K(backup_set_desc));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    tenant_id_ = tenant_id;
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    is_inited_ = true;
  }
  return ret;
}

int ObLSTabletIdReader::get_tablet_id_list(
    share::ObBackupDataType &backup_data_type, const int64_t turn_id,
    const share::ObLSID &ls_id, common::ObIArray<common::ObTabletID> &tablet_list)
{
  int ret = OB_SUCCESS;
  if (turn_id < 0 || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(turn_id), K(ls_id));
  } else if (backup_data_type.is_sys_backup()) {
    ObLS::ObLSInnerTabletIDIter tablet_iter;
    ObTabletID tablet_id;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_iter.get_next(tablet_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet id", K(ret));
        }
      } else if (OB_FAIL(tablet_list.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    }
  } else if (OB_FAIL(store_.read_tablet_list(backup_data_type, turn_id, ls_id, tablet_list))) {
    LOG_WARN("failed to read minor tablet to ls info", K(ret), K(turn_id), K(ls_id));
  }
  return ret;
}

/* ObITabletLogicMacroIdReader */

ObITabletLogicMacroIdReader::ObITabletLogicMacroIdReader()
    : is_inited_(false), batch_size_(0), tablet_id_(), table_key_(), sstable_(NULL), tablet_handle_(NULL)
{}

ObITabletLogicMacroIdReader::~ObITabletLogicMacroIdReader()
{}

/* ObTabletLogicMacroIdReader */

ObTabletLogicMacroIdReader::ObTabletLogicMacroIdReader()
    : ObITabletLogicMacroIdReader(), lock_(common::ObLatchIds::BACKUP_LOCK), allocator_(), datum_range_(), meta_iter_()
{}

ObTabletLogicMacroIdReader::~ObTabletLogicMacroIdReader()
{}

int ObTabletLogicMacroIdReader::init(const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key, const blocksstable::ObSSTable &sstable, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const storage::ObITableReadInfo *index_read_info = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!tablet_id.is_valid() || !tablet_handle.is_valid() || !table_key.is_valid() || !sstable.is_valid() ||
             batch_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id), K(table_key), K(batch_size));
  } else if (FALSE_IT(datum_range_.set_whole_range())) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_sstable_read_info(&sstable, index_read_info))) {
    LOG_WARN("failed to get index read info ", KR(ret), K(sstable));
  } else if (OB_FAIL(meta_iter_.open(datum_range_,
                 ObMacroBlockMetaType::DATA_BLOCK_META,
                 sstable,
                 *index_read_info,
                 allocator_))) {
    LOG_WARN("failed to open sec meta iterator", K(ret));
  } else {
    tablet_id_ = tablet_id;
    table_key_ = table_key;
    sstable_ = &sstable;
    batch_size_ = batch_size;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletLogicMacroIdReader::get_next_batch(common::ObIArray<ObBackupMacroBlockId> &id_array)
{
  int ret = OB_SUCCESS;
  id_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro id reader do not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    ObDataMacroBlockMeta data_macro_block_meta;
    while (OB_SUCC(ret)) {
      data_macro_block_meta.reset();
      if (id_array.count() >= MACRO_BLOCK_BATCH_SIZE) {
        break;
      } else if (OB_FAIL(meta_iter_.get_next(data_macro_block_meta))) {
        if (OB_ITER_END == ret) {
          // if iter end, do not log warn
        } else {
          LOG_WARN("failed to get next", K(ret), K_(tablet_id));
        }
      } else {
        ObBackupMacroBlockId macro_id;
        macro_id.logic_id_ = data_macro_block_meta.get_logic_id();
        macro_id.macro_block_id_ = data_macro_block_meta.get_macro_id();
        macro_id.nested_offset_ = data_macro_block_meta.nested_offset_;
        macro_id.nested_size_ = data_macro_block_meta.nested_size_;
        if (OB_FAIL(id_array.push_back(macro_id))) {
          LOG_WARN("failed to push back", K(ret), K(macro_id));
        }
      }
    }
  }
  return ret;
}

/* ObIMacroBlockBackupReader */

ObIMacroBlockBackupReader::ObIMacroBlockBackupReader() : is_inited_(false), logic_id_(), block_info_()
{}

ObIMacroBlockBackupReader::~ObIMacroBlockBackupReader()
{}

/* ObMacroBlockBackupReader */

ObMacroBlockBackupReader::ObMacroBlockBackupReader()
    : ObIMacroBlockBackupReader(), is_data_ready_(false), result_code_(), macro_handle_(),
      buffer_reader_()
{}

ObMacroBlockBackupReader::~ObMacroBlockBackupReader()
{}

int ObMacroBlockBackupReader::init(const ObBackupMacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(macro_id));
  } else {
    is_data_ready_ = false;
    logic_id_ = macro_id.logic_id_;
    block_info_.macro_id_ = macro_id.macro_block_id_;
    block_info_.nested_offset_ = macro_id.nested_offset_;
    block_info_.nested_size_ = macro_id.nested_size_;
    is_inited_ = true;
  }
  return ret;
}

int ObMacroBlockBackupReader::get_macro_block_data(
    blocksstable::ObBufferReader &buffer_reader, blocksstable::ObLogicMacroBlockId &logic_id, ObIAllocator *io_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(process_(io_allocator))) {
    LOG_WARN("failed to process", K(ret));
  } else if (OB_FAIL(result_code_)) {
    LOG_WARN("error happened during fetch macro block", K(ret));
  } else if (!is_data_ready_) {
    ret = OB_ERR_SYS;
    LOG_WARN("macro block data not ready", K(ret));
  } else {
    buffer_reader = buffer_reader_;
    logic_id = logic_id_;
  }
  return ret;
}

void ObMacroBlockBackupReader::reset()
{
  is_inited_ = false;
  is_data_ready_ = false;
  result_code_ = OB_SUCCESS;
  macro_handle_.reset();
  block_info_.reset();
}

int ObMacroBlockBackupReader::process_(ObIAllocator *io_allocator)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockReadInfo read_info;
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  if (is_data_ready_) {
    LOG_INFO("macro data is ready, no need fetch", K(ret));
  } else if (OB_FAIL(get_macro_read_info_(logic_id_, read_info))) {
    LOG_WARN("failed to get macro block read info", K(ret), K(logic_id_));
  } else if (OB_ISNULL(io_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io allocator is null", K(ret));
  } else if (OB_ISNULL(read_info.buf_ =
      reinterpret_cast<char*>(io_allocator->alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(read_info.size_));
  } else {
    if (OB_FAIL(ObBlockManager::read_block(read_info, macro_handle_))) {
      LOG_WARN("failed to read block", K(ret), K(read_info));
    } else if (!macro_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read handle is not valid, cannot wait", K(ret), K(macro_handle_));
    } else if (OB_FAIL(macro_handle_.wait())) {
      LOG_WARN("failed to wait macro handle", K(ret), K(read_info));
    } else {
      buffer_reader_.assign(read_info.buf_, macro_handle_.get_data_size());
      int64_t size = 0;
      if (OB_FAIL(get_macro_block_size_(buffer_reader_, size))) {
        LOG_WARN("failed to get macro block size", K(ret), K(buffer_reader_));
      } else {
        buffer_reader_.set_pos(size);
        LOG_INFO("fetch macro block data", K(logic_id_));
      }
    }
  }

  if (OB_SUCC(ret) && !is_data_ready_) {
    is_data_ready_ = true;
  } else {
    result_code_ = ret;
  }
  return ret;
}

int ObMacroBlockBackupReader::get_macro_read_info_(
    const blocksstable::ObLogicMacroBlockId &logic_id, blocksstable::ObMacroBlockReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!logic_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id));
  } else {
    read_info.macro_block_id_ = block_info_.macro_id_;
    read_info.offset_ = block_info_.nested_offset_;
    read_info.size_ = block_info_.nested_size_;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_READ);
    read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    read_info.io_desc_.set_sys_module_id(ObIOModule::BACKUP_READER_IO);
  }
  return ret;
}

int ObMacroBlockBackupReader::get_macro_block_size_(const blocksstable::ObBufferReader &buffer, int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;
  if (buffer.capacity() < common_header.get_serialize_size()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("data buf not enough for ObMacroBlockCommonHeader",
        K(ret),
        K(buffer),
        "need_size",
        common_header.get_serialize_size());
  } else if (OB_FAIL(common_header.deserialize(buffer.data(), buffer.capacity(), pos))) {
    LOG_ERROR("deserialize common header fail", K(ret), K(buffer), K(pos), K(common_header));
  } else if (buffer.capacity() - pos < ObSSTableMacroBlockHeader::get_fixed_header_size()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("data buf not enough for ObSSTableMacroBlockHeader",
        K(ret),
        K(buffer),
        "need_size",
        ObSSTableMacroBlockHeader::get_fixed_header_size());
  } else {
    const int64_t header_size = common_header.get_serialize_size();
    const int64_t payload_size = common_header.get_payload_size();
    size = header_size + payload_size;
  }
  return ret;
}

/* ObMultiMacroBlockBackupReader */

ObMultiMacroBlockBackupReader::ObMultiMacroBlockBackupReader()
    : is_inited_(false), read_size_(0), reader_idx_(0), reader_type_(), macro_list_(), readers_()
{}

ObMultiMacroBlockBackupReader::~ObMultiMacroBlockBackupReader()
{
  reset();
}

int ObMultiMacroBlockBackupReader::init(const uint64_t tenant_id,
    const ObMacroBlockReaderType &reader_type, const common::ObIArray<ObBackupMacroBlockId> &list)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_UNLIKELY(list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("list can not be empty", K(ret));
  } else if (OB_FAIL(macro_list_.assign(list))) {
    LOG_WARN("failed to assign list", K(ret), K(list));
  } else {
    readers_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      ObIMacroBlockBackupReader *reader = NULL;
      if (OB_FAIL(alloc_macro_block_reader_(tenant_id, reader_type, reader))) {
        LOG_WARN("failed to alloc macro block reader", K(ret), K(reader_type));
      } else if (OB_FAIL(readers_.push_back(reader))) {
        LOG_WARN("failed to push back reader", K(ret));
        reader->~ObIMacroBlockBackupReader();
      }
    }
    if (OB_SUCC(ret)) {
      reader_idx_ = 0;
      read_size_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMultiMacroBlockBackupReader::get_next_macro_block(
    blocksstable::ObBufferReader &data, blocksstable::ObLogicMacroBlockId &logic_id, ObIAllocator *io_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (reader_idx_ >= readers_.count()) {
    ret = OB_ITER_END;
    LOG_DEBUG("multi macro block backup reader get end", K(ret));
  } else if (OB_FAIL(fetch_macro_block_with_retry_(data, logic_id, io_allocator))) {
    LOG_WARN("failed to fetch macro block with retry", K(ret));
  } else {
    ++reader_idx_;
  }
  return ret;
}

void ObMultiMacroBlockBackupReader::reset()
{
  for (int64_t i = 0; i < readers_.count(); ++i) {
    if (OB_NOT_NULL(readers_.at(i))) {
      readers_.at(i)->reset();
      ObLSBackupFactory::free(readers_.at(i));
    }
  }
  is_inited_ = false;
  LOG_INFO("free multi macro block backup reader");
}

void ObMultiMacroBlockBackupReader::reset_prev_macro_block_reader_(const int64_t idx)
{
  const int64_t prev_idx = idx - 1;
  if (prev_idx >= 0 && prev_idx < readers_.count()) {
    if (OB_NOT_NULL(readers_.at(prev_idx))) {
      readers_.at(prev_idx)->reset();
    }
  }
}

int ObMultiMacroBlockBackupReader::alloc_macro_block_reader_(const uint64_t tenant_id,
    const ObMacroBlockReaderType &reader_type, ObIMacroBlockBackupReader *&reader)
{
  int ret = OB_SUCCESS;
  ObIMacroBlockBackupReader *tmp_reader = NULL;
  if (OB_ISNULL(tmp_reader = ObLSBackupFactory::get_macro_block_backup_reader(reader_type, tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get macro block backup reader", K(ret));
  } else {
    reader = tmp_reader;
  }
  return ret;
}

int ObMultiMacroBlockBackupReader::prepare_macro_block_reader_(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= readers_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(idx), K(readers_.count()));
  } else {
    readers_.at(idx)->reset();
    const ObBackupMacroBlockId &macro_id = macro_list_.at(idx);
    if (OB_FAIL(readers_.at(idx)->init(macro_id))) {
      LOG_WARN("failed to init reader", K(ret), K(idx), K(macro_id));
    }
  }
  return ret;
}

int ObMultiMacroBlockBackupReader::fetch_macro_block_with_retry_(
    blocksstable::ObBufferReader &data, blocksstable::ObLogicMacroBlockId &logic_id, ObIAllocator *io_allocator)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  while (retry_times < OB_MAX_RETRY_TIMES) {
    if (retry_times >= 1) {
      LOG_WARN("retry get macro block", K(retry_times));
    }
    if (OB_FAIL(fetch_macro_block_(data, logic_id, io_allocator))) {
      LOG_WARN("failed to fetch macro block", K(ret), K(retry_times));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_BACKUP_READ_MACRO_BLOCK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        SERVER_EVENT_SYNC_ADD("backup", "fetch_macro_block_failed",
                              "logic_id", logic_id);
        DEBUG_SYNC(AFTER_BACKUP_FETCH_MACRO_BLOCK_FAILED);
      }
    }
#endif
    if (OB_SUCC(ret)) {
      break;
    }
    if (OB_FAIL(ret)) {
      retry_times++;
      ob_usleep(FETCH_MACRO_BLOCK_RETRY_INTERVAL);
    }
  }
  return ret;
}

int ObMultiMacroBlockBackupReader::fetch_macro_block_(
    blocksstable::ObBufferReader &data, blocksstable::ObLogicMacroBlockId &logic_id, ObIAllocator *io_allocator)
{
  int ret = OB_SUCCESS;
  const int64_t idx = reader_idx_;
  reset_prev_macro_block_reader_(idx);
  if (idx < 0 || idx >= readers_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(idx));
  } else if (OB_FAIL(prepare_macro_block_reader_(idx))) {
    LOG_WARN("failed to prepare macro block reader", K(ret));
  } else if (OB_FAIL(readers_.at(idx)->get_macro_block_data(data, logic_id, io_allocator))) {
    LOG_WARN("failed to get macro block data", K(ret), K(idx), K(readers_.count()));
  }
  return ret;
}

/* ObITabletMetaBackupReader */

ObITabletMetaBackupReader::ObITabletMetaBackupReader()
    : is_inited_(false), tablet_id_(), backup_data_type_(), tablet_handle_(NULL)
{}

ObITabletMetaBackupReader::~ObITabletMetaBackupReader()
{}

/* ObTabletMetaBackupReader */

ObTabletMetaBackupReader::ObTabletMetaBackupReader() : ObITabletMetaBackupReader(), buffer_writer_("BackupReader")
{}

ObTabletMetaBackupReader::~ObTabletMetaBackupReader()
{}

int ObTabletMetaBackupReader::init(const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type,
    storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    backup_data_type_ = backup_data_type;
    tablet_handle_ = &tablet_handle;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletMetaBackupReader::get_meta_data(blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(tablet_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is null", K(ret));
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_BACKUP_READ_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else {
    ObTablet &tablet = *tablet_handle_->get_obj();
    ObBackupTabletMeta backup_tablet_meta;
    backup_tablet_meta.tablet_id_ = tablet_id_;
    if (OB_FAIL(tablet.build_migration_tablet_param(backup_tablet_meta.tablet_meta_))) {
      LOG_WARN("failed to build migration tablet param", K(ret), K(tablet_id_));
    } else if (!backup_tablet_meta.tablet_meta_.ha_status_.is_valid_for_backup()) {
      ret = OB_REPLICA_CANNOT_BACKUP;
      LOG_WARN("tablet meta is not valid for backup", K(ret), K(backup_tablet_meta));
    } else if (OB_FAIL(buffer_writer_.write_serialize(backup_tablet_meta))) {
      LOG_WARN("failed to write serialize", K(ret), K(backup_tablet_meta));
    } else {
      buffer_reader.assign(buffer_writer_.data(), buffer_writer_.length(), buffer_writer_.length());
    }
  }
  return ret;
}

/* ObSSTableMetaBackupReader */

ObSSTableMetaBackupReader::ObSSTableMetaBackupReader()
  : ObITabletMetaBackupReader(), sstable_array_(), buffer_writer_("BackupReader"), table_store_wrapper_()
{}

ObSSTableMetaBackupReader::~ObSSTableMetaBackupReader()
{}

int ObSSTableMetaBackupReader::init(const common::ObTabletID &tablet_id,
    const share::ObBackupDataType &backup_data_type, storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    backup_data_type_ = backup_data_type;
    tablet_handle_ = &tablet_handle;
    ObTablet &tablet = *tablet_handle_->get_obj();
    if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper_))) {
      LOG_WARN("failed to fetch table store from tablet", K(ret));
    } else if (OB_FAIL(ObBackupUtils::get_sstables_by_data_type(
        tablet_handle, backup_data_type_, *table_store_wrapper_.get_member(), sstable_array_))) {
      LOG_WARN("failed to get sstables by data type", K(ret), K(tablet_handle), K_(backup_data_type));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTableMetaBackupReader::get_meta_data(blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("do not init", K(ret));
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_BACKUP_READ_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array_.count(); ++i) {
      int64_t pos = 0;
      ObSSTable *sstable_ptr = sstable_array_.at(i).get_sstable();
      if (OB_ISNULL(sstable_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table ptr", K(ret), K(i));
      } else {
        // TODO(COLUMN_STORE) Attention !!! column store table key is COSSTable now! maybe should adapt.
        const ObITable::TableKey &table_key = sstable_ptr->get_key();
        ObTablet *tablet = tablet_handle_->get_obj();
        ObBackupSSTableMeta backup_sstable_meta;
        backup_sstable_meta.tablet_id_ = tablet_id_;
        if ((backup_data_type_.is_major_backup() && !sstable_ptr->is_major_sstable())
            || (backup_data_type_.is_minor_backup() && !sstable_ptr->is_minor_sstable() && !sstable_ptr->is_ddl_dump_sstable() && !sstable_ptr->is_mds_sstable())) {
          ret = OB_ERR_SYS;
          LOG_WARN("get incorrect table type", K(ret), K(i), K_(backup_data_type), KP(sstable_ptr));
        } else if (OB_FAIL(tablet->build_migration_sstable_param(table_key, backup_sstable_meta.sstable_meta_))) {
          LOG_WARN("failed to build migration sstable param", K(ret), K(table_key));
        } else if (OB_FAIL(get_macro_block_id_list_(*sstable_ptr, backup_sstable_meta))) {
          LOG_WARN("failed to get macro block id list", K(ret), KPC(sstable_ptr));
        } else if (OB_FAIL(buffer_writer_.write_serialize(backup_sstable_meta))) {
          LOG_WARN("failed to write serialize", K(ret), K(table_key), K(backup_sstable_meta));
        } else {
          LOG_INFO("backup sstable meta", K(table_key), K(tablet_id_), K(backup_sstable_meta));
        }
      }
    }
    if (OB_SUCC(ret)) {
      buffer_reader.assign(buffer_writer_.data(), buffer_writer_.length(), buffer_writer_.length());
    }
  }
  return ret;
}

int ObSSTableMetaBackupReader::get_macro_block_id_list_(
    const blocksstable::ObSSTable &sstable, ObBackupSSTableMeta &sstable_meta)
{
  int ret = OB_SUCCESS;
  if (!sstable.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(sstable));
  } else if (OB_ISNULL(tablet_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle should not be null", K(ret));
  } else if (OB_FAIL(ObBackupUtils::fetch_macro_block_logic_id_list(
                 *tablet_handle_, sstable, sstable_meta.logic_id_list_))) {
    LOG_WARN("failed to fetch macro block logic id list", K(ret), KPC_(tablet_handle), K(sstable));
  }
  return ret;
}

/* ObTabletPhysicalIDMetaBackupReader */

ObTabletPhysicalIDMetaBackupReader::ObTabletPhysicalIDMetaBackupReader()
    : is_inited_(false), ctx_(NULL), tablet_id_(), buffer_writer_("BackupReader")
{}

ObTabletPhysicalIDMetaBackupReader::~ObTabletPhysicalIDMetaBackupReader()
{}

int ObTabletPhysicalIDMetaBackupReader::init(const common::ObTabletID &tablet_id, ObBackupTabletCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> table_ptr_list;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!tablet_id.is_valid() || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id), KP(ctx));
  } else {
    ctx_ = ctx;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }

  return ret;
}

int ObTabletPhysicalIDMetaBackupReader::get_meta_data(blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("do not init", K(ret));
  } else if (OB_FAIL(check_ctx_completed_())) {
    LOG_WARN("failed to check ctx completed", K(ret));
  } else if (OB_FAIL(buffer_writer_.ensure_space(OB_BACKUP_READ_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_writer_.write_serialize(ctx_->mappings_))) {
    LOG_WARN("failed to write serialize", K(ret), KPC(ctx_));
  } else {
    buffer_reader.assign(buffer_writer_.data(), buffer_writer_.length(), buffer_writer_.length());
  }
  return ret;
}

int ObTabletPhysicalIDMetaBackupReader::check_ctx_completed_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (!ctx_->is_all_loaded_) {
    ret = OB_ERR_SYS;
    LOG_WARN("ctx is not all loaded", K(ret), KPC_(ctx));
  } else {
    const int64_t sstable_count = ctx_->mappings_.sstable_count_;
    const ObBackupMacroBlockIDMappingsMeta &mappings = ctx_->mappings_;
    const ObBackupMacroBlockIDMapping *mapping = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_count; ++i) {
      if (OB_ISNULL(mapping = mappings.id_map_list_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("id mapping should not be null", K(ret), K(i), K(mappings));
      } else {
        const ObArray<ObBackupMacroBlockIDPair> &pair_list = mapping->id_pair_list_;
        for (int64_t j = 0; OB_SUCC(ret) && j < pair_list.count(); ++j) {
          const ObBackupMacroBlockIDPair &pair = pair_list.at(j);
          if (pair.physical_id_ == ObBackupPhysicalID::get_default()) {
            ret = OB_ERR_SYS;
            ctx_->print_ctx();
            LOG_WARN("backup macro block is not completed", K(ret), K(pair));
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
