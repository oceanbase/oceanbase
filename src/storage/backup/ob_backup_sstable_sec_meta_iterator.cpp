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

#include "storage/backup/ob_backup_sstable_sec_meta_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_restore_struct.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace backup {

ObBackupSSTableSecMetaIterator::ObBackupSSTableSecMetaIterator()
    : is_inited_(false), output_idx_(-1), tablet_id_(),
      table_key_(), allocator_(), table_handle_(),
      datum_range_(), sec_meta_iterator_(), mod_()
{
  // TODO:yangyi.yyy, adapt mod
  mod_.storage_id_ = 1;
  mod_.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
}

ObBackupSSTableSecMetaIterator::~ObBackupSSTableSecMetaIterator() {}

int ObBackupSSTableSecMetaIterator::init(
    const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle,
    const storage::ObITable::TableKey &table_key,
    const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &backup_set_desc,
    ObBackupMetaIndexStore &meta_index_store)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  ObBackupDataType backup_data_type;
  ObBackupPath backup_path;
  ObArray<ObBackupSSTableMeta> sstable_meta_array;
  ObBackupSSTableMeta *sstable_meta_ptr = NULL;
  ObTabletCreateSSTableParam create_sstable_param;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("iterator init twice", K(ret));
  } else if (OB_FAIL(get_backup_data_type_(table_key, backup_data_type))) {
    LOG_WARN("failed to get backup data type", K(ret), K(table_key));
  } else if (OB_FAIL(
                 get_meta_index_(tablet_id, meta_index_store, meta_index))) {
    LOG_WARN("failed to get meta index", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_backup_data_path_(backup_dest, backup_set_desc,
                                           backup_data_type, meta_index,
                                           backup_path))) {
    LOG_WARN("failed to get backup data path", K(ret), K(backup_data_type),
             K(meta_index));
  } else if (OB_FAIL(read_backup_sstable_metas_(
                 backup_dest, backup_path, meta_index, sstable_meta_array))) {
    LOG_WARN("failed to read backup sstable metas", K(ret), K(backup_path),
             K(meta_index));
  } else if (OB_FAIL(get_backup_sstable_meta_ptr_(table_key, sstable_meta_array,
                                                  sstable_meta_ptr))) {
    LOG_WARN("failed to get backup sstable meta ptr", K(ret), K(table_key));
  } else if (OB_FAIL(build_create_sstable_param_(tablet_handle,
                                                 *sstable_meta_ptr,
                                                 create_sstable_param))) {
    LOG_WARN("failed to build create sstable param", K(ret),
             KPC(sstable_meta_ptr));
  } else if (OB_FAIL(create_tmp_sstable_(create_sstable_param))) {
    LOG_WARN("failed to create tmp sstable", K(ret), K(create_sstable_param));
  } else if (OB_FAIL(init_sstable_sec_meta_iter_(tablet_handle))) {
    LOG_WARN("failed to init sstable sec meta iter", K(ret), K(tablet_id));
  } else {
    output_idx_ = 0;
    tablet_id_ = tablet_id;
    table_key_ = table_key;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::get_next(ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (OB_FAIL(sec_meta_iterator_.get_next(macro_meta))) {
    LOG_WARN("failed to get next", K(ret));
  } else {
    LOG_INFO("get next macro block meta", K_(output_idx),K_(tablet_id), K_(table_key), K(macro_meta));
    output_idx_++;
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::get_backup_data_type_(
    const storage::ObITable::TableKey &table_key,
    share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key, backup_data_type))) {
    LOG_WARN("failed to get backup data type", K(ret), K(table_key));
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::get_meta_index_(
    const common::ObTabletID &tablet_id,
    ObBackupMetaIndexStore &meta_index_store, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  const ObBackupMetaType meta_type = BACKUP_SSTABLE_META;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(tablet_id));
  } else if (OB_FAIL(meta_index_store.get_backup_meta_index(
                 tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id));
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::get_backup_data_path_(
    const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &backup_set_desc,
    const ObBackupDataType &backup_data_type,
    const ObBackupMetaIndex &meta_index, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  // backup and restore different mode
  if (!backup_data_type.is_valid() || !meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_data_type), K(meta_index));
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(
                 backup_dest, meta_index.ls_id_,
                 backup_data_type, meta_index.turn_id_, meta_index.retry_id_,
                 meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(backup_dest),
             K(backup_set_desc), K(backup_data_type), K(meta_index));
  } else {
    LOG_INFO("get backup data path", K(backup_path));
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::read_backup_sstable_metas_(
    const share::ObBackupDest &backup_dest,
    const share::ObBackupPath &backup_path, const ObBackupMetaIndex &meta_index,
    common::ObIArray<ObBackupSSTableMeta> &sstable_meta_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLSBackupRestoreUtil::read_sstable_metas(backup_path.get_obstr(),
                                                        backup_dest.get_storage_info(),
                                                        mod_,
                                                        meta_index,
                                                        sstable_meta_array))) {
    LOG_WARN("failed to read tablet meta", K(ret), K(backup_path),
             K(meta_index));
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::get_backup_sstable_meta_ptr_(
    const storage::ObITable::TableKey &table_key,
    common::ObArray<ObBackupSSTableMeta> &backup_metas,
    ObBackupSSTableMeta *&ptr)
{
  int ret = OB_SUCCESS;
  ptr = NULL;
  ARRAY_FOREACH_X(backup_metas, idx, cnt, OB_SUCC(ret)) {
    const ObBackupSSTableMeta &sstable_meta = backup_metas.at(idx);
    if (!sstable_meta.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable meta is not valid", K(ret), K(sstable_meta));
    } else if (sstable_meta.sstable_meta_.table_key_ == table_key) {
      ptr = &backup_metas.at(idx);
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable meta should not be null", K(ret), K(backup_metas),
             K(table_key));
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::build_create_sstable_param_(
    const storage::ObTabletHandle &tablet_handle,
    const ObBackupSSTableMeta &backup_sstable_meta,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  if (0 != backup_sstable_meta.sstable_meta_.basic_meta_.data_macro_block_count_) {
    if (OB_FAIL(build_create_none_empty_sstable_param_(backup_sstable_meta, param))) {
      LOG_WARN("failed to build create none empty sstable param", K(ret));
    }
  } else {
    if (OB_FAIL(build_create_empty_sstable_param_(tablet_handle, backup_sstable_meta, param))) {
      LOG_WARN("failed to build create empty sstable param", K(ret));
    }
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::build_create_none_empty_sstable_param_(
    const ObBackupSSTableMeta &backup_sstable_meta,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableMergeRes merge_res;
  if (!backup_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build create sstable param get invalid argument", K(ret),
             K(backup_sstable_meta));
  } else if (0 == backup_sstable_meta.sstable_meta_.basic_meta_.data_macro_block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable param has data macro block, can not build sstable from basic meta", K(ret), K(backup_sstable_meta));
  } else if (OB_FAIL(backup_sstable_meta.sstable_meta_.get_merge_res(merge_res))) {
    LOG_WARN("failed to get merge res", K(ret));
  } else if (OB_FAIL(param.init_for_ha(backup_sstable_meta.sstable_meta_, merge_res))) {
    LOG_WARN("failed to init create sstable param", K(ret));
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::build_create_empty_sstable_param_(
    const storage::ObTabletHandle &tablet_handle,
    const ObBackupSSTableMeta &backup_sstable_meta,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  if (!backup_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build create sstable param get invalid argument", K(ret),
             K(backup_sstable_meta));
  } else if (0 != backup_sstable_meta.sstable_meta_.basic_meta_.data_macro_block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable param has data macro block, can not build sstable from basic meta", K(ret), K(backup_sstable_meta));
  } else if (OB_FAIL(param.init_for_ha(backup_sstable_meta.sstable_meta_))) {
    LOG_WARN("failed to init create sstable param", K(ret));
  }
  return ret;
}

// TODO(yanfeng): need wait chengji refactor the interface of ObSSTableSecMetaIterator
int ObBackupSSTableSecMetaIterator::create_tmp_sstable_(
    const ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.table_key_.is_co_sstable()) {
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(
        param, allocator_, table_handle_))) {
      LOG_WARN("failed to create sstable for migrate", K(ret));
    }
  } else {
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(
        param, allocator_, table_handle_))) {
      LOG_WARN("failed to create co sstable", K(ret));
    }
  }
  return ret;
}

int ObBackupSSTableSecMetaIterator::init_sstable_sec_meta_iter_(const storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  datum_range_.set_whole_range();
  ObSSTable *sstable = NULL;
  const storage::ObITableReadInfo *index_read_info = NULL;
  if (OB_FAIL(table_handle_.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K_(table_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_sstable_read_info(sstable, index_read_info))) {
    LOG_WARN("failed to get sstable read info", K(ret), KPC(sstable));
  } else if (OB_FAIL(sec_meta_iterator_.open(datum_range_,
                                             ObMacroBlockMetaType::DATA_BLOCK_META,
                                             *sstable,
                                             *index_read_info,
                                             allocator_))) {
    LOG_WARN("failed to open sec meta iterator", K(ret));
  }
  return ret;
}

} // namespace backup
} // namespace oceanbase
