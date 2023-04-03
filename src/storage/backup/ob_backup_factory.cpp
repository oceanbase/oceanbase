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
#include "storage/backup/ob_backup_factory.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase {
namespace backup {

ObILSTabletIdReader *ObLSBackupFactory::get_ls_tablet_id_reader(const ObLSTabletIdReaderType &type)
{
  ObILSTabletIdReader *reader = NULL;
  if (LS_TABLET_ID_READER == type) {
    reader = OB_NEW(ObLSTabletIdReader, ObModIds::BACKUP);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown tablet reader type", K(type));
  }
  return reader;
}

ObITabletLogicMacroIdReader *ObLSBackupFactory::get_tablet_logic_macro_id_reader(const ObTabletLogicIdReaderType &type)
{
  ObITabletLogicMacroIdReader *reader = NULL;
  if (TABLET_LOGIC_ID_READER == type) {
    reader = OB_NEW(ObTabletLogicMacroIdReader, ObModIds::BACKUP);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown reader type", K(type));
  }
  return reader;
}

ObIMacroBlockBackupReader *ObLSBackupFactory::get_macro_block_backup_reader(const ObMacroBlockReaderType &type)
{
  ObIMacroBlockBackupReader *reader = NULL;
  if (LOCAL_MACRO_BLOCK_READER == type) {
    reader = OB_NEW(ObMacroBlockBackupReader, ObModIds::BACKUP);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown reader type", K(type));
  }
  return reader;
}

ObMultiMacroBlockBackupReader *ObLSBackupFactory::get_multi_macro_block_backup_reader()
{
  return OB_NEW(ObMultiMacroBlockBackupReader, ObModIds::BACKUP);
}

ObITabletMetaBackupReader *ObLSBackupFactory::get_tablet_meta_backup_reader(const ObTabletMetaReaderType &type)
{
  ObITabletMetaBackupReader *reader = NULL;
  if (TABLET_META_READER == type) {
    reader = OB_NEW(ObTabletMetaBackupReader, ObModIds::BACKUP);
  } else if (SSTABLE_META_READER == type) {
    reader = OB_NEW(ObSSTableMetaBackupReader, ObModIds::BACKUP);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown reader type", K(type));
  }
  return reader;
}

ObIBackupIndexIterator *ObLSBackupFactory::get_backup_index_iterator(const ObBackupIndexIteratorType &type)
{
  ObIBackupIndexIterator *iterator = NULL;
  if (BACKUP_MACRO_BLOCK_INDEX_ITERATOR == type) {
    iterator = OB_NEW(ObBackupMacroBlockIndexIterator, ObModIds::BACKUP);
  } else if (BACKUP_MACRO_RANGE_INDEX_ITERATOR == type) {
    iterator = OB_NEW(ObBackupMacroRangeIndexIterator, ObModIds::BACKUP);
  } else if (BACKUP_META_INDEX_ITERATOR == type) {
    iterator = OB_NEW(ObBackupMetaIndexIterator, ObModIds::BACKUP);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown iterator type", K(type));
  }
  return iterator;
}

ObIBackupTabletProvider *ObLSBackupFactory::get_backup_tablet_provider(const ObBackupTabletProviderType &type)
{
  ObIBackupTabletProvider *provider = NULL;
  if (BACKUP_TABLET_PROVIDER == type) {
    provider = OB_NEW(ObBackupTabletProvider, ObModIds::BACKUP);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown provider type", K(type));
  }
  return provider;
}

ObIBackupMacroBlockIndexFuser *ObLSBackupFactory::get_backup_macro_index_fuser(const ObBackupMacroIndexFuserType &type)
{
  ObIBackupMacroBlockIndexFuser *fuser = NULL;
  if (type == BACKUP_MACRO_INDEX_MINOR_FUSER) {
    fuser = OB_NEW(ObBackupMacroIndexMinorFuser, ObModIds::BACKUP);
  } else if (type == BACKUP_MACRO_INDEX_MAJOR_FUSER) {
    fuser = OB_NEW(ObBackupMacroIndexMajorFuser, ObModIds::BACKUP);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown fuser type", K(type));
  }
  return fuser;
}

ObBackupTabletCtx *ObLSBackupFactory::get_backup_tablet_ctx()
{
  return OB_NEW(ObBackupTabletCtx, ObModIds::BACKUP);
}

void ObLSBackupFactory::free(ObILSTabletIdReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    if (LS_TABLET_ID_READER == reader->get_type()) {
      OB_DELETE(ObILSTabletIdReader, ObModIds::BACKUP, reader);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObLSBackupFactory::free(ObITabletLogicMacroIdReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    if (TABLET_LOGIC_ID_READER == reader->get_type()) {
      OB_DELETE(ObITabletLogicMacroIdReader, ObModIds::BACKUP, reader);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObLSBackupFactory::free(ObIMacroBlockBackupReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    if (LOCAL_MACRO_BLOCK_READER == reader->get_type()) {
      OB_DELETE(ObIMacroBlockBackupReader, ObModIds::BACKUP, reader);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObLSBackupFactory::free(ObMultiMacroBlockBackupReader *&reader)
{
  OB_DELETE(ObMultiMacroBlockBackupReader, ObModIds::BACKUP, reader);
}

void ObLSBackupFactory::free(ObITabletMetaBackupReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    if (TABLET_META_READER == reader->get_type()) {
      OB_DELETE(ObITabletMetaBackupReader, ObModIds::BACKUP, reader);
    } else if (SSTABLE_META_READER == reader->get_type()) {
      OB_DELETE(ObITabletMetaBackupReader, ObModIds::BACKUP, reader);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown reader type", "type", reader->get_type());
    }
  }
}

void ObLSBackupFactory::free(ObBackupMetaIndexIterator *&iterator)
{
  OB_DELETE(ObBackupMetaIndexIterator, ObModIds::BACKUP, iterator);
}

void ObLSBackupFactory::free(ObIMacroBlockIndexIterator *&iterator)
{
  if (OB_NOT_NULL(iterator)) {
    if (BACKUP_MACRO_BLOCK_INDEX_ITERATOR == iterator->get_type()) {
      OB_DELETE(ObIMacroBlockIndexIterator, ObModIds::BACKUP, iterator);
    } else if (BACKUP_MACRO_RANGE_INDEX_ITERATOR == iterator->get_type()) {
      OB_DELETE(ObIMacroBlockIndexIterator, ObModIds::BACKUP, iterator);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown iterator type", "type", iterator->get_type());
    }
  }
}

void ObLSBackupFactory::free(ObIBackupTabletProvider *&provider)
{
  if (OB_NOT_NULL(provider)) {
    if (BACKUP_TABLET_PROVIDER == provider->get_type()) {
      OB_DELETE(ObIBackupTabletProvider, ObModIds::BACKUP, provider);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown provider type", "type", provider->get_type());
    }
  }
}

void ObLSBackupFactory::free(ObIBackupMacroBlockIndexFuser *&fuser)
{
  if (OB_NOT_NULL(fuser)) {
    if (BACKUP_MACRO_INDEX_MINOR_FUSER == fuser->get_type()) {
      OB_DELETE(ObIBackupMacroBlockIndexFuser, ObModIds::BACKUP, fuser);
    } else if (BACKUP_MACRO_INDEX_MAJOR_FUSER == fuser->get_type()) {
      OB_DELETE(ObIBackupMacroBlockIndexFuser, ObModIds::BACKUP, fuser);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unknown fuser type", "type", fuser->get_type());
    }
  }
}

void ObLSBackupFactory::free(ObBackupTabletCtx *&ctx)
{
  OB_DELETE(ObBackupTabletCtx, ObModIds::BACKUP, ctx);
}

}  // namespace backup
}  // namespace oceanbase
