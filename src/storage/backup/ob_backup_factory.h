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

#ifndef STORAGE_LOG_STREAM_BACKUP_FACTORY_H_
#define STORAGE_LOG_STREAM_BACKUP_FACTORY_H_

#include "storage/backup/ob_backup_index_merger.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_reader.h"
#include "storage/backup/ob_backup_utils.h"

namespace oceanbase {
namespace backup {

class ObLSBackupFactory {
public:
  static ObILSTabletIdReader *get_ls_tablet_id_reader(const ObLSTabletIdReaderType &type, const uint64_t tenant_id);
  static ObITabletLogicMacroIdReader *get_tablet_logic_macro_id_reader(const ObTabletLogicIdReaderType &type, const uint64_t tenant_id);
  static ObIMacroBlockBackupReader *get_macro_block_backup_reader(const ObMacroBlockReaderType &type, const uint64_t tenant_id);
  static ObMultiMacroBlockBackupReader *get_multi_macro_block_backup_reader(const uint64_t tenant_id);
  static ObITabletMetaBackupReader *get_tablet_meta_backup_reader(const ObTabletMetaReaderType &type, const uint64_t tenant_id);
  static ObIBackupIndexIterator *get_backup_index_iterator(const ObBackupIndexIteratorType &type, const uint64_t tenant_id);
  static ObIBackupTabletProvider *get_backup_tablet_provider(const ObBackupTabletProviderType &type, const uint64_t tenant_id);
  static ObIBackupMacroBlockIndexFuser *get_backup_macro_index_fuser(const ObBackupMacroIndexFuserType &type, const uint64_t tenant_id);
  static ObBackupTabletCtx *get_backup_tablet_ctx(const uint64_t tenant_id);

  static void free(ObILSTabletIdReader *&reader);
  static void free(ObITabletLogicMacroIdReader *&reader);
  static void free(ObIMacroBlockBackupReader *&reader);
  static void free(ObMultiMacroBlockBackupReader *&reader);
  static void free(ObITabletMetaBackupReader *&reader);
  static void free(ObBackupMetaIndexIterator *&iterator);
  static void free(ObIMacroBlockIndexIterator *&iterator);
  static void free(ObBackupMacroBlockIndexIterator *&iterator);
  static void free(ObBackupMacroRangeIndexIterator *&iterator);
  static void free(ObIBackupTabletProvider *&provider);
  static void free(ObBackupTabletProvider *&provider);
  static void free(ObIBackupMacroBlockIndexFuser *&fuser);
  static void free(ObBackupTabletCtx *&ctx);

private:
  template <class IT>
  static void component_free(IT *component)
  {
    if (OB_LIKELY(NULL != component)) {
      op_free(component);
      component = NULL;
    }
  }
};

}  // namespace backup
}  // namespace oceanbase

#endif