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
#include "storage/backup/ob_backup_tablet_pairing_helper.h"
#include "storage/backup/ob_backup_data_store.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace backup
{

/* ObBackupTabletPairingHelper */

ObBackupTabletPairingHelper::ObBackupTabletPairingHelper()
  : is_inited_(false),
    tablet_pairing_map_()
{
}

ObBackupTabletPairingHelper::~ObBackupTabletPairingHelper()
{
  reset();
}

int ObBackupTabletPairingHelper::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupTabletPairingHelper init twice", K(ret));
  } else if (OB_FAIL(tablet_pairing_map_.create(DEFAULT_BUCKET_SIZE, "BkPairingMap", "BkPairingMap", tenant_id))) {
    LOG_WARN("failed to create tablet pairing map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObBackupTabletPairingHelper::reset()
{
  if (tablet_pairing_map_.created()) {
    tablet_pairing_map_.destroy();
  }
  is_inited_ = false;
}

int ObBackupTabletPairingHelper::add_pairing(
    const ObTabletID &tablet_id,
    const ObTabletID &paired_tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!tablet_id.is_valid() || !paired_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(paired_tablet_id));
  } else if (OB_FAIL(tablet_pairing_map_.set_refactored(tablet_id, paired_tablet_id, 0/*no overwrite*/))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      // Verify existing value is consistent
      ObTabletID existing_paired;
      if (OB_FAIL(tablet_pairing_map_.get_refactored(tablet_id, existing_paired))) {
        LOG_WARN("failed to get existing pairing for conflict check", K(ret), K(tablet_id));
      } else if (existing_paired != paired_tablet_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet pairing conflict: same tablet_id maps to different paired tablets",
                 K(ret), K(tablet_id), K(existing_paired), K(paired_tablet_id));
      }
    } else {
      LOG_WARN("failed to set pairing", K(ret), K(tablet_id), K(paired_tablet_id));
    }
  }
  return ret;
}

int ObBackupTabletPairingHelper::get_paired_tablet_id(
    const ObTabletID &tablet_id,
    ObTabletID &paired_tablet_id) const
{
  int ret = OB_SUCCESS;
  paired_tablet_id.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else {
    ObTabletID value;
    if (OB_FAIL(tablet_pairing_map_.get_refactored(tablet_id, value))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("failed to get pairing", K(ret), K(tablet_id));
      }
    } else {
      paired_tablet_id = value;
    }
  }
  return ret;
}

int ObBackupTabletPairingHelper::verify_descendent_count_match(
    const ObIArray<ObTabletID> &descendents,
    const ObIArray<ObTabletID> &paired_descendents)
{
  int ret = OB_SUCCESS;
  if (descendents.count() != paired_descendents.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("descendent count mismatch", K(ret),
             "src_count", descendents.count(),
             "paired_count", paired_descendents.count(),
             K(descendents), K(paired_descendents));
  }
  return ret;
}

int ObBackupTabletPairingHelper::load_from_ls_file(
    const ObBackupDest &backup_set_dest,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  ObBackupTabletPairingInfoDesc desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!backup_set_dest.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest), K(ls_id));
  } else if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("failed to init backup data store", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.read_ls_tablet_pairing_info(ls_id, desc))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret || OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // file may not exist if no pairings in this LS
      LOG_INFO("ls tablet pairing info file not exist, skip", K(ls_id));
    } else {
      LOG_WARN("failed to read ls tablet pairing info", K(ret), K(ls_id));
    }
  } else if (OB_FAIL(load_from_desc(desc))) {
    LOG_WARN("failed to load from desc", K(ret));
  }
  return ret;
}

int ObBackupTabletPairingHelper::write_to_ls_file(
    const ObBackupDest &backup_set_dest,
    const ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  ObBackupTabletPairingInfoDesc desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!backup_set_dest.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest), K(ls_id));
  } else if (OB_FAIL(export_to_desc(desc))) {
    LOG_WARN("failed to export to desc", K(ret));
  } else if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("failed to init backup data store", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.write_ls_tablet_pairing_info(ls_id, desc))) {
    LOG_WARN("failed to write ls tablet pairing info", K(ret), K(ls_id));
  }
  return ret;
}

int ObBackupTabletPairingHelper::load_from_tenant_file(
    const ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  ObBackupTabletPairingInfoDesc desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("failed to init backup data store", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.read_tablet_pairing_info(desc))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret || OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // file may not exist if no pairings
      LOG_INFO("tenant tablet pairing info file not exist, skip");
    } else {
      LOG_WARN("failed to read tenant tablet pairing info", K(ret));
    }
  } else if (OB_FAIL(load_from_desc(desc))) {
    LOG_WARN("failed to load from desc", K(ret));
  }
  return ret;
}

int ObBackupTabletPairingHelper::write_to_tenant_file(
    const ObBackupDest &backup_set_dest) const
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  ObBackupTabletPairingInfoDesc desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(export_to_desc(desc))) {
    LOG_WARN("failed to export to desc", K(ret));
  } else if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("failed to init backup data store", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.write_tablet_pairing_info(desc))) {
    LOG_WARN("failed to write tenant tablet pairing info", K(ret));
  }
  return ret;
}

int ObBackupTabletPairingHelper::load_from_desc(const ObBackupTabletPairingInfoDesc &desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    // File stores one direction per pair; rebuild bidirectional map on load.
    ARRAY_FOREACH_X(desc.pairing_list_, idx, cnt, OB_SUCC(ret)) {
      const ObBackupTabletPairingItem &item = desc.pairing_list_.at(idx);
      if (OB_FAIL(add_pairing(item.tablet_id_, item.paired_tablet_id_))) {
        LOG_WARN("failed to add pairing from desc", K(ret), K(item));
      } else if (OB_FAIL(add_pairing(item.paired_tablet_id_, item.tablet_id_))) {
        LOG_WARN("failed to add reverse pairing from desc", K(ret), K(item));
      }
    }
  }
  return ret;
}

int ObBackupTabletPairingHelper::export_to_desc(ObBackupTabletPairingInfoDesc &desc) const
{
  int ret = OB_SUCCESS;
  desc.pairing_list_.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    // The in-memory map holds both directions (A->A' and A'->A) for O(1) bidirectional
    // lookup. To avoid doubling the file size, only export the canonical direction
    // (smaller tablet_id -> larger tablet_id); load_from_desc rebuilds both directions.
    FOREACH_X(iter, tablet_pairing_map_, OB_SUCC(ret)) {
      const ObTabletID &tablet_id = iter->first;
      const ObTabletID &paired_tablet_id = iter->second;
      if (tablet_id.id() < paired_tablet_id.id()) {
        ObBackupTabletPairingItem item(tablet_id, paired_tablet_id);
        if (OB_FAIL(desc.pairing_list_.push_back(item))) {
          LOG_WARN("failed to push back pairing item", K(ret), K(item));
        }
      }
    }
  }
  return ret;
}

} // namespace backup
} // namespace oceanbase
