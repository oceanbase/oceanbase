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

#ifndef OCEANBASE_STORAGE_BACKUP_OB_BACKUP_TABLET_PAIRING_HELPER_H_
#define OCEANBASE_STORAGE_BACKUP_OB_BACKUP_TABLET_PAIRING_HELPER_H_

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace storage
{
struct ObBackupTabletPairingInfoDesc;
} // namespace storage

namespace backup
{

// Manages tablet pairing relationships (e.g., data tablet <-> LOB meta tablet).
// Used during backup to ensure paired tablets are backed up or skipped together.
class ObBackupTabletPairingHelper
{
public:
  ObBackupTabletPairingHelper();
  ~ObBackupTabletPairingHelper();
  int init(const uint64_t tenant_id);
  void reset();

  // Add a bidirectional pairing relationship to the internal map.
  // Caller should call this twice for each pair: add_pairing(A, A') and add_pairing(A', A).
  int add_pairing(const common::ObTabletID &tablet_id, const common::ObTabletID &paired_tablet_id);

  // Query the paired tablet ID. Returns OB_ENTRY_NOT_EXIST if not found.
  int get_paired_tablet_id(const common::ObTabletID &tablet_id, common::ObTabletID &paired_tablet_id) const;

  // Verify that two descendant arrays have the same count.
  //
  // NOTE: This is a structural check only — it does NOT verify that any specific
  // tablet in `descendents` is actually paired with a specific tablet in
  // `paired_descendents`. Such content-level verification is not possible here:
  // the pairing snapshot in tablet_pairing_info.obbak only records pre-split
  // tablets (A↔A'), while these arrays contain post-split leaf descendents
  // (A1/A2/... and A1'/A2'/...) that are absent from the snapshot.
  //
  // A count match is sufficient because cascading split guarantees the primary
  // and its auxiliary (e.g. data tablet and its LOB meta tablet) split
  // symmetrically — equal leaf counts imply the split structure is consistent.
  // An unequal count signals broken symmetry and is rejected.
  static int verify_descendent_count_match(
      const common::ObIArray<common::ObTabletID> &descendents,
      const common::ObIArray<common::ObTabletID> &paired_descendents);

  // Load pairing relationships from the LS-level external file into the in-memory map.
  int load_from_ls_file(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id);

  // Write pairing relationships from the in-memory map to the LS-level external file.
  int write_to_ls_file(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id) const;

  // Load pairing relationships from the tenant-level external file into the in-memory map.
  int load_from_tenant_file(const share::ObBackupDest &backup_set_dest);

  // Write pairing relationships from the in-memory map to the tenant-level external file.
  int write_to_tenant_file(const share::ObBackupDest &backup_set_dest) const;

  // Populate the in-memory map from a descriptor (used during aggregation).
  int load_from_desc(const storage::ObBackupTabletPairingInfoDesc &desc);

  // Export the in-memory map into a descriptor (used during file writing).
  int export_to_desc(storage::ObBackupTabletPairingInfoDesc &desc) const;

  // Number of pairs. The internal map stores both directions per pair, so divide by 2.
  int64_t get_pairing_count() const { return tablet_pairing_map_.size() / 2; }
  bool is_empty() const { return 0 == tablet_pairing_map_.size(); }
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(K_(is_inited), "pairing_count", tablet_pairing_map_.size() / 2);

private:
  static const int64_t DEFAULT_BUCKET_SIZE = 1000;
  bool is_inited_;
  common::hash::ObHashMap<common::ObTabletID, common::ObTabletID> tablet_pairing_map_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletPairingHelper);
};

} // namespace backup
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BACKUP_OB_BACKUP_TABLET_PAIRING_HELPER_H_
