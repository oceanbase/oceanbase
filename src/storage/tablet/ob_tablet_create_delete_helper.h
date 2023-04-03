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

#ifndef OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER

#include <stdint.h>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_tablet_id.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/scn.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_common.h"

namespace oceanbase
{
namespace obrpc
{
struct ObBatchCreateTabletArg;
struct ObBatchRemoveTabletArg;
struct ObCreateTabletInfo;
}

namespace share
{
namespace schema
{
class ObTableSchema;
}
}

namespace transaction
{
struct ObMulSourceDataNotifyArg;
}

namespace storage
{
class ObLS;
class ObTabletMapKey;
class ObTabletCreateSSTableParam;
class ObTableHandleV2;
class ObTabletIDSet;

class ObTabletCreateInfo final
{
public:
  ObTabletCreateInfo();
  ~ObTabletCreateInfo() = default;
  ObTabletCreateInfo(const ObTabletCreateInfo &other);
  ObTabletCreateInfo &operator=(const ObTabletCreateInfo &other);
  bool is_valid() { return data_tablet_id_.is_valid(); }
  TO_STRING_KV(K_(create_data_tablet),
               K_(data_tablet_id),
               K_(index_tablet_id_array),
               K_(lob_meta_tablet_id),
               K_(lob_piece_tablet_id));
public:
  bool create_data_tablet_;
  ObTabletID data_tablet_id_;
  common::ObSArray<common::ObTabletID> index_tablet_id_array_;
  ObTabletID lob_meta_tablet_id_;
  ObTabletID lob_piece_tablet_id_;
};

class ObTabletCreateDeleteHelper
{
private:
  typedef common::hash::ObHashSet<common::ObTabletID, hash::NoPthreadDefendMode> NonLockedHashSet;
public:
  ObTabletCreateDeleteHelper(
      ObLS &ls,
      ObTabletIDSet &tablet_id_set);
public:
  int prepare_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int redo_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int commit_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
  int abort_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int tx_end_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int prepare_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int redo_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int commit_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int tx_end_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int abort_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
public:
  static int get_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle,
      const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US);
  static int check_and_get_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle,
      const int64_t timeout_us);
  static int acquire_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle,
      const bool only_acquire = false);
  static int check_need_create_empty_major_sstable(
      const share::schema::ObTableSchema &table_schema,
      bool &need_create_sstable);
  static int build_create_sstable_param(
      const share::schema::ObTableSchema &table_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObTabletCreateSSTableParam &param);
  static int create_sstable(
      const ObTabletCreateSSTableParam &param,
      ObTableHandleV2 &table_handle);
  static bool is_pure_data_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_mixed_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_pure_aux_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_pure_hidden_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool find_related_aux_info(
    const obrpc::ObBatchCreateTabletArg &arg,
    const common::ObTabletID &data_tablet_id,
    int64_t &idx);
  static int prepare_data_for_tablet_status(const ObTabletID &tablet_id, const ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags);

  static int prepare_data_for_binding_info(const ObTabletID &tablet_id, const ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static void print_memtables_for_table(ObTabletHandle &tablet_handle);
private:
  static int verify_tablets_absence(
      const obrpc::ObBatchCreateTabletArg &arg,
      common::ObIArray<ObTabletCreateInfo> &tablet_create_info_array);
  static int check_tablet_existence(
      const obrpc::ObBatchCreateTabletArg &arg,
      bool &is_valid);
  static int check_tablet_absence(
      const obrpc::ObBatchCreateTabletArg &arg,
      bool &is_valid);
  static int check_pure_data_or_mixed_tablets_info(
      const share::ObLSID &ls_id,
      const obrpc::ObCreateTabletInfo &info,
      bool &is_valid);
  static int check_pure_index_or_hidden_tablets_info(
      const share::ObLSID &ls_id,
      const obrpc::ObCreateTabletInfo &info,
      bool &is_valid);
  static int build_tablet_create_info(
      const obrpc::ObBatchCreateTabletArg &arg,
      common::ObIArray<ObTabletCreateInfo> &tablet_create_info_array);
  static int get_all_existed_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const share::SCN &scn,
      common::ObIArray<common::ObTabletID> &existed_tablet_id_array,
      NonLockedHashSet &existed_tablet_id_set);
  static int build_batch_create_tablet_arg(
      const obrpc::ObBatchCreateTabletArg &old_arg,
      const NonLockedHashSet &existed_tablet_id_set,
      obrpc::ObBatchCreateTabletArg &new_arg);
  static int get_tablet_schema_index(
      const common::ObTabletID &tablet_id,
      const common::ObIArray<common::ObTabletID> &table_ids,
      int64_t &index);
  static int set_tablet_final_status(
      ObTabletHandle &tablet_handle,
      const ObTabletStatus::Status status,
      const share::SCN &tx_scn,
      const share::SCN &memtable_scn,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op = memtable::MemtableRefOp::NONE);
  static bool check_tablet_status(
      const ObTabletHandle &tablet_handle,
      const ObTabletStatus::Status expected_status);
  static int check_tablet_status(
      const obrpc::ObBatchRemoveTabletArg &arg,
      bool &normal);
  static int fill_aux_infos(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      ObTabletCreateInfo &tablet_create_info);
private:
  int replay_prepare_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_prepare_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int record_tablet_id(
      const common::ObIArray<ObTabletCreateInfo> &tablet_create_info_array);
  int handle_special_tablets_for_replay(
      const common::ObIArray<common::ObTabletID> &existed_tablet_id_array,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int set_scn(
      const common::ObIArray<ObTabletCreateInfo> &tablet_create_info_array,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int print_multi_data_for_create_tablet(
      const common::ObIArray<ObTabletCreateInfo> &tablet_create_info_array);
  int print_multi_data_for_remove_tablet(
      const obrpc::ObBatchRemoveTabletArg &arg);
  int batch_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int create_tablet(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int ensure_skip_create_all_tablets_safe(
      const obrpc::ObBatchCreateTabletArg &arg,
      const share::SCN &scn);
  int build_pure_data_tablet(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int build_mixed_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int build_pure_aux_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int build_pure_hidden_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int build_mixed_hidden_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &info,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_create_tablet(
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const common::ObTabletID &lob_meta_tablet_id,
      const common::ObTabletID &lob_piece_tablet_id,
      const common::ObIArray<common::ObTabletID> &index_tablet_array,
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags,
      const share::schema::ObTableSchema &table_schema,
      const lib::Worker::CompatMode &compat_mode,
      ObTabletHandle &tablet_handle);
  int do_commit_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
  int do_commit_create_tablet(
      const common::ObTabletID &tablet_id,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_abort_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_tx_end_create_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int roll_back_remove_tablets(
      const obrpc::ObBatchCreateTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int roll_back_remove_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_abort_create_tablet(
      ObTabletHandle &tablet_handle,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_commit_remove_tablet(
      const common::ObTabletID &tablet_id,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_abort_remove_tablet(
      const common::ObTabletID &tablet_id,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int do_tx_end_remove_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int replay_verify_tablets(
      const obrpc::ObBatchRemoveTabletArg &arg,
      const share::SCN &scn,
      common::ObIArray<common::ObTabletID> &tablet_id_array);
private:
  ObLS &ls_;
  ObTabletIDSet &tablet_id_set_;
};

class ObSimpleBatchCreateTabletArg
{
public:
  ObSimpleBatchCreateTabletArg(const obrpc::ObBatchCreateTabletArg &arg);
  ~ObSimpleBatchCreateTabletArg() = default;
public:
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  const obrpc::ObBatchCreateTabletArg &arg_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER
