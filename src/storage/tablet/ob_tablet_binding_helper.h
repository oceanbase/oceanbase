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

#ifndef OCEANBASE_STORAGE_OB_TABLET_BINDING_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_BINDING_HELPER

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "lib/ob_define.h"
#include "storage/memtable/ob_multi_source_data.h"
#include "storage/tx/ob_trans_define.h"
#include "share/scn.h"

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
class ObLSID;
}

namespace transaction
{
struct ObMulSourceDataNotifyArg;
class ObTransID;
}

namespace storage
{
class ObLS;
class ObLSHandle;
class ObTabletHandle;
class ObTabletTxMultiSourceDataUnit;
class ObTabletMapKey;

class ObTabletBindingInfo : public memtable::ObIMultiSourceDataUnit
{
public:
  OB_UNIS_VERSION_V(1);
public:
  typedef common::ObSArray<common::ObTabletID> TabletArray;

  ObTabletBindingInfo();
  virtual ~ObTabletBindingInfo() {}
  int assign(const ObTabletBindingInfo &arg);

  virtual int deep_copy(const memtable::ObIMultiSourceDataUnit *src, ObIAllocator *allocator = nullptr) override;
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int64_t get_data_size() const override { return sizeof(ObTabletBindingInfo); }
  virtual memtable::MultiSourceDataUnitType type() const override { return memtable::MultiSourceDataUnitType::TABLET_BINDING_INFO; }

  TO_STRING_KV(K_(redefined), K_(snapshot_version), K_(schema_version), K_(data_tablet_id), K_(hidden_tablet_ids), K_(lob_meta_tablet_id), K_(lob_piece_tablet_id), K_(is_tx_end), K_(unsynced_cnt_for_multi_data));
public:
  bool redefined_;
  int64_t snapshot_version_; // if redefined it is max readable snapshot, else it is min readable snapshot.
  int64_t schema_version_;
  common::ObTabletID data_tablet_id_;
  common::ObSEArray<common::ObTabletID, 2> hidden_tablet_ids_;
  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletBindingInfo);
};

class ObBatchUnbindTabletArg final
{
public:
  ObBatchUnbindTabletArg();
  ~ObBatchUnbindTabletArg() {}
  int assign(const ObBatchUnbindTabletArg &other);
  inline bool is_redefined() const { return schema_version_ != OB_INVALID_VERSION; }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(schema_version), K_(orig_tablet_ids), K_(hidden_tablet_ids));
  OB_UNIS_VERSION_V(1);

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t schema_version_;
  ObSArray<ObTabletID> orig_tablet_ids_;
  ObSArray<ObTabletID> hidden_tablet_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchUnbindTabletArg);
};

class ObTabletBindingPrepareCtx final
{
public:
  ObTabletBindingPrepareCtx()
    : skip_idx_(), last_idx_(-1) {}
  ~ObTabletBindingPrepareCtx() {}
public:
  ObSArray<int64_t> skip_idx_;
  int64_t last_idx_;
};

class ObTabletBindingHelper final
{
public:
  ObTabletBindingHelper(const ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags)
    : ls_(ls), trans_flags_(trans_flags) {}
  ~ObTabletBindingHelper() {}

  // create tablet
  static int lock_tablet_binding_for_create(
      const obrpc::ObBatchCreateTabletArg &arg,
      ObLS &ls,
      const transaction::ObMulSourceDataNotifyArg &trans_flags,
      ObTabletBindingPrepareCtx &ctx);
  static void rollback_lock_tablet_binding_for_create(
      const obrpc::ObBatchCreateTabletArg &arg,
      ObLS &ls,
      const transaction::ObMulSourceDataNotifyArg &prepare_trans_flags,
      const ObTabletBindingPrepareCtx &ctx);
  static int set_scn_for_create(const obrpc::ObBatchCreateTabletArg &arg, ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int unlock_tablet_binding_for_create(const obrpc::ObBatchCreateTabletArg &arg, ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int modify_tablet_binding_for_create(const obrpc::ObBatchCreateTabletArg &arg, ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int add_tablet_binding(
      const obrpc::ObBatchCreateTabletArg &arg,
      const obrpc::ObCreateTabletInfo &tinfo,
      ObTabletHandle &orig_tablet_handle,
      const ObIArray<ObTabletID> &index_tablet_ids_to_add,
      const ObIArray<ObTabletID> &hidden_tablet_ids_to_add,
      const transaction::ObMulSourceDataNotifyArg &trans_flags);

  // unbind tablet
  static int lock_tablet_binding_for_unbind(const ObBatchUnbindTabletArg &arg, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int set_scn_for_unbind(const ObBatchUnbindTabletArg &arg, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int unlock_tablet_binding_for_unbind(const ObBatchUnbindTabletArg &arg, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int fix_binding_info_for_modify_tablet_binding(const ObBatchUnbindTabletArg &arg, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int modify_tablet_binding_for_unbind(const ObBatchUnbindTabletArg &arg, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int check_skip_tx_end(const ObTabletID &tablet_id, const ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags, bool &skip);
  static int on_tx_end_for_modify_tablet_binding(const ObBatchUnbindTabletArg &arg, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int fix_binding_info_for_create_tablets(const obrpc::ObBatchCreateTabletArg &arg, const ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags);

  // dml snapshot/schema version guard
  static int check_schema_version(common::ObIArray<ObTabletHandle> &handle, const int64_t schema_version);
  static int check_schema_version(ObTabletHandle &handle, const int64_t schema_version);
  static int check_snapshot_readable(ObTabletHandle &handle, const int64_t snapshot_version);

  // common
  static bool has_lob_tablets(const obrpc::ObBatchCreateTabletArg &arg, const obrpc::ObCreateTabletInfo &info);
  static int check_need_dec_cnt_for_abort(const ObTabletTxMultiSourceDataUnit &tx_data, bool &need_dec);
  static int lock_and_set_tx_data(ObTabletHandle &handle, ObTabletTxMultiSourceDataUnit &tx_data, const bool for_replay);
  static int lock_tablet_binding(ObTabletHandle &handle, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int set_scn(ObTabletHandle &handle, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int unlock_tablet_binding(ObTabletHandle &handle, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int check_is_locked(ObTabletHandle &handle, const transaction::ObTransID &tx_id, bool &is_locked);
  static int get_ls(const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  int get_tablet(const ObTabletID &tablet_id, ObTabletHandle &handle) const;

  // TODO(lihongqin.lhq): remove this interface later
  int replay_get_tablet(const ObTabletMapKey &key, ObTabletHandle &handle) const;
private:
  int lock_tablet_binding(const ObTabletID &tablet_id) const;
  int lock_tablet_binding(const common::ObIArray<ObTabletID> &tablet_ids) const;
  int set_scn(const ObTabletID &tablet_id) const;
  int set_scn(const common::ObIArray<ObTabletID> &tablet_ids) const;
  int unlock_tablet_binding(const ObTabletID &tablet_id) const;
  int unlock_tablet_binding(const common::ObIArray<ObTabletID> &tablet_ids) const;
  static int prepare_data_for_tablet(const ObTabletID &tablet_id, const ObLS &ls, const transaction::ObMulSourceDataNotifyArg &trans_flags);
  int fix_unsynced_cnt_for_binding_info(const ObTabletID &tablet_id);
private:
  const ObLS &ls_;
  const transaction::ObMulSourceDataNotifyArg &trans_flags_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletBindingHelper);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_BINDING_HELPER
