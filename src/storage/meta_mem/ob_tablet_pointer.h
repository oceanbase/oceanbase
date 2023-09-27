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

#ifndef OCEANBASE_STORAGE_OB_TABLET_POINTER
#define OCEANBASE_STORAGE_OB_TABLET_POINTER

#include "lib/lock/ob_mutex.h"
#include "storage/meta_mem/ob_meta_pointer.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/tablet/ob_tablet_ddl_info.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/multi_data_source/mds_table_handler.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObTabletDDLKvMgr;
typedef ObMetaObjGuard<ObTabletDDLKvMgr> ObDDLKvMgrHandle;

class ObTabletPointer : public ObMetaPointer<ObTablet>
{
  friend class ObTablet;
  friend class ObLSTabletService;
  friend class ObTenantMetaMemMgr;
public:
  ObTabletPointer();
  ObTabletPointer(
      const ObLSHandle &ls_handle,
      const ObMemtableMgrHandle &memtable_mgr_handle);
  virtual ~ObTabletPointer();
  virtual void reset() override;

  virtual int set_attr_for_obj(ObTablet *tablet) override;
  virtual int dump_meta_obj(ObMetaObjGuard<ObTablet> &guard, void *&free_obj) override;

  virtual int deep_copy(char *buf, const int64_t buf_len, ObMetaPointer<ObTablet> *&value) const override;
  virtual int64_t get_deep_copy_size() const override;

  virtual int acquire_obj(ObTablet *&t) override;
  virtual int release_obj(ObTablet *&t) override;

  // do not KPC memtable_mgr, may dead lock
  INHERIT_TO_STRING_KV("ObMetaPointer", ObMetaPointer, K_(ls_handle), K_(ddl_kv_mgr_handle),
      KP(memtable_mgr_handle_.get_memtable_mgr()), K_(ddl_info), K_(initial_state), KP_(old_version_chain));
public:
  bool get_initial_state() const;
  void set_initial_state(const bool initial_state);
  int create_ddl_kv_mgr(const share::ObLSID &ls_id, const ObTabletID &tablet_id, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  void get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int get_mds_table(mds::MdsTableHandle &handle, bool not_exist_create = false);
  // interfaces forward to mds_table_handler_
  void mark_mds_table_deleted();
  void set_tablet_status_written();
  bool is_tablet_status_written() const;
  int try_release_mds_nodes_below(const share::SCN &scn);
  int try_gc_mds_table();
  int release_memtable_and_mds_table_for_ls_offline();
  int get_min_mds_ckpt_scn(share::SCN &scn);
  ObLS *get_ls() const;
private:
  int wash_obj();
  int add_tablet_to_old_version_chain(ObTablet *tablet);
  int remove_tablet_from_old_version_chain(ObTablet *tablet);
private:
  ObLSHandle ls_handle_; // 24B
  ObDDLKvMgrHandle ddl_kv_mgr_handle_; // 48B
  ObMemtableMgrHandle memtable_mgr_handle_; // 16B
  ObTabletDDLInfo ddl_info_; // 32B
  bool initial_state_; // 1B
  ObByteLock ddl_kv_mgr_lock_; // 1B
  mds::ObMdsTableHandler mds_table_handler_;// 48B
  ObTablet *old_version_chain_; // 8B
  DISALLOW_COPY_AND_ASSIGN(ObTabletPointer); // 272B
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_POINTER
