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

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_thread_cond.h"
#include "storage/meta_mem/ob_meta_pointer.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/tablet/ob_tablet_ddl_info.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tx_storage/ob_ls_handle.h"

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
  virtual int dump_meta_obj(bool &is_washed) override;

  virtual int deep_copy(char *buf, const int64_t buf_len, ObMetaPointer<ObTablet> *&value) const override;
  virtual int64_t get_deep_copy_size() const override;

  INHERIT_TO_STRING_KV("ObMetaPointer", ObMetaPointer, K_(ls_handle), K_(ddl_kv_mgr_handle),
      KP(memtable_mgr_handle_.get_memtable_mgr()), K_(ddl_info), K_(redefined_schema_version));
public:
  int set_tx_data(const ObTabletTxMultiSourceDataUnit &tx_data);
  int get_tx_data(ObTabletTxMultiSourceDataUnit &tx_data) const;
  int set_redefined_schema_version(const int64_t schema_version);
  int get_redefined_schema_version(int64_t &schema_version) const;
  int create_ddl_kv_mgr(const share::ObLSID &ls_id, const ObTabletID &tablet_id, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  void get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
private:
  int wash_obj();
  virtual int do_post_work_for_load() override;
private:
  ObLSHandle ls_handle_;
  ObDDLKvMgrHandle ddl_kv_mgr_handle_;
  ObMemtableMgrHandle memtable_mgr_handle_;
  ObTabletDDLInfo ddl_info_;
  ObTabletTxMultiSourceDataUnit tx_data_;
  int64_t redefined_schema_version_;
  common::ObThreadCond cond_;
  mutable common::TCRWLock msd_lock_;
  lib::ObMutex ddl_kv_mgr_lock_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletPointer);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_POINTER
