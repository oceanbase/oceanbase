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

#ifndef OCEABASE_STORAGE_HA_DAG
#define OCEABASE_STORAGE_HA_DAG

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_restore_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObStorageHAResultMgr final
{
public:
  ObStorageHAResultMgr();
  ~ObStorageHAResultMgr();
  int get_result(int32_t &result);
  int set_result(const int32_t result, const bool allow_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  int check_allow_retry(bool &allow_retry);
  void reuse();
  void reset();
  int get_retry_count(int32_t &retry_count);
  int get_first_failed_task_id(share::ObTaskId &task_id);
  TO_STRING_KV(K_(result), K_(retry_count), K_(allow_retry), K_(failed_task_id_list));

private:
  static const int64_t MAX_RETRY_CNT = 3;
  common::SpinRWLock lock_;
  int32_t result_;
  int32_t retry_count_;
  bool allow_retry_;
  common::ObSEArray<share::ObTaskId, MAX_RETRY_CNT> failed_task_id_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageHAResultMgr);
};

struct ObIHADagNetCtx
{
public:
  enum DagNetCtxType {
    LS_PREPARE_MIGRATION = 0,
    LS_MIGRATION = 1,
    LS_COMPLETE_MIGRATION = 2,
    LS_RESTORE = 3,
    TABLET_GROUP_RESTORE = 4,
    BACKFILL_TX = 5,
    TRANSFER_BACKFILL_TX = 6,
    MAX
  };

  ObIHADagNetCtx();
  virtual ~ObIHADagNetCtx();
  virtual int fill_comment(char *buf, const int64_t buf_len) const = 0;
  virtual DagNetCtxType get_dag_net_ctx_type() = 0;
  virtual bool is_valid() const = 0;
  int set_result(const int32_t result, const bool need_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  virtual int check_allow_retry(bool &allow_retry);
  int get_result(int32_t &result);
  void reuse();
  void reset();
  int check_is_in_retry(bool &is_in_retry);
  int get_retry_count(int32_t &retry_count);
  int get_first_failed_task_id(share::ObTaskId &task_id);

  VIRTUAL_TO_STRING_KV(K("ObIHADagNetCtx"), K_(result_mgr));
private:
  ObStorageHAResultMgr result_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIHADagNetCtx);
};

class ObStorageHADag : public share::ObIDag
{
public:
  explicit ObStorageHADag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObStorageHADag();
  virtual int inner_reset_status_for_retry();
  virtual bool check_can_retry();
  int check_is_in_retry(bool &is_in_retry);

  int set_result(const int32_t result, const bool allow_retry = true,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  virtual int report_result();
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  ObIHADagNetCtx *get_ha_dag_net_ctx() const { return ha_dag_net_ctx_; }
  virtual bool is_ha_dag() const override { return true; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, KPC_(ha_dag_net_ctx), K_(result_mgr));
protected:
  ObIHADagNetCtx *ha_dag_net_ctx_;
  ObStorageHAResultMgr result_mgr_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHADag);
};

class ObStorageHADagUtils
{
public:
  static int deal_with_fo(
      const int err,
      share::ObIDag *dag,
      const bool allow_retry = true);
  static int get_ls(
      const share::ObLSID &ls_id,
      ObLSHandle &ls_handle);
  static int check_self_is_valid_member(
      const share::ObLSID &ls_id,
      bool &is_valid_member);
};

class ObHATabletGroupCtx
{
public:
  ObHATabletGroupCtx();
  virtual ~ObHATabletGroupCtx();
  int init(const common::ObIArray<ObTabletID> &tablet_id_array);
  int get_next_tablet_id(common::ObTabletID &tablet_id);
  int get_all_tablet_ids(common::ObIArray<ObTabletID> &tablet_id);
  void reuse();

  TO_STRING_KV(K_(tablet_id_array), K_(index));
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObArray<ObTabletID> tablet_id_array_;
  int64_t index_;
  DISALLOW_COPY_AND_ASSIGN(ObHATabletGroupCtx);
};

class ObHATabletGroupMgr
{
public:
  ObHATabletGroupMgr();
  virtual ~ObHATabletGroupMgr();
  int init();
  int get_next_tablet_group_ctx(
      ObHATabletGroupCtx *&tablet_group_ctx);
  int build_tablet_group_ctx(
      const common::ObIArray<ObTabletID> &tablet_id_array);
  void reuse();

  TO_STRING_KV(K_(tablet_group_ctx_array), K_(index));
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObArenaAllocator allocator_;
  ObArray<ObHATabletGroupCtx *> tablet_group_ctx_array_;
  int64_t index_;
  DISALLOW_COPY_AND_ASSIGN(ObHATabletGroupMgr);
};

class ObStorageHATaskUtils
{
public:
  static int check_need_copy_sstable(
      const ObMigrationSSTableParam &param,
      ObTabletHandle &tablet_handle,
      bool &need_copy);

private:
  static int check_major_sstable_need_copy_(
      const ObMigrationSSTableParam &param,
      ObTabletHandle &tablet_handle,
      bool &need_copy);

  static int check_minor_sstable_need_copy_(
      const ObMigrationSSTableParam &param,
      ObTabletHandle &tablet_handle,
      bool &need_copy);

  static int check_ddl_sstable_need_copy_(
      const ObMigrationSSTableParam &param,
      ObTabletHandle &tablet_handle,
      bool &need_copy);

};


}
}
#endif
