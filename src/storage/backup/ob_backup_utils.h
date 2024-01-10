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

#ifndef STORAGE_LOG_STREAM_BACKUP_UTILS_H_
#define STORAGE_LOG_STREAM_BACKUP_UTILS_H_

#include "storage/meta_mem/ob_tablet_handle.h"
#include "common/ob_tablet_id.h"
#include "common/object/ob_object.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_ls_id.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/ob_i_table.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/backup/ob_backup_index_store.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase {
namespace storage
{
struct ObBackupLSMetaInfosDesc;
}
namespace share
{
class SCN;
}
namespace backup {

struct ObLSBackupCtx;
class ObITabletLogicMacroIdReader;
class ObBackupMacroBlockIndexStore;

class ObBackupUtils {
public:
  static int get_sstables_by_data_type(const storage::ObTabletHandle &tablet_handle, const share::ObBackupDataType &backup_data_type,
      const storage::ObTabletTableStore &table_store, common::ObIArray<storage::ObITable *> &sstable_array);
  static int check_tablet_with_major_sstable(const storage::ObTabletHandle &tablet_handle, bool &with_major);
  static int fetch_macro_block_logic_id_list(const storage::ObTabletHandle &tablet_handle,
      const blocksstable::ObSSTable &sstable, common::ObIArray<blocksstable::ObLogicMacroBlockId> &logic_id_list);
  static int report_task_result(const int64_t job_id, const int64_t task_id, const uint64_t tenant_id,
      const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, const share::ObTaskId trace_id,
      const share::ObTaskId &dag_id, const int64_t result, ObBackupReportCtx &report_ctx);
  static int check_ls_validity(const uint64_t tenant_id, const share::ObLSID &ls_id);
  static int check_ls_valid_for_backup(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t local_rebuild_seq);
  static int calc_start_replay_scn(const share::ObBackupSetTaskAttr &set_task_attr,
      const storage::ObBackupLSMetaInfosDesc &ls_meta_infos, const share::ObTenantArchiveRoundAttr &round_attr,
      share::SCN &start_replay_scn);
private:
  static int check_tablet_minor_sstable_validity_(const storage::ObTabletHandle &tablet_handle,
      const common::ObIArray<storage::ObITable *> &minor_sstable_array);
  static int check_tablet_ddl_sstable_validity_(const storage::ObTabletHandle &tablet_handle,
      const common::ObIArray<storage::ObITable *> &ddl_sstable_array);
  static int get_ls_leader_(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &leader);
  static int fetch_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObAddr &leader_addr, common::ObIArray<common::ObAddr> &addr_list);
};

struct ObBackupTabletCtx final {
  ObBackupTabletCtx();
  ~ObBackupTabletCtx();
  void reuse();
  void print_ctx();
  int record_macro_block_physical_id(const storage::ObITable::TableKey &table_key,
      const blocksstable::ObLogicMacroBlockId &logic_id, const ObBackupPhysicalID &physical_id);
  TO_STRING_KV(K_(total_tablet_meta_count), K_(finish_tablet_meta_count), K_(total_sstable_meta_count),
      K_(finish_sstable_meta_count), K_(reused_macro_block_count), K_(total_macro_block_count),
      K_(finish_macro_block_count), K_(is_all_loaded));
  int64_t total_tablet_meta_count_;
  int64_t total_sstable_meta_count_;
  int64_t reused_macro_block_count_;
  int64_t total_macro_block_count_;
  int64_t finish_tablet_meta_count_;
  int64_t finish_sstable_meta_count_;
  int64_t finish_macro_block_count_;
  bool is_all_loaded_;
  ObBackupMacroBlockIDMappingsMeta mappings_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletCtx);
};

struct ObBackupProviderItem;

class ObBackupTabletStat final {
public:
  ObBackupTabletStat();
  ~ObBackupTabletStat();
  int init(const uint64_t tenant_id, const int64_t backup_set_id, const share::ObLSID &ls_id,
      const share::ObBackupDataType &backup_data_type);
  int prepare_tablet_sstables(const uint64_t tenant_id, const share::ObBackupDataType &backup_data_type, const common::ObTabletID &tablet_id,
      const storage::ObTabletHandle &tablet_handle, const common::ObIArray<storage::ObITable *> &sstable_array);
  int mark_items_pending(
      const share::ObBackupDataType &backup_data_type, const common::ObIArray<ObBackupProviderItem> &items);
  int mark_items_reused(const share::ObBackupDataType &backup_data_type,
      const common::ObIArray<ObBackupProviderItem> &items, common::ObIArray<ObBackupPhysicalID> &physical_ids);
  int mark_item_reused(const share::ObBackupDataType &backup_data_type,
      const ObITable::TableKey &table_key, const ObBackupMacroBlockIDPair &id_pair);
  int mark_item_finished(const share::ObBackupDataType &backup_data_type, const ObBackupProviderItem &item,
      const ObBackupPhysicalID &physical_id, bool &is_all_finished);
  int get_tablet_stat(const common::ObTabletID &tablet_id, ObBackupTabletCtx *&ctx);
  int free_tablet_stat(const common::ObTabletID &tablet_id);
  int print_tablet_stat() const;
  void set_backup_data_type(const share::ObBackupDataType &backup_data_type);
  void reuse();
  void reset();

private:
  int get_tablet_stat_(const common::ObTabletID &tablet_id, const bool create_if_not_exist, ObBackupTabletCtx *&stat);
  int check_tablet_finished_(const common::ObTabletID &tablet_id, bool &can_release);
  int do_with_stat_when_pending_(const ObBackupProviderItem &item);
  int do_with_stat_when_reused_(const ObBackupProviderItem &item, const ObBackupPhysicalID &physical_id);
  int do_with_stat_when_finish_(const ObBackupProviderItem &item, const ObBackupPhysicalID &physical_id);
  int alloc_stat_(ObBackupTabletCtx *&stat);
  void free_stat_(ObBackupTabletCtx *&stat);
  void report_event_(const common::ObTabletID &tablet_id, const ObBackupTabletCtx &tablet_ctx);

private:
  typedef common::hash::ObHashMap<common::ObTabletID, ObBackupTabletCtx *> ObBackupTabletCtxMap;
  struct PrintTabletStatOp {
    int operator()(common::hash::HashMapPair<common::ObTabletID, ObBackupTabletCtx *> &entry);
  };
  static const int64_t DEFAULT_BUCKET_COUNT = 1000;

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  share::ObLSID ls_id_;
  ObBackupTabletCtxMap stat_map_;
  share::ObBackupDataType backup_data_type_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletStat);
};

class ObBackupTabletHolder final {
public:
  ObBackupTabletHolder();
  ~ObBackupTabletHolder();
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id);
  int hold_tablet(const common::ObTabletID &tablet_id, storage::ObTabletHandle &tablet_handle);
  int get_tablet(const common::ObTabletID &tablet_id, storage::ObTabletHandle &tablet_handle);
  int release_tablet(const common::ObTabletID &tablet_id);
  bool is_empty() const;
  void reuse();
  void reset();

private:
  typedef common::hash::ObHashMap<common::ObTabletID, storage::ObTabletHandle> TabletHandleMap;

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  TabletHandleMap holder_map_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletHolder);
};

class ObBackupDiskChecker {
public:
  ObBackupDiskChecker();
  virtual ~ObBackupDiskChecker();
  int init(ObBackupTabletHolder &holder);
  int check_disk_space();

private:
  bool is_inited_;
  ObBackupTabletHolder *tablet_holder_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDiskChecker);
};

enum ObBackupProviderItemType {
  PROVIDER_ITEM_MACRO_ID = 0,
  PROVIDER_ITEM_SSTABLE_META = 1,
  PROVIDER_ITEM_TABLET_META = 2,
  PROVIDER_ITEM_MAX,
};

class ObBackupProviderItem {
  friend class ObBackupTabletStat;
public:
  ObBackupProviderItem();
  virtual ~ObBackupProviderItem();
  // for tablet meta and sstable meta
  int set_with_fake(const ObBackupProviderItemType &item_type, const common::ObTabletID &tablet_id);
  // for macro block
  int set(const ObBackupProviderItemType &item_type, const ObBackupMacroBlockId &backup_macro_id,
      const storage::ObITable::TableKey &table_key, const common::ObTabletID &tablet_id);
  bool operator==(const ObBackupProviderItem &other) const;
  bool operator!=(const ObBackupProviderItem &other) const;
  ObBackupProviderItemType get_item_type() const;
  blocksstable::ObLogicMacroBlockId get_logic_id() const;
  blocksstable::MacroBlockId get_macro_block_id() const;
  const storage::ObITable::TableKey &get_table_key() const;
  common::ObTabletID get_tablet_id() const;
  int64_t get_nested_offset() const;
  int64_t get_nested_size() const;
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObBackupProviderItem &src, char *buf, int64_t len, int64_t &pos);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(item_type), K_(logic_id), K_(table_key), K_(tablet_id), K_(nested_offset), K_(nested_size), K_(timestamp));
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  // for parallel external sort serialization restriction
  static ObITable::TableKey get_fake_table_key_();
  static blocksstable::ObLogicMacroBlockId get_fake_logic_id_();
  static blocksstable::MacroBlockId get_fake_macro_id_();

private:
  ObBackupProviderItemType item_type_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  blocksstable::MacroBlockId macro_block_id_;
  storage::ObITable::TableKey table_key_;
  common::ObTabletID tablet_id_;  // logic_id_.tablet_id_ may not equal to tablet_id_
  int64_t nested_offset_;
  int64_t nested_size_;
  int64_t timestamp_;
};

class ObBackupProviderItemCompare {
public:
  ObBackupProviderItemCompare(int &sort_ret);
  void set_backup_data_type(const share::ObBackupDataType &backup_data_type);
  bool operator()(const ObBackupProviderItem *left, const ObBackupProviderItem *right);
  int &result_code_;

private:
  share::ObBackupDataType backup_data_type_;
};

enum ObBackupTabletProviderType {
  BACKUP_TABLET_PROVIDER = 0,
  MAX_BACKUP_TABLET_PROVIDER,
};

class ObIBackupTabletProvider {
public:
  ObIBackupTabletProvider() = default;
  virtual ~ObIBackupTabletProvider() = default;
  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual bool is_run_out() = 0;
  virtual void set_backup_data_type(const share::ObBackupDataType &backup_data_type) = 0;
  virtual share::ObBackupDataType get_backup_data_type() const = 0;
  virtual int get_next_batch_items(common::ObIArray<ObBackupProviderItem> &items, int64_t &task_id) = 0;
  virtual ObBackupTabletProviderType get_type() const = 0;
};

class ObBackupTabletProvider : public ObIBackupTabletProvider {
public:
  ObBackupTabletProvider();
  virtual ~ObBackupTabletProvider();
  int init(const ObLSBackupParam &param, const share::ObBackupDataType &backup_data_type, ObLSBackupCtx &ls_backup_ctx,
      ObBackupIndexKVCache &index_kv_cache, common::ObMySQLProxy &sql_proxy);
  virtual void reset() override;
  virtual void reuse() override;
  virtual bool is_run_out() override;
  virtual void set_backup_data_type(const share::ObBackupDataType &backup_data_type) override;
  virtual share::ObBackupDataType get_backup_data_type() const override;
  virtual int get_next_batch_items(common::ObIArray<ObBackupProviderItem> &items, int64_t &task_id) override;
  virtual ObBackupTabletProviderType get_type() const override
  {
    return BACKUP_TABLET_PROVIDER;
  }

private:
  int get_sys_tablet_list_(const share::ObLSID &ls_id, common::ObIArray<common::ObTabletID> &tablet_id_list);
  int inner_get_batch_items_(const int64_t batch_size, common::ObIArray<ObBackupProviderItem> &items);
  int prepare_batch_tablet_(const uint64_t tenant_id, const share::ObLSID &ls_id);
  int prepare_tablet_(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
      const share::ObBackupDataType &backup_data_type, int64_t &count);

  // make sure clog checkpoint scn of the returned tablet is >= consistent_scn.
  int get_tablet_handle_(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
      storage::ObTabletHandle &tablet_handle);
  int get_consistent_scn_(share::SCN &consistent_scn) const;
  int report_tablet_skipped_(const common::ObTabletID &tablet_id, const share::ObBackupSkippedType &skipped_type,
                             const share::ObBackupDataType &backup_data_type);
  int get_tablet_skipped_type_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id, share::ObBackupSkippedType &skipped_type);
  int hold_tablet_handle_(const common::ObTabletID &tablet_id, storage::ObTabletHandle &tablet_handle);
  int fetch_tablet_sstable_array_(const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
      const ObTabletTableStore &table_store, const share::ObBackupDataType &backup_data_type,
      common::ObIArray<storage::ObITable *> &sstable_array);
  int prepare_tablet_logic_id_reader_(const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
      const storage::ObITable::TableKey &table_key, const blocksstable::ObSSTable &sstable,
      ObITabletLogicMacroIdReader *&reader);
  int fetch_all_logic_macro_block_id_(const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
      const storage::ObITable::TableKey &table_key, const blocksstable::ObSSTable &sstable, int64_t &total_count);
  int add_macro_block_id_item_list_(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
      const common::ObIArray<ObBackupMacroBlockId> &list, int64_t &added_count);
  int add_sstable_item_(const common::ObTabletID &tablet_id);
  int add_tablet_item_(const common::ObTabletID &tablet_id);
  int remove_duplicates_(common::ObIArray<ObBackupProviderItem> &array);
  int check_macro_block_need_skip_(const blocksstable::ObLogicMacroBlockId &logic_id, bool &need_skip);
  int inner_check_macro_block_need_skip_(const blocksstable::ObLogicMacroBlockId &logic_id,
      const common::ObArray<ObBackupMacroBlockIDPair> &reused_pair_list, bool &need_skip);
  int check_tablet_status_(const storage::ObTabletHandle &tablet_handle, bool &is_normal);
  int check_tablet_continuity_(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
      const storage::ObTabletHandle &tablet_handle);
  int check_tx_data_can_explain_user_data_(const storage::ObTabletHandle &tablet_handle, bool &can_explain);
  int build_tenant_meta_index_store_(const share::ObBackupDataType &backup_data_type);
  int get_tenant_meta_index_turn_id_(int64_t &turn_id);
  int get_tenant_meta_index_retry_id_(const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, int64_t &retry_id);
  int check_tablet_replica_validity_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type);
  int compare_prev_item_(const ObBackupProviderItem &item);

private:
  static const int64_t BATCH_SIZE = 2000;
  static const int64_t MACRO_BLOCK_SIZE = 2 << 20;
  static const int64_t BUF_MEM_LIMIT = 32 * MACRO_BLOCK_SIZE;
  static const int64_t FILE_BUF_SIZE = MACRO_BLOCK_SIZE;
  static const int64_t EXPIRE_TIMESTAMP = 0;
  typedef storage::ObExternalSort<ObBackupProviderItem, ObBackupProviderItemCompare> ExternalSort;

private:
  bool is_inited_;
  bool is_run_out_;
  bool meet_end_;
  int sort_ret_;
  mutable lib::ObMutex mutex_;
  ObLSBackupParam param_;
  share::ObBackupDataType backup_data_type_;
  int64_t cur_task_id_;
  ExternalSort external_sort_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObBackupIndexKVCache *index_kv_cache_;
  common::ObMySQLProxy *sql_proxy_;
  ObBackupProviderItemCompare backup_item_cmp_;
  ObBackupMetaIndexStore meta_index_store_;
  ObBackupProviderItem prev_item_;
  bool has_prev_item_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletProvider);
};

class ObBackupMacroBlockTaskMgr {
public:
  ObBackupMacroBlockTaskMgr();
  virtual ~ObBackupMacroBlockTaskMgr();
  int init(const share::ObBackupDataType &backup_data_type, const int64_t batch_size);
  void set_backup_data_type(const share::ObBackupDataType &backup_data_type);
  share::ObBackupDataType get_backup_data_type() const;
  int receive(const int64_t task_id, const common::ObIArray<ObBackupProviderItem> &id_list);
  int deliver(common::ObIArray<ObBackupProviderItem> &id_list, int64_t &file_id);
  int64_t get_pending_count() const;
  int64_t get_ready_count() const;
  bool has_remain() const;
  void reset();
  void reuse();

private:
  int wait_task_(const int64_t task_id);
  int finish_task_(const int64_t task_id);
  int transfer_list_without_lock_();
  int get_from_ready_list_(common::ObIArray<ObBackupProviderItem> &list);
  int put_to_pending_list_(const common::ObIArray<ObBackupProviderItem> &list);

private:
  static const int64_t BATCH_MOVE_COUNT = 128;
  static const int64_t DEFAULT_WAIT_TIME_MS = 10 * 1000;     // 10s
  static const int64_t DEFAULT_WAIT_TIMEOUT_MS = 20 * 1000;  // 20s

private:
  bool is_inited_;
  share::ObBackupDataType backup_data_type_;
  int64_t batch_size_;
  mutable lib::ObMutex mutex_;
  common::ObThreadCond cond_;
  int64_t max_task_id_;
  int64_t file_id_;
  volatile int64_t cur_task_id_;
  ObArray<ObBackupProviderItem> pending_list_;
  ObArray<ObBackupProviderItem> ready_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroBlockTaskMgr);
};

}  // namespace backup
}  // namespace oceanbase

#endif
