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

#ifndef STORAGE_LOG_STREAM_BACKUP_CTX_H_
#define STORAGE_LOG_STREAM_BACKUP_CTX_H_

#include "storage/meta_mem/ob_tablet_handle.h"
#include "common/ob_tablet_id.h"
#include "common/storage/ob_io_device.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/utility/utility.h"
#include "share/ob_ls_id.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_tmp_file.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/backup/ob_backup_file_writer_ctx.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase {
namespace backup {

struct ObSimpleBackupStat final {
  ObSimpleBackupStat();
  ~ObSimpleBackupStat();
  void reset();
  TO_STRING_KV(K_(start_ts), K_(end_ts), K_(tablet_meta_count), K_(sstable_meta_count), K_(macro_block_count));
  int64_t start_ts_;
  int64_t end_ts_;
  int64_t total_bytes_;
  int64_t tablet_meta_count_;
  int64_t sstable_meta_count_;
  int64_t macro_block_count_;
  int64_t reused_macro_block_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSimpleBackupStat);
};

class ObSimpleBackupStatMgr final {
public:
  ObSimpleBackupStatMgr();
  ~ObSimpleBackupStatMgr();
  int init(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
      const uint64_t tenant_id, const share::ObLSID &ls_id);
  int mark_begin(const share::ObBackupDataType &backup_data_type);
  int mark_end(const share::ObBackupDataType &backup_data_type);
  int add_macro_block(const share::ObBackupDataType &backup_data_type, const blocksstable::ObLogicMacroBlockId &logic_id);
  int add_sstable_meta(const share::ObBackupDataType &backup_data_type, const storage::ObITable::TableKey &table_key);
  int add_tablet_meta(const share::ObBackupDataType &backup_data_type, const common::ObTabletID &tablet_id);
  int add_bytes(const share::ObBackupDataType &backup_data_type, const int64_t bytes);
  int do_compare(const ObSimpleBackupStatMgr &other);
  void print_stat();

private:
  int get_stat_(const share::ObBackupDataType &backup_data_type, ObSimpleBackupStat *&stat);
  int get_idx_(const share::ObBackupDataType &backup_data_type, int64_t &idx);
  void reset_stat_list_();
  int inner_do_compare_(const char *backup_data_event, const ObSimpleBackupStat &lhs, const ObSimpleBackupStat &rhs);
  int add_missing_event_(const char *backup_data_event, const common::ObIArray<common::ObTabletID> &missing_tablets,
      const common::ObIArray<storage::ObITable::TableKey> &missing_table_keys,
      const common::ObIArray<blocksstable::ObLogicMacroBlockId> &missing_logic_ids);
  template <class T>
  int get_missing_items_(const common::ObIArray<T> &lhs, const common::ObIArray<T> &rhs, common::ObIArray<T> &missing);
  template <class T>
  int print_missing_items_(const common::ObIArray<T> &list);

private:
  static const int64_t STAT_ARRAY_SIZE = 3;
  mutable lib::ObMutex mutex_;
  bool is_inited_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSimpleBackupStat stat_list_[STAT_ARRAY_SIZE];
  DISALLOW_COPY_AND_ASSIGN(ObSimpleBackupStatMgr);
};

template <class T>
int ObSimpleBackupStatMgr::get_missing_items_(
    const common::ObIArray<T> &lhs, const common::ObIArray<T> &rhs, common::ObIArray<T> &missing)
{
  int ret = OB_SUCCESS;
  missing.reset();
  if (lhs.count() < rhs.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "count not expected", K(ret), K(lhs.count()), K(rhs.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < lhs.count(); ++i) {
      bool exist = false;
      const T &lhs_item = lhs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < rhs.count(); ++j) {
        const T &rhs_item = lhs.at(j);
        if (lhs_item == rhs_item) {
          exist = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !exist) {
        if (OB_FAIL(missing.push_back(lhs_item))) {
          STORAGE_LOG(WARN, "failed to push back", K(ret), K(lhs_item));
        }
      }
    }
  }
  return ret;
}

template <class T>
int ObSimpleBackupStatMgr::print_missing_items_(const common::ObIArray<T> &missing)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < missing.count(); ++i) {
    const T &item = missing.at(i);
    STORAGE_LOG(ERROR, "BACKUP ITEM IS MISSING", K_(tenant_id), K_(ls_id), K(item));
  }
  return ret;
}

struct ObBackupDataCtx {
public:
  ObBackupDataCtx();
  virtual ~ObBackupDataCtx();
  int open(const ObLSBackupDataParam &param, const share::ObBackupDataType &type, const int64_t file_id,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  int write_backup_file_header(const ObBackupFileHeader &file_header);
  int write_macro_block_data(const blocksstable::ObBufferReader &macro_data,
      const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index);
  int write_meta_data(const blocksstable::ObBufferReader &meta_data, const common::ObTabletID &tablet_id,
      const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index);
  int64_t get_file_size() const
  {
    return file_write_ctx_.get_file_size();
  }
  int close();

private:
  int64_t get_data_file_size() const
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      return share::DEFAULT_BACKUP_DATA_FILE_SIZE;
    } else {
      return tenant_config->backup_data_file_size;
    }
  }
  int open_file_writer_(const share::ObBackupPath &backup_path);
  int prepare_file_write_ctx_(
      const ObLSBackupDataParam &param, const share::ObBackupDataType &type, const int64_t file_id,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  int get_macro_block_backup_path_(const int64_t file_id, share::ObBackupPath &backup_path);
  int write_macro_block_data_(const blocksstable::ObBufferReader &buffer, const blocksstable::ObLogicMacroBlockId &logic_id,
      ObBackupMacroBlockIndex &macro_index);
  int write_meta_data_(const blocksstable::ObBufferReader &buffer, const common::ObTabletID &tablet_id,
      const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index);
  int build_macro_block_index_(const blocksstable::ObLogicMacroBlockId &logic_id, const int64_t offset, const int64_t length,
      ObBackupMacroBlockIndex &macro_index);
  int build_meta_index_(const common::ObTabletID &tablet_id, const ObBackupMetaType &meta_type, const int64_t offset,
      const int64_t length, ObBackupMetaIndex &meta_index);
  int append_macro_block_index_(const ObBackupMacroBlockIndex &macro_index);
  int append_meta_index_(const ObBackupMetaIndex &meta_index);
  template <typename IndexType>
  int append_index_(const IndexType &index, ObBackupIndexBufferNode &buffer_node);
  template <typename IndexType>
  int encode_index_to_buffer_(const common::ObIArray<IndexType> &list, blocksstable::ObBufferWriter &buffer_writer);
  int flush_index_list_();
  int flush_macro_block_index_list_();
  int flush_meta_index_list_();
  int write_macro_block_index_list_(const common::ObIArray<ObBackupMacroBlockIndex> &index_list);
  int write_meta_index_list_(const common::ObIArray<ObBackupMetaIndex> &index_list);
  template <class IndexType>
  int write_index_list_(const ObBackupBlockType &index_type, const common::ObIArray<IndexType> &index_list);
  int build_common_header_(const ObBackupBlockType &block_type, const int64_t data_length, const int64_t align_length,
      share::ObBackupCommonHeader *&common_header);
  int write_data_align_(
      const blocksstable::ObBufferReader &buffer, const ObBackupBlockType &block_type, const int64_t alignment);
  int check_trailer_(const ObBackupDataFileTrailer &trailer);
  int flush_trailer_();

private:
  static const int64_t TMP_FILE_READ_TIMEOUT_MS = 5000;  // 5s

public:
  bool is_inited_;
  int64_t file_id_;
  int64_t file_offset_;
  ObLSBackupDataParam param_;
  share::ObBackupDataType backup_data_type_;
  common::ObIODevice *dev_handle_;
  common::ObIOFd io_fd_;
  ObBackupFileWriteCtx file_write_ctx_;
  ObBackupIndexBufferNode macro_index_buffer_node_;
  ObBackupIndexBufferNode meta_index_buffer_node_;
  ObBackupDataFileTrailer file_trailer_;
  blocksstable::ObSelfBufferWriter tmp_buffer_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataCtx);
};

struct ObBackupRecoverRetryCtx {
  ObBackupRecoverRetryCtx();
  virtual ~ObBackupRecoverRetryCtx();
  void reuse();
  void reset();
  TO_STRING_KV(
      K_(has_need_skip_tablet_id),
      K_(need_skip_tablet_id),
      K_(has_need_skip_logic_id),
      K_(need_skip_logic_id),
      K_(reused_pair_list));

  bool has_need_skip_tablet_id_;
  bool has_need_skip_logic_id_;
  common::ObTabletID need_skip_tablet_id_;
  blocksstable::ObLogicMacroBlockId need_skip_logic_id_;
  common::ObArray<ObBackupMacroBlockIDPair> reused_pair_list_;
};
class ObILSTabletIdReader;
struct ObLSBackupCtx {
public:
  ObLSBackupCtx();
  virtual ~ObLSBackupCtx();
  int open(
      const ObLSBackupParam &param, const share::ObBackupDataType &backup_data_type, common::ObMySQLProxy &sql_proxy,
      ObBackupIndexKVCache &index_kv_cache, common::ObInOutBandwidthThrottle &bandwidth_throttle);
  int next(common::ObTabletID &tablet_id);
  void set_backup_data_type(const share::ObBackupDataType &backup_data_type);
  int set_tablet(const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *tablet_handle);
  int get_tablet(const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *&tablet_handle);
  int release_tablet(const common::ObTabletID &tablet_id);
  void set_result_code(const int64_t result, bool &is_set);
  int64_t get_result_code() const;
  void set_finished();
  bool is_finished() const;
  int close();
  void reset();
  void reuse();

  int get_max_file_id(int64_t &max_file_id);
  int set_max_file_id(const int64_t file_id);
  int wait_task(const int64_t file_id);
  int finish_task(const int64_t file_id);
  int64_t get_prefetch_task_id()
  {
    return ATOMIC_FAA(&prefetch_task_id_, 1);
  }

  ObBackupTabletHolder &get_tablet_holder()
  {
    return tablet_holder_;
  }

private:
  struct BackupRetryCmp {
    bool operator()(const ObBackupRetryDesc &lhs, const ObBackupRetryDesc &rhs)
    {
      return lhs.retry_id_ < rhs.retry_id_;
    }
  };
  int recover_last_retry_ctx_();
  int check_and_sort_retry_list_(
      const int64_t cur_turn_id, const int64_t cur_retry_id, common::ObArray<ObBackupRetryDesc> &retry_list);
  int recover_last_minor_retry_ctx_(common::ObArray<ObBackupRetryDesc> &retry_list);
  int recover_last_major_retry_ctx_(common::ObArray<ObBackupRetryDesc> &retry_list);
  int inner_recover_last_minor_retry_ctx_(const ObBackupRetryDesc &retry_desc, bool &found);
  int inner_recover_last_major_retry_ctx_(const ObBackupRetryDesc &retry_desc, bool &found);
  int get_last_persist_macro_block_(const ObBackupRetryDesc &retry_desc, bool &found);
  int get_last_persist_tablet_meta_(const ObBackupRetryDesc &retry_desc, bool &found);
  int inner_get_last_persist_tablet_meta_(
      const common::ObIArray<ObBackupMetaIndex> &meta_index_list, common::ObTabletID &tablet_id, bool &found);
  int get_all_retries_of_turn_(const int64_t turn_id, common::ObIArray<ObBackupRetryDesc> &retry_list);
  int recover_need_reuse_macro_block_(const common::ObIArray<ObBackupRetryDesc> &retry_list);
  int inner_recover_need_reuse_macro_block_(const ObBackupRetryDesc &retry_desc, bool &is_end);

private:
  int prepare_tablet_id_reader_(ObILSTabletIdReader *&reader);
  int get_all_tablet_id_list_(ObILSTabletIdReader *reader, common::ObIArray<common::ObTabletID> &tablet_list);
  int seperate_tablet_id_list_(const common::ObIArray<common::ObTabletID> &tablet_id_list,
      common::ObIArray<common::ObTabletID> &sys_tablet_list, common::ObIArray<common::ObTabletID> &data_tablet_id_list);
  int inner_do_next_(common::ObTabletID &tablet_id);
  int check_need_skip_(const common::ObTabletID &tablet_id, bool &need_skip);
  int check_need_skip_minor_(const common::ObTabletID &tablet_id, bool &need_skip);
  int check_need_skip_major_(const common::ObTabletID &tablet_id, bool &need_skip);
  int get_next_tablet_(common::ObTabletID &tablet_id);
  void add_recover_retry_ctx_event_(const ObBackupRetryDesc &retry_desc);

public:
  bool is_inited_;
  mutable lib::ObMutex mutex_;
  common::ObThreadCond cond_;
  bool is_finished_;
  int64_t result_code_;
  int64_t max_file_id_;
  int64_t prefetch_task_id_;
  volatile int64_t finished_file_id_;
  ObLSBackupParam param_;
  ObLSBackupStat backup_stat_;
  share::ObBackupDataType backup_data_type_;
  int64_t task_idx_;
  ObBackupTabletStat tablet_stat_;
  ObBackupTabletHolder tablet_holder_;
  ObSimpleBackupStatMgr stat_mgr_;
  common::ObArray<common::ObTabletID> sys_tablet_id_list_;
  common::ObArray<common::ObTabletID> data_tablet_id_list_;
  ObBackupRecoverRetryCtx backup_retry_ctx_;
  common::ObMySQLProxy *sql_proxy_;
  int64_t rebuild_seq_; // rebuild seq of backup ls meta
  int64_t check_tablet_info_cost_time_;
  share::SCN backup_tx_table_filled_tx_scn_;
  ObBackupTabletChecker tablet_checker_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupCtx);
};

}  // namespace backup
}  // namespace oceanbase

#endif
