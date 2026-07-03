/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef STORAGE_BACKUP_OB_BACKUP_INDEX_REBUILD_TASK_H_
#define STORAGE_BACKUP_OB_BACKUP_INDEX_REBUILD_TASK_H_

#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_se_array.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_index_merger.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/ob_parallel_external_sort.h"

namespace oceanbase {
namespace backup {

struct ObBackupIndexRebuildWorkItem {
  ObBackupIndexRebuildWorkItem();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(retry_desc), K_(file_id));
  ObBackupRetryDesc retry_desc_;
  int64_t file_id_;
};

class ObBackupMacroIndexMergeFinishTask : public share::ObITask {
public:
  ObBackupMacroIndexMergeFinishTask();
  virtual ~ObBackupMacroIndexMergeFinishTask();
  int init(const ObLSBackupDataParam &param,
           const ObBackupIndexLevel &index_level,
           const ObCompressorType &compressor_type,
           ObLSBackupCtx *ls_backup_ctx,
           const ObBackupReportCtx &report_ctx,
           const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items);
  virtual int process() override;
  int check_is_iter_end(bool &is_end);
  int get_next_batch(common::ObIArray<ObBackupIndexRebuildWorkItem> &batch);
  int add_items_to_sort(const common::ObIArray<ObBackupMacroBlockIndex> &index_list);
  const ObBackupIndexMergeParam &get_merge_param() const { return merge_param_; }
  ObLSBackupCtx *get_ls_backup_ctx() const { return ls_backup_ctx_; }
  // report failure to the backup system on behalf of a failed read task, so that
  // an aborted read phase still propagates the error result like the serial path.
  void report_read_task_failed(const int result_code);
  TO_STRING_KV(K_(merge_param), K_(work_idx), "work_count", work_items_.count());

private:
  class BackupMacroBlockIndexComparator {
  public:
    BackupMacroBlockIndexComparator(int &sort_ret);
    bool operator()(const ObBackupMacroBlockIndex *left, const ObBackupMacroBlockIndex *right);
    int &result_code_;
  };
  typedef storage::ObExternalSort<ObBackupMacroBlockIndex, BackupMacroBlockIndexComparator> MacroExternalSort;

  int prepare_merge_ctx_();
  int feed_prev_backup_set_to_sort_();
  int do_external_sort_();
  int consume_sort_output_();
  int get_next_macro_index_(ObBackupMacroBlockIndex &macro_index);
  int get_next_batch_macro_index_list_(const int64_t batch_size,
      common::ObIArray<ObBackupMacroBlockIndex> &index_list);
  int write_macro_index_list_(const common::ObArray<ObBackupMacroBlockIndex> &index_list);
  int flush_index_tree_();
  int get_output_file_path_(share::ObBackupPath &backup_path);

  int create_meta_index_phase_();

private:
  static const int64_t BATCH_SIZE = 2000;
  static const int64_t READ_BATCH_SIZE = 8;
  static const int64_t MACRO_BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t BUF_MEM_LIMIT = 32 * MACRO_BLOCK_SIZE;
  static const int64_t FILE_BUF_SIZE = MACRO_BLOCK_SIZE;
  static const int64_t EXPIRE_TIMESTAMP = 0;

  bool is_inited_;
  lib::ObMutex mutex_;
  int64_t work_idx_;
  common::ObArray<ObBackupIndexRebuildWorkItem> work_items_;

  ObBackupIndexMergeParam merge_param_;
  ObLSBackupDataParam param_;
  ObBackupIndexLevel index_level_;
  ObCompressorType compressor_type_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObBackupReportCtx report_ctx_;

  int64_t offset_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  blocksstable::ObSelfBufferWriter compress_buf_writer_;
  common::ObIODevice *dev_handle_;
  common::ObIOFd io_fd_;
  ObBackupFileWriteCtx write_ctx_;
  ObBackupIndexBufferNode buffer_node_;

  int64_t consume_count_;
  MacroExternalSort external_sort_;
  int sort_result_;
  BackupMacroBlockIndexComparator comparator_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroIndexMergeFinishTask);
};

class ObBackupMacroIndexReadTask : public share::ObITask {
public:
  ObBackupMacroIndexReadTask();
  virtual ~ObBackupMacroIndexReadTask();
  int init(const ObBackupIndexMergeParam &merge_param,
           ObBackupMacroIndexMergeFinishTask *finish_task);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;

private:
  int read_file_index_(const ObBackupIndexRebuildWorkItem &item,
                       common::ObIArray<ObBackupMacroBlockIndex> &index_list);

private:
  bool is_inited_;
  ObBackupIndexMergeParam merge_param_;
  ObBackupMacroIndexMergeFinishTask *finish_task_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroIndexReadTask);
};

class ObBackupMetaIndexMergeFinishTask : public share::ObITask {
public:
  ObBackupMetaIndexMergeFinishTask();
  virtual ~ObBackupMetaIndexMergeFinishTask();
  int init(const ObLSBackupDataParam &param,
           const ObBackupIndexLevel &index_level,
           const ObCompressorType &compressor_type,
           ObLSBackupCtx *ls_backup_ctx,
           const ObBackupReportCtx &report_ctx,
           const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items);
  virtual int process() override;
  int check_is_iter_end(bool &is_end);
  int get_next_batch(common::ObIArray<ObBackupIndexRebuildWorkItem> &batch);
  int add_items_to_sort(const common::ObIArray<ObBackupMetaIndex> &index_list);
  const ObBackupIndexMergeParam &get_merge_param() const { return merge_param_; }
  ObLSBackupCtx *get_ls_backup_ctx() const { return ls_backup_ctx_; }
  // report failure to the backup system on behalf of a failed read task, so that
  // an aborted read phase still propagates the error result like the serial path.
  void report_read_task_failed(const int result_code);
  TO_STRING_KV(K_(merge_param), K_(work_idx), "work_count", work_items_.count());

private:
  class BackupMetaIndexComparator {
  public:
    BackupMetaIndexComparator(int &sort_ret);
    bool operator()(const ObBackupMetaIndex *left, const ObBackupMetaIndex *right);
    int &result_code_;
  };
  typedef storage::ObExternalSort<ObBackupMetaIndex, BackupMetaIndexComparator> MetaExternalSort;

  int prepare_merge_ctx_();
  int do_external_sort_();
  int consume_sort_output_();
  // get the next raw meta index from the sorted output (no fuse)
  virtual int get_next_meta_index_(ObBackupMetaIndex &meta_index);
  // get the next fused meta index, fuse adjacent records with the same meta_key
  // by selecting the largest (turn_id, retry_id) version
  int get_next_fused_meta_index_(ObBackupMetaIndex &meta_index);
  int write_meta_index_list_(const common::ObArray<ObBackupMetaIndex> &index_list);
  int flush_index_tree_();
  int get_output_file_path_(share::ObBackupPath &backup_path);
  void record_server_event_(const int64_t cost_us) const;
  int report_check_tablet_info_event_();

private:
  static const int64_t BATCH_SIZE = 2000;
  static const int64_t READ_BATCH_SIZE = 8;
  static const int64_t MACRO_BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t BUF_MEM_LIMIT = 32 * MACRO_BLOCK_SIZE;
  static const int64_t FILE_BUF_SIZE = MACRO_BLOCK_SIZE;
  static const int64_t EXPIRE_TIMESTAMP = 0;

  bool is_inited_;
  lib::ObMutex mutex_;
  int64_t work_idx_;
  common::ObArray<ObBackupIndexRebuildWorkItem> work_items_;

  ObBackupIndexMergeParam merge_param_;
  ObLSBackupDataParam param_;
  ObBackupIndexLevel index_level_;
  ObCompressorType compressor_type_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObBackupReportCtx report_ctx_;

  int64_t offset_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  blocksstable::ObSelfBufferWriter compress_buf_writer_;
  common::ObIODevice *dev_handle_;
  common::ObIOFd io_fd_;
  ObBackupFileWriteCtx write_ctx_;
  ObBackupIndexBufferNode buffer_node_;

  int64_t consume_count_;
  MetaExternalSort external_sort_;
  int sort_result_;
  BackupMetaIndexComparator comparator_;
  common::ObArray<ObBackupMetaIndex> tmp_index_list_;
  // pending state used by get_next_fused_meta_index_ to fuse across calls
  bool has_pending_meta_index_;
  bool sort_iter_end_;
  ObBackupMetaIndex pending_meta_index_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexMergeFinishTask);
};

class ObBackupMetaIndexReadTask : public share::ObITask {
public:
  ObBackupMetaIndexReadTask();
  virtual ~ObBackupMetaIndexReadTask();
  int init(const ObBackupIndexMergeParam &merge_param,
           ObBackupMetaIndexMergeFinishTask *finish_task);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;

private:
  int read_file_index_(const ObBackupIndexRebuildWorkItem &item,
                       common::ObIArray<ObBackupMetaIndex> &index_list);
  int filter_meta_index_list_(common::ObIArray<ObBackupMetaIndex> &index_list);

private:
  bool is_inited_;
  ObBackupIndexMergeParam merge_param_;
  ObBackupMetaIndexMergeFinishTask *finish_task_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexReadTask);
};

class ObBackupIndexRebuildPrepareTask : public share::ObITask {
public:
  ObBackupIndexRebuildPrepareTask();
  virtual ~ObBackupIndexRebuildPrepareTask();
  int init(const ObLSBackupDataParam &param,
           const ObBackupIndexLevel &index_level,
           ObLSBackupCtx *ls_backup_ctx,
           ObIBackupTabletProvider *provider,
           ObBackupMacroBlockTaskMgr *task_mgr,
           ObBackupIndexKVCache *kv_cache,
           const ObBackupReportCtx &report_ctx,
           const ObCompressorType &compressor_type);
  virtual int process() override;

private:
  int check_all_tablet_released_();
  int mark_ls_task_final_();
  bool need_build_index_(const bool is_build_macro_index) const;
  int collect_work_items_(const ObBackupIndexMergeParam &merge_param,
                          common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items);
  int create_macro_index_phase_(const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items);
  int create_meta_index_phase_(const common::ObIArray<ObBackupIndexRebuildWorkItem> &work_items);

private:
  bool is_inited_;
  ObLSBackupDataParam param_;
  ObBackupIndexLevel index_level_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupReportCtx report_ctx_;
  ObCompressorType compressor_type_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupIndexRebuildPrepareTask);
};

}  // namespace backup
}  // namespace oceanbase

#endif  // STORAGE_BACKUP_OB_BACKUP_INDEX_REBUILD_TASK_H_
