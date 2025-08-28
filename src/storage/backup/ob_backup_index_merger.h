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

#ifndef STORAGE_LOG_STREAM_BACKUP_INDEX_MERGER_H_
#define STORAGE_LOG_STREAM_BACKUP_INDEX_MERGER_H_

#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_data_struct.h"

namespace oceanbase {
namespace backup {

enum ObBackupMacroIndexFuserType {
  BACKUP_MACRO_INDEX_MINOR_FUSER = 0,
  BACKUP_MACRO_INDEX_MAJOR_FUSER = 1,
  MAX_BACKUP_MACRO_INDEX_FUSER,
};

class ObIBackupMacroBlockIndexFuser {
public:
  ObIBackupMacroBlockIndexFuser();
  virtual ~ObIBackupMacroBlockIndexFuser();
  virtual int fuse(MERGE_ITER_ARRAY &iter) = 0;
  virtual ObBackupMacroIndexFuserType get_type() const = 0;
  int get_result(ObBackupMacroRangeIndex &result);

protected:
  MERGE_ITER_ARRAY iter_array_;
  ObBackupMacroRangeIndex result_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupMacroBlockIndexFuser);
};

class ObBackupMacroIndexMajorFuser : public ObIBackupMacroBlockIndexFuser {
public:
  ObBackupMacroIndexMajorFuser();
  virtual ~ObBackupMacroIndexMajorFuser();
  virtual int fuse(MERGE_ITER_ARRAY &iter) override;
  virtual ObBackupMacroIndexFuserType get_type() const override
  {
    return BACKUP_MACRO_INDEX_MAJOR_FUSER;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroIndexMajorFuser);
};

class ObBackupMacroIndexMinorFuser : public ObIBackupMacroBlockIndexFuser {
public:
  ObBackupMacroIndexMinorFuser();
  virtual ~ObBackupMacroIndexMinorFuser();
  virtual int fuse(MERGE_ITER_ARRAY &iter) override;
  virtual ObBackupMacroIndexFuserType get_type() const override
  {
    return BACKUP_MACRO_INDEX_MINOR_FUSER;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroIndexMinorFuser);
};

class ObBackupMetaIndexFuser final {
  static const int64_t DEFAULT_ITER_COUNT = 2;
  typedef ObSEArray<ObBackupMetaIndexIterator *, DEFAULT_ITER_COUNT> MERGE_ITER_ARRAY;
public:
  ObBackupMetaIndexFuser();
  ~ObBackupMetaIndexFuser();
  int fuse(const MERGE_ITER_ARRAY &iter);
  int get_result(ObBackupMetaIndex &result);

private:

  MERGE_ITER_ARRAY iter_array_;
  ObBackupMetaIndex result_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexFuser);
};

class ObIBackupMultiLevelIndexBuilder {
public:
  ObIBackupMultiLevelIndexBuilder();
  virtual ~ObIBackupMultiLevelIndexBuilder();
  int init(const int64_t start_offset, const ObCompressorType compressor_type,
      ObBackupIndexBufferNode &node, ObBackupFileWriteCtx &write_ctx);
  template <class T>
  int build_index();
  void reset();

protected:
  virtual int build_next_level_index_(ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node) = 0;

protected:
  template <class IndexIndex>
  int build_and_flush_index_tree_();
  int alloc_new_buffer_node_(const uint64_t tenant_id, const ObBackupBlockType &block_type, const int64_t node_level,
      ObBackupIndexBufferNode *&new_node);
  int check_need_build_next_level_(ObBackupIndexBufferNode *node, bool &need_build) const;
  template <class IndexIndex>
  int build_next_level_index_impl_(ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node);
  template <class IndexType>
  int write_compressed_data_(const common::ObIArray<IndexType> &index_list, const ObCompressorType &compressor_type,
      blocksstable::ObSelfBufferWriter &buffer_writer, int64_t &data_length, int64_t &data_zlength);
  template <class IndexIndex>
  int write_index_list_(const ObBackupBlockType &block_type, const int64_t node_level,
      const common::ObIArray<IndexIndex> &index_index_list);
  template <class IndexType>
  int encode_index_to_buffer_(const common::ObIArray<IndexType> &index_list, blocksstable::ObBufferWriter &buffer_writer);
  int get_index_tree_height_(int64_t &height) const;
  int flush_trailer_();

protected:
  static const int64_t READ_TIMEOUT_MS = 5000;

protected:
  bool is_inited_;
  int64_t cur_block_offset_;
  int64_t cur_block_length_;
  ObBackupIndexBufferNode *leaf_;
  ObBackupIndexBufferNode *dummy_;
  ObBackupIndexBufferNode *root_;
  ObBackupFileWriteCtx *write_ctx_;
  common::ObArenaAllocator allocator_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  ObCompressorType compressor_type_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupMultiLevelIndexBuilder);
};

class ObBackupMultiLevelMacroIndexBuilder : public ObIBackupMultiLevelIndexBuilder {
public:
  ObBackupMultiLevelMacroIndexBuilder() = default;
  virtual ~ObBackupMultiLevelMacroIndexBuilder() = default;

private:
  virtual int build_next_level_index_(ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMultiLevelMacroIndexBuilder);
};

class ObBackupMultiLevelMetaIndexBuilder : public ObIBackupMultiLevelIndexBuilder {
public:
  ObBackupMultiLevelMetaIndexBuilder() = default;
  virtual ~ObBackupMultiLevelMetaIndexBuilder() = default;

private:
  virtual int build_next_level_index_(ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMultiLevelMetaIndexBuilder);
};

class ObBackupMultiLevelMacroBlockIndexBuilder : public ObIBackupMultiLevelIndexBuilder {
public:
  ObBackupMultiLevelMacroBlockIndexBuilder() = default;
  virtual ~ObBackupMultiLevelMacroBlockIndexBuilder() = default;

private:
  virtual int build_next_level_index_(ObBackupIndexBufferNode &cur_node, ObBackupIndexBufferNode &next_node) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMultiLevelMacroBlockIndexBuilder);
};

class ObIBackupIndexMerger {
public:
  ObIBackupIndexMerger();
  virtual ~ObIBackupIndexMerger();
  virtual int merge_index() = 0;
  TO_STRING_KV(K_(merge_param));

protected:
  virtual int get_all_retries_(const int64_t task_id, const uint64_t tenant_id, const share::ObBackupDataType &backup_data_type,
      const share::ObLSID &ls_id, common::ObISQLClient &sql_proxy, common::ObIArray<ObBackupRetryDesc> &retry_list);
  int open_file_writer_(const share::ObBackupPath &backup_path, const share::ObBackupStorageInfo *storage_info, const int64_t dest_id);
  int prepare_file_write_ctx_(common::ObInOutBandwidthThrottle &bandwidth_throttle, ObBackupFileWriteCtx &write_ctx);
  template <class IndexType>
  int encode_index_to_buffer_(const common::ObIArray<IndexType> &index_list, blocksstable::ObBufferWriter &buffer_writer);
  template <class IndexType, class IndexIndexType>
  int write_index_list_(const ObBackupBlockType &block_type, const common::ObIArray<IndexType> &index_list);
  template <class IndexType> 
  int write_compressed_data_(const common::ObIArray<IndexType> &index_list, const ObCompressorType &compressor_type,
      blocksstable::ObSelfBufferWriter &buffer_writer, int64_t &data_length, int64_t &data_zlength);
  int write_backup_file_header_(const ObBackupFileType &file_type);
  int build_backup_file_header_(const ObBackupFileType &file_type, ObBackupFileHeader &file_header);
  int write_backup_file_header_(const ObBackupFileHeader &file_header);
  int build_multi_level_index_header_(
      const int64_t index_type, const int64_t index_level, ObBackupMultiLevelIndexHeader &header);

protected:
  virtual int flush_index_tree_() = 0;

protected:
  bool is_inited_;
  ObBackupIndexMergeParam merge_param_;
  int64_t offset_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  common::ObIODevice *dev_handle_;
  common::ObIOFd io_fd_;
  ObBackupFileWriteCtx write_ctx_;
  ObBackupIndexBufferNode buffer_node_;
  common::ObISQLClient *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupIndexMerger);
};

class ObBackupMacroBlockIndexMerger : public ObIBackupIndexMerger {
public:
  ObBackupMacroBlockIndexMerger();
  virtual ~ObBackupMacroBlockIndexMerger();
  int init(const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  void reset();
  virtual int merge_index() override;

protected:
  virtual int alloc_merge_iter_(const bool tenant_level, const ObBackupIndexMergeParam &merge_param,
      const ObBackupRetryDesc &desc, ObIMacroBlockIndexIterator *&iter);

private:
  int prepare_merge_ctx_(const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  int prepare_merge_iters_(const ObBackupIndexMergeParam &merge_param,
      const common::ObIArray<ObBackupRetryDesc> &retry_list, common::ObISQLClient &sql_proxy,
      MERGE_ITER_ARRAY &merge_iters);
  int prepare_prev_backup_set_index_iter_(
      const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy, ObIMacroBlockIndexIterator *&iter);
  int get_prev_tenant_index_retry_id_(const ObBackupIndexMergeParam &merge_param,
      const share::ObBackupSetDesc &prev_backup_set_desc, const int64_t prev_turn_id, int64_t &retry_id);
  int get_unfinished_iters_(const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &unfinished_iters);
  int find_minimum_iters_(const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &min_iters);
  int prepare_merge_fuser_(ObIBackupMacroBlockIndexFuser *&fuser);
  int fuse_iters_(MERGE_ITER_ARRAY &merge_iters, ObIBackupMacroBlockIndexFuser *fuser);
  int process_result_(const ObBackupMacroRangeIndex &index);
  int move_iters_next_(MERGE_ITER_ARRAY &merge_iters);
  int compare_index_iters_(ObIMacroBlockIndexIterator *lhs, ObIMacroBlockIndexIterator *rhs, int64_t &cmp_ret);
  int write_macro_index_list_();

private:
  int get_output_file_path_(const ObBackupIndexMergeParam &merge_param, share::ObBackupPath &backup_path);
  virtual int flush_index_tree_() override;

private:
  static const int64_t DEFAULT_ITER_COUNT = 2;

private:
  ObBackupMacroBlockIndexComparator comparator_;
  MERGE_ITER_ARRAY merge_iter_array_;
  common::ObArray<ObBackupMacroRangeIndex> tmp_index_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroBlockIndexMerger);
};

class ObBackupMetaIndexMerger : public ObIBackupIndexMerger {
  static const int64_t DEFAULT_ITER_COUNT = 2;
  typedef ObSEArray<ObBackupMetaIndexIterator *, DEFAULT_ITER_COUNT> MERGE_ITER_ARRAY;

public:
  ObBackupMetaIndexMerger();
  virtual ~ObBackupMetaIndexMerger();
  int init(const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  void reset();
  virtual int merge_index() override;

protected:
  virtual int alloc_merge_iter_(const ObBackupIndexMergeParam &merge_param, const ObBackupRetryDesc &retry_desc,
      ObBackupMetaIndexIterator *&iter);

private:
  int prepare_merge_ctx_(
      const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  int prepare_merge_iters_(const ObBackupIndexMergeParam &merge_param,
      const common::ObIArray<ObBackupRetryDesc> &retry_list, MERGE_ITER_ARRAY &merge_iters);
  int get_unfinished_iters_(const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &unfinished_iters);
  int find_minimum_iters_(const MERGE_ITER_ARRAY &merge_iters, MERGE_ITER_ARRAY &min_iters);
  int get_fuse_result_(const MERGE_ITER_ARRAY &iters, ObBackupMetaIndex &meta_index);
  int process_result_(const ObBackupMetaIndex &index);
  int compare_index_iters_(ObBackupMetaIndexIterator *lhs, ObBackupMetaIndexIterator *rhs, int64_t &cmp_ret);
  int move_iters_next_(MERGE_ITER_ARRAY &iters);
  int write_meta_index_list_();

private:
  int get_output_file_path_(
      const ObBackupIndexMergeParam &merge_param, share::ObBackupPath &path);
  virtual int flush_index_tree_() override;

private:
  ObBackupMetaIndexComparator comparator_;
  MERGE_ITER_ARRAY merge_iter_array_;
  common::ObArray<ObBackupMetaIndex> tmp_index_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexMerger);
};

// To address the issue of the macro index not being totally ordered by logic ID across all 4GB backup data files, we can follow these steps:
// 1. Fetching Macro Block Index: Read and extract the macro block index stored in the trailer section of each backup data file.
// 2. Local Sorting: Perform a parallel external sort on the fetched macro block indexes. This will ensure that they are sorted locally within each file.
// 3. Merging Sorted Lists: Merge all the sorted lists obtained from different backup data files into a single list using an efficient merging algorithm like merge sort or heap merge.
// 4. Compression and Flushing: Compress and flush a batch of sorted macro block indexes into the backup device. Each compressed block should be approximately 16KB in size to optimize storage efficiency.
// 5. Building Multi-Level Tree: Construct a multi-level tree structure using the flushed macro block indexes as leaf nodes. This tree will allow for quick retrieval of specific macro blocks during incremental backups when reusing existing blocks is necessary.
// By following these steps, we can achieve a totally ordered list of macro block indexes across all backup data files and efficiently access them when needed for incremental backups.

// TODO(yanfeng): comments need include an estimated space calculation method and examples actual sizes. 

class ObBackupUnorderdMacroBlockIndexMerger : public ObIBackupIndexMerger
{
  class BackupMacroBlockIndexComparator
  {
  public:
    BackupMacroBlockIndexComparator(int &sort_ret);
    bool operator()(const ObBackupMacroBlockIndex *left, const ObBackupMacroBlockIndex *right);
    int &result_code_;
  };
  typedef storage::ObExternalSort<ObBackupMacroBlockIndex, BackupMacroBlockIndexComparator> ExternalSort;
public:
  ObBackupUnorderdMacroBlockIndexMerger();
  virtual ~ObBackupUnorderdMacroBlockIndexMerger();
  int init(const ObBackupIndexMergeParam &merge_param, common::ObMySQLProxy &sql_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  virtual int merge_index() override;
  void reset();
  TO_STRING_KV(K_(merge_param), K_(total_count), K_(consume_count), K_(input_size), K_(output_size));

private:
  virtual int get_prev_tenant_index_retry_id_(const ObBackupIndexMergeParam &merge_param,
      const share::ObBackupSetDesc &prev_backup_set_desc, const int64_t prev_turn_id, int64_t &retry_id);
  int prepare_merge_ctx_(const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle, common::ObIArray<ObIMacroBlockIndexIterator *> &merge_iters);
  virtual int prepare_macro_block_iterators_(
      const ObBackupIndexMergeParam &merge_param, const common::ObIArray<ObBackupRetryDesc> &retry_list, 
      common::ObISQLClient &sql_proxy, common::ObIArray<ObIMacroBlockIndexIterator *> &iterators);
  int prepare_prev_backup_set_index_iter_(
      const ObBackupIndexMergeParam &merge_param, common::ObISQLClient &sql_proxy, ObIMacroBlockIndexIterator *&iter);
  int alloc_merge_iter_(const bool tenant_level, const ObBackupIndexMergeParam &merge_param,
      const ObBackupRetryDesc &retry_desc, ObIMacroBlockIndexIterator *&iter);
  int prepare_parallel_external_sort_(const uint64_t tenant_id);
  int feed_iterators_to_external_sort_(const common::ObArray<ObIMacroBlockIndexIterator *> &iterators);
  int feed_iterator_to_external_sort_(ObIMacroBlockIndexIterator *iterators);
  int do_external_sort_();
  int consume_sort_output_();
  int get_next_batch_macro_index_list_(const int64_t batch_size,
      common::ObIArray<ObBackupMacroBlockIndex> &index_list);
  int get_next_macro_index_(ObBackupMacroBlockIndex &macro_index);
  int write_macro_index_list_(const common::ObArray<ObBackupMacroBlockIndex> &index_list);

private:
  int get_output_file_path_(
      const ObBackupIndexMergeParam &merge_param, share::ObBackupPath &path);
  virtual int flush_index_tree_() override;

private:
  static const int64_t BATCH_SIZE = 2000;
  static const int64_t MACRO_BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t BUF_MEM_LIMIT = 32 * MACRO_BLOCK_SIZE;
  static const int64_t FILE_BUF_SIZE = MACRO_BLOCK_SIZE;
  static const int64_t EXPIRE_TIMESTAMP = 0;

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  int64_t total_count_;
  int64_t consume_count_;
  ExternalSort external_sort_;
  int result_;
  BackupMacroBlockIndexComparator comparator_;
  int64_t input_size_;
  int64_t output_size_;
  ObBackupIndexBlockCompressor compressor_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupUnorderdMacroBlockIndexMerger);
};

}  // namespace backup
}  // namespace oceanbase

#endif
