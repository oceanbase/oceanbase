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

#ifndef STORAGE_LOG_STREAM_BACKUP_ITERATOR_H_
#define STORAGE_LOG_STREAM_BACKUP_ITERATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_ctx.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase {
namespace backup {

class ObBackupDataFileRangeOp : public ObBaseDirEntryOperator {
public:
  ObBackupDataFileRangeOp(const common::ObString &file_prefix);
  virtual ~ObBackupDataFileRangeOp();
  int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<int64_t> &file_list);

private:
  common::ObString file_prefix_;
  common::ObArray<int64_t> file_id_list_;
};

enum ObBackupIndexIteratorType {
  BACKUP_MACRO_BLOCK_INDEX_ITERATOR = 0,
  BACKUP_MACRO_RANGE_INDEX_ITERATOR = 1,
  BACKUP_META_INDEX_ITERATOR = 2,
  MAX_BACKUP_INDEX_ITERATOR,
};

class ObIBackupIndexIterator {
  friend class ObBackupDataFileRangeOp;
  friend class ObBackupMacroBlockIndexStore;

public:
  ObIBackupIndexIterator();
  virtual ~ObIBackupIndexIterator();
  ObIBackupIndexIterator(const int64_t task_id, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id);
  virtual ObBackupIndexIteratorType get_type() const = 0;
  virtual int next() = 0;
  virtual bool is_iter_end() const = 0;

protected:
  int get_file_id_list_(const bool need_read_inner_table, common::ObIArray<int64_t> &file_id_list);
  int get_last_file_id_from_inner_table_(int64_t &last_file_id);
  int get_data_backup_file_path_(const int64_t file_id, share::ObBackupPath &backup_path) const;
  static int get_backup_file_length_(
      const share::ObBackupPath &backup_path, const share::ObBackupStorageInfo *storage_info, int64_t &file_length);
  static int pread_file_(const common::ObString &backup_path, const share::ObBackupStorageInfo *storage_info,
      const int64_t offset, const int64_t read_size, char *buf);
  static int read_data_file_trailer_(const share::ObBackupPath &backup_path, const share::ObBackupStorageInfo *storage_info,
      ObBackupDataFileTrailer &data_file_trailer);
  static int read_index_file_trailer_(const share::ObBackupPath &backup_path, const share::ObBackupStorageInfo *storage_info,
      ObBackupMultiLevelIndexTrailer &index_file_trailer);
  static int read_backup_index_block_(const share::ObBackupPath &backup_path,
      const share::ObBackupStorageInfo *storage_info, const int64_t offset, const int64_t length,
      common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer);
  template <class IndexType>
  static int parse_from_index_blocks_impl_(const int64_t offset, blocksstable::ObBufferReader &buffer_reader,
      common::ObIArray<IndexType> &index_list, common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list);
  static int extract_backup_file_id_(
      const common::ObString &file_name, const common::ObString &prefix, int64_t &file_id, bool &match);

protected:
  bool is_inited_;
  int64_t task_id_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  share::ObBackupDataType backup_data_type_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t cur_file_id_;
  common::ObArray<int64_t> file_id_list_;
  common::ObArray<ObBackupIndexBlockDesc> block_desc_list_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupIndexIterator);
};

class ObIMacroBlockIndexIterator : public ObIBackupIndexIterator {
public:
  ObIMacroBlockIndexIterator() = default;
  virtual ~ObIMacroBlockIndexIterator() = default;
  virtual int get_cur_index(ObBackupMacroRangeIndex &index) = 0;
  VIRTUAL_TO_STRING_KV(K_(is_inited));
};

static const int64_t DEFAULT_MACRO_ITER_COUNT = 2;
typedef common::ObSEArray<ObIMacroBlockIndexIterator *, DEFAULT_MACRO_ITER_COUNT> MERGE_ITER_ARRAY;

class ObBackupMacroBlockIndexIterator : public ObIMacroBlockIndexIterator {
  friend class ObLSBackupCtx;

public:
  ObBackupMacroBlockIndexIterator();
  virtual ~ObBackupMacroBlockIndexIterator();
  int init(const int64_t task_id, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
      const bool need_read_inner_table);
  virtual int next() override;
  virtual bool is_iter_end() const override;
  virtual int get_cur_index(ObBackupMacroRangeIndex &index) override;
  virtual ObBackupIndexIteratorType get_type() const override
  {
    return BACKUP_MACRO_BLOCK_INDEX_ITERATOR;
  }
  TO_STRING_KV(K_(task_id), K_(backup_dest), K_(tenant_id), K_(backup_set_desc), K_(ls_id), K_(backup_data_type),
      K_(turn_id), K_(retry_id), K_(cur_file_id), K_(file_id_list), K_(cur_idx), K(cur_index_list_.count()), K_(cur_index_list),
     "type", "backup macro block index iterator");

private:
  bool need_fetch_new_() const;
  int do_fetch_new_();
  int inner_do_fetch_new_(const int64_t file_id);
  int parse_from_index_blocks_(const int64_t offset, blocksstable::ObBufferReader &buffer,
      common::ObIArray<ObBackupMacroBlockIndex> &index_list, common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list);
  int fetch_macro_index_list_(const int64_t file_id, common::ObIArray<ObBackupMacroBlockIndex> &cur_list,
      common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list);
  int inner_get_next_macro_range_index_(ObBackupMacroRangeIndex &index);
  int get_next_block_last_index_(ObBackupIndexBlockDesc &block_desc);

protected:
  int64_t cur_idx_;
  common::ObArray<ObBackupMacroBlockIndex> cur_index_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroBlockIndexIterator);
};

class ObBackupMacroRangeIndexIterator : public ObIMacroBlockIndexIterator {
public:
  ObBackupMacroRangeIndexIterator();
  virtual ~ObBackupMacroRangeIndexIterator();
  int init(const int64_t task_id, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id);
  virtual int next() override;
  virtual bool is_iter_end() const override;
  virtual int get_cur_index(ObBackupMacroRangeIndex &index) override;
  virtual ObBackupIndexIteratorType get_type() const override
  {
    return BACKUP_MACRO_RANGE_INDEX_ITERATOR;
  }
  TO_STRING_KV(K_(backup_path), K_(tenant_id), K_(backup_set_desc), K_(ls_id), K_(cur_idx), K_(cur_index_list), K(cur_index_list_.count()),
      "type", "backup_macro_range_index_iterator");

private:
  int get_range_index_backup_path_(share::ObBackupPath &backup_path) const;
  bool need_fetch_new_() const;
  int do_fetch_new_();
  int read_block_(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const int64_t offset,
      const int64_t length, common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer_reader);
  int decode_block_(blocksstable::ObBufferReader &buffer_reader);
  int fetch_new_block_();
  int get_macro_range_index_list_(
      blocksstable::ObBufferReader &buffer_reader, common::ObIArray<ObBackupMacroRangeIndex> &index_list);
  int get_data_file_size_(int64_t &file_size);
  int get_current_read_size_(int64_t &read_size);

private:
  bool meet_end_;
  int64_t read_offset_;
  int64_t file_length_;
  share::ObBackupPath backup_path_;
  blocksstable::ObBufferReader buffer_reader_;
  int64_t cur_idx_;
  common::ObArray<ObBackupMacroRangeIndex> cur_index_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroRangeIndexIterator);
};

class ObBackupMetaIndexIterator : public ObIBackupIndexIterator {
  friend class ObLSBackupCtx;

public:
  ObBackupMetaIndexIterator();
  virtual ~ObBackupMetaIndexIterator();
  int init(const int64_t task_id, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
      const bool is_sec_meta, const bool need_read_inner_table = true);
  virtual int next() override;
  virtual bool is_iter_end() const override;
  int get_cur_index(ObBackupMetaIndex &meta_index);
  virtual ObBackupIndexIteratorType get_type() const override
  {
    return BACKUP_META_INDEX_ITERATOR;
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(backup_data_type), K_(cur_file_id), K_(file_id_list), K_(cur_idx),
      K(cur_index_list_.count()), K_(turn_id), K_(retry_id));

private:
  bool need_fetch_new_() const;
  int do_fetch_new_();
  int inner_do_fetch_new_(const int64_t file_id);
  int parse_from_index_blocks_(const int64_t offset, blocksstable::ObBufferReader &buffer,
      common::ObIArray<ObBackupMetaIndex> &index_list, common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list);
  int fetch_meta_index_list_(const int64_t file_id, common::ObIArray<ObBackupMetaIndex> &index_list,
      common::ObIArray<ObBackupIndexBlockDesc> &block_desc_list);
  int filter_meta_index_list_(common::ObIArray<ObBackupMetaIndex> &index_list);

private:
  bool is_sec_meta_;
  int64_t cur_idx_;
  common::ObArray<ObBackupMetaIndex> cur_index_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexIterator);
};

}  // namespace backup
}  // namespace oceanbase

#endif
