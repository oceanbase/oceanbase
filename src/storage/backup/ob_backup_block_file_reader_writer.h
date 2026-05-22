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

#ifndef _OCEANBASE_BACKUP_OB_BACKUP_BLOCK_FILE_READER_WRITER_H_
#define _OCEANBASE_BACKUP_OB_BACKUP_BLOCK_FILE_READER_WRITER_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "common/storage/ob_io_device.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_mutex.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_serialize_provider.h"

namespace oceanbase
{
namespace backup
{

// Data type enumeration for different kinds of payload stored in block files
enum ObBackupBlockFileDataType {
  MACRO_BLOCK_ID = 0,
  FILE_PATH_INFO = 1,
  MAX_TYPE,
};

enum ObBackupFileListDataType
{
  BACKUP_FILE_LIST_DIR_INFO = 0,
  BACKUP_FILE_LIST_FILE_INFO = 1,
  BACKUP_FILE_LIST_MAX_TYPE = 2,
};

class ObIBackupBlockFileItem
{
public:
  virtual ~ObIBackupBlockFileItem() {}
  virtual bool is_valid() const = 0;
  virtual int64_t get_serialize_size() const = 0;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const = 0;
  virtual int deserialize(const char *buf, const int64_t buf_len, int64_t &pos) = 0;
  virtual void reset() = 0;
  virtual int64_t data_type() const = 0;
};

class ObBackupBlockFileMacroIdItem final : public ObIBackupBlockFileItem
{
  OB_UNIS_VERSION(1);

public:
  ObBackupBlockFileMacroIdItem() : macro_id_() {}
  explicit ObBackupBlockFileMacroIdItem(const blocksstable::MacroBlockId &id) : macro_id_(id) {}
  ~ObBackupBlockFileMacroIdItem() override {}

  bool is_valid() const override { return macro_id_.is_valid(); }
  void reset() override { macro_id_.reset(); }
  int64_t data_type() const override { return MACRO_BLOCK_ID; }
  int assign(const ObBackupBlockFileMacroIdItem &other);

  TO_STRING_KV(K_(macro_id));

public:
  blocksstable::MacroBlockId macro_id_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupBlockFileMacroIdItem);
};

struct ObBackupFileInfo final : public ObIBackupBlockFileItem
{
  OB_UNIS_VERSION(1);

public:
  ObBackupFileInfo();
  ~ObBackupFileInfo() override {}

  bool is_valid() const override;
  void reset() override;
  int set_file_info(const char *file_name, const int64_t file_size);
  int set_file_info(const share::ObBackupPath &file_path, const int64_t file_size);
  int set_dir_info(const char *dir_name);
  int64_t data_type() const override { return FILE_PATH_INFO; }
  bool is_file() const { return BACKUP_FILE_LIST_FILE_INFO == type_; }
  bool is_dir() const { return BACKUP_FILE_LIST_DIR_INFO == type_; }
  int assign(const ObBackupFileInfo &other);
  bool operator<(const ObBackupFileInfo &other) const;

  TO_STRING_KV(K_(type), K_(file_size), K_(path));

public:
  ObBackupFileListDataType type_;
  int64_t file_size_;
  share::ObBackupPathString path_;
};

struct ObBackupFileListInfo final
{
public:
  ObBackupFileListInfo();
  ~ObBackupFileListInfo() {}
  void reset();
  bool is_empty() const { return file_list_.empty(); }
  bool is_valid() const;
  int push_file_info(const ObBackupFileInfo &file_info);
  int push_file_info(const share::ObBackupPath &file_path, const int64_t file_size);
  int push_dir_info(const share::ObBackupPath &dir_path);
  int assign(const ObBackupFileListInfo &other);
  int64_t count() const { return file_list_.count(); }
  void sort_file_list();
  TO_STRING_KV(K_(file_list));
  common::ObArray<ObBackupFileInfo> file_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupFileListInfo);
};

class ObBackupDirListOp : public ObBaseDirEntryOperator
{
public:
  ObBackupDirListOp(ObIArray<ObBackupFileInfo> &filelist);
  virtual ~ObBackupDirListOp() {}
  bool need_get_file_meta() const override { return true; }
  int func(const dirent *entry) override;

  TO_STRING_KV(K_(filelist));

private:
  ObIArray<ObBackupFileInfo> &filelist_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDirListOp);
};

/*
Single File Format
+------------------+
| Block 0 (2MB)    |  <-- entry_block (FirstWritten Block)
| - CommonHeader   |
| - BlockFileHeader|
| - Data           |
+------------------+
| Block 1 (2MB)    |
| - CommonHeader   |
| - BlockFileHeader|
| - Data           |
+------------------+
| ...              |
+------------------+
| Block N (2MB)    |
| - CommonHeader   |
| - BlockFileHeader|
| - Data           |
+------------------+
| FileTailer       |
| - entry_block_id |
| - total_blocks   |
| - file_length    |
| - checksum       |
+------------------+

Multi File Format（64MB Limit）

When the data exceeds 64MB, it will be automatically divided into multiple files:

File data.0 (Up to 64MB):
+------------------+
| Block 0 (2MB)    | <-- entry_block
| Block 1 (2MB)    |
| ...              |
| Block M (2MB)    |
+------------------+
(File packaging, without tailer)

File data.1 (Up to 64MB):
+------------------+
| Block M+1 (2MB)  |
| Block M+2 (2MB)  |
| ...              |
| Block N (2MB)    |
+------------------+
(File packaging, without tailer)

File data.2 (The last file):
+------------------+
| Block N+1 (2MB)  |
| ...              |
| Block K (2MB)    |
+------------------+
| FileTailer       | <-- Only the last file has a tailer.
| - entry_block_id |     Contains all the information of the entire file group
| - total_blocks   |     "entry_block" points to the first last written block
+------------------+
```
*/

struct ObBackupBlockFileAddr final
{
  OB_UNIS_VERSION(1);
public:
  ObBackupBlockFileAddr();
  ~ObBackupBlockFileAddr();
  int set(const int64_t file_id, const int64_t offset, const int64_t length);
  void reset();
  bool is_valid() const;
  void operator=(const ObBackupBlockFileAddr &other);
  uint64_t calc_checksum(const uint64_t checksum) const;

  TO_STRING_KV(K_(file_id), K_(offset), K_(length));

  int64_t file_id_;
  int64_t offset_;
  int64_t length_;
};

struct ObBackupBlockFileHeader final
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t VERSION = 0;
  static const int64_t MAGIC = 1;
public:
  ObBackupBlockFileHeader();
  ~ObBackupBlockFileHeader();
  bool is_valid() const;

  TO_STRING_KV(K_(version), K_(magic), K_(data_type), K_(item_count));
  int32_t version_;
  int32_t magic_;
  int64_t data_type_;  // ObBackupBlockFileDataType
  int64_t item_count_;
};

// The metadata structure at the end of the file is used to record the entry information of the block file sequence.
struct ObBackupBlockFileTailer final
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t VERSION = 1;
  // Magic marker for file tailer integrity identification
  static const int64_t MAGIC = 0x54414C49455246;

public:
  ObBackupBlockFileTailer();
  ~ObBackupBlockFileTailer();
  void reset();
  bool is_valid() const;
  int check_integrity() const;
  uint64_t calc_checksum_for_log() const { return calc_checksum_(); }
  int set_entry_block_info(const ObBackupBlockFileAddr &entry_block_addr,
                           const int64_t total_block_count,
                           const int64_t last_file_id,
                           const int64_t file_length);

  TO_STRING_KV(K_(version), K_(magic), K_(entry_block_addr), K_(total_block_count),
               K_(last_file_id), K_(file_length), K_(checksum));

  int64_t version_;
  int64_t magic_;
  ObBackupBlockFileAddr entry_block_addr_;  // Address of the block file entry block
  int64_t total_block_count_;
  int64_t last_file_id_;                     // ID of the last backup file carrying this tailer
  int64_t file_length_;                       // Total file length (excluding tail metadata)
  uint64_t checksum_;

private:
  uint64_t calc_checksum_() const;
};

class ObBackupBlockFileWriter final
{
public:
  ObBackupBlockFileWriter();
  ~ObBackupBlockFileWriter();

  int init(const common::ObObjectStorageInfo *storage_info,
           const ObBackupBlockFileDataType data_type,
           const share::ObBackupFileSuffix &suffix,
           const share::ObBackupPath &file_list_dir,
           const int64_t dest_id,
           const int64_t max_file_size);

  int write_block(const blocksstable::ObBufferReader &buffer_reader,
                  const int64_t item_count,
                  const int64_t data_type,
                  ObBackupBlockFileAddr &block_addr);

  int close(const ObBackupBlockFileAddr &entry_block_addr);

private:
  int check_and_switch_file_();

  int switch_to_new_file_();

  int build_block_data_(const blocksstable::ObBufferReader &buffer_reader,
                        const int64_t item_count,
                        const int64_t data_type,
                        blocksstable::ObSelfBufferWriter &block_writer);

  int write_block_to_storage_(const blocksstable::ObBufferWriter &block_data,
                              ObBackupBlockFileAddr &block_addr);

  int write_file_tailer_(const ObBackupBlockFileAddr &entry_block_addr);

  int get_file_path_(const int64_t file_id, share::ObBackupPath &path);

  int ensure_dir_created_();

  int close_current_file_();

private:
  static const int64_t DATA_VERSION = 1;
  static const int64_t BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE; // 2MB
  static const int64_t TAILER_SIZE = sizeof(ObBackupBlockFileTailer);

private:
  bool is_inited_;
  bool is_dir_created_;
  const common::ObObjectStorageInfo *storage_info_;
  ObBackupBlockFileDataType data_type_;
  share::ObBackupFileSuffix suffix_;
  share::ObBackupPath file_list_dir_;
  int64_t current_file_id_;       // Current file ID (starting from 0)
  int64_t file_offset_;
  int64_t total_block_count_;
  int64_t total_file_count_;
  common::ObStorageIdMod mod_;
  common::ObArenaAllocator allocator_;
  int64_t max_file_size_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupBlockFileWriter);
};

class ObBackupBlockFileReader final
{
public:
  ObBackupBlockFileReader();
  ~ObBackupBlockFileReader();

  int init(const common::ObObjectStorageInfo *storage_info,
           const ObBackupBlockFileDataType data_type,
           const share::ObBackupPath &file_list_dir,
           const share::ObBackupFileSuffix &suffix,
           const ObStorageIdMod &mod);

  // Get the next block (forward iteration)
  int get_next_block(blocksstable::ObBufferReader &buffer_reader,
                     ObBackupBlockFileAddr &block_addr,
                     int64_t &item_count,
                     int64_t &data_type);

private:
  int detect_file_count_();

  int read_last_file_tailer_();

  int read_block_(const ObBackupBlockFileAddr &block_addr,
                 blocksstable::ObBufferReader &buffer_reader);

  int parse_block_headers_(blocksstable::ObBufferReader &buffer_reader,
                           int64_t &data_type);

  int get_file_path_(const int64_t file_id, share::ObBackupPath &uri);

  int ensure_current_file_size_();

private:
  static const int64_t BLOCK_SIZE;
  static const int64_t TAILER_SIZE;

private:
  bool is_inited_;
  share::ObBackupPath file_list_dir_;
  share::ObBackupFileSuffix suffix_;
  ObBackupBlockFileDataType data_type_;
  const common::ObObjectStorageInfo *storage_info_;
  ObBackupBlockFileTailer file_tailer_;
  share::ObBackupCommonHeader common_header_;
  ObBackupBlockFileHeader block_file_header_;
  int64_t total_block_count_;
  int64_t read_block_count_;
  int64_t file_count_;
  int64_t current_file_id_;     // Current file being read
  int64_t current_offset_;       // Current offset in the file
  int64_t current_file_size_;   // Cached length of the current file being read
  char *buf_;
  int64_t buf_len_;
  ObArenaAllocator allocator_;
  ObStorageIdMod mod_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupBlockFileReader);
};

class ObBackupBlockFileItemWriter final
{
public:
  ObBackupBlockFileItemWriter();
  ~ObBackupBlockFileItemWriter();

  int init(const common::ObObjectStorageInfo *storage_info,
           const ObBackupBlockFileDataType data_type,
           const share::ObBackupFileSuffix &suffix,
           const share::ObBackupPath &file_list_dir,
           const int64_t dest_id,
           const int64_t max_file_size);

  int write_item(const ObIBackupBlockFileItem &item);

  int close(ObBackupBlockFileAddr &entry_block_addr,
            int64_t &total_item_count,
            int64_t &total_block_count);

private:
  int check_buffer_capacity_(const int64_t need_size, bool &need_flush);
  int flush_buffer_();
  int build_block_data_(blocksstable::ObSelfBufferWriter &block_writer);

private:
  static const int64_t BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE; // 2MB
  static const int64_t RESERVED_SIZE = 1024 * 16; // Reserve 16KB for headers

private:
  bool is_inited_;
  bool is_closed_;
  ObBackupBlockFileWriter block_writer_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  common::ObArray<ObBackupBlockFileAddr> block_addr_list_;
  int64_t current_item_count_;  // The number of items currently in the buffer zone
  int64_t total_item_count_;
  int64_t total_block_count_;
  int64_t current_block_data_type_;
  common::ObArenaAllocator allocator_;
  mutable lib::ObMutex mutex_;
  int64_t max_file_size_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupBlockFileItemWriter);
};

class ObBackupBlockFileItemReader final
{
public:
  ObBackupBlockFileItemReader();
  ~ObBackupBlockFileItemReader();

  int init(const common::ObObjectStorageInfo *storage_info,
           const ObBackupBlockFileDataType data_type,
           const share::ObBackupPath &file_list_dir,
           const share::ObBackupFileSuffix &suffix,
           const ObStorageIdMod &mod);

  int get_next_item(ObIBackupBlockFileItem &item);

private:
  int load_next_block_();
  int validate_block_type_(int64_t file_data_type);

private:
  bool is_inited_;
  ObBackupBlockFileReader block_reader_;
  blocksstable::ObBufferReader current_buffer_;
  int64_t current_pos_;
  int64_t current_item_count_;
  int64_t item_index_;
  int64_t current_data_type_;
  bool has_more_blocks_;
  ObBackupBlockFileAddr entry_block_addr_;
  int64_t total_block_count_;
  common::ObArenaAllocator allocator_;
  mutable lib::ObMutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupBlockFileItemReader);
};

class ObBackupFileListWriterUtil final
{
public:
  static int add_file_to_file_list_info(
      const share::ObBackupPath &file_path,
      const share::ObBackupStorageInfo storage_info,
      ObBackupFileListInfo &file_list_info);
  static int add_file_to_file_list_info(
      const share::ObBackupPath &file_path,
      const share::ObIBackupSerializeProvider &file_serializer,
      ObBackupFileListInfo &file_list_info);
  static int write_file_list_to_path(
      const common::ObObjectStorageInfo *storage_info,
      const share::ObBackupFileSuffix &suffix,
      const share::ObBackupPath &file_list_dir,
      const int64_t dest_id,
      ObBackupFileListInfo &file_list_info);
};

class ObBackupFileListReaderUtil final
{
public:
  static int get_all_dir_list(
      const common::ObObjectStorageInfo *storage_info,
      const share::ObBackupPath &dir_path,
      const share::ObBackupFileSuffix &suffix,
      const common::ObStorageIdMod &storage_id_mod,
      common::ObIArray<share::ObBackupPathString> &dir_list);
  static int read_file_list_from_path(
      const common::ObObjectStorageInfo *storage_info,
      const share::ObBackupPath &file_list_dir,
      const share::ObBackupFileSuffix &suffix,
      const common::ObStorageIdMod &storage_id_mod,
      ObBackupFileListInfo &file_list_info);
  static int compose_absolute_path(
      const share::ObBackupPath &dir_path,
      const share::ObBackupPathString &relative_path,
      share::ObBackupPathString &child_dir_path);
  static int get_ls_id_list(
      const common::ObObjectStorageInfo *storage_info,
      const share::ObBackupPath &file_list_dir,
      const share::ObBackupFileSuffix &suffix,
      const common::ObStorageIdMod &storage_id_mod,
      common::ObIArray<share::ObLSID> &ls_id_list);
private:
  static int get_dir_list_(
      const common::ObObjectStorageInfo *storage_info,
      const share::ObBackupPath &dir_path,
      const share::ObBackupFileSuffix &suffix,
      const common::ObStorageIdMod &storage_id_mod,
      common::ObIArray<share::ObBackupPathString> &dir_list);
};

} // namespace backup
} // namespace oceanbase

#endif // _OCEANBASE_BACKUP_OB_BACKUP_BLOCK_FILE_READER_WRITER_H_
