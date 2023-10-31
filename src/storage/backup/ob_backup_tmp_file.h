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

#ifndef STORAGE_LOG_STREAM_BACKUP_TMP_FILE_H_
#define STORAGE_LOG_STREAM_BACKUP_TMP_FILE_H_

#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "storage/blocksstable/ob_data_buffer.h"

namespace oceanbase {
namespace backup {

class ObBackupTmpFile {
public:
  ObBackupTmpFile();
  virtual ~ObBackupTmpFile();
  int open(const uint64_t tenant_id);
  int write(const char *buf, const int64_t size);
  int close();
  bool is_opened() const { return is_opened_; }

  int64_t get_fd() const
  {
    return file_fd_;
  }
  int64_t get_dir() const
  {
    return file_dir_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_file_size() const
  {
    return file_size_;
  }

  TO_STRING_KV(K_(is_opened), K_(tenant_id), K_(file_dir), K_(file_fd), K_(file_size));

private:
  int get_io_info_(const char *buf, const int64_t size, const int64_t timeout_ms, blocksstable::ObTmpFileIOInfo &io_info);

private:
  bool is_opened_;
  uint64_t tenant_id_;
  int64_t file_dir_;
  int64_t file_fd_;
  int64_t file_size_;
};

class ObBackupIndexBufferNode {
public:
  ObBackupIndexBufferNode();
  virtual ~ObBackupIndexBufferNode();
  int init(const uint64_t tenant_id, const ObBackupBlockType &block_type, const int64_t node_level);
  template <typename T>
  int put_backup_index(const T &backup_index);
  template <typename T>
  int get_backup_index(T &backup_index);
  void reset();

public:
  bool is_inited() const;
  ObBackupTmpFile &get_tmp_file();
  int64_t get_estimate_write_size() const;
  uint64_t get_tenant_id() const;
  ObBackupBlockType get_block_type() const;
  int64_t get_node_level() const;
  void add_estimate_write_size(const int64_t size);
  int64_t get_read_offset() const;
  void set_next(ObBackupIndexBufferNode *next);
  ObBackupIndexBufferNode *get_next() const;
  int64_t get_read_count() const;
  int64_t get_write_count() const;

  TO_STRING_KV(
      K_(is_inited), K_(tenant_id), K_(block_type), K_(node_level), K_(read_count), K_(write_count), KP_(next));

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObBackupBlockType block_type_;
  int64_t node_level_;
  int64_t estimate_size_;
  ObBackupTmpFile tmp_file_;
  int64_t read_offset_;
  ObBackupIndexBufferNode *next_;
  int64_t read_count_;
  int64_t write_count_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
};

template <typename T>
int ObBackupIndexBufferNode::put_backup_index(const T &backup_index)
{
  int ret = OB_SUCCESS;
  int64_t last_pos = buffer_writer_.pos();
  const int64_t need_write_size = backup_index.get_serialize_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "buffer node do not init", K(ret));
  } else if (OB_FAIL(buffer_writer_.write_serialize(backup_index))) {
    OB_LOG(WARN, "failed to write serialize", K(ret), K(backup_index));
  } else if (buffer_writer_.pos() - last_pos > need_write_size) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN,
        "serialize size must not be larger than need write size",
        K(ret),
        "cur_pos",
        buffer_writer_.pos(),
        K(last_pos),
        K(need_write_size),
        K(backup_index));
  } else if (OB_FAIL(tmp_file_.write(buffer_writer_.data(), buffer_writer_.pos()))) {
    OB_LOG(WARN, "failed to write to tmp file", K(ret), K_(buffer_writer));
  } else {
    add_estimate_write_size(buffer_writer_.pos() - last_pos);
    buffer_writer_.reuse();
    write_count_++;
  }
  return ret;
}

template <typename T>
int ObBackupIndexBufferNode::get_backup_index(T &backup_index)
{
  int ret = OB_SUCCESS;
  backup_index.reset();
  const int64_t need_read_size = sizeof(T);
  const int64_t timeout_ms = 5000;
  blocksstable::ObTmpFileIOInfo io_info;
  blocksstable::ObTmpFileIOHandle handle;
  io_info.fd_ = tmp_file_.get_fd();
  io_info.tenant_id_ = tmp_file_.get_tenant_id();
  io_info.io_desc_.set_wait_event(2);
  io_info.size_ = std::min(need_read_size, estimate_size_ - read_offset_);
  io_info.io_timeout_ms_ = timeout_ms;
  common::ObArenaAllocator allocator;
  char *buf = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "buffer node do not init", K(ret));
  } else if (need_read_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "get invalid args", K(ret), K(need_read_size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(need_read_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to alloc memory", K(ret), K(need_read_size));
  } else if (FALSE_IT(io_info.buf_ = buf)) {
  } else if (OB_FAIL(blocksstable::ObTmpFileManager::get_instance().pread(io_info, read_offset_, handle))) {
    OB_LOG(WARN, "failed to pread from tmp file", K(ret), K(io_info), K_(read_offset), K(need_read_size));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, need_read_size);
    if (OB_FAIL(buffer_reader.read_serialize(backup_index))) {
      OB_LOG(WARN, "failed to read serialize", K(ret));
    } else {
      read_offset_ += buffer_reader.pos();
      read_count_++;
    }
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase

#endif
