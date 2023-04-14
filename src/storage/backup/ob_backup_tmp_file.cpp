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

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_backup_tmp_file.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

namespace oceanbase {
namespace backup {

/* ObBackupTmpFile */

ObBackupTmpFile::ObBackupTmpFile()
  : is_opened_(false),
    tenant_id_(OB_INVALID_ID),
    file_dir_(-1),
    file_fd_(-1),
    file_size_(0)
{}

ObBackupTmpFile::~ObBackupTmpFile()
{}

int ObBackupTmpFile::open(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tmp file init twice", K(ret));
  } else if (OB_FAIL(ObTmpFileManager::get_instance().alloc_dir(file_dir_))) {
    LOG_WARN("failed to alloc dir", K(ret));
  } else if (OB_FAIL(ObTmpFileManager::get_instance().open(file_fd_, file_dir_))) {
    LOG_WARN("failed to open tmp file", K(ret), K(file_dir_));
  } else {
    tenant_id_ = tenant_id;
    file_size_ = 0;
    is_opened_ = true;
    LOG_INFO("success to open backup tmp file", K_(tenant_id), K_(file_dir), K_(file_fd));
  }
  return ret;
}

int ObBackupTmpFile::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTmpFileIOInfo io_info;
  const int64_t timeout_ms = 5000;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tmp file init twice", K(ret));
  } else if (OB_FAIL(get_io_info_(buf, size, io_info))) {
    LOG_WARN("failed to get io info", K(ret), K(buf), K(size));
  } else if (OB_FAIL(ObTmpFileManager::get_instance().write(io_info, timeout_ms))) {
    LOG_WARN("failed to write tmp file", K(ret), K(io_info), K(timeout_ms));
  } else {
    file_size_ += size;
    LOG_INFO("backup tmp file write", K(buf), K(size));
  }
  return ret;
}

int ObBackupTmpFile::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tmp file do not init", K(ret));
  } else if (OB_FAIL(ObTmpFileManager::get_instance().remove(file_fd_))) {
    LOG_WARN("failed to remove tmp file fd", K(ret), K(file_fd_));
  } else {
    is_opened_ = false;
    LOG_INFO("success to close backup tmp file", K(ret), K_(tenant_id), K_(file_dir), K_(file_fd));
  }
  return ret;
}

int ObBackupTmpFile::get_io_info_(const char *buf, const int64_t size, ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  io_info.reset();
  io_info.fd_ = file_fd_;
  io_info.tenant_id_ = tenant_id_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = const_cast<char *>(buf);
  io_info.size_ = size;
  return ret;
}

/* ObBackupIndexBufferNode */

ObBackupIndexBufferNode::ObBackupIndexBufferNode()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      block_type_(BACKUP_BLOCK_MAX),
      node_level_(-1),
      estimate_size_(0),
      tmp_file_(),
      read_offset_(0),
      next_(NULL),
      read_count_(0),
      write_count_(0),
      buffer_writer_("BackupTmpFile")
{}

ObBackupIndexBufferNode::~ObBackupIndexBufferNode()
{
  reset();
}

int ObBackupIndexBufferNode::init(
    const uint64_t tenant_id, const ObBackupBlockType &block_type, const int64_t node_level)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || node_level < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(node_level));
  } else if (OB_FAIL(tmp_file_.open(tenant_id))) {
    LOG_WARN("failed to open tmp file", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    block_type_ = block_type;
    node_level_ = node_level;
    estimate_size_ = 0;
    next_ = NULL;
    read_count_ = 0;
    write_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupIndexBufferNode::reset()
{
  int tmp_ret = OB_SUCCESS;
  if (!tmp_file_.is_opened()) {
    // do nothing
  } else if (OB_TMP_FAIL(tmp_file_.close())) {
    LOG_ERROR_RET(tmp_ret, "failed to close tmp file", K(tmp_ret));
  }
}

bool ObBackupIndexBufferNode::is_inited() const
{
  return is_inited_;
}

ObBackupTmpFile &ObBackupIndexBufferNode::get_tmp_file()
{
  return tmp_file_;
}

int64_t ObBackupIndexBufferNode::get_estimate_write_size() const
{
  return estimate_size_;
}

uint64_t ObBackupIndexBufferNode::get_tenant_id() const
{
  return tenant_id_;
}

ObBackupBlockType ObBackupIndexBufferNode::get_block_type() const
{
  return block_type_;
}

int64_t ObBackupIndexBufferNode::get_node_level() const
{
  return node_level_;
}

void ObBackupIndexBufferNode::add_estimate_write_size(const int64_t size)
{
  estimate_size_ += size;
}

int64_t ObBackupIndexBufferNode::get_read_offset() const
{
  return read_offset_;
}

void ObBackupIndexBufferNode::set_next(ObBackupIndexBufferNode *next)
{
  next_ = next;
}

ObBackupIndexBufferNode *ObBackupIndexBufferNode::get_next() const
{
  return next_;
}

int64_t ObBackupIndexBufferNode::get_read_count() const
{
  return read_count_;
}

int64_t ObBackupIndexBufferNode::get_write_count() const
{
  return write_count_;
}

}  // namespace backup
}  // namespace oceanbase
