// owner: anli.hx
// owner group: log

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

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>

#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private
#include "lib/utility/ob_tracepoint.h"
#include "share/resource_manager/ob_resource_manager.h"       // ObResourceManager
#include "logservice/palf/log_entry.h"
#include "logservice/palf/log_meta_entry.h"

const std::string TEST_NAME = "log_engine";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogClusterLogEngine : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterLogEngine()
      : ObSimpleLogClusterTestEnv(),
        id_(INVALID_PALF_ID),
        palf_epoch_(0),
        leader_idx_(-1),
        last_reloaded_writable_size_(-1),
        last_reloaded_need_block_header_(false),
        log_engine_(NULL)
  {
  }
  ~TestObSimpleLogClusterLogEngine() { destroy(); }
  int init()
  {
    int ret = OB_SUCCESS;
    id_ = ATOMIC_AAF(&palf_id_, 1);
    if (OB_FAIL(create_paxos_group(id_, leader_idx_, leader_))) {
      PALF_LOG(ERROR, "create_paxos_group failed", K(ret));
    } else {
      log_engine_ = &leader_.palf_handle_impl_->log_engine_;
    }
    return ret;
  }
  int reload(const LSN &log_tail_redo, const LSN &log_tail_meta, const LSN &base_lsn)
  {
    int ret = OB_SUCCESS;
    palf_epoch_ = ATOMIC_AAF(&palf_epoch_, 1);
    LogGroupEntryHeader entry_header;
    bool is_integrity = true;
    ObILogAllocator *alloc_mgr = log_engine_->alloc_mgr_;
    LogRpc *log_rpc = log_engine_->log_net_service_.log_rpc_;
    LogIOWorkerBase *log_io_worker = log_engine_->io_task_submitter_;
    LogSharedQueueTh *log_shared_queue_th = log_engine_->log_shared_queue_th_;
    palf::LogPlugins *plugins = log_engine_->plugins_;
    const LogIOMode desired_io_mode = log_engine_->log_meta_
        .get_log_replica_property_meta().get_log_io_mode();
    LogIOAdapter io_adapter;
    LogEngine log_engine;
    ILogBlockPool *log_block_pool = log_engine_->log_storage_.block_mgr_.log_block_pool_;
    if (OB_FAIL(io_adapter.init(1002, LOG_IO_DEVICE_WRAPPER.get_local_device(), &G_RES_MGR, &OB_IO_MANAGER))) {
      PALF_LOG(WARN, "io_adapter init failed", K(ret));
    } else if (OB_FAIL(log_engine.load(leader_.palf_handle_impl_->palf_id_,
                                leader_.palf_handle_impl_->log_dir_,
                                alloc_mgr,
                                log_block_pool,
                                &(leader_.palf_handle_impl_->log_cache_),
                                log_rpc,
                                log_io_worker,
                                log_shared_queue_th,
                                plugins,
                                entry_header,
                                palf_epoch_,
                                PALF_BLOCK_SIZE,
                                PALF_META_BLOCK_SIZE,
                                &io_adapter,
                                desired_io_mode,
                                is_integrity))) {
      PALF_LOG(WARN, "load failed", K(ret));
    } else if (log_tail_redo != log_engine.log_storage_.log_tail_
        || log_tail_meta != log_engine.log_meta_storage_.log_tail_
        || base_lsn != log_engine.log_meta_.log_snapshot_meta_.base_lsn_) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "reload failed", K(ret), K(log_engine), KPC(log_engine_), K(log_tail_redo), K(log_tail_meta), K(base_lsn));
    } else {
      last_reloaded_writable_size_ = log_engine.log_storage_.curr_block_writable_size_;
      last_reloaded_need_block_header_ = log_engine.log_storage_.need_append_block_header_;
      PALF_LOG(INFO, "reload success", K(log_engine), KPC(log_engine_));
    }
    return ret;
  }
  int restart_current_palf()
  {
    int ret = OB_SUCCESS;
    const int64_t restart_server_idx = leader_idx_;
    leader_.reset();
    log_engine_ = NULL;
    if (OB_FAIL(restart_server(restart_server_idx))) {
      PALF_LOG(WARN, "restart server failed", K(ret), K(restart_server_idx), K(id_));
    } else if (OB_FAIL(get_leader(id_, leader_, leader_idx_))) {
      PALF_LOG(WARN, "get leader after restart failed", K(ret), K(id_), K(restart_server_idx));
    } else {
      log_engine_ = &leader_.palf_handle_impl_->log_engine_;
    }
    return ret;
  }

  int persist_log_io_mode(const LogIOMode io_mode)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(log_engine_)) {
      ret = OB_NOT_INIT;
      PALF_LOG(WARN, "log engine is null", K(ret));
    } else if (OB_FAIL(log_engine_->update_log_io_mode_after_recovery_(io_mode))) {
      PALF_LOG(WARN, "persist log io mode failed", K(ret),
               "io_mode", log_io_mode_to_str(io_mode));
    }
    return ret;
  }

  int persist_log_replica_property_meta(const LogReplicaPropertyMeta &property_meta)
  {
    int ret = OB_SUCCESS;
    LogMeta next_log_meta;
    if (OB_ISNULL(log_engine_) || !property_meta.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(ret), KP(log_engine_), K(property_meta));
    } else if (FALSE_IT(next_log_meta = log_engine_->log_meta_)) {
    } else if (OB_FAIL(next_log_meta.update_log_replica_property_meta(property_meta))) {
      PALF_LOG(WARN, "update log replica property meta failed", K(ret), K(property_meta));
    } else if (OB_FAIL(log_engine_->append_log_meta_(next_log_meta))) {
      PALF_LOG(WARN, "append log meta failed", K(ret), K(property_meta));
    } else {
      log_engine_->log_meta_ = next_log_meta;
    }
    return ret;
  }

  int delete_block_by_human(const block_id_t block_id)
  {
    int ret = OB_SUCCESS;
    int pret = 0;
    char file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    char backup_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    const char *log_dir = log_engine_->log_storage_.block_mgr_.log_dir_;
    if (OB_FAIL(convert_to_normal_block(log_dir, block_id, file_path, OB_MAX_FILE_NAME_LENGTH))) {
      PALF_LOG(WARN, "convert_to_normal_block failed", K(ret), K(log_dir), K(block_id));
    } else if (0 > (pret = snprintf(backup_path, sizeof(backup_path), "%s/deleted_block_%ld_%ld",
                                    TEST_NAME.c_str(), id_, block_id))
               || pret >= static_cast<int>(sizeof(backup_path))) {
      ret = OB_BUF_NOT_ENOUGH;
      PALF_LOG(WARN, "construct deleted block backup path failed", K(ret), K(pret),
               K(id_), K(block_id));
    } else if (0 != rename(file_path, backup_path)) {
      ret = convert_sys_errno();
      PALF_LOG(WARN, "move block out of log directory failed", K(ret), K(block_id),
               K(file_path), K(backup_path));
    }
    return ret;
  }
  int restore_block_by_human(const block_id_t block_id)
  {
    int ret = OB_SUCCESS;
    int pret = 0;
    char file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    char backup_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    const char *log_dir = log_engine_->log_storage_.block_mgr_.log_dir_;
    if (OB_FAIL(convert_to_normal_block(log_dir, block_id, file_path, OB_MAX_FILE_NAME_LENGTH))) {
      PALF_LOG(WARN, "convert_to_normal_block failed", K(ret), K(log_dir), K(block_id));
    } else if (0 > (pret = snprintf(backup_path, sizeof(backup_path), "%s/deleted_block_%ld_%ld",
                                    TEST_NAME.c_str(), id_, block_id))
               || pret >= static_cast<int>(sizeof(backup_path))) {
      ret = OB_BUF_NOT_ENOUGH;
      PALF_LOG(WARN, "construct deleted block backup path failed", K(ret), K(pret),
               K(id_), K(block_id));
    } else if (0 != rename(backup_path, file_path)) {
      ret = convert_sys_errno();
      PALF_LOG(WARN, "restore block to log directory failed", K(ret), K(block_id),
               K(file_path), K(backup_path));
    }
    return ret;
  }
  int write_test_page_to_block_(LogStorage &log_storage,
                                const block_id_t block_id,
                                const offset_t write_offset,
                                char *write_buf)
  {
    int ret = OB_SUCCESS;
    char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    ObIOFd io_fd;
    LogIOAdapter io_adapter;
    int64_t write_size = 0;
    if (!is_valid_block_id(block_id) || write_offset < 0
        || 0 != write_offset % LOG_DIO_ALIGN_SIZE
        || write_offset + LOG_DIO_ALIGN_SIZE > log_storage.logical_block_size_
        || OB_ISNULL(write_buf)) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid test page write argument", K(ret), K(block_id),
               K(write_offset), K(log_storage.logical_block_size_), KP(write_buf));
    } else if (OB_FAIL(convert_to_normal_block(log_storage.block_mgr_.log_dir_,
                                               block_id,
                                               block_path,
                                               OB_MAX_FILE_NAME_LENGTH))) {
      PALF_LOG(WARN, "convert_to_normal_block failed", K(ret), K(block_id),
               K(log_storage.block_mgr_.log_dir_));
    } else if (OB_FAIL(io_adapter.init(1002, LOG_IO_DEVICE_WRAPPER.get_local_device(),
                                       &G_RES_MGR, &OB_IO_MANAGER))) {
      PALF_LOG(WARN, "io_adapter init failed", K(ret));
    } else if (OB_FAIL(io_adapter.open(block_path, LOG_WRITE_FLAG, FILE_OPEN_MODE, io_fd))) {
      PALF_LOG(WARN, "open block file failed", K(ret), K(block_path));
    // 直接修改目标 block 文件，避免测试辅助逻辑改变 LogBlockMgr 当前 writable handler
    // 或 server log pool 的 block 分配状态。
    } else if (OB_FAIL(io_adapter.pwrite(io_fd,
                                         write_buf,
                                         LOG_DIO_ALIGN_SIZE,
                                         MAX_INFO_BLOCK_SIZE + write_offset,
                                         write_size))) {
      PALF_LOG(WARN, "write test page failed", K(ret), K(block_id), K(write_offset), K(write_size));
    } else if (write_size != LOG_DIO_ALIGN_SIZE) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "unexpected test page write size", K(ret), K(block_id), K(write_offset), K(write_size));
    }
    if (io_fd.is_valid()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(io_adapter.close(io_fd))) {
        PALF_LOG(WARN, "close block file failed", K(tmp_ret), K(io_fd));
      }
      if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        PALF_LOG(WARN, "close block file overrides return code", K(ret), K(io_fd));
      }
    }
    return ret;
  }
  int read_test_page_from_block_(LogStorage &log_storage,
                                 const block_id_t block_id,
                                 const offset_t read_offset,
                                 char *read_buf)
  {
    int ret = OB_SUCCESS;
    int64_t read_size = 0;
    ReadBufGuard read_buf_guard("TestLogEngine", LOG_DIO_ALIGN_SIZE);
    ReadBuf &raw_read_buf = read_buf_guard.read_buf_;
    LogIOContext io_ctx(MTL_ID(), log_storage.palf_id_, LogIOUser::RESTART);
    if (!is_valid_block_id(block_id) || read_offset < 0
        || 0 != read_offset % LOG_DIO_ALIGN_SIZE
        || read_offset + LOG_DIO_ALIGN_SIZE > log_storage.logical_block_size_
        || OB_ISNULL(read_buf) || !raw_read_buf.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid test page read argument", K(ret), K(block_id),
               K(read_offset), K(log_storage.logical_block_size_), KP(read_buf));
    } else if (OB_FAIL(log_storage.log_reader_.pread(block_id,
                                                     MAX_INFO_BLOCK_SIZE + read_offset,
                                                     LOG_DIO_ALIGN_SIZE,
                                                     raw_read_buf,
                                                     read_size,
                                                     io_ctx))) {
      PALF_LOG(WARN, "read test page failed", K(ret), K(block_id), K(read_offset));
    } else if (LOG_DIO_ALIGN_SIZE != read_size) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "unexpected test page read size", K(ret), K(block_id),
               K(read_offset), K(read_size));
    } else {
      MEMCPY(read_buf, raw_read_buf.buf_, LOG_DIO_ALIGN_SIZE);
    }
    return ret;
  }
  int count_open_fds_for_file_(const char *file_path, int64_t &fd_count)
  {
    int ret = OB_SUCCESS;
    DIR *fd_dir = NULL;
    struct dirent *entry = NULL;
    char *end_ptr = NULL;
    long parsed_fd = -1;
    struct stat target_stat;
    struct stat fd_stat;
    fd_count = 0;
    if (OB_ISNULL(file_path)) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(ret), KP(file_path));
    } else if (0 != ::stat(file_path, &target_stat)) {
      ret = convert_sys_errno();
      PALF_LOG(WARN, "stat target file failed", K(ret), K(file_path), K(errno));
    } else if (OB_ISNULL(fd_dir = ::opendir("/proc/self/fd"))) {
      ret = convert_sys_errno();
      PALF_LOG(WARN, "open process fd directory failed", K(ret), K(errno));
    } else {
      while (OB_SUCC(ret)) {
        errno = 0;
        entry = ::readdir(fd_dir);
        if (OB_ISNULL(entry)) {
          if (0 != errno) {
            ret = convert_sys_errno();
            PALF_LOG(WARN, "read process fd directory failed", K(ret), K(errno));
          }
          break;
        }
        end_ptr = NULL;
        errno = 0;
        parsed_fd = ::strtol(entry->d_name, &end_ptr, 10);
        if (0 == errno && end_ptr != entry->d_name && '\0' == *end_ptr
            && 0 <= parsed_fd && parsed_fd <= INT_MAX
            && 0 == ::fstat(static_cast<int>(parsed_fd), &fd_stat)
            && target_stat.st_dev == fd_stat.st_dev && target_stat.st_ino == fd_stat.st_ino) {
          ++fd_count;
        }
      }
    }
    if (OB_NOT_NULL(fd_dir) && 0 != ::closedir(fd_dir)) {
      const int tmp_ret = convert_sys_errno();
      PALF_LOG(WARN, "close process fd directory failed", K(tmp_ret), K(errno));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
        PALF_LOG(WARN, "close process fd directory overrides return code", K(ret));
      }
    }
    return ret;
  }
  int write_dirty_byte_after_tail_at_distance(const LSN &tail_lsn,
                                              const offset_t dirty_tail_distance,
                                              LSN &dirty_lsn)
  {
    int ret = OB_SUCCESS;
    LogStorage &log_storage = leader_.palf_handle_impl_->log_engine_.log_storage_;
    char *write_buf = NULL;
    dirty_lsn.reset();
    if (!tail_lsn.is_valid() || dirty_tail_distance <= 0 || log_storage.logical_block_size_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid dirty suffix position", K(ret), K(tail_lsn),
               K(dirty_tail_distance), K(log_storage.logical_block_size_));
    } else {
      const LSN nonzero_tail_lsn = tail_lsn + dirty_tail_distance;
      dirty_lsn = nonzero_tail_lsn - 1;
      const block_id_t block_id = lsn_2_block(tail_lsn, log_storage.logical_block_size_);
      const block_id_t dirty_block_id = lsn_2_block(dirty_lsn, log_storage.logical_block_size_);
      const offset_t dirty_offset = lsn_2_offset(dirty_lsn, log_storage.logical_block_size_);
      const offset_t write_offset = lower_align(dirty_offset, LOG_DIO_ALIGN_SIZE);
      const offset_t dirty_page_offset = dirty_offset - write_offset;
      if (dirty_block_id != block_id || write_offset + LOG_DIO_ALIGN_SIZE > log_storage.logical_block_size_) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid dirty suffix position", K(ret), K(tail_lsn), K(dirty_lsn),
                 K(block_id), K(dirty_block_id), K(dirty_offset), K(write_offset),
                 K(log_storage.logical_block_size_));
      } else if (OB_ISNULL(write_buf = reinterpret_cast<char *>(
                     ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "TestLogEngine")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(WARN, "allocate dirty suffix buffer failed", K(ret), K(tail_lsn), K(dirty_lsn));
      } else if (FALSE_IT(MEMSET(write_buf, 0, LOG_DIO_ALIGN_SIZE))) {
      } else if (FALSE_IT(write_buf[dirty_page_offset] = '\x7f')) {
      } else if (OB_FAIL(write_test_page_to_block_(log_storage, block_id, write_offset, write_buf))) {
        PALF_LOG(WARN, "write dirty suffix byte failed", K(ret), K(tail_lsn), K(dirty_lsn),
                 K(block_id), K(write_offset), K(dirty_page_offset));
      } else {
        PALF_LOG(INFO, "write dirty suffix byte success", K(tail_lsn), K(dirty_lsn),
                 K(dirty_tail_distance), K(block_id), K(write_offset), K(dirty_page_offset));
      }
    }
    if (NULL != write_buf) {
      ob_free_align(write_buf);
    }
    return ret;
  }
  int write_dirty_byte_after_tail(const LSN &tail_lsn, LSN &dirty_lsn)
  {
    int ret = OB_SUCCESS;
    LogStorage &log_storage = leader_.palf_handle_impl_->log_engine_.log_storage_;
    if (!tail_lsn.is_valid() || log_storage.logical_block_size_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid dirty suffix position", K(ret), K(tail_lsn),
               K(log_storage.logical_block_size_));
    } else {
      const offset_t tail_offset = lsn_2_offset(tail_lsn, log_storage.logical_block_size_);
      const offset_t dirty_offset = upper_align(tail_offset + 1, LOG_DIO_ALIGN_SIZE);
      const offset_t dirty_tail_distance = dirty_offset + 1 - tail_offset;
      if (OB_FAIL(write_dirty_byte_after_tail_at_distance(tail_lsn, dirty_tail_distance, dirty_lsn))) {
        PALF_LOG(WARN, "write dirty byte after tail failed", K(ret), K(tail_lsn),
                 K(dirty_tail_distance));
      }
    }
    return ret;
  }
  int write_valid_group_header_after_tail(const LSN &tail_lsn,
                                          const offset_t header_distance,
                                          LSN &header_lsn)
  {
    int ret = OB_SUCCESS;
    LogStorage &log_storage = leader_.palf_handle_impl_->log_engine_.log_storage_;
    char *write_buf = NULL;
    char dummy_data = '\0';
    LogWriteBuf log_write_buf;
    LogGroupEntryHeader header;
    int64_t data_checksum = 0;
    int64_t pos = 0;
    header_lsn.reset();
    if (!tail_lsn.is_valid() || header_distance <= 0 || log_storage.logical_block_size_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid group header position", K(ret), K(tail_lsn),
               K(header_distance), K(log_storage.logical_block_size_));
    } else {
      header_lsn = tail_lsn + header_distance;
      const block_id_t block_id = lsn_2_block(tail_lsn, log_storage.logical_block_size_);
      const block_id_t header_block_id = lsn_2_block(header_lsn, log_storage.logical_block_size_);
      const offset_t header_offset = lsn_2_offset(header_lsn, log_storage.logical_block_size_);
      const offset_t write_offset = lower_align(header_offset, LOG_DIO_ALIGN_SIZE);
      const offset_t header_page_offset = header_offset - write_offset;
      if (header_block_id != block_id
          || header_page_offset + LogGroupEntryHeader::HEADER_SER_SIZE > LOG_DIO_ALIGN_SIZE
          || write_offset + LOG_DIO_ALIGN_SIZE > log_storage.logical_block_size_) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid group header position", K(ret), K(tail_lsn), K(header_lsn),
                 K(block_id), K(header_block_id), K(header_offset), K(write_offset),
                 K(header_page_offset), K(log_storage.logical_block_size_));
      } else if (OB_ISNULL(write_buf = reinterpret_cast<char *>(
                     ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "TestLogEngine")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(WARN, "allocate group header buffer failed", K(ret), K(tail_lsn), K(header_lsn));
      } else if (OB_FAIL(log_write_buf.push_back(&dummy_data, sizeof(dummy_data)))) {
        PALF_LOG(WARN, "push dummy data failed", K(ret));
      } else if (OB_FAIL(header.generate(false /* is_raw_write */,
                                         true /* is_padding_log */,
                                         log_write_buf,
                                         0 /* data_len */,
                                         share::SCN::base_scn(),
                                         1,
                                         tail_lsn,
                                         1,
                                         data_checksum))) {
        PALF_LOG(WARN, "generate group entry header failed", K(ret), K(tail_lsn), K(header_lsn));
      } else {
        header.update_header_checksum();
        MEMSET(write_buf, 0, LOG_DIO_ALIGN_SIZE);
        pos = header_page_offset;
        if (OB_FAIL(header.serialize(write_buf, LOG_DIO_ALIGN_SIZE, pos))) {
          PALF_LOG(WARN, "serialize group entry header failed", K(ret), K(header), K(pos), K(header_lsn));
        } else if (OB_FAIL(write_test_page_to_block_(log_storage, block_id, write_offset, write_buf))) {
          PALF_LOG(WARN, "write group entry header failed", K(ret), K(tail_lsn), K(header_lsn),
                   K(block_id), K(write_offset), K(header_page_offset));
        } else {
          PALF_LOG(INFO, "write group entry header after tail success", K(tail_lsn), K(header_lsn),
                   K(header_distance), K(block_id), K(write_offset), K(header_page_offset), K(header));
        }
      }
    }
    if (NULL != write_buf) {
      ob_free_align(write_buf);
    }
    return ret;
  }
  int write_meta_entry_after_tail(LogStorage &log_storage,
                                  const LSN &tail_lsn,
                                  const offset_t entry_distance,
                                  const bool corrupt_entry,
                                  LSN &entry_lsn)
  {
    int ret = OB_SUCCESS;
    char *write_buf = NULL;
    const char meta_data = 'm';
    LogMetaEntryHeader header;
    LogMetaEntry entry;
    int64_t pos = 0;
    entry_lsn.reset();
    if (!tail_lsn.is_valid() || entry_distance < 0 || log_storage.logical_block_size_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid meta entry position", K(ret), K(tail_lsn),
               K(entry_distance), K(log_storage.logical_block_size_));
    } else {
      entry_lsn = tail_lsn + entry_distance;
      const block_id_t block_id = lsn_2_block(tail_lsn, log_storage.logical_block_size_);
      const block_id_t entry_block_id = lsn_2_block(entry_lsn, log_storage.logical_block_size_);
      const offset_t entry_offset = lsn_2_offset(entry_lsn, log_storage.logical_block_size_);
      const offset_t write_offset = lower_align(entry_offset, LOG_DIO_ALIGN_SIZE);
      const offset_t page_offset = entry_offset - write_offset;
      if (entry_block_id != block_id
          || page_offset + LogMetaEntryHeader::HEADER_SER_SIZE + sizeof(meta_data)
                 > LOG_DIO_ALIGN_SIZE
          || write_offset + LOG_DIO_ALIGN_SIZE > log_storage.logical_block_size_) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid meta entry position", K(ret), K(tail_lsn), K(entry_lsn),
                 K(block_id), K(entry_block_id), K(entry_offset), K(write_offset),
                 K(page_offset), K(log_storage.logical_block_size_));
      } else if (OB_ISNULL(write_buf = reinterpret_cast<char *>(
                     ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "TestLogEngine")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(WARN, "allocate meta entry buffer failed", K(ret), K(tail_lsn), K(entry_lsn));
      } else if (OB_FAIL(header.generate(&meta_data, sizeof(meta_data)))) {
        PALF_LOG(WARN, "generate meta entry header failed", K(ret), K(entry_lsn));
      } else if (OB_FAIL(entry.generate(header, &meta_data))) {
        PALF_LOG(WARN, "generate meta entry failed", K(ret), K(entry_lsn));
      } else {
        MEMSET(write_buf, 0, LOG_DIO_ALIGN_SIZE);
        pos = page_offset;
        if (OB_FAIL(entry.serialize(write_buf, LOG_DIO_ALIGN_SIZE, pos))) {
          PALF_LOG(WARN, "serialize meta entry failed", K(ret), K(entry_lsn), K(pos));
        } else if (corrupt_entry && FALSE_IT(write_buf[page_offset] ^= 1)) {
        } else if (OB_FAIL(write_test_page_to_block_(log_storage, block_id, write_offset, write_buf))) {
          PALF_LOG(WARN, "write meta entry after tail failed", K(ret), K(tail_lsn), K(entry_lsn));
        }
      }
    }
    if (NULL != write_buf) {
      ob_free_align(write_buf);
    }
    return ret;
  }
  int write_valid_log_entry_at_tail(LogStorage &log_storage, const LSN &tail_lsn)
  {
    int ret = OB_SUCCESS;
    char *write_buf = NULL;
    const char log_data = 'l';
    LogEntryHeader header;
    int64_t pos = 0;
    if (!tail_lsn.is_valid() || log_storage.logical_block_size_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid log entry position", K(ret), K(tail_lsn),
               K(log_storage.logical_block_size_));
    } else {
      const block_id_t block_id = lsn_2_block(tail_lsn, log_storage.logical_block_size_);
      const offset_t entry_offset = lsn_2_offset(tail_lsn, log_storage.logical_block_size_);
      const offset_t write_offset = lower_align(entry_offset, LOG_DIO_ALIGN_SIZE);
      const offset_t page_offset = entry_offset - write_offset;
      if (page_offset + LogEntryHeader::HEADER_SER_SIZE + sizeof(log_data) > LOG_DIO_ALIGN_SIZE) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "log entry crosses test page", K(ret), K(tail_lsn),
                 K(entry_offset), K(write_offset), K(page_offset));
      } else if (OB_ISNULL(write_buf = reinterpret_cast<char *>(
                     ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "TestLogEngine")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(WARN, "allocate log entry buffer failed", K(ret), K(tail_lsn));
      } else if (OB_FAIL(read_test_page_from_block_(log_storage, block_id, write_offset, write_buf))) {
        PALF_LOG(WARN, "read page before writing log entry failed", K(ret), K(tail_lsn));
      } else if (OB_FAIL(header.generate_header(&log_data, sizeof(log_data), share::SCN::base_scn()))) {
        PALF_LOG(WARN, "generate log entry header failed", K(ret), K(tail_lsn));
      } else {
        pos = page_offset;
        if (OB_FAIL(header.serialize(write_buf, LOG_DIO_ALIGN_SIZE, pos))) {
          PALF_LOG(WARN, "serialize log entry header failed", K(ret), K(tail_lsn), K(pos));
        } else if (FALSE_IT(write_buf[pos] = log_data)) {
        } else if (OB_FAIL(write_test_page_to_block_(log_storage, block_id, write_offset, write_buf))) {
          PALF_LOG(WARN, "write log entry at tail failed", K(ret), K(tail_lsn));
        }
      }
    }
    if (NULL != write_buf) {
      ob_free_align(write_buf);
    }
    return ret;
  }
  int read_byte_at_lsn(LogStorage &log_storage, const LSN &lsn, char &byte)
  {
    int ret = OB_SUCCESS;
    int64_t out_read_size = 0;
    ReadBufGuard read_buf_guard("TestLogEngine", LOG_DIO_ALIGN_SIZE);
    ReadBuf &read_buf = read_buf_guard.read_buf_;
    LogIOContext io_ctx(MTL_ID(), log_storage.palf_id_, LogIOUser::RESTART);
    byte = '\0';
    if (!lsn.is_valid() || !read_buf.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), K(read_buf));
    } else if (OB_FAIL(log_storage.log_reader_.pread(lsn_2_block(lsn, log_storage.logical_block_size_),
                                                     MAX_INFO_BLOCK_SIZE
                                                     + lsn_2_offset(lsn, log_storage.logical_block_size_),
                                                     sizeof(byte),
                                                     read_buf,
                                                     out_read_size,
                                                     io_ctx))) {
      PALF_LOG(WARN, "read byte at lsn failed", K(ret), K(lsn));
    } else if (out_read_size != sizeof(byte)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "unexpected read size", K(ret), K(lsn), K(out_read_size));
    } else {
      byte = read_buf.buf_[0];
    }
    return ret;
  }
  int read_byte_at_lsn_from_dir(const char *log_dir,
                                const int64_t palf_id,
                                const int64_t logical_block_size,
                                const LSN &lsn,
                                char &byte)
  {
    int ret = OB_SUCCESS;
    int64_t out_read_size = 0;
    LogIOAdapter io_adapter;
    LogReader log_reader;
    ReadBufGuard read_buf_guard("TestLogEngine", LOG_DIO_ALIGN_SIZE);
    ReadBuf &read_buf = read_buf_guard.read_buf_;
    LogIOContext io_ctx(MTL_ID(), palf_id, LogIOUser::RESTART);
    byte = '\0';
    if (OB_ISNULL(log_dir) || '\0' == log_dir[0] || palf_id < 0 || logical_block_size <= 0
        || !lsn.is_valid() || !read_buf.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(ret), KP(log_dir), K(palf_id),
               K(logical_block_size), K(lsn), K(read_buf));
    } else if (OB_FAIL(io_adapter.init(1002, LOG_IO_DEVICE_WRAPPER.get_local_device(),
                                       &G_RES_MGR, &OB_IO_MANAGER))) {
      PALF_LOG(WARN, "io_adapter init failed", K(ret));
    } else if (OB_FAIL(log_reader.init(log_dir, logical_block_size + MAX_INFO_BLOCK_SIZE, &io_adapter))) {
      PALF_LOG(WARN, "log_reader init failed", K(ret), K(log_dir), K(logical_block_size));
    } else if (OB_FAIL(log_reader.pread(lsn_2_block(lsn, logical_block_size),
                                        MAX_INFO_BLOCK_SIZE + lsn_2_offset(lsn, logical_block_size),
                                        sizeof(byte),
                                        read_buf,
                                        out_read_size,
                                        io_ctx))) {
      PALF_LOG(WARN, "read byte at lsn failed", K(ret), K(lsn), K(log_dir));
    } else if (out_read_size != sizeof(byte)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "unexpected read size", K(ret), K(lsn), K(out_read_size), K(log_dir));
    } else {
      byte = read_buf.buf_[0];
    }
    log_reader.destroy();
    return ret;
  }
  int write_several_blocks(const block_id_t base_block_id, const int block_count)
  {
    int64_t long_buf_len = 16383 * 128;
    LogWriteBuf write_buf;
    char *long_buf = reinterpret_cast<char *>(ob_malloc(long_buf_len, "test_log_engine"));
    LogGroupEntryHeader header;
    int64_t log_checksum;
    const block_id_t donot_delete_block_before_this = 3;
    write_buf.reset();
    memset(long_buf, 0, long_buf_len);
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(long_buf, long_buf_len));
    // EXPECT_EQ(32, write_buf.write_buf_.count());
    EXPECT_EQ(OB_SUCCESS,
              header.generate(false,
                              true,
                              write_buf,
                              long_buf_len - sizeof(LogGroupEntryHeader),
                              share::SCN::base_scn(),
                              1,
                              LSN(donot_delete_block_before_this * PALF_BLOCK_SIZE),
                              1,
                              log_checksum));
    header.update_header_checksum();
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, header.serialize(long_buf, long_buf_len, pos));
    int ret = OB_SUCCESS;
    LogStorage &log_storage = leader_.palf_handle_impl_->log_engine_.log_storage_;
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID, max_block_id = LOG_INVALID_BLOCK_ID;
    if (block_count == 0) {
      ret = OB_INVALID_ARGUMENT;
      return ret;
    }
    bool need_submit_log = true;
    if (OB_FAIL(log_storage.get_block_id_range(min_block_id, max_block_id)) && OB_ENTRY_NOT_EXIST != ret) {
      PALF_LOG(ERROR, "get_block_id_range failed", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      min_block_id = base_block_id;
      max_block_id = base_block_id;
      ret = OB_SUCCESS;
    }
    block_id_t end_block_id = max_block_id + block_count;
    PALF_LOG(INFO, "runlin trace before", K(end_block_id), K(max_block_id));
    do {
      if (max_block_id < end_block_id) {
        need_submit_log = true;
        ret = OB_SUCCESS;
      } else {
        need_submit_log = false;
      }
      share::SCN tmp_scn;
      tmp_scn.convert_for_logservice(max_block_id);
      if (true == need_submit_log && OB_FAIL(log_storage.writev(log_storage.log_tail_, write_buf, tmp_scn))) {
        PALF_LOG(ERROR, "submit_log failed", K(ret));
      } else {
      }
      if (OB_FAIL(log_storage.get_block_id_range(min_block_id, max_block_id))) {
        PALF_LOG(ERROR, "get_block_id_range failed", K(ret));
      }
    } while (OB_SUCC(ret) && true == need_submit_log);
    PALF_LOG(INFO, "runlin trace after", K(end_block_id), K(max_block_id));
    return ret;
  }
  void destroy() {}
  int64_t id_;
  int64_t palf_epoch_;
  int64_t leader_idx_;
  int64_t last_reloaded_writable_size_;
  bool last_reloaded_need_block_header_;
  LogEngine *log_engine_;
  PalfHandleImplGuard leader_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = false;
int64_t log_entry_size = 2 * 1024 * 1024 + 16 * 1024;

// 验证flashback过程中宕机重启
TEST_F(TestObSimpleLogClusterLogEngine, flashback_restart)
{
  SET_CASE_LOG_FILE(TEST_NAME, "flashback_restart");
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin flashback_restart");
  PalfHandleImplGuard leader;
  int64_t id_1 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_1 = 0;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_1, leader_idx_1, leader));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx_1, palf_env));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 66, leader_idx_1, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  SCN scn;
  LogStorage *log_storage = &leader.get_palf_handle_impl()->log_engine_.log_storage_;
  LSN log_tail = log_storage->log_tail_;
  scn = leader.get_palf_handle_impl()->get_end_scn();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 33, leader_idx_1, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  int64_t mode_version;
  AccessMode mode;
  EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->get_access_mode(mode_version, mode));
  LSN flashback_lsn(PALF_BLOCK_SIZE*lsn_2_block(log_tail, PALF_BLOCK_SIZE));
  EXPECT_EQ(OB_SUCCESS, log_storage->begin_flashback(flashback_lsn));
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleImplGuard leader1;
    EXPECT_EQ(OB_SUCCESS, get_leader(id_1, leader1, leader_idx_1));
    LogStorage *log_storage = &leader1.get_palf_handle_impl()->log_engine_.log_storage_;
    EXPECT_LE(2, log_storage->block_mgr_.max_block_id_);
    EXPECT_EQ(OB_SUCCESS, log_storage->block_mgr_.create_tmp_block_handler(2));
    EXPECT_EQ(OB_SUCCESS, log_storage->update_manifest_(3));
    EXPECT_EQ(OB_SUCCESS, log_storage->block_mgr_.delete_block_from_back_to_front_until(2));
    {
      LogBlockMgr *block_mgr = &log_storage->block_mgr_;
      int block_id = 2;
      int ret = OB_SUCCESS;
      // 1. rename "block_id.tmp" to "block_id.flashback"
      // 2. delete "block_id", make sure each block has returned into BlockPool
      // 3. rename "block_id.flashback" to "block_id"
      // NB: for restart, the block which named 'block_id.flashback' must be renamed to 'block_id'
      char tmp_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      char flashback_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      if (block_id != block_mgr->curr_writable_block_id_) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "block_id is not same as curr_writable_handler_, unexpected error",
            K(ret), K(block_id), KPC(block_mgr));
      } else if (OB_FAIL(block_id_to_string(block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
	PALF_LOG(ERROR, "block_id_to_string failed", K(ret), K(block_id));
      } else if (OB_FAIL(block_id_to_tmp_string(block_id, tmp_block_path, OB_MAX_FILE_NAME_LENGTH))) {
	PALF_LOG(ERROR, "block_id_to_tmp_string failed", K(ret), K(block_id));
      } else if (OB_FAIL(block_id_to_flashback_string(block_id, flashback_block_path, OB_MAX_FILE_NAME_LENGTH))) {
	PALF_LOG(ERROR, "block_id_to_flashback_string failed", K(ret), K(block_id));
      } else if (OB_FAIL(block_mgr->do_rename_and_fsync_(tmp_block_path, flashback_block_path))) {
        PALF_LOG(ERROR, "do_rename_and_fsync_ failed", K(ret), KPC(block_mgr));
      } else {
        PALF_LOG(INFO, "rename_tmp_block_handler_to_normal success", K(ret), KPC(block_mgr));
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
}

TEST_F(TestObSimpleLogClusterLogEngine, exception_path)
{
  SET_CASE_LOG_FILE(TEST_NAME, "exception_path");
  EXPECT_EQ(OB_SUCCESS, init());
  OB_LOGGER.set_log_level("TRACE");
  // TODO: to be reopened by runlin.
  ObTenantMutilAllocator *allocator =
      dynamic_cast<ObTenantMutilAllocator *>(log_engine_->alloc_mgr_);
  OB_ASSERT(NULL != allocator);
  allocator->set_limit(32);
  FlushLogCbCtx flush_ctx;
  LogWriteBuf write_buf;
  const char *buf = "hello";
  EXPECT_FALSE(flush_ctx.is_valid());
  EXPECT_FALSE(write_buf.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_engine_->submit_flush_log_task(flush_ctx, write_buf));
  flush_ctx.lsn_ = LSN(1);
  flush_ctx.scn_ = share::SCN::base_scn();
  EXPECT_EQ(OB_INVALID_ARGUMENT, write_buf.push_back(NULL, strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf, strlen(buf)));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, log_engine_->submit_flush_log_task(flush_ctx, write_buf));
  write_buf.reset();
  const int64_t long_buf_len = MAX_LOG_BODY_SIZE;
  char *long_buf = reinterpret_cast<char *>(ob_malloc(long_buf_len, "test_log_engine"));
  LogGroupEntryHeader header;
  int64_t log_checksum;
  const block_id_t donot_delete_block_before_this = 3;
  write_buf.reset();
  memset(long_buf, 0, long_buf_len);

  // Test LogStorage
  LogStorage *log_storage = &log_engine_->log_storage_;
  LogStorage *meta_storage = &log_engine_->log_meta_storage_;
  block_id_t min_block_id, max_block_id;
  share::SCN tmp_scn;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            log_engine_->append_log(LSN(LOG_INVALID_LSN_VAL), write_buf, tmp_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_storage->writev(LSN(LOG_INVALID_LSN_VAL), write_buf, tmp_scn));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_engine_->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(LSN(0), log_engine_->get_begin_lsn());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(LSN(0), log_storage->get_begin_lsn());
  EXPECT_EQ(OB_SUCCESS, log_storage->truncate_prefix_blocks(LSN(0)));
  EXPECT_EQ(true, log_storage->need_append_block_header_);
  EXPECT_EQ(true, log_storage->need_switch_block_());
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_storage->truncate(LSN(100000000)));
  // no block id 1
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_storage->delete_block(1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, meta_storage->append_meta(buf, 10000000));

  int64_t log_id = 1;
  share::SCN scn = share::SCN::base_scn();
  LSN truncate_lsn;
  allocator->set_limit(1*1024*1024*1024);

  EXPECT_EQ(OB_SUCCESS, write_several_blocks(0, 11));
  PALF_LOG(INFO, "after write_several_blocks 11");

  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(0, min_block_id);
  EXPECT_EQ(11, max_block_id);

  // 测试truncate场景
  block_id_t truncate_block_id = max_block_id - 2;
  EXPECT_EQ(OB_SUCCESS, log_storage->truncate(LSN(truncate_block_id * PALF_BLOCK_SIZE)));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  // 此时最后一个block是空的
  EXPECT_EQ(log_storage->log_tail_, LSN(truncate_block_id * PALF_BLOCK_SIZE));
  EXPECT_EQ(PALF_BLOCK_SIZE, log_storage->curr_block_writable_size_);
  EXPECT_TRUE(log_storage->need_append_block_header_);
  EXPECT_EQ(truncate_block_id, max_block_id);
  EXPECT_EQ(lsn_2_block(log_engine_->log_meta_storage_.log_block_header_.min_lsn_, PALF_BLOCK_SIZE), truncate_block_id + 1);

  LogSnapshotMeta snapshot_meta;
  LogInfo prev_log_info;
  prev_log_info.generate_by_default();
  EXPECT_EQ(OB_SUCCESS, snapshot_meta.generate(LSN(1 * PALF_BLOCK_SIZE), prev_log_info, LSN(0)));
  EXPECT_EQ(OB_SUCCESS, log_engine_->log_meta_.update_log_snapshot_meta(snapshot_meta));
  EXPECT_EQ(OB_SUCCESS, log_engine_->append_log_meta_(log_engine_->log_meta_));
  EXPECT_EQ(OB_SUCCESS, log_storage->delete_block(0));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(1, min_block_id);
  EXPECT_EQ(LSN(max_block_id * PALF_BLOCK_SIZE), log_storage->log_tail_);

  log_storage = log_engine_->get_log_storage();
  LogBlockHeader block_header;
  share::SCN scn_0;
  share::SCN scn_11;
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_storage->get_block_min_scn(0, scn_0));
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_storage->read_block_header_(0, block_header));
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND,
            log_storage->get_block_min_scn(truncate_block_id, scn_11));
  LSN log_tail = log_engine_->log_storage_.log_tail_;
  share::SCN ts_origin = scn_11;
  PALF_LOG(INFO, "after second write_several_blocks 1", K(truncate_block_id), K(max_block_id));
  // 由于truncate之后，最后一个文件是空的，因此max_block_id = truncate_block_id
  EXPECT_EQ(OB_SUCCESS, write_several_blocks(0, 1));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_min_scn(truncate_block_id, scn_11));
  EXPECT_NE(scn_11, ts_origin);

  // 测试重启场景
  EXPECT_EQ(OB_SUCCESS, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  PALF_LOG(INFO, "after reload1");

  //测试truncate_prefix 场景
  block_id_t truncate_prefix_block_id = 4;
  prev_log_info.lsn_ = LSN(truncate_prefix_block_id*PALF_BLOCK_SIZE)-100;
  prev_log_info.log_id_ = 0;
  prev_log_info.log_proposal_id_ = 0;
  prev_log_info.scn_ = share::SCN::min_scn();
  prev_log_info.accum_checksum_ = 0;
  EXPECT_EQ(OB_SUCCESS, snapshot_meta.generate(LSN(truncate_block_id*PALF_BLOCK_SIZE), prev_log_info, LSN(truncate_prefix_block_id*PALF_BLOCK_SIZE)));
  EXPECT_EQ(OB_SUCCESS, log_engine_->log_meta_.update_log_snapshot_meta(snapshot_meta));
  EXPECT_EQ(OB_SUCCESS, log_engine_->append_log_meta_(log_engine_->log_meta_));
  EXPECT_EQ(OB_SUCCESS,
            log_storage->truncate_prefix_blocks(LSN(truncate_prefix_block_id * PALF_BLOCK_SIZE)));
  // 测试truncate_prefix后,继续写一个block
  write_several_blocks(0, 1);
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(truncate_prefix_block_id, min_block_id);
  EXPECT_EQ(truncate_block_id+2, max_block_id);

  // 测试目录清空场景，此时log_tail应该为truncate_prefix_block_id
  // 目录清空之后，会重置log_tail
  truncate_prefix_block_id = max_block_id + 2;
  LSN new_base_lsn(truncate_prefix_block_id*PALF_BLOCK_SIZE);
  prev_log_info.lsn_ = new_base_lsn - 100;
  prev_log_info.log_id_ = 0;
  prev_log_info.log_proposal_id_ = 0;
  prev_log_info.scn_ =SCN::min_scn();
  prev_log_info.accum_checksum_ = 0;
  EXPECT_EQ(OB_SUCCESS, snapshot_meta.generate(new_base_lsn, prev_log_info, new_base_lsn));
  EXPECT_EQ(OB_SUCCESS, log_engine_->log_meta_.update_log_snapshot_meta(snapshot_meta));
  EXPECT_EQ(OB_SUCCESS, log_engine_->append_log_meta_(log_engine_->log_meta_));
  const LSN old_log_tail = log_engine_->log_storage_.log_tail_;
  EXPECT_EQ(OB_SUCCESS, log_engine_->truncate_prefix_blocks(new_base_lsn));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_storage->get_block_id_range(min_block_id, max_block_id));
  // truncate_prefix_block_id 和 prev_lsn对应的block_id一样
  EXPECT_EQ(log_storage->log_tail_, LSN(truncate_prefix_block_id * PALF_BLOCK_SIZE));
  // truncate_prefxi_blocks后，min_block_info被置为无效
  EXPECT_EQ(false, is_valid_block_id(log_engine_->min_block_id_));
  EXPECT_EQ(false, log_engine_->min_block_min_scn_.is_valid());
  EXPECT_EQ(false, log_engine_->min_block_max_scn_.is_valid());

  // 测试目录清空后，读数据是否正常报错
  ReadBufGuard buf_guard("dummy", 100);
  int64_t out_read_size;
  LogIOContext io_ctx(LogIOUser::DEFAULT);
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND,
            log_storage->pread(LSN((truncate_prefix_block_id + 1) * PALF_BLOCK_SIZE),
                               100,
                               buf_guard.read_buf_,
                               out_read_size,
                               io_ctx));
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND,
            log_storage->pread(LSN((truncate_prefix_block_id - 1) * PALF_BLOCK_SIZE),
                               100,
                               buf_guard.read_buf_,
                               out_read_size,
                               io_ctx));
  // 测试目录清空后，重启是否正常
  EXPECT_EQ(OB_SUCCESS, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  {
    block_id_t tmp_block_id = LOG_INVALID_BLOCK_ID;
    SCN tmp_scn;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_engine_->get_min_block_info(tmp_block_id, tmp_scn));
  }

  PALF_LOG(INFO, "directory is empty");
  // 测试目录清空后，写数据是否正常
  // 此时log_tail为truncate_prefix_block_id的头部
  const block_id_t expected_min_block_id = lsn_2_block(log_storage->log_tail_, log_storage->logical_block_size_);
  EXPECT_EQ(OB_SUCCESS, write_several_blocks(expected_min_block_id, 3));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(expected_min_block_id, min_block_id);
  EXPECT_EQ(expected_min_block_id+3, max_block_id);
  share::SCN scn_cur;
  EXPECT_EQ(OB_SUCCESS, log_engine_->get_block_min_scn(max_block_id, scn_cur));

  // 测试人为删除文件的重启场景
  EXPECT_EQ(OB_SUCCESS, log_engine_->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(OB_SUCCESS, delete_block_by_human(max_block_id));
  EXPECT_EQ(OB_ERR_UNEXPECTED, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  EXPECT_EQ(OB_SUCCESS, restore_block_by_human(max_block_id));
  EXPECT_EQ(OB_SUCCESS, delete_block_by_human(min_block_id));
  EXPECT_EQ(OB_ERR_UNEXPECTED, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  EXPECT_EQ(OB_SUCCESS, restore_block_by_human(min_block_id));

  if (OB_NOT_NULL(long_buf)) {
    ob_free(long_buf);
  }
  leader_.reset();
  PALF_LOG(INFO, "end exception_path");
}

TEST_F(TestObSimpleLogClusterLogEngine, async_restart_truncate_dirty_suffix)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_restart_truncate_dirty_suffix");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader_, 1, id_, 16 * 1024));
  LSN max_lsn = leader_.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(max_lsn, leader_));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  const LSN expected_tail = log_storage->log_tail_;
  const int64_t logical_block_size = log_storage->logical_block_size_;
  const int64_t palf_id = log_storage->palf_id_;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  int64_t open_fd_count_before_recovery = 0;
  int64_t open_fd_count_after_recovery = 0;
  LSN dirty_lsn;
  char dirty_byte = '\0';
  ASSERT_TRUE(expected_tail.is_valid());
  ASSERT_LT(0, snprintf(log_dir, sizeof(log_dir), "%s", log_storage->block_mgr_.log_dir_));
  ASSERT_EQ(OB_SUCCESS,
            convert_to_normal_block(log_dir,
                                    log_storage->block_mgr_.curr_writable_block_id_,
                                    block_path,
                                    sizeof(block_path)));
  ASSERT_EQ(max_lsn, expected_tail);
  // 模拟异步写宕机镜像：逻辑 tail 连续，但最后一个 block 在 tail 之后仍残留本批次
  // 尚未发布的非零字节，重启应截断该物理后缀。
  ASSERT_EQ(OB_SUCCESS, write_dirty_byte_after_tail(expected_tail, dirty_lsn));
  ASSERT_TRUE(dirty_lsn.is_valid());
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, dirty_lsn, dirty_byte));
  ASSERT_NE('\0', dirty_byte);
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));
  ASSERT_EQ(OB_SUCCESS, count_open_fds_for_file_(block_path, open_fd_count_before_recovery));
  ASSERT_LT(0, open_fd_count_before_recovery);

  ASSERT_EQ(OB_SUCCESS,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  // 临时 recovery engine 已经析构，此时该 block 的 fd 数应回到原始 engine 持有的基线；
  // 如果截断留下的 handler 未关闭，后续 load_last_block_ 再次打开时会多泄漏一个 fd。
  ASSERT_EQ(OB_SUCCESS, count_open_fds_for_file_(block_path, open_fd_count_after_recovery));
  EXPECT_EQ(open_fd_count_before_recovery, open_fd_count_after_recovery);
  leader_.reset();
  ASSERT_EQ(OB_SUCCESS,
            read_byte_at_lsn_from_dir(log_dir, palf_id, logical_block_size, dirty_lsn, dirty_byte));
  EXPECT_EQ('\0', dirty_byte);
}

TEST_F(TestObSimpleLogClusterLogEngine, async_restart_recovers_before_mode_rewrite_and_remains_appendable)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_restart_recovers_before_mode_rewrite_and_remains_appendable");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader_, 1, id_, 16 * 1024));
  LSN max_lsn = leader_.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(max_lsn, leader_));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader_, max_lsn));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  const LSN expected_tail = log_storage->log_tail_;
  LSN dirty_lsn;
  char dirty_byte = '\0';
  PalfOptions options;
  ASSERT_EQ(OB_SUCCESS, leader_.palf_env_impl_->get_options(options));
  ASSERT_FALSE(options.enable_async_io_);
  ASSERT_EQ(max_lsn, expected_tail);
  ASSERT_NE(0, lsn_2_offset(expected_tail, log_storage->logical_block_size_));
  ASSERT_EQ(OB_SUCCESS, write_dirty_byte_after_tail(expected_tail, dirty_lsn));
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));

  // Recovery must first use the persisted ASYNC mode to remove the old dirty
  // suffix, then append the current SYNC mode to LogMeta.
  ASSERT_EQ(OB_SUCCESS, restart_current_palf());
  log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  EXPECT_EQ(LogIOMode::SYNC,
            leader_.palf_handle_impl_->log_engine_.log_meta_
                .get_log_replica_property_meta().get_log_io_mode());
  EXPECT_EQ(expected_tail, log_storage->log_tail_);
  EXPECT_EQ(log_storage->logical_block_size_
                - lsn_2_offset(expected_tail, log_storage->logical_block_size_),
            log_storage->curr_block_writable_size_);
  EXPECT_FALSE(log_storage->need_append_block_header_);
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, dirty_lsn, dirty_byte));
  EXPECT_EQ('\0', dirty_byte);

  LSN appended_lsn;
  share::SCN appended_scn;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader_, appended_lsn, appended_scn));
  EXPECT_EQ(expected_tail + LogGroupEntryHeader::HEADER_SER_SIZE, appended_lsn);
  const LSN appended_tail = leader_.palf_handle_impl_->get_max_lsn();
  ASSERT_GT(appended_tail, appended_lsn);
  ASSERT_EQ(OB_SUCCESS, wait_lsn_until_flushed(appended_tail, leader_));
  ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader_, appended_tail));

  ASSERT_EQ(OB_SUCCESS, restart_current_palf());
  EXPECT_EQ(appended_tail, leader_.palf_handle_impl_->get_max_lsn());
  PalfGroupBufferIterator iterator;
  LogGroupEntry entry;
  LSN read_lsn;
  ASSERT_EQ(OB_SUCCESS,
            leader_.palf_handle_impl_->alloc_palf_group_buffer_iterator(expected_tail, iterator));
  ASSERT_EQ(OB_SUCCESS, iterator.next());
  ASSERT_EQ(OB_SUCCESS, iterator.get_entry(entry, read_lsn));
  EXPECT_EQ(expected_tail, read_lsn);
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, async_restart_accepts_dirty_suffix_at_group_buffer_limit)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_restart_accepts_dirty_suffix_at_group_buffer_limit");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader_, 1, id_, 16 * 1024));
  LSN max_lsn = leader_.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(max_lsn, leader_));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  const LSN expected_tail = log_storage->log_tail_;
  const int64_t logical_block_size = log_storage->logical_block_size_;
  const int64_t palf_id = log_storage->palf_id_;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  LSN dirty_lsn;
  char dirty_byte = '\0';
  ASSERT_LT(0, snprintf(log_dir, sizeof(log_dir), "%s", log_storage->block_mgr_.log_dir_));
  ASSERT_EQ(max_lsn, expected_tail);
  ASSERT_EQ(OB_SUCCESS,
            write_dirty_byte_after_tail_at_distance(expected_tail,
                                                    FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE,
                                                    dirty_lsn));
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, dirty_lsn, dirty_byte));
  ASSERT_NE('\0', dirty_byte);
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));

  ASSERT_EQ(OB_SUCCESS,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  leader_.reset();
  ASSERT_EQ(OB_SUCCESS,
            read_byte_at_lsn_from_dir(log_dir, palf_id, logical_block_size, dirty_lsn, dirty_byte));
  EXPECT_EQ('\0', dirty_byte);
}

TEST_F(TestObSimpleLogClusterLogEngine, async_restart_rejects_dirty_suffix_over_group_buffer)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_restart_rejects_dirty_suffix_over_group_buffer");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader_, 1, id_, 16 * 1024));
  LSN max_lsn = leader_.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(max_lsn, leader_));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  const LSN expected_tail = log_storage->log_tail_;
  LSN dirty_lsn;
  char dirty_byte = '\0';
  ASSERT_TRUE(expected_tail.is_valid());
  ASSERT_EQ(max_lsn, expected_tail);
  ASSERT_EQ(OB_SUCCESS,
            write_dirty_byte_after_tail_at_distance(expected_tail,
                                                    FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE + 1,
                                                    dirty_lsn));
  ASSERT_TRUE(dirty_lsn.is_valid());
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, dirty_lsn, dirty_byte));
  ASSERT_NE('\0', dirty_byte);
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, async_restart_rejects_empty_last_block_after_non_full_block)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_restart_rejects_empty_last_block_after_non_full_block");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader_, 1, id_, 16 * 1024));
  LSN max_lsn = leader_.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(max_lsn, leader_));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  const LSN expected_tail = log_storage->log_tail_;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  ASSERT_TRUE(expected_tail.is_valid());
  ASSERT_EQ(max_lsn, expected_tail);
  ASSERT_EQ(0, lsn_2_block(expected_tail, log_storage->logical_block_size_));
  ASSERT_LT(lsn_2_offset(expected_tail, log_storage->logical_block_size_),
            log_storage->logical_block_size_);
  ASSERT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  ASSERT_EQ(0, min_block_id);
  ASSERT_EQ(0, max_block_id);
  const block_id_t empty_block_id = max_block_id + 1;

  // Async recovery accepts an invalid tail only in the last block. After the
  // empty last block is skipped, invalid data in the previous non-full block
  // must be returned without treating it as a recoverable dirty suffix.
  ASSERT_EQ(OB_SUCCESS, log_storage->block_mgr_.switch_next_block(empty_block_id));
  ASSERT_EQ(OB_SUCCESS, log_storage->update_manifest_(empty_block_id + 1));
  ASSERT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  ASSERT_EQ(empty_block_id, max_block_id);
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));

  EXPECT_EQ(OB_INVALID_DATA,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, async_restart_recovers_dirty_empty_last_block_after_full_block)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_restart_recovers_dirty_empty_last_block_after_full_block");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, write_several_blocks(0, 1));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  ASSERT_LT(min_block_id, max_block_id);
  const block_id_t empty_block_id = max_block_id;
  const LSN expected_tail(empty_block_id * log_storage->logical_block_size_);
  const int64_t logical_block_size = log_storage->logical_block_size_;
  const int64_t palf_id = log_storage->palf_id_;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  LSN dirty_lsn;
  char dirty_byte = '\0';
  ASSERT_LT(0, snprintf(log_dir, sizeof(log_dir), "%s", log_storage->block_mgr_.log_dir_));

  // 正常 switch_next_block() 前一个 block 已写满。truncate 保留新建的最后 block，
  // 但删除其中全部有效 entry；随后写入的字节模拟该 block 内未发布的异步 fragment。
  ASSERT_EQ(OB_SUCCESS, log_storage->truncate(expected_tail));
  ASSERT_EQ(expected_tail, log_storage->log_tail_);
  ASSERT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  ASSERT_EQ(empty_block_id, max_block_id);
  ASSERT_EQ(OB_SUCCESS, write_dirty_byte_after_tail(expected_tail, dirty_lsn));
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, dirty_lsn, dirty_byte));
  ASSERT_NE('\0', dirty_byte);
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));

  ASSERT_EQ(OB_SUCCESS,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  EXPECT_EQ(logical_block_size, last_reloaded_writable_size_);
  EXPECT_TRUE(last_reloaded_need_block_header_);
  leader_.reset();
  ASSERT_EQ(OB_SUCCESS,
            read_byte_at_lsn_from_dir(log_dir, palf_id, logical_block_size, dirty_lsn, dirty_byte));
  EXPECT_EQ('\0', dirty_byte);
}

TEST_F(TestObSimpleLogClusterLogEngine, sync_restart_rejects_mid_log_hole_but_async_recovers)
{
  SET_CASE_LOG_FILE(TEST_NAME, "sync_restart_rejects_mid_log_hole_but_async_recovers");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader_, 1, id_, 16 * 1024));
  LSN max_lsn = leader_.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(max_lsn, leader_));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  const LSN expected_tail = log_storage->log_tail_;
  const int64_t logical_block_size = log_storage->logical_block_size_;
  const int64_t palf_id = log_storage->palf_id_;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  LSN header_lsn;
  char header_byte = '\0';
  ASSERT_TRUE(expected_tail.is_valid());
  ASSERT_LT(0, snprintf(log_dir, sizeof(log_dir), "%s", log_storage->block_mgr_.log_dir_));
  ASSERT_EQ(max_lsn, expected_tail);
  // 在 expected_tail 后留出全零区再写入合法 header，保证解析错误后仍有物理数据。
  // 同步恢复必须报错；异步恢复可把它视为未发布脏尾并截断。
  ASSERT_EQ(OB_SUCCESS,
            write_valid_group_header_after_tail(expected_tail, LOG_DIO_ALIGN_SIZE, header_lsn));
  ASSERT_TRUE(header_lsn.is_valid());
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, header_lsn, header_byte));
  ASSERT_NE('\0', header_byte);
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::SYNC));
  EXPECT_EQ(OB_INVALID_DATA,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));

  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));
  ASSERT_EQ(OB_SUCCESS,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  leader_.reset();
  ASSERT_EQ(OB_SUCCESS,
            read_byte_at_lsn_from_dir(log_dir, palf_id, logical_block_size, header_lsn, header_byte));
  EXPECT_EQ('\0', header_byte);
}

TEST_F(TestObSimpleLogClusterLogEngine, async_mode_does_not_relax_meta_storage_recovery)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_mode_does_not_relax_meta_storage_recovery");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  LogStorage *meta_storage = &log_engine_->log_meta_storage_;
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));
  const LSN expected_meta_tail = meta_storage->log_tail_;
  LSN entry_lsn;
  char entry_byte = '\0';
  ASSERT_EQ(OB_SUCCESS,
            write_meta_entry_after_tail(*meta_storage,
                                        expected_meta_tail,
                                        LOG_DIO_ALIGN_SIZE,
                                        false /* corrupt_entry */,
                                        entry_lsn));
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*meta_storage, entry_lsn, entry_byte));
  ASSERT_NE('\0', entry_byte);

  // The persisted mode only relaxes redo-tail recovery. Meta storage always
  // uses the strict synchronous integrity rules.
  EXPECT_EQ(OB_INVALID_DATA,
            reload(log_engine_->log_storage_.log_tail_,
                   expected_meta_tail,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*meta_storage, entry_lsn, entry_byte));
  EXPECT_NE('\0', entry_byte);
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, log_meta_storage_falls_back_from_torn_latest_entry)
{
  SET_CASE_LOG_FILE(TEST_NAME, "log_meta_storage_falls_back_from_torn_latest_entry");
  OB_LOGGER.set_log_level("TRACE");
  ASSERT_EQ(OB_SUCCESS, init());
  LogStorage *meta_storage = &log_engine_->log_meta_storage_;
  const LSN expected_meta_tail = meta_storage->log_tail_;
  LSN corrupt_entry_lsn;
  char corrupt_byte = '\0';
  ASSERT_EQ(OB_SUCCESS,
            write_meta_entry_after_tail(*meta_storage,
                                        expected_meta_tail,
                                        0,
                                        true /* corrupt_entry */,
                                        corrupt_entry_lsn));
  ASSERT_EQ(expected_meta_tail, corrupt_entry_lsn);
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*meta_storage, corrupt_entry_lsn, corrupt_byte));
  ASSERT_NE('\0', corrupt_byte);

  // A torn latest LogMeta entry has an invalid outer checksum. The shared meta
  // storage recovery must keep the previous valid entry and its logical tail.
  EXPECT_EQ(OB_SUCCESS,
            reload(log_engine_->log_storage_.log_tail_,
                   expected_meta_tail,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, sync_restart_rejects_partial_log_but_async_recovers)
{
  SET_CASE_LOG_FILE(TEST_NAME, "sync_restart_rejects_partial_log_but_async_recovers");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader_, 1, id_, 16 * 1024));
  LSN max_lsn = leader_.palf_handle_impl_->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(max_lsn, leader_));
  LogStorage *log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  const LSN expected_tail = log_storage->log_tail_;
  char partial_log_byte = '\0';
  ASSERT_EQ(max_lsn, expected_tail);
  ASSERT_EQ(OB_SUCCESS, write_valid_log_entry_at_tail(*log_storage, expected_tail));
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, expected_tail, partial_log_byte));
  ASSERT_NE('\0', partial_log_byte);

  // 异步 DIO 写对齐尾页时，可能把最后一个完整 group 后的裸 LogEntryHeader 一并落盘。
  // 相同磁盘内容在同步恢复下必须报错，在异步恢复下应截断这段未发布后缀。
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::SYNC));
  EXPECT_EQ(OB_PARTIAL_LOG,
            reload(expected_tail,
                   log_engine_->log_meta_storage_.log_tail_,
                   log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, expected_tail, partial_log_byte));
  EXPECT_NE('\0', partial_log_byte);

  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));
  ASSERT_EQ(OB_SUCCESS, restart_current_palf());
  log_storage = &leader_.palf_handle_impl_->log_engine_.log_storage_;
  EXPECT_EQ(expected_tail, log_storage->log_tail_);
  ASSERT_EQ(OB_SUCCESS, read_byte_at_lsn(*log_storage, expected_tail, partial_log_byte));
  EXPECT_EQ('\0', partial_log_byte);

  LSN appended_lsn;
  share::SCN appended_scn;
  ASSERT_EQ(OB_SUCCESS, submit_log(leader_, appended_lsn, appended_scn));
  EXPECT_EQ(expected_tail + LogGroupEntryHeader::HEADER_SER_SIZE, appended_lsn);
  const LSN appended_tail = leader_.palf_handle_impl_->get_max_lsn();
  ASSERT_GT(appended_tail, appended_lsn);
  ASSERT_EQ(OB_SUCCESS, wait_lsn_until_flushed(appended_tail, leader_));
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, async_io_option_changes_only_after_reinit)
{
  SET_CASE_LOG_FILE(TEST_NAME, "async_io_option_changes_only_after_reinit");
  OB_LOGGER.set_log_level("TRACE");
  EXPECT_EQ(OB_SUCCESS, init());
  ObSimpleLogServer *server = dynamic_cast<ObSimpleLogServer *>(get_cluster()[leader_idx_]);
  PalfOptions options;
  PalfOptions observed_options;
  ASSERT_NE(static_cast<ObSimpleLogServer *>(NULL), server);
  ASSERT_EQ(OB_SUCCESS, leader_.palf_env_impl_->get_options(options));
  ASSERT_FALSE(options.enable_async_io_);
  options.enable_async_io_ = true;

  // Runtime option refresh cannot change the process-lifetime writer mode.
  // A PalfEnv reinitialization applies the new config and persists it in LogMeta.
  ASSERT_EQ(OB_SUCCESS, leader_.palf_env_impl_->update_options(options));
  ASSERT_EQ(OB_SUCCESS, leader_.palf_env_impl_->get_options(observed_options));
  EXPECT_FALSE(observed_options.enable_async_io_);
  EXPECT_EQ(LogIOMode::SYNC,
            log_engine_->log_meta_.get_log_replica_property_meta().get_log_io_mode());

  server->set_enable_async_io(true);
  ASSERT_EQ(OB_SUCCESS, restart_current_palf());
  ASSERT_EQ(OB_SUCCESS, leader_.palf_env_impl_->get_options(observed_options));
  EXPECT_TRUE(observed_options.enable_async_io_);
  EXPECT_EQ(LogIOMode::ASYNC,
            log_engine_->log_meta_.get_log_replica_property_meta().get_log_io_mode());

  // Restore the shared mittest server to its default for later cases.
  server->set_enable_async_io(false);
  ASSERT_EQ(OB_SUCCESS, restart_current_palf());
  ASSERT_EQ(OB_SUCCESS, leader_.palf_env_impl_->get_options(observed_options));
  EXPECT_FALSE(observed_options.enable_async_io_);
  EXPECT_EQ(LogIOMode::SYNC,
            log_engine_->log_meta_.get_log_replica_property_meta().get_log_io_mode());
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, restart_upgrades_v1_sync_mode_to_v2)
{
  SET_CASE_LOG_FILE(TEST_NAME, "restart_upgrades_v1_sync_mode_to_v2");
  OB_LOGGER.set_log_level("TRACE");
  ASSERT_EQ(OB_SUCCESS, init());
  LogReplicaPropertyMeta property_meta =
      log_engine_->log_meta_.get_log_replica_property_meta();
  property_meta.version_ = LogReplicaPropertyMeta::LOG_REPLICA_PROPERTY_META_VERSION;
  property_meta.io_mode = LogIOMode::SYNC;
  ASSERT_TRUE(property_meta.is_valid());
  ASSERT_EQ(OB_SUCCESS, persist_log_replica_property_meta(property_meta));
  const LSN v1_meta_tail = log_engine_->log_meta_storage_.log_tail_;

  ASSERT_EQ(OB_SUCCESS, restart_current_palf());
  property_meta = log_engine_->log_meta_.get_log_replica_property_meta();
  EXPECT_EQ(LogReplicaPropertyMeta::LOG_REPLICA_PROPERTY_META_VERSION_V2,
            property_meta.version_);
  EXPECT_EQ(LogIOMode::SYNC, property_meta.get_log_io_mode());
  EXPECT_GT(log_engine_->log_meta_storage_.log_tail_, v1_meta_tail);
  leader_.reset();
}

TEST_F(TestObSimpleLogClusterLogEngine, log_io_mode_update_failure_cleans_reload_state)
{
  SET_CASE_LOG_FILE(TEST_NAME, "log_io_mode_update_failure_cleans_reload_state");
  OB_LOGGER.set_log_level("TRACE");
  ASSERT_EQ(OB_SUCCESS, init());
  ASSERT_EQ(OB_SUCCESS, persist_log_io_mode(LogIOMode::ASYNC));
  PalfEnvImpl *env = leader_.palf_env_impl_;
  const LSKey key(id_);
  char palf_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  bool dir_exist = false;
  common::EventItem event_item;
  common::EventItem reset_item;
  event_item.error_code_ = OB_ERR_UNEXPECTED;
  event_item.occur_ = 1;
  event_item.trigger_freq_ = 0;
  ASSERT_NE(static_cast<PalfEnvImpl *>(NULL), env);
  ASSERT_GT(snprintf(palf_dir, sizeof(palf_dir), "%s/%ld", env->log_dir_, id_), 0);

  leader_.reset();
  {
    PalfEnvImpl::WLockGuard guard(env->palf_meta_lock_);
    ASSERT_EQ(OB_SUCCESS, env->palf_handle_impl_map_.del(key));
  }
  const int64_t baseline_count = env->palf_handle_impl_map_.count();
  ASSERT_EQ(OB_SUCCESS,
            common::EventTable::set_event("ERRSIM_PALF_UPDATE_LOG_IO_MODE_FAIL", event_item));
  const int reload_ret = env->reload_palf_handle_impl_(id_);
  EXPECT_EQ(OB_SUCCESS,
            common::EventTable::set_event("ERRSIM_PALF_UPDATE_LOG_IO_MODE_FAIL", reset_item));
  EXPECT_EQ(OB_ERR_UNEXPECTED, reload_ret);
  EXPECT_EQ(baseline_count, env->palf_handle_impl_map_.count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, env->palf_handle_impl_map_.contains_key(key));
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(palf_dir, dir_exist));
  EXPECT_TRUE(dir_exist);

  ASSERT_EQ(OB_SUCCESS, env->reload_palf_handle_impl_(id_));
  EXPECT_EQ(baseline_count + 1, env->palf_handle_impl_map_.count());
  IPalfHandleImpl *handle = NULL;
  ASSERT_EQ(OB_SUCCESS, env->get_palf_handle_impl(id_, handle));
  PalfHandleImpl *impl = dynamic_cast<PalfHandleImpl *>(handle);
  ASSERT_NE(static_cast<PalfHandleImpl *>(NULL), impl);
  EXPECT_EQ(LogIOMode::SYNC,
            impl->log_engine_.log_meta_.get_log_replica_property_meta().get_log_io_mode());
  env->revert_palf_handle_impl(handle);
  ASSERT_EQ(OB_SUCCESS, env->remove_palf_handle_impl(id_));
}

TEST_F(TestObSimpleLogClusterLogEngine, finish_handle_init_failure_cleans_create_and_reload_state)
{
  SET_CASE_LOG_FILE(TEST_NAME, "finish_handle_init_failure_cleans_create_and_reload_state");
  OB_LOGGER.set_log_level("TRACE");
  ObSimpleLogServer *server = dynamic_cast<ObSimpleLogServer *>(get_cluster()[0]);
  ASSERT_NE(static_cast<ObSimpleLogServer *>(NULL), server);
  PalfEnvImpl *env = dynamic_cast<PalfEnvImpl *>(server->get_palf_env());
  ASSERT_NE(static_cast<PalfEnvImpl *>(NULL), env);
  const int64_t baseline_count = env->palf_handle_impl_map_.count();
  PalfBaseInfo base_info;
  base_info.generate_by_default();
  common::EventItem event_item;
  common::EventItem reset_item;
  event_item.error_code_ = OB_ERR_UNEXPECTED;
  event_item.occur_ = 1;
  event_item.trigger_freq_ = 0;

  const int64_t create_palf_id = ATOMIC_AAF(&palf_id_, 1);
  const LSKey create_key(create_palf_id);
  char create_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  bool create_dir_exist = true;
  IPalfHandleImpl *handle = NULL;
  ASSERT_GT(snprintf(create_dir, sizeof(create_dir), "%s/%ld", env->log_dir_, create_palf_id), 0);
  ASSERT_EQ(OB_SUCCESS,
            common::EventTable::set_event("ERRSIM_PALF_FINISH_HANDLE_INIT_FAIL", event_item));
  const int create_ret = env->create_palf_handle_impl(
      create_palf_id, AccessMode::APPEND, base_info, LogReplicaType::NORMAL_REPLICA, handle);
  EXPECT_EQ(OB_SUCCESS,
            common::EventTable::set_event("ERRSIM_PALF_FINISH_HANDLE_INIT_FAIL", reset_item));
  EXPECT_EQ(OB_ERR_UNEXPECTED, create_ret);
  EXPECT_EQ(static_cast<IPalfHandleImpl *>(NULL), handle);
  EXPECT_EQ(baseline_count, env->palf_handle_impl_map_.count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, env->palf_handle_impl_map_.contains_key(create_key));
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(create_dir, create_dir_exist));
  EXPECT_FALSE(create_dir_exist);

  const int64_t reload_palf_id = ATOMIC_AAF(&palf_id_, 1);
  id_ = reload_palf_id;
  const LSKey reload_key(reload_palf_id);
  char reload_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  bool reload_dir_exist = false;
  ASSERT_GT(snprintf(reload_dir, sizeof(reload_dir), "%s/%ld", env->log_dir_, reload_palf_id), 0);
  ASSERT_EQ(OB_SUCCESS,
            env->create_palf_handle_impl(reload_palf_id,
                                         AccessMode::APPEND,
                                         base_info,
                                         LogReplicaType::NORMAL_REPLICA,
                                         handle));
  ASSERT_NE(static_cast<IPalfHandleImpl *>(NULL), handle);
  env->revert_palf_handle_impl(handle);
  handle = NULL;
  ASSERT_EQ(baseline_count + 1, env->palf_handle_impl_map_.count());
  {
    PalfEnvImpl::WLockGuard guard(env->palf_meta_lock_);
    // PalfEnv::destroy() 只销毁 map，不把 PALF 标记为已删除，因此目录会保留，
    // 下一次初始化仍能走 reload 路径。
    ASSERT_EQ(OB_SUCCESS, env->palf_handle_impl_map_.del(reload_key));
  }
  ASSERT_EQ(baseline_count, env->palf_handle_impl_map_.count());
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(reload_dir, reload_dir_exist));
  ASSERT_TRUE(reload_dir_exist);

  ASSERT_EQ(OB_SUCCESS,
            common::EventTable::set_event("ERRSIM_PALF_FINISH_HANDLE_INIT_FAIL", event_item));
  const int reload_ret = env->reload_palf_handle_impl_(reload_palf_id);
  EXPECT_EQ(OB_SUCCESS,
            common::EventTable::set_event("ERRSIM_PALF_FINISH_HANDLE_INIT_FAIL", reset_item));
  EXPECT_EQ(OB_ERR_UNEXPECTED, reload_ret);
  EXPECT_EQ(baseline_count, env->palf_handle_impl_map_.count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, env->palf_handle_impl_map_.contains_key(reload_key));
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(reload_dir, reload_dir_exist));
  EXPECT_TRUE(reload_dir_exist);

  // 再次 reload 成功可同时证明上一次失败清除了 map 中的残留值和调用方引用。
  ASSERT_EQ(OB_SUCCESS, env->reload_palf_handle_impl_(reload_palf_id));
  EXPECT_EQ(baseline_count + 1, env->palf_handle_impl_map_.count());
  ASSERT_EQ(OB_SUCCESS, env->get_palf_handle_impl(reload_palf_id, handle));
  PalfHandleImpl *reloaded_handle = dynamic_cast<PalfHandleImpl *>(handle);
  ASSERT_NE(static_cast<PalfHandleImpl *>(NULL), reloaded_handle);
  EXPECT_EQ(common::RefHandle::BORN_REF + 1, reloaded_handle->get_uref());
  env->revert_palf_handle_impl(handle);
  handle = NULL;
  EXPECT_EQ(common::RefHandle::BORN_REF, reloaded_handle->get_uref());
  ASSERT_EQ(OB_SUCCESS, env->remove_palf_handle_impl(reload_palf_id));
  EXPECT_EQ(baseline_count, env->palf_handle_impl_map_.count());
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(reload_dir, reload_dir_exist));
  EXPECT_FALSE(reload_dir_exist);
}


TEST_F(TestObSimpleLogClusterLogEngine, io_reducer_basic_func)
{
  SET_CASE_LOG_FILE(TEST_NAME, "io_reducer_func");
  update_server_log_disk(4*1024*1024*1024ul);
  update_disk_options(4*1024*1024*1024ul/palf::PALF_PHY_BLOCK_SIZE);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin io_reducer_basic_func");
  PalfHandleImplGuard leader_1;
  int64_t id_1 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_1 = 0;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_1, leader_idx_1, leader_1));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx_1, palf_env));

  LogIOWorkerBase *log_io_worker = leader_1.palf_handle_impl_->log_engine_.io_task_submitter_;

  int64_t prev_log_id_1 = 0;
	LogEngine *log_engine = &leader_1.palf_handle_impl_->log_engine_;
	IOTaskCond io_task_cond_1(id_1, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_1(id_1, log_engine->palf_epoch_);
  // 单日志流场景
  // 卡住log_io_worker的处理
  {
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, id_1, 110));
    const int64_t log_id = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_1.cond_.signal();
    wait_lsn_until_flushed(max_lsn, leader_1);
    EXPECT_EQ(OB_ITER_END, read_log(leader_1));
    // sw内部做了自适应freeze之后这个等式可能不成立, 因为上层可能基于写盘反馈触发提交下一个io_task
    prev_log_id_1 = log_id;
  }
  // 单日志流场景
  // 当聚合度为1的时候，应该走正常的提交流程，目前暂未实现，先通过has_batched_size不计算绕过
  {
    // 聚合度为1的忽略
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_1));
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, id_1, 110));
    const int64_t log_id = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_1.cond_.signal();
    wait_lsn_until_flushed(max_lsn, leader_1);
    EXPECT_EQ(2, io_task_verify_1.count_);
    prev_log_id_1 = log_id;
  }

  // 多日志流场景
  int64_t id_2 = ATOMIC_AAF(&palf_id_, 1);
  int64_t prev_log_id_2 = 0;
  int64_t leader_idx_2 = 0;
  PalfHandleImplGuard leader_2;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_2, leader_idx_2, leader_2));
	IOTaskCond io_task_cond_2(id_2, leader_2.get_palf_handle_impl()->log_engine_.palf_epoch_);
  IOTaskVerify io_task_verify_2(id_2, leader_2.get_palf_handle_impl()->log_engine_.palf_epoch_);
  {
    LogIOWorkerBase *log_io_worker = leader_2.palf_handle_impl_->log_engine_.io_task_submitter_;
    // 聚合度为1的忽略
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_2));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_2));
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_1));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, id_1, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1024, id_2, 110));

    const int64_t log_id_1 = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_1 = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    const int64_t log_id_2 = leader_2.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_2 = leader_2.palf_handle_impl_->sw_.get_max_lsn();
    sleep(1);
    io_task_cond_2.cond_.signal();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    wait_lsn_until_flushed(max_lsn_2, leader_2);
    EXPECT_EQ(3, io_task_verify_1.count_);
    EXPECT_EQ(1, io_task_verify_2.count_);

    // ls1已经有个一个log_id被忽略聚合了
    prev_log_id_2 = log_id_2;
    prev_log_id_1 = log_id_1;
  }

  // 三个日志流，stripe为2
  // 目前不支持可配的LogIOWorkerConfig，此测试暂时不打开，但结果是对的
  // int64_t id_3 = ATOMIC_AAF(&palf_id_, 1);
  // int64_t leader_idx_3 = 0;
  // int64_t prev_log_id_3 = 0;
  // PalfHandleImplGuard leader_3;
  // IOTaskCond io_task_cond_3;
  // IOTaskVerify io_task_verify_3;
  // io_task_cond_3.init(id_3);
  // io_task_verify_3.init(id_3);
  // EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_3, leader_idx_3, leader_3));
  // {
  //   EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_3));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_3, 1, id_3, 110));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
  //   sleep(1);
  //   io_task_cond_3.cond_.signal();
  //   const int64_t log_id_1 = leader_1.palf_handle_impl_->sw_.get_max_log_id();
  //   LSN max_lsn_1 = leader_1.palf_handle_impl_->sw_.get_max_lsn();
  //   const int64_t log_id_2 = leader_2.palf_handle_impl_->sw_.get_max_log_id();
  //   LSN max_lsn_2 = leader_2.palf_handle_impl_->sw_.get_max_lsn();
  //   const int64_t log_id_3 = leader_3.palf_handle_impl_->sw_.get_max_log_id();
  //   LSN max_lsn_3 = leader_3.palf_handle_impl_->sw_.get_max_lsn();
  //   wait_lsn_until_flushed(max_lsn_1, leader_1);
  //   wait_lsn_until_flushed(max_lsn_2, leader_2);
  //   wait_lsn_until_flushed(max_lsn_3, leader_3);
  // }
  // 验证切文件场景
  int64_t id_3 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_3 = 0;
  int64_t prev_log_id_3 = 0;
  PalfHandleImplGuard leader_3;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_3, leader_idx_3, leader_3));
	IOTaskCond io_task_cond_3(id_3, leader_3.get_palf_handle_impl()->log_engine_.palf_epoch_);
  IOTaskVerify io_task_verify_3(id_3, leader_3.get_palf_handle_impl()->log_engine_.palf_epoch_);
  {
    LogIOWorkerBase *log_io_worker = leader_3.palf_handle_impl_->log_engine_.io_task_submitter_;
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_3));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_3, 1, id_3, 110));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
    sleep(1);
    io_task_cond_3.cond_.signal();
    const int64_t log_id_1 = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_1 = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    const int64_t log_id_2 = leader_2.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_2 = leader_2.palf_handle_impl_->sw_.get_max_lsn();
    const int64_t log_id_3 = leader_3.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_3 = leader_3.palf_handle_impl_->sw_.get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    wait_lsn_until_flushed(max_lsn_2, leader_2);
    wait_lsn_until_flushed(max_lsn_3, leader_3);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 31, leader_idx_1, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 2, leader_idx_1, 900 *1024));
    max_lsn_1 = leader_1.palf_handle_impl_->get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);

    PALF_LOG(INFO, "current log_tail", K(leader_1.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(0, leader_1.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.min_block_id_);

    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, leader_idx_1, 300));
    max_lsn_1 = leader_1.palf_handle_impl_->get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    EXPECT_EQ(2, leader_1.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.max_block_id_);

    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, leader_idx_1, 300));
    max_lsn_1 = leader_1.palf_handle_impl_->get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    EXPECT_EQ(OB_ITER_END, read_log(leader_1));
  }

  // 测试epoch change
  PALF_LOG(INFO, "begin test epoch change");
  int64_t id_4 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_4 = 0;
  int64_t prev_log_id_4 = 0;
  PalfHandleImplGuard leader_4;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_4, leader_idx_4, leader_4));
	IOTaskCond io_task_cond_4(id_4, leader_4.get_palf_handle_impl()->log_engine_.palf_epoch_);
	IOTaskVerify io_task_verify_4(id_4, leader_4.get_palf_handle_impl()->log_engine_.palf_epoch_);
  {
    LogIOWorkerBase *log_io_worker = leader_4.palf_handle_impl_->log_engine_.io_task_submitter_;
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_4));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_4, 10, id_4, 110));
    sleep(1);
    LSN max_lsn = leader_4.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_4.cond_.signal();
    PALF_LOG(INFO, "after signal");
    // signal之后需要sleep一会等前面的日志都提交给io_worker,
    // 否则在反馈模式下, 这批日志可能会延迟submit, 排在下一个cond task后面
    sleep(1);
    wait_lsn_until_flushed(max_lsn, leader_4);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_4));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_4, 10, id_4, 110));
    sleep(1);
    leader_4.palf_handle_impl_->log_engine_.palf_epoch_++;
    io_task_cond_4.cond_.signal();
    LSN log_tail = leader_4.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
    PALF_LOG(INFO, "after signal", K(max_lsn), K(log_tail));
    sleep(1);
    log_tail = leader_4.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
    PALF_LOG(INFO, "after flused case 4", K(max_lsn), K(log_tail));
    EXPECT_EQ(max_lsn, log_tail);
  }

  // 测试truncate
  PALF_LOG(INFO, "begin test truncate");
  int64_t id_5 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_5 = 0;
  int64_t prev_log_id_5 = 0;
  PalfHandleImplGuard leader_5;
  IOTaskCond io_task_cond_5(id_5, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_5(id_5, log_engine->palf_epoch_);
  TruncateLogCbCtx ctx(LSN(0));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_5, leader_idx_5, leader_5));
  {
    LogIOWorkerBase *log_io_worker = leader_5.palf_handle_impl_->log_engine_.io_task_submitter_;
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_5));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_5, 10, id_5, 110));
    LSN max_lsn = leader_5.palf_handle_impl_->sw_.get_max_lsn();
    sleep(2);
    // 在提交truncate log task之前需先等待之前的日志提交写盘
    io_task_cond_5.cond_.signal();
    wait_lsn_until_flushed(max_lsn, leader_5);
    EXPECT_EQ(OB_SUCCESS, leader_5.palf_handle_impl_->log_engine_.submit_truncate_log_task(ctx));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_5));
    sleep(1);
    io_task_cond_5.cond_.signal();
    // wait_lsn_until_flushed(max_lsn, leader_5);
    EXPECT_EQ(0, leader_5.palf_handle_impl_->log_engine_.log_storage_.log_tail_);
  }

  PALF_LOG(INFO, "begin test sw full case");
  // 测试滑动窗口满场景
  // 聚合的两条日志分别在头尾部
  int64_t id_6 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_6 = 0;
  int64_t prev_log_id_6 = 0;
  PalfHandleImplGuard leader_6;
  IOTaskCond io_task_cond_6(id_6, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_6(id_6, log_engine->palf_epoch_);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_6, leader_idx_6, leader_6));
  {
     LogIOWorkerBase *log_io_worker = leader_6.palf_handle_impl_->log_engine_.io_task_submitter_;
    {
      EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 15, id_6, log_entry_size));
      sleep(2);
      LSN max_lsn = leader_6.palf_handle_impl_->sw_.get_max_lsn();
      wait_lsn_until_flushed(max_lsn, leader_6);
      EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_6));
      EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, 10*1024));
      sleep(1);
      LSN max_lsn1 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
      int64_t remain_size = LEADER_DEFAULT_GROUP_BUFFER_SIZE - max_lsn1.val_ - LogGroupEntryHeader::HEADER_SER_SIZE - LogEntryHeader::HEADER_SER_SIZE;
      EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, remain_size));
      sleep(1);
      LSN max_lsn2 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
      PALF_LOG_RET(ERROR, OB_SUCCESS, "runlin trace", K(max_lsn2), K(max_lsn1), K(remain_size), K(max_lsn));
      EXPECT_EQ(max_lsn2, LSN(LEADER_DEFAULT_GROUP_BUFFER_SIZE));
      io_task_cond_6.cond_.signal();
      wait_lsn_until_flushed(max_lsn2, leader_6);
    }
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 3, id_6, log_entry_size));
    sleep(2);
    LSN max_lsn = leader_6.palf_handle_impl_->sw_.get_max_lsn();
    wait_lsn_until_flushed(max_lsn, leader_6);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_6));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, 10*1024));
    sleep(1);
    LSN max_lsn1 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
    int64_t remain_size = FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE - max_lsn1.val_ - LogGroupEntryHeader::HEADER_SER_SIZE - LogEntryHeader::HEADER_SER_SIZE;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, remain_size));
    sleep(1);
    LSN max_lsn2 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
    PALF_LOG_RET(ERROR, OB_SUCCESS, "runlin trace", K(max_lsn2), K(max_lsn1), K(remain_size), K(max_lsn));
    EXPECT_EQ(max_lsn2, LSN(FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, 100));
    sleep(1);
    LSN max_lsn3 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_6.cond_.signal();
    //EXPECT_EQ(max_lsn, leader_6.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.log_tail_);
    wait_lsn_until_flushed(max_lsn3, leader_6);
    LSN log_tail = leader_6.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
    EXPECT_EQ(max_lsn3, log_tail);
  }

}


TEST_F(TestObSimpleLogClusterLogEngine, limit_reduce_task)
{
  SET_CASE_LOG_FILE(TEST_NAME, "limit_reduce_task");
  // 验证限制单个reduce task size为1M
  int64_t id_7 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_7 = 0;
  int64_t prev_log_id_7 = 0;
  PalfHandleImplGuard leader_7;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_7, leader_idx_7, leader_7));
	LogEngine *log_engine = &leader_7.palf_handle_impl_->log_engine_;
  IOTaskCond io_task_cond_7(id_7, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_7(id_7, log_engine->palf_epoch_);
  {
    BatchLogIOFlushLogTask::SINGLE_TASK_MAX_SIZE = 1*1024*1024;
    ASSERT_FALSE(leader_7.palf_env_impl_->log_io_worker_wrapper_.enable_async_io_);
    LogIOWorker *log_io_worker = static_cast<LogIOWorker *>(
        leader_7.palf_handle_impl_->log_engine_.io_task_submitter_);
    log_io_worker->batch_io_task_mgr_.handle_count_ = 0;
    // case1: 测试单条日志超过SINGLE_TASK_MAX_SIZE
    // 阻塞提交
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_7));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_7, 15, id_7, log_entry_size));
    io_task_cond_7.cond_.signal();
    {
      LSN max_lsn = leader_7.palf_handle_impl_->sw_.get_max_lsn();
      wait_lsn_until_flushed(max_lsn, leader_7);
    }
    // 单条日志超过SINGLE_TASK_MAX_SIZE，会reduce一次, 第二条日志不会被reduce
    EXPECT_EQ(8, log_io_worker->batch_io_task_mgr_.handle_count_);

    PALF_LOG(INFO, "case 2");

    // case2：测试日志大小混合场景
    // 阻塞提交
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_7));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_7, 15, id_7, 1024));
    io_task_cond_7.cond_.signal();
    {
      LSN max_lsn = leader_7.palf_handle_impl_->sw_.get_max_lsn();
      wait_lsn_until_flushed(max_lsn, leader_7);
    }
    EXPECT_LE(8, log_io_worker->batch_io_task_mgr_.handle_count_);
    int64_t prev_handle_count = log_io_worker->batch_io_task_mgr_.handle_count_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_7, 15, id_7, 1024));
    {
      LSN max_lsn = leader_7.palf_handle_impl_->sw_.get_max_lsn();
      wait_lsn_until_flushed(max_lsn, leader_7);
    }
    EXPECT_LE(prev_handle_count, log_io_worker->batch_io_task_mgr_.handle_count_);
    prev_handle_count = log_io_worker->batch_io_task_mgr_.handle_count_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_7, 1, id_7, log_entry_size));
    {
      LSN max_lsn = leader_7.palf_handle_impl_->sw_.get_max_lsn();
      wait_lsn_until_flushed(max_lsn, leader_7);
    }
    EXPECT_EQ(prev_handle_count+1, log_io_worker->batch_io_task_mgr_.handle_count_);
    prev_handle_count = log_io_worker->batch_io_task_mgr_.handle_count_;
    PALF_LOG(INFO, "after first LT");

    // case3：测试小日志场景
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_7));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_7, 30, id_7, 1024));
    io_task_cond_7.cond_.signal();
    {
      LSN max_lsn = leader_7.palf_handle_impl_->sw_.get_max_lsn();
      wait_lsn_until_flushed(max_lsn, leader_7);
    }
    EXPECT_LE(prev_handle_count, log_io_worker->batch_io_task_mgr_.handle_count_);
    PALF_LOG(INFO, "after second LT");
  }

  PALF_LOG(INFO, "end io_reducer_basic_func");
}

//TEST_F(TestObSimpleLogClusterLogEngine, io_reducer_performance)
//{
//  SET_CASE_LOG_FILE(TEST_NAME, "io_reducer_performance");
//
//  OB_LOGGER.set_log_level("ERROR");
//  int64_t id = ATOMIC_AAF(&palf_id_, 1);
//  int64_t leader_idx = 0;
//  PalfHandleImplGuard leader;
//  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
//  leader.palf_env_impl_->log_io_worker_.batch_io_task_mgr_.handle_count_ = 0;
//  int64_t start_ts = ObTimeUtility::current_time();
//  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 40 * 10000, leader_idx, 100));
//  const LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
//  wait_lsn_until_flushed(max_lsn, leader);
//  const int64_t handle_count = leader.palf_env_impl_->log_io_worker_.batch_io_task_mgr_.handle_count_;
//  const int64_t log_id = leader.palf_handle_impl_->sw_.get_max_log_id();
//  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
//  PALF_LOG(ERROR, "runlin trace performance", K(cost_ts), K(log_id), K(max_lsn), K(has_batched_size), K(handle_count));
//}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv) { RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME); }
