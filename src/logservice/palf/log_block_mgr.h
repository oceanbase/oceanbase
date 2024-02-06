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

#ifndef OCEANBASE_LOGSERVICE_LOG_FILE_MGR_
#define OCEANBASE_LOGSERVICE_LOG_FILE_MGR_

#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "log_define.h"
#include "log_reader.h"                   // LogReader
#include "log_block_handler.h"            // LogBlockHandler
#include "lsn.h"                          // LSN
#include "log_block_pool_interface.h"     // ILogBlocKMgr

namespace oceanbase
{
namespace palf
{

class LogWriteBuf;
class LogGroupEntryHeader;

class LogBlockMgr {
public:
  LogBlockMgr();
  ~LogBlockMgr();

  // @brief this function used to initialize.
  int init(const char *log_dir, const block_id_t block_id,
           const int64_t align_size,
           const int64_t align_buf_size,
           int64_t log_block_size, ILogBlockPool *log_block_pool);
  void reset(const block_id_t init_block_id);

  void destroy();

  int switch_next_block(const block_id_t next_block_id);

  // @brief this function used to write data at specified offset.
  // Swithing block when failed to write or reach block size.
  int pwrite(const block_id_t block_id,
             const offset_t offset,
             const char *buf,
             const int64_t buf_len);

  // @brief this function used to write data at specified offset.
  // Swithing block when failed to write or reach block size.
  int writev(const block_id_t block_id,
             const offset_t offset,
             const LogWriteBuf &write_buf);

  int truncate(const block_id_t block_id,
               const offset_t offset);
  // @brief used to get min block id and max block id
  // @retval
  //   OB_SUCCESS
  //   OB_ENTRY_NOT_EXIST, min_block_id and max_block_id will reset to init_block_id
  int get_block_id_range(block_id_t &min_block_id,
                         block_id_t &max_block_id) const;
  int delete_block(block_id_t  block_id);

  int load_block_handler(const block_id_t block_id,
                         const offset_t offset);

	int create_tmp_block_handler(const block_id_t block_id);

  // ======================= flashback =====================
  int delete_block_from_back_to_front_until(const block_id_t block_id);
  int rename_tmp_block_handler_to_normal(const block_id_t block_id);
  // =======================================================
  TO_STRING_KV(K_(log_dir), K_(dir_fd), K_(min_block_id), K_(max_block_id), K_(curr_writable_block_id));
private:
  // @brief this function used to rebuild 'blocks_'
  // Firstly, scan the directory, get the name of all blocks;
  // Secondly, construct virtual blocks array;
  // Thirdly, according to virtual blocks array, read the header of each
  // physical block, rebuild 'blocks_';
  // Finally, reopen the last virtual block.
  // int reload_(const block_id_t block_id, const offset_t offset);
  int do_delete_block_(const block_id_t block_id);
  int delete_block_from_back_to_front_until_(const block_id_t block_id);
  int do_truncate_(const block_id_t block_id, const offset_t offset);
  int do_scan_dir_(const char *dir, const block_id_t initial_block_id, ILogBlockPool *log_block_pool);
  int do_rename_and_fsync_(const char *block_path, const char *tmp_block_path);
  bool empty_() const;
  int try_recovery_last_block_(const char *log_dir);

  int check_after_truncate_(const char *block_path, const offset_t offset);
  const int64_t SLEEP_TS_US = 1 * 1000;

private:
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];
  LogBlockHandler curr_writable_handler_;
  block_id_t curr_writable_block_id_;
  offset_t log_block_size_;
  // 'min_block_id_' caches the minimum block id on disk, and the same as 'max_block_id_',
  // update 'min_block_id_' and 'max_block_id_' to 0 at first switch block after reboot or
  // after restart when there is no block.
  //
  // inc 'min_block_id_' when delete block, if 'min_block_id_' is invalid(means there is no
  // block), return OB_NO_SUCH_FILE_OR_DIRECTORY.
  //
  // inc 'max_block_id_' when switch block, if 'max_block_id_' is invalid(means there is no
  // block), set 'max_block_id_' and 'min_block_id_' to 0.
  //
  // 'max_block_id_' need reset correctly for truncate.
  //
  // 'delete_block' and 'switch_next_block' will not operate on same block.
  // 'block_id_cache_lock_' just only avoid concurrent access between 'delete_block'
  // ('truncate', 'switch_next_block') and 'get_block_id_range'.
  // NB: 'truncate' and 'switch_next_block' will not concurrent.
  mutable ObSpinLock block_id_cache_lock_;
  mutable block_id_t min_block_id_;
  mutable block_id_t max_block_id_;
  ILogBlockPool *log_block_pool_;
  int dir_fd_;
  int64_t align_size_;
  int64_t align_buf_size_;
  bool is_inited_;
};
} // end of logservice
} // end of oceanbase

#endif
