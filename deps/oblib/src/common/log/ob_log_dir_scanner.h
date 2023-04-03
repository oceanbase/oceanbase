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

#ifndef OCEANBASE_SHARE_LOG_OB_LOG_DIR_SCANNER_
#define OCEANBASE_SHARE_LOG_OB_LOG_DIR_SCANNER_

#include "lib/container/ob_vector.h"

namespace oceanbase
{
namespace common
{
struct ObSimpleLogFile
{
  char name[OB_MAX_FILE_NAME_LENGTH];
  uint64_t id;

  enum FileType
  {
    UNKNOWN = 1,
    LOG = 2,
    CKPT = 3
  };

  int assign(const char *filename, FileType &type);
  bool is_log_id(const char *str) const ;
  uint64_t str_to_uint64(const char *str) const;
  bool operator< (const ObSimpleLogFile &r) const;
};

class ObLogDirScanner
{
public:
  ObLogDirScanner();
  virtual ~ObLogDirScanner();

  /**
   * ObLogScanner will scan folder and sort the file by file id.
   * Return the min and max id, and checkpoint id
   *
   * !!! An exception is the discontinuous log files. In this case, init return OB_DISCONTINUOUS_LOG
   *
   * @param [in] log_dir directory path
   * @return OB_SUCCESS success
   * @return OB_DISCONTINUOUS_LOG scan meet discontinuous log files
   * @return otherwise fail
   */
  int init(const char *log_dir);

  /// @brief get minimum commit log id
  /// @retval OB_SUCCESS
  /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
  int get_min_log_id(uint64_t &log_id) const;

  /// @brief get maximum commit log id
  /// @retval OB_SUCCESS
  /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
  int get_max_log_id(uint64_t &log_id) const;

  /// @brief get maximum checkpoint id
  /// @retval OB_SUCCESS
  /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
  int get_max_ckpt_id(uint64_t &ckpt_id) const;

  bool has_log() const;
  bool has_ckpt() const;
  void reset();

private:
  /**
   * Find all log files under directory
   */
  int search_log_dir_(const char *log_dir);

  /**
   * Check if logs are continuous
   * Return min and max file id
   */
  int check_continuity_(const ObVector<ObSimpleLogFile> &files, uint64_t &min_file_id,
                        uint64_t &max_file_id);

  inline int check_inner_stat() const
  {
    int ret = OB_SUCCESS;
    if (!is_init_) {
      SHARE_LOG(ERROR, "ObLogDirScanner has not been initialized.");
      ret = OB_NOT_INIT;
    }
    return ret;
  }

  DISALLOW_COPY_AND_ASSIGN(ObLogDirScanner);

private:
  uint64_t min_log_id_;
  uint64_t max_log_id_;
  uint64_t max_ckpt_id_;
  bool has_log_;
  bool has_ckpt_;
  bool is_init_;
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_LOG_OB_LOG_DIR_SCANNER_
