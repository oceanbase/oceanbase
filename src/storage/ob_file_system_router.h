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

#ifndef OB_FILE_SYSTEM_ROUTER_H_
#define OB_FILE_SYSTEM_ROUTER_H_

#include "common/ob_zone.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_zone_info.h"
#include "storage/blocksstable/ob_log_file_spec.h"

namespace oceanbase {
namespace storage {

class ObFileSystemRouter final
{
public:
  static ObFileSystemRouter & get_instance();
  int init(const char *data_dir,
           const char *cluster_name,
           const int64_t cluster_id,
           const char *zone,
           const common::ObAddr &svr_addr);

  OB_INLINE const char* get_data_dir() const { return data_dir_; }
  OB_INLINE const char* get_slog_dir() const { return slog_dir_; }
  OB_INLINE const char* get_clog_dir() const { return clog_dir_; }
  int get_tenant_clog_dir(
      const uint64_t tenant_id,
      char (&tenant_clog_dir)[common::MAX_PATH_SIZE]);

  // only work in local file system
  OB_INLINE const char* get_sstable_dir() const { return sstable_dir_; }

  OB_INLINE int64_t get_svr_seq() const { return svr_seq_; }
  OB_INLINE void set_svr_seq(const int64_t svr_seq) { svr_seq_ = svr_seq; }

  OB_INLINE bool is_single_zone_deployment_on() const { return false; }

  OB_INLINE const blocksstable::ObLogFileSpec &get_clog_file_spec() const { return clog_file_spec_; }
  OB_INLINE const blocksstable::ObLogFileSpec &get_slog_file_spec() const { return slog_file_spec_; }

private:
  ObFileSystemRouter();
  virtual ~ObFileSystemRouter() = default;

  void reset();
  int init_shm_file_path();
  int init_local_dirs(const char* data_dir);

private:
  char data_dir_[common::MAX_PATH_SIZE];
  char slog_dir_[common::MAX_PATH_SIZE];
  char clog_dir_[common::MAX_PATH_SIZE];
  char sstable_dir_[common::MAX_PATH_SIZE];

  blocksstable::ObLogFileSpec clog_file_spec_;
  blocksstable::ObLogFileSpec slog_file_spec_;
  int64_t svr_seq_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObFileSystemRouter);
};
#define OB_FILE_SYSTEM_ROUTER (::oceanbase::storage::ObFileSystemRouter::get_instance())
} // namespace storage
} // namespace oceanbase
#endif /* OB_FILE_SYSTEM_ROUTER_H_ */
