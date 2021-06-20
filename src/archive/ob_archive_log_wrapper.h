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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_LOG_WRAPPER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_LOG_WRAPPER_H_

#include "common/ob_partition_key.h"
#include "clog/ob_log_define.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObSavedStorageInfoV2;
}  // namespace storage
namespace common {
class ObBaseStorageInfo;
}
namespace clog {
class ObILogEngine;
}
namespace archive {
using namespace oceanbase::storage;
using namespace oceanbase::clog;

class ObArchiveLogWrapper {
public:
  static const int64_t MAX_LOCATE_RETRY_TIMES = 3;

public:
  ObArchiveLogWrapper()
      : inited_(false), partition_service_(NULL), log_engine_(NULL), next_ilog_file_id_(common::OB_INVALID_FILE_ID)
  {}

public:
  int init(ObPartitionService* partition_service, ObILogEngine* log_engine);
  int query_max_ilog_file_id(file_id_t& max_ilog_id);
  int locate_ilog_by_log_id(const common::ObPGKey& pg_key, const uint64_t start_log_id, uint64_t& end_log_id,
      bool& ilog_file_exist, file_id_t& ilog_id);
  int get_pg_max_log_id(const common::ObPGKey& pg_key, uint64_t& ret_max_log_id);
  int check_is_from_restore(const common::ObPGKey& pg_key, bool& is_from_restore);
  int get_all_saved_info(const common::ObPGKey& pg_key, storage::ObSavedStorageInfoV2& info);
  int get_pg_log_archive_status(const common::ObPGKey& pg_key, ObPGLogArchiveStatus& status);
  int get_pg_first_log_submit_ts(const common::ObPGKey& pg_key, int64_t& submit_ts);

private:
  void refresh_next_ilog_file_id_();

private:
  bool inited_;
  ObPartitionService* partition_service_;
  ObILogEngine* log_engine_;
  file_id_t next_ilog_file_id_;
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_LOG_WRAPPER_H_ */
