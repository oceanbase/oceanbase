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

#ifndef OCEANBASE_ARCHIVE_FAKE_FILE_STORE_
#define OCEANBASE_ARCHIVE_FAKE_FILE_STORE_

#include "archive/ob_archive_log_file_store.h"
#include "archive/ob_log_archive_struct.h"
#include "clog/ob_log_reader_interface.h"

namespace oceanbase
{
namespace archive
{
class ObArchiveFakeFileStore : public ObIArchiveLogFileStore
{
static const int64_t MAX_PATH_LENGTH = 1000;
public:
  virtual int init_for_dump(const char *path);
  virtual int read_data_direct(const ObArchiveReadParam &param,
                               clog::ObReadBuf &rbuf,
                               clog::ObReadRes &res);
private:
  bool inited_;
  char path_[MAX_PATH_LENGTH];
};
}
}
#endif /* OCEANBASE_ARCHIVE_FAKE_FILE_STORE_ */
