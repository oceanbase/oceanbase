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

#ifndef OCEANBASE_ARCHIVE_OB_LOG_ARCHIVE_DEFINE_H_
#define OCEANBASE_ARCHIVE_OB_LOG_ARCHIVE_DEFINE_H_
// this file define some enum and const variables
//
#include "lib/ob_define.h"

namespace oceanbase {
namespace archive {
typedef uint32_t file_id_t;
typedef int32_t offset_t;
//================ start of const define =================//
const int64_t OB_MAX_ARCHIVE_PATH_LENGTH = 1536;  // MAX_URI_LENGTH + 512
const int64_t OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH = 512;
const int64_t MAX_PG_MGR_THREAD_NUM = 5;
const int64_t ARCHIVE_HASH_NUM = 47;
const int64_t DEFAULT_ARCHIVE_DATA_FILE_SIZE = 64 * 1024 * 1024L;
const int64_t DEFAULT_ARCHIVE_INDEX_FILE_SIZE = 64 * 1024L;
const int64_t DEFAULT_ARCHIVE_WAIT_TIME_AFTER_EAGAIN = 100;
const int64_t ARCHIVE_IO_MAX_RETRY_TIME = 15 * 60 * 1000 * 1000L;
const int64_t DEFAULT_ARCHIVE_RETRY_INTERVAL = 5 * 1000 * 1000L;
const int64_t ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE = sizeof(int32_t);
const int64_t MAX_SPLIT_TASK_COUNT = 1024 * 100;
const int64_t DEFAULT_CLOG_CURSOR_NUM = 8;

const int64_t MAX_ILOG_FETCHER_CONSUME_DELAY = 10 * 1000 * 1000L;
const int64_t ILOG_FETCHER_CONSUME_CLOG_COUNT_THRESHOLD = 100;
const int64_t ILOG_FETCHER_CONSUME_CLOG_SIZE_THRESHOLD = 1 * 1000 * 1000L;

// buffer size reserved for ObArchiveBlockMeta, which current is 88 bytes
const int64_t RESERVED_SIZE_FOR_ARCHIVE_BLOCK_META = 200;
// buffer size reserved for ObArchiveCompressEncryptedChunkHeader and OB_AES_BLOCK_SIZE, which current is 130 bytes
const int64_t RESERVED_SIZE_FOR_COMPRESSED_ENCRYPTED_HEADER = 200;
// max size of single archive block, include block meta, archive_data_meta, and archive data(logs)
const int64_t MAX_ARCHIVE_BLOCK_SIZE = RESERVED_SIZE_FOR_ARCHIVE_BLOCK_META +
                                       RESERVED_SIZE_FOR_COMPRESSED_ENCRYPTED_HEADER + common::OB_MAX_LOG_BUFFER_SIZE;

static const int64_t SEND_TASK_CAPACITY_LIMIT = 1 * 1024 * 1024 * 1024L;  // 1G
static const int64_t SEND_TASK_PAGE_SIZE = common::OB_MALLOC_MIDDLE_BLOCK_SIZE;
static const int64_t ARCHIVE_THREAD_WAIT_INTERVAL = 100 * 1000L;
static const int64_t MAX_ARCHIVE_THREAD_NAME_LENGTH = 50;

static const int64_t MAX_ARCHIVE_TASK_STATUS_QUEUE_CAPACITY = common::OB_MAX_PARTITION_NUM_PER_SERVER;
static const int64_t MAX_ARCHIVE_TASK_STATUS_POP_TIMEOUT = 100 * 1000L;
enum LogArchiveFileType {
  LOG_ARCHIVE_FILE_TYPE_INVALID = 0,
  LOG_ARCHIVE_FILE_TYPE_INDEX = 1,
  LOG_ARCHIVE_FILE_TYPE_DATA = 2,
  LOG_ARCHIVE_FILE_TYPE_MAX
};

enum ObLogArchiveContentType {
  OB_ARCHIVE_TASK_TYPE_INVALID = 0,
  OB_ARCHIVE_TASK_TYPE_KICKOFF = 1,
  OB_ARCHIVE_TASK_TYPE_CHECKPOINT = 2,
  OB_ARCHIVE_TASK_TYPE_CLOG_SPLIT = 3,
  OB_ARCHIVE_TASK_TYPE_MAX,
};

//================ end of const define =================//

}  // end of namespace archive
}  // end of namespace oceanbase
#endif  // OCEANBASE_ARCHIVE_OB_LOG_ARCHIVE_DEFINE_H_
