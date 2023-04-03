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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_BATCH_HEADER_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_BATCH_HEADER_H_

#include <inttypes.h>
#include "lib/ob_define.h"
#include "lib/utility/serialization.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace storage
{
struct ObStorageLogBatchHeader
{
  int16_t magic_;
  int16_t version_;
  int16_t header_len_;
  int16_t cnt_;
  int32_t rez_;
  int32_t total_len_;
  uint64_t checksum_;

  static const int16_t MAGIC_NUMBER = static_cast<int16_t>(0xAABBL);
  static const int16_t HEADER_VERSION = 1;

  ObStorageLogBatchHeader();
  ~ObStorageLogBatchHeader();

  TO_STRING_KV(K_(magic),
               K_(cnt),
               K_(total_len),
               K_(checksum))

  // calculate data's checksum
  uint64_t cal_checksum(const char *log_data, const int32_t data_len);
  // check data integrity
  int check_data(const char *data);
  // check batch header integrity
  int check_batch_header();

  NEED_SERIALIZE_AND_DESERIALIZE;
};
}
}

#endif