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

#include "ob_log_checksum_V2.h"
#include "lib/checksum/ob_crc64.h"
#include "ob_log_define.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObLogChecksum::ObLogChecksum() : is_inited_(false), partition_key_(), accum_checksum_(0), verify_checksum_(0)
{}

int ObLogChecksum::init(uint64_t log_id, const int64_t accum_checksum, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!partition_key.is_valid() || log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_inited_ = true;
    partition_key_ = partition_key;
    accum_checksum_ = accum_checksum;
    verify_checksum_ = accum_checksum;
  }
  return ret;
}

int ObLogChecksum::acquire_accum_checksum(const int64_t data_checksum, int64_t& accum_checksum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    accum_checksum_ = common::ob_crc64(accum_checksum_, const_cast<int64_t*>(&data_checksum), sizeof(data_checksum));
    accum_checksum = accum_checksum_;
  }
  return ret;
}

int ObLogChecksum::verify_accum_checksum(const int64_t data_checksum, const int64_t accum_checksum)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    verify_checksum_ = common::ob_crc64(verify_checksum_, const_cast<int64_t*>(&data_checksum), sizeof(data_checksum));
    if (verify_checksum_ != accum_checksum) {
      CLOG_LOG(ERROR, "accum_checksum not match", K_(partition_key), K_(verify_checksum), K(accum_checksum));
      ret = common::OB_CHECKSUM_ERROR;
    }
  }
  return ret;
}

void ObLogChecksum::set_accum_checksum(const int64_t accum_checksum)
{
  accum_checksum_ = accum_checksum;
  CLOG_LOG(INFO, "set_accum_checksum", K_(partition_key), K(accum_checksum));
}

void ObLogChecksum::set_accum_checksum(const uint64_t log_id, const int64_t accum_checksum)
{
  accum_checksum_ = accum_checksum;
  verify_checksum_ = accum_checksum;
  CLOG_LOG(INFO, "set_accum_checksum", K_(partition_key), K(log_id), K(accum_checksum));
}

void ObLogChecksum::set_verify_checksum(const uint64_t log_id, const int64_t verify_checksum)
{
  verify_checksum_ = verify_checksum;
  CLOG_LOG(INFO, "set_verify_checksum", K_(partition_key), K(log_id), K(verify_checksum));
}

int64_t ObLogChecksum::get_verify_checksum() const
{
  return verify_checksum_;
}

}  // namespace clog
}  // namespace oceanbase
