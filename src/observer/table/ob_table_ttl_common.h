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

#ifndef _OB_TABLE_TTL_COMMON_H
#define _OB_TABLE_TTL_COMMON_H

#include "common/ob_partition_key.h"

namespace oceanbase
{
namespace observer
{

// value not greater than 0 is invalid, which is ignored in ttl task
struct ObTTLPara final
{
public:
  ObTTLPara() : ttl_(0), 
                max_version_(0) {}
  bool is_valid() const
  {
    return ttl_ > 0 || max_version_ > 0;
  }
  TO_STRING_KV(K_(ttl), K_(max_version));
public:
  int32_t  ttl_;
  int32_t  max_version_;
};

struct ObTTLTaskInfo final
{
public:
  ObTTLTaskInfo() : pkey_(),
                    task_id_(common::OB_INVALID_ID),
                    is_user_trigger_(true),
                    row_key_(),
                    ttl_del_cnt_(0),
                    max_version_del_cnt_(0),
                    scan_cnt_(0),
                    err_code_(OB_SUCCESS) {}
  bool is_valid() const
  {
    return pkey_.is_valid() && common::OB_INVALID_ID != task_id_; 
  }
  TO_STRING_KV(K_(pkey), K_(task_id), K_(is_user_trigger), K_(row_key),
               K_(ttl_del_cnt), K_(max_version_del_cnt),
               K_(scan_cnt), K_(err_code));
public:
  ObPartitionKey   pkey_;
  int64_t          task_id_;
  bool             is_user_trigger_;
  ObString         row_key_; 
  int64_t          ttl_del_cnt_;
  int64_t          max_version_del_cnt_;
  int64_t          scan_cnt_;
  int64_t          err_code_;  
};

} // end namespace observer
} // end namespace oceanbase

#endif /*  _OB_TABLE_TTL_COMMON_H */