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

#ifndef OCEANBASE_STORAGE_OB_NON_TRANS_LOG_
#define OCEANBASE_STORAGE_OB_NON_TRANS_LOG_

namespace oceanbase {
namespace storage {
class ObNonTransLog {
public:
  ObNonTransLog()
  {}
  virtual ~ObNonTransLog()
  {}
  virtual int replace_tenant_id(const uint64_t new_tenant_id) = 0;
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_NON_TRANS_LOG_
