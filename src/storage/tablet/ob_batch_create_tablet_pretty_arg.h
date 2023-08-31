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

#ifndef OCEANBASE_STORAGE_OB_BATCH_CREATE_TABLET_PRETTY_ARG
#define OCEANBASE_STORAGE_OB_BATCH_CREATE_TABLET_PRETTY_ARG

#include <stdint.h>

namespace oceanbase
{
namespace obrpc
{
struct ObBatchCreateTabletArg;
}

namespace storage
{
class ObBatchCreateTabletPrettyArg
{
public:
  ObBatchCreateTabletPrettyArg(const obrpc::ObBatchCreateTabletArg &arg);
  ~ObBatchCreateTabletPrettyArg() = default;
  ObBatchCreateTabletPrettyArg(const ObBatchCreateTabletPrettyArg&) = delete;
  ObBatchCreateTabletPrettyArg &operator=(const ObBatchCreateTabletPrettyArg&) = delete;
public:
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  const obrpc::ObBatchCreateTabletArg &arg_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_BATCH_CREATE_TABLET_PRETTY_ARG
