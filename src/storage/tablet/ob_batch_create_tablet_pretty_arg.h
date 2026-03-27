/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
