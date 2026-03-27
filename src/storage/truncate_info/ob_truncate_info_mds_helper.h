// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_MDS_HELPER_H_
#define OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_MDS_HELPER_H_
#include "/usr/include/stdint.h"
#include "src/share/scn.h"
namespace oceanbase
{
namespace storage
{
namespace mds
{
struct BufferCtx;
}
class ObTruncateInfoMdsHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_MDS_HELPER_H_
