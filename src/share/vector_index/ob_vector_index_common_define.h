/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 #ifndef OCEANBASE_SHARE_VECTOR_INDEX_COMMON_DEFINE_H_
 #define OCEANBASE_SHARE_VECTOR_INDEX_COMMON_DEFINE_H_

namespace oceanbase
{
namespace share
{

constexpr static int64_t  OB_VSAG_MAX_EF_SEARCH = 160000;
constexpr static int64_t  OB_VECTOR_INDEX_MAX_SEGMENT_CNT = 10;
constexpr static int64_t  OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH = 256;
constexpr static int64_t  OB_VECTOR_INDEX_MERGE_BASE_PERCENTAGE = 20;

enum ObVectorIndexRecordType
{
  VIRT_INC, // increment index
  VIRT_BITMAP,
  VIRT_SNAP, // snapshot index
  VIRT_DATA, // data tablet/table
  VIRT_EMBEDDED, // embedded table
  VIRT_MAX
};

enum ObCanSkip3rdAnd4thVecIndex
{
  NOT_INITED,
  SKIP,
  NOT_SKIP,
  SKIPMAX
};

}  // namespace share
}  // namespace oceanbase

#endif