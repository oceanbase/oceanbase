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