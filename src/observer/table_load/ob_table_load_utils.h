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

#pragma once

#include "lib/container/ob_iarray.h"
#include "share/table/ob_table_load_array.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace blocksstable
{
class ObStorageDatum;
class ObDatumRange;
class ObDatumRowkey;
class ObDatumRow;
}  // namespace blocksstable
namespace common
{
class ObAddr;
class ObString;
class ObObj;
class ObStoreRowkey;
class ObStoreRange;
class ObNewRow;
}  // namespace common
namespace table
{
  class ObTableApiCredential;
} // namespace table
namespace sql
{
class ObSQLSessionInfo;
} // namespace sql
namespace observer
{
class ObGlobalContext;

class ObTableLoadUtils
{
public:
  template<class T>
  static int deep_copy(const T &src, T &dest, common::ObIAllocator &allocator);
  static int deep_copy(const common::ObString &src, common::ObString &dest, common::ObIAllocator &allocator);
  static int deep_copy(const common::ObObj &src, common::ObObj &dest, common::ObIAllocator &allocator);
  static int deep_copy(const common::ObNewRow &src, common::ObNewRow &dest, common::ObIAllocator &allocator);
  static int deep_copy(const common::ObStoreRowkey &src, common::ObStoreRowkey &dest, common::ObIAllocator &allocator);
  static int deep_copy(const common::ObStoreRange &src, common::ObStoreRange &dest, common::ObIAllocator &allocator);
  static int deep_copy(const blocksstable::ObDatumRow &src, blocksstable::ObDatumRow &dest, common::ObIAllocator &allocator);
  static int deep_copy(const blocksstable::ObStorageDatum &src, blocksstable::ObStorageDatum &dest, common::ObIAllocator &allocator);
  static int deep_copy(const blocksstable::ObDatumRowkey &src, blocksstable::ObDatumRowkey &dest, common::ObIAllocator &allocator);
  static int deep_copy(const blocksstable::ObDatumRange &src, blocksstable::ObDatumRange &dest, common::ObIAllocator &allocator);
  static int deep_copy(const sql::ObSQLSessionInfo &src, sql::ObSQLSessionInfo &dest, common::ObIAllocator &allocator);

  template<class T>
  static int deep_copy(const table::ObTableLoadArray<T> &src, table::ObTableLoadArray<T> &dest, common::ObIAllocator &allocator);

  template<class T>
  static int deep_copy_transform(const table::ObTableLoadArray<T> &src, table::ObTableLoadArray<T> &dest, common::ObIAllocator &allocator, const common::ObIArray<int64_t> &idx_array);

  template<class T>
  static int deep_copy(const common::ObIArray<T> &src, table::ObTableLoadArray<T> &dest, common::ObIAllocator &allocator);
  static bool is_local_addr(const common::ObAddr &addr);
  static int create_session_info(sql::ObSQLSessionInfo *&session_info, sql::ObFreeSessionCtx &free_session_ctx);
  static void free_session_info(sql::ObSQLSessionInfo *session_info, const sql::ObFreeSessionCtx &free_session_ctx);
  static const int64_t CREDENTIAL_BUF_SIZE = 256;
  static int generate_credential(uint64_t tenant_id, uint64_t user_id, uint64_t database_id,
                                 int64_t expire_ts, uint64_t user_token,
                                 common::ObIAllocator &allocator, common::ObString &credential_str);
  static int generate_credential(uint64_t tenant_id, uint64_t user_id, uint64_t database_id,
                                 int64_t expire_ts, uint64_t user_token, char *buf, int64_t size,
                                 common::ObString &credential_str);
  static int check_user_access(const common::ObString &credential_str, const observer::ObGlobalContext &gctx, table::ObTableApiCredential &credential);
};

template<class T>
int ObTableLoadUtils::deep_copy(const T &src, T &dest, common::ObIAllocator &allocator)
{
  dest = src;
  return common::OB_SUCCESS;
}

template<class T>
int ObTableLoadUtils::deep_copy(const table::ObTableLoadArray<T> &src, table::ObTableLoadArray<T> &dest, common::ObIAllocator &allocator)
{
  int ret = common::OB_SUCCESS;
  dest.reset();
  if (!src.empty()) {
    if (OB_FAIL(dest.create(src.count(), allocator))) {
      OB_LOG(WARN, "fail to create", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
      if (OB_FAIL(deep_copy(src[i], dest[i], allocator))) {
        OB_LOG(WARN, "fail to deep copy", KR(ret));
      }
    }
  }
  return ret;
}

template<class T>
int ObTableLoadUtils::deep_copy_transform(const table::ObTableLoadArray<T> &src, table::ObTableLoadArray<T> &dest, common::ObIAllocator &allocator, const common::ObIArray<int64_t> &idx_array)
{
  int ret = common::OB_SUCCESS;
  dest.reset();
  if (!src.empty()) {
    int64_t col_count = idx_array.count();
    int64_t row_count = src.count() / col_count;
    if (OB_FAIL(dest.create(src.count(), allocator))) {
      OB_LOG(WARN, "fail to create", KR(ret));
    } else {
      const T* src_row = &src[0];
      T* dest_row = &dest[0];
      for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && (j < col_count); ++j) {
          if (OB_FAIL(deep_copy(src_row[idx_array.at(j)], dest_row[j], allocator))) {
            OB_LOG(WARN, "fail to deep copy", KR(ret), K(row_count), K(col_count), K(i), K(j));
          }
        }
        src_row += col_count;
        dest_row += col_count;
      }
    }
  }
  return ret;
}

template<class T>
int ObTableLoadUtils::deep_copy(const common::ObIArray<T> &src, table::ObTableLoadArray<T> &dest, common::ObIAllocator &allocator)
{
  int ret = common::OB_SUCCESS;
  dest.reset();
  if (!src.empty()) {
    if (OB_FAIL(dest.create(src.count(), allocator))) {
      OB_LOG(WARN, "fail to create", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
      if (OB_FAIL(deep_copy(src.at(i), dest[i], allocator))) {
        OB_LOG(WARN, "fail to deep copy", KR(ret));
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
