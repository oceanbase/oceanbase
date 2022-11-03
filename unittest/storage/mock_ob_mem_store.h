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

#ifndef MOCK_OB_MEM_STORE_H_
#define MOCK_OB_MEM_STORE_H_

#include <gmock/gmock.h>

namespace oceanbase
{
namespace storage
{

class MockObMemStore : public ObIStore
{
public:
  MOCK_METHOD0(destroy, void());
  MOCK_METHOD6(get,
               int(const ObStoreCtx &ctx,
                   const ObQueryFlag query_flag,
                   const uint64_t table_id,
                   const common::ObStoreRowkey &rowkey,
                   const common::ObIArray<share::schema::ObColDesc> &columns,
                   const ObStoreRow *&row));
  MOCK_METHOD6(scan,
               int(const ObStoreCtx &ctx,
                   const ObQueryFlag query_flag,
                   const uint64_t table_id,
                   const common::ObStoreRange &key_range,
                   const common::ObIArray<share::schema::ObColDesc> &columns,
                   ObStoreRowIterator *&row_iter));
  MOCK_METHOD6(multi_get,
               int(const ObStoreCtx &ctx,
                   const ObQueryFlag query_flag,
                   const uint64_t table_id,
                   const common::ObIArray<common::ObStoreRowkey> &rowkeys,
                   const common::ObIArray<share::schema::ObColDesc> &columns,
                   ObStoreRowIterator *&row_iter));
  MOCK_METHOD5(set,
               int(const ObStoreCtx &ctx,
                   const uint64_t table_id,
                   const int64_t rowkey_size,
                   const common::ObIArray<share::schema::ObColDesc> &columns,
                   ObStoreRowIterator &row_iter));
  MOCK_METHOD5(set,
               int(const ObStoreCtx &ctx,
                   const uint64_t table_id,
                   const int64_t rowkey_size,
                   const common::ObIArray<share::schema::ObColDesc> &columns,
                   const ObStoreRow &row));
  MOCK_METHOD4(lock,
               int(const ObStoreCtx &ctx,
                   const uint64_t table_id,
                   const common::ObIArray<share::schema::ObColDesc> &columns,
                   common::ObNewRowIterator &row_iter));
  MOCK_METHOD4(lock,
               int(const ObStoreCtx &ctx,
                   const uint64_t table_id,
                   const common::ObIArray<share::schema::ObColDesc> &columns,
                   const common::ObNewRow &row));
  MOCK_METHOD1(revert_iter,
               int(ObStoreRowIterator *iter));
  MOCK_METHOD1(revert_row,
               int(const ObStoreRow *row));
  MOCK_METHOD3(replay,
               int(const ObStoreCtx &ctx, const char *data, const int64_t data_len));
  MOCK_METHOD5(estimate_get_cost,
               int(const ObQueryFlag query_flag,
                       const uint64_t table_id,
                       const common::ObIArray<common::ObStoreRowkey> &rowkeys,
                       const common::ObIArray<share::schema::ObColDesc> &columns,
                       ObPartitionEst &cost_metrics));
  MOCK_METHOD5(estimate_scan_cost,
              int(const ObQueryFlag query_flag,
                     const uint64_t table_id,
                     const common::ObStoreRange &key_range,
                     const common::ObIArray<share::schema::ObColDesc> &columns,
                     ObPartitionEst &cost_metrics));

  enum ObStoreType get_store_type() const
  {
    return ACTIVE_MEMSTORE;
  }
};

}  // namespace storage
}  // namespace oceanbase


#endif
