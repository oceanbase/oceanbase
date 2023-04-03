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

#ifndef MOCK_OB_SS_STORE_H_
#define MOCK_OB_SS_STORE_H_

#include <gmock/gmock.h>
#include "storage/ob_ss_store.h"

namespace oceanbase
{
namespace storage
{

class MockObSSStore : public ObSSStore
{
public:
  MockObSSStore() {}
  virtual ~MockObSSStore() {}

  MOCK_METHOD4(
      get,
      int(
        const storage::ObTableAccessParam &param,
        storage::ObTableAccessContext &context,
        const common::ObStoreRowkey &rowkey,
        ObStoreRowIterator *&row_iter));
  MOCK_METHOD4(
      scan,
      int(
        const ObTableAccessParam &param,
        ObTableAccessContext &context,
        const common::ObStoreRange &key_range,
        ObStoreRowIterator *&row_iter));
  MOCK_METHOD4(
      multi_get,
      int(
        const ObTableAccessParam &param,
        ObTableAccessContext &context,
        const common::ObIArray<common::ObStoreRowkey> &rowkeys,
        ObStoreRowIterator *&row_iter));
  MOCK_METHOD4(
      multi_scan,
      int(
        const ObTableAccessParam &param,
        ObTableAccessContext &context,
        const common::ObIArray<common::ObStoreRange> &ranges,
        ObStoreRowIterator *&row_iter));

  MOCK_METHOD5(
      estimate_get_cost,
      int (const common::ObQueryFlag query_flag,
        const uint64_t table_id,
        const common::ObIArray<common::ObStoreRowkey> &rowkeys,
        const common::ObIArray<share::schema::ObColDesc> &columns,
        ObPartitionEst &cost_metrics));

  MOCK_METHOD5(
      estimate_scan_cost,
      int (const common::ObQueryFlag query_flag,
        const uint64_t table_id,
        const common::ObStoreRange &key_range,
        const common::ObIArray<share::schema::ObColDesc> &columns,
        ObPartitionEst &cost_metrics));

   MOCK_METHOD5(
       estimate_multi_scan_cost,
       int (
         const common::ObQueryFlag query_flag,
         const uint64_t table_id,
         const common::ObIArray<common::ObStoreRange> &ranges,
         const common::ObIArray<share::schema::ObColDesc> &columns,
         ObPartitionEst &cost_metrics));


  //virtual enum storage::ObStoreType get_store_type() const { return MAJOR_SSTORE; }

  MOCK_CONST_METHOD0(
      get_store_type,
      enum storage::ObStoreType());

};

}
}



#endif /* MOCK_OB_SS_STORE_H_ */
