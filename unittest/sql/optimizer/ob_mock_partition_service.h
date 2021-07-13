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

#ifndef _OB_MOCK_PARTITION_SERVICE_H_
#define _OB_MOCK_PARTITION_SERVICE_H_
#undef private
#undef protected
#include <gmock/gmock.h>
#include "ob_mock_partition.h"
#define private public
#define protected public
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_dml_param.h"
#include "share/ob_server_locality_cache.h"

using namespace oceanbase;
namespace test {
class MockPartitionService : public oceanbase::storage::ObPartitionService {
public:
  MockPartitionService()
  {}
  virtual ~MockPartitionService()
  {}

  virtual int get_server_locality_array(
      common::ObIArray<share::ObServerLocality>& server_locality_array, bool& has_readonly_zone) const
  {
    int ret = OB_SUCCESS;
    UNUSED(server_locality_array);
    UNUSED(has_readonly_zone);
    return ret;
  }

  int get_scan_cost(
      const oceanbase::storage::ObTableScanParam& param, oceanbase::storage::ObPartitionEst& cost_estimate)
  {
    UNUSED(param);
    cost_estimate.logical_row_count_ = 100.0;
    cost_estimate.physical_row_count_ = 100.0;
    return OB_SUCCESS;
  }

  int get_partition(const common::ObPartitionKey& pkey, oceanbase::storage::ObIPartitionGroup*& partition) const
  {
    UNUSED(pkey);
    partition = const_cast<MockPartition*>(&partition_);
    return OB_SUCCESS;
  }

  int get_partition(const common::ObPartitionKey& pkey, oceanbase::storage::ObIPartitionGroupGuard& guard) const
  {
    UNUSED(pkey);
    guard.set_partition_group(this->get_pg_mgr(), const_cast<MockPartition&>(partition_));
    return OB_SUCCESS;
  }

  int table_scan(common::ObVTableScanParam& param, common::ObNewRowIterator*& result)
  {
    UNUSED(param);
    int ret = OB_SUCCESS;
    ObObj* value = OB_NEW(ObObj, ObModIds::TEST);
    value->set_type(ObIntType);
    value->set_int(1);
    value->set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

    ObNewRow* row = OB_NEW(ObNewRow, ObModIds::TEST);
    row->cells_ = value;
    row->count_ = 1;

    ObSingleRowIteratorWrapper* single = OB_NEW(ObSingleRowIteratorWrapper, ObModIds::TEST);
    single->set_row(row);
    result = single;
    return ret;
  }

  int revert_scan_iter(common::ObNewRowIterator* iter)
  {
    UNUSED(iter);
    return OB_SUCCESS;
  }

  int insert_rows(const transaction::ObTransDesc& trans_desc, const storage::ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    int ret = OB_SUCCESS;
    common::ObNewRow* row = NULL;
    while (OB_SUCCESS == (ret = row_iter->get_next_row(row)))
      ;
    affected_rows = 1;
    return OB_SUCCESS;
  }

  int update_rows(const transaction::ObTransDesc& trans_desc, const storage::ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    UNUSED(updated_column_ids);
    UNUSED(row_iter);
    affected_rows = 1;
    return OB_SUCCESS;
  }

  int delete_rows(const transaction::ObTransDesc& trans_desc, const storage::ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows)
  {
    UNUSED(trans_desc);
    UNUSED(dml_param);
    UNUSED(pkey);
    UNUSED(column_ids);
    int ret = OB_SUCCESS;
    common::ObNewRow* row = NULL;
    while (OB_SUCCESS == (ret = row_iter->get_next_row(row)))
      ;
    affected_rows = 1;
    return OB_SUCCESS;
  }

  obrpc::ObCommonRpcProxy& get_rs_rpc_proxy()
  {
    static obrpc::ObCommonRpcProxy rs_proxy;
    return rs_proxy;
  }

  virtual storage::ObIPartitionGroupIterator* alloc_pg_iter()
  {
    /*
      storage::ObPartitionIterator *part_iter = new storage::ObPartitionIterator();
      part_iter->set_partition_service(*this);
      return part_iter;
      */
    return nullptr;
  }

  MockPartition partition_;
};

}  // end namespace test

#endif /* _OB_MOCK_PARTITION_SERVICE_H_ */
