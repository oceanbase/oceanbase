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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_SCAN_CREATE_DOMAIN_INDEX_H_
#define OCEANBASE_SQL_ENGINE_TABLE_SCAN_CREATE_DOMAIN_INDEX_H_

#include "ob_table_scan.h"

namespace oceanbase {
namespace sql {

class ObTableScanCreateDomainIndex : public ObTableScan {
public:
  explicit ObTableScanCreateDomainIndex(common::ObIAllocator& allocator);
  virtual ~ObTableScanCreateDomainIndex();
  void set_create_index_table_id(const uint64_t create_index_id);
  uint64_t get_create_index_table_id() const;

protected:
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const override;

private:
  int get_domain_index_col_pos(ObExecContext& ctx, const common::ObIArray<int32_t>& col_ids,
      const int64_t schema_version, int64_t& domain_index_pos) const;

protected:
  using ObTableScan::ObTableScanCtx;

private:
  static const int64_t DEFAULT_WORD_COUNT = 8;
  mutable ObSEArray<ObString, DEFAULT_WORD_COUNT> words_;
  mutable int64_t word_index_;
  mutable int64_t domain_index_col_pos_;
  mutable ObObjType orig_obj_type_;
  int64_t create_index_id_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_TABLE_SCAN_CREATE_DOMAIN_INDEX_H_
