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

#ifndef OB_MULTI_VERSION_MANAGER_H_
#define OB_MULTI_VERSION_MANAGER_H_

#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_schema.h"
#include "lib/string/ob_string.h"
#include "common/ob_hint.h"
namespace oceanbase {
namespace storage {

struct ObMultiVersionRowInfo {
  ObMultiVersionRowInfo();
  ~ObMultiVersionRowInfo()
  {}
  void reset();
  bool is_valid() const
  {
    return rowkey_column_cnt_ > 0 && multi_version_rowkey_column_cnt_ > 0 && column_cnt_ > 0;
  }
  int64_t rowkey_column_cnt_;
  int64_t multi_version_rowkey_column_cnt_;
  int64_t column_cnt_;
  int64_t trans_version_index_;
};

class ObMultiVersionColDescGenerate {
public:
  typedef common::ObSEArray<int32_t, common::OB_DEFAULT_COL_DEC_NUM> OutColsProject;

public:
  ObMultiVersionColDescGenerate();
  ~ObMultiVersionColDescGenerate();
  int init(const share::schema::ObTableSchema* schema);
  void reset();
  int generate_column_ids(common::ObIArray<share::schema::ObColDesc>& column_ids);
  const common::ObIArray<int32_t>* get_out_cols_project()
  {
    return &out_cols_project_;
  }
  int generate_multi_version_row_info(const ObMultiVersionRowInfo*& multi_version_row_info);

private:
  int generate_out_cols_project(const common::ObIArray<share::schema::ObColDesc>& column_ids);

private:
  bool is_inited_;
  const share::schema::ObTableSchema* schema_;
  ObMultiVersionRowInfo row_info_;
  // In order to be compatible with MINOR_SSSTORE, MINOR_SSSTORE does not have a multi-version column
  // need a mapping relationship when output
  OutColsProject out_cols_project_;
  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionColDescGenerate);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_MULTI_VERISON_MANAGER_H_
