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

#include "lib/container/ob_se_array.h"
#include "share/schema/ob_table_schema.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_i_store.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;
namespace unittest
{
#include "strutils.h"
typedef ObSEArray<share::schema::ObColDesc, 64> ColDescArray;
class ColDescBuilder
{
public:
  const ColDescArray& get_columns() const { return columns_; }
  void add(uint64_t col_id, ObObjType col_type, ObCollationType col_collation) {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = col_id;
    col_desc.col_type_.set_type(col_type);
    col_desc.col_type_.set_collation_type(col_collation);
    columns_.push_back(col_desc);
  }
private:
  ColDescArray columns_;
};

class RowIterBuilder
{
public:
  enum { MAX_ROWKEY_OBJ = 64 };
  RowIterBuilder(const ColDescArray& cols): cols_(cols) {}
  ~RowIterBuilder(){}
  ObMtRowIterator& build(const char* str) {
    ObStoreRow row;
    row.row_val_.cells_ = build_obj_array(str);
    row.row_val_.count_ = cols_.count();
    row.set_dml(T_DML_UPDATE);
    iter_.add_row(row);
    return iter_;
  }
  ObObj* build_obj_array(const char* str) {
    char buf[4096];
    Tokenizer tok(strcpy(buf, str), " ");
    for(int64_t i = 0; i < cols_.count(); i++) {
      parse_obj(obj_array_[i], tok.next());
    }
    return obj_array_;
  }
private:
  static int parse_obj(ObObj& obj, const char* val)
  {
    int err = OB_SUCCESS;
    obj.set_int(atoi(val));
    return err;
  }
private:
  const ColDescArray& cols_;
  ObObj obj_array_[MAX_ROWKEY_OBJ];
  ObMtRowIterator iter_;
};

}; // end namespace memtable
}; // end namespace oceanbase
