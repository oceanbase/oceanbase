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

#ifndef OCEANBASE_UNITEST_BLOCKSSTABLE_OB_ROW_GENERATE_
#define OCEANBASE_UNITEST_BLOCKSSTABLE_OB_ROW_GENERATE_

#include "share/schema/ob_table_schema.h"
#include "storage/ob_i_store.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_malloc.h"
#include "share/ob_errno.h"
#include "common/object/ob_object.h"
#include "common/object/ob_obj_type.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{

namespace blocksstable
{

class ObRowGenerate
{
public:
  ObRowGenerate();
  ~ObRowGenerate();
  inline int init(const share::schema::ObTableSchema &src_schema, bool is_multi_version_row = false);
  inline int init(
      const share::schema::ObTableSchema &src_schema,
      common::ObArenaAllocator *allocator,
      bool is_multi_version_row = false);
  inline int get_next_row(ObDatumRow &row);
  inline int get_next_row(const int64_t seed, ObDatumRow &row);
  inline int get_next_row(const int64_t seed,
                          const int64_t trans_version,
                          const ObDmlFlag dml_flag,
                          ObDatumRow &row);
  inline int get_next_row(const int64_t seed,
                          const int64_t trans_version,
                          const ObDmlFlag dml_flag,
                          const bool is_compacted_row,
                          const bool is_last_row,
                          const bool is_first_row,
                          ObDatumRow &row);

  inline int get_next_row(const int64_t seed, const common::ObArray<uint64_t> &nop_column_idxs, ObDatumRow &row);
  inline int check_one_row(const ObDatumRow &row, bool &exist);
  // TODO @hanhui to be removed
  inline int get_next_row(storage::ObStoreRow &row);
  inline int get_next_row(const int64_t seed, storage::ObStoreRow &row);
  inline int get_next_row(const int64_t seed,
      const int64_t trans_version,
      const ObDmlFlag dml_flag,
      storage::ObStoreRow &row);
  inline int get_next_row(const int64_t seed,
      const int64_t trans_version,
      const ObDmlFlag dml_flag,
      const bool is_compacted_row,
      const bool is_last_row,
      const bool is_first_row,
      storage::ObStoreRow &row);

  inline int get_next_row(const int64_t seed, const common::ObArray<uint64_t> &nop_column_idxs, storage::ObStoreRow &row);
  inline int check_one_row(const storage::ObStoreRow &row, bool &exist);
  inline void reset() { seed_ = 0; column_list_.reset(); is_inited_ = false; allocator_.reset(); is_multi_version_row_ = false; }
  const share::schema::ObTableSchema &get_schema() const { return schema_; }

private:
  int generate_one_row(ObDatumRow& row, const int64_t seed, const ObDmlFlag dml_flag = DF_INSERT, const int64_t trans_version = 0);
  int generate_one_row(storage::ObStoreRow& row, const int64_t seed, const ObDmlFlag dml_flag = DF_INSERT, const int64_t trans_version = 0);
  int set_obj(const common::ObObjType &column_type, const uint64_t column_id, const int64_t seed, common::ObObj &obj, const int64_t trans_version);
  int compare_obj(const common::ObObjType &column_type, const int64_t value, const common::ObObj obj, bool &exist);
  int get_seed(const common::ObObjType &column_type, const common::ObObj obj, int64_t &seed);
int generate_urowid_obj(const int64_t rowkey_pos,
                        const int64_t seed,
                        const int64_t value,
                        ObObj &urowid_obj);

private:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator *p_allocator_;
  share::schema::ObTableSchema schema_;
  storage::ObColDescArray column_list_;
  int64_t seed_;
  bool is_multi_version_row_;
  bool is_inited_;
  bool is_reused_;
};

}//storage
}//oceanbase

//.ipp file implement the inline function, no need link
#include "ob_row_generate.ipp"

#endif //OCEANBASE_UNITEST_BLOCKSSTABLE_OB_ROW_GENERATE_
