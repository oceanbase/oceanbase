/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_HNSW_INDEX_HNSW_ANN_HELPER_H
#define OCEANBASE_SHARE_HNSW_INDEX_HNSW_ANN_HELPER_H

#include "share/vector_index/ob_hnsw_index_reader.h"

namespace oceanbase {
namespace share {

struct CreateAnnReaderArgs {
  int64_t m_;
  int64_t ef_construction_;
  ObHNSWOption::DistanceFunc dfunc_;
  const ObString &index_table_name_;
  const ObString &index_db_name_;
  const ObString &vector_column_name_;
  int64_t pkey_count_;
  uint64_t tenant_id_;
  int64_t hnsw_dim_;
  int64_t cur_max_level_;
  common::ObArray<ObString> *rowkeys_;
  common::ObMySQLTransaction *trans_;
};

class ObHNSWAnnHelper {
public:
  static int mtl_init(ObHNSWAnnHelper *&ann_helper);
  int init();
  int get_ann_reader(int64_t ann_index_td_id, ObHNSWIndexReader *&reader);
  int create_ann_reader(int64_t ann_index_td_id, CreateAnnReaderArgs *args,
                        ObHNSWIndexReader *&reader);
  void destroy() {
    ann_index_readers_.reuse();
    allocator_.reset();
  }

private:
  int create_ann_reader_with_args(int64_t ann_index_td_id,
                                  CreateAnnReaderArgs *args,
                                  ObHNSWIndexReader *&reader);
  common::ObArenaAllocator allocator_;
  hash::ObHashMap<int64_t, ObHNSWIndexReader *> ann_index_readers_;
};

} // namespace share
} // namespace oceanbase

#endif