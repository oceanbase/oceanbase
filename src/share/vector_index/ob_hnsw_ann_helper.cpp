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
#define USING_LOG_PREFIX RS

#include "share/vector_index/ob_hnsw_ann_helper.h"

namespace oceanbase {
namespace share {

int ObHNSWAnnHelper::mtl_init(ObHNSWAnnHelper *&ann_helper) {
  return ann_helper->init();
}

int ObHNSWAnnHelper::init() {
  int ret = OB_SUCCESS;
  ObMemAttr mattr(MTL_ID(), "HNSW");
  allocator_.set_attr(mattr);
  if (OB_FAIL(ann_index_readers_.create(1024, "HNSW", "HNSW", MTL_ID()))) {
    LOG_WARN("fail to create ann index readers", K(ret));
  }
  return ret;
}

int ObHNSWAnnHelper::get_ann_reader(int64_t ann_index_td_id,
                                    ObHNSWIndexReader *&reader) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ann_index_readers_.get_refactored(ann_index_td_id, reader))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get reader from ann_index_readers_", K(ret),
               K(ann_index_td_id));
    }
  }
  return ret;
}

int ObHNSWAnnHelper::create_ann_reader(int64_t ann_index_td_id,
                                       CreateAnnReaderArgs *args,
                                       ObHNSWIndexReader *&reader) {
  int ret = OB_SUCCESS;
  OB_ASSERT(MTL_ID() == args->tenant_id_);
  if (OB_FAIL(create_ann_reader_with_args(ann_index_td_id, args, reader))) {
    LOG_WARN("fail to create ann reader", K(ret));
  } else if (OB_FAIL(
                 ann_index_readers_.set_refactored(ann_index_td_id, reader))) {
    LOG_WARN("fail to set reader into ann_index_readers_", K(ret),
             K(ann_index_td_id));
  }
  return ret;
}

int ObHNSWAnnHelper::create_ann_reader_with_args(int64_t ann_index_td_id,
                                                 CreateAnnReaderArgs *args,
                                                 ObHNSWIndexReader *&reader) {
  int ret = OB_SUCCESS;
  void *reader_buf = nullptr;
  reader = nullptr;
  ObString index_table_name;
  ObString index_db_name;
  ObString vector_column_name;
  ObMemAttr mattr(MTL_ID(), "HNSW");
  if (OB_ISNULL(reader_buf = allocator_.alloc(sizeof(ObHNSWIndexReader)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObHNSWIndexReader", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, args->index_table_name_,
                                     index_table_name))) {
    LOG_WARN("fail to write string", K(ret), K(args->index_table_name_));
  } else if (OB_FAIL(ob_write_string(allocator_, args->index_db_name_,
                                     index_db_name))) {
    LOG_WARN("fail to write string", K(ret), K(args->index_db_name_));
  } else if (OB_FAIL(ob_write_string(allocator_, args->vector_column_name_,
                                     vector_column_name))) {
    LOG_WARN("fail to write string", K(ret), K(args->vector_column_name_));
  } else if (OB_FALSE_IT(reader = new (reader_buf) ObHNSWIndexReader(
                             args->m_, args->ef_construction_, args->dfunc_,
                             index_table_name, index_db_name, args->pkey_count_,
                             vector_column_name, args->tenant_id_, mattr))) {
  } else if (OB_FALSE_IT(reader->set_hnsw_vector_dim(args->hnsw_dim_))) {
  } else if (OB_FALSE_IT(reader->set_max_ep_level(args->cur_max_level_))) {
  } else if (OB_FALSE_IT(reader->set_ep_pkeys_for_search(args->rowkeys_))) {
  } else if (OB_FAIL(reader->init())) {
    LOG_WARN("fail to init hnsw index reader", K(ret));
  } else if (OB_FAIL(reader->init_ep_for_search(args->trans_))) {
    LOG_WARN("fail to init entry point", K(ret));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase