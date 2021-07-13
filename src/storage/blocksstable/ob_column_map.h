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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_COLUMN_MAP_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_COLUMN_MAP_H_
#include "share/ob_define.h"
#include "common/object/ob_object.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/ob_i_store.h"
#include "ob_block_sstable_struct.h"
namespace oceanbase {
namespace blocksstable {

struct ObFullMacroBlockMeta;

struct ObColumnIndexItem {
  uint64_t column_id_;
  common::ObObjMeta request_column_type_;
  common::ObObjMeta macro_column_type_;  // column type stored in macro meta
  int16_t store_index_;                  // request_index-->store_index
  bool is_column_type_matched_;
  OB_INLINE const common::ObObjMeta& get_obj_meta() const
  {
    return request_column_type_;
  }
  TO_STRING_KV(
      K_(column_id), K_(request_column_type), K_(macro_column_type), K_(store_index), K_(is_column_type_matched));
};

class ObColumnHashSet {
public:
  ObColumnHashSet()
      : allocator_(nullptr), column_ids_(nullptr), chain_(nullptr), bucket_(nullptr), capacity_(0), shift_(0)
  {}

  ~ObColumnHashSet()
  {
    reset();
  }

  void reset()
  {
    if (nullptr != allocator_) {
      if (nullptr != chain_) {
        allocator_->free(chain_);
        chain_ = nullptr;
        bucket_ = nullptr;
      }
      allocator_ = nullptr;
    }
    capacity_ = 0;
  }

  OB_INLINE int init(const int64_t column_cnt, uint16_t* column_ids, common::ObIAllocator& allocator)
  {
    int ret = common::OB_SUCCESS;
    allocator_ = &allocator;
    const int64_t bucket_cnt = common::next_pow2(column_cnt * 2 - 1);
    const int64_t size = sizeof(int16_t) * (column_cnt + bucket_cnt);
    shift_ = 16 - __builtin_ctz(static_cast<uint32_t>(bucket_cnt));
    if (size > capacity_) {
      if (nullptr != chain_ && nullptr != allocator_) {
        allocator_->free(chain_);
        chain_ = nullptr;
        bucket_ = nullptr;
      }
      if (OB_ISNULL(chain_ = static_cast<int16_t*>(allocator.alloc(size)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        capacity_ = size;
      }
    }
    if (OB_SUCC(ret)) {
      memset(chain_, 0xff, size);
      bucket_ = chain_ + column_cnt;
      column_ids_ = column_ids;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < column_cnt; ++idx) {
        uint16_t cid = column_ids_[idx];
        uint16_t pos = static_cast<uint16_t>(MAGIC * cid) >> shift_;
        int16_t cur_idx = bucket_[pos];
        while (OB_UNLIKELY(-1 != cur_idx)) {
          if (OB_UNLIKELY(column_ids_[cur_idx] == cid)) {
            ret = common::OB_ENTRY_EXIST;
            STORAGE_LOG(WARN, "duplicate column ids", K(ret), K(cid), K(cur_idx), K(idx));
            break;
          } else {
            cur_idx = chain_[cur_idx];
          }
        }
        if (OB_SUCC(ret)) {
          chain_[idx] = bucket_[pos];
          bucket_[pos] = static_cast<int16_t>(idx);
        }
      }
    }
    return ret;
  }

  OB_INLINE void get_index(const uint16_t column_id, int64_t& idx)
  {
    uint16_t pos = static_cast<uint16_t>(MAGIC * column_id) >> shift_;
    int16_t cur_idx = bucket_[pos];
    while (OB_LIKELY(-1 != cur_idx)) {
      if (column_ids_[cur_idx] == column_id) {
        break;
      } else {
        cur_idx = chain_[cur_idx];
      }
    }
    idx = cur_idx;
  }

private:
  static const uint16_t MAGIC = 3079;
  common::ObIAllocator* allocator_;
  uint16_t* column_ids_;
  int16_t* chain_;  // collision chain, each cell contain the index of the next element in the same bucket
  int16_t* bucket_;
  int64_t capacity_;
  int shift_;
};

class ObColumnMap {
public:
  ObColumnMap();
  virtual ~ObColumnMap()
  {}

  int init(common::ObIAllocator& allocator, const int64_t schema_version, const int64_t schema_rowkey_cnt,
      const int64_t store_cnt, const common::ObIArray<share::schema::ObColDesc>& out_cols,
      const share::schema::ColumnMap* cols_id_map = nullptr, const common::ObIArray<int32_t>* projector = nullptr,
      const int multi_version_rowkey_type = storage::ObMultiVersionRowkeyHelpper::MVRC_NONE);

  int rebuild(const ObFullMacroBlockMeta& macro_meta, bool force_rebuild = false);
  void reuse();
  void reset();

  OB_INLINE bool is_valid() const
  {
    return is_inited_;
  }
  OB_INLINE const share::schema::ColumnMap* get_cols_map() const
  {
    return cols_id_map_;
  }
  OB_INLINE int64_t get_request_count() const
  {
    return request_count_;
  }
  OB_INLINE int64_t get_store_count() const
  {
    return store_count_;
  }
  OB_INLINE int64_t get_rowkey_store_count() const
  {
    return rowkey_store_count_;
  }
  OB_INLINE int64_t get_seq_read_column_count() const
  {
    return seq_read_column_count_;
  }
  OB_INLINE const ObColumnIndexItem* get_column_indexs() const
  {
    return column_indexs_;
  }
  OB_INLINE bool is_multi_version() const
  {
    return 0 != multi_version_rowkey_cnt_;
  }
  OB_INLINE bool is_all_column_matched() const
  {
    return is_all_column_matched_;
  }
  OB_INLINE int64_t get_schema_version() const
  {
    return schema_version_;
  }
  OB_INLINE int32_t get_multi_version_rowkey_cnt() const
  {
    return multi_version_rowkey_cnt_;
  }
  TO_STRING_KV(K_(request_count), K_(store_count), K_(rowkey_store_count), K_(schema_version), KP_(cols_id_map),
      K_(is_inited), K_(multi_version_rowkey_cnt));

private:
  int init_column_index(const uint64_t col_id, const int64_t schema_rowkey_cnt, const int64_t store_index,
      const int64_t store_cnt, const bool is_multi_version, ObColumnIndexItem* column_index);

private:
  int64_t request_count_;
  int64_t store_count_;
  int64_t rowkey_store_count_;
  int64_t schema_version_;
  ObColumnHashSet column_hash_;
  const share::schema::ColumnMap* cols_id_map_;
  int64_t seq_read_column_count_;  // the count of common prefix between request columns and store columns
  int64_t cur_idx_;
  ObColumnIndexItem* column_indexs_;
  bool is_inited_;
  bool is_all_column_matched_;
  int8_t multi_version_rowkey_cnt_;
  bool create_col_id_map_;
  common::ObIAllocator* allocator_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
