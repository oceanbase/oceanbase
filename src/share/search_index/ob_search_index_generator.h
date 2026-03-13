/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_SEARCH_INDEX_GENERATOR_H_
#define OCEANBASE_SHARE_OB_SEARCH_INDEX_GENERATOR_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "share/datum/ob_datum.h"
#include "share/vector/ob_i_vector.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "ob_search_index_config_filter.h"

namespace oceanbase
{
namespace common
{
class ObCollectionArrayType;
}

namespace share
{

// Search index rowkey: [column_idx, path, value, doc_id]
struct ObSearchIndexRow
{
public:
  // for json/array null
  explicit ObSearchIndexRow(uint64_t column_idx, uint64_t doc_id)
    : column_idx_(column_idx), path_(), value_(), doc_id_(doc_id)
  {
    path_.set_null();
    value_.set_null();
  }

  // for scalar type, the path is null
  explicit ObSearchIndexRow(uint64_t column_idx, ObString &value, uint64_t doc_id)
    : column_idx_(column_idx), path_(), value_(value.ptr(), value.length(), false), doc_id_(doc_id)
  {
    path_.set_null();
  }

  explicit ObSearchIndexRow(uint64_t column_idx, ObString &path, ObString &value, uint64_t doc_id)
    : column_idx_(column_idx),
      path_(path.ptr(), path.length(), false),
      value_(value.ptr(), value.length(), false),
      doc_id_(doc_id) {}

  ObSearchIndexRow() : column_idx_(OB_INVALID_ID), path_(), value_(), doc_id_(OB_INVALID_ID) {}

  TO_STRING_KV(K_(column_idx), K_(path), K_(value), K_(doc_id));

  int build_datum_row(common::ObIAllocator &allocator, blocksstable::ObDatumRow &datum_row);
  uint64_t column_idx_;
  ObDatum path_;
  ObDatum value_;
  uint64_t doc_id_;
};

// Key for document-level (path, value) deduplication to handle nested array duplicates
struct PathValueKey
{
  common::ObString path_;
  common::ObString value_;
};

struct PathValueKeyHash
{
  int operator()(const PathValueKey &k, uint64_t &res) const
  {
    uint64_t h = 0;
    if (k.path_.ptr() != nullptr && k.path_.length() > 0) {
      h = common::murmurhash(k.path_.ptr(), k.path_.length(), h);
    }
    if (k.value_.ptr() != nullptr && k.value_.length() > 0) {
      h = common::murmurhash(k.value_.ptr(), k.value_.length(), h);
    }
    res = h;
    return common::OB_SUCCESS;
  }
};

struct PathValueKeyEqual
{
  bool operator()(const PathValueKey &a, const PathValueKey &b) const
  {
    bool bret = true;
    if (a.path_.length() != b.path_.length() || a.value_.length() != b.value_.length()) {
      bret = false;
    } else if (a.path_.length() > 0 && (a.path_.ptr() == nullptr || b.path_.ptr() == nullptr ||
         MEMCMP(a.path_.ptr(), b.path_.ptr(), a.path_.length()) != 0)) {
      bret = false;
    } else if (a.value_.length() > 0 && (a.value_.ptr() == nullptr || b.value_.ptr() == nullptr ||
         MEMCMP(a.value_.ptr(), b.value_.ptr(), a.value_.length()) != 0)) {
      bret = false;
    }
    return bret;
  }
};

typedef ObSEArray<ObSearchIndexRow, 16> ObSearchIndexRows;
typedef hash::ObHashSet<PathValueKey, hash::NoPthreadDefendMode,
                        PathValueKeyHash, PathValueKeyEqual> PathValueDedupSet;

// Search index row generator: generate search index rows for multiple columns
class ObSearchIndexRowGenerator {
private:
  // Generator: generate search index rows for a single column
  class Generator {
  public:
    Generator(ObIAllocator &allocator, ObIAllocator &inner_allocator)
      : allocator_(allocator),
        inner_allocator_(inner_allocator),
        column_idx_(OB_INVALID_ID),
        enc_param_(),
        obj_meta_(),
        arr_type_(nullptr),
        elem_generator_(nullptr),
        config_filter_(nullptr) {}
    virtual ~Generator() {}

    int init(const uint64_t column_idx, const ObObjMeta &obj_meta,
             const common::ObCollectionArrayType *arr_type,
             const ObString *column_comment = nullptr);
    int generate_rows(const int64_t doc_id, const ObDatum& datum, ObSearchIndexRows &rows);
    int generate_rows(const ObIVector &doc_id_vec, const ObIVector &data_vec,
                      const int64_t batch_size, ObSearchIndexRows &rows);

    TO_STRING_KV(K_(column_idx), K_(enc_param), K_(obj_meta));

  private:
    int init_array_generator(const common::ObCollectionArrayType *arr_type);
    int generate_json_rows(const int64_t doc_id, const ObDatum& datum, ObSearchIndexRows &rows);
    int generate_array_rows(const int64_t doc_id, const ObDatum& datum, ObSearchIndexRows &rows);
    int generate_rows_for_array_obj(const int64_t doc_id,
                                    common::ObIArrayType *arr_obj,
                                    ObSearchIndexRows &rows);
    int generate_rows(const int64_t doc_id, ObIJsonBase *j_base,
                      ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                      ObSearchIndexRows &rows,
                      PathValueDedupSet &dedup_set);

  private:
    ObIAllocator &allocator_;
    ObIAllocator &inner_allocator_;
    uint64_t column_idx_;
    ObEncParam enc_param_;
    ObObjMeta obj_meta_;

    // for array type
    common::ObCollectionArrayType *arr_type_;
    Generator *elem_generator_;
    const ObSearchIndexConfigFilter *config_filter_;
  };

public:
  explicit ObSearchIndexRowGenerator()
    : row_allocator_(common::ObMemAttr(MTL_ID(), "SearchIdxRow")),
      inner_allocator_(common::ObMemAttr(MTL_ID(), "SearchIdxGen")),
      generators_(inner_allocator_), row_projector_(inner_allocator_), row_projector_ptr_(nullptr)
      {}
  virtual ~ObSearchIndexRowGenerator() { reset(); }

  void reset();

  int init(const common::ObIArray<int32_t> &included_cid_idxes,
           const common::ObIArray<ObObjMeta> &included_obj_metas,
           const common::ObIArray<int64_t> &row_projector,
           const common::ObIArray<ObString> &column_comments,
           bool rowkey_only,
           const common::ObIArray<common::ObCollectionArrayType*> *arr_types);

  int generate_rows(const sql::ObChunkDatumStore::StoredRow *store_row,
                        blocksstable::ObDatumRow *&rows, int64_t& row_cnt);
  int generate_rows(const blocksstable::ObDatumRow &store_row,
                        blocksstable::ObDatumRow *&rows, int64_t& row_cnt);
  int set_row_projector(const common::ObFixedArray<int64_t, ObIAllocator> *row_projector);
private:
  // row allocator for search index rows
  common::ObArenaAllocator row_allocator_;
  // inner allocator for generators and row_projector structures
  common::ObArenaAllocator inner_allocator_;
  common::ObFixedArray<Generator*, ObIAllocator> generators_;
  common::ObFixedArray<int64_t, ObIAllocator> row_projector_;
  const common::ObFixedArray<int64_t, ObIAllocator> *row_projector_ptr_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SEARCH_INDEX_GENERATOR_H_
