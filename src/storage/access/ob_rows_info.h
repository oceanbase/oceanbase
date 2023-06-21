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

#ifndef OB_STORAGE_ROWS_INFO_H
#define OB_STORAGE_ROWS_INFO_H

#include "ob_table_access_context.h"
#include "share/allocator/ob_reserve_arena.h"

namespace oceanbase
{
namespace storage
{
struct ObRowsInfo
{
public:
  typedef common::ObReserveArenaAllocator<1024> ObStorageReserveAllocator;
  explicit ObRowsInfo();
  ~ObRowsInfo();
  OB_INLINE bool is_valid() const
  {
    return is_inited_ && exist_helper_.is_valid() && delete_count_ >= 0
        && rowkeys_.count() >= delete_count_ && OB_NOT_NULL(rows_);
  }
  int init(
      const ObRelativeTable &table,
      ObStoreCtx &store_ctx,
      const ObITableReadInfo &rowkey_read_info);
  int check_duplicate(ObStoreRow *rows, const int64_t row_count, ObRelativeTable &table);
  const blocksstable::ObDatumRowkey& get_duplicate_rowkey() const { return min_key_; }
  blocksstable::ObDatumRowkey& get_duplicate_rowkey() { return min_key_; }
  const blocksstable::ObDatumRowkey& get_min_rowkey() const { return min_key_; }
  int check_min_rowkey_boundary(const blocksstable::ObDatumRowkey &max_rowkey, bool &may_exist);
  int refine_rowkeys();
  int clear_found_rowkey(const int64_t rowkey_idx);
  void reuse_scan_mem_allocator() { scan_mem_allocator_.reuse(); }
  bool is_inited() const { return is_inited_; }
  OB_INLINE bool all_rows_found() { return delete_count_ == rowkeys_.count(); }
  TO_STRING_KV(K_(rowkeys), K_(min_key), K_(table_id), K_(delete_count), K_(exist_helper));
public:
  struct ExistHelper final {
    ExistHelper();
    ~ExistHelper();
    int init(
        const ObRelativeTable &table,
        ObStoreCtx &store_ctx,
        const ObITableReadInfo &rowkey_read_info,
        ObStorageReserveAllocator &allocator);
    OB_INLINE bool is_valid() const { return is_inited_; }
    TO_STRING_KV(K_(table_iter_param), K_(table_access_context));
    ObTableIterParam table_iter_param_;
    ObTableAccessContext table_access_context_;
    bool is_inited_;
  };
private:
  struct RowsCompare {
    RowsCompare(const blocksstable::ObStorageDatumUtils &datum_utils,
                blocksstable::ObDatumRowkey &dup_key,
                const bool check_dup,
                int &ret)
      : datum_utils_(datum_utils),
        dup_key_(dup_key),
        check_dup_(check_dup),
        ret_(ret)
    {}
    ~RowsCompare() = default;
    OB_INLINE bool operator() (const blocksstable::ObDatumRowkey &left, const blocksstable::ObDatumRowkey &right)
    {
      int cmp_ret = 0;
      int &ret = ret_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(left.compare(right, datum_utils_, cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare datum rowkey", K(ret), K(left), K(right));
      } else if (OB_UNLIKELY(check_dup_ && 0 == cmp_ret)) {
        ret_ = common::OB_ERR_PRIMARY_KEY_DUPLICATE;
        dup_key_ = left;
        STORAGE_LOG(WARN, "Rowkey already exists", K_(dup_key), K_(ret));
      }
      return cmp_ret < 0;
    }
    const blocksstable::ObStorageDatumUtils &datum_utils_;
    blocksstable::ObDatumRowkey &dup_key_;
    bool check_dup_;
    int &ret_;
  };
private:
  void reuse();
  ObStorageReserveAllocator scan_mem_allocator_; // scan/rescan level memory entity, only for query
  ObStorageReserveAllocator key_allocator_;
public:
  static const int64_t DEFAULT_ROW_KEY_ARR_SIZE = 1;
  common::ObSEArray<blocksstable::ObDatumRowkey, DEFAULT_ROW_KEY_ARR_SIZE> rowkeys_;
  ObStoreRow *rows_;
  ExistHelper exist_helper_;
  uint64_t table_id_;
  ObTabletID tablet_id_;
private:
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  blocksstable::ObDatumRowkey min_key_;
  int64_t delete_count_;
  int16_t rowkey_column_num_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObRowsInfo);
};

} // namespace storage
} // namespace oceanbase
#endif
