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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/ob_partition_range_spliter.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;

class ObTenantMetaMemMgr;

void ObCompactionBufferWriter::reset()
{
}

int ObTenantMetaMemMgr::fetch_tenant_config()
{
  return OB_SUCCESS;
}

static int get_number(const char *str, const char *&endpos, int64_t &num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "str can not been null", KR(ret));
  } else {
    char *pos = nullptr;
    errno = 0;
    num = ::strtoll(str, &pos, 10);
    if (pos == nullptr || pos == str || errno != 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "fail to strtoll", KR(ret), K(str), K(errno), K(pos));
    } else {
      endpos = pos;
    }
  }
  return ret;
}

struct ObMockRowkeyRange
{
  ObMockRowkeyRange() : start_(-1), end_(-1) {}
  TO_STRING_KV(K_(start), K_(end));

  bool operator==(const ObMockRowkeyRange &other) const
  {
    return start_ == other.start_ && end_ == other.end_;
  }
  bool operator!=(const ObMockRowkeyRange &other) const
  {
    return !(*this == other);
  }

  int64_t start_; // 包含
  int64_t end_;   // 不包含
};

struct ObMockRowkeyRanges
{
  void reset() { ranges_.reset(); }
  int add_range(int64_t start, int64_t end);
  int from(const ObString &str);

  int64_t count() const { return ranges_.count(); }
  ObMockRowkeyRange &at(int64_t pos) { return ranges_.at(pos); }

  bool operator==(const ObMockRowkeyRanges &other) const
  {
    bool equal = false;
    if (ranges_.count() == other.ranges_.count()) {
      equal = true;
      for (int64_t i = 0; i < ranges_.count(); ++i) {
        if (ranges_.at(i) != other.ranges_.at(i)) {
          equal = false;
          break;
        }
      }
    }
    return equal;
  }
  bool operator!=(const ObMockRowkeyRanges &other) const
  {
    return !(*this == other);
  }

  ObSEArray<ObMockRowkeyRange, 128> ranges_;
};

int ObMockRowkeyRanges::add_range(int64_t start, int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start >= end)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(start), K(end));
  } else {
    bool need_push_back = true;

    if (!ranges_.empty()) {
      ObMockRowkeyRange &last_range = ranges_.at(ranges_.count() - 1);
      if (last_range.end_ > start) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid argument", KR(ret), K(start), K(end), K(last_range));
      } else if (last_range.end_ == start) {
        last_range.end_ = end;
        need_push_back = false;
      }
    }

    if (OB_SUCC(ret) && need_push_back) {
      ObMockRowkeyRange range;
      range.start_ = start;
      range.end_ = end;
      if (OB_FAIL(ranges_.push_back(range))) {
        STORAGE_LOG(WARN, "failed to push ranges", KR(ret));
      }
    }
  }
  return ret;
}

int ObMockRowkeyRanges::from(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument empty str", KR(ret));
  } else {
    ranges_.reset();
    const char *pos1 = str.ptr(), *pos2 = nullptr;
    int64_t num1 = 0, num2 = 0;
    while (OB_SUCC(ret) && *pos1 != '\0') {
      if (OB_FAIL(get_number(pos1, pos2, num1))) {
        STORAGE_LOG(WARN, "failed to get number", KR(ret), K(pos1));
      } else {
        if (*pos2 == ',' || *pos2 == '\0') {
          if (OB_FAIL(add_range(num1, num1 + 1))) {
            STORAGE_LOG(WARN, "failed to add range", KR(ret), K(num1));
          }
        } else if (strncmp(pos2, "...", 3) == 0) {
          pos1 = pos2 + 3;
          if (OB_FAIL(get_number(pos1, pos2, num2))) {
            STORAGE_LOG(WARN, "failed to get number", KR(ret), K(pos1));
          } else if (OB_FAIL(add_range(num1, num2 + 1))) {
            STORAGE_LOG(WARN, "failed to add range", KR(ret), K(num1), K(num2));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected str", KR(ret), K(pos2));
        }

        if (OB_SUCC(ret)) {
          while (*pos2 != '\0' && (*pos2 == ' ' || *pos2 == ','))
            ++pos2;
          pos1 = pos2;
          pos2 = nullptr;
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

struct ObMockDatumRowkey
{
public:
  static const int64_t COLUMN_NUM = 1;
  ObMockDatumRowkey() {}
  ObMockDatumRowkey(int64_t val)
  {
    datum_.set_int(val);
    rowkey_.assign(&datum_, 1);
  }
  ~ObMockDatumRowkey() = default;
  ObMockDatumRowkey(const ObMockDatumRowkey &other)
  {
    datum_ = other.datum_;
    rowkey_.assign(&datum_, 1);
  }
  int64_t get_int() const { return datum_.get_int(); }
  bool is_min_rowkey() const { return rowkey_.is_min_rowkey(); }
  bool is_max_rowkey() const { return rowkey_.is_max_rowkey(); }
  void set_min_rowkey() { rowkey_.set_min_rowkey(); }
  void set_max_rowkey() { rowkey_.set_max_rowkey(); }
  ObMockDatumRowkey &operator=(const ObMockDatumRowkey &other)
  {
    datum_ = other.datum_;
    rowkey_.assign(&datum_, 1);
    return *this;
  }
  bool operator==(const ObMockDatumRowkey &other) const
  {
    return datum_ == other.datum_;
  }
  bool operator!=(const ObMockDatumRowkey &other) const
  {
    return !(*this == other);
  }
  bool operator<(const ObMockDatumRowkey &other) const
  {
    if (*this == other) {
      return false;
    } else if (is_min_rowkey() || other.is_max_rowkey()) {
      return true;
    } else if (is_max_rowkey() || other.is_min_rowkey()) {
      return false;
    } else {
      return get_int() < other.get_int();
    }
  }
  bool operator>(const ObMockDatumRowkey &other) const
  {
    if (*this == other) {
      return false;
    } else if (is_max_rowkey() || other.is_min_rowkey()) {
      return true;
    } else if (is_min_rowkey() || other.is_max_rowkey()) {
      return false;
    } else {
      return get_int() > other.get_int();
    }
  }
  bool operator<=(const ObMockDatumRowkey &other) const
  {
    return !(*this > other);
  }
  bool operator>=(const ObMockDatumRowkey &other) const
  {
    return !(*this < other);
  }

  static ObMockDatumRowkey get_min()
  {
    static ObMockDatumRowkey rowkey;
    rowkey.set_min_rowkey();
    return rowkey;
  }
  static ObMockDatumRowkey get_max()
  {
    static ObMockDatumRowkey rowkey;
    rowkey.set_max_rowkey();
    return rowkey;
  }

  TO_STRING_KV("val", datum_.get_int());

public:
  ObStorageDatum datum_;
  ObDatumRowkey rowkey_;
};

static int64_t get_val(const ObMockDatumRowkey &rowkey)
{
  if (rowkey.is_min_rowkey()) {
    return INT64_MIN;
  } else if (rowkey.is_max_rowkey()) {
    return INT64_MAX;
  } else {
    return rowkey.get_int();
  }
}

static int64_t get_val(const ObDatumRowkey &rowkey)
{
  if (rowkey.is_min_rowkey()) {
    return INT64_MIN;
  } else if (rowkey.is_max_rowkey()) {
    return INT64_MAX;
  } else {
    return rowkey.datums_[0].get_int();
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMockSSTableV2;

class ObMockSSTableSecMetaIterator : public ObSSTableSecMetaIterator
{
public:
  ObMockSSTableSecMetaIterator() : sstable_(nullptr), macro_block_idx_(0) {}
  virtual ~ObMockSSTableSecMetaIterator() = default;

  int get_next(ObDataMacroBlockMeta &macro_meta) override;

private:
  ObMockSSTableV2 *sstable_;
  int macro_block_idx_;
};

class ObMockSSTableV2 : public ObSSTable
{
public:
  ObMockSSTableV2() = default;
  virtual ~ObMockSSTableV2() = default;
  int scan_secondary_meta(ObIAllocator &allocator, ObSSTableSecMetaIterator *&meta_iter);
  int get_last_rowkey(ObDatumRowkey &rowkey);
  int add_macro_block_meta(const int64_t endkey);
  int from(const ObString &str);
  void reset()
  {
    endkeys_.reset();
    allocator_.reset();
  }
private:
  ObArenaAllocator allocator_;
  ObSEArray<ObMockDatumRowkey, 64> endkeys_;
};


int ObMockSSTableSecMetaIterator::get_next(ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else if (macro_block_idx_ >= sstable_->endkeys_.count()) {
    ret = OB_ITER_END;
  } else {
    blocksstable::ObDataMacroBlockMeta &meta = dynamic_cast<blocksstable::ObDataMacroBlockMeta &>(macro_meta);
    meta.end_key_ = sstable_->endkeys_.at(macro_block_idx_).rowkey_;
    meta.val_.rowkey_count_ = meta.end_key_.get_datum_cnt();
    meta.val_.column_count_ = meta.val_.rowkey_count_ + 1;
    meta.val_.micro_block_count_ = 1;
    meta.val_.occupy_size_ = 100;
    meta.val_.original_size_ = 100;
    meta.val_.data_zsize_ = 100;
    meta.val_.logic_id_.tablet_id_ = 1;
    meta.val_.logic_id_.logic_version_ = 1;
    meta.val_.macro_id_.set_block_index(100);
    meta.val_.version_ = ObDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION;
    meta.val_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    meta.val_.row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    ++macro_block_idx_;
  }
  return ret;
}

int ObMockSSTableV2::add_macro_block_meta(const int64_t endkey)
{
  int ret = OB_SUCCESS;
  ObMockDatumRowkey new_rowkey(endkey);
  if (!endkeys_.empty()) {
    const ObMockDatumRowkey &last = endkeys_.at(endkeys_.count() - 1);
    if (last >= new_rowkey) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(last), K(endkey));
    }
    LOG_INFO("judge last key", KR(ret), K(endkey), K(last));
  }

  if (OB_SUCC(ret)) {
    int64_t val = get_val(new_rowkey);
    if (OB_FAIL(endkeys_.push_back(new_rowkey))) {
      LOG_WARN("failed to push endkey", KR(ret));
    }
    LOG_INFO("add macro block", KR(ret), K(endkey), K(new_rowkey));
  }
  return ret;
}

int ObMockSSTableV2::from(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument empty str", KR(ret));
  } else {
    endkeys_.reset();
    const char *pos1 = str.ptr(), *pos2 = nullptr;
    int64_t num = 0;
    while (OB_SUCC(ret) && *pos1 != '\0') {
      if (OB_FAIL(get_number(pos1, pos2, num))) {
        STORAGE_LOG(WARN, "failed to get number", KR(ret), K(pos1));
      } else if (OB_FAIL(add_macro_block_meta(num))) {
        STORAGE_LOG(WARN, "failed to add macro meta", KR(ret), K(num));
      } else {
        while (*pos2 != '\0' && (*pos2 == ' ' || *pos2 == ','))
          ++pos2;
        pos1 = pos2;
        pos2 = nullptr;
      }
    }
  }
  return ret;
}

int ObMockSSTableV2::scan_secondary_meta(ObIAllocator &allocator, ObSSTableSecMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObMockSSTableSecMetaIterator *iter = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMockSSTableSecMetaIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory", KR(ret));
  } else if (OB_ISNULL(iter = new (buf) ObMockSSTableSecMetaIterator())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null pointer of secondary meta iterator", KR(ret));
  } else {
    iter->sstable_ = this;
    iter->is_inited_ = true;
    meta_iter = iter;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    iter->~ObMockSSTableSecMetaIterator();
  }
  return ret;
}

int ObMockSSTableV2::get_last_rowkey(ObDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  if (endkeys_.empty()) {
    endkey = ObMockDatumRowkey::get_min().rowkey_;
  } else {
    endkey = endkeys_.at(endkeys_.count() - 1).rowkey_;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMockIncrementalIterator : public ObPartitionIncrementalRangeSpliter::ObIncrementalIterator
{
public:
  ObMockIncrementalIterator(compaction::ObTabletMergeCtx &merge_ctx, ObIAllocator &allocator)
    : ObIncrementalIterator(merge_ctx, allocator), cur_range_pos_(0), cur_rowkey_pos_(0) {}
  virtual ~ObMockIncrementalIterator() {}
  void reset();
  int init();
  int get_next_row(const ObDatumRow *&row) override;
  int set_ranges(const ObMockRowkeyRanges &ranges);
public:
  int64_t cur_range_pos_;
  int64_t cur_rowkey_pos_;
  ObMockRowkeyRanges ranges_;
  ObDatumRow row_;
  ObSEArray<ObColDesc, ObMockDatumRowkey::COLUMN_NUM> out_cols_;
};

void ObMockIncrementalIterator::reset()
{
  cur_range_pos_ = 0;
  cur_rowkey_pos_ = 0;
}

int ObMockIncrementalIterator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", KR(ret));
  } else {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = 1;
    col_desc.col_type_.set_int();
    col_desc.col_order_ = ASC;

    if (row_.init(allocator_, ObMockDatumRowkey::COLUMN_NUM)) {
      STORAGE_LOG(WARN, "failed to init datum row", KR(ret));
    } else if (OB_FAIL(out_cols_.push_back(col_desc))) {
      STORAGE_LOG(WARN, "failed to push back col desc", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return OB_SUCCESS;
}

int ObMockIncrementalIterator::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (cur_range_pos_ >= ranges_.count()) {
        ret = OB_ITER_END;
      } else {
        ObMockRowkeyRange &range = ranges_.at(cur_range_pos_);
        if (range.start_ + cur_rowkey_pos_ >= range.end_) {
          // next range
          ++cur_range_pos_;
          cur_rowkey_pos_ = 0;
        } else {
          ObObj obj;
          obj.set_int(range.start_ + cur_rowkey_pos_);
          if (OB_FAIL(row_.storage_datums_[0].from_obj(obj))) {
            STORAGE_LOG(WARN, "failed to datum from obj", KR(ret));
          } else {
            row = &row_;
            ++cur_rowkey_pos_;
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObMockIncrementalIterator::set_ranges(const ObMockRowkeyRanges &ranges)
{
  ranges_ = ranges;
  cur_range_pos_ = 0;
  cur_rowkey_pos_ = 0;
  return OB_SUCCESS;
}

class ObMockPartitionIncrementalRangeSpliter : public ObPartitionIncrementalRangeSpliter
{
public:
  int init_incremental_iter() override;
  int get_incremental_iter(ObMockIncrementalIterator *&iter);
  int get_major_sstable_end_rowkey(ObDatumRowkey &rowkey) override;
  int scan_major_sstable_secondary_meta(const ObDatumRange &scan_range, ObSSTableSecMetaIterator *&meta_iter) override;
};

int ObMockPartitionIncrementalRangeSpliter::init_incremental_iter()
{
  int ret = OB_SUCCESS;
  ObMockIncrementalIterator *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else if (nullptr != iter_) {
    iter = dynamic_cast<ObMockIncrementalIterator *>(iter_);
    iter->reset();
  } else {
    ObMockIncrementalIterator *iter = nullptr;
    if (OB_ISNULL(iter = OB_NEWx(ObMockIncrementalIterator, allocator_, *merge_ctx_, *allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", KR(ret), "size", sizeof(ObMockIncrementalIterator));
    } else if (OB_FAIL(iter->init())) {
      STORAGE_LOG(WARN, "failed to init incremental iterator", KR(ret));
    } else {
      iter_ = iter;
    }
  }
  return ret;
}

int ObMockPartitionIncrementalRangeSpliter::get_incremental_iter(ObMockIncrementalIterator *&iter)
{
  int ret = OB_SUCCESS;
  iter = nullptr;
  if (iter_ == nullptr) {
    if (OB_FAIL(init_incremental_iter())) {
      STORAGE_LOG(WARN, "failed to init incremental iter", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    iter = dynamic_cast<ObMockIncrementalIterator *>(iter_);
  }
  return ret;
}

int ObMockPartitionIncrementalRangeSpliter::get_major_sstable_end_rowkey(ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObMockSSTableV2 *sstable = dynamic_cast<ObMockSSTableV2 *>(major_sstable_);
  if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null sstable", KR(ret));
  } else if (OB_FAIL(sstable->get_last_rowkey(rowkey))) {
    STORAGE_LOG(WARN, "failed to get major sstable last rowkey", KR(ret), K(*major_sstable_));
  }
  return ret;
}

int ObMockPartitionIncrementalRangeSpliter::scan_major_sstable_secondary_meta(
  const ObDatumRange &scan_range, ObSSTableSecMetaIterator *&meta_iter)
{
  UNUSED(scan_range);
  int ret = OB_SUCCESS;
  ObMockSSTableV2 *sstable = dynamic_cast<ObMockSSTableV2 *>(major_sstable_);
  if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null sstable", KR(ret));
  } else if (OB_FAIL(sstable->scan_secondary_meta(*allocator_, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

class TestPartitionIncrementalRangeSliter : public ::testing::Test
{
  static const int64_t MAX_BUF_LENGTH = 1024;
public:
  TestPartitionIncrementalRangeSliter()
    : tenant_id_(1), tenant_base_(tenant_id_), merge_ctx_(param_, allocator_), buf_(nullptr), is_inited_(false)
  {
    major_sstable_.meta_ = &major_sstable_meta_;
    minor_sstable_.meta_ = &minor_sstable_meta_;
  }
  virtual ~TestPartitionIncrementalRangeSliter() {}

public:
  virtual void SetUp();
  virtual void TearDown();
private:
  void set_tablet_size(int64_t tablet_size)
  {
    storage_schema_.tablet_size_ = tablet_size;
    range_spliter_.tablet_size_ = tablet_size;
  }
  void set_major_sstable_meta(int64_t macro_block_count, int64_t occupy_size, int64_t row_count)
  {
    major_sstable_.meta_->basic_meta_.data_macro_block_count_ = macro_block_count;
    major_sstable_.meta_->basic_meta_.occupy_size_ = occupy_size;
    major_sstable_.meta_->basic_meta_.row_count_ = row_count;
    major_sstable_.addr_.set_none_addr();
    major_sstable_.meta_cache_.data_macro_block_count_ = macro_block_count;
    major_sstable_.meta_cache_.occupy_size_ = occupy_size;
    major_sstable_.meta_cache_.row_count_ = row_count;
    major_sstable_.meta_->is_inited_ = true;
  }
  int set_major_sstable_macro_blocks(const ObString &str);
  void reset_major_sstable()
  {
    set_major_sstable_meta(0, 0, 0);
    major_sstable_.reset();
  }
  int set_ranges(const ObString &str);
  void reset_ranges();
  int set_default_noisy_row_num_skipped(int64_t num);
  int set_default_row_num_per_range(int64_t num);

private:
  int check_iterator_result(storage::ObIStoreRowIterator *iter, const ObString &result, bool &equal);
  int check_ranges_result(const ObIArray<ObDatumRange> &ranges, const ObString &result, bool &equal);

private:
  void inner_test_get_next_row(const ObString &ranges_str, const ObString &rows_str);
  void inner_test_get_next_meta(const ObString &metas_str);
  void inner_test_is_incremental(const ObString &ranges_str, const bool is_incremenal);
  void inner_test_split_ranges(const ObString &ranges_str, const ObString &split_ranges, bool full_merge = false);

private:
  const uint64_t tenant_id_;
  share::ObTenantBase tenant_base_;
  ObArenaAllocator allocator_;
  compaction::ObTabletMergeDagParam param_;
  ObStorageSchema storage_schema_;
  ObTablet tablet_;
  ObMockSSTableV2 major_sstable_;
  ObSSTableMeta major_sstable_meta_;
  ObStorageMetaHandle mock_major_sstable_handle_;
  ObSSTable minor_sstable_;  // 增量
  ObSSTableMeta minor_sstable_meta_;
  ObStorageMetaHandle mock_minor_sstable_handle_;
  compaction::ObTabletMergeCtx merge_ctx_;
  ObSEArray<ObColDesc, 3> col_descs_;

  char *buf_;

  ObMockPartitionIncrementalRangeSpliter range_spliter_;

  bool is_inited_;
};

void TestPartitionIncrementalRangeSliter::SetUp()
{
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  if (!is_inited_) {
    int ret = OB_SUCCESS;
    OB_SERVER_BLOCK_MGR.super_block_.body_.macro_block_size_ = 1;

    ObTenantMetaMemMgr *t3m = OB_NEW(ObTenantMetaMemMgr, ObModIds::TEST, tenant_id_);
    ret = t3m->init();
    ASSERT_EQ(OB_SUCCESS, ret);
    tenant_base_.set(t3m);
    share::ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    // table schema
    storage_schema_.tablet_size_ = 1024;
    storage_schema_.rowkey_array_.set_allocator(&allocator_);
    storage_schema_.rowkey_array_.reserve(1);
    ObStorageRowkeyColumnSchema rowkey_col;
    rowkey_col.column_idx_ = 1 + common::OB_APP_MIN_COLUMN_ID;
    rowkey_col.meta_type_.set_int();
    rowkey_col.order_ = ASC;
    ASSERT_EQ(OB_SUCCESS, storage_schema_.rowkey_array_.push_back(rowkey_col));

    // major sstable
    major_sstable_.set_table_type(ObITable::MAJOR_SSTABLE);
    major_sstable_.key_.tablet_id_ = 1;
    major_sstable_.meta_->basic_meta_.data_macro_block_count_ = 0;
    major_sstable_.meta_->basic_meta_.occupy_size_ = 0;
    major_sstable_.meta_->basic_meta_.row_count_ = 0;
    major_sstable_.valid_for_reading_ = true;
    major_sstable_.addr_.set_none_addr();
    major_sstable_.meta_cache_.data_macro_block_count_ =  major_sstable_.meta_->basic_meta_.data_macro_block_count_;
    major_sstable_.meta_->is_inited_ = true;

    // minor sstable
    minor_sstable_.set_table_type(ObITable::MINOR_SSTABLE);
    minor_sstable_.key_.tablet_id_ = 1;
    minor_sstable_.addr_.set_none_addr();
    minor_sstable_.meta_cache_.data_macro_block_count_ = minor_sstable_.meta_->basic_meta_.data_macro_block_count_;
    minor_sstable_.meta_->is_inited_ = true;

    // merge ctx
    compaction::ObStaticMergeParam &static_param = merge_ctx_.static_param_;
    merge_ctx_.static_param_.schema_ = &storage_schema_;
    ASSERT_EQ(OB_SUCCESS, static_param.tables_handle_.add_sstable(&major_sstable_, mock_major_sstable_handle_));
    ASSERT_EQ(OB_SUCCESS, static_param.tables_handle_.add_sstable(&minor_sstable_, mock_minor_sstable_handle_));
    merge_ctx_.tablet_handle_.obj_ = &tablet_;
    merge_ctx_.tablet_handle_.allocator_ = &allocator_;
    ObColDesc col_desc;
    col_desc.col_id_ = 1 + common::OB_APP_MIN_COLUMN_ID;
    col_desc.col_type_.set_int();
    col_descs_.reset();
    ASSERT_EQ(OB_SUCCESS, col_descs_.push_back(col_desc));
    ASSERT_EQ(OB_SUCCESS, storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(col_descs_));
    void *ptr = nullptr;
    ASSERT_NE(nullptr, ptr = allocator_.alloc(sizeof(ObRowkeyReadInfo)));
    tablet_.rowkey_read_info_ = new (ptr) ObRowkeyReadInfo();
    ASSERT_EQ(OB_SUCCESS, tablet_.rowkey_read_info_->init(allocator_, col_descs_.count(), 1, lib::is_oracle_mode(), col_descs_));

    // buf
    buf_ = static_cast<char *>(allocator_.alloc(MAX_BUF_LENGTH));
    ASSERT_NE(nullptr, buf_);

    // range spliter
    ASSERT_EQ(OB_SUCCESS, range_spliter_.init(merge_ctx_, allocator_));

    is_inited_ = true;
  }
}

void TestPartitionIncrementalRangeSliter::TearDown()
{
  ASSERT_TRUE(is_inited_);
  merge_ctx_.tablet_handle_.get_obj()->rowkey_read_info_->reset();
  merge_ctx_.tablet_handle_.obj_ = nullptr;
  reset_major_sstable();
  reset_ranges();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  OB_DELETE(ObTenantMetaMemMgr, ObModIds::TEST, t3m);
  tenant_base_.destroy();
  share::ObTenantEnv::set_tenant(nullptr);
}

int TestPartitionIncrementalRangeSliter::set_major_sstable_macro_blocks(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(major_sstable_.from(str))) {
    STORAGE_LOG(WARN, "failed to get macro blocks from str", KR(ret), K(str));
  }
  return ret;
}

int TestPartitionIncrementalRangeSliter::set_ranges(const ObString &str)
{
  int ret = OB_SUCCESS;
  ObMockIncrementalIterator *iter = nullptr;
  if (OB_FAIL(range_spliter_.get_incremental_iter(iter))) {
    STORAGE_LOG(WARN, "failed to get incremental iter", KR(ret));
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null iter", KR(ret));
  } else if (OB_FAIL(iter->ranges_.from(str))) {
    STORAGE_LOG(WARN, "failed to get ranges from str", KR(ret), K(str));
  }
  return ret;
}

void TestPartitionIncrementalRangeSliter::reset_ranges()
{
  ObMockIncrementalIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, range_spliter_.get_incremental_iter(iter));
  ASSERT_NE(nullptr, iter);
  iter->reset();
  iter->ranges_.reset();
}

int TestPartitionIncrementalRangeSliter::set_default_noisy_row_num_skipped(int64_t num)
{
  int ret = OB_SUCCESS;
  if (num < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(num));
  } else {
    range_spliter_.default_noisy_row_num_skipped_ = num;
  }
  return ret;
}

int TestPartitionIncrementalRangeSliter::set_default_row_num_per_range(int64_t num)
{
  int ret = OB_SUCCESS;
  if (num < 1) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(num));
  } else {
    range_spliter_.default_row_num_per_range_ = num;
  }
  return ret;
}

int TestPartitionIncrementalRangeSliter::check_iterator_result(storage::ObIStoreRowIterator *iter, const ObString &result, bool &equal)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid agrument", KR(ret));
  } else {
    const ObDatumRow *row = nullptr;
    int64_t length = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_row(row))) {
        if (ret != OB_ITER_END) {
          STORAGE_LOG(WARN, "failed to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        int64_t val = row->storage_datums_[0].get_int();
        length += ::snprintf(buf_ + length, MAX_BUF_LENGTH - length, "%ld,", val);
      }
    }

    if (OB_SUCC(ret)) {
      if (length > 0) {
        --length;  // 去掉末尾的','
      }
      buf_[length] = '\0';
      equal = (length == result.length() && ::memcmp(buf_, result.ptr(), length) == 0);
    }
  }
  return ret;
}

int TestPartitionIncrementalRangeSliter::check_ranges_result(const ObIArray<ObDatumRange> &ranges, const ObString &result, bool &equal)
{
  int ret = OB_SUCCESS;
  if (ranges.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(ranges), K(result));
  } else if (OB_UNLIKELY(!ranges.at(0).get_start_key().is_min_rowkey() || !ranges.at(0).is_left_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected ranges", KR(ret), K(ranges));
  } else if (OB_UNLIKELY(!ranges.at(ranges.count() - 1).get_end_key().is_max_rowkey() ||
                         !ranges.at(ranges.count() - 1).is_right_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected ranges", KR(ret), K(ranges));
  } else if (ranges.count() == 1 || result.empty()) {
    equal = (ranges.count() == 1 && result.empty());
  } else {
    int64_t length = 0;
    for (int64_t i = 1; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObDatumRange &prev_range = ranges.at(i - 1);
      const ObDatumRange &cur_range = ranges.at(i);
      if (OB_UNLIKELY(!prev_range.is_right_closed() || !cur_range.is_left_open())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected range", K(ret), K(i), K(prev_range), K(cur_range));
      } else if (OB_UNLIKELY(get_val(cur_range.get_start_key()) != get_val(prev_range.get_end_key()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ranges", K(ret), K(i), K(prev_range), K(cur_range));
      } else {
        int64_t val = get_val(cur_range.get_start_key());
        length += ::snprintf(buf_ + length, MAX_BUF_LENGTH - length, "%ld,", val);
      }
    }
    // STORAGE_LOG(DEBUG, "ranges result", KR(ret), K(buf_), K(result));
    if (OB_SUCC(ret)) {
      if (length > 0) {
        --length;  // 去掉末尾的','
      }
      buf_[length] = '\0';
      equal = (length == result.length() && ::memcmp(buf_, result.ptr(), length) == 0);
      if (!equal) {
        STORAGE_LOG(DEBUG, "ranges result", KR(ret), K(buf_));
      }
    }
  }
  return ret;
}

void TestPartitionIncrementalRangeSliter::inner_test_get_next_row(const ObString &ranges_str, const ObString &rows_str)
{
  bool equal = false;

  ObMockRowkeyRanges ranges;
  ASSERT_EQ(OB_SUCCESS, ranges.from(ranges_str));

  ObMockIncrementalIterator iter(merge_ctx_, allocator_);
  ASSERT_EQ(OB_SUCCESS, iter.init());
  ASSERT_EQ(OB_SUCCESS, iter.set_ranges(ranges));

  ASSERT_EQ(OB_SUCCESS, check_iterator_result(&iter, rows_str, equal));
  ASSERT_TRUE(equal);
}

void TestPartitionIncrementalRangeSliter::inner_test_get_next_meta(const ObString &metas_str)
{
  ObMockSSTableV2 sstable;
  ASSERT_EQ(OB_SUCCESS, sstable.from(metas_str));

  ObStoreRange range;
  ObSSTableSecMetaIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable.scan_secondary_meta(allocator_, iter));
  ASSERT_NE(nullptr, iter);

  int ret = OB_SUCCESS;
  int64_t length = 0;
  ObDataMacroBlockMeta blk_meta;
  while (OB_SUCC(iter->get_next(blk_meta))) {
    int64_t val = get_val(blk_meta.end_key_);
    length += ::snprintf(buf_ + length, MAX_BUF_LENGTH - length, "%ld,", val);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  if (length > 0) {
    --length;  // 去掉末尾的','
  }
  buf_[length] = '\0';
  ASSERT_EQ(length, metas_str.length());
  ASSERT_EQ(0, ::memcmp(buf_, metas_str.ptr(), length));
}

void TestPartitionIncrementalRangeSliter::inner_test_is_incremental(const ObString &ranges_str, const bool is_incremental)
{
  bool result = false;

  ASSERT_EQ(OB_SUCCESS, set_ranges(ranges_str));

  ASSERT_EQ(OB_SUCCESS, range_spliter_.check_is_incremental(result));
  ASSERT_EQ(is_incremental, result);
}

void TestPartitionIncrementalRangeSliter::inner_test_split_ranges(const ObString &ranges_str, const ObString &split_ranges, bool full_merge)
{
  bool is_incremental = false;
  bool equal = false;
  ObSEArray<ObDatumRange, 64> range_array;

  merge_ctx_.static_param_.is_full_merge_ = full_merge;

  ASSERT_EQ(OB_SUCCESS, set_ranges(ranges_str));

  ASSERT_EQ(OB_SUCCESS, range_spliter_.check_is_incremental(is_incremental));
  ASSERT_TRUE(is_incremental);

  ASSERT_EQ(OB_SUCCESS, range_spliter_.split_ranges(range_array));
  ASSERT_EQ(OB_SUCCESS, check_ranges_result(range_array, split_ranges, equal));
  ASSERT_TRUE(equal);
}

TEST_F(TestPartitionIncrementalRangeSliter, test_ranges)
{
  {
    const char *ranges_str = "1,3,5";
    ObMockRowkeyRanges ranges1;
    ASSERT_EQ(OB_SUCCESS, ranges1.from(ranges_str));

    ObMockRowkeyRanges ranges2;
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(1, 2));
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(3, 4));
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(5, 6));

    EXPECT_TRUE(ranges1 == ranges2);
  }

  {
    const char *ranges_str = "1...100";
    ObMockRowkeyRanges ranges1;
    ASSERT_EQ(OB_SUCCESS, ranges1.from(ranges_str));

    ObMockRowkeyRanges ranges2;
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(1, 101));

    EXPECT_TRUE(ranges1 == ranges2);
  }

  {
    const char *ranges_str = "1,2,3,5...100";
    ObMockRowkeyRanges ranges1;
    ASSERT_EQ(OB_SUCCESS, ranges1.from(ranges_str));

    ObMockRowkeyRanges ranges2;
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(1, 4));
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(5, 101));

    EXPECT_TRUE(ranges1 == ranges2);
  }

  {
    const char *ranges_str = "1...100,110,201...300";
    ObMockRowkeyRanges ranges1;
    ASSERT_EQ(OB_SUCCESS, ranges1.from(ranges_str));

    ObMockRowkeyRanges ranges2;
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(1, 101));
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(110, 111));
    ASSERT_EQ(OB_SUCCESS,ranges2.add_range(201, 301));

    EXPECT_TRUE(ranges1 == ranges2);
  }
}

TEST_F(TestPartitionIncrementalRangeSliter, test_get_next_row)
{
  EXPECT_NO_FATAL_FAILURE(inner_test_get_next_row("1,3,5", "1,3,5"));
  EXPECT_NO_FATAL_FAILURE(inner_test_get_next_row("1,2,3,5...10", "1,2,3,5,6,7,8,9,10"));
  EXPECT_NO_FATAL_FAILURE(inner_test_get_next_row("7...10,15,21...23", "7,8,9,10,15,21,22,23"));
}

TEST_F(TestPartitionIncrementalRangeSliter, test_get_next_meta)
{
  //ASSERT_EQ(OB_SUCCESS, set_major_sstable_macro_blocks("1,3,5"));
  //ASSERT_NE(OB_SUCCESS, set_major_sstable_macro_blocks("1,3,3"));

  EXPECT_NO_FATAL_FAILURE(inner_test_get_next_meta("1"));
  //EXPECT_NO_FATAL_FAILURE(inner_test_get_next_meta("1,3,5"));
}

TEST_F(TestPartitionIncrementalRangeSliter, test_empty_major_sstable_check_is_incremental)
{
  ASSERT_EQ(OB_SUCCESS, set_default_noisy_row_num_skipped(100));
  ASSERT_EQ(OB_SUCCESS, set_default_row_num_per_range(100));

  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...20", true));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...100", true));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...101", true));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...2000", true));
}

TEST_F(TestPartitionIncrementalRangeSliter, test_empty_major_sstable_split_range)
{
  ASSERT_EQ(OB_SUCCESS, set_default_noisy_row_num_skipped(100));
  ASSERT_EQ(OB_SUCCESS, set_default_row_num_per_range(100));

  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...50", ""));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...100", ""));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...110", "100"));
  /*
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...2000",
    "100,200,300,400,500,600,700,800,900,1000,"
    "1100,1200,1300,1400,1500,1600,1700,1800,"
    "1900"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...6400",
    "100,200,300,400,500,600,700,800,900,1000,"
    "1100,1200,1300,1400,1500,1600,1700,1800,1900,2000,"
    "2100,2200,2300,2400,2500,2600,2700,2800,2900,3000,"
    "3100,3200,3300,3400,3500,3600,3700,3800,3900,4000,"
    "4100,4200,4300,4400,4500,4600,4700,4800,4900,5000,"
    "5100,5200,5300,5400,5500,5600,5700,5800,5900,6000,"
    "6100,6200,6300"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...6500",
    "200,400,600,800,1000,1200,1400,1600,1800,2000,"
    "2200,2400,2600,2800,3000,3200,3400,3600,3800,4000,"
    "4200,4400,4600,4800,5000,5200,5400,5600,5800,6000,"
    "6200,6400"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...8000",
    "200,400,600,800,1000,1200,1400,1600,1800,2000,"
    "2200,2400,2600,2800,3000,3200,3400,3600,3800,4000,"
    "4200,4400,4600,4800,5000,5200,5400,5600,5800,6000,"
    "6200,6400,6600,6800,7000,7200,7400,7600,7800"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...64000",
    "1000,2000,3000,4000,5000,6000,7000,8000,9000,10000,"
    "11000,12000,13000,14000,15000,16000,17000,18000,19000,20000,"
    "21000,22000,23000,24000,25000,26000,27000,28000,29000,30000,"
    "31000,32000,33000,34000,35000,36000,37000,38000,39000,40000,"
    "41000,42000,43000,44000,45000,46000,47000,48000,49000,50000,"
    "51000,52000,53000,54000,55000,56000,57000,58000,59000,60000,"
    "61000,62000,63000"));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("1...64100",
    "1100,2200,3300,4400,5500,6600,7700,8800,9900,11000,"
    "12100,13200,14300,15400,16500,17600,18700,19800,20900,22000,"
    "23100,24200,25300,26400,27500,28600,29700,30800,31900,33000,"
    "34100,35200,36300,37400,38500,39600,40700,41800,42900,44000,"
    "45100,46200,47300,48400,49500,50600,51700,52800,53900,55000,"
    "56100,57200,58300,59400,60500,61600,62700,63800"));
  */
}

TEST_F(TestPartitionIncrementalRangeSliter, test_not_empty_major_sstable_check_is_incremental)
{
  // set major sstable meta
  set_major_sstable_meta(1/*macro_block_count*/, 1/*occupy_size*/, 1/*row_count*/);

  // set range spliter
  ASSERT_EQ(OB_SUCCESS, set_default_noisy_row_num_skipped(100));
  ASSERT_EQ(OB_SUCCESS, set_default_row_num_per_range(100));

  ASSERT_EQ(OB_SUCCESS, set_major_sstable_macro_blocks("60"));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...20", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...80", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...100", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...101", true));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("51...100", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("51...150", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("51...151", true));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("101...120", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("101...200", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("101...201", true));

  ASSERT_EQ(OB_SUCCESS, set_major_sstable_macro_blocks("200"));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...80", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...100", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...120", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_is_incremental("1...1200", false));
}

TEST_F(TestPartitionIncrementalRangeSliter, test_not_empty_major_sstable_split_range)
{
  set_tablet_size(2);

  // set major sstable
  set_major_sstable_meta(7/*macro_block_count*/, 700/*occupy_size*/, 700/*row_count*/);
  ASSERT_EQ(OB_SUCCESS, set_major_sstable_macro_blocks("100,200,300,400,500,600,700"));

  // set range spliter
  ASSERT_EQ(OB_SUCCESS, set_default_noisy_row_num_skipped(100));
  ASSERT_EQ(OB_SUCCESS, set_default_row_num_per_range(100));

  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...2201", "2200", false/*full_merge*/));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...2201", "200,400,600,700,2200", true));

  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...3000", "2200,2400,2600,2800", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...3000", "200,400,600,700,2200,2400,2600,2800", true));

  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...14800",
    "2200,2400,2600,2800,3000,3200,3400,3600,3800,4000,"
    "4200,4400,4600,4800,5000,5200,5400,5600,5800,6000,"
    "6200,6400,6600,6800,7000,7200,7400,7600,7800,8000,"
    "8200,8400,8600,8800,9000,9200,9400,9600,9800,10000,"
    "10200,10400,10600,10800,11000,11200,11400,11600,11800,12000,"
    "12200,12400,12600,12800,13000,13200,13400,13600,13800,14000,"
    "14200,14400,14600", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...14800",
    "400,700,2400,2800,3200,3600,4000,4400,4800,5200,"
    "5600,6000,6400,6800,7200,7600,8000,8400,8800,9200,"
    "9600,10000,10400,10800,11200,11600,12000,12400,12800,13200,"
    "13600,14000,14400", true));

  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...148000",
    "4400,6800,9200,11600,14000,16400,18800,21200,23600,26000,"
    "28400,30800,33200,35600,38000,40400,42800,45200,47600,50000,"
    "52400,54800,57200,59600,62000,64400,66800,69200,71600,74000,"
    "76400,78800,81200,83600,86000,88400,90800,93200,95600,98000,"
    "100400,102800,105200,107600,110000,112400,114800,117200,119600,122000,"
    "124400,126800,129200,131600,134000,136400,138800,141200,143600,146000", false));
  EXPECT_NO_FATAL_FAILURE(inner_test_split_ranges("2001...148000",
    "3600,6000,8400,10800,13200,15600,18000,20400,22800,25200,"
    "27600,30000,32400,34800,37200,39600,42000,44400,46800,49200,"
    "51600,54000,56400,58800,61200,63600,66000,68400,70800,73200,"
    "75600,78000,80400,82800,85200,87600,90000,92400,94800,97200,"
    "99600,102000,104400,106800,109200,111600,114000,116400,118800,121200,"
    "123600,126000,128400,130800,133200,135600,138000,140400,142800,145200,"
    "147600", true));
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_partition_incremental_range_spliter.log*");
  oceanbase::ObLogger::get_logger().set_file_name("test_partition_incremental_range_spliter.log");
  oceanbase::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
