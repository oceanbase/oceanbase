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

#include "storage/direct_load/ob_direct_load_table_store.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"
#include "storage/direct_load/ob_direct_load_fast_heap_table_builder.h"
#include "storage/direct_load/ob_direct_load_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_table_builder_allocator.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace table;

/**
 * ObDirectLoadTableStoreParam
 */

ObDirectLoadTableStoreParam::ObDirectLoadTableStoreParam()
  : snapshot_version_(0),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    cmp_funcs_(nullptr),
    file_mgr_(nullptr),
    is_multiple_mode_(false),
    is_fast_heap_table_(false),
    insert_table_ctx_(nullptr),
    fast_heap_table_ctx_(nullptr),
    dml_row_handler_(nullptr),
    extra_buf_(nullptr),
    extra_buf_size_(0)
{
}

ObDirectLoadTableStoreParam::~ObDirectLoadTableStoreParam()
{
}

bool ObDirectLoadTableStoreParam::is_valid() const
{
  return snapshot_version_ > 0 && table_data_desc_.is_valid() && nullptr != datum_utils_ &&
         nullptr != col_descs_ && nullptr != cmp_funcs_ && nullptr != file_mgr_ &&
         (!is_fast_heap_table_ ||
          (nullptr != insert_table_ctx_ && nullptr != fast_heap_table_ctx_)) &&
         nullptr != dml_row_handler_;
}

/**
 * ObDirectLoadTableStoreBucket
 */

ObDirectLoadTableStoreBucket::ObDirectLoadTableStoreBucket()
  : param_(nullptr), table_builder_allocator_(nullptr), table_builder_(nullptr), is_inited_(false)
{
}

ObDirectLoadTableStoreBucket::~ObDirectLoadTableStoreBucket()
{
}

int ObDirectLoadTableStoreBucket::init(const ObDirectLoadTableStoreParam &param,
                                       const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadTableStore init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(tablet_id));
  } else {
    table_builder_allocator_ = get_table_builder_allocator();
    if (param.is_multiple_mode_) {
      // new external multi partition table
      ObDirectLoadExternalMultiPartitionTableBuildParam external_mp_table_build_param;
      external_mp_table_build_param.table_data_desc_ = param.table_data_desc_;
      external_mp_table_build_param.datum_utils_ = param.datum_utils_;
      external_mp_table_build_param.file_mgr_ = param.file_mgr_;
      external_mp_table_build_param.extra_buf_ = param.extra_buf_;
      external_mp_table_build_param.extra_buf_size_ = param.extra_buf_size_;
      ObDirectLoadExternalMultiPartitionTableBuilder *external_mp_table_builder = nullptr;
      if (OB_ISNULL(
            external_mp_table_builder =
              table_builder_allocator_->alloc<ObDirectLoadExternalMultiPartitionTableBuilder>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObDirectLoadExternalMultiPartitionTableBuilder", KR(ret));
      } else if (OB_FAIL(external_mp_table_builder->init(external_mp_table_build_param))) {
        LOG_WARN("fail to init external multi partition table builder", KR(ret));
      }
      table_builder_ = external_mp_table_builder;
    } else if (param.is_fast_heap_table_) {
      abort_unless(param.table_data_desc_.is_heap_table_);
      // new fast heap table
      ObDirectLoadFastHeapTableBuildParam fast_heap_table_build_param;
      fast_heap_table_build_param.tablet_id_ = tablet_id;
      fast_heap_table_build_param.snapshot_version_ = param.snapshot_version_;
      fast_heap_table_build_param.table_data_desc_ = param.table_data_desc_;
      fast_heap_table_build_param.datum_utils_ = param.datum_utils_;
      fast_heap_table_build_param.col_descs_ = param.col_descs_;
      fast_heap_table_build_param.cmp_funcs_ = param.cmp_funcs_;
      fast_heap_table_build_param.insert_table_ctx_ = param.insert_table_ctx_;
      fast_heap_table_build_param.fast_heap_table_ctx_ = param.fast_heap_table_ctx_;
      fast_heap_table_build_param.dml_row_handler_ = param.dml_row_handler_;
      fast_heap_table_build_param.online_opt_stat_gather_ = param.online_opt_stat_gather_;
      ObDirectLoadFastHeapTableBuilder *fast_heap_table_builder = nullptr;
      if (OB_ISNULL(fast_heap_table_builder =
                      table_builder_allocator_->alloc<ObDirectLoadFastHeapTableBuilder>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObDirectLoadFastHeapTableBuilder", KR(ret));
      } else if (OB_FAIL(fast_heap_table_builder->init(fast_heap_table_build_param))) {
        LOG_WARN("fail to init sstable builder", KR(ret));
      }
      table_builder_ = fast_heap_table_builder;
    } else {
      abort_unless(!param.table_data_desc_.is_heap_table_);
      // new sstable
      ObDirectLoadSSTableBuildParam sstable_build_param;
      sstable_build_param.tablet_id_ = tablet_id;
      sstable_build_param.table_data_desc_ = param.table_data_desc_;
      sstable_build_param.datum_utils_ = param.datum_utils_;
      sstable_build_param.file_mgr_ = param.file_mgr_;
      ObDirectLoadSSTableBuilder *sstable_builder = nullptr;
      if (OB_ISNULL(sstable_builder =
                      table_builder_allocator_->alloc<ObDirectLoadSSTableBuilder>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObDirectLoadSSTableBuilder", KR(ret));
      } else if (OB_FAIL(sstable_builder->init(sstable_build_param))) {
        LOG_WARN("fail to init sstable builder", KR(ret));
      }
      table_builder_ = sstable_builder;
    }
    if (OB_SUCC(ret)) {
      param_ = &param;
      is_inited_ = true;
    } else {
      clean_up();
    }
  }
  return ret;
}

int ObDirectLoadTableStoreBucket::append_row(const ObTabletID &tablet_id,
                                             const ObTableLoadSequenceNo &seq_no,
                                             const ObDatumRow &datum_row)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, table_store_bucket_append_row);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTableStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !datum_row.is_valid() ||
                         datum_row.get_column_count() != param_->table_data_desc_.column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(datum_row), KPC(param_));
  } else {
    if (OB_FAIL(table_builder_->append_row(tablet_id, seq_no, datum_row))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTableStoreBucket::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTableStore not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_->close())) {
      LOG_WARN("fail to close table builder", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTableStoreBucket::get_tables(ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                                             ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTableStore not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_->get_tables(table_array, allocator))) {
      LOG_WARN("fail to get tables", KR(ret));
    }
  }
  return ret;
}

void ObDirectLoadTableStoreBucket::clean_up()
{
  if (nullptr != table_builder_) {
    table_builder_allocator_->free(table_builder_);
    table_builder_allocator_ = nullptr;
    table_builder_ = nullptr;
  }
}

/**
 * ObDirectLoadTableStore
 */

ObDirectLoadTableStore::~ObDirectLoadTableStore()
{
  for (int64_t i = 0; i < bucket_ptr_array_.count(); i++) {
    if (bucket_ptr_array_.at(i) != nullptr) {
      bucket_ptr_array_.at(i)->~ObDirectLoadTableStoreBucket();
    }
  }
}

int ObDirectLoadTableStore::init(const ObDirectLoadTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadTableStore init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    const uint64_t tenant_id = MTL_ID();
    param_ = param;
    allocator_.set_tenant_id(tenant_id);
    if (OB_FAIL(tablet_index_.create(64, "TLD_TS_PartMap", "TLD_TS_PartMap", tenant_id))) {
      LOG_WARN("fail to create hashmap", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadTableStore::new_bucket(ObDirectLoadTableStoreBucket *&bucket)
{
  int ret = OB_SUCCESS;
  bucket = nullptr;
  if (OB_ISNULL(bucket = OB_NEWx(ObDirectLoadTableStoreBucket, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadTableStoreBucket", K(ret));
  } else if (OB_FAIL(bucket_ptr_array_.push_back(bucket))) {
    LOG_WARN("fail to push back bucket", KR(ret));
  }
  return ret;
}

int ObDirectLoadTableStore::get_bucket(const ObTabletID &tablet_id,
                                       ObDirectLoadTableStoreBucket *&bucket)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, table_store_get_bucket);
  int ret = OB_SUCCESS;
  bucket = nullptr;
  if (!param_.is_multiple_mode_) {
    if (OB_FAIL(tablet_index_.get_refactored(tablet_id, bucket))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(new_bucket(bucket))) {
          LOG_WARN("fail to new bucket", KR(ret));
        } else if (OB_FAIL(bucket->init(param_, tablet_id))) {
          LOG_WARN("fail to init bucket", KR(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(tablet_index_.set_refactored(tablet_id, bucket))) {
            LOG_WARN("fail to set refactored", KR(ret), K(tablet_id));
          }
        }
      }
    }
  } else {
    if (bucket_ptr_array_.count() >= 1) {
      bucket = bucket_ptr_array_.at(0);
    } else {
      if (OB_FAIL(new_bucket(bucket))) {
        LOG_WARN("fail to new bucket", KR(ret));
      } else if (OB_FAIL(bucket->init(param_, tablet_id))) {
        LOG_WARN("fail to init bucket", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadTableStore::append_row(const ObTabletID &tablet_id, const ObTableLoadSequenceNo &seq_no, const ObDatumRow &datum_row)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, table_store_append_row);
  OB_TABLE_LOAD_STATISTICS_COUNTER(table_store_row_count);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTableStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !datum_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(datum_row));
  } else {
    ObDirectLoadTableStoreBucket *bucket = nullptr;
    if (OB_FAIL(get_bucket(tablet_id, bucket))) {
      LOG_WARN("fail to get bucket", KR(ret), K(tablet_id));
    } else if (OB_FAIL(bucket->append_row(tablet_id, seq_no, datum_row))) {
      LOG_WARN("fail to append row to bucket", KR(ret), K(tablet_id), K(datum_row));
    }
  }
  return ret;
}

int ObDirectLoadTableStore::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTableStore not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_ptr_array_.count(); ++i) {
      if (OB_FAIL(bucket_ptr_array_.at(i)->close())) {
        LOG_WARN("fail to close bucket", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDirectLoadTableStore::get_tables(ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTableStore not init", KR(ret), KP(this));
  } else {
    table_array.reset();
    ObSEArray<ObIDirectLoadPartitionTable *, 16> bucket_table_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_ptr_array_.count(); ++i) {
      bucket_table_array.reset();
      if (OB_FAIL(bucket_ptr_array_.at(i)->get_tables(bucket_table_array, allocator))) {
        LOG_WARN("fail to get tables from bucket", KR(ret), K(i));
      } else if (OB_UNLIKELY(bucket_table_array.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty bucket table array", KR(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < bucket_table_array.count(); ++j) {
        if (OB_FAIL(table_array.push_back(bucket_table_array.at(j)))) {
          LOG_WARN("fail to push back table", KR(ret));
        }
      }
    }
  }
  return ret;
}

void ObDirectLoadTableStore::clean_up()
{
  for (int64_t i = 0; i < bucket_ptr_array_.count(); ++i) {
    bucket_ptr_array_.at(i)->clean_up();
  }
}

} // namespace storage
} // namespace oceanbase
