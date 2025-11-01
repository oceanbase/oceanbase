/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_table_builder.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace observer
{
ObTableLoadTableBuildParam::ObTableLoadTableBuildParam() : table_data_desc_(), file_mgr_(nullptr) {}

bool ObTableLoadTableBuildParam::is_valid() const
{
  return table_data_desc_.is_valid() && OB_NOT_NULL(file_mgr_);
}

ObTableLoadTableBuilder::ObTableLoadTableBuilder() : is_inited_(false) {}

ObTableLoadTableBuilder::~ObTableLoadTableBuilder() {}

int ObTableLoadTableBuilder::init(const ObTableLoadTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadTableBuilder init twice", KR(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    ObDirectLoadExternalMultiPartitionTableBuildParam builder_param;
    builder_param.table_data_desc_ = param.table_data_desc_;
    builder_param.file_mgr_ = param.file_mgr_;
    builder_param.extra_buf_ = reinterpret_cast<char *>(1);
    builder_param.extra_buf_size_ = 4096;
    if (OB_FAIL(table_builder_.init(builder_param))) {
      LOG_WARN("fail to init new_builder", KR(ret));
    } else if (OB_FAIL(insert_datum_row_.init(param.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(delete_datum_row_.init(param.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      delete_datum_row_.is_delete_ = true;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTableBuilder::append_row(const ObTabletID &tablet_id,
                                        const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else if (OB_FAIL(table_builder_.append_row(tablet_id, datum_row))) {
    LOG_WARN("fail to append row", KR(ret));
  }
  return ret;
}

int ObTableLoadTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_.close())) {
      LOG_WARN("fail to close table builder", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTableBuilder::get_tables(storage::ObDirectLoadTableHandleArray &table_array,
                                        storage::ObDirectLoadTableManager *table_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataTableBuilder not init", KR(ret), KP(this));
  } else if (OB_FAIL(table_builder_.get_tables(table_array, table_mgr))) {
    LOG_WARN("fail to get tables", KR(ret));
  }
  return ret;
}

void ObTableLoadTableBuilderMgr::reset()
{
  if (table_builder_map_.created()) {
    FOREACH(it, table_builder_map_)
    {
      ObTableLoadTableBuilder *table_builder = it->second;
      table_builder->~ObTableLoadTableBuilder();
      allocator_.free(table_builder);
    }
    table_builder_map_.destroy();
    allocator_.reset();
  }
}

int ObTableLoadTableBuilderMgr::init(ObDirectLoadTableStore *table_store,
                                     storage::ObDirectLoadTmpFileManager *file_mgr,
                                     ObDirectLoadTableManager *table_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadTableBuilderMgr init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(table_store) || !table_store->is_valid() ||
                         OB_ISNULL(file_mgr) || OB_ISNULL(table_mgr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(table_store), KP(file_mgr), KP(table_mgr));
  } else if (OB_FAIL(table_builder_map_.create(1024, "TLD_TBMap", "TLD_TBMap", MTL_ID()))) {
    LOG_WARN("fail to create hashmap", KR(ret));
  } else {
    table_store_ = table_store;
    file_mgr_ = file_mgr;
    table_mgr_ = table_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadTableBuilderMgr::acquire_table_builder(ObTableLoadTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  ObTableLoadTableBuilder *new_builder = nullptr;
  ObTableLoadTableBuildParam builder_param;
  builder_param.table_data_desc_ = table_store_->get_table_data_desc();
  builder_param.file_mgr_ = file_mgr_;
  if (OB_ISNULL(new_builder = OB_NEWx(ObTableLoadTableBuilder, &safe_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadDataTableBuilder", KR(ret));
  } else if (OB_FAIL(new_builder->init(builder_param))) {
    LOG_WARN("fail to init new_builder", KR(ret));
  } else {
    table_builder = new_builder;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(new_builder)) {
    new_builder->~ObTableLoadTableBuilder();
    safe_allocator_.free(new_builder);
    new_builder = nullptr;
  }
  return ret;
}

int ObTableLoadTableBuilderMgr::get_table_builder(ObTableLoadTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTableBuilderMgr not init", KR(ret), KP(this));
  } else {
    const int64_t part_id = get_tid_cache();
    if (OB_FAIL(table_builder_map_.get_refactored(part_id, table_builder))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get table builder", KR(ret), K(get_tid_cache()));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(acquire_table_builder(table_builder))) {
          LOG_WARN("fail to acquire table builder", KR(ret));
        } else if (OB_ISNULL(table_builder)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table builder is null", KR(ret));
        } else if (OB_FAIL(table_builder_map_.set_refactored(part_id, table_builder))) {
          LOG_WARN("fail to set hashmap", KR(ret), K(part_id));
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(table_builder)) {
          table_builder->~ObTableLoadTableBuilder();
          safe_allocator_.free(table_builder);
          table_builder = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadTableBuilderMgr::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTableBuilderMgr not init", KR(ret), KP(this));
  } else {
    FOREACH_X(it, table_builder_map_, OB_SUCC(ret))
    {
      ObTableLoadTableBuilder *table_builder = it->second;
      ObDirectLoadTableHandleArray table_handle_array;
      if (OB_FAIL(table_builder->close())) {
        LOG_WARN("fail to close table builder", KR(ret));
      } else if (OB_FAIL(table_builder->get_tables(table_handle_array, table_mgr_))) {
        LOG_WARN("fail to get tables", KR(ret));
      } else if (OB_FAIL(table_store_->add_tables(table_handle_array))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase