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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_lob_table_builder.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace table;

/**
 * ObTableLoadLobTableBuildParam
 */

ObTableLoadLobTableBuildParam::ObTableLoadLobTableBuildParam()
  : lob_table_ctx_(nullptr), lob_column_idxs_(nullptr), table_data_desc_(), file_mgr_(nullptr)
{
}

bool ObTableLoadLobTableBuildParam::is_valid() const
{
  return nullptr != lob_table_ctx_ && nullptr != lob_column_idxs_ && !lob_column_idxs_->empty() &&
         table_data_desc_.is_valid() && nullptr != file_mgr_;
}

/**
 * ObTableLoadLobTableBuilder
 */

ObTableLoadLobTableBuilder::ObTableLoadLobTableBuilder()
  : lob_table_ctx_(nullptr), lob_column_idxs_(nullptr), is_inited_(false)
{
}

ObTableLoadLobTableBuilder::~ObTableLoadLobTableBuilder() {}

int ObTableLoadLobTableBuilder::init(const ObTableLoadLobTableBuildParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadLobTableBuilder init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    ObDirectLoadExternalMultiPartitionTableBuildParam builder_param;
    builder_param.table_data_desc_ = param.table_data_desc_;
    builder_param.file_mgr_ = param.file_mgr_;
    builder_param.extra_buf_ = reinterpret_cast<char *>(1);
    builder_param.extra_buf_size_ = 4096;
    if (OB_FAIL(datum_row_.init(param.table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (OB_FAIL(table_builder_.init(builder_param))) {
      LOG_WARN("fail to init table builder", KR(ret));
    } else {
      lob_table_ctx_ = param.lob_table_ctx_;
      lob_column_idxs_ = param.lob_column_idxs_;
      datum_row_.is_delete_ = true;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadLobTableBuilder::append_delete_row(const ObTabletID &tablet_id,
                                                  const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadLobTableBuilder not init", KR(ret), KP(this));
  } else {
    datum_row_.seq_no_ = datum_row.seq_no_;
    ObTabletID lob_tablet_id;
    if (OB_FAIL(lob_table_ctx_->get_tablet_id(tablet_id, lob_tablet_id))) {
      LOG_WARN("fail to get tablet id", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs_->count(); i++) {
      int64_t lob_idx = lob_column_idxs_->at(i);
      ObStorageDatum &datum = datum_row.storage_datums_[lob_idx];
      const ObLobCommon &lob_common = datum.get_lob_data();
      if (!lob_common.in_row_) {
        const ObLobId &lob_id = reinterpret_cast<const ObLobData *>(lob_common.buffer_)->id_;
        datum_row_.storage_datums_[0].set_string(reinterpret_cast<const char *>(&lob_id),
                                                 sizeof(ObLobId));
        if (OB_FAIL(table_builder_.append_row(lob_tablet_id, datum_row_))) {
          LOG_WARN("fail to append row", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadLobTableBuilder::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadLobTableBuilder not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_.close())) {
      LOG_WARN("fail to close table builder", KR(ret));
    }
  }
  return ret;
}

int64_t ObTableLoadLobTableBuilder::get_row_count() const { return table_builder_.get_row_count(); }

int ObTableLoadLobTableBuilder::get_tables(ObDirectLoadTableHandleArray &table_array,
                                           ObDirectLoadTableManager *table_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadLobTableBuilder not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_builder_.get_tables(table_array, table_mgr))) {
      LOG_WARN("fail to get tables", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
