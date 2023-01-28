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

#include "ob_lob_data_reader.h"
#include "ob_lob_manager.h"
#include "share/ob_lob_access_utils.h"

namespace oceanbase
{
using namespace common;

namespace storage
{

ObLobDataReader::ObLobDataReader()
  : is_inited_(false), tablet_id_(), access_ctx_(nullptr),
    allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObLobDataReader::~ObLobDataReader()
{
}

int ObLobDataReader::init(const ObTableIterParam &iter_param, storage::ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobDataReader has already been inited", K(ret));
  } else if (context.timeout_ == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument. timeout is 0", K(context));
  } else if (OB_ISNULL(context.store_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument. Null store ctx", K(context));
  } else if (!context.store_ctx_->mvcc_acc_ctx_.snapshot_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument. snapshot invalid", K(context.store_ctx_->mvcc_acc_ctx_.snapshot_));
  } else {
    access_ctx_ = &context;
    tablet_id_ = iter_param.tablet_id_;
    is_inited_ = true;
  }
  return ret;
}

void ObLobDataReader::reuse()
{
  allocator_.reuse();
}

void ObLobDataReader::reset()
{
  is_inited_ = false;
  allocator_.reset();
  access_ctx_ = nullptr;
  tablet_id_.reset();
}

int ObLobDataReader::read_lob_data_impl(blocksstable::ObStorageDatum &datum, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  const ObLobCommon &lob_common = datum.get_lob_data();
  ObString output_data;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (datum.len_ < sizeof(ObLobCommon)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid datum len", K(ret), K(datum));
  } else {
    ObLobManager* lob_mngr = MTL(ObLobManager*);
    if (OB_ISNULL(lob_mngr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get lob manager failed.", K(ret));
    } else {
      ObLobAccessParam param;
      param.snapshot_.core_ = access_ctx_->store_ctx_->mvcc_acc_ctx_.snapshot_;
      param.snapshot_.valid_ = true;
      param.snapshot_.source_ = transaction::ObTxReadSnapshot::SRC::LS;
      param.snapshot_.snapshot_lsid_ = access_ctx_->store_ctx_->ls_id_;
      param.sql_mode_ = access_ctx_->sql_mode_;
      param.ls_id_ = access_ctx_->ls_id_;
      param.tablet_id_ = tablet_id_;
      param.allocator_ = &allocator_;
      param.lob_common_ = const_cast<ObLobCommon*>(&lob_common);
      param.byte_size_ = param.lob_common_->get_byte_size(datum.len_);
      param.handle_size_ = datum.len_;
      param.coll_type_ = coll_type;
      param.timeout_ = access_ctx_->timeout_;
      param.scan_backward_ = false;
      param.offset_ = 0;
      // use byte len for get whold lob data
      param.len_ = param.byte_size_;

      if (param.byte_size_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("calc byte size is negative.", K(ret), K(datum), K(param));
      } else if (param.len_ == 0) {
        output_data.assign_ptr(param.lob_common_->buffer_, param.len_);
        datum.set_string(output_data);
      } else {
        // should not come here if lob locator v2 is used
        // no need to read full lob data directly from storage
        char *buf = static_cast<char *>(allocator_.alloc(param.byte_size_));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc output buffer.", K(ret), K(param));
        } else {
          output_data.assign_buffer(buf, param.byte_size_);
          if (OB_FAIL(lob_mngr->query(param, output_data))) {
            LOG_WARN("falied to query lob tablets.", K(ret), K(param));
          } else if (output_data.length() != param.len_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("query result length is not equal.", K(ret), K(output_data), K(param));
          } else {
            LOG_DEBUG("read output for query.", K(output_data));
            datum.set_string(output_data);
          }
        }
      }
    }
  }
  return ret;
}

int ObLobDataReader::read_lob_data(blocksstable::ObStorageDatum &datum, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", KP(this), K(ret));
  } else if (datum.is_nop() || datum.is_null()) {
  } else if (OB_FAIL(read_lob_data_impl(datum, coll_type))) {
    LOG_WARN("fail to read lob data", K(ret));
  }
  return ret;
}

int ObLobDataReader::fuse_disk_lob_header(common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (!obj.is_lob_storage() || obj.is_nop_value() || obj.is_null()) {
  } else {
    ObString data = obj.get_string();
    ObString out;
    if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, data, out))) {
      LOG_WARN("failed to fill header for lob data", K(ret), K(data));
    } else {
      obj.set_string(obj.get_type(), out);
    }
  }
  return ret;
}

} /* namespace blocksstable*/
} /* namespace oceanbase */
