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

#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

int ObTabletObjLoadHelper::read_from_addr(
    common::ObArenaAllocator &allocator,
    const ObMetaDiskAddr &meta_addr,
    /*out*/ char *&buf,
    /*out*/ int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  buf_len = 0;

  if (OB_UNLIKELY(!meta_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta addr", K(ret), K(meta_addr));
  } else if (meta_addr.is_block()) {
    // read from storage
    if (OB_FAIL(read_from_storage_(allocator,
                                   meta_addr,
                                   buf,
                                   buf_len))) {
      LOG_WARN("fail to read from storage", K(ret), K(meta_addr), KP(buf),
        K(buf_len));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported meta addr", K(ret), K(meta_addr));
  }
  return ret;
}

int ObTabletObjLoadHelper::read_from_storage_(
    common::ObArenaAllocator &allocator,
    const ObMetaDiskAddr &meta_addr,
    char *&buf,
    int64_t &buf_len)
{
  OB_ASSERT(meta_addr.is_valid() && meta_addr.is_block());

  int ret = OB_SUCCESS;
  buf = nullptr;
  buf_len = 0;

  ObSharedObjectReadInfo read_info;
  read_info.addr_ = meta_addr;
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.ls_epoch_ = 0; /* ls_epoch for share storage */
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
  ObSharedObjectReadHandle io_handle(allocator);

  if (OB_FAIL(ObSharedObjectReaderWriter::async_read(read_info, io_handle))) {
    LOG_WARN("fail to async read", K(ret), K(read_info));
  } else if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait io_hanlde", K(ret), K(read_info));
  } else if (OB_FAIL(io_handle.get_data(allocator, buf, buf_len))) {
    LOG_WARN("fail to get data", K(ret), K(read_info));
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
