/**
 * Copyright (c) 2023 OceanBase
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

#include "storage/meta_store/ob_storage_meta_io_util.h"
namespace oceanbase
{
namespace storage
{
#ifdef OB_BUILD_SHARED_STORAGE
int ObStorageMetaIOUtil::check_meta_existence(
    const blocksstable::ObStorageObjectOpt &opt, const int64_t ls_epoch, bool &is_exist)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId object_id;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
    LOG_WARN("fail to get object id", K(ret), K(opt));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_is_exist_object(object_id, ls_epoch, is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(object_id));
  }
  return ret;
}

#endif

} // namespace storage
} // namespace oceanbase