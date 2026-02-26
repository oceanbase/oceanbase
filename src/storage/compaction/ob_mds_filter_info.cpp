//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_mds_filter_info.h"
namespace oceanbase
{
using namespace storage;
namespace compaction
{
const static char * MdsFilterInfoTypeStr[] = {
    "TRUNCATE_INFO",
    "TTL_FILTER_INFO",
    "MDS_FILTER_TYPE_MAX"
};

const char *ObMdsFilterInfo::filter_info_type_to_str(const MdsFilterInfoType &type)
{
  STATIC_ASSERT(static_cast<int64_t>(MDS_FILTER_TYPE_MAX + 1) == ARRAYSIZEOF(MdsFilterInfoTypeStr), "mds filter info str len is mismatch");
  const char *str = "";
  if (is_valid_filter_info_type(type)) {
    str = MdsFilterInfoTypeStr[type];
  } else {
    str = "INVALID_PART_TYPE";
  }
  return str;
}

int ObMdsFilterInfo::assign(ObIAllocator &allocator, const ObMdsFilterInfo &mds_filter_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(truncate_info_keys_.assign(allocator, mds_filter_info.truncate_info_keys_))) {
    LOG_WARN("failed to assign truncate info keys", KR(ret), K(mds_filter_info));
  } else {
    info_ = mds_filter_info.info_;
  }
  return ret;
}

void ObMdsFilterInfo::destroy(ObIAllocator &allocator)
{
  info_ = 0;
  truncate_info_keys_.destroy(allocator);
}

int ObMdsFilterInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not serialize empty mds filter info", KR(ret), KPC(this));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, info_);
    if (FAILEDx((truncate_info_keys_.serialize(buf, buf_len, pos)))) {
      LOG_WARN("failed to serialize truncate info keys", KR(ret), K_(truncate_info_keys));
    }
  }
  return ret;
}

int ObMdsFilterInfo::deserialize(
      ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, info_);
    if (FAILEDx(truncate_info_keys_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize truncate info keys", KR(ret), K_(truncate_info_keys));
    }
  }
  return ret;
}

int64_t ObMdsFilterInfo::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, info_);
  len += truncate_info_keys_.get_serialize_size();
  return len;
}

} // namespace compaction
} // namespace oceanbase
