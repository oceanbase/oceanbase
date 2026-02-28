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
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
namespace oceanbase
{
using namespace storage;
using namespace share;
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
  if (!mds_filter_info.truncate_info_keys_.is_empty()
      && OB_FAIL(truncate_info_keys_.assign(allocator, mds_filter_info.truncate_info_keys_))) {
    LOG_WARN("failed to assign truncate info keys", KR(ret), K(mds_filter_info));
  } else if (!mds_filter_info.ttl_filter_info_keys_.is_empty()
      && OB_FAIL(ttl_filter_info_keys_.assign(allocator, mds_filter_info.ttl_filter_info_keys_))) {
    LOG_WARN("failed to assign ttl filter info keys", KR(ret), K(mds_filter_info));
  } else {
    info_ = mds_filter_info.info_;
    mlog_purge_scn_ = mds_filter_info.mlog_purge_scn_;
  }
  return ret;
}

void ObMdsFilterInfo::destroy(ObIAllocator &allocator)
{
  info_ = 0;
  truncate_info_keys_.destroy(allocator);
  ttl_filter_info_keys_.destroy(allocator);
  mlog_purge_scn_ = 0;
}

int ObMdsFilterInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const // FARM COMPAT WHITELIST
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not serialize empty mds filter info", KR(ret), KPC(this));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, info_);
    if (FAILEDx((truncate_info_keys_.serialize(buf, buf_len, pos)))) {
      LOG_WARN("failed to serialize truncate info keys", KR(ret), K_(truncate_info_keys));
    }
    if (OB_SUCC(ret) && version_ >= MDS_FILTER_INFO_VERSION_V2) {
      if (OB_FAIL(ttl_filter_info_keys_.serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize ttl filter info keys", KR(ret), K_(ttl_filter_info_keys));
      } else {
        LST_DO_CODE(OB_UNIS_ENCODE, mlog_purge_scn_);
      }
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
    LOG_WARN("invalid argument", KR(ret), K(buf), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, info_);
    if (FAILEDx(truncate_info_keys_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize truncate info keys", KR(ret), K_(truncate_info_keys));
    }
    if (OB_SUCC(ret) && version_ >= MDS_FILTER_INFO_VERSION_V2) {
      if (OB_FAIL(ttl_filter_info_keys_.deserialize(allocator, buf, data_len, pos))) {
        LOG_WARN("failed to deserialize ttl filter info keys", KR(ret), K_(ttl_filter_info_keys));
      } else {
        LST_DO_CODE(OB_UNIS_DECODE, mlog_purge_scn_);
      }
    }
  }
  return ret;
}

int64_t ObMdsFilterInfo::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, info_);
  len += truncate_info_keys_.get_serialize_size();
  if (version_ >= MDS_FILTER_INFO_VERSION_V2) {
    len += ttl_filter_info_keys_.get_serialize_size();
    LST_DO_CODE(OB_UNIS_ADD_LEN, mlog_purge_scn_);
  }
  return len;
}

int ObMdsFilterInfo::init(
    ObIAllocator &allocator,
    const uint64_t tenant_data_version,
    const ObMdsInfoDistinctMgr &mds_info_mgr)
{
  int ret = OB_SUCCESS;
  if (tenant_data_version >= ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION) {
    version_ = MDS_FILTER_INFO_VERSION_V2;
  } else if (OB_UNLIKELY(!mds_info_mgr.get_ttl_filter_info_distinct_mgr().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ttl filter info distinct mgr is not empty", KR(ret), K(mds_info_mgr));
  } else {
    version_ = MDS_FILTER_INFO_VERSION_V1;
  }
  if (FAILEDx(mds_info_mgr.fill_mds_filter_info(
      allocator,
      *this))) {
    LOG_WARN("failed to fill mds filter info", KR(ret), K(mds_info_mgr));
  }
  return ret;
}

int ObMdsFilterInfo::init(
  const uint64_t tenant_data_version,
  const int64_t mlog_purge_scn)
{
  int ret = OB_SUCCESS;
  if (tenant_data_version < ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support mlog purge scn for tenant data version", KR(ret), K(tenant_data_version));
  } else if (OB_UNLIKELY(mlog_purge_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mlog purge scn", KR(ret), K(mlog_purge_scn));
  } else {
    mlog_purge_scn_ = mlog_purge_scn;
    version_ = MDS_FILTER_INFO_VERSION_V2;
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
