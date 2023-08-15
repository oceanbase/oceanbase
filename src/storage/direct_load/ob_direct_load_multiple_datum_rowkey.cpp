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

#include "storage/direct_load/ob_direct_load_multiple_datum_rowkey.h"
#include "observer/table_load/ob_table_load_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObDirectLoadMultipleDatumRowkey::ObDirectLoadMultipleDatumRowkey()
{
}

ObDirectLoadMultipleDatumRowkey::~ObDirectLoadMultipleDatumRowkey()
{
}

void ObDirectLoadMultipleDatumRowkey::reset()
{
  tablet_id_.reset();
  datum_array_.reset();
}

void ObDirectLoadMultipleDatumRowkey::reuse()
{
  tablet_id_.reset();
  datum_array_.reuse();
}

int64_t ObDirectLoadMultipleDatumRowkey::get_deep_copy_size() const
{
  int64_t size = 0;
  size += datum_array_.get_deep_copy_size();
  return size;
}

int ObDirectLoadMultipleDatumRowkey::deep_copy(const ObDirectLoadMultipleDatumRowkey &src,
                                               char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = src.get_deep_copy_size();
  if (OB_UNLIKELY(!src.is_valid() || len - pos < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src), K(len), K(deep_copy_size));
  } else {
    reuse();
    tablet_id_ = src.tablet_id_;
    if (OB_FAIL(datum_array_.deep_copy(src.datum_array_, buf, len, pos))) {
      LOG_WARN("fail to deep copy datum array", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMultipleDatumRowkey::deep_copy(const ObDirectLoadMultipleDatumRowkey &src,
                                               ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src));
  } else {
    const int64_t deep_copy_size = src.get_deep_copy_size();
    char *buf = nullptr;
    int64_t pos = 0;
    if (deep_copy_size > 0 &&
        OB_ISNULL(buf = static_cast<char *>(allocator.alloc(deep_copy_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(deep_copy_size));
    } else if (OB_FAIL(deep_copy(src, buf, deep_copy_size, pos))) {
      LOG_WARN("fail to deep copy", KR(ret));
    }
  }

  return ret;
}

int ObDirectLoadMultipleDatumRowkey::assign(const ObTabletID &tablet_id, ObStorageDatum *datums,
                                            int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || nullptr == datums || count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), KP(datums), K(count));
  } else {
    reset();
    tablet_id_ = tablet_id;
    if (OB_FAIL(datum_array_.assign(datums, count))) {
      LOG_WARN("fail to assign datum array", KR(ret));
    }
  }
  return ret;
}

bool ObDirectLoadMultipleDatumRowkey::is_valid() const {
  return is_min_rowkey() || is_max_rowkey() || (datum_array_.is_valid() && datum_array_.count_ > 0);
}

int ObDirectLoadMultipleDatumRowkey::get_rowkey(ObDatumRowkey &rowkey) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || 0 == datum_array_.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid rowkey", KR(ret), KPC(this));
  } else if (OB_FAIL(rowkey.assign(datum_array_.datums_, datum_array_.count_))) {
    LOG_WARN("fail to assign rowkey", KR(ret));
  }
  return ret;
}

int ObDirectLoadMultipleDatumRowkey::compare(const ObDirectLoadMultipleDatumRowkey &rhs,
                                             const blocksstable::ObStorageDatumUtils &datum_utils,
                                             int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(*this), K(rhs));
  } else {
    cmp_ret = tablet_id_.compare(rhs.tablet_id_);
    if (0 == cmp_ret && (!is_min_rowkey() && !is_max_rowkey())) {
      ObDatumRowkey lhs_rowkey;
      ObDatumRowkey rhs_rowkey;
      if (OB_FAIL(get_rowkey(lhs_rowkey))) {
        LOG_WARN("fail to assign rowkey", KR(ret), KPC(this));
      } else if (OB_FAIL(rhs.get_rowkey(rhs_rowkey))) {
        LOG_WARN("fail to assign rowkey", KR(ret), K(rhs));
      } else if (OB_FAIL(lhs_rowkey.compare(rhs_rowkey, datum_utils, cmp_ret))) {
        LOG_WARN("fail to compare rowkey", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleDatumRowkey::set_tablet_min_rowkey(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else {
    reset();
    const ObDatumRowkey &min_rowkey = ObDatumRowkey::MIN_ROWKEY;
    if (OB_FAIL(datum_array_.assign(min_rowkey.datums_, min_rowkey.datum_cnt_))) {
      LOG_WARN("fail to assign datum array", KR(ret));
    } else {
      tablet_id_ = tablet_id;
    }
  }
  return ret;
}

int ObDirectLoadMultipleDatumRowkey::set_tablet_max_rowkey(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else {
    reset();
    const ObDatumRowkey &max_rowkey = ObDatumRowkey::MAX_ROWKEY;
    if (OB_FAIL(datum_array_.assign(max_rowkey.datums_, max_rowkey.datum_cnt_))) {
      LOG_WARN("fail to assign datum array", KR(ret));
    } else {
      tablet_id_ = tablet_id;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIMPLE(ObDirectLoadMultipleDatumRowkey)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tablet_id_.id(), datum_array_);
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObDirectLoadMultipleDatumRowkey)
{
  int ret = OB_SUCCESS;
  reuse();
  uint64_t id = 0;
  LST_DO_CODE(OB_UNIS_DECODE, id, datum_array_);
  if (OB_SUCC(ret)) {
    tablet_id_ = id;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObDirectLoadMultipleDatumRowkey)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tablet_id_.id(), datum_array_);
  return len;
}

/**
 * ObDirectLoadMultipleDatumRowkeyCompare
 */

ObDirectLoadMultipleDatumRowkeyCompare::ObDirectLoadMultipleDatumRowkeyCompare()
  : datum_utils_(nullptr), result_code_(OB_SUCCESS)
{
}

int ObDirectLoadMultipleDatumRowkeyCompare::init(const ObStorageDatumUtils &datum_utils)
{
  int ret = OB_SUCCESS;
  datum_utils_ = &datum_utils;
  return ret;
}

bool ObDirectLoadMultipleDatumRowkeyCompare::operator()(const ObDirectLoadMultipleDatumRowkey *lhs,
                                                        const ObDirectLoadMultipleDatumRowkey *rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_ISNULL(datum_utils_) || OB_ISNULL(lhs) || OB_ISNULL(rhs) ||
      OB_UNLIKELY(!lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datum_utils_), KP(lhs), KP(rhs));
  } else {
    if (OB_FAIL(lhs->compare(*rhs, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare rowkey", KR(ret), KP(lhs), K(rhs), K(datum_utils_));
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

} // namespace storage
} // namespace oceanbase
