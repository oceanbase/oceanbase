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

#ifndef OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_
#define OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_

#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_table_param.h"
namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
}
namespace compaction
{

class ObICompactionFilter
{
public:
  ObICompactionFilter()
  {
  }

  virtual ~ObICompactionFilter() {}
  // for statistics
  enum ObFilterRet
  {
    FILTER_RET_NOT_CHANGE = 0,
    FILTER_RET_REMOVE = 1,
    FILTER_RET_MAX = 2,
  };
  const static char *ObFilterRetStr[];
  const static char *get_filter_ret_str(const int64_t idx);
  static bool is_valid_filter_ret(const ObFilterRet filter_ret);

  struct ObFilterStatistics
  {
    ObFilterStatistics()
    {
      MEMSET(row_cnt_, 0, sizeof(row_cnt_));
    }
    ~ObFilterStatistics() {}
    void add(const ObFilterStatistics &other);
    void inc(ObFilterRet filter_ret);
    void reset();
    int64_t to_string(char *buf, const int64_t buf_len) const;
    void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
    int64_t row_cnt_[FILTER_RET_MAX];
  };

  enum CompactionFilterType : uint8_t
  {
    TX_DATA_MINOR,
    MDS_MINOR_FILTER_DATA,
    MDS_MINOR_CROSS_LS,
    MDS_IN_MEDIUM_INFO,
    REORG_INFO_MINOR,
    FILTER_TYPE_MAX
  };
  const static char *ObFilterTypeStr[];
  const static char *get_filter_type_str(const int64_t idx);

  // need be thread safe
  virtual int filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) = 0;
  virtual CompactionFilterType get_filter_type() const = 0;

  VIRTUAL_TO_STRING_KV("filter_type", get_filter_type_str(get_filter_type()));
};

struct ObCompactionFilterFactory final
{
public:
  template <typename T, typename... Args>
  static int alloc_compaction_filter(
    common::ObIAllocator &allocator,
    ObICompactionFilter *&compaction_filter,
    Args&... args)
  {
    compaction_filter = nullptr;
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    T *new_filter = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
    } else {
      new_filter = new (buf) T();
      if (OB_FAIL(new_filter->init(args...))) {
        STORAGE_LOG(WARN, "failed to init filter", K(ret));
        allocator.free(new_filter);
        new_filter = nullptr;
      } else {
        compaction_filter = new_filter;
      }
    }
    return ret;
  }
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_
