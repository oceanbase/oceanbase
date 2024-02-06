// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_LOG_FETCHER_DATA_DICT_IN_LOG_TABLE_H_
#define OCEANBASE_LOG_FETCHER_DATA_DICT_IN_LOG_TABLE_H_

#include "logservice/palf/lsn.h"

namespace oceanbase
{
namespace logfetcher
{
// About OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_TNAME
struct DataDictionaryInLogInfo
{
  DataDictionaryInLogInfo() { reset(); }

  bool is_valid() const
  {
    return common::OB_INVALID_TIMESTAMP != snapshot_scn_
      && start_lsn_.is_valid()
      && end_lsn_.is_valid();
  }

  void reset()
  {
    snapshot_scn_ = common::OB_INVALID_TIMESTAMP;
    start_lsn_.reset();
    end_lsn_.reset();
  }

  void reset(
      const int64_t snapshot_scn,
      const palf::LSN &start_lsn,
      const palf::LSN &end_lsn)
  {
    snapshot_scn_ = snapshot_scn;
    start_lsn_ = start_lsn;
    end_lsn_ = end_lsn;
  }

  DataDictionaryInLogInfo &operator=(const DataDictionaryInLogInfo &other)
  {
    snapshot_scn_ = other.snapshot_scn_;
    start_lsn_ = other.start_lsn_;
    end_lsn_ = other.end_lsn_;
    return *this;
  }

  TO_STRING_KV(
      K_(snapshot_scn),
      K_(start_lsn),
      K_(end_lsn));

  int64_t snapshot_scn_;
  palf::LSN start_lsn_;
  palf::LSN end_lsn_;
};

} // namespace logfetcher
} // namespace oceanbase

#endif
