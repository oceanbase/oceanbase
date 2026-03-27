// Copyright (c) 2022 OceanBase
// SPDX-License-Identifier: Apache-2.0

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
      && common::OB_INVALID_TIMESTAMP != end_scn_
      && start_lsn_.is_valid()
      && end_lsn_.is_valid();
  }

  void reset()
  {
    snapshot_scn_ = common::OB_INVALID_TIMESTAMP;
    end_scn_ = common::OB_INVALID_TIMESTAMP;
    start_lsn_.reset();
    end_lsn_.reset();
  }

  void reset(
      const int64_t snapshot_scn,
      const int64_t end_scn,
      const palf::LSN &start_lsn,
      const palf::LSN &end_lsn)
  {
    snapshot_scn_ = snapshot_scn;
    end_scn_ = end_scn;
    start_lsn_ = start_lsn;
    end_lsn_ = end_lsn;
  }

  DataDictionaryInLogInfo &operator=(const DataDictionaryInLogInfo &other)
  {
    snapshot_scn_ = other.snapshot_scn_;
    end_scn_ = other.end_scn_;
    start_lsn_ = other.start_lsn_;
    end_lsn_ = other.end_lsn_;
    return *this;
  }

  TO_STRING_KV(
      K_(snapshot_scn),
      K_(end_scn),
      K_(start_lsn),
      K_(end_lsn));

  int64_t snapshot_scn_;
  int64_t end_scn_;
  palf::LSN start_lsn_;
  palf::LSN end_lsn_;
};

} // namespace logfetcher
} // namespace oceanbase

#endif
