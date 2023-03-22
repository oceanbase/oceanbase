/*
 * (C) Copyright 2022 Alipay Inc. All Rights Reserved.
 * Authors:
 *     Danling <>
 */

#ifndef OCEANBASE_SHARE_OB_TABLE_RANGE_H_
#define OCEANBASE_SHARE_OB_TABLE_RANGE_H_

#include "share/scn.h"

namespace oceanbase
{
namespace share
{
struct ObScnRange
{
  OB_UNIS_VERSION(1);
public:
  static const SCN MIN_SCN;
  static const SCN MAX_SCN;

  ObScnRange();
  int64_t hash() const;

  OB_INLINE void reset()
  {
    start_scn_ = MIN_SCN;
    end_scn_ = MIN_SCN;
  }

  OB_INLINE bool is_valid() const
  {
    return start_scn_.is_valid() && end_scn_.is_valid() && end_scn_ >= start_scn_;
  }

  OB_INLINE bool is_empty() const
  {
    return end_scn_ == start_scn_;
  }

  OB_INLINE bool operator == (const ObScnRange &range) const
  {
    return start_scn_ == range.start_scn_
        && end_scn_ == range.end_scn_;
  }

  OB_INLINE bool operator != (const ObScnRange &range) const
  {
    return !this->operator==(range);
  }

  OB_INLINE bool contain(const SCN &scn) const
  {
    return is_valid() && start_scn_ < scn
      && end_scn_ >= scn;
  }
  TO_STRING_KV(K_(start_scn), K_(end_scn));

public:
  SCN start_scn_;
  SCN end_scn_;
};


} //namespace share
} //namespace oceanbase

#endif //OCEANBASE_SHARE_OB_TABLE_RANGE_H_
