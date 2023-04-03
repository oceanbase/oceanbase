// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   xiaotao.ht <>

#pragma once

#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/timezone/ob_oracle_format_models.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTimeConverter
{
public:
  ObTableLoadTimeConverter();
  ~ObTableLoadTimeConverter();
  int init(const ObString &format);
  int str_to_datetime_oracle(const common::ObString &str, const common::ObTimeConvertCtx &cvrt_ctx,
                             common::ObDateTime &value) const;

private:
  int str_to_ob_time(const common::ObString &str, const common::ObTimeConvertCtx &cvrt_ctx,
                     const common::ObObjType target_type, common::ObTime &ob_time,
                     common::ObScale &scale) const;

private:
  common::ObSEArray<common::ObDFMElem, common::ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems_;
  common::ObFixedBitSet<OB_DEFAULT_BITSET_SIZE_FOR_DFM> elem_flags_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase