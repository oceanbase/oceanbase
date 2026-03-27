/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TABLE_DEFINE_H
#define _OB_TABLE_DEFINE_H 1
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace table
{
using common::ObString;
using common::ObRowkey;
using common::ObObj;
using common::ObIArray;
using common::ObSEArray;

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_DEFINE_H */
