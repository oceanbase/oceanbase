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

#ifndef OCEANBASE_ENGINE_OB_ENGINE_OP_TRAITS_H_
#define OCEANBASE_ENGINE_OB_ENGINE_OP_TRAITS_H_

// helper template to get some operator of old && new engine.

namespace oceanbase
{
namespace sql
{
class ObOpSpec;
class ObTableScanSpec;
class ObMVTableScanSpec;
class ObTableInsertSpec;
class ObTableModifySpec;
class ObGranuleIteratorSpec;

template <bool NEW_ENG>
struct ObEngineOpTraits {};

template <>
struct ObEngineOpTraits<true>
{
  typedef ObOpSpec Root;
  typedef ObTableScanSpec TSC;
  typedef ObMVTableScanSpec MV_TSC;
  typedef ObTableModifySpec TableModify;
  typedef ObTableInsertSpec TableInsert;
  typedef ObGranuleIteratorSpec GI;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_ENGINE_OP_TRAITS_H_
