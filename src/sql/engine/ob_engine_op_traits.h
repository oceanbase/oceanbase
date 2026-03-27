/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
