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

#ifndef __OCENABASE_ROOTSERVER_OB_ZONE_UNIT_PROVIDER_H__
#define __OCENABASE_ROOTSERVER_OB_ZONE_UNIT_PROVIDER_H__

#include "share/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "share/unit/ob_unit_info.h"
#include "common/ob_zone.h"
#include "ob_balance_info.h"

namespace oceanbase
{
namespace rootserver
{

class ObZoneUnitAdaptor
{
public:
  virtual const share::ObUnitInfo *at(int64_t idx) const = 0;
  virtual int64_t count() const = 0;
  virtual int get_target_unit_idx(
      const int64_t unit_offset,
      common::hash::ObHashSet<int64_t> &unit_set,
      const bool is_primary_part,
      int64_t &unit_idx) const = 0;
  virtual int update_tg_pg_count(
      const int64_t unit_idx,
      const bool is_primary_partition) = 0;
};

class ObZoneUnitsProvider
{
public:
  virtual int find_zone(const common::ObZone &zone, const ObZoneUnitAdaptor *&zua) = 0;
  virtual int get_all_zone_units(ZoneUnitArray& zone_unit) const = 0;
  virtual int get_all_ptr_zone_units(ZoneUnitPtrArray& ptr_zone_unit) const = 0;
  virtual int prepare_for_next_partition(const common::hash::ObHashSet<int64_t> &unit_set) {UNUSED(unit_set); return OB_NOT_SUPPORTED;}
  virtual bool exist(const ObZone &zone, const uint64_t unit_id) const { UNUSED(zone); UNUSED(unit_id); return false; }
};


class ObAliveZoneUnitAdaptor : public ObZoneUnitAdaptor
{
public:
  ObAliveZoneUnitAdaptor() {}
  void set_zone_unit(const UnitPtrArray *my_zu)
  {
    zu_ = my_zu;
  }
  virtual const share::ObUnitInfo *at(int64_t idx) const;
  virtual int64_t count() const;
  virtual int get_target_unit_idx(
      const int64_t unit_offset,
      common::hash::ObHashSet<int64_t> &unit_set,
      const bool is_primary_partition,
      int64_t &unit_idx) const override;
  virtual int update_tg_pg_count(
      const int64_t unit_idx,
      const bool is_primary_partition) override;
private:
  const UnitPtrArray *zu_;

  DISALLOW_COPY_AND_ASSIGN(ObAliveZoneUnitAdaptor);
};



class ObAliveZoneUnitsProvider : public ObZoneUnitsProvider
{
public:
  ObAliveZoneUnitsProvider()
    : all_zone_unit_ptrs_(),
      zone_unit_adaptor_(),
      inited_(false) {}
  virtual ~ObAliveZoneUnitsProvider() {}
  virtual int get_all_zone_units(ZoneUnitArray& zone_unit) const override;
  virtual int get_all_ptr_zone_units(ZoneUnitPtrArray& zone_unit_ptr) const override;
  virtual int find_zone(const common::ObZone &zone, const ObZoneUnitAdaptor *&zua) override;
public:
  int init(
      const ZoneUnitPtrArray &all_zone_units);
  virtual int prepare_for_next_partition(
      const common::hash::ObHashSet<int64_t> &unit_set) override;
private:
  class UnitSortOp final
  {
  public:
    UnitSortOp() : ret_(common::OB_SUCCESS) {}
    ~UnitSortOp() {}
    bool operator()(share::ObUnitInfo *left, share::ObUnitInfo *right);
    int get_ret() const { return ret_; }
  private:
    int ret_;
  };
  class ZoneUnitSortOp final
  {
  public:
    ZoneUnitSortOp() : ret_(common::OB_SUCCESS) {}
    ~ZoneUnitSortOp() {}
    bool operator()(UnitPtrArray &left, UnitPtrArray &right);
    int get_ret() const { return ret_; }
  private:
    int ret_;
  };
private:
  ZoneUnitPtrArray all_zone_unit_ptrs_;
  ZoneUnitPtrArray available_zone_unit_ptrs_;
  ObAliveZoneUnitAdaptor zone_unit_adaptor_;
  bool inited_;

  DISALLOW_COPY_AND_ASSIGN(ObAliveZoneUnitsProvider);
};


class ObAllZoneUnitAdaptor : public ObZoneUnitAdaptor
{
public:
  ObAllZoneUnitAdaptor() {}
  void set_zone_unit(const UnitStatArray *all_unit)
  {
    all_unit_ = all_unit;
  }
  virtual const share::ObUnitInfo *at(int64_t idx) const;
  virtual int64_t count() const;
  virtual int get_target_unit_idx(
      const int64_t unit_offset,
      common::hash::ObHashSet<int64_t> &unit_set,
      const bool is_primary_partition,
      int64_t &unit_idx) const override;
  virtual int update_tg_pg_count(
      const int64_t unit_idx,
      const bool is_primary_partition) override;
private:
  const UnitStatArray *all_unit_;

  DISALLOW_COPY_AND_ASSIGN(ObAllZoneUnitAdaptor);
};

/* for ObRereplication */
// only provide logonly unit
class ObZoneLogonlyUnitProvider : public ObZoneUnitsProvider
{
public:
  ObZoneLogonlyUnitProvider(const ZoneUnitArray &all_zone_units) :
      all_zone_units_(all_zone_units) {}
  virtual ~ObZoneLogonlyUnitProvider() {}
  virtual int find_zone(const common::ObZone &zone, const ObZoneUnitAdaptor *&zua);
  virtual int get_all_zone_units(ZoneUnitArray& zone_unit) const override;
  virtual int get_all_ptr_zone_units(ZoneUnitPtrArray& zone_unit_ptr) const override;
  virtual bool exist(const ObZone &zone, const uint64_t unit_id) const override;
private:
  const ZoneUnitArray &all_zone_units_;
  UnitStatArray all_unit_;
  ObAllZoneUnitAdaptor zone_unit_adaptor_;

  DISALLOW_COPY_AND_ASSIGN(ObZoneLogonlyUnitProvider);
};

/* for ObRereplication */
// filter the logonly unit
class ObZoneUnitsWithoutLogonlyProvider : public ObZoneUnitsProvider
{
public:
  ObZoneUnitsWithoutLogonlyProvider(const ZoneUnitArray &all_zone_units) :
      all_zone_units_(all_zone_units) {}
  virtual ~ObZoneUnitsWithoutLogonlyProvider() {}
  virtual int get_all_zone_units(ZoneUnitArray& zone_unit) const override;
  virtual int get_all_ptr_zone_units(ZoneUnitPtrArray& zone_unit_ptr) const override;
  virtual int find_zone(const common::ObZone &zone, const ObZoneUnitAdaptor *&zua);
private:
  const ZoneUnitArray &all_zone_units_;
  UnitStatArray all_unit_;
  ObAllZoneUnitAdaptor zone_unit_adaptor_;

  DISALLOW_COPY_AND_ASSIGN(ObZoneUnitsWithoutLogonlyProvider);
};

/* for ObRereplication */
class ObAllZoneUnitsProvider : public ObZoneUnitsProvider
{
public:
  ObAllZoneUnitsProvider(const ZoneUnitArray &all_zone_units) :
      all_zone_units_(all_zone_units) {}
  virtual ~ObAllZoneUnitsProvider() {}
  virtual int find_zone(const common::ObZone &zone, const ObZoneUnitAdaptor *&zua);
  virtual int get_all_zone_units(ZoneUnitArray& zone_unit) const override;
  virtual int get_all_ptr_zone_units(ZoneUnitPtrArray& zone_unit_ptr) const override;
private:
  const ZoneUnitArray &all_zone_units_;
  UnitStatArray all_unit_;
  ObAllZoneUnitAdaptor zone_unit_adaptor_;

  DISALLOW_COPY_AND_ASSIGN(ObAllZoneUnitsProvider);
};


}
}
#endif /* __OCENABASE_ROOTSERVER_OB_ZONE_UNIT_PROVIDER_H__ */
//// end of header file
