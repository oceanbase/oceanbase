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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_TRIGGER_INFO_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_TRIGGER_INFO_H_
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_trigger_mgr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObTriggerEvents
{
#define TE_INSERT (common::ObDmlEventType::DE_INSERTING)
#define TE_UPDATE (common::ObDmlEventType::DE_UPDATING)
#define TE_DELETE (common::ObDmlEventType::DE_DELETING)
public:
  ObTriggerEvents()
    : bit_value_(0)
  {}
  OB_INLINE void reset() { set_value(0); }
  OB_INLINE void set_value(uint64_t value) { bit_value_ = value; }
  OB_INLINE void add_insert_event() { bit_value_ |= TE_INSERT; }
  OB_INLINE void add_update_event() { bit_value_ |= TE_UPDATE; }
  OB_INLINE void add_delete_event() { bit_value_ |= TE_DELETE; }
  OB_INLINE uint64_t get_value() const { return bit_value_; }
  OB_INLINE bool has_insert_event() const { return has_insert_event(bit_value_); }
  OB_INLINE bool has_update_event() const { return has_update_event(bit_value_); }
  OB_INLINE bool has_delete_event() const { return has_delete_event(bit_value_); }
  OB_INLINE bool has_value(uint64_t value) const { return 0 != (bit_value_ & value); }
  OB_INLINE static uint64_t get_insert_event() { return TE_INSERT; }
  OB_INLINE static uint64_t get_update_event() { return TE_UPDATE; }
  OB_INLINE static uint64_t get_delete_event() { return TE_DELETE; }
  OB_INLINE static uint64_t get_all_event() { return (TE_INSERT | TE_UPDATE | TE_DELETE); }
  // mysql insert duplicate
  OB_INLINE static bool has_insert_event(uint64_t value) { return 0 != (value & TE_INSERT); }
  OB_INLINE static bool has_update_event(uint64_t value) { return 0 != (value & TE_UPDATE); }
  OB_INLINE static bool has_delete_event(uint64_t value) { return 0 != (value & TE_DELETE); }
  OB_INLINE static bool is_insert_event(uint64_t value) { return TE_INSERT == value; }
  OB_INLINE static bool is_update_event(uint64_t value) { return TE_UPDATE == value; }
  OB_INLINE static bool is_delete_event(uint64_t value) { return TE_DELETE == value; }
public:
  uint64_t bit_value_;
#undef TE_INSERT
#undef TE_UPDATE
#undef TE_DELETE
};

class ObTimingPoints
{
public:
  ObTimingPoints()
    : bit_value_(0)
  {}
  OB_INLINE void reset() { set_value(0); }
  OB_INLINE void merge(const ObTimingPoints &other) { bit_value_ |= other.get_value(); }
  OB_INLINE void set_value(uint64_t value) { bit_value_ = value; }
  OB_INLINE void add_before_stmt() { before_stmt_ = 1; }
  OB_INLINE void add_before_row() { before_row_ = 1; }
  OB_INLINE void add_after_row() { after_row_ = 1; }
  OB_INLINE void add_after_stmt() { after_stmt_ = 1; }
  OB_INLINE void add_instead_row() { instead_row_ = 1; }
  OB_INLINE void add_when_condition() { when_condition_ = 1; }
  OB_INLINE uint64_t get_value() const { return bit_value_; }
  OB_INLINE bool has_before_stmt() const { return 1 == before_stmt_; }
  OB_INLINE bool has_before_row() const { return 1 == before_row_; }
  OB_INLINE bool has_after_row() const { return 1 == after_row_; }
  OB_INLINE bool has_after_stmt() const { return 1 == after_stmt_; }
  OB_INLINE bool has_instead_row() const { return 1 == instead_row_; }
  OB_INLINE bool has_when_condition() const { return 1 == when_condition_; }
  OB_INLINE bool has_before_point() const { return 1 == before_stmt_ || 1 == before_row_; }
  OB_INLINE bool has_after_point() const { return 1 == after_stmt_ || 1 == after_row_; }
  OB_INLINE bool has_stmt_point() const { return 1 == before_stmt_ || 1 == after_stmt_; }
  OB_INLINE bool has_row_point() const { return 1 == before_row_ || 1 == after_row_ || 1 == instead_row_; }
  OB_INLINE bool only_before_row() const { return 1 == before_row_ && 0 == after_row_
                                           && 0 == before_stmt_ && 0 == after_stmt_ && 0 == instead_row_; }
  OB_INLINE bool only_after_row() const { return 0 == before_row_ && 1 == after_row_
                                           && 0 == before_stmt_ && 0 == after_stmt_ && 0 == instead_row_; }
  OB_INLINE bool only_before_stmt() const { return 0 == before_row_ && 0 == after_row_
                                            && 1 == before_stmt_ && 0 == after_stmt_ && 0 == instead_row_; }
  OB_INLINE bool only_after_stmt() const { return 0 == before_row_ && 0 == after_row_
                                           && 0 == before_stmt_ && 1 == after_stmt_ && 0 == instead_row_; }
public:
  union
  {
    uint64_t bit_value_;
    struct {
      uint64_t when_condition_:1;
      uint64_t before_stmt_:1;
      uint64_t before_row_:1;
      uint64_t after_row_:1;
      uint64_t after_stmt_:1;
      uint64_t instead_row_:1;
      uint64_t reserved_:58;
    };
  };
};

class ObTriggerFlags
{
public:
  ObTriggerFlags()
    : bit_value_(0)
  {}
  OB_INLINE void reset() { set_value(0); }
  OB_INLINE void set_value(uint64_t value) { bit_value_ = value; }
  OB_INLINE void set_enable() { enable_ = 1; }
  OB_INLINE void set_disable() { enable_ = 0; }
  OB_INLINE uint64_t get_value() const { return bit_value_; }
  OB_INLINE bool is_enable() const { return 1 == enable_; }
public:
  union
  {
    uint64_t bit_value_;
    struct {
      uint64_t enable_:1;
      uint64_t reserved_:63;
    };
  };
};

enum TgTimingEvent
{
  TG_TIMING_EVENT_INVALID = -1,
  TG_BEFORE_INSERT = 0,
  TG_BEFORE_UPDATE,
  TG_BEFORE_DELETE,
  TG_AFTER_INSERT,
  TG_AFTER_UPDATE,
  TG_AFTER_DELETE
};

class ObTriggerInfo : public ObSimpleTriggerSchema
{
  OB_UNIS_VERSION(1);
public:
  enum TriggerType
  {
    TT_INVALID = 0,
    TT_SIMPLE_DML,
    TT_COMPOUND_DML,
    TT_INSTEAD_DML,
    TT_SYSTEM,
  };
  enum ReferenceType
  {
    RT_OLD = 0,
    RT_NEW,
    RT_PARENT,
    RT_MAX,
  };
  enum PackageSouceType
  {
    SPEC_AND_BODY = 0,
    SPEC_ONLY,
    BODY_ONLY
  };
  enum OrderType
  {
    OT_INVALID = 0,
    OT_FOLLOWS,
    OT_PRECEDES,
  };

  class TriggerContext
  {
  public:
    void dispatch_decalare_execute(const ObTriggerInfo &trigger_info,
                                   common::ObString *&simple_declare,
                                   common::ObString *&simple_execute,
                                   common::ObString *&tg_body);
    common::ObString before_stmt_declare_;
    common::ObString before_stmt_execute_;
    common::ObString before_row_declare_;
    common::ObString before_row_execute_;
    common::ObString after_row_declare_;
    common::ObString after_row_execute_;
    common::ObString after_stmt_declare_;
    common::ObString after_stmt_execute_;
    common::ObString trigger_body_; // for mysql trigger
    common::ObString compound_declare_; // only for compound trigger
  };

  struct ActionOrderComparator
  {
    ActionOrderComparator() : ret_(common::OB_SUCCESS) {}
    bool operator() (const ObTriggerInfo *left, const ObTriggerInfo *right);
    int get_ret() const { return ret_; }
    int ret_;
  };

public:
  ObTriggerInfo()
  {
    reset();
  }
  explicit ObTriggerInfo(common::ObIAllocator *allocator)
    : ObSimpleTriggerSchema(allocator),
      package_spec_info_(allocator),
      package_body_info_(allocator)
  {
    reset();
  }
  ObTriggerInfo(const ObTriggerInfo &trigger_info)
    : ObSimpleTriggerSchema(trigger_info)
  {
    reset();
    *this = trigger_info;
  }
  virtual ~ObTriggerInfo() {}
  ObTriggerInfo &operator=(const ObTriggerInfo &trigger_info);
  int assign(const ObTriggerInfo &other);

  virtual void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    package_spec_info_.set_tenant_id(tenant_id);
    package_body_info_.set_tenant_id(tenant_id);
  }
  virtual void set_trigger_id(uint64_t trigger_id)
  {
    trigger_id_ = trigger_id;
    package_spec_info_.set_package_id(get_trigger_spec_package_id(trigger_id));
    package_body_info_.set_package_id(get_trigger_body_package_id(trigger_id));
  }
  OB_INLINE void set_owner_id(uint64_t owner_id)
  {
    owner_id_ = owner_id;
    package_spec_info_.set_owner_id(owner_id);
    package_body_info_.set_owner_id(owner_id);
  }
  virtual void set_database_id(uint64_t database_id)
  {
    database_id_ = database_id;
    package_spec_info_.set_database_id(database_id);
    package_body_info_.set_database_id(database_id);
  }
  virtual void set_schema_version(int64_t schema_version)
  {
    schema_version_ = schema_version;
    package_spec_info_.set_schema_version(schema_version);
    package_body_info_.set_schema_version(schema_version);
  }
  OB_INLINE void set_base_object_id(uint64_t object_id) { base_object_id_ = object_id; }
  OB_INLINE void set_base_object_type(int64_t object_type) { base_object_type_ = static_cast<ObSchemaType>(object_type); }
  OB_INLINE void set_trigger_type(int64_t trigger_type)
  {
    trigger_type_ = static_cast<TriggerType>(trigger_type);
    package_spec_info_.set_type(PACKAGE_TYPE);
    package_body_info_.set_type(PACKAGE_BODY_TYPE);
  }
  OB_INLINE void set_simple_dml_type() { trigger_type_ = TT_SIMPLE_DML; }
  OB_INLINE void set_compound_dml_type() { trigger_type_ = TT_COMPOUND_DML; }
  OB_INLINE void set_instead_dml_type() { trigger_type_ = TT_INSTEAD_DML; }
  OB_INLINE void set_system_type() { trigger_type_ = TT_SYSTEM; }
  OB_INLINE void set_trigger_events(uint64_t value) { trigger_events_.set_value(value); }
  OB_INLINE void add_insert_event() { trigger_events_.add_insert_event(); }
  OB_INLINE void add_update_event() { trigger_events_.add_update_event(); }
  OB_INLINE void add_delete_event() { trigger_events_.add_delete_event(); }
  OB_INLINE void set_timing_points(uint64_t value) { timing_points_.set_value(value); }
  OB_INLINE void add_before_stmt() { timing_points_.add_before_stmt(); }
  OB_INLINE void add_after_stmt() { timing_points_.add_after_stmt(); }
  OB_INLINE void add_before_row() { timing_points_.add_before_row(); }
  OB_INLINE void add_after_row() { timing_points_.add_after_row(); }
  OB_INLINE void add_instead_row() { timing_points_.add_instead_row(); }
  OB_INLINE void set_trigger_flags(uint64_t value) { trigger_flags_.set_value(value); }
  OB_INLINE void set_enable() { trigger_flags_.set_enable(); }
  OB_INLINE void set_disable() { trigger_flags_.set_disable(); }
  OB_INLINE void set_is_enable(bool flag) {
    if (flag) {
      trigger_flags_.set_enable();
    } else {
      trigger_flags_.set_disable();
    }
  }
  virtual int set_trigger_name(const common::ObString &trigger_name)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(deep_copy_str(trigger_name, trigger_name_))) {
      SHARE_SCHEMA_LOG(WARN, "failed to deep copy trigger name", K(ret), K(trigger_name));
    } else if (OB_FAIL(package_spec_info_.set_package_name(trigger_name))) {
      SHARE_SCHEMA_LOG(WARN, "failed to set package name", K(ret), K(trigger_name));
    } else if (OB_FAIL(package_body_info_.set_package_name(trigger_name))) {
      SHARE_SCHEMA_LOG(WARN, "failed to set package name", K(ret), K(trigger_name));
    }
    return ret;
  }
  OB_INLINE int set_update_columns(const common::ObString &update_columns)
  { return deep_copy_str(update_columns, update_columns_); }
  OB_INLINE void assign_update_columns(char *columns_buf, int64_t buf_len)
  { update_columns_.assign_ptr(columns_buf, static_cast<int32_t>(buf_len)); }
  OB_INLINE int set_ref_old_name(const common::ObString &ref_name)
  { return deep_copy_str(ref_name, reference_names_[RT_OLD]); }
  OB_INLINE int set_ref_new_name(const common::ObString &ref_name)
  { return deep_copy_str(ref_name, reference_names_[RT_NEW]); }
  OB_INLINE int set_ref_parent_name(const common::ObString &ref_name)
  { return deep_copy_str(ref_name, reference_names_[RT_PARENT]); }
  OB_INLINE int set_when_condition(const common::ObString &when_condition)
  {
    int ret = common::OB_SUCCESS;
    if (!when_condition.empty()) {
      timing_points_.add_when_condition();
      ret = deep_copy_str(when_condition, when_condition_);
    }
    return ret;
  }
  OB_INLINE int set_trigger_body(const common::ObString &trigger_body)
  { return deep_copy_str(trigger_body, trigger_body_); }
  OB_INLINE int set_trigger_priv_user(const common::ObString &priv_user)
  { return deep_copy_str(priv_user, priv_user_); }
  OB_INLINE int set_package_spec_source(const common::ObString &source)
  { return package_spec_info_.set_source(source); }
  OB_INLINE int set_package_body_source(const common::ObString &source)
  { return package_body_info_.set_source(source); }
  OB_INLINE void set_package_flag(int64_t flag)
  {
    package_spec_info_.set_flag(flag);
    package_body_info_.set_flag(flag);
  }
  OB_INLINE void set_package_comp_flag(int64_t comp_flag)
  {
    package_spec_info_.set_comp_flag(comp_flag);
    package_body_info_.set_comp_flag(comp_flag);
  }
  OB_INLINE int set_package_exec_env(const common::ObString &exec_env)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(package_spec_info_.set_exec_env(exec_env))) {
      SHARE_SCHEMA_LOG(WARN, "failed to set package exec env", K(ret), K(exec_env));
    } else if (OB_FAIL(package_body_info_.set_exec_env(exec_env))) {
      SHARE_SCHEMA_LOG(WARN, "failed to set package exec env", K(ret), K(exec_env));
    }
    return ret;
  }
  OB_INLINE void set_sql_mode(uint64_t sql_mode) { sql_mode_ = sql_mode; }
  OB_INLINE void set_order_type(OrderType order_type) { order_type_ = order_type; }
  OB_INLINE void set_order_type(int64_t order_type) { order_type_ = static_cast<OrderType>(order_type); }
  OB_INLINE int set_ref_trg_db_name(const common::ObString &ref_trg_db_name)
  { return deep_copy_str(ref_trg_db_name, ref_trg_db_name_); }
  OB_INLINE int set_ref_trg_name(const common::ObString &ref_trg_name)
  { return deep_copy_str(ref_trg_name, ref_trg_name_); }
  OB_INLINE void set_action_order(int64_t action_order) { action_order_ = action_order; }
//OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
//OB_INLINE uint64_t get_trigger_id() const { return trigger_id_; }
  OB_INLINE uint64_t get_owner_id() const { return owner_id_; }
//OB_INLINE uint64_t get_database_id() const { return database_id_; }
//OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE uint64_t get_base_object_id() const { return base_object_id_; }
  OB_INLINE uint64_t get_data_table_id() const { return get_base_object_id(); }
  OB_INLINE int64_t get_base_object_type() const { return static_cast<int64_t>(base_object_type_); }
  OB_INLINE bool is_based_on_table() const { return TABLE_SCHEMA == base_object_type_; }
  OB_INLINE int64_t get_trigger_type() const { return static_cast<int64_t>(trigger_type_); }
  OB_INLINE bool is_simple_dml_type() const { return TT_SIMPLE_DML == trigger_type_; }
  OB_INLINE bool is_compound_dml_type() const { return TT_COMPOUND_DML == trigger_type_; }
  OB_INLINE bool is_instead_dml_type() const { return TT_INSTEAD_DML == trigger_type_; }
  OB_INLINE bool is_system_type() const { return TT_SYSTEM == trigger_type_; }
  OB_INLINE uint64_t get_trigger_events() const { return trigger_events_.get_value(); }
  OB_INLINE bool has_insert_event() const { return trigger_events_.has_insert_event(); }
  OB_INLINE bool has_update_event() const { return trigger_events_.has_update_event(); }
  OB_INLINE bool has_delete_event() const { return trigger_events_.has_delete_event(); }
  OB_INLINE uint64_t get_timing_points() const { return timing_points_.get_value(); }
  OB_INLINE bool has_before_stmt_point() const { return timing_points_.has_before_stmt(); }
  OB_INLINE bool has_after_stmt_point() const { return timing_points_.has_after_stmt(); }
  OB_INLINE bool has_before_row_point() const { return timing_points_.has_before_row(); }
  OB_INLINE bool has_after_row_point() const { return timing_points_.has_after_row(); }
  OB_INLINE bool has_instead_row() const { return timing_points_.has_instead_row(); }
  OB_INLINE bool has_when_condition() const { return timing_points_.has_when_condition(); }
  OB_INLINE bool has_before_point() const { return timing_points_.has_before_point(); }
  OB_INLINE bool has_after_point() const { return timing_points_.has_after_point(); }
  OB_INLINE bool has_stmt_point() const { return timing_points_.has_stmt_point(); }
  OB_INLINE bool has_row_point() const { return timing_points_.has_row_point(); }
  OB_INLINE uint64_t get_trigger_flags() const { return trigger_flags_.get_value(); }
  OB_INLINE bool is_enable() const { return trigger_flags_.is_enable(); }
//OB_INLINE const common::ObString &get_trigger_name() const { return trigger_name_; }
  OB_INLINE const common::ObString &get_update_columns() const { return update_columns_; }
  OB_INLINE const common::ObString &get_ref_old_name() const { return reference_names_[RT_OLD]; }
  OB_INLINE const common::ObString &get_ref_new_name() const { return reference_names_[RT_NEW]; }
  OB_INLINE const common::ObString &get_ref_parent_name() const { return reference_names_[RT_PARENT]; }
  OB_INLINE const common::ObString &get_when_condition() const { return when_condition_; }
  OB_INLINE const common::ObString &get_trigger_body() const { return trigger_body_; }
  OB_INLINE const common::ObString &get_trigger_priv_user() const { return priv_user_; }
  OB_INLINE const common::ObString &get_package_spec_source() const { return package_spec_info_.get_source(); }
  OB_INLINE const ObPackageInfo &get_package_spec_info() const { return package_spec_info_; }
  OB_INLINE const common::ObString &get_package_body_source() const { return package_body_info_.get_source(); }
  OB_INLINE const ObPackageInfo &get_package_body_info() const { return package_body_info_; }
  OB_INLINE int64_t get_package_flag() const { return package_spec_info_.get_flag(); }
  OB_INLINE int64_t get_package_comp_flag() const { return package_spec_info_.get_comp_flag(); }
  OB_INLINE const common::ObString &get_package_exec_env() const { return package_spec_info_.get_exec_env(); }
  OB_INLINE uint64_t get_sql_mode() const { return sql_mode_; }
  OB_INLINE OrderType get_order_type() const { return order_type_; }
  OB_INLINE int64_t get_order_type_value() const { return static_cast<int64_t>(order_type_); }
  OB_INLINE bool is_order_follows() const { return OT_FOLLOWS == order_type_; }
  OB_INLINE bool is_order_precedes() const { return OT_PRECEDES == order_type_; }
  OB_INLINE const common::ObString &get_ref_trg_db_name() const { return ref_trg_db_name_; }
  OB_INLINE const common::ObString &get_ref_trg_name() const { return ref_trg_name_; }
  OB_INLINE int64_t get_action_order() const { return action_order_; }

  OB_INLINE void set_analyze_flag(uint64_t flag) { analyze_flag_ = flag; }
  OB_INLINE uint64_t get_analyze_flag() const { return analyze_flag_; }

  OB_INLINE void set_no_sql() { is_no_sql_ = true; is_reads_sql_data_ = false; is_modifies_sql_data_ = false; is_contains_sql_ = false; }
  OB_INLINE bool is_no_sql() const { return is_no_sql_; }
  OB_INLINE void set_reads_sql_data() { is_no_sql_ = false; is_reads_sql_data_ = true; is_modifies_sql_data_ = false; is_contains_sql_ = false; }
  OB_INLINE bool is_reads_sql_data() const { return is_reads_sql_data_; }
  OB_INLINE void set_modifies_sql_data() { is_no_sql_ = false; is_reads_sql_data_ = false; is_modifies_sql_data_ = true; is_contains_sql_ = false; }
  OB_INLINE bool is_modifies_sql_data() const { return is_modifies_sql_data_; }
  OB_INLINE void set_contains_sql() { is_no_sql_ = false; is_reads_sql_data_ = false; is_modifies_sql_data_ = false; is_contains_sql_ = true; }
  OB_INLINE bool is_contains_sql() const { return is_contains_sql_; }

  OB_INLINE void set_wps(bool v) { is_wps_ = v; }
  OB_INLINE bool is_wps() const { return is_wps_; }
  OB_INLINE void set_rps(bool v) { is_rps_ = v; }
  OB_INLINE bool is_rps() const { return is_rps_; }
  OB_INLINE void set_has_sequence(bool v) { is_has_sequence_ = v; }
  OB_INLINE bool is_has_sequence() const { return is_has_sequence_; }
  OB_INLINE void set_has_out_param(bool v) { is_has_out_param_ = v; }
  OB_INLINE bool is_has_out_param() const { return is_has_out_param_; }
  OB_INLINE void set_external_state(bool v) { is_external_state_ = v; }
  OB_INLINE bool is_external_state() const { return is_external_state_; }
  OB_INLINE bool is_row_level_before_trigger() const { return is_simple_dml_type() && timing_points_.only_before_row(); }
  OB_INLINE bool is_row_level_after_trigger() const { return is_simple_dml_type() && timing_points_.only_after_row(); }
  OB_INLINE bool is_stmt_level_before_trigger() const { return is_simple_dml_type() && timing_points_.only_before_stmt(); }
  OB_INLINE bool is_stmt_level_after_trigger() const { return is_simple_dml_type() && timing_points_.only_after_stmt(); }
  OB_INLINE bool has_event(uint64_t value) const { return trigger_events_.has_value(value); }
  OB_INLINE static uint64_t get_insert_event() { return ObTriggerEvents::get_insert_event(); }
  OB_INLINE static uint64_t get_update_event() { return ObTriggerEvents::get_update_event(); }
  OB_INLINE static uint64_t get_delete_event() { return ObTriggerEvents::get_delete_event(); }
  OB_INLINE TgTimingEvent get_timing_event() {
    TgTimingEvent t = TG_TIMING_EVENT_INVALID;
    if (lib::is_mysql_mode()) {
      if (has_before_row_point()) {
        t = has_insert_event() ? TG_BEFORE_INSERT : (has_update_event() ? TG_BEFORE_UPDATE
                                                                        : TG_BEFORE_DELETE);
      } else {
        t = has_insert_event() ? TG_AFTER_INSERT : (has_update_event() ? TG_AFTER_UPDATE
                                                                       : TG_AFTER_DELETE);
      }
    }
    return t;
  }
  // trigger id <=> package id.
  OB_INLINE static bool is_trigger_package_id(uint64_t package_id)
  {
    return package_id != common::OB_INVALID_ID
            && (package_id & common::OB_MOCK_TRIGGER_PACKAGE_ID_MASK) != 0;
  }
  OB_INLINE static bool is_trigger_body_package_id(uint64_t package_id)
  {
    return is_trigger_package_id(package_id)
            && (package_id & common::OB_MOCK_PACKAGE_BODY_ID_MASK) != 0;
  }
  OB_INLINE static uint64_t get_trigger_spec_package_id(uint64_t trigger_id)
  {
    return (trigger_id | common::OB_MOCK_TRIGGER_PACKAGE_ID_MASK);
  }
  OB_INLINE static uint64_t get_trigger_body_package_id(uint64_t trigger_id)
  {
    return trigger_id | common::OB_MOCK_TRIGGER_PACKAGE_ID_MASK | common::OB_MOCK_PACKAGE_BODY_ID_MASK;
  }
  OB_INLINE static uint64_t get_package_trigger_id(uint64_t package_id)
  {
    return package_id & ~common::OB_MOCK_TRIGGER_PACKAGE_ID_MASK & ~common::OB_MOCK_PACKAGE_BODY_ID_MASK;
  }
  bool is_valid_for_create() const;
  virtual void reset();
  virtual bool is_valid() const;
  virtual int deep_copy(const ObTriggerInfo &other);
  virtual int64_t get_convert_size() const;

  static OB_INLINE bool is_same_timing_event(const ObTriggerInfo &left, const ObTriggerInfo right)
  {
    return left.get_timing_points() == right.get_timing_points()
           && left.get_trigger_events() == right.get_trigger_events();
  }
  static int gen_package_source(const uint64_t tenant_id,
                                const uint64_t tg_package_id,
                                common::ObString &source,
                                bool is_header,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                common::ObIAllocator &alloc);
  int gen_package_source(const common::ObString &base_object_database,
                         const common::ObString &base_object_name,
                         const ParseNode &parse_node,
                         const common::ObDataTypeCastParams &dtc_params);
  int gen_procedure_source(const common::ObString &base_object_database,
                           const common::ObString &base_object_name,
                           const ParseNode &parse_node,
                           const ObDataTypeCastParams &dtc_params,
                           ObString &procedure_source);
#ifdef OB_BUILD_ORACLE_PL
  static int replace_trigger_name_in_body(ObTriggerInfo &trigger_info,
                                  common::ObIAllocator &alloc,
                                  share::schema::ObSchemaGetterGuard &schema_guard);
#endif
  static int replace_table_name_in_body(ObTriggerInfo &trigger_info,
                                        common::ObIAllocator &alloc,
                                        const common::ObString &base_object_database,
                                        const common::ObString &base_object_name,
                                        bool is_oracle_mode);
  TO_STRING_KV(K(tenant_id_),
               K(trigger_id_),
               K(owner_id_),
               K(database_id_),
               K(schema_version_),
               K(base_object_id_),
               K(base_object_type_),
               K(trigger_type_),
               K(trigger_events_.bit_value_),
               K(timing_points_.bit_value_),
               K(trigger_flags_.bit_value_),
               K(trigger_name_),
               K(update_columns_),
               K(reference_names_[RT_OLD]),
               K(reference_names_[RT_NEW]),
               K(reference_names_[RT_PARENT]),
               K(when_condition_),
               K(trigger_body_),
               K(package_spec_info_.get_source()),
               K(package_body_info_.get_source()),
               K(sql_mode_),
               K(priv_user_),
               K(order_type_),
               K(ref_trg_db_name_),
               K(ref_trg_name_),
               K(action_order_),
               K(analyze_flag_));
protected:
  static int gen_package_source_simple(const ObTriggerInfo &trigger_info,
                                       const common::ObString &base_object_database,
                                       const common::ObString &base_object_name,
                                       const ParseNode &parse_node,
                                       const common::ObDataTypeCastParams &dtc_params,
                                       common::ObString &spec_source,
                                       common::ObString &body_source,
                                       common::ObIAllocator &alloc,
                                       const PackageSouceType type = SPEC_AND_BODY);
  static int gen_package_source_compound(const ObTriggerInfo &trigger_info,
                                         const common::ObString &base_object_database,
                                         const common::ObString &base_object_name,
                                         const ParseNode &parse_node,
                                         const common::ObDataTypeCastParams &dtc_params,
                                         common::ObString &spec_source,
                                         common::ObString &body_source,
                                         common::ObIAllocator &alloc,
                                         const PackageSouceType type = SPEC_AND_BODY);
  static void calc_package_source_size(const ObTriggerInfo &trigger_info,
                                       const common::ObString &base_object_database,
                                       const common::ObString &base_object_name,
                                       int64_t &spec_size, int64_t &body_size);
  static int fill_package_spec_source(const ObTriggerInfo &trigger_info,
                                      const common::ObString &base_object_database,
                                      const common::ObString &base_object_name,
                                      const int64_t spec_size,
                                      common::ObString &spec_source,
                                      common::ObIAllocator &alloc);
  static int fill_package_body_source(const ObTriggerInfo &trigger_info,
                                      const common::ObString &base_object_database,
                                      const common::ObString &base_object_name,
                                      const int64_t body_size,
                                      const TriggerContext &trigger_ctx,
                                      common::ObString &body_source,
                                      common::ObIAllocator &alloc);
  static int fill_row_routine_spec(const char *spec_fmt,
                                   const ObTriggerInfo &trigger_info,
                                   const common::ObString &base_object_database,
                                   const common::ObString &base_object_name,
                                   char *buf, int64_t buf_len, int64_t &pos,
                                   const bool is_before_row);
  static int fill_when_routine_body(const char *body_fmt,
                                    const ObTriggerInfo &trigger_info,
                                    const common::ObString &base_object_database,
                                    const common::ObString &base_object_name,
                                    const common::ObString &body_execute,
                                    char *buf, int64_t buf_len, int64_t &pos);
  static int fill_row_routine_body(const ObTriggerInfo &trigger_info,
                                   const common::ObString &base_object_database,
                                   const common::ObString &base_object_name,
                                   const TriggerContext &trigger_ctx,
                                   char *buf, int64_t buf_len, int64_t &pos,
                                   const bool is_before_row);
  static int fill_stmt_routine_body(const ObTriggerInfo &trigger_info,
                                    const TriggerContext &trigger_ctx,
                                    char *buf, int64_t buf_len, int64_t &pos,
                                    const bool is_before);
  static int fill_compound_declare_body(const char *body_fmt,
                                        const common::ObString &body_declare,
                                        char *buf, int64_t buf_len, int64_t &pos);
protected:
//uint64_t tenant_id_;                            // set by user
//uint64_t trigger_id_;                           // set by sys
  uint64_t owner_id_;                             // set by sys
//uint64_t database_id_;                          // set by sys
//int64_t  schema_version_;                       // set by sys
  uint64_t base_object_id_;                       // set by user
  ObSchemaType base_object_type_;                 // set by user
  TriggerType trigger_type_;                      // set by user
  ObTriggerEvents trigger_events_;                // set by user
  ObTimingPoints timing_points_;                  // set by user
  ObTriggerFlags trigger_flags_;                  // set by user
//common::ObString trigger_name_;                 // set by user
  common::ObString update_columns_;               // set by user
  common::ObString reference_names_[RT_MAX];      // set by user
  common::ObString when_condition_;               // set by user
  common::ObString trigger_body_;                 // set by user
  ObPackageInfo package_spec_info_;
  ObPackageInfo package_body_info_;
  uint64_t sql_mode_;
  common::ObString priv_user_;
  OrderType order_type_;                    // trigger指定的排序方式
  common::ObString ref_trg_db_name_;              // 排序方式中指定的trigger的db name
  common::ObString ref_trg_name_;                 // 排序方式中指定的trigger的name
  int64_t action_order_;                          // 该值在rs端计算,从系统表里面读出来的值是有意义的
  union {
    uint64_t analyze_flag_;
    struct {
      uint64_t is_no_sql_ : 1;
      uint64_t is_reads_sql_data_ : 1;
      uint64_t is_modifies_sql_data_ : 1;
      uint64_t is_contains_sql_ : 1;
      uint64_t is_wps_ : 1;
      uint64_t is_rps_ : 1;
      uint64_t is_has_sequence_ : 1;
      uint64_t is_has_out_param_ : 1;
      uint64_t is_external_state_ : 1;
      uint64_t reserved_:54;
    };
  };
};


}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_TRIGGER_INFO_H_ */
