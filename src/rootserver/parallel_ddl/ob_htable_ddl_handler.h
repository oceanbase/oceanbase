/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_ROOTSERVER_OB_HTABLE_DDL_HANDLER_H_
#define OCEANBASE_ROOTSERVER_OB_HTABLE_DDL_HANDLER_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "rootserver/parallel_ddl/ob_set_kv_attribute_helper.h"
#include "share/table/ob_table_ddl_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObParallelDDLRes;
class ObCreateTableRes;
}
namespace rootserver
{
class ObCreateTablegroupHelper;
class ObDropTablegroupHelper;
class ObCreateTableHelper;
class ObDropTableHelper;
class ObHTableDDLHandler
{
public:
  ObHTableDDLHandler(ObDDLService &ddl_service,
                     share::schema::ObMultiVersionSchemaService &schema_service,
                     const obrpc::ObHTableDDLArg &arg,
                     obrpc::ObHTableDDLRes &res)
      : is_inited_(false),
        allocator_("HTbDDLH"),
        ddl_service_(ddl_service),
        schema_service_(schema_service),
        arg_(arg),
        res_(res),
        ddl_type_(arg.ddl_type_)
  {}
  virtual ~ObHTableDDLHandler() = default;
public:
  virtual int init() = 0;
  virtual int handle() = 0;
protected:
  int gen_task_id_and_schema_versions_(const uint64_t schema_version_cnt,
                                       int64_t &task_id);
  int wait_and_end_ddl_trans_(const int return_ret,
                              const int64_t task_id,
                              ObDDLSQLTransaction &trans,
                              bool &need_clean_failed);
  virtual int construct_result_();
  template <typename T1, typename T2>
  int clean_on_fail_commit_(T1 *tablegroup_helper,
                                    ObIArray<T2*> &helpers)
  {
    int ret = OB_SUCCESS;

    // tablegroup
    if (OB_NOT_NULL(tablegroup_helper)) {
      tablegroup_helper->clean_on_fail_commit();
    }
    // tables
    for (int64_t i = 0; i < helpers.count(); i++) { // overwrite ret
      T2 *helper = helpers.at(i);
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "helper is null", KR(ret), K(i));
      } else if (OB_FAIL(helper->clean_on_fail_commit())) {
        RS_LOG(WARN, "fail to clean on fail commit", KR(ret), K(i));
      }
    }

    return ret;
  }
  template <typename T>
  static int alloc_and_construct_(common::ObIAllocator &allocator,
                                  int64_t count,
                                  common::ObIArray<T*> &array)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(array.prepare_allocate(count))) {
      RS_LOG(WARN, "fail to prepare allocate array", K(ret), K(count));
    } else {
      void *buf = allocator.alloc(sizeof(T) * count);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        RS_LOG(WARN, "fail to alloc memory for array", K(ret), K(count));
      } else {
        for (int64_t i = 0; i < count; ++i) {
          array.at(i) = new (static_cast<char*>(buf) + sizeof(T) * i) T ();
        }
      }
    }
    return ret;
  }
  obrpc::ObHTableDDLType get_ddl_type() const { return ddl_type_; }
protected:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  ObDDLService &ddl_service_;
  share::schema::ObMultiVersionSchemaService &schema_service_;
  const obrpc::ObHTableDDLArg &arg_;
  obrpc::ObHTableDDLRes &res_;
  const obrpc::ObHTableDDLType ddl_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableDDLHandler);
};

// only use for ObHTableHandler to lock obj before executing
class ObHTableLockHelper : public ObDDLHelper
{
public:
  ObHTableLockHelper(share::schema::ObMultiVersionSchemaService *schema_service,
                     const uint64_t tenant_id,
                     ObDDLSQLTransaction *external_trans);
  virtual ~ObHTableLockHelper() {}
  int lock_objects(const common::ObString &tablegroup_name,
                   const common::ObString &database_name,
                   const common::ObIArray<common::ObString> &table_names,
                   bool need_lock_id);
  int check_htable_exist(const common::ObString &tablegroup_name,
                         const common::ObString &database_name,
                         const common::ObIArray<common::ObString> &table_names);
  int check_htable_not_exist(const common::ObString &tablegroup_name,
                             const common::ObString &database_name,
                             const common::ObIArray<common::ObString> &table_names);
private:
  int lock_database_by_name_(const ObString &database_name);
  int lock_tablegroup_and_tables_by_name_(const ObString &tablegroup_name,
                                          const common::ObString &database_name,
                                          const ObIArray<common::ObString> &table_names);
  int lock_htable_objects_by_id_();
private:
  // unused
  virtual int init_() override { return OB_SUCCESS; }
  virtual int lock_objects_() override { return OB_NOT_IMPLEMENT; }
  virtual int generate_schemas_() override { return OB_NOT_IMPLEMENT; }
  virtual int calc_schema_version_cnt_() override { return OB_NOT_IMPLEMENT; }
  virtual int operate_schemas_() override { return OB_NOT_IMPLEMENT; }
  virtual int operation_before_commit_() override { return OB_NOT_IMPLEMENT; }
  virtual int clean_on_fail_commit_() override { return OB_NOT_IMPLEMENT; }
  virtual int construct_and_adjust_result_(int &return_ret) override { return OB_NOT_IMPLEMENT; }
private:
  uint64_t tablegroup_id_;
  uint64_t database_id_;
  common::ObSEArray<uint64_t, 4> table_ids_;
};

class ObCreateHTableHandler : public ObHTableDDLHandler
{
public:
  ObCreateHTableHandler(ObDDLService &ddl_service,
                        share::schema::ObMultiVersionSchemaService &schema_service,
                        const obrpc::ObHTableDDLArg &arg,
                        obrpc::ObHTableDDLRes &res)
      : ObHTableDDLHandler(ddl_service, schema_service, arg, res),
        param_(nullptr),
        schema_version_cnt_(0),
        task_id_(common::OB_INVALID_ID),
        tablegroup_name_(),
        database_name_()
  {}
  virtual ~ObCreateHTableHandler() = default;
public:
  virtual int init() override;
  virtual int handle() override;
private:
  int gen_create_helpers_(ObCreateTablegroupHelper *&create_tablegroup_helper,
                          ObCreateTableGroupRes *&create_tablegroup_result,
                          common::ObIArray<ObCreateTableHelper*> &create_table_helpers,
                          common::ObIArray<ObCreateTableRes*> &create_table_results,
                          ObDDLSQLTransaction &trans);
  void clear_helpers_(ObCreateTablegroupHelper *create_tablegroup_helper,
                      ObCreateTableGroupRes *create_tablegroup_result,
                      common::ObIArray<ObCreateTableHelper*> &create_table_helpers,
                      common::ObIArray<ObCreateTableRes*> &create_table_results);
private:
  table::ObCreateHTableDDLParam *param_;
  int64_t schema_version_cnt_;
  int64_t task_id_;
  common::ObString tablegroup_name_;
  common::ObString database_name_;
  ObSEArray<common::ObString, 4> table_names_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateHTableHandler);
};

class ObDropHTableHandler : public ObHTableDDLHandler
{
public:
  ObDropHTableHandler(ObDDLService &ddl_service,
                      share::schema::ObMultiVersionSchemaService &schema_service,
                      const obrpc::ObHTableDDLArg &arg,
                      obrpc::ObHTableDDLRes &res)
      : ObHTableDDLHandler(ddl_service, schema_service, arg, res),
        param_(nullptr),
        schema_version_cnt_(0),
        task_id_(common::OB_INVALID_ID),
        tablegroup_name_(),
        database_name_()
  {}
  virtual ~ObDropHTableHandler() = default;
public:
  virtual int init() override;
  virtual int handle() override;
private:
  int gen_drop_helpers_(ObDropTablegroupHelper *&drop_tablegroup_helper,
                        ObParallelDDLRes *&drop_tablegroup_result,
                        common::ObIArray<ObDropTableHelper*> &drop_table_helpers,
                        common::ObIArray<ObDropTableArg*> &drop_table_args,
                        common::ObIArray<ObDropTableRes*> &drop_table_results,
                        ObDDLSQLTransaction &trans);
  void clear_helpers_(ObDropTablegroupHelper *drop_tablegroup_helper,
                      ObParallelDDLRes *drop_tablegroup_result,
                      common::ObIArray<ObDropTableHelper*> &drop_table_helpers,
                      common::ObIArray<ObDropTableArg*> &drop_table_args,
                      common::ObIArray<ObDropTableRes*> &drop_table_results);
  int init_drop_table_args_(common::ObIArray<ObDropTableArg*> &drop_table_args);
private:
  table::ObDropHTableDDLParam *param_;
  int64_t schema_version_cnt_;
  int64_t task_id_;
  common::ObString tablegroup_name_;
  common::ObString database_name_;
  ObSEArray<common::ObString, 4> table_names_;
  ObSEArray<uint64_t, 4> table_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropHTableHandler);
};

class ObHTableControlHandler : public ObHTableDDLHandler
{
public:
  ObHTableControlHandler(ObDDLService &ddl_service,
                         share::schema::ObMultiVersionSchemaService &schema_service,
                         const obrpc::ObHTableDDLArg &arg,
                         obrpc::ObHTableDDLRes &res)
      : ObHTableDDLHandler(ddl_service, schema_service, arg, res),
        kv_attribute_helper_(&schema_service, arg, res)
  {}
  virtual ~ObHTableControlHandler() = default;
public:
  virtual int init() override;
  virtual int handle() override;
private:
  ObSetKvAttributeHelper kv_attribute_helper_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableControlHandler);
};

class ObHTableDDLHandlerGuard
{
public:
  ObHTableDDLHandlerGuard(common::ObIAllocator &allocator)
      : handler_(nullptr),
        allocator_(allocator)
  {}
  ~ObHTableDDLHandlerGuard()
  {
    OB_DELETEx(ObHTableDDLHandler, &allocator_, handler_);
  }
public:
  int get_handler(ObDDLService &ddl_service,
                  share::schema::ObMultiVersionSchemaService &schema_service,
                  const obrpc::ObHTableDDLArg &arg,
                  obrpc::ObHTableDDLRes &res,
                  ObHTableDDLHandler *&handler);
private:
  ObHTableDDLHandler *handler_;
  common::ObIAllocator &allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableDDLHandlerGuard);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif // OCEANBASE_ROOTSERVER_OB_HTABLE_DDL_HANDLER_H_
