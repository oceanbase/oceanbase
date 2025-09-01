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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_CATALOG_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_CATALOG_H_
#include <memory>
#include "ob_odps_jni_connector.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/ob_external_catalog.h"
namespace oceanbase {
namespace sql {

class ObOdpsJniCatalogAgent;
typedef std::shared_ptr<ObOdpsJniCatalogAgent> JNICatalogPtr;
class ObOdpsJniCatalogAgent: public ObOdpsJniConnector
{
public:
    ObOdpsJniCatalogAgent(ObString factory_class): jni_catalog_factory_class_(factory_class) {}
    ~ObOdpsJniCatalogAgent() {
        do_close();
    }
    /*
        init all method
    */
    int do_init(ObIAllocator &allocator, const share::ObODPSCatalogProperties &properties);
    int do_query_table_list(ObIAllocator &allocator, ObIArray<ObString> &tbl_names);
    int do_query_table_info(ObIAllocator &allocator, const ObString &tbl_name, int64_t &last_modification_time_s);
    int do_query_table_option(ObString access_id_, ObString access_key_, ObString project_name_, ObString tableName);
    int do_close();
private:
    int get_java_catalog_service_class();
    int get_java_catalog_service_obj(ObIAllocator &allocator, const share::ObODPSCatalogProperties &properties);
private:
    JNIEnv *env_ = nullptr;
    ObString jni_catalog_factory_class_;
    jclass jni_catalog_service_class_ = nullptr;
    jobject jni_catalog_service_obj_ = nullptr;
};

JNICatalogPtr create_odps_jni_catalog();
}
}
#endif