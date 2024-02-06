set(CPACK_GENERATOR "RPM")
# use seperated RPM SPECs and generate different RPMs
set(CPACK_COMPONENTS_IGNORE_GROUPS 1)
set(CPACK_RPM_COMPONENT_INSTALL ON)
# use "server" as main component so its RPM filename won't have "server"
set(CPACK_RPM_MAIN_COMPONENT "server")
# let rpmbuild determine rpm filename
set(CPACK_RPM_FILE_NAME "RPM-DEFAULT")
## Stardard debuginfo generating instructions in cmake.  However 6u
## server with rpm-4.8.0 which doesn't support dwarf4 won't generate
## BUILDID for RPM. And Our debuginfo package doesn't contain source
## code. Thus we don't use the way cmake sugguests.
#set(CPACK_RPM_DEBUGINFO_PACKAGE ON)
#set(CPACK_RPM_BUILD_SOURCE_DIRS_PREFIX "/usr/src/debug")
# RPM package informations.
set(CPACK_PACKAGING_INSTALL_PREFIX /home/admin/oceanbase)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "OceanBase is a distributed relational database")
set(CPACK_PACKAGE_VENDOR "OceanBase Inc.")
set(CPACK_RPM_PACKAGE_RELEASE ${OB_RELEASEID})
if (OB_BUILD_OPENSOURCE)
set(CPACK_PACKAGE_NAME "oceanbase-ce")
set(CPACK_PACKAGE_VERSION "${OceanBase_CE_VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${OceanBase_CE_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${OceanBase_CE_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${OceanBase_CE_VERSION_PATCH}")
set(CPACK_RPM_PACKAGE_URL "${OceanBase_CE_HOMEPAGE_URL}")
set(CPACK_RPM_PACKAGE_RELEASE_DIST ON)
## set relocation path install prefix for each component
set(CPACK_RPM_DEVEL_PACKAGE_PREFIX /usr)
set(CPACK_RPM_UTILS_PACKAGE_PREFIX /usr)
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home/admin")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home/admin/oceanbase")
else()
set(CPACK_PACKAGE_NAME "oceanbase")
set(CPACK_PACKAGE_VERSION "${OceanBase_VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${OceanBase_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${OceanBase_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${OceanBase_VERSION_PATCH}")
set(CPACK_RPM_PACKAGE_URL "${OceanBase_HOMEPAGE_URL}")
endif()
set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
set(CPACK_RPM_PACKAGE_DESCRIPTION "OceanBase is a distributed relational database")
set(CPACK_RPM_PACKAGE_LICENSE "Mulan PubL v2.")
set(CPACK_RPM_DEFAULT_USER "admin")
set(CPACK_RPM_DEFAULT_GROUP "admin")
set(CPACK_RPM_SPEC_MORE_DEFINE
  "%global _missing_build_ids_terminate_build 0
%global _find_debuginfo_opts -g
%define __strip ${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/devtools/bin/llvm-strip
%undefine __brp_mangle_shebangs
%global __requires_exclude ^\(/bin/bash\|/usr/bin/\.*\)$
%define __debug_install_post %{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} %{_builddir}/%{?buildsubdir};%{nil}
%if \\\"%name\\\" != \\\"oceanbase-ce-sql-parser\\\" && \\\"%name\\\" != \\\"oceanbase-sql-parser\\\"
%debug_package
%endif
")

## TIPS
#
# - PATH is relative to the **ROOT directory** of project other than the cmake directory.

set(BITCODE_TO_ELF_LIST "")

## server
if (OB_BUILD_OPENSOURCE)
install(PROGRAMS
  tools/import_time_zone_info.py
  tools/import_srs_data.py
  ${CMAKE_BINARY_DIR}/src/observer/observer
  DESTINATION bin
  COMPONENT server)
else()
install(PROGRAMS
  script/dooba/dooba
  tools/import_time_zone_info.py
  tools/import_srs_data.py
  ${CMAKE_BINARY_DIR}/tools/ob_admin/ob_admin
  tools/ob_admin/io_bench/bench_io.sh
  ${CMAKE_BINARY_DIR}/src/observer/observer
  $<$<STREQUAL:"${ARCHITECTURE}","x86_64">:${DEVTOOLS_DIR}/bin/obstack>
  DESTINATION bin
  COMPONENT server)
endif()

install(FILES
  src/sql/fill_help_tables-ob.sql
  tools/timezone_V1.log
  tools/default_srs_data_mysql.sql
  tools/upgrade/upgrade_pre.py
  tools/upgrade/upgrade_post.py
  tools/upgrade/upgrade_checker.py
  tools/upgrade/upgrade_health_checker.py
  tools/upgrade/oceanbase_upgrade_dep.yml
  DESTINATION etc
  COMPONENT server)

install(
  DIRECTORY src/share/inner_table/sys_package/
  DESTINATION admin
  COMPONENT server)

## oceanbase-cdc
if (NOT OB_SO_CACHE AND OB_BUILD_CDC)
include(GNUInstallDirs)
install(
  TARGETS obcdc obcdc_tailf
  COMPONENT cdc
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/libobcdc
  )

get_property(CDCMSG_HEADER_DIR GLOBAL PROPERTY CDC_MSG_HEADER_DIR)
install(
  DIRECTORY
  ${CDCMSG_HEADER_DIR}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  COMPONENT cdc
  )

if(OB_BUILD_OPENSOURCE)
install(
  FILES
    ${PROJECT_SOURCE_DIR}/src/logservice/libobcdc/tests/libobcdc.conf
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}
  COMPONENT cdc
  )
else()
install(
  FILES
    ${PROJECT_SOURCE_DIR}/src/logservice/libobcdc/tests/libobcdc.conf
    ${PROJECT_SOURCE_DIR}/src/logservice/libobcdc/tests/timezone_info.conf
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}
  COMPONENT cdc
  )
endif()
endif()

## oceanbase-sql-parser
if (OB_BUILD_LIBOB_SQL_PROXY_PARSER)

  if (ENABLE_THIN_LTO)
    message(STATUS "add libob_sql_proxy_parser_static_to_elf")
    add_custom_command(
      OUTPUT libob_sql_proxy_parser_static_to_elf
      COMMAND ${CMAKE_SOURCE_DIR}/cmake/script/bitcode_to_elfobj --ld=${OB_LD_BIN} --input=${CMAKE_BINARY_DIR}/src/sql/parser/libob_sql_proxy_parser_static.a --output=${CMAKE_BINARY_DIR}/src/sql/parser/libob_sql_proxy_parser_static.a
      DEPENDS ob_sql_proxy_parser_static
      COMMAND_EXPAND_LISTS)
    list(APPEND BITCODE_TO_ELF_LIST libob_sql_proxy_parser_static_to_elf)
  endif()

  install(PROGRAMS
    ${CMAKE_BINARY_DIR}/src/sql/parser/libob_sql_proxy_parser_static.a
    DESTINATION lib
    COMPONENT sql-parser
    )
endif()

install(FILES
  src/objit/include/objit/common/ob_item_type.h
  deps/oblib/src/common/sql_mode/ob_sql_mode.h
  src/sql/parser/ob_sql_parser.h
  src/sql/parser/parse_malloc.h
  src/sql/parser/parser_proxy_func.h
  src/sql/parser/parse_node.h
  DESTINATION include
  COMPONENT sql-parser)

install(FILES
  src/objit/include/objit/common/ob_item_type.h
  deps/oblib/src/common/sql_mode/ob_sql_mode.h
  src/sql/parser/ob_sql_parser.h
  src/sql/parser/parse_malloc.h
  src/sql/parser/parser_proxy_func.h
  src/sql/parser/parse_node.h
  DESTINATION include
  COMPONENT sql-parser)

## oceanbsae-table
install(FILES
  deps/oblib/src/common/data_buffer.h
  deps/oblib/src/common/ob_accuracy.h
  deps/oblib/src/common/ob_action_flag.h
  deps/oblib/src/common/ob_common_utility.h
  deps/oblib/src/common/ob_range.h
  deps/oblib/src/common/ob_region.h
  deps/oblib/src/common/ob_role.h
  deps/oblib/src/common/ob_string_buf.h
  deps/oblib/src/common/ob_string_buf.ipp
  deps/oblib/src/common/ob_timeout_ctx.h
  deps/oblib/src/common/ob_zerofill_info.h
  deps/oblib/src/common/ob_zone.h
  deps/oblib/src/common/object/ob_obj_type.h
  deps/oblib/src/common/object/ob_object.h
  deps/oblib/src/common/rowkey/ob_rowkey.h
  deps/oblib/src/common/rowkey/ob_rowkey_info.h
  deps/oblib/src/lib/alloc/abit_set.h
  deps/oblib/src/lib/alloc/alloc_assist.h
  deps/oblib/src/lib/alloc/alloc_func.h
  deps/oblib/src/lib/alloc/alloc_struct.h
  deps/oblib/src/lib/alloc/block_set.h
  deps/oblib/src/lib/alloc/ob_malloc_allocator.h
  deps/oblib/src/lib/alloc/ob_tenant_ctx_allocator.h
  deps/oblib/src/lib/alloc/object_mgr.h
  deps/oblib/src/lib/alloc/object_set.h
  deps/oblib/src/lib/allocator/ob_allocator.h
  deps/oblib/src/lib/allocator/ob_cached_allocator.h
  deps/oblib/src/lib/allocator/ob_concurrent_fifo_allocator.h
  deps/oblib/src/lib/allocator/ob_fifo_allocator.h
  deps/oblib/src/lib/allocator/ob_lf_fifo_allocator.h
  deps/oblib/src/lib/allocator/ob_malloc.h
  deps/oblib/src/lib/allocator/ob_mod_define.h
  deps/oblib/src/lib/allocator/ob_pcounter.h
  deps/oblib/src/lib/allocator/ob_pooled_allocator.h
  deps/oblib/src/lib/allocator/ob_retire_station.h
  deps/oblib/src/lib/allocator/ob_small_allocator.h
  deps/oblib/src/lib/allocator/ob_tc_malloc.h
  deps/oblib/src/lib/allocator/page_arena.h
  deps/oblib/src/lib/atomic/ob_atomic.h
  deps/oblib/src/lib/atomic/ob_atomic_reference.h
  deps/oblib/src/lib/charset/ob_charset.h
  deps/oblib/src/lib/charset/ob_config.h
  deps/oblib/src/lib/charset/ob_ctype.h
  deps/oblib/src/lib/charset/ob_mysql_global.h
  deps/oblib/src/lib/checksum/ob_crc64.h
  deps/oblib/src/lib/container/ob_array.h
  deps/oblib/src/lib/container/ob_array_helper.h
  deps/oblib/src/lib/container/ob_array_iterator.h
  deps/oblib/src/lib/container/ob_array_serialization.h
  deps/oblib/src/lib/container/ob_array_wrap.h
  deps/oblib/src/lib/container/ob_bit_set.h
  deps/oblib/src/lib/container/ob_fixed_array.h
  deps/oblib/src/lib/container/ob_iarray.h
  deps/oblib/src/lib/container/ob_se_array.h
  deps/oblib/src/lib/container/ob_vector.h
  deps/oblib/src/lib/container/ob_vector.ipp
  deps/oblib/src/lib/core_local/ob_core_local_storage.h
  deps/oblib/src/lib/file/config.h
  deps/oblib/src/lib/file/ob_string_util.h
  deps/oblib/src/lib/hash/mprotect.h
  deps/oblib/src/lib/hash/ob_array_index_hash_set.h
  deps/oblib/src/lib/hash/ob_hashmap.h
  deps/oblib/src/lib/hash/ob_hashset.h
  deps/oblib/src/lib/hash/ob_hashtable.h
  deps/oblib/src/lib/hash/ob_hashutils.h
  deps/oblib/src/lib/hash/ob_iteratable_hashmap.h
  deps/oblib/src/lib/hash/ob_linear_hash_map.h
  deps/oblib/src/lib/hash/ob_placement_hashutils.h
  deps/oblib/src/lib/hash/ob_pointer_hashmap.h
  deps/oblib/src/lib/hash/ob_serialization.h
  deps/oblib/src/lib/hash_func/murmur_hash.h
  deps/oblib/src/lib/hash_func/ob_hash_func.h
  deps/oblib/src/lib/json/ob_yson.h
  deps/oblib/src/lib/json/ob_yson_encode.h
  deps/oblib/src/lib/list/ob_dlink_node.h
  deps/oblib/src/lib/list/ob_dlist.h
  deps/oblib/src/lib/list/ob_list.h
  deps/oblib/src/lib/lock/cond.h
  deps/oblib/src/lib/lock/ob_lock.h
  deps/oblib/src/lib/lock/ob_monitor.h
  deps/oblib/src/lib/lock/mutex.h
  deps/oblib/src/lib/lock/ob_bucket_lock.h
  deps/oblib/src/lib/lock/ob_drw_lock.h
  deps/oblib/src/lib/lock/ob_latch.h
  deps/oblib/src/lib/lock/ob_lock_guard.h
  deps/oblib/src/lib/lock/ob_mutex.h
  deps/oblib/src/lib/lock/ob_small_spin_lock.h
  deps/oblib/src/lib/lock/ob_spin_lock.h
  deps/oblib/src/lib/lock/ob_spin_rwlock.h
  deps/oblib/src/lib/lock/ob_thread_cond.h
  deps/oblib/src/lib/lock/ob_rwlock.h
  deps/oblib/src/lib/metrics/ob_counter.h
  deps/oblib/src/lib/net/ob_addr.h
  deps/oblib/src/lib/net/ob_net_util.h
  deps/oblib/src/lib/number/ob_number_v2.h
  deps/oblib/src/lib/ob_date_unit_type.h
  deps/oblib/src/lib/ob_define.h
  deps/oblib/src/lib/ob_errno.h
  deps/oblib/src/lib/ob_name_def.h
  deps/oblib/src/lib/ob_name_id_def.h
  deps/oblib/src/lib/oblog/ob_log.h
  deps/oblib/src/lib/oblog/ob_log_module.h
  deps/oblib/src/lib/oblog/ob_log_print_kv.h
  deps/oblib/src/lib/oblog/ob_trace_log.h
  deps/oblib/src/lib/profile/ob_atomic_event.h
  deps/oblib/src/lib/queue/ob_dedup_queue.h
  deps/oblib/src/lib/queue/ob_fixed_queue.h
  deps/oblib/src/lib/queue/ob_link.h
  deps/oblib/src/lib/random/ob_random.h
  deps/oblib/src/lib/resource/achunk_mgr.h
  deps/oblib/src/lib/resource/ob_cache_washer.h
  deps/oblib/src/lib/resource/ob_resource_mgr.h
  deps/oblib/src/lib/stat/ob_latch_define.h
  deps/oblib/src/lib/string/ob_fixed_length_string.h
  deps/oblib/src/lib/string/ob_string.h
  deps/oblib/src/lib/string/ob_strings.h
  deps/oblib/src/lib/thread_local/ob_tsi_factory.h
  deps/oblib/src/lib/thread_local/ob_tsi_utils.h
  deps/oblib/src/lib/time/Time.h
  deps/oblib/src/lib/time/ob_time_utility.h
  deps/oblib/src/lib/timezone/ob_time_convert.h
  deps/oblib/src/lib/timezone/ob_timezone_info.h
  deps/oblib/src/lib/trace/ob_seq_event_recorder.h
  deps/oblib/src/lib/trace/ob_trace_event.h
  deps/oblib/src/lib/utility/ob_hang_fatal_error.h
  deps/oblib/src/lib/utility/ob_macro_utils.h
  deps/oblib/src/lib/utility/ob_print_kv.h
  deps/oblib/src/lib/utility/ob_print_utils.h
  deps/oblib/src/lib/utility/ob_rate_limiter.h
  deps/oblib/src/lib/utility/ob_serialization_helper.h
  deps/oblib/src/lib/utility/ob_template_utils.h
  deps/oblib/src/lib/utility/ob_unify_serialize.h
  deps/oblib/src/lib/utility/serialization.h
  deps/oblib/src/lib/utility/utility.h
  deps/oblib/src/lib/wait_event/ob_wait_class.h
  deps/oblib/src/lib/wait_event/ob_wait_event.h
  src/share/config/ob_common_config.h
  src/share/config/ob_config.h
  src/share/config/ob_config_helper.h
  src/share/mysql_errno.h
  src/share/object/ob_obj_cast.h
  src/share/partition_table/ob_partition_location.h
  src/share/table/ob_table.h
  src/share/table/ob_table_rpc_proxy.h
  src/share/table/ob_table_rpc_struct.h
  src/libtable/src/libobtable.h
  src/libtable/src/ob_table.h
  src/libtable/src/ob_hkv_table.h
  src/libtable/src/ob_pstore.h
  src/libtable/src/ob_table_service_client.h
  src/libtable/src/ob_table_service_config.h
  src/libtable/src/ob_table_define.h
  DESTINATION include
  COMPONENT table)

install(FILES
  src/libtable/examples/ob_pstore_example.cpp
  src/libtable/examples/ob_kvtable_example.cpp
  src/libtable/examples/ob_table_example.cpp
  src/libtable/examples/example_makefile.mk
  DESTINATION examples
  COMPONENT table)

if (OB_BUILD_LIBOBTABLE)

  if (ENABLE_THIN_LTO)
    message(STATUS "add libobtable_static_to_elf")
    add_custom_command(
      OUTPUT libobtable_static_to_elf
      COMMAND ${CMAKE_SOURCE_DIR}/cmake/script/bitcode_to_elfobj --ld=${OB_LD_BIN} --input=${CMAKE_BINARY_DIR}/src/libtable/src/libobtable_static.a --output=${CMAKE_BINARY_DIR}/src/libtable/src/libobtable_static.a
      DEPENDS obtable_static
      COMMAND_EXPAND_LISTS)
      list(APPEND BITCODE_TO_ELF_LIST libobtable_static_to_elf)
  endif()

  install(PROGRAMS
    ${CMAKE_BINARY_DIR}/src/libtable/src/libobtable.so
    ${CMAKE_BINARY_DIR}/src/libtable/src/libobtable.so.1
    ${CMAKE_BINARY_DIR}/src/libtable/src/libobtable.so.1.0.0
    ${CMAKE_BINARY_DIR}/src/libtable/src/libobtable_static.a
    DESTINATION lib
    COMPONENT table)
endif()

if(OB_BUILD_OPENSOURCE)
## oceanbase-libs
install(PROGRAMS
  deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so.1
  deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so.1.0.1
  deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so
  deps/3rd/usr/local/oceanbase/deps/devel/lib/mariadb/libmariadb.so
  deps/3rd/usr/local/oceanbase/deps/devel/lib/mariadb/libmariadb.so.3
  DESTINATION lib
  COMPONENT libs
)
if(OB_BUILD_OBADMIN)
    ## oceanbase-utils
    install(PROGRAMS
      ${CMAKE_BINARY_DIR}/tools/ob_admin/ob_admin
      ${CMAKE_BINARY_DIR}/tools/ob_error/src/ob_error
      DESTINATION /usr/bin
      COMPONENT utils
    )
  endif()
endif()

# install cpack to make everything work
include(CPack)

#add rpm target to create RPMS
add_custom_target(rpm
  COMMAND +make package
  DEPENDS
  observer obcdc_tailf obtable obtable_static
  ob_admin ob_error ob_sql_proxy_parser_static
  ${BITCODE_TO_ELF_LIST}
  )
