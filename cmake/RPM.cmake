set(CPACK_GENERATOR "RPM")
# use seperated RPM SPECs and generate different RPMs
set(CPACK_COMPONENTS_IGNORE_GROUPS 1)
set(CPACK_RPM_COMPONENT_INSTALL ON)
# use "server" as main component so its RPM filename won't have "server"
set(CPACK_RPM_MAIN_COMPONENT "server")
# let rpmbuild determine rpm filename
set(CPACK_RPM_FILE_NAME "RPM-DEFAULT")
set(CPACK_RPM_PACKAGE_RELEASE ${OB_RELEASEID})
set(CPACK_RPM_PACKAGE_RELEASE_DIST ON)
# RPM package informations.
set(CPACK_PACKAGING_INSTALL_PREFIX /home/admin/oceanbase)
# set relocation path install prefix for each component
set(CPACK_RPM_DEVEL_PACKAGE_PREFIX /usr)
set(CPACK_RPM_UTILS_PACKAGE_PREFIX /usr)
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home/admin")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home/admin/oceanbase")
set(CPACK_PACKAGE_NAME "oceanbase-ce")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "OceanBase CE is a distributed relational database")
set(CPACK_PACKAGE_VENDOR "Ant Group CO., Ltd.")
set(CPACK_PACKAGE_VERSION 3.1.5)
set(CPACK_PACKAGE_VERSION_MAJOR 3)
set(CPACK_PACKAGE_VERSION_MINOR 1)
set(CPACK_PACKAGE_VERSION_PATCH 5)
set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
set(CPACK_RPM_PACKAGE_URL "https://open.oceanbase.com")
set(CPACK_RPM_PACKAGE_DESCRIPTION "OceanBase CE is a distributed relational database")
set(CPACK_RPM_PACKAGE_LICENSE "Mulan PubL v2.")
set(CPACK_RPM_DEFAULT_USER "admin")
set(CPACK_RPM_DEFAULT_GROUP "admin")
set(CPACK_RPM_SPEC_MORE_DEFINE
  "%global _missing_build_ids_terminate_build 0
%global _find_debuginfo_opts -g
%define __debug_install_post %{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} %{_builddir}/%{?buildsubdir};%{nil}
%define debug_package %{nil}")

## TIPS
#
# - PATH is relative to the **ROOT directory** of project other than the cmake directory.

## server
install(PROGRAMS
  tools/timezone/import_time_zone_info.py
  ${CMAKE_BINARY_DIR}/src/observer/observer
  $<$<STREQUAL:${ARCHITECTURE},"x86_64">:tools/timezone/mysql_tzinfo_to_sql>
  DESTINATION bin
  COMPONENT server)

install(FILES
  tools/timezone/timezone_V1.log
  tools/upgrade/upgrade_pre.py
  tools/upgrade/upgrade_post.py
  tools/upgrade/upgrade_post_checker.py
  tools/upgrade/upgrade_checker.py
  tools/upgrade/upgrade_cluster_health_checker.py
  tools/upgrade/upgrade_rolling_pre.py
  tools/upgrade/upgrade_rolling_post.py
  tools/upgrade/priv_checker.py
  tools/upgrade/oceanbase_upgrade_dep.yml
  DESTINATION etc
  COMPONENT server)

## oceanbase-devel
# libobcdc.so and libob_sql_proxy_parser_static.a
set(OCEANBASE_DEVEL_LIB_FILES "")
set(OCEANBASE_DEVEL_INCLUDE_FILES deps/oblib/src/lib/ob_errno.h)
set(OCEANBASE_DEVEL_BIN_FILES "")

message(STATUS "OB_BUILD_LIBOB_SQL_PROXY_PARSER ${OB_BUILD_LIBOB_SQL_PROXY_PARSER}")
if (OB_BUILD_LIBOB_SQL_PROXY_PARSER)
  # lib
  list(APPEND OCEANBASE_DEVEL_LIB_FILES ${CMAKE_BINARY_DIR}/src/sql/parser/libob_sql_proxy_parser_static.a)

  # headers
  list(APPEND OCEANBASE_DEVEL_INCLUDE_FILES deps/oblib/src/common/sql_mode/ob_sql_mode.h)
  list(APPEND OCEANBASE_DEVEL_INCLUDE_FILES src/sql/parser/ob_item_type.h)
  list(APPEND OCEANBASE_DEVEL_INCLUDE_FILES src/sql/parser/ob_sql_parser.h)
  list(APPEND OCEANBASE_DEVEL_INCLUDE_FILES src/sql/parser/parse_malloc.h)
  list(APPEND OCEANBASE_DEVEL_INCLUDE_FILES src/sql/parser/parser_proxy_func.h)
  list(APPEND OCEANBASE_DEVEL_INCLUDE_FILES src/sql/parser/parse_node.h)
endif()

if (OB_BUILD_LIBOBLOG)
  # lib
  # list(APPEND OCEANBASE_DEVEL_LIB_FILES ${CMAKE_BINARY_DIR}/tools/libobcdc/src/libobcdc.a)
  list(APPEND OCEANBASE_DEVEL_LIB_FILES ${CMAKE_BINARY_DIR}/tools/obcdc/src/libobcdc.so)
  list(APPEND OCEANBASE_DEVEL_LIB_FILES ${CMAKE_BINARY_DIR}/tools/obcdc/src/libobcdc.so.1)
  list(APPEND OCEANBASE_DEVEL_LIB_FILES ${CMAKE_BINARY_DIR}/tools/obcdc/src/libobcdc.so.1.0.0)

  # include lilboblog header
  list(APPEND OCEANBASE_DEVEL_INCLUDE_FILES tools/obcdc/src/libobcdc.h)

  # bin
  list(APPEND OCEANBASE_DEVEL_BIN_FILES ${CMAKE_BINARY_DIR}/tools/obcdc/tests/obcdc_tailf)
endif()

set(CPACK_RPM_DEVEL_DEFAULT_USER "root")
set(CPACK_RPM_DEVEL_DEFAULT_GROUP "root")

install(PROGRAMS
  ${OCEANBASE_DEVEL_LIB_FILES}
  DESTINATION /usr/lib
  COMPONENT devel
)

install(FILES
  ${OCEANBASE_DEVEL_INCLUDE_FILES}
  DESTINATION /usr/include
  COMPONENT devel
)

install(PROGRAMS
  ${OCEANBASE_DEVEL_BIN_FILES}
  DESTINATION /usr/bin
  COMPONENT devel
)

if (OB_BUILD_LIBOBLOG)
  install(DIRECTORY
    "deps/logmessage/include/"
    DESTINATION /usr/include/oblogmsg
    COMPONENT devel)
endif()

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

# utils
set(CPACK_RPM_UTILS_DEFAULT_USER "root")
set(CPACK_RPM_UTILS_DEFAULT_GROUP "root")

if (OB_BUILD_TOOLS)
  install(PROGRAMS
    ${CMAKE_BINARY_DIR}/tools/ob_admin/ob_admin
    ${CMAKE_BINARY_DIR}/tools/ob_error/src/ob_error
    DESTINATION /usr/bin
    COMPONENT utils
  )
endif()
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/utils_post.script "/sbin/ldconfig /home/admin/oceanbase/lib")
set(CPACK_RPM_UTILS_POST_INSTALL_SCRIPT_FILE  ${CMAKE_CURRENT_BINARY_DIR}/utils_post.script)
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/utils_postun.script "/sbin/ldconfig")
set(CPACK_RPM_UTILS_POST_UNINSTALL_SCRIPT_FILE  ${CMAKE_CURRENT_BINARY_DIR}/utils_postun.script)

# install cpack to make everything work
include(CPack)

#add rpm target to create RPMS
add_custom_target(rpm
  COMMAND +make package
  DEPENDS
  observer ob_admin ob_error
  ob_sql_proxy_parser_static)
