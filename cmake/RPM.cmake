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

include(cmake/Pack.cmake)

set(CPACK_RPM_PACKAGE_RELEASE ${OB_RELEASEID})
if (OB_DISABLE_LSE)
  ob_insert_nonlse_to_package_version(${CPACK_RPM_PACKAGE_RELEASE} CPACK_RPM_PACKAGE_RELEASE)
  message(STATUS "CPACK_RPM_PACKAGE_RELEASE: ${CPACK_RPM_PACKAGE_RELEASE}")
endif()

if (OB_BUILD_OPENSOURCE)
  set(CPACK_RPM_PACKAGE_URL "${OceanBase_CE_HOMEPAGE_URL}")
  set(CPACK_RPM_PACKAGE_RELEASE_DIST ON)
  ## set relocation path install prefix for each component
  set(CPACK_RPM_DEVEL_PACKAGE_PREFIX /usr)
  set(CPACK_RPM_UTILS_PACKAGE_PREFIX /usr)
  list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home")
  list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home/admin")
  list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/home/admin/oceanbase")
else()
  set(CPACK_RPM_PACKAGE_URL "${OceanBase_HOMEPAGE_URL}")
endif()

set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
set(CPACK_RPM_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION})
set(CPACK_RPM_PACKAGE_LICENSE "Mulan PubL v2.")
set(CPACK_RPM_DEFAULT_USER "admin")
set(CPACK_RPM_DEFAULT_GROUP "admin")
if (OB_BUILD_OPENSOURCE)
  set(DEBUG_INSTALL_POST "mv $RPM_BUILD_ROOT/../server/home/admin/oceanbase/bin/obshell %{_builddir}/obshell; %{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} %{_builddir}/%{?buildsubdir}; mv %{_builddir}/obshell $RPM_BUILD_ROOT/../server/home/admin/oceanbase/bin/obshell; %{nil}")
else()
  set(DEBUG_INSTALL_POST "%{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} %{_builddir}/%{?buildsubdir};%{nil}")
endif()
set(CPACK_RPM_SPEC_MORE_DEFINE
  "%global _missing_build_ids_terminate_build 0
%global _find_debuginfo_opts -g
%global __brp_check_rpaths %{nil}
%define __strip ${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/devtools/bin/llvm-strip
%undefine __brp_mangle_shebangs
%global __requires_exclude ^\(/bin/bash\|/usr/bin/\.*\)$
%define __debug_install_post ${DEBUG_INSTALL_POST}
%if \\\"%name\\\" != \\\"oceanbase-ce-sql-parser\\\" && \\\"%name\\\" != \\\"oceanbase-sql-parser\\\"
%debug_package
%endif
")

# systemd define on rpm
if (OB_BUILD_OPENSOURCE)
  set(CPACK_RPM_SERVER_PACKAGE_REQUIRES "oceanbase-ce-libs = ${CPACK_PACKAGE_VERSION}, jq, systemd")

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_install.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_install.sh
                @ONLY)
  set(CPACK_RPM_SERVER_PRE_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_install.sh)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_install.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_install.sh
                @ONLY)
  set(CPACK_RPM_SERVER_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_install.sh)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_uninstall.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_uninstall.sh
                @ONLY)
  set(CPACK_RPM_SERVER_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_uninstall.sh)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_uninstall.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_uninstall.sh
                @ONLY)
  set(CPACK_RPM_SERVER_POST_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_uninstall.sh)
endif()

## server
if (NOT OB_BUILD_OPENSOURCE)
install(PROGRAMS
  ${DEVTOOLS_DIR}/bin/obstack
  DESTINATION bin
  COMPONENT server)
endif()

# add the rpm post and pre script
if (OB_BUILD_OPENSOURCE)
install(FILES
  tools/systemd/profile/pre_install.sh
  tools/systemd/profile/post_install.sh
  tools/systemd/profile/post_uninstall.sh
  tools/systemd/profile/pre_uninstall.sh
  DESTINATION profile
  COMPONENT server)
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
