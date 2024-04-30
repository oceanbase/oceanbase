set(CPACK_GENERATOR "DEB")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEB_MAIN_COMPONENT "server")
set(CPACK_DEBIAN_SERVER_DEBUGINFO_PACKAGE ON)

include(cmake/Pack.cmake)

# rename server package name
set(CPACK_DEBIAN_SERVER_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
set(CPACK_DEBIAN_PACKAGE_RELEASE ${OB_RELEASEID})

find_program(LSB_RELEASE_EXEC lsb_release)
if(LSB_RELEASE_EXEC)
  execute_process(
    COMMAND ${LSB_RELEASE_EXEC} -is
    OUTPUT_VARIABLE DEBIAN_NAME
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  string(TOLOWER "${DEBIAN_NAME}" DEBIAN_NAME)
  execute_process(
    COMMAND ${LSB_RELEASE_EXEC} -rs
    OUTPUT_VARIABLE DEBIAN_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
endif(LSB_RELEASE_EXEC)
if(DEBIAN_NAME AND DEBIAN_VERSION)
  set(CPACK_DEBIAN_PACKAGE_RELEASE "${CPACK_DEBIAN_PACKAGE_RELEASE}${DEBIAN_NAME}${DEBIAN_VERSION}")
endif()

if (OB_DISABLE_LSE)
  ob_insert_nonlse_to_package_version(${CPACK_DEBIAN_PACKAGE_RELEASE} CPACK_DEBIAN_PACKAGE_RELEASE)
  message(STATUS "CPACK_DEBIAN_PACKAGE_RELEASE: ${CPACK_DEBIAN_PACKAGE_RELEASE}")
endif()

set(CPACK_DEBIAN_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
set(CPACK_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION})
set(CPACK_PACKAGE_CONTACT "${OceanBase_CE_HOMEPAGE_URL}")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "OceanBase")
set(CPACK_DEBIAN_PACKAGE_SECTION "database")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "Optional")

# systemd define on deb
if (OB_BUILD_OPENSOURCE)
  set(CPACK_DEBIAN_SERVER_PACKAGE_DEPENDS "oceanbase-ce-libs (= ${CPACK_PACKAGE_VERSION}-${CPACK_DEBIAN_PACKAGE_RELEASE})")
  set(CPACK_DEBIAN_SERVER_PACKAGE_DEPENDS "${CPACK_DEBIAN_SERVER_PACKAGE_DEPENDS}, jq, systemd")

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_install.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/preinst
                @ONLY)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_install.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/postinst
                @ONLY)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/pre_uninstall.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/prerm
                @ONLY)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/post_uninstall.sh.template
                ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/postrm
                @ONLY)

  set(CPACK_DEBIAN_SERVER_PACKAGE_CONTROL_EXTRA
    ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/postinst
    ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/prerm
    ${CMAKE_CURRENT_SOURCE_DIR}/tools/systemd/profile/postrm)
endif()

# add the deb post and pre script
if (OB_BUILD_OPENSOURCE)
install(FILES
  tools/systemd/profile/postinst
  tools/systemd/profile/prerm
  tools/systemd/profile/postrm
  DESTINATION profile
  COMPONENT server)
endif()

# install cpack to make everything work
include(CPack)

#add deb target to create DEBS
add_custom_target(deb
  COMMAND +make package
  DEPENDS
  observer obcdc_tailf obtable obtable_static
  ob_admin ob_error ob_sql_proxy_parser_static
  ${BITCODE_TO_ELF_LIST}
  )
