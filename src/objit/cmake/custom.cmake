# Copyright (c) 2014-2016 Alibaba Inc. All Rights Reserved.
#
# Author:

# GET_LINK_LIBS(<target> <output variable>)
# Get link libs by traverse INTERFACE_LINK_LIBRARIES property.
FUNCTION (GET_LINK_LIBS tgt _result)
  GET_PROPERTY(libs TARGET ${tgt} PROPERTY INTERFACE_LINK_LIBRARIES SET)
  IF (${libs})
    GET_TARGET_PROPERTY(libs ${tgt} INTERFACE_LINK_LIBRARIES)
    FOREACH (lib ${libs})
      IF (TARGET ${lib})
        GET_LINK_LIBS(${lib} liblst)
        LIST(APPEND curlist "${liblst}")
      ENDIF ()
    ENDFOREACH ()
  ENDIF ()
  LIST(APPEND curlist ${tgt})
  LIST(REMOVE_DUPLICATES curlist)
  SET(${_result} ${curlist} PARENT_SCOPE)
ENDFUNCTION (GET_LINK_LIBS)
