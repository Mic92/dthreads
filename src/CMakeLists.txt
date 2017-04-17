file(GLOB_RECURSE srcFiles "tthread/*.cpp")
add_library(tthread SHARED ${srcFiles})

set(TTHREAD_DEFINITIONS "")
if(CMAKE_BUILD_TYPE MATCHES DEBUG)
  set(TTHREAD_DEFINITIONS "${TTHREAD_DEFINITIONS};DDEBUG_ENABLED;DCHECK_SCHEDULE")
endif(CMAKE_BUILD_TYPE MATCHES DEBUG)
if(HAS_BUILTIN_RETURN_ADDRESS)
  set(TTHREAD_DEFINITIONS "${TTHREAD_DEFINITIONS};BUILTIN_RETURN_ADDRESS")
endif(HAS_BUILTIN_RETURN_ADDRESS)
# Get some characteristics about running.
#set(TTHREAD_DEFINITIONS "${TTHREAD_DEFINITIONS};-DGET_CHARACTERISTICS")

set(src ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(tthread PROPERTIES
  COMPILE_FLAGS "-O3 -g -Wall -Wextra -pedantic -fPIC -g -fvisibility=hidden -fvisibility-inlines-hidden"
  COMPILE_DEFINITIONS "SSE_SUPPORT;NDEBUG;LOCK_OWNERSHIP;DETERM_MEMORY_ALLOC;LOCK_OWNERSHIP;${TTHREAD_DEFINITIONS}"
  # By forcing to load all symbols at instant, we avoid segmentation faults in
  # GOT, when entries are updated in the segmentation fault handler.
  LINK_FLAGS -Wl,-z,now
  INCLUDE_DIRECTORIES "${src}/include;${src}/../include;${src}/include/heaplayers;${src}/include/heaplayers/util"
  COMPILE_FEATURES cxx_variadic_macros cxx_static_assert cxx_auto_type
  LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT};${LIBDL_LIBRARIES}"
)
target_link_libraries(tthread papi)

install(TARGETS tthread
  DESTINATION ${LIB_INSTALL_DIR}
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

add_subdirectory(schas)
