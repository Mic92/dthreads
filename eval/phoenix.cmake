set(PHOENIX_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/phoenix/phoenix-2.0/lib/libphoenix.a)

add_custom_target(
  git_submodule_init
  COMMAND git submodule update --init
  COMMENT "Checkout phoenix")

add_library(phoenix STATIC IMPORTED)
set_property(TARGET phoenix APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(phoenix PROPERTIES IMPORTED_LOCATION_NOCONFIG ${PHOENIX_LIBRARY})
add_custom_command(
   OUTPUT "${PHOENIX_LIBRARY}"
   COMMAND make
   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/phoenix/phoenix-2.0
   COMMENT "Libphoenix makefile target"
   DEPENDS git_submodule_init)
add_custom_target(libphoenix DEPENDS "${PHOENIX_LIBRARY}")
add_dependencies(phoenix libphoenix)

include(DownloadDataset)
include(AddPhoenixBenchmark)

set(PHOENIX_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(PHOENIX_SRC ${CMAKE_CURRENT_SOURCE_DIR}/tests)
DownloadDataset(http://csl.stanford.edu/%7Echristos/data/histogram.tar.gz
  histogram.tar.gz
  ${CMAKE_CURRENT_SOURCE_DIR}/datasets)
AddPhoenixBenchmark(histogram
  ARGS ${DATASET_HOME}/histogram_datafiles/large.bmp
  FILES ${PHOENIX_SRC}/histogram/histogram-pthread.c)

DownloadDataset(http://csl.stanford.edu/%7Echristos/data/linear_regression.tar.gz
  linear_regression.tar.gz
  ${CMAKE_CURRENT_SOURCE_DIR}/datasets)
AddPhoenixBenchmark(linear_regression
  ARGS ${DATASET_HOME}/linear_regression_datafiles/key_file_500MB.txt
  FILES ${PHOENIX_SRC}/linear_regression/linear_regression-pthread.c)

DownloadDataset(http://csl.stanford.edu/%7Echristos/data/reverse_index.tar.gz
  reverse_index.tar.gz
  ${CMAKE_CURRENT_SOURCE_DIR}/datasets)
AddPhoenixBenchmark(reverse_index
  ARGS ${DATASET_HOME}/reverse_index_datafiles
  FILES ${PHOENIX_SRC}/reverse_index/reverseindex-pthread.c)

DownloadDataset(http://csl.stanford.edu/%7Echristos/data/string_match.tar.gz
  string_match.tar.gz
  ${CMAKE_CURRENT_SOURCE_DIR}/datasets)
AddPhoenixBenchmark(string_match
  ARGS ${DATASET_HOME}/string_match_datafiles/key_file_500MB.txt
  FILES ${PHOENIX_SRC}/string_match/string_match-pthread.c)

DownloadDataset(http://csl.stanford.edu/%7Echristos/data/word_count.tar.gz
  word_count.tar.gz
  ${CMAKE_CURRENT_SOURCE_DIR}/datasets)
AddPhoenixBenchmark(word_count
  ARGS ${DATASET_HOME}/word_count_datafiles/word_100MB.txt
  FILES
    ${PHOENIX_SRC}/word_count/word_count-pthread.c
    ${PHOENIX_SRC}/word_count/sort-pthread.c)

DownloadDataset(http://csl.stanford.edu/~christos/sw/phoenix/phoenix_regular.tar.gz
  phoenix_regular.tar.gz
  ${CMAKE_CURRENT_SOURCE_DIR}/datasets)

AddPhoenixBenchmark(kmeans
  ARGS -d 3 -c 1000 -p 100000 -s 1000
  FILES ${PHOENIX_SRC}/kmeans/kmeans-pthread.c)

AddPhoenixBenchmark(matrix_multiply
  ARGS 2000 2000
  FILES ${PHOENIX_SRC}/matrix_multiply/matrix_multiply-pthread.c)

AddPhoenixBenchmark(pca
  ARGS -r 4000 -c 4000 -s 100
  FILES ${PHOENIX_SRC}/pca/pca-pthread.c)

set(phoenix_benchmarks histogram linear_regression reverse_index string_match word_count kmeans pca)

foreach(bench ${phoenix_benchmarks})
  list(APPEND phoenix_pthread_benchmarks bench-${bench}-pthread)
  list(APPEND phoenix_tthread_benchmarks bench-${bench}-tthread)
  list(APPEND phoenix_build_targets ${bench})
endforeach()

add_custom_target(phoenix-tthread DEPENDS ${phoenix_tthread_benchmarks})
add_custom_target(phoenix-pthread DEPENDS ${phoenix_pthread_benchmarks})
add_custom_target(build-phoenix DEPENDS ${phoenix_build_targets})
