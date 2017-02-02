add_executable(raft-unit-test test.cc)

target_link_libraries( raft-unit-test  ${Boost_LOG_LIBRARY}
${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} )


