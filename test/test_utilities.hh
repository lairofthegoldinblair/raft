#ifndef __RAFT_TEST_UTILITIES_HH__
#define __RAFT_TEST_UTILITIES_HH__

#include <deque>
#include <string>

#include "boost/variant.hpp"

#include "native/messages.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "util/builder_communicator.hh"

namespace raft {
  namespace test {
    template<typename _Messages>
    class builder_metafunction
    {
    public:
      typedef raft::native::builders type;
    };

    template<>
    class builder_metafunction<raft::fbs::messages>
    {
    public:
      typedef raft::fbs::builders type;
    };


    template<typename _Messages>
    class variant_base_communicator
    {
    public:
  
      typedef size_t endpoint;
      template<typename _T>
      void send(endpoint ep, const std::string& address, _T && msg)
      {
        q.push_front(std::move(msg));
      }
  
      typedef boost::variant<typename _Messages::request_vote_traits_type::arg_type, typename _Messages::vote_response_traits_type::arg_type,
                             typename _Messages::append_entry_traits_type::arg_type, typename _Messages::append_entry_response_traits_type::arg_type,
                             typename _Messages::append_checkpoint_chunk_traits_type::arg_type, typename _Messages::append_checkpoint_chunk_response_traits_type::arg_type> any_msg_type;
      std::deque<any_msg_type> q;
    };

    struct generic_communicator_metafunction
    {
      template <typename _Messages>
      struct apply
      {
        typedef raft::util::builder_communicator<_Messages, typename builder_metafunction<_Messages>::type, variant_base_communicator<_Messages>> type;
      };
    };

    struct native_client_metafunction
    {
      template <typename _Messages>
      struct apply
      {
        typedef raft::native::client<_Messages> type;
      };
    };
  }
}

#endif
