#ifndef __RAFT_TEST_UTILITIES_HH__
#define __RAFT_TEST_UTILITIES_HH__

#include <deque>
#include <string>

#include "boost/variant.hpp"

#include "native/messages.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"

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
    class generic_communicator
    {
    public:
      typedef typename builder_metafunction<_Messages>::type builders_type;
      typedef typename builders_type::request_vote_builder_type request_vote_builder;
      typedef typename builders_type::vote_response_builder_type vote_response_builder;
      typedef typename builders_type::append_entry_builder_type append_entry_builder;
      typedef typename builders_type::append_response_builder_type append_response_builder;
      typedef typename builders_type::append_checkpoint_chunk_builder_type append_checkpoint_chunk_builder;
      typedef typename builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
      typedef typename _Messages::checkpoint_header_traits_type checkpoint_header_traits;
  
      typedef size_t endpoint;
      template<typename _T>
      void send(endpoint ep, const std::string& address, _T && msg)
      {
        q.push_front(std::move(msg));
      }
  
      void vote_request(endpoint ep, const std::string & address,
                        uint64_t recipient_id,
                        uint64_t term_number,
                        uint64_t candidate_id,
                        uint64_t last_log_index,
                        uint64_t last_log_term)
      {
        auto msg = request_vote_builder().recipient_id(recipient_id).term_number(term_number).candidate_id(candidate_id).last_log_index(last_log_index).last_log_term(last_log_term).finish();
        send(ep, address, std::move(msg));	
      }

      template<typename EntryProvider>
      void append_entry(endpoint ep, const std::string& address,
                        uint64_t recipient_id,
                        uint64_t term_number,
                        uint64_t leader_id,
                        uint64_t previous_log_index,
                        uint64_t previous_log_term,
                        uint64_t leader_commit_index,
                        uint64_t num_entries,
                        EntryProvider entries)
      {
        append_entry_builder bld;
        bld.recipient_id(recipient_id).term_number(term_number).leader_id(leader_id).previous_log_index(previous_log_index).previous_log_term(previous_log_term).leader_commit_index(leader_commit_index);
        for(uint64_t i=0; i<num_entries; ++i) {
          bld.entry(entries(i));
        }
        auto tmp = bld.finish();
        q.push_front(std::move(tmp));
      }
	
      void append_entry_response(endpoint ep, const std::string& address,
                                 uint64_t recipient_id,
                                 uint64_t term_number,
                                 uint64_t request_term_number,
                                 uint64_t begin_index,
                                 uint64_t last_index,
                                 bool success)
      {
        auto msg = append_response_builder().recipient_id(recipient_id).term_number(term_number).request_term_number(request_term_number).begin_index(begin_index).last_index(last_index).success(success).finish();
        q.push_front(std::move(msg));
      }

      void vote_response(endpoint ep, const std::string& address,
                         uint64_t peer_id,
                         uint64_t term_number,
                         uint64_t request_term_number,
                         bool granted)
      {
        auto msg = vote_response_builder().peer_id(peer_id).term_number(term_number).request_term_number(request_term_number).granted(granted).finish();
        q.push_front(std::move(msg));
      }

      void append_checkpoint_chunk(endpoint ep, const std::string& address,
                                   uint64_t recipient_id,
                                   uint64_t term_number,
                                   uint64_t leader_id,
                                   const typename _Messages::checkpoint_header_type & last_checkpoint_header,
                                   uint64_t checkpoint_begin,
                                   uint64_t checkpoint_end,
                                   bool checkpoint_done,
                                   raft::slice && data)
      {
        append_checkpoint_chunk_builder bld;
        bld.recipient_id(recipient_id).term_number(term_number).leader_id(leader_id).checkpoint_begin(checkpoint_begin).checkpoint_end(checkpoint_end).checkpoint_done(checkpoint_done).data(std::move(data));
        {
          auto chb = bld.last_checkpoint_header();
          chb.last_log_entry_index(checkpoint_header_traits::last_log_entry_index(&last_checkpoint_header));
          chb.last_log_entry_term(checkpoint_header_traits::last_log_entry_term(&last_checkpoint_header));
          chb.last_log_entry_cluster_time(checkpoint_header_traits::last_log_entry_cluster_time(&last_checkpoint_header));
          chb.index(checkpoint_header_traits::index(&last_checkpoint_header));
          chb.configuration(checkpoint_header_traits::configuration(&last_checkpoint_header));
        }
        q.push_front(bld.finish());
      }		       
  
      void append_checkpoint_chunk_response(endpoint ep, const std::string& address,
                                            uint64_t recipient_id,
                                            uint64_t term_number,
                                            uint64_t request_term_number,
                                            uint64_t bytes_stored)
      {
        auto msg = append_checkpoint_chunk_response_builder().recipient_id(recipient_id).term_number(term_number).request_term_number(request_term_number).bytes_stored(bytes_stored).finish();
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
        typedef generic_communicator<_Messages> type;
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
