#ifndef __RAFT_TEST_UTILITIES_HH__
#define __RAFT_TEST_UTILITIES_HH__

#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "boost/dynamic_bitset.hpp"
#include "boost/format.hpp"
#include "boost/test/test_tools.hpp"
#include "boost/variant.hpp"

#include "native/messages.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "protocol.hh"
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
  
      typedef boost::variant<typename _Messages::vote_request_traits_type::arg_type, typename _Messages::vote_response_traits_type::arg_type,
                             typename _Messages::append_entry_request_traits_type::arg_type, typename _Messages::append_entry_response_traits_type::arg_type,
                             typename _Messages::append_checkpoint_chunk_request_traits_type::arg_type, typename _Messages::append_checkpoint_chunk_response_traits_type::arg_type> any_msg_type;
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

    struct log_header_write_test : public raft::log_header_write
    {
      uint64_t current_term_ = std::numeric_limits<uint64_t>::max();
      uint64_t voted_for_ = std::numeric_limits<uint64_t>::max();

      void async_write_log_header(uint64_t current_term, uint64_t voted_for) override
      {
        current_term_ = current_term;
        voted_for_ = voted_for;
      }

      bool empty() const
      {
        return current_term_ == std::numeric_limits<uint64_t>::max() &&
          voted_for_ == std::numeric_limits<uint64_t>::max();
      }

      void reset()
      {
        current_term_ = std::numeric_limits<uint64_t>::max();
        voted_for_ = std::numeric_limits<uint64_t>::max();
      }
    };

    inline int32_t string_slice_compare(std::string_view str, raft::slice && sl)
    {
      auto cmp_len = (std::min)(str.size(), sl.size());
      auto cmp = ::memcmp(str.data(), sl.data(), cmp_len);
      if (cmp != 0) return cmp;
      if (str.size() < sl.size()) return -1;
      if (str.size() > sl.size()) return 1;
      return 0;
    }

    template<typename _Messages>
    class client : public raft::client_completion_operation<_Messages>
    {
    public:
      typedef _Messages messages_type;
      typedef typename messages_type::simple_configuration_description_type simple_configuration_description_type;
      typedef typename messages_type::client_result_type client_result_type;
      std::deque<raft::native::client_response> responses;
      std::deque<raft::native::set_configuration_response> configuration_responses;

      client()
        :
        client_completion_operation<_Messages>(&do_client_complete)
      {
      }
      static raft::native::client_result convert(client_result_type result)
      {
	if (_Messages::client_result_success() == result) {
	  return raft::native::client_result::SUCCESS;
	} else if (_Messages::client_result_fail() == result) {
	  return raft::native::client_result::FAIL;
	} else if (_Messages::client_result_not_leader() == result) {
	  return raft::native::client_result::NOT_LEADER;
	} else if (_Messages::client_result_session_expired() == result) {
	  return raft::native::client_result::SESSION_EXPIRED;
	} else {
	  return raft::native::client_result::RETRY;
	}
      }
      void on_configuration_response(client_result_type result)
      {
	raft::native::set_configuration_response resp;
	resp.result = convert(result);
	configuration_responses.push_front(resp);
      }
    
      void on_configuration_response(client_result_type result, std::vector<std::pair<uint64_t, std::string>> && bad_servers)
      {
	raft::native::set_configuration_response resp;
	resp.result = convert(result);
	for(const auto & bs : bad_servers) {
	  resp.bad_servers.servers.push_back({bs.first, bs.second});
	}
	configuration_responses.push_front(resp);
      }
      static void do_client_complete(client_completion_operation<_Messages> * base,
                                     client_result_type result,
                                     std::vector<std::pair<uint64_t, std::string>> && bad_servers)
      {
        client * cli(static_cast<client *>(base));
        cli->on_configuration_response(result, std::move(bad_servers));
      }      
    };
    
    enum class TestFixtureInitialization { LOG, CHECKPOINT, EMPTY };

    template<typename _TestType>
    class RaftTestFixtureBase
    {
    public:
      typedef typename _TestType::messages_type messages_type;
      typedef typename _TestType::messages_type::client_result_type client_result_type;
      typedef typename _TestType::messages_type::vote_request_traits_type::arg_type vote_request_arg_type;
      typedef typename _TestType::messages_type::vote_request_traits_type vote_request_traits;
      typedef typename _TestType::builders_type::vote_request_builder_type vote_request_builder;
      typedef typename _TestType::messages_type::vote_response_traits_type vote_response_traits;
      typedef typename _TestType::messages_type::vote_response_traits_type::arg_type vote_response_arg_type;
      typedef typename _TestType::builders_type::vote_response_builder_type vote_response_builder;
      typedef typename _TestType::messages_type::append_entry_request_traits_type append_entry_request_traits;
      typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
      typedef typename _TestType::builders_type::append_entry_request_builder_type append_entry_request_builder;
      typedef typename _TestType::messages_type::append_entry_response_traits_type append_entry_response_traits;
      typedef typename _TestType::messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
      typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
      typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits;
      typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_request_arg_type;
      typedef typename _TestType::builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
      typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type append_checkpoint_chunk_response_traits;
      typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;
      typedef typename _TestType::builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
      typedef typename _TestType::messages_type::checkpoint_header_traits_type checkpoint_header_traits;
      typedef typename _TestType::messages_type::log_entry_traits_type log_entry_traits;
      typedef typename _TestType::messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
      typedef typename _TestType::messages_type::server_description_traits_type server_description_traits;
      typedef typename _TestType::messages_type::simple_configuration_description_traits_type simple_configuration_description_traits;
      typedef typename _TestType::messages_type::configuration_description_traits_type configuration_description_traits;
      typedef typename _TestType::messages_type::set_configuration_request_traits_type set_configuration_request_traits;
      typedef typename _TestType::builders_type::set_configuration_request_builder_type set_configuration_request_builder;
      typedef typename _TestType::messages_type::get_configuration_request_traits_type get_configuration_request_traits;
      typedef typename _TestType::builders_type::get_configuration_request_builder_type get_configuration_request_builder;
      typedef typename _TestType::messages_type::get_configuration_response_traits_type get_configuration_response_traits;
      typedef typename _TestType::builders_type::get_configuration_response_builder_type get_configuration_response_builder;
      typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
      typedef typename _TestType::builders_type::linearizable_command_request_builder_type linearizable_command_request_builder;
      typedef typename _TestType::builders_type::open_session_request_builder_type open_session_request_builder;
      typedef typename _TestType::serialization_type serialization_type;
      typedef raft::protocol<generic_communicator_metafunction, typename _TestType::messages_type> raft_type;
      typedef client<typename _TestType::messages_type> client_type;
      std::size_t cluster_size;
      typename raft_type::communicator_type comm;
      client_type c;
      typename raft_type::log_type l;
      typename raft_type::checkpoint_data_store_type store;
      std::shared_ptr<typename raft_type::configuration_manager_type> cm;
      std::shared_ptr<raft_type> protocol;
      append_checkpoint_chunk_request_arg_type five_servers;
      log_header_write_test log_header_write_;
      std::vector<uint8_t> checkpoint_load_state;

      uint64_t initial_cluster_time;
      std::chrono::time_point<std::chrono::steady_clock> now;

      // append_checkpoint_chunk_request_arg_type six_servers;
      template<typename _Builder>
      void add_five_servers(_Builder b)
      {
        b.server().id(0).address("192.168.1.1");
        b.server().id(1).address("192.168.1.2");
        b.server().id(2).address("192.168.1.3");
        b.server().id(3).address("192.168.1.4");
        b.server().id(4).address("192.168.1.5"); 
      }
      template<typename _Builder>
      void add_six_servers(_Builder b)
      {
        b.server().id(0).address("192.168.1.1");
        b.server().id(1).address("192.168.1.2");
        b.server().id(2).address("192.168.1.3");
        b.server().id(3).address("192.168.1.4");
        b.server().id(4).address("192.168.1.5"); 
        b.server().id(5).address("192.168.1.6"); 
      }

      RaftTestFixtureBase(TestFixtureInitialization init=TestFixtureInitialization::CHECKPOINT)
        :
        cluster_size(5),
        cm(new typename raft_type::configuration_manager_type(0)),
        initial_cluster_time(0),
        now(std::chrono::steady_clock::now())
      {
        // Most of the checkpoint tests expect this block size
        store.block_size(2);
        // Glue log to log_header_write
        l.set_log_header_writer(&log_header_write_);
    
        if (init == TestFixtureInitialization::CHECKPOINT) {
          // Builder interface only supports creating a checkpoint header in the context of an append_checkpoint_chunk message
          {
            append_checkpoint_chunk_request_builder accb;
            {
              auto chb = accb.last_checkpoint_header();
              {
                auto cdb = chb.index(0).log_entry_index_end(0).last_log_entry_term(0).last_log_entry_cluster_time(0).configuration();
                {
                  add_five_servers(cdb.from());
	    
                }
                {
                  auto fsb = cdb.to();
                }
              }
            }
            five_servers = accb.finish();
          }
          cm->set_checkpoint(append_checkpoint_chunk_request_traits::last_checkpoint_header(five_servers), now);
          BOOST_CHECK(cm->configuration().is_valid());
          BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
          BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
          BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
          BOOST_CHECK(cm->configuration().includes_self());
          protocol.reset(new raft_type(comm, l, store, *cm.get(), now));
          BOOST_CHECK_EQUAL(0U, protocol->current_term());
          BOOST_CHECK_EQUAL(0U, protocol->cluster_time());
          BOOST_CHECK_EQUAL(0U, protocol->commit_index());
          BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
          BOOST_CHECK_EQUAL(0U, comm.q.size());
        } else if (init == TestFixtureInitialization::LOG) {
          initial_cluster_time = 253;
          log_entry_builder leb;
          {	
            auto cb = leb.term(0).cluster_time(initial_cluster_time).configuration();
            add_five_servers(cb.from());
            cb.to();
          }
          l.append(leb.finish());
          l.update_header(0, raft_type::INVALID_PEER_ID());
          protocol.reset(new raft_type(comm, l, store, *cm.get(), now));
          BOOST_CHECK(cm->configuration().is_valid());
          BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
          BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
          BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
          BOOST_CHECK(cm->configuration().includes_self());
          BOOST_CHECK_EQUAL(0U, protocol->current_term());
          BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
          BOOST_CHECK_EQUAL(0U, protocol->commit_index());
          BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
          BOOST_CHECK_EQUAL(0U, comm.q.size());
          BOOST_CHECK_EQUAL(0U, l.index_begin());
          BOOST_CHECK_EQUAL(1U, l.index_end());
          BOOST_CHECK_EQUAL(initial_cluster_time, l.last_entry_cluster_time());
        } else if (init == TestFixtureInitialization::EMPTY) {
          l.update_header(0, raft_type::INVALID_PEER_ID());
          protocol.reset(new raft_type(comm, l, store, *cm.get(), now));
          BOOST_CHECK(!cm->configuration().is_valid());
          BOOST_CHECK_EQUAL(std::numeric_limits<uint64_t>::max(), cm->configuration().configuration_id());
          BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
          BOOST_CHECK_EQUAL(0U, cm->configuration().num_known_peers());
          BOOST_CHECK(!cm->configuration().includes_self());
          BOOST_CHECK_EQUAL(0U, protocol->current_term());
          BOOST_CHECK_EQUAL(0U, protocol->cluster_time());
          BOOST_CHECK_EQUAL(0U, protocol->commit_index());
          BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
          BOOST_CHECK_EQUAL(0U, comm.q.size());
          BOOST_CHECK_EQUAL(0U, l.index_begin());
          BOOST_CHECK_EQUAL(0U, l.index_end());
        }
        protocol->set_state_machine_for_checkpoint([this](raft::checkpoint_block b, bool is_final) {
                                                     if (!b.is_null()) {
                                                       auto buf = reinterpret_cast<const uint8_t *>(b.data());
                                                       this->checkpoint_load_state.insert(this->checkpoint_load_state.end(), buf, buf+b.size());
                                                     } else {
                                                       this->checkpoint_load_state.clear();
                                                     }
                                                   });
      }
      ~RaftTestFixtureBase() {}

      void make_leader(uint64_t term, bool respond_to_noop=true);
      void make_follower_with_checkpoint(uint64_t term, uint64_t log_entry);
      void become_follower_with_vote_request(uint64_t term);
      void become_follower_with_vote_request(uint64_t term, uint64_t log_index_end, uint64_t last_log_term);
      void send_noop(uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end);
      void send_open_session(uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end);
      void send_linearizable_command(uint64_t session_id, uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end, raft::slice && cmd);
      void send_append_entry(uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end,
                             const std::pair<log_entry_const_arg_type, raft::util::call_on_delete > & le);
      void commit_one_log_entry(uint64_t term, uint64_t client_index);
      void send_heartbeats();
      void send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index);
      void send_client_request(uint64_t term, const char * cmd, uint64_t client_index, const boost::dynamic_bitset<> & send_responses_from);
      void send_client_request(uint64_t term, const char * cmd, uint64_t client_index,
                               const boost::dynamic_bitset<> & expect_append_entries_for,
                               const boost::dynamic_bitset<> & send_responses_from);
      std::size_t num_known_peers() { return cm->configuration().num_known_peers(); }
      void stage_new_server(uint64_t term, uint64_t commit_index);
      void check_configuration_servers(uint64_t term, uint64_t request_id, uint64_t configuration_id, std::size_t num_servers);
      void check_configuration_retry(uint64_t term, uint64_t request_id);
    };

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::make_leader(uint64_t term, bool respond_to_noop)
    {
      now += std::chrono::milliseconds(500);
      protocol->on_timer(now);
      // TODO: Check this once we start using a synthetic clock in tests
      auto cluster_time = protocol->cluster_time();
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(raft_type::CANDIDATE, protocol->get_state());
      BOOST_CHECK(protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(term, log_header_write_.current_term_);
      BOOST_CHECK_EQUAL(0U, log_header_write_.voted_for_);
      log_header_write_.reset();
      protocol->on_log_header_sync(now);
      BOOST_CHECK(!protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
      BOOST_CHECK(log_header_write_.empty());
      while(comm.q.size() > 0) {
        comm.q.pop_back();
      }
      for(uint64_t p=1; p!=num_known_peers(); ++p) {
        auto vote_response_msg = vote_response_builder().peer_id(p).term_number(term).request_term_number(term).granted(true).finish();
        protocol->on_vote_response(std::move(vote_response_msg), now);
      }
      BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      BOOST_TEST(cluster_time < protocol->cluster_time());
      BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
      for(uint64_t p=1; p!=num_known_peers(); ++p) {
        BOOST_CHECK(log_entry_traits::is_noop(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
        if (respond_to_noop) {
          auto resp = append_entry_response_builder().recipient_id(p).term_number(term).request_term_number(term).index_begin(0).index_end(append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back()))+1).success(true).finish();
          protocol->on_append_entry_response(std::move(resp), now);
        }
        comm.q.pop_back();
      }
      // Update intial_cluster_time to current
      initial_cluster_time = protocol->cluster_time();
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::commit_one_log_entry(uint64_t term,
                                                              uint64_t client_index)
    {
      BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      BOOST_CHECK_EQUAL(0U, comm.q.size());
      // This will send append_entries request to everyone in the cluster
      // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
      protocol->on_timer(now);
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
      std::size_t expected = 1;
      while(comm.q.size() > 0) {
        BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(client_index, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
        // Can't really check this in general
        // BOOST_CHECK_EQUAL(client_index > 0 ? term : 0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(client_index, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(term, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
        auto resp = append_entry_response_builder().recipient_id(expected).term_number(term).request_term_number(term).index_begin(client_index).index_end(client_index+1).success(true).finish();
        protocol->on_append_entry_response(std::move(resp), now);
        expected += 1;
        comm.q.pop_back();
      }
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_heartbeats()
    {
      auto term = protocol->current_term();
      auto client_index = protocol->log_index_end();
      BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      BOOST_CHECK_EQUAL(0U, comm.q.size());
      // This will send append_entries request to everyone in the cluster
      // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
      protocol->on_timer(now);
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
      std::size_t expected = 1;
      while(comm.q.size() > 0) {
        BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
        auto resp = append_entry_response_builder().recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(client_index).success(true).finish();
        protocol->on_append_entry_response(std::move(resp), now);
        expected += 1;
        comm.q.pop_back();
      }
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index)
    {
      boost::dynamic_bitset<> responses;
      responses.resize(num_known_peers(), 1);
      send_client_request(term, cmd, client_index, responses);
      BOOST_CHECK_EQUAL(client_index+1, protocol->commit_index());
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_client_request(uint64_t term, const char * cmd, uint64_t client_index,
                                                             const boost::dynamic_bitset<> & send_responses_from)
    {
      boost::dynamic_bitset<> expected_append_entries;
      expected_append_entries.resize(num_known_peers(), 1);
      send_client_request(term, cmd, client_index, expected_append_entries, send_responses_from);      
      // auto initial_commit_index = protocol->commit_index();
      // // Fire off a client_request
      // BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      // protocol->on_command(raft::slice(reinterpret_cast<const uint8_t *>(cmd), ::strlen(cmd)), now);
      // BOOST_CHECK_EQUAL(term, protocol->current_term());
      // // TODO: Use synthetic time and be more precise about this
      // BOOST_TEST(initial_cluster_time < protocol->cluster_time());
      // BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      // BOOST_CHECK_EQUAL(0U, comm.q.size());
      // initial_cluster_time = protocol->cluster_time();
      // // This will send append_entries request to everyone in the cluster
      // // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
      // protocol->on_timer(now);
      // BOOST_CHECK_EQUAL(term, protocol->current_term());
      // BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      // BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      // BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
      // std::size_t expected = 1;
      // std::size_t num_responses = 0;
      // while(comm.q.size() > 0) {
      //   BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
      //   BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
      //   BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
      //   BOOST_CHECK_EQUAL(client_index, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
      //   // Can't really check this in general
      //   // BOOST_CHECK_EQUAL(client_index > 0 ? term : 0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
      //   BOOST_CHECK_EQUAL(client_index, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
      //   BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
      //   BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
      //   BOOST_CHECK_EQUAL(term, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
      //   BOOST_CHECK_EQUAL(0, string_slice_compare(cmd, log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0))));
      //   if (send_responses_from.test(expected)) {
      //     num_responses += 1;
      //   }
      //   auto resp = append_entry_response_builder().recipient_id(expected).term_number(term).request_term_number(term).index_begin(client_index).index_end(client_index+1).success(send_responses_from.test(expected)).finish();
      //   protocol->on_append_entry_response(std::move(resp), now);
      //   if (num_responses<3) {
      //     BOOST_CHECK_EQUAL(initial_commit_index, protocol->commit_index());
      //   } else {
      //     // Majority vote!
      //     BOOST_CHECK_EQUAL(initial_commit_index + 1U, protocol->commit_index());
      //   }
      //   expected += 1;
      //   comm.q.pop_back();
      // }
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_client_request(uint64_t term, const char * cmd, uint64_t client_index,
                                                             const boost::dynamic_bitset<> & expect_append_entries_for,
                                                             const boost::dynamic_bitset<> & send_responses_from)
    {
      auto initial_commit_index = protocol->commit_index();
      // Fire off a client_request
      now += std::chrono::milliseconds(1);
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      protocol->on_command(std::make_pair(raft::slice(reinterpret_cast<const uint8_t *>(cmd), ::strlen(cmd)), raft::util::call_on_delete()), now);
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      // TODO: Use synthetic time and be more precise about this
      BOOST_TEST(initial_cluster_time < protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      BOOST_CHECK_EQUAL(0U, comm.q.size());
      initial_cluster_time = protocol->cluster_time();
      // This will send append_entries request to everyone in the cluster
      // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
      protocol->on_timer(now);
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::LEADER, protocol->get_state());
      std::size_t num_responses = 0;
      BOOST_REQUIRE_EQUAL(num_known_peers(), expect_append_entries_for.size());
      // Number of set bits (ignoring the one at 0) should be equal to number of response
      BOOST_CHECK((expect_append_entries_for.test(0) && expect_append_entries_for.count() == comm.q.size()+1) ||
                  (!expect_append_entries_for.test(0) && expect_append_entries_for.count() == comm.q.size()));
      for(std::size_t expected=1; expected < num_known_peers(); ++expected) {
        if (!expect_append_entries_for.test(expected)) {
          continue;
        }
        BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(client_index, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
        // Can't really check this in general
        // BOOST_CHECK_EQUAL(client_index > 0 ? term : 0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(client_index, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
        BOOST_CHECK_EQUAL(term, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
        BOOST_CHECK_EQUAL(0, string_slice_compare(cmd, log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0))));
        if (send_responses_from.test(expected)) {
          num_responses += 1;
        }
        auto resp = append_entry_response_builder().recipient_id(expected).term_number(term).request_term_number(term).index_begin(client_index).index_end(client_index+1).success(send_responses_from.test(expected)).finish();
        protocol->on_append_entry_response(std::move(resp), now);
        if (num_responses<3) {
          BOOST_CHECK_EQUAL(initial_commit_index, protocol->commit_index());
        } else {
          // Majority vote!
          BOOST_CHECK_EQUAL(initial_commit_index + 1U, protocol->commit_index());
        }
        comm.q.pop_back();
      }
    }
    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::make_follower_with_checkpoint(uint64_t term, uint64_t log_entry)
    {
      {
        uint8_t data=0;
        append_checkpoint_chunk_request_builder bld;
        bld.recipient_id(0).term_number(term).leader_id(1).checkpoint_begin(0).checkpoint_end(1).checkpoint_done(true).data(raft::slice(&data, 1));
        {
          auto chb = bld.last_checkpoint_header();
          {
            auto cdb = chb.index(0).log_entry_index_end(log_entry).last_log_entry_term(term).configuration();
            {
              auto fsb = cdb.from();
            }
            {
              auto fsb = cdb.to();
            }
          }
        }
        auto msg = bld.finish();
        protocol->on_append_checkpoint_chunk_request(std::move(msg), now);
      }
      BOOST_CHECK(protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
      BOOST_CHECK_EQUAL(0U, comm.q.size());
      BOOST_CHECK_EQUAL(term, log_header_write_.current_term_);
      BOOST_CHECK_EQUAL(1U, log_header_write_.voted_for_);
      log_header_write_.reset();
      protocol->on_log_header_sync(now);
      BOOST_CHECK(!protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
      BOOST_REQUIRE_EQUAL(0U, comm.q.size());
      BOOST_CHECK(log_header_write_.empty());

      protocol->on_checkpoint_sync();
      BOOST_CHECK(!protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
      BOOST_REQUIRE_EQUAL(1U, comm.q.size());
      BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::recipient_id(boost::get<append_checkpoint_chunk_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_checkpoint_chunk_response_traits::term_number(boost::get<append_checkpoint_chunk_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_checkpoint_chunk_response_traits::request_term_number(boost::get<append_checkpoint_chunk_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::bytes_stored(boost::get<append_checkpoint_chunk_response_arg_type>(comm.q.back())));
      comm.q.pop_back();
      BOOST_CHECK(log_header_write_.empty());
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::become_follower_with_vote_request(uint64_t term)
    {
      become_follower_with_vote_request(term, 0, 0);
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::become_follower_with_vote_request(uint64_t term, uint64_t log_index_end, uint64_t last_log_term)
    {
      auto expected_vote = term < protocol->current_term() || !protocol->candidate_log_more_complete(log_index_end, last_log_term) ?
        raft_type::INVALID_PEER_ID() : 1U;
      auto msg = vote_request_builder().recipient_id(0).term_number(term).candidate_id(1).log_index_end(log_index_end).last_log_term(last_log_term).finish();
      protocol->on_vote_request(std::move(msg), now);
      BOOST_CHECK(protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
      BOOST_CHECK_EQUAL(0U, comm.q.size());
      BOOST_CHECK_EQUAL(term, log_header_write_.current_term_);
      BOOST_CHECK_EQUAL(expected_vote, log_header_write_.voted_for_);
      log_header_write_.reset();
      protocol->on_log_header_sync(now);
      BOOST_CHECK(!protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(initial_cluster_time, protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
      BOOST_CHECK_EQUAL(1U, comm.q.size());
      BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(term, vote_response_traits::term_number(boost::get<vote_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(term, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(comm.q.back())));
      // Don't worry about vote; it will be no unless the server's log was empty
      //BOOST_CHECK(vote_response_traits::granted(boost::get<vote_response_arg_type>(comm.q.back())));
      comm.q.pop_back();
      BOOST_CHECK(log_header_write_.empty());
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_noop(uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end)
    {
      auto le = log_entry_traits::create_noop(term, 23432343);
      send_append_entry(leader_id, term, log_index_begin, previous_log_term, leader_commit_index_end, le);
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_open_session(uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end)
    {
      auto lcmd = serialization_type::serialize_log_entry_command(open_session_request_builder().finish());
      auto le = log_entry_traits::create_command(term, 23432343, std::move(lcmd));
      send_append_entry(leader_id, term, log_index_begin, previous_log_term, leader_commit_index_end, le);
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_linearizable_command(uint64_t session_id, uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end, raft::slice && cmd)
    {
      auto lcmd = serialization_type::serialize_log_entry_command(linearizable_command_request_builder().session_id(session_id).first_unacknowledged_sequence_number(0).sequence_number(0).command(std::move(cmd)).finish());
      auto le = log_entry_traits::create_command(term, 23432343, std::move(lcmd));
      send_append_entry(leader_id, term, log_index_begin, previous_log_term, leader_commit_index_end, le);
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::send_append_entry(uint64_t leader_id, uint64_t term, uint64_t log_index_begin, uint64_t previous_log_term, uint64_t leader_commit_index_end,
                                                           const std::pair<log_entry_const_arg_type, raft::util::call_on_delete > & le)
    {
      auto entry_cluster_time = log_entry_traits::cluster_time(le.first);
      auto original_state = protocol->get_state();
      append_entry_request_builder bld;
      bld.request_id(992345).recipient_id(0).term_number(term).leader_id(leader_id).log_index_begin(log_index_begin).previous_log_term(previous_log_term).leader_commit_index_end(leader_commit_index_end).entry(le);
      auto msg = bld.finish();
      protocol->on_append_entry_request(std::move(msg), now);
      BOOST_CHECK((raft_type::FOLLOWER == original_state && !protocol->log_header_sync_required()) ||
                  (raft_type::FOLLOWER != original_state && protocol->log_header_sync_required()));
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_TEST(((protocol->log_header_sync_required() && initial_cluster_time == protocol->cluster_time()) ||
                  (!protocol->log_header_sync_required() && 23432343U == protocol->cluster_time())));
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
      BOOST_CHECK_EQUAL(0U, comm.q.size());
      BOOST_TEST(((protocol->log_header_sync_required() && term == log_header_write_.current_term_) ||
                  (!protocol->log_header_sync_required() && std::numeric_limits<uint64_t>::max() == log_header_write_.current_term_)));
      BOOST_TEST(raft_type::INVALID_PEER_ID(), log_header_write_.voted_for_);
      if (protocol->log_header_sync_required()) {
        log_header_write_.reset();
        protocol->on_log_header_sync(now);
      }
      BOOST_CHECK(!protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(term, protocol->current_term());
      BOOST_CHECK_EQUAL(entry_cluster_time, protocol->cluster_time());
      initial_cluster_time = entry_cluster_time;
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, protocol->get_state());
      BOOST_CHECK_EQUAL(leader_commit_index_end, protocol->commit_index());
      BOOST_CHECK_EQUAL(0U, comm.q.size());
      protocol->on_log_sync(log_index_begin+1, now);
      BOOST_CHECK_EQUAL(1U, comm.q.size());
      BOOST_REQUIRE(0U < comm.q.size());
      BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(992345U, append_entry_response_traits::request_id(boost::get<append_entry_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(log_index_begin, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(comm.q.back())));
      BOOST_CHECK_EQUAL(log_index_begin+1, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(comm.q.back())));
      BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(comm.q.back())));
      comm.q.pop_back();
      BOOST_CHECK(log_header_write_.empty());
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::stage_new_server(uint64_t term, uint64_t commit_index)
    {
      // Assumes that leader is 0
      uint64_t leader_id=0;
      // Assumes that everything in leader log is committed
      {
        set_configuration_request_builder bld;
        add_six_servers(bld.old_id(0).new_configuration());
        auto req = bld.finish();
        BOOST_CHECK_EQUAL(6U, simple_configuration_description_traits::size(&set_configuration_request_traits::new_configuration(req)));
        protocol->on_set_configuration(c, std::move(req), now);
        BOOST_CHECK_EQUAL(6U, num_known_peers());
      }
      auto new_server_id = num_known_peers()-1;
  
      // Run timer then we should get append_entries for the newly added server
      protocol->on_timer(now);
      BOOST_CHECK_EQUAL(1U, comm.q.size());
      while(comm.q.size() > 0) {
        BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(leader_id, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(commit_index, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
        BOOST_CHECK_EQUAL(commit_index, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
        auto resp = append_entry_response_builder().recipient_id(new_server_id).term_number(term).request_term_number(term).index_begin(0).index_end(commit_index).success(true).finish();
        protocol->on_append_entry_response(std::move(resp), now);
        comm.q.pop_back();
      }
      BOOST_CHECK(!cm->configuration().staging_servers_caught_up());
      BOOST_CHECK(cm->configuration().is_staging());
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::check_configuration_servers(uint64_t term, uint64_t request_id, uint64_t configuration_id, std::size_t num_servers)
    {
      auto client_index = this->protocol->commit_index();
      bool called = false;
      client_result_type cr = messages_type::client_result_fail();
      uint64_t config_id = std::numeric_limits<uint64_t>::max();
      std::vector<std::pair<uint64_t, std::string> > servers;
      auto cb = [&cr, &called, &config_id, &servers](client_result_type result, uint64_t id, std::vector<std::pair<uint64_t, std::string>> && cfg) {
                  cr = result;
                  called = true;
                  config_id = id;
                  servers = std::move(cfg);
                };
      
      BOOST_TEST(request_id == this->protocol->request_id());
      get_configuration_request_builder bld;
      auto req = bld.finish();
      this->protocol->on_get_configuration(std::move(cb), std::move(req), this->now);
      BOOST_TEST(request_id+1 == this->protocol->request_id());
      BOOST_TEST(!called);
      BOOST_CHECK_EQUAL(this->num_known_peers()-1, this->comm.q.size());
      std::size_t expected = 1;
      std::size_t quorum = this->num_known_peers()/2;
      while(this->comm.q.size() > 0) {
        auto req_request_id = append_entry_request_traits::request_id(boost::get<append_entry_request_arg_type>(this->comm.q.back()));
        BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
        BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(client_index).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
        if (expected >= quorum) {
          BOOST_TEST(called);
          BOOST_TEST(messages_type::client_result_success() == cr);
          BOOST_TEST(configuration_id == config_id);
          BOOST_TEST(num_servers == servers.size());
          for(std::size_t i = 0 ; i<servers.size(); ++i) {
            BOOST_TEST(servers[i].first == i);
            BOOST_TEST(servers[i].second == (boost::format("192.168.1.%1%") % (i+1)).str());
          }
        } else {
          BOOST_TEST(!called);
        }
        expected += 1;
        this->comm.q.pop_back();
      }      
    }

    template<typename _TestType>
    void RaftTestFixtureBase<_TestType>::check_configuration_retry(uint64_t term, uint64_t request_id)
    {
      auto client_index = this->protocol->commit_index();
      bool called = false;
      client_result_type cr = messages_type::client_result_fail();
      uint64_t config_id = std::numeric_limits<uint64_t>::max();
      std::vector<std::pair<uint64_t, std::string> > servers;
      auto cb = [&cr, &called, &config_id, &servers](client_result_type result, uint64_t id, std::vector<std::pair<uint64_t, std::string>> && cfg) {
                  cr = result;
                  called = true;
                  config_id = id;
                  servers = std::move(cfg);
                };
      BOOST_TEST(request_id == this->protocol->request_id());
      get_configuration_request_builder bld;
      auto req = bld.finish();
      this->protocol->on_get_configuration(std::move(cb), std::move(req), this->now);
      BOOST_TEST(request_id+1 == this->protocol->request_id());
      BOOST_TEST(!called);
      BOOST_CHECK_EQUAL(this->num_known_peers()-1, this->comm.q.size());
      std::size_t expected = 1;
      std::size_t quorum = this->num_known_peers()/2;
      while(this->comm.q.size() > 0) {
        auto req_request_id = append_entry_request_traits::request_id(boost::get<append_entry_request_arg_type>(this->comm.q.back()));
        BOOST_CHECK_EQUAL(this->protocol->request_id(), req_request_id);
        BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
        auto resp = append_entry_response_builder().request_id(req_request_id).recipient_id(expected).term_number(term).request_term_number(term).index_begin(0).index_end(client_index).success(true).finish();
        this->protocol->on_append_entry_response(std::move(resp), this->now);
        if (expected >= quorum) {
          BOOST_TEST(called);
          BOOST_TEST(messages_type::client_result_retry() == cr);
          BOOST_TEST(0 == config_id);
          BOOST_TEST(0U == servers.size());
        } else {
          BOOST_TEST(!called);
        }
        expected += 1;
        this->comm.q.pop_back();
      }      
    }
  }
}

#endif
