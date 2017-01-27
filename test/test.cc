#include <chrono>
#include <thread>

#include "server.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>

#define BOOST_TEST_MODULE RaftTests
#include <boost/test/unit_test.hpp>

struct init_logging
{
  init_logging() {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::debug);
  }
};

init_logging il;

template<typename _T>
struct test_peer_T
{
  uint64_t peer_id;
  uint64_t match_index;
  boost::logic::tribool vote_;
  bool exited_;
  raft::server_description::address_type address;
  std::shared_ptr<_T> configuration_change_;  
  void exit()
  {
    exited_ = true;
  }
};

struct test_peer_metafunction
{
  template<typename _T>
  struct apply
  {
    typedef test_peer_T<_T> type;
  };
};

struct test_peer : public test_peer_T<raft::peer_configuration_change> {};

class RaftSimpleConfigurationTestFixture
{
public:
  raft::simple_configuration<test_peer> c;

  void make_configuration(std::size_t sz);
  void make_configuration_with_holes(std::size_t sz);
};

void RaftSimpleConfigurationTestFixture::make_configuration(std::size_t sz)
{
  for (std::size_t i=0; i<sz; ++i) {
    c.peers_.push_back(std::shared_ptr<test_peer>(new test_peer()));
    c.peers_.back()->peer_id=i;
    c.peers_.back()->match_index=0;
    c.peers_.back()->exited_=false;
  }
}

void RaftSimpleConfigurationTestFixture::make_configuration_with_holes(std::size_t sz)
{
  for (std::size_t i=0; i<sz; ++i) {
    c.peers_.push_back(std::shared_ptr<test_peer>(new test_peer()));
    c.peers_.back()->peer_id=2*i;
    c.peers_.back()->match_index=0;
    c.peers_.back()->exited_=false;
  }
}
BOOST_FIXTURE_TEST_CASE(MajorityVoteOdd, RaftSimpleConfigurationTestFixture)
{
  make_configuration(5);
  for(std::size_t i=0; i<5; ++i) {
    BOOST_CHECK(!c.has_majority_vote(i));
  }
  c.peers_[1]->vote_ = true;
  for(std::size_t i=0; i<5; ++i) {
    BOOST_CHECK(!c.has_majority_vote(i));
  }
  c.peers_[2]->vote_ = true;
  BOOST_CHECK(c.has_majority_vote(0));
  BOOST_CHECK(!c.has_majority_vote(1));
  BOOST_CHECK(!c.has_majority_vote(2));
  BOOST_CHECK(c.has_majority_vote(3));
  BOOST_CHECK(c.has_majority_vote(4));

  c.peers_[3]->vote_ = false;
  BOOST_CHECK(c.has_majority_vote(0));
  BOOST_CHECK(!c.has_majority_vote(1));
  BOOST_CHECK(!c.has_majority_vote(2));
  BOOST_CHECK(c.has_majority_vote(3));
  BOOST_CHECK(c.has_majority_vote(4));

  c.peers_[4]->vote_ = true;
  for(std::size_t i=0; i<5; ++i) {
    BOOST_CHECK(c.has_majority_vote(i));
  }
}

BOOST_FIXTURE_TEST_CASE(MajorityVoteEven, RaftSimpleConfigurationTestFixture)
{
  make_configuration(4);
  for(std::size_t i=0; i<4; ++i) {
    BOOST_CHECK(!c.has_majority_vote(i));
  }
  c.peers_[1]->vote_ = true;
  for(std::size_t i=0; i<4; ++i) {
    BOOST_CHECK(!c.has_majority_vote(i));
  }
  c.peers_[2]->vote_ = true;
  BOOST_CHECK(c.has_majority_vote(0));
  BOOST_CHECK(!c.has_majority_vote(1));
  BOOST_CHECK(!c.has_majority_vote(2));
  BOOST_CHECK(c.has_majority_vote(3));

  c.peers_[3]->vote_ = true;
  for(std::size_t i=0; i<4; ++i) {
    BOOST_CHECK(c.has_majority_vote(i));
  }

  raft::configuration_description desc;
  desc.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}};
  raft::configuration<test_peer, raft::configuration_description> config(0);
  BOOST_CHECK_EQUAL(0U, config.num_known_peers());
  BOOST_CHECK_EQUAL(0U, config.my_cluster_id());
  BOOST_CHECK(!config.includes_self());
  config.set_configuration(1, desc);
  BOOST_CHECK_EQUAL(3U, config.num_known_peers());
  BOOST_CHECK_EQUAL(0U, config.my_cluster_id());
  BOOST_CHECK_EQUAL(1U, config.configuration_id());
  BOOST_CHECK(config.includes_self());
  BOOST_CHECK_EQUAL(0U, config.self().peer_id);
  BOOST_CHECK(boost::algorithm::equals("192.168.1.1", config.self().address));
  BOOST_CHECK_EQUAL(1U, config.peer_from_id(1).peer_id);
  BOOST_CHECK(boost::algorithm::equals("192.168.1.2", config.peer_from_id(1).address));
  BOOST_CHECK_EQUAL(2U, config.peer_from_id(2).peer_id);
  BOOST_CHECK(boost::algorithm::equals("192.168.1.3", config.peer_from_id(2).address));
}

// TODO: Unit tests for match index quorum

BOOST_FIXTURE_TEST_CASE(IncludesTest, RaftSimpleConfigurationTestFixture)
{
  make_configuration_with_holes(4);
  for(std::size_t i=0; i<4; ++i) {
    BOOST_CHECK(c.includes(2*i));
  }
  for(std::size_t i=0; i<=4; ++i) {
    BOOST_CHECK(!c.includes(2*i+1));
  }
}

BOOST_AUTO_TEST_CASE(BasicConfigurationManagerTests)
{
  raft::configuration_manager<test_peer_metafunction, raft::configuration_description> mgr(0);
  for(uint64_t i=0; i<20; ++i) {
    BOOST_CHECK(!mgr.has_configuration_at(i));
  }
  raft::configuration_description desc;
  desc.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}};
  mgr.add_logged_description(5, desc);
  BOOST_CHECK_EQUAL(5U, mgr.configuration().configuration_id());
  BOOST_CHECK_EQUAL(3U, mgr.configuration().num_known_peers());
  for(uint64_t i=0; i<5; ++i) {
    BOOST_CHECK(!mgr.has_configuration_at(i));
  }
  for(uint64_t i=5; i<20; ++i) {
    BOOST_CHECK(mgr.has_configuration_at(i));
  }
  raft::configuration_description desc2;
  desc2.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"}};
  mgr.add_logged_description(10, desc2);
  BOOST_CHECK_EQUAL(10U, mgr.configuration().configuration_id());
  BOOST_CHECK_EQUAL(4U, mgr.configuration().num_known_peers());
  for(uint64_t i=0; i<5; ++i) {
    BOOST_CHECK(!mgr.has_configuration_at(i));
  }
  for(uint64_t i=5; i<10; ++i) {
    BOOST_CHECK(mgr.has_configuration_at(i));
    raft::configuration_checkpoint<raft::configuration_description> ckpt;
    mgr.get_checkpoint_state(i, ckpt);
    BOOST_CHECK_EQUAL(5U, ckpt.index);
  }
  for(uint64_t i=10; i<20; ++i) {
    BOOST_CHECK(mgr.has_configuration_at(i));
    raft::configuration_checkpoint<raft::configuration_description> ckpt;
    mgr.get_checkpoint_state(i, ckpt);
    BOOST_CHECK_EQUAL(10U, ckpt.index);
  }
}

BOOST_AUTO_TEST_CASE(ConfigurationManagerSetCheckpointTests)
{
  raft::configuration_manager<test_peer_metafunction, raft::configuration_description> cm(0);
  raft::configuration_manager<test_peer_metafunction, raft::configuration_description>::checkpoint_type ckpt;
  BOOST_CHECK(!ckpt.is_valid());
  ckpt.index = 0;
  ckpt.description.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  BOOST_CHECK(ckpt.is_valid());
  cm.set_checkpoint(ckpt);
  BOOST_CHECK_EQUAL(0U, cm.configuration().configuration_id());
  BOOST_CHECK_EQUAL(0U, cm.configuration().my_cluster_id());
  BOOST_CHECK_EQUAL(5U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  BOOST_CHECK(cm.has_configuration_at(0));
}

BOOST_AUTO_TEST_CASE(BasicStateMachineTests)
{
  // std::size_t cluster_size(5);
  // std::vector<raft::server::peer_type> peers(cluster_size);
  // for(std::size_t i=0; i<cluster_size; ++i) {
  //   peers[i].peer_id = i;
  // }
  // raft::server::configuration_manager_type cm(0, peers);

  raft::server::configuration_manager_type cm(0);
  raft::server::configuration_manager_type::checkpoint_type ckpt;
  ckpt.index = 0;
  ckpt.description.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  cm.set_checkpoint(ckpt);
  BOOST_CHECK_EQUAL(0U, cm.configuration().configuration_id());
  BOOST_CHECK_EQUAL(5U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  std::size_t cluster_size = cm.configuration().num_known_peers();

  raft::server::communicator_type comm;
  raft::server::client_type c;
  raft::server::log_type l;
  raft::server::checkpoint_data_store_type store;
 
  raft::server s(comm, c, l, store, cm);
  BOOST_CHECK_EQUAL(0U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(1U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::CANDIDATE, s.get_state());
  BOOST_CHECK(s.log_header_sync_required());
  s.on_log_header_sync();
  BOOST_CHECK(!s.log_header_sync_required());
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(1).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(cluster_size - 1, comm.q.size());
  std::size_t expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(0U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::request_vote>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::request_vote>(comm.q.back()).candidate_id);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::request_vote>(comm.q.back()).term_number);
    expected += 1;
    comm.q.pop_back();
  }

  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::CANDIDATE, s.get_state());
  BOOST_CHECK(s.log_header_sync_required());
  s.on_log_header_sync();
  BOOST_CHECK(!s.log_header_sync_required());
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(1).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(cluster_size - 1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(0U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::request_vote>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::request_vote>(comm.q.back()).candidate_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::request_vote>(comm.q.back()).term_number);
    expected += 1;
    comm.q.pop_back();
  }

  // Provide one vote
  raft::vote_response vote_response_msg;
  vote_response_msg.peer_id = 1;
  vote_response_msg.term_number = 2;
  vote_response_msg.request_term_number = 2;
  vote_response_msg.granted = true;
  s.on_vote_response(vote_response_msg);
  BOOST_CHECK_EQUAL(true, s.get_peer_from_id(1).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::CANDIDATE, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Any vote from prior term should be ignored
  vote_response_msg.peer_id = 2;
  vote_response_msg.term_number = 1;
  vote_response_msg.request_term_number = 1;
  vote_response_msg.granted = true;
  s.on_vote_response(vote_response_msg);
  BOOST_CHECK_EQUAL(true, s.get_peer_from_id(1).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::CANDIDATE, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Valid vote! Become leader
  vote_response_msg.peer_id = 2;
  vote_response_msg.term_number = 2;
  vote_response_msg.request_term_number = 2;
  vote_response_msg.granted = true;
  s.on_vote_response(vote_response_msg);
  BOOST_CHECK_EQUAL(true, s.get_peer_from_id(1).vote_);
  BOOST_CHECK_EQUAL(true, s.get_peer_from_id(2).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(raft::server::log_entry_type::NOOP, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].type);
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 2;
    resp.request_term_number = 2;
    resp.begin_index = 0;
    resp.last_index = 1;
    resp.success = true;
    s.on_append_response(resp);
    if (expected<3) {
      BOOST_CHECK_EQUAL(0U, s.commit_index());
    } else {
      BOOST_CHECK_EQUAL(1U, s.commit_index());
    }
    expected += 1;
    comm.q.pop_back();
  }

  // Nothing should change here
  std::this_thread::sleep_for (std::chrono::milliseconds(1));
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  
  // Wait a bit and should get a round of heartbeat messages
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    // TODO: What about the next 3 values ????
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    expected += 1;
    comm.q.pop_back();
  }

  // Old append_entry should elicit a response with updated term
  raft::server::append_entry_type ae_msg;
  ae_msg.recipient_id = 0;
  ae_msg.term_number = 1;
  ae_msg.leader_id = 0;
  ae_msg.previous_log_index = ae_msg.previous_log_term = ae_msg.leader_commit_index = 0;
  ae_msg.entry.resize(0);
  s.on_append_entry(ae_msg);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(3U, comm.q.back().which());
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_response>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
    comm.q.pop_back();
  }

  // Fire off a client_request
  raft::client_request cli_req;
  cli_req.id = 1;
  cli_req.command = "1";
  s.on_client_request(cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("1", boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 2;
    resp.request_term_number = 2;
    resp.begin_index = 1;
    resp.last_index = 2;
    resp.success = true;
    s.on_append_response(resp);
    if (expected!=3) {
      BOOST_CHECK_EQUAL(0U, c.responses.size());
    } else {
      // Majority vote!
      BOOST_CHECK_EQUAL(1U, c.responses.size());
      BOOST_CHECK_EQUAL(1U, c.responses.back().id);
      c.responses.pop_back();
    }
    if (expected<3) {
      BOOST_CHECK_EQUAL(1U, s.commit_index());
    } else {
      BOOST_CHECK_EQUAL(2U, s.commit_index());
    }
    expected += 1;
    comm.q.pop_back();
  }

  // Fire off two client requests
  cli_req.id = 2;
  cli_req.command = "2";
  s.on_client_request(cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  cli_req.id = 3;
  cli_req.command = "3";
  s.on_client_request(cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log has sync'd to disk so we only need 2
  // other members of the cluster to ack
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  s.on_log_sync(4);
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("2", boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
    BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[1].type);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[1].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("3", boost::get<raft::server::append_entry_type>(comm.q.back()).entry[1].data.c_str()));
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 2;
    resp.request_term_number = 2;
    resp.begin_index = 2;
    resp.last_index = 4;
    resp.success = true;
    if (expected<=2) {
      s.on_append_response(resp);
    }
    if (expected!=2) {
      BOOST_CHECK_EQUAL(0U, c.responses.size());
    } else {
      // Majority vote!
      BOOST_CHECK_EQUAL(2U, c.responses.size());
      BOOST_CHECK_EQUAL(2U, c.responses.back().id);
      c.responses.pop_back();
      BOOST_CHECK_EQUAL(3U, c.responses.back().id);
      c.responses.pop_back();
    }
    if (expected<2) {
      BOOST_CHECK_EQUAL(2U, s.commit_index());
    } else {
      BOOST_CHECK_EQUAL(4U, s.commit_index());
    }
    expected += 1;
    comm.q.pop_back();
  }

  // We skipped append_response from peers 3,4 in the last go round so they need resending of entries
  cli_req.id = 4;
  cli_req.command = "4";
  s.on_client_request(cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(expected <= 2 ? 4U : 2U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(4U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    if (expected <= 2) {
      BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
      BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].type);
      BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].term);
      BOOST_CHECK_EQUAL(0, ::strcmp("4", boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
    } else {
      BOOST_CHECK_EQUAL(3U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
      for(std::size_t i=0; i<=2; ++i) {
	BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[i].type);
	BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[i].term);
	BOOST_CHECK_EQUAL(0, ::strcmp((boost::format("%1%") % (i+2)).str().c_str(),
				      boost::get<raft::server::append_entry_type>(comm.q.back()).entry[i].data.c_str()));
      }
    }
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 2;
    resp.request_term_number = 2;
    resp.begin_index = expected <= 2 ? 4 : 2;
    resp.last_index = 5;
    resp.success = true;
    s.on_append_response(resp);
    if (expected!=3) {
      BOOST_CHECK_EQUAL(0U, c.responses.size());
    } else {
      // Majority vote!
      BOOST_CHECK_EQUAL(1U, c.responses.size());
      BOOST_CHECK_EQUAL(4U, c.responses.back().id);
      c.responses.pop_back();
    }
    expected += 1;
    comm.q.pop_back();
  }
  
  // TODO: Simulate two servers at the same term, follower loses connection to leader, is missing log entries,
  // heartbeats time out.  Candidate then regains connection to leader and starts a new election.  The leader gets
  // a vote request at new term but doesn't vote for candidate due to the fact that the candidate is behind on log messages.
}

BOOST_AUTO_TEST_CASE(InitializeFromNonEmptyLog)
{
  // Valid initialization must either be completely empty or there must be a configuration
  // somewhere (log or checkpoint).
  raft::server::communicator_type comm;
  raft::server::client_type c;
  raft::server::log_type l;
  raft::server::checkpoint_data_store_type store;

  std::vector<raft::server::log_entry_type> entries;
  raft::server::configuration_description_server_type server_desc = { 2, "192.168.1.1" };
  entries.emplace_back(raft::server::get_bootstrap_log_entry(server_desc));
  l.append(entries);
  l.update_header(entries.back().term, raft::server::INVALID_PEER_ID);
 
  raft::server::configuration_manager_type cm(2);
  raft::server s(comm, c, l, store, cm);
  BOOST_CHECK_EQUAL(entries.back().term, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s.get_state());
  BOOST_CHECK(cm.has_configuration_at(0));
  BOOST_CHECK_EQUAL(1U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  BOOST_CHECK_EQUAL(2U, cm.configuration().self().peer_id);
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Run timer then we should become leader of a single node cluster
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(entries.back().term+1U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::CANDIDATE, s.get_state());
  BOOST_CHECK(s.log_header_sync_required());
  s.on_log_header_sync();
  BOOST_CHECK(!s.log_header_sync_required());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
}

class RaftTestFixtureBase
{
public:
  std::size_t cluster_size;
  raft::server::communicator_type comm;
  raft::server::client_type c;
  raft::server::log_type l;
  raft::server::checkpoint_data_store_type store;
  std::shared_ptr<raft::server::configuration_manager_type> cm;
  std::shared_ptr<raft::server> s;
  raft::server::simple_configuration_description_type five_servers;
  raft::server::simple_configuration_description_type six_servers;

  void make_leader(uint64_t term, bool respond_to_noop=true);
  void make_follower_with_checkpoint(uint64_t term, uint64_t log_entry);
  void become_follower_with_vote_request(uint64_t term);
  void send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index);
  void send_client_request(uint64_t term, const char * cmd, uint64_t client_index, const boost::dynamic_bitset<> & send_responses_from);
  std::size_t num_known_peers() { return cm->configuration().num_known_peers(); }

  void stage_new_server(uint64_t term, uint64_t commit_index);
};

void RaftTestFixtureBase::stage_new_server(uint64_t term, uint64_t commit_index)
{
  // Assumes that leader is 0
  uint64_t leader_id=0;
  // Assumes that everything in leader log is committed
  raft::server::set_configuration_request_type req;
  req.old_id = 0;
  req.new_configuration = six_servers;
  s->on_set_configuration(req);
  BOOST_CHECK_EQUAL(req.new_configuration.servers.size(), num_known_peers());
  
  // Run timer then we should get append_entries for the newly added server
  s->on_timer();
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(term, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(leader_id, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(commit_index, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(commit_index, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    raft::append_response resp;
    resp.recipient_id = req.new_configuration.servers.size()-1;
    resp.term_number = term;
    resp.request_term_number = term;
    resp.begin_index = 0;
    resp.last_index = commit_index;
    resp.success = true;
    s->on_append_response(resp);
    comm.q.pop_back();
  }
  BOOST_CHECK(!cm->configuration().staging_servers_caught_up());
  BOOST_CHECK(cm->configuration().is_staging());
}

void RaftTestFixtureBase::make_leader(uint64_t term, bool respond_to_noop)
{
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s->on_timer();
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::CANDIDATE, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(0U, comm.q.back().which());
    comm.q.pop_back();
  }
  raft::vote_response vote_response_msg;
  for(uint64_t p=1; p!=num_known_peers(); ++p) {
    vote_response_msg.peer_id = p;
    vote_response_msg.term_number = term;
    vote_response_msg.request_term_number = term;
    vote_response_msg.granted = true;
    s->on_vote_response(vote_response_msg);
  }
  BOOST_CHECK_EQUAL(raft::server::LEADER, s->get_state());
  BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
  for(uint64_t p=1; p!=num_known_peers(); ++p) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(raft::server::log_entry_type::NOOP, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].type);
    if (respond_to_noop) {
      raft::append_response resp;
      resp.recipient_id = p;
      resp.term_number = term;
      resp.request_term_number = term;
      resp.begin_index = 0;
      resp.last_index = boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index+1;
      resp.success = true;
      s->on_append_response(resp);
    }
    comm.q.pop_back();
  }
}

void RaftTestFixtureBase::make_follower_with_checkpoint(uint64_t term, uint64_t log_entry)
{
  {
    raft::server::append_checkpoint_chunk_type msg;
    msg.recipient_id = 0;
    msg.term_number = term;
    msg.leader_id = 1;
    msg.last_checkpoint_index = log_entry;
    msg.checkpoint_begin = 0;
    msg.checkpoint_end = 1;
    msg.checkpoint_done = true;
    msg.data.push_back(0);
    s->on_append_checkpoint_chunk(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(0U, comm.q.size());

  s->on_checkpoint_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(term, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(term, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
  comm.q.pop_back();
}

void RaftTestFixtureBase::become_follower_with_vote_request(uint64_t term)
{
  raft::request_vote msg;
  msg.recipient_id=0;
  msg.term_number=term;
  msg.candidate_id=1;
  msg.last_log_index=0;
  msg.last_log_term=0;
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(term, boost::get<raft::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(term, boost::get<raft::vote_response>(comm.q.back()).request_term_number);
  // Don't worry about vote; it will be no unless the server's log was empty
  //BOOST_CHECK(boost::get<raft::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();
}

void RaftTestFixtureBase::send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index)
{
  boost::dynamic_bitset<> responses;
  responses.resize(num_known_peers(), 1);
  send_client_request(term, cmd, client_index, responses);
  BOOST_CHECK_EQUAL(client_index+1, s->commit_index());
}

void RaftTestFixtureBase::send_client_request(uint64_t term, const char * cmd, uint64_t client_index,
					      const boost::dynamic_bitset<> & send_responses_from)
{
  // Fire off a client_request
  raft::client_request cli_req;

  cli_req.id = 1;
  cli_req.command = cmd;
  s->on_client_request(cli_req);
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  s->on_timer();
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s->get_state());
  BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
  std::size_t expected = 1;
  std::size_t num_responses = 0;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(term, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(client_index, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    // Can't really check this in general
    // BOOST_CHECK_EQUAL(client_index > 0 ? term : 0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(client_index, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(term, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp(cmd, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
      raft::append_response resp;
      resp.recipient_id = expected;
      resp.term_number = term;
      resp.request_term_number = term;
      resp.begin_index = client_index;
      resp.last_index = client_index+1;
      if (send_responses_from.test(expected)) {
	resp.success = true;
	num_responses += 1;
      } else {
	resp.success = false;
      }
      s->on_append_response(resp);
    if (num_responses!=3) {
      BOOST_CHECK_EQUAL(0U, c.responses.size());
    } else {
      // Majority vote!
      BOOST_CHECK_EQUAL(1U, c.responses.size());
      BOOST_CHECK_EQUAL(1U, c.responses.back().id);
      c.responses.pop_back();
    }
    expected += 1;
    comm.q.pop_back();
  }
}

class RaftTestFixture : public RaftTestFixtureBase
{
public:
  // std::size_t cluster_size;
  // raft::server::communicator_type comm;
  // raft::server::client_type c;
  // raft::server::log_type l;
  // raft::server::checkpoint_data_store_type store;
  // std::shared_ptr<raft::server::configuration_manager_type> cm;
  // std::shared_ptr<raft::server> s;
  // raft::server::simple_configuration_description_type five_servers;
  // raft::server::simple_configuration_description_type six_servers;

  RaftTestFixture();
  ~RaftTestFixture()
  {
  }

  // void make_leader(uint64_t term);
  // void make_follower_with_checkpoint(uint64_t term, uint64_t log_entry);
  // void become_follower_with_vote_request(uint64_t term);
  // void send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index);
  // void send_client_request(uint64_t term, const char * cmd, uint64_t client_index, const boost::dynamic_bitset<> & send_responses_from);
  // std::size_t num_known_peers() { return cm->configuration().num_known_peers(); }

  // void stage_new_server(uint64_t term, uint64_t commit_index);
};

RaftTestFixture::RaftTestFixture()
{
  cluster_size = 5;
  five_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  six_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"},  {5, "192.168.1.6"}};
  // std::vector<raft::server::peer_type> peers(cluster_size);
  // for(std::size_t i=0; i<cluster_size; ++i) {
  //   peers[i].peer_id = i;
  // }
  // cm.reset(new raft::server::configuration_manager_type(0, peers));
  cm.reset(new raft::server::configuration_manager_type(0));
  // std::vector<raft::server::log_entry_type> entries;
  // entries.push_back(raft::server::log_entry_type());
  // entries.back().type = raft::server::log_entry_type::CONFIGURATION;
  // entries.back().term = 0;
  // entries.back().configuration.from = five_servers;
  // l.append(entries);
  // l.update_header(entries.back().term, raft::server::INVALID_PEER_ID);
  raft::server::configuration_manager_type::checkpoint_type ckpt;
  ckpt.index = 0;
  ckpt.description.from = five_servers;
  BOOST_CHECK(ckpt.is_valid());
  cm->set_checkpoint(ckpt);
  BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
  BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
  BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
  BOOST_CHECK(cm->configuration().includes_self());
  s.reset(new raft::server(comm, c, l, store, *cm.get()));
  // BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
  // BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
  // BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
  // BOOST_CHECK(cm->configuration().includes_self());
  BOOST_CHECK_EQUAL(0U, s->current_term());
  BOOST_CHECK_EQUAL(0U, s->commit_index());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // BOOST_CHECK_EQUAL(0U, l.start_index());
  // BOOST_CHECK_EQUAL(1U, l.last_index());
}

BOOST_FIXTURE_TEST_CASE(AppendEntriesLogSync, RaftTestFixture)
{
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 1;
    msg.entry.back().data = "1";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_sync(1);
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Pretend a leader from expired term sends a message, this should respond with current term
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 0;
    msg.leader_id = 2;
    msg.previous_log_index = 2;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 1;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 0;
    msg.entry.back().data = "0";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(!boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Supposing the leader has committed lets go to another message
  // which creates a gap.  This should be rejected by the peer.
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 2;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 1;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 1;
    msg.entry.back().data = "3";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(!boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Send three messages with the first one a duplicate
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    for(std::size_t i=1; i<=3; ++i) {
      msg.entry.push_back(raft::server::log_entry_type());
      msg.entry.back().type = raft::server::log_entry_type::COMMAND;
      msg.entry.back().term = 1;
      msg.entry.back().data = (boost::format("%1%") % i).str();
    }
    s->on_append_entry(msg);
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_sync(3);
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Let's go to another message
  // which uses a newer term.  The idea in this example is that entries previously
  // sent to the peer didn't get committed but a new leader got elected and DID commit
  // at those indexes and is trying to append from them on the new term.  We must reject
  // so that the new leader backs up to find where its log agrees with that of the peer.
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 3;
    msg.leader_id = 2;
    msg.previous_log_index = 3;
    msg.previous_log_term = 3;
    msg.leader_commit_index = 3;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 3;
    msg.entry.back().data = "4";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(!boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Let's suppose that only log entry at index 1 on term 1 got committed.  We should be able to
  // overwrite log entries starting at that point.
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 3;
    msg.leader_id = 2;
    msg.previous_log_index = 1;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 3;
    for (std::size_t i=2; i<=4; ++i) {
      msg.entry.push_back(raft::server::log_entry_type());
      msg.entry.back().type = raft::server::log_entry_type::COMMAND;
      msg.entry.back().term = 3;
      msg.entry.back().data = (boost::format("%1%a") % i).str();
    }
    s->on_append_entry(msg);
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_sync(4);
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(4U, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();
}

#include <iostream>
BOOST_FIXTURE_TEST_CASE(AppendEntriesNegativeResponse, RaftTestFixture)
{
  // Make me leader
  make_leader(1);
  // Client request to trigger append entries
  raft::client_request cli_req;
  cli_req.id = 1;
  cli_req.command = "1";
  s->on_client_request(cli_req);

  // On first attempt have clients respond negatively.  On second have them succeed
  for(std::size_t attempt=0; attempt<=1; ++attempt) {
    std::cout << "AppendEntriesNegativeResponse attempt " << attempt << std::endl;
    // Wait so the server will try to send log records.
    s->on_timer();
    BOOST_CHECK_EQUAL(1U, s->current_term());
    BOOST_CHECK_EQUAL(raft::server::LEADER, s->get_state());
    BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
    uint64_t expected = 1;
    while(comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(2U, comm.q.back().which());
      BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
      BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
      BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
      BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
      BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 0U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
      BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
      BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 2U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
      BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[attempt].type);
      BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry[attempt].term);
      BOOST_CHECK_EQUAL(0, ::strcmp("1", boost::get<raft::server::append_entry_type>(comm.q.back()).entry[attempt].data.c_str()));
      raft::append_response resp;
      resp.recipient_id = expected;
      resp.term_number = 1;
      resp.request_term_number = 1;
      resp.begin_index = attempt == 0 ? 1 : 0;
      resp.last_index = attempt == 0 ? 1 : 2;
      resp.success = attempt == 0 ? false : true;
      s->on_append_response(resp);
      if (attempt==0 || expected!=3) {
	BOOST_CHECK_EQUAL(0U, c.responses.size());
      } else {
	// Majority vote!
	BOOST_CHECK_EQUAL(1U, c.responses.size());
	BOOST_CHECK_EQUAL(1U, c.responses.back().id);
	c.responses.pop_back();
      }
      expected += 1;
      comm.q.pop_back();
    }
  }
}

BOOST_FIXTURE_TEST_CASE(AppendEntriesSlowHeaderSync, RaftTestFixture)
{
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 1;
    msg.entry.back().data = "1";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 2;
    msg.leader_id = 2;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 2;
    msg.entry.back().data = "2";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    raft::server::append_checkpoint_chunk_type msg;
    msg.recipient_id = 0;
    msg.term_number = 2;
    msg.leader_id = 2;
    msg.last_checkpoint_index = 0;
    msg.checkpoint_begin = 0;
    msg.checkpoint_end = 1;
    msg.checkpoint_done = false;
    msg.data.push_back(0);
    s->on_append_checkpoint_chunk(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // This one doesn't require a new term so it gets queued awaiting the log header sync
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 1;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 2;
    msg.entry.back().data = "2";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());

  // TODO: Validate that the 2 append entries have beeen processed and are
  // awaiting log sync.
  
  s->on_log_sync(2);
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(2U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(BasicOnVoteRequestTest, RaftTestFixture)
{
  // FOLLOWER -> FOLLOWER
  raft::request_vote msg;
  msg.recipient_id=0;
  msg.term_number=1;
  msg.candidate_id=1;
  msg.last_log_index=0;
  msg.last_log_term=0;
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(OnVoteRequestSlowHeaderSyncTest, RaftTestFixture)
{
  // FOLLOWER -> FOLLOWER
  raft::request_vote msg;
  msg.recipient_id=0;
  msg.term_number=1;
  msg.candidate_id=1;
  msg.last_log_index=0;
  msg.last_log_term=0;
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // We are still waiting for header to sync to disk so will ignore subsequent request
  msg.recipient_id=0;
  msg.term_number=2;
  msg.candidate_id=2;
  msg.last_log_index=0;
  msg.last_log_term=0;
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  // Send again now that we are sync'd
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(2U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(2U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  // Send a couple of entries
  for(std::size_t i=1; i<=3; ++i) {
    {
      raft::server::append_entry_type msg;
      msg.recipient_id = 0;
      msg.term_number = 2;
      msg.leader_id = 1;
      msg.previous_log_index = i-1;
      msg.previous_log_term = i==1 ? 0 : 2;
      msg.leader_commit_index = i-1;
      msg.entry.push_back(raft::server::log_entry_type());
      msg.entry.back().type = raft::server::log_entry_type::COMMAND;
      msg.entry.back().term = 2;
      msg.entry.back().data = (boost::format("%1%") % i).str();
      s->on_append_entry(msg);
    }
    s->on_log_sync(i);
    BOOST_CHECK_EQUAL(2U, s->current_term());
    BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
    BOOST_CHECK_EQUAL(1U, comm.q.size());
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_response>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
    BOOST_CHECK_EQUAL(i-1, boost::get<raft::append_response>(comm.q.back()).begin_index);
    BOOST_CHECK_EQUAL(i, boost::get<raft::append_response>(comm.q.back()).last_index);
    BOOST_CHECK(boost::get<raft::append_response>(comm.q.back()).success);
    comm.q.pop_back();
  }

  // Now initiate a leadership change but without up to date log; this should advance term but
  // we should not get the vote.
  msg.recipient_id=0;
  msg.term_number=3;
  msg.candidate_id=1;
  msg.last_log_index=0;
  msg.last_log_term=0;
  s->on_request_vote(msg);
  // Updated current_term requires header sync
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(!boost::get<raft::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  // Now initiate a leadership change but with up to date log we'll get the vote
  msg.recipient_id=0;
  msg.term_number=3;
  msg.candidate_id=1;
  msg.last_log_index=3;
  msg.last_log_term=2;
  s->on_request_vote(msg);
  // Updated voted_for (but not update current_term) requires header sync
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Let's suppose that the leader here got a quorum from other peers
  // it could start appending entries.  We have the correct term so we should
  // queue these up and continue to wait for the existing header sync to complete.
  // TODO: We could also get an append_entry for the current term without ever seeing
  // a request_vote from the leader.
  {
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 3;
    msg.leader_id = 1;
    msg.previous_log_index = 3;
    msg.previous_log_term = 2;
    msg.leader_commit_index = 4;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 3;
    msg.entry.back().data = "4";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  s->on_log_sync(4);
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(3, boost::get<raft::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(4, boost::get<raft::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::append_response>(comm.q.back()).success);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(AppendCheckpointChunk, RaftTestFixture)
{
  {
    raft::server::append_checkpoint_chunk_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.last_checkpoint_index = 2;
    msg.checkpoint_begin = 0;
    msg.checkpoint_end = 1;
    msg.checkpoint_done = true;
    msg.data.push_back(0);
    s->on_append_checkpoint_chunk(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(0U, comm.q.size());

  s->on_checkpoint_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(AppendCheckpointChunkSlowHeaderSync, RaftTestFixture)
{
  {
    raft::server::append_checkpoint_chunk_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.last_checkpoint_index = 2;
    msg.checkpoint_begin = 0;
    msg.checkpoint_end = 1;
    msg.checkpoint_done = false;
    msg.data.push_back(0);
    s->on_append_checkpoint_chunk(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    raft::server::append_entry_type msg;
    msg.recipient_id = 0;
    msg.term_number = 2;
    msg.leader_id = 2;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::server::log_entry_type());
    msg.entry.back().type = raft::server::log_entry_type::COMMAND;
    msg.entry.back().term = 2;
    msg.entry.back().data = "2";
    s->on_append_entry(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    raft::server::append_checkpoint_chunk_type msg;
    msg.recipient_id = 0;
    msg.term_number = 2;
    msg.leader_id = 2;
    msg.last_checkpoint_index = 10;
    msg.checkpoint_begin = 0;
    msg.checkpoint_end = 1;
    msg.checkpoint_done = false;
    msg.data.push_back(0);
    s->on_append_checkpoint_chunk(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  {
    // This one doesn't require a new term so it gets queued awaiting the log header sync
    raft::server::append_checkpoint_chunk_type msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.last_checkpoint_index = 2;
    msg.checkpoint_begin = 1;
    msg.checkpoint_end = 2;
    msg.checkpoint_done = true;
    msg.data.push_back(0);
    s->on_append_checkpoint_chunk(msg);
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
  comm.q.pop_back();

  s->on_checkpoint_sync();
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
  comm.q.pop_back();

  // TODO: What happens if we get an append entry for the new term while still waiting for the log header sync?  No reason it
  // couldn't be queued right?
}

BOOST_FIXTURE_TEST_CASE(ClientCheckpointTest, RaftTestFixture)
{
  uint64_t term = 1;
  make_leader(term);

  const char * cmd = "1";
  uint64_t client_index=l.last_index();
  send_client_request_and_commit(term, cmd, client_index++);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  
  auto ckpt = s->begin_checkpoint(1U);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_REQUIRE(nullptr != ckpt.get());
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_index);
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_term);
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
  ckpt->write(&data[0], 5U);
  s->complete_checkpoint(1U, ckpt);
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(ckpt == s->checkpoint().last_checkpoint());
}

BOOST_FIXTURE_TEST_CASE(ClientPartialCheckpointTest, RaftTestFixture)
{
  uint64_t term = 1;
  make_leader(term);

  uint64_t client_index=l.last_index();
  send_client_request_and_commit(term, "1", client_index++);  
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  send_client_request_and_commit(term, "2", client_index++);  
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());

  auto ckpt = s->begin_checkpoint(1U);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_REQUIRE(nullptr != ckpt.get());
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_index);
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_term);
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());

  uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
  ckpt->write(&data[0], 5U);
  s->complete_checkpoint(1U, ckpt);
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(ckpt == s->checkpoint().last_checkpoint());
}

BOOST_FIXTURE_TEST_CASE(ClientCheckpointOldTermTest, RaftTestFixture)
{
  uint64_t term = 1;
  make_leader(term);

  uint64_t client_index=l.last_index();
  send_client_request_and_commit(term, "1", client_index++);  
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  // Advance term; we should still be able to checkpoint at the old term
  term = 2;
  become_follower_with_vote_request(term);
  term = 3;
  make_leader(term);
  client_index=l.last_index();
  send_client_request_and_commit(term, "2", client_index++);  
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  
  auto ckpt = s->begin_checkpoint(1U);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_REQUIRE(nullptr != ckpt.get());
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_index);
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_term);
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());

  uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
  ckpt->write(&data[0], 5U);
  s->complete_checkpoint(1U, ckpt);
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(ckpt == s->checkpoint().last_checkpoint());
}

BOOST_FIXTURE_TEST_CASE(ClientCheckpointNegativeTest, RaftTestFixture)
{
  uint64_t term = 1;
  make_leader(term);

  uint64_t client_index=l.last_index();
  send_client_request_and_commit(term, "1", client_index++);  
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  auto ckpt = s->begin_checkpoint(l.last_index()+1);
  BOOST_CHECK(nullptr == ckpt.get());
}

// Test that append_entries will send a checkpoint that needs log entries the leader has discarded
// post checkpoint
BOOST_FIXTURE_TEST_CASE(AppendEntriesCheckpoint, RaftTestFixture)
{
  uint64_t term = 1;
  make_leader(term);

  const char * cmd = "1";
  uint64_t client_index=l.last_index();
  // Send success response from all peers except 1.  This will commit entry
  // so that it can be checkpointed.
  boost::dynamic_bitset<> responses;
  responses.resize(num_known_peers(), true);
  responses.flip(1);
  send_client_request(term, cmd, client_index++, responses);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  
  auto ckpt = s->begin_checkpoint(1U);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_REQUIRE(nullptr != ckpt.get());
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_index);
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_term);
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
  ckpt->write(&data[0], 5U);
  s->complete_checkpoint(1U, ckpt);
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(ckpt == s->checkpoint().last_checkpoint());

  // Fire timer.  Peer 1 still doesn't have first log entry but since that entry is
  // discarded, a checkpoint will need to be sent to 1.
  s->on_timer();
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  raft::append_checkpoint_chunk_response resp;
  // Ack with bytes_stored=2 twice to validate the checkpoint protocol will resend data if requested
  for(int i=0; i<2; ++i) {
    resp.recipient_id = 1;
    resp.term_number = 1U;
    resp.request_term_number = 1U;
    resp.bytes_stored = 2U;
    s->on_append_checkpoint_chunk_response(resp);  
    BOOST_REQUIRE_EQUAL(1U, comm.q.size());
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
    BOOST_CHECK_EQUAL(4U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
    comm.q.pop_back();
  }

  resp.recipient_id = 1;
  resp.term_number = 1U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 4U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(4U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(5U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  resp.recipient_id = 1;
  resp.term_number = 1U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 5U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_REQUIRE_EQUAL(0U, comm.q.size());
}

// Test that a checkpoint transfer is properly cancelled by a term update
BOOST_FIXTURE_TEST_CASE(AppendEntriesCheckpointAbandon, RaftTestFixture)
{
  uint64_t term = 1;
  make_leader(term);

  const char * cmd = "1";
  uint64_t client_index=l.last_index();
  // Send success response from all peers except 1.  This will commit entry
  // so that it can be checkpointed.
  boost::dynamic_bitset<> responses;
  responses.resize(num_known_peers(), true);
  responses.flip(1);
  send_client_request(term, cmd, client_index++, responses);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  
  auto ckpt = s->begin_checkpoint(1U);
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_REQUIRE(nullptr != ckpt.get());
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_index);
  BOOST_CHECK_EQUAL(1U, ckpt->header().last_log_entry_term);
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
  ckpt->write(&data[0], 5U);
  s->complete_checkpoint(1U, ckpt);
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(1U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(ckpt == s->checkpoint().last_checkpoint());

  // Fire timer.  Peer 1 still doesn't have first log entry but since that entry is
  // discarded, a checkpoint will need to be sent to 1.
  s->on_timer();
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::server::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  raft::append_checkpoint_chunk_response resp;
  resp.recipient_id = 1;
  resp.term_number = 2U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 2U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(2U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();

  // Send unexpected response; this should be happily ignored
  resp.recipient_id = 2;
  resp.term_number = 2U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 2U;
  s->on_append_checkpoint_chunk_response(resp);  
}

BOOST_FIXTURE_TEST_CASE(JointConsensusAddServer, RaftTestFixture)
{
  uint64_t term=1;
  make_leader(term);
  uint64_t commit_index = s->commit_index();
  uint64_t client_index=l.last_index();
  send_client_request_and_commit(term, "1", client_index++);  

  BOOST_CHECK_EQUAL(5U, num_known_peers());
  stage_new_server(term, commit_index+1);

  // This should catch up newly added server.  Based on this we should
  // move to transitional
  send_client_request_and_commit(term, "2", client_index++);
  BOOST_CHECK(cm->configuration().staging_servers_caught_up());
  BOOST_CHECK_EQUAL(raft::server::log_entry_type::COMMAND, l.last_entry().type);
  // TODO: Should I really have to do this on_timer call to trigger the transitional entry????
  s->on_timer();
  BOOST_CHECK(cm->configuration().is_transitional());
  BOOST_CHECK_EQUAL(raft::server::log_entry_type::CONFIGURATION, l.last_entry().type);
  BOOST_CHECK_EQUAL(5U, l.last_entry().configuration.from.servers.size());
  BOOST_CHECK_EQUAL(6U, l.last_entry().configuration.to.servers.size());

  uint64_t expected=1;
  BOOST_CHECK_EQUAL(num_known_peers() - 1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(commit_index+2, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(3U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 1;
    resp.request_term_number = 1;
    resp.begin_index = 3;
    resp.last_index = 4;
    resp.success = true;
    s->on_append_response(resp);
    comm.q.pop_back();

    // We need quorum from both the old set of 5 servers and the new set of 6
    if(expected < 4) {
      BOOST_CHECK_EQUAL(commit_index + 2U, s->commit_index());
      // Should have the transitional entry in the log.
      BOOST_CHECK(cm->configuration().is_transitional());
      BOOST_CHECK_EQUAL(6U, num_known_peers());
      BOOST_CHECK_EQUAL(raft::server::log_entry_type::CONFIGURATION, l.last_entry().type);
      BOOST_CHECK_EQUAL(5U, l.last_entry().configuration.from.servers.size());
      BOOST_CHECK_EQUAL(6U, l.last_entry().configuration.to.servers.size());
    } else {
      BOOST_CHECK_EQUAL(commit_index + 3U, s->commit_index());
      // Should get a new stable config entry in the log
      BOOST_CHECK(cm->configuration().is_stable());
      BOOST_CHECK_EQUAL(6U, num_known_peers());
      BOOST_CHECK_EQUAL(raft::server::log_entry_type::CONFIGURATION, l.last_entry().type);
      BOOST_CHECK_EQUAL(6U, l.last_entry().configuration.from.servers.size());
      BOOST_CHECK_EQUAL(0U, l.last_entry().configuration.to.servers.size());
    }
    if(expected != 4) {
      BOOST_CHECK_EQUAL(0U, c.configuration_responses.size());
    } else {
      BOOST_CHECK_EQUAL(1U, c.configuration_responses.size());
      BOOST_CHECK_EQUAL(raft::SUCCESS, c.configuration_responses.front().result);
      BOOST_CHECK_EQUAL(0U, c.configuration_responses.front().bad_servers.servers.size());
      c.configuration_responses.pop_back();
    }
    expected += 1;
  }  
}

BOOST_FIXTURE_TEST_CASE(JointConsensusAddServerLostLeadershipFailure, RaftTestFixture)
{
  uint64_t term=1;
  make_leader(term);
  uint64_t commit_index=s->commit_index();
  uint64_t log_index=l.last_index();
  send_client_request_and_commit(term, "1", log_index);  

  BOOST_CHECK_EQUAL(5U, num_known_peers());
  stage_new_server(term, commit_index+1U);

  // This should catch up newly added server.  Based on this we should
  // move to transitional
  send_client_request_and_commit(term, "2", log_index+1);
  BOOST_CHECK(cm->configuration().staging_servers_caught_up());
  // TODO: Should I really have to do this on_timer call to trigger the transitional entry????
  s->on_timer();
  BOOST_CHECK(cm->configuration().is_transitional());
  BOOST_CHECK_EQUAL(0U, c.configuration_responses.size());

  // Now lose leadership by sending a log entry that conflicts with the transitional
  // configuration.  This triggers a failure in the configuration change and rollback
  // to prior configuration
  raft::server::append_entry_type msg;
  msg.recipient_id = 0;
  msg.term_number = term+1;
  msg.leader_id = 1;
  msg.previous_log_index = log_index+2;
  msg.previous_log_term = term;
  msg.leader_commit_index = s->commit_index();
  msg.entry.push_back(raft::server::log_entry_type());
  msg.entry.back().type = raft::server::log_entry_type::COMMAND;
  msg.entry.back().term = term+1;
  msg.entry.back().data = "1";
  s->on_append_entry(msg);
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(raft::FAIL, c.configuration_responses.front().result);
  BOOST_CHECK(cm->configuration().is_stable());
  BOOST_CHECK_EQUAL(5U, num_known_peers());
}

BOOST_FIXTURE_TEST_CASE(JointConsensusAddServerLostLeadershipSuccess, RaftTestFixture)
{
  uint64_t term=1;
  make_leader(term);
  uint64_t commit_index=s->commit_index();
  uint64_t log_index=l.last_index();
  send_client_request_and_commit(term, "1", log_index);  

  BOOST_CHECK_EQUAL(5U, num_known_peers());
  stage_new_server(term, commit_index+1U);

  // This should catch up newly added server.  Based on this we should
  // move to transitional
  send_client_request_and_commit(term, "2", log_index+1);
  BOOST_CHECK(cm->configuration().staging_servers_caught_up());
  // TODO: Should I really have to do this on_timer call to trigger the transitional entry????
  s->on_timer();
  BOOST_CHECK(cm->configuration().is_transitional());
  BOOST_CHECK_EQUAL(0U, c.configuration_responses.size());

  // Now lose leadership by sending a log entry that is consistent with the transitional
  // configuration and in fact tells the former leader the transitional config is committed.
  // This triggers a successful completion of the configuration change.  Since I am not the leader
  // I don't log the new stable config (the new leader does that).
  raft::server::append_entry_type msg;
  msg.recipient_id = 0;
  msg.term_number = term+1;
  msg.leader_id = 1;
  msg.previous_log_index = log_index+3;
  msg.previous_log_term = term;
  msg.leader_commit_index = commit_index+3;
  msg.entry.push_back(raft::server::log_entry_type());
  msg.entry.back().type = raft::server::log_entry_type::COMMAND;
  msg.entry.back().term = term+1;
  msg.entry.back().data = "1";
  s->on_append_entry(msg);
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(raft::SUCCESS, c.configuration_responses.front().result);
}

class RaftConfigurationTestFixture : public RaftTestFixtureBase
{
public:
  // std::size_t cluster_size;
  // raft::server::communicator_type comm;
  // raft::server::client_type c;
  // raft::server::log_type l;
  // raft::server::checkpoint_data_store_type store;
  // std::shared_ptr<raft::server::configuration_manager_type> cm;
  // std::shared_ptr<raft::server> s;
  // raft::server::simple_configuration_description_type five_servers;
  // raft::server::simple_configuration_description_type six_servers;

  RaftConfigurationTestFixture();
  ~RaftConfigurationTestFixture()
  {
  }

  // void make_leader(uint64_t term);
  // void make_follower_with_checkpoint(uint64_t term, uint64_t log_entry);
  // void become_follower_with_vote_request(uint64_t term);
  // void send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index);
  // void send_client_request(uint64_t term, const char * cmd, uint64_t client_index, const boost::dynamic_bitset<> & send_responses_from);
  // std::size_t num_known_peers() { return cm->configuration().num_known_peers(); }

  // void stage_new_server(uint64_t term, uint64_t commit_index);
};

RaftConfigurationTestFixture::RaftConfigurationTestFixture()
{
  cluster_size = 5;
  five_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  six_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"},  {5, "192.168.1.6"}};
  cm.reset(new raft::server::configuration_manager_type(0));
  std::vector<raft::server::log_entry_type> entries;
  entries.push_back(raft::server::log_entry_type());
  entries.back().type = raft::server::log_entry_type::CONFIGURATION;
  entries.back().term = 0;
  entries.back().configuration.from = five_servers;
  l.append(entries);
  l.update_header(entries.back().term, raft::server::INVALID_PEER_ID);
  s.reset(new raft::server(comm, c, l, store, *cm.get()));
  BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
  BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
  BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
  BOOST_CHECK(cm->configuration().includes_self());
  BOOST_CHECK_EQUAL(0U, s->current_term());
  BOOST_CHECK_EQUAL(0U, s->commit_index());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, l.start_index());
  BOOST_CHECK_EQUAL(1U, l.last_index());
}

BOOST_FIXTURE_TEST_CASE(JointConsensusAddServerNewLeaderFinishesCommit, RaftConfigurationTestFixture)
{
  // As FOLLOWER, get a transitional config from the leader
  BOOST_CHECK(cm->configuration().is_stable());
  uint64_t term=0;
  uint64_t log_index=l.last_index();
  uint64_t commit_index=s->commit_index();
  raft::server::append_entry_type msg;
  msg.recipient_id = 0;
  msg.term_number = term;
  msg.leader_id = 1;
  msg.previous_log_index = log_index;
  msg.previous_log_term = term;
  msg.leader_commit_index = commit_index;
  msg.entry.push_back(raft::server::log_entry_type());
  msg.entry.back().type = raft::server::log_entry_type::CONFIGURATION;
  msg.entry.back().term = term;
  msg.entry.back().configuration.from = five_servers;
  msg.entry.back().configuration.to = six_servers;
  s->on_append_entry(msg);
  BOOST_CHECK(cm->configuration().is_transitional());
  BOOST_CHECK_EQUAL(5U, l.last_entry().configuration.from.servers.size());
  BOOST_CHECK_EQUAL(6U, l.last_entry().configuration.to.servers.size());

  make_leader(term+1, false);

  // Now leader and have the config entry so should try to replicate it but a new leader
  // is optimistic and assumes that all peers have its log entries.  It will append a NOOP
  // and replicate that.
  s->on_timer();
  uint64_t expected=1;
  BOOST_CHECK_EQUAL(num_known_peers() - 1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::server::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(term+1, boost::get<raft::server::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(commit_index, boost::get<raft::server::append_entry_type>(comm.q.back()).leader_commit_index);
    // Assumes peer also has the transitional config log entry
    BOOST_CHECK_EQUAL(log_index+1U, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(term, boost::get<raft::server::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::server::append_entry_type>(comm.q.back()).entry.size());
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = term+1;
    resp.request_term_number = term+1;
    resp.begin_index = log_index+1;
    resp.last_index = log_index+2;
    resp.success = true;
    s->on_append_response(resp);
    comm.q.pop_back();

    // We need quorum from both the old set of 5 servers and the new set of 6.
    // When we do commit, we'll commit the transitional entry and the NOOP
    if(expected < 4) {
      BOOST_CHECK_EQUAL(commit_index, s->commit_index());
      // Should have the transitional entry in the log.
      BOOST_CHECK(cm->configuration().is_transitional());
      BOOST_CHECK_EQUAL(6U, num_known_peers());
      BOOST_CHECK_EQUAL(raft::server::log_entry_type::NOOP, l.last_entry().type);
    } else {
      // The log should be fully committed up through the NOOP entry  regardless of where
      // commit_index started
      BOOST_CHECK_EQUAL(log_index+2, s->commit_index());
      // Should get a new stable config entry in the log
      BOOST_CHECK(cm->configuration().is_stable());
      BOOST_CHECK_EQUAL(6U, num_known_peers());
      BOOST_CHECK_EQUAL(raft::server::log_entry_type::CONFIGURATION, l.last_entry().type);
      BOOST_CHECK_EQUAL(6U, l.last_entry().configuration.from.servers.size());
      BOOST_CHECK_EQUAL(0U, l.last_entry().configuration.to.servers.size());
    }
    // Did not initiate the config change so should not send a response
    BOOST_CHECK_EQUAL(0U, c.configuration_responses.size());
    expected += 1;
  }  
}

// TODO: Leader creates some log entries but fails to replicate them and crashes.  While crashed the rest of the cluster picks up
// and moves forward on a new term (or many new terms) and successfully replicate entries.  Once the former leader rejoins the cluster
// and the new leader figures out the last index in the former leader's log that doesn't cause a gap, we'll detect a term mismatch on
// the tail of the old leaders log and eventually make our way back to the point at which the logs agree.
// TODO: What happens if a peer dies while we are sending a checkpoint and then restarts needing us to start sending from scratch
// TODO: client sends request to leader, leader sends out append entries and log sync.  A quorum of peers commit the request.  A rogue peers starts an
// election and the leader loses leadership (prior to receiving append entry responses).  The client request is committed but how does the client
// learn that this is true?  It doesn't.  See Section 6.3 ("Implementing linearizable semantics") of Ongaro's thesis; upshot is that the consensus protocol
// gives at least once guarantees and that state machines have to add in the the at most once guarantee through use of duplicate command detection (using
// unique (clientid,id) request ids and memoized responses).
// TODO: Submit config change to a leader then get that leader to append the transitional log.  At that point lose leadership.  Eventually the new leader will
// decide whether the transitional configuration gets kept or not (in the latter case the transitional log entry will get removed in the former leader and
// it will go back to the previous stable configuration).
// TODO: Test edge case in which a transition config gets logged on LEADER and then another COMMAND.  Suppose that the new leader doesn't have the COMMAND but
// does have the transitional config.  The config should get committed.
// TODO: Test edge case in which a transition config gets logged on LEADER then the lose leadership.  Suppose that we get a checkpoint from a new leader that
// contains the new config.  We can tell the client.  Conversely we can learn from the checkpoint that the config DIDN'T get committed.  Perhaps it is easier to
// just take the logcabin approach and say that a leader with a transitional config tells the client that it failed immediately upon losing leadership.


#include <boost/asio.hpp>
#include "leveldb_log.hh"
#include "posix_file.hh"
#include "asio_block_device.hh"

BOOST_AUTO_TEST_CASE(LevelDBLogWriterTest)
{
  raft::posix::writable_file device("raft_test_log.bin");
  raft::leveldb::log_writer<raft::posix::writable_file> writer(device);
  std::vector<uint8_t> entry({ 0, 1, 2, 3, 4 });
  writer.append_record(&entry[0], entry.size());
}

BOOST_AUTO_TEST_CASE(LevelDBAsioLogWriterTest)
{
  boost::asio::io_service ios;
  int fd = ::open("raft_test_log.bin", O_WRONLY);
  raft::asio::writable_file device(ios, fd);
  raft::leveldb::log_writer<raft::asio::writable_file> writer(device);
  std::vector<uint8_t> entry({ 100, 101, 102, 104, 105 });
  writer.append_record(&entry[0], entry.size());
  ios.run();
  ::close(fd);
}

#include "asio_server.hh"

BOOST_AUTO_TEST_CASE(RaftAsioTest)
{
  boost::asio::io_service ios;
  raft::server::simple_configuration_description_type config;
  config.servers = {{0, "127.0.0.1:9133"}, {1, "127.0.0.1:9134"}, {2, "127.0.0.1:9135"}, {3, "127.0.0.1:9136"},  {4, "127.0.0.1:9137"}};
  raft::asio::tcp_server s(ios, 0, config);
  for(std::size_t i=0; i<10; ++i) {
    boost::system::error_code ec;
    ios.run_one(ec);
  }
}

