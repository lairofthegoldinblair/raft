#include <chrono>
#include <thread>

#include "server.hh"

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

BOOST_AUTO_TEST_CASE(BasicStateMachineTests)
{
  std::size_t cluster_size(5);
  std::vector<raft::peer> peers(cluster_size);
  for(std::size_t i=0; i<cluster_size; ++i) {
    peers[i].peer_id = i;
  }
  raft::test_communicator comm;
  raft::client c;
  raft::server s(comm, c, 0, peers);
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
    BOOST_CHECK_EQUAL(expected, boost::get<raft::append_entry>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_id);
    // TODO: What about the next 3 values ????
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
    expected += 1;
    comm.q.pop_back();
  }

  // Nothing should change here
  std::this_thread::sleep_for (std::chrono::milliseconds(1));
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  
  // Wait a bit and should get another round of heartbeat messages
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(raft::server::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::append_entry>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_id);
    // TODO: What about the next 3 values ????
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
    expected += 1;
    comm.q.pop_back();
  }

  // Old append_entry should elicit a response with updated term
  raft::append_entry ae_msg;
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
    BOOST_CHECK_EQUAL(expected, boost::get<raft::append_entry>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(raft::log_entry::COMMAND, boost::get<raft::append_entry>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("1", boost::get<raft::append_entry>(comm.q.back()).entry[0].data.c_str()));
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 2;
    resp.request_term_number = 2;
    resp.begin_index = 0;
    resp.last_index = 1;
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
  s.on_log_sync(3);
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::append_entry>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(raft::log_entry::COMMAND, boost::get<raft::append_entry>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("2", boost::get<raft::append_entry>(comm.q.back()).entry[0].data.c_str()));
    BOOST_CHECK_EQUAL(raft::log_entry::COMMAND, boost::get<raft::append_entry>(comm.q.back()).entry[1].type);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).entry[1].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("3", boost::get<raft::append_entry>(comm.q.back()).entry[1].data.c_str()));
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 2;
    resp.request_term_number = 2;
    resp.begin_index = 1;
    resp.last_index = 3;
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
    BOOST_CHECK_EQUAL(expected, boost::get<raft::append_entry>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(expected <= 2 ? 3U : 1U, boost::get<raft::append_entry>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(3U, boost::get<raft::append_entry>(comm.q.back()).leader_commit_index);
    if (expected <= 2) {
      BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
      BOOST_CHECK_EQUAL(raft::log_entry::COMMAND, boost::get<raft::append_entry>(comm.q.back()).entry[0].type);
      BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).entry[0].term);
      BOOST_CHECK_EQUAL(0, ::strcmp("4", boost::get<raft::append_entry>(comm.q.back()).entry[0].data.c_str()));
    } else {
      BOOST_CHECK_EQUAL(3U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
      for(std::size_t i=0; i<=2; ++i) {
	BOOST_CHECK_EQUAL(raft::log_entry::COMMAND, boost::get<raft::append_entry>(comm.q.back()).entry[i].type);
	BOOST_CHECK_EQUAL(2U, boost::get<raft::append_entry>(comm.q.back()).entry[i].term);
	BOOST_CHECK_EQUAL(0, ::strcmp((boost::format("%1%") % (i+2)).str().c_str(),
				      boost::get<raft::append_entry>(comm.q.back()).entry[i].data.c_str()));
      }
    }
    raft::append_response resp;
    resp.recipient_id = expected;
    resp.term_number = 2;
    resp.request_term_number = 2;
    resp.begin_index = expected <= 2 ? 3 : 1;
    resp.last_index = 4;
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

class RaftTestFixture
{
public:
  std::size_t cluster_size;
  raft::test_communicator comm;
  raft::client c;
  std::shared_ptr<raft::server> s;

  RaftTestFixture();
  ~RaftTestFixture()
  {
  }

  void make_leader(uint64_t term);
  void make_follower_with_checkpoint(uint64_t term, uint64_t log_entry);
  void become_follower_with_vote_request(uint64_t term);
  void send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index);
  void send_client_request(uint64_t term, const char * cmd, uint64_t client_index, const boost::dynamic_bitset<> & send_responses_from);
};

RaftTestFixture::RaftTestFixture()
  :
  cluster_size(5)
{
  std::vector<raft::peer> peers(cluster_size);
  for(std::size_t i=0; i<cluster_size; ++i) {
    peers[i].peer_id = i;
  }
  s.reset(new raft::server(comm, c, 0, peers));
  BOOST_CHECK_EQUAL(0U, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
}

void RaftTestFixture::make_leader(uint64_t term)
{
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s->on_timer();
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(raft::server::CANDIDATE, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(0U, comm.q.back().which());
    comm.q.pop_back();
  }
  raft::vote_response vote_response_msg;
  for(uint64_t p=1; p!=cluster_size; ++p) {
    vote_response_msg.peer_id = p;
    vote_response_msg.term_number = term;
    vote_response_msg.request_term_number = term;
    vote_response_msg.granted = true;
    s->on_vote_response(vote_response_msg);
  }
  BOOST_CHECK_EQUAL(raft::server::LEADER, s->get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    comm.q.pop_back();
  }
}

void RaftTestFixture::make_follower_with_checkpoint(uint64_t term, uint64_t log_entry)
{
  {
    raft::append_checkpoint_chunk msg;
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

void RaftTestFixture::become_follower_with_vote_request(uint64_t term)
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

void RaftTestFixture::send_client_request_and_commit(uint64_t term, const char * cmd, uint64_t client_index)
{
  boost::dynamic_bitset<> responses;
  responses.resize(cluster_size, 1);
  send_client_request(term, cmd, client_index, responses);
  BOOST_CHECK_EQUAL(client_index+1, s->commit_index());
}

void RaftTestFixture::send_client_request(uint64_t term, const char * cmd, uint64_t client_index,
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
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  std::size_t expected = 1;
  std::size_t num_responses = 0;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::append_entry>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(term, boost::get<raft::append_entry>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(client_index, boost::get<raft::append_entry>(comm.q.back()).previous_log_index);
    // Can't really check this in general
    // BOOST_CHECK_EQUAL(client_index > 0 ? term : 0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(client_index, boost::get<raft::append_entry>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(raft::log_entry::COMMAND, boost::get<raft::append_entry>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(term, boost::get<raft::append_entry>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp(cmd, boost::get<raft::append_entry>(comm.q.back()).entry[0].data.c_str()));
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

BOOST_FIXTURE_TEST_CASE(AppendEntriesLogSync, RaftTestFixture)
{
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 0;
    msg.leader_id = 2;
    msg.previous_log_index = 2;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 1;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 2;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 1;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    for(std::size_t i=1; i<=3; ++i) {
      msg.entry.push_back(raft::log_entry());
      msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 3;
    msg.leader_id = 2;
    msg.previous_log_index = 3;
    msg.previous_log_term = 3;
    msg.leader_commit_index = 3;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 3;
    msg.leader_id = 2;
    msg.previous_log_index = 1;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 3;
    for (std::size_t i=2; i<=4; ++i) {
      msg.entry.push_back(raft::log_entry());
      msg.entry.back().type = raft::log_entry::COMMAND;
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
    // Wait so the server will try to send log records.
    s->on_timer();
    BOOST_CHECK_EQUAL(1U, s->current_term());
    BOOST_CHECK_EQUAL(raft::server::LEADER, s->get_state());
    BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
    uint64_t expected = 1;
    while(comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(2U, comm.q.back().which());
      BOOST_CHECK_EQUAL(expected, boost::get<raft::append_entry>(comm.q.back()).recipient_id);
      BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).term_number);
      BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_id);
      BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_index);
      BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).previous_log_term);
      BOOST_CHECK_EQUAL(0U, boost::get<raft::append_entry>(comm.q.back()).leader_commit_index);
      BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).entry.size());
      BOOST_CHECK_EQUAL(raft::log_entry::COMMAND, boost::get<raft::append_entry>(comm.q.back()).entry[0].type);
      BOOST_CHECK_EQUAL(1U, boost::get<raft::append_entry>(comm.q.back()).entry[0].term);
      BOOST_CHECK_EQUAL(0, ::strcmp("1", boost::get<raft::append_entry>(comm.q.back()).entry[0].data.c_str()));
      raft::append_response resp;
      resp.recipient_id = expected;
      resp.term_number = 1;
      resp.request_term_number = 1;
      resp.begin_index = 0;
      resp.last_index = attempt == 0 ? 0 : 1;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 2;
    msg.leader_id = 2;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_checkpoint_chunk msg;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 1;
    msg.leader_id = 1;
    msg.previous_log_index = 1;
    msg.previous_log_term = 1;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
      raft::append_entry msg;
      msg.recipient_id = 0;
      msg.term_number = 2;
      msg.leader_id = 1;
      msg.previous_log_index = i-1;
      msg.previous_log_term = i==1 ? 0 : 2;
      msg.leader_commit_index = i-1;
      msg.entry.push_back(raft::log_entry());
      msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 3;
    msg.leader_id = 1;
    msg.previous_log_index = 3;
    msg.previous_log_term = 2;
    msg.leader_commit_index = 4;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_checkpoint_chunk msg;
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
    raft::append_checkpoint_chunk msg;
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
    raft::append_entry msg;
    msg.recipient_id = 0;
    msg.term_number = 2;
    msg.leader_id = 2;
    msg.previous_log_index = 0;
    msg.previous_log_term = 0;
    msg.leader_commit_index = 0;
    msg.entry.push_back(raft::log_entry());
    msg.entry.back().type = raft::log_entry::COMMAND;
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
    raft::append_checkpoint_chunk msg;
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
    raft::append_checkpoint_chunk msg;
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
  uint64_t client_index=0;
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

  uint64_t client_index=0;
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

  uint64_t client_index=0;
  send_client_request_and_commit(term, "1", client_index++);  
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  // Advance term; we should still be able to checkpoint at the old term
  term = 2;
  become_follower_with_vote_request(term);
  term = 3;
  make_leader(term);
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

  uint64_t client_index=0;
  send_client_request_and_commit(term, "1", client_index++);  
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_index());
  BOOST_CHECK_EQUAL(0U, s->checkpoint().last_checkpoint_term());
  BOOST_CHECK(nullptr == s->checkpoint().last_checkpoint().get());
  auto ckpt = s->begin_checkpoint(2U);
  BOOST_CHECK(nullptr == ckpt.get());
}

// Test that append_entries will send a checkpoint that needs log entries the leader has discarded
// post checkpoint
BOOST_FIXTURE_TEST_CASE(AppendEntriesCheckpoint, RaftTestFixture)
{
  uint64_t term = 1;
  make_leader(term);

  const char * cmd = "1";
  uint64_t client_index=0;
  // Send success response from all peers except 1.  This will commit entry
  // so that it can be checkpointed.
  boost::dynamic_bitset<> responses;
  responses.resize(cluster_size, true);
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
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  raft::append_checkpoint_chunk_response resp;
  resp.recipient_id = 1;
  resp.term_number = 1U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 2U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(4U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  resp.recipient_id = 1;
  resp.term_number = 1U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 4U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(4U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(5U, boost::get<raft::append_checkpoint_chunk>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  resp.recipient_id = 1;
  resp.term_number = 1U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 5U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_REQUIRE_EQUAL(0U, comm.q.size());
}


// TODO: Leader creates some log entries but fails to replicate them and crashes.  While crashed the rest of the cluster picks up
// and moves forward on a new term (or many new terms) and successfully replicate entries.  Once the former leader rejoins the cluster
// and the new leader figures out the last index in the former leaders log that doesn't cause a gap, we'll detect a term mismatch on
// the tail of the old leaders log and eventually make our way back to the point at which the logs agree.
