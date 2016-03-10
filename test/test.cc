#include <chrono>
#include <thread>

#include "server.hh"

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
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
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
