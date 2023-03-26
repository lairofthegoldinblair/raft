#include <array>
#include <chrono>
#include <thread>

#include "messages.hh"
#include "protocol.hh"
#include "slice.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>

#include <deque>
#include "boost/variant.hpp"

#define BOOST_TEST_MODULE RaftTests
#include <boost/test/unit_test.hpp>

template<typename peer_type, typename configuration_description_type>
class test_configuration
{
public:
  typedef typename std::vector<peer_type>::iterator peer_iterator;
private:
  // The cluster
  std::vector<peer_type> cluster_;
  // My cluster id/index
  std::size_t cluster_idx_;
  // ????
  configuration_description_type default_;
public:
  test_configuration(std::size_t cluster_idx, const std::vector<peer_type>& peers)
    :
    cluster_(peers),
    cluster_idx_(cluster_idx)
  {
  }

  peer_type & self() {
    return cluster_[cluster_idx_];
  }

  std::size_t num_known_peers() const
  {
    return cluster_.size();
  }

  peer_iterator begin_peers()
  {
    return cluster_.begin();
  }

  peer_iterator end_peers()
  {
    return cluster_.end();
  }

  std::size_t my_cluster_id() const
  {
    return cluster_idx_;
  }

  peer_type & peer_from_id(uint64_t peer_id) {
    return cluster_[peer_id];
  }

  const peer_type & get_peer_from_id(uint64_t peer_id) const {
    return cluster_.at(peer_id);
  }

  bool has_quorum() const {
    // Majority quorum logic
    std::size_t num_votes(0);
    for(auto & p : cluster_) {
      if(p.peer_id == cluster_idx_ || p.vote_) {
	num_votes += 1;
      }
    }
    return num_votes > (cluster_.size()/2);
  }

  uint64_t get_committed(uint64_t last_synced_index) const {
    // Figure out the minimum over a quorum.
    std::vector<uint64_t> acked;
    // For a leader, syncing to a log is "acking"
    acked.push_back(last_synced_index);
    // For peers we need an append response to get an ack
    for(std::size_t i=0; i<num_known_peers(); ++i) {
      if (i != my_cluster_id()) {
	acked.push_back(get_peer_from_id(i).match_index_);
      }
    }
    std::sort(acked.begin(), acked.end());
    return acked[(acked.size()-1)/2];
  }

  void reset_staging_servers()
  {
    // Noop
  }

  uint64_t configuration_id() const {
    return 0;
  }

  bool includes_self() const {
    return true;
  }

  bool is_transitional() const {
    return false;
  }

  const configuration_description_type & description() const {
    return default_;
  }
};

class test_configuration_change
{
public:
  // We start out with an unattainable goal but this will get fixed up in the next interval.
  test_configuration_change(std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
  }
    
  void on_append_response(std::chrono::time_point<std::chrono::steady_clock> clock_now,
			  uint64_t match_index,
			  uint64_t last_log_index)
  {
  }

  bool is_caught_up() const
  {
    return true;
  }
};

template<typename _Peer, typename configuration_description_type>
class test_configuration_manager
{
public:
  typedef typename _Peer::template apply<raft::peer_configuration_change>::type peer_type;
  typedef test_configuration<peer_type, configuration_description_type> configuration_type;
  typedef configuration_description_type description_type;
  typedef typename configuration_description_type::checkpoint_type checkpoint_type;
private:
  configuration_type configuration_;
  checkpoint_type default_;
public:
  test_configuration_manager(std::size_t cluster_idx, const std::vector<peer_type>& peers)
    :
    configuration_(cluster_idx, peers)
  {
  }
    
  const configuration_type & configuration() const
  {
    return configuration_;
  }

  configuration_type & configuration()
  {
    return configuration_;
  }

  bool has_configuration_at(uint64_t log_index) const
  {
    return false;
  }

  void add_logged_description(uint64_t log_index, const description_type & description)
  {
    // Not supported
  }

  void set_checkpoint(const checkpoint_type & description)
  {
    // TODO: Validate that config is same
  }
    
  const checkpoint_type &  get_checkpoint()
  {
    return default_;
  }
    
  void get_checkpoint_state(uint64_t log_index, checkpoint_type & ck) const
  {
  }

  void truncate_prefix(uint64_t )
  {
  }

  void truncate_suffix(uint64_t )
  {
  }    
};

template<typename _Messages>
class test_communicator
{
public:
  typedef size_t endpoint;
  template<typename _T>
  void send(endpoint ep, const std::string& address, const _T & msg)
  {
    q.push_front(msg);
  }
  
  void vote_request(endpoint ep, const std::string & address,
		    uint64_t recipient_id,
		    uint64_t term_number,
		    uint64_t candidate_id,
		    uint64_t last_log_index,
		    uint64_t last_log_term)
  {
    typename _Messages::request_vote_type msg;
    msg.recipient_id_=recipient_id;
    msg.term_number_=term_number;
    msg.candidate_id_=candidate_id;
    msg.last_log_index_=last_log_index;
    msg.last_log_term_=last_log_term;
    send(ep, address, msg);	
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
    typename _Messages::append_entry_type msg;
    msg.set_recipient_id(recipient_id);
    msg.set_term_number(term_number);
    msg.set_leader_id(leader_id);
    msg.set_previous_log_index(previous_log_index);
    msg.set_previous_log_term(previous_log_term);
    msg.set_leader_commit_index(leader_commit_index);
    for(uint64_t i=0; i<num_entries; ++i) {
      msg.add_entry(entries(i));
    }
    q.push_front(msg);
  }
	
  void append_entry_response(endpoint ep, const std::string& address,
			     uint64_t recipient_id,
			     uint64_t term_number,
			     uint64_t request_term_number,
			     uint64_t begin_index,
			     uint64_t last_index,
			     bool success)
  {
    typename _Messages::append_entry_response_type msg;
    msg.recipient_id = recipient_id;
    msg.term_number = term_number;
    msg.request_term_number = request_term_number;
    msg.begin_index = begin_index;
    msg.last_index = last_index;
    msg.success = success;
    q.push_front(msg);
  }

  void vote_response(endpoint ep, const std::string& address,
		     uint64_t peer_id,
		     uint64_t term_number,
		     uint64_t request_term_number,
		     bool granted)
  {
    typename _Messages::vote_response_type msg;
    msg.peer_id = peer_id;
    msg.term_number = term_number;
    msg.request_term_number = request_term_number;
    msg.granted = granted;
    q.push_front(msg);
  }

  void append_checkpoint_chunk(endpoint ep, const std::string& address,
			       uint64_t recipient_id,
			       uint64_t term_number,
			       uint64_t leader_id,
			       uint64_t last_checkpoint_index,
			       uint64_t last_checkpoint_term,
			       raft::configuration_checkpoint<raft::configuration_description> last_checkpoint_configuration,
			       uint64_t checkpoint_begin,
			       uint64_t checkpoint_end,
			       bool checkpoint_done,
			       raft::slice data)
  {
    typename _Messages::append_checkpoint_chunk_type msg;
    msg.recipient_id=recipient_id;
    msg.term_number=term_number;
    msg.leader_id=leader_id;
    msg.last_checkpoint_index=last_checkpoint_index;
    msg.last_checkpoint_term=last_checkpoint_term;
    msg.last_checkpoint_configuration=last_checkpoint_configuration;
    msg.checkpoint_begin=checkpoint_begin;
    msg.checkpoint_end=checkpoint_end;
    msg.checkpoint_done=checkpoint_done;
    msg.data.assign(raft::slice::buffer_cast<const uint8_t *>(data),
		    raft::slice::buffer_cast<const uint8_t *>(data) + raft::slice::buffer_size(data));
    q.push_front(msg);
  }		       
  
  void append_checkpoint_chunk_response(endpoint ep, const std::string& address,
					uint64_t recipient_id,
					uint64_t term_number,
					uint64_t request_term_number,
					uint64_t bytes_stored)
  {
    typename _Messages::append_checkpoint_chunk_response_type msg;    
    msg.recipient_id = recipient_id;
    msg.term_number = term_number;
    msg.request_term_number = request_term_number;
    msg.bytes_stored = bytes_stored;
    q.push_front(msg);
  }

  typedef boost::variant<typename _Messages::request_vote_type, typename _Messages::vote_response_type,
			 typename _Messages::append_entry_type, typename _Messages::append_entry_response_type,
			 typename _Messages::append_checkpoint_chunk_type, typename _Messages::append_checkpoint_chunk_response_type> any_msg_type;
  std::deque<any_msg_type> q;
};

struct communicator_metafunction
{
  template <typename _Messages>
  struct apply
  {
    typedef test_communicator<_Messages> type;
  };
};

typedef raft::protocol<communicator_metafunction, raft::native::messages> test_raft_type;

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
  raft::configuration_algorithm<test_peer, raft::configuration_description> config(0);
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
  raft::configuration_manager<test_peer_metafunction, raft::configuration_description, raft::native::messages> mgr(0);
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
  raft::configuration_manager<test_peer_metafunction, raft::configuration_description, raft::native::messages> cm(0);
  raft::configuration_manager<test_peer_metafunction, raft::configuration_description, raft::native::messages>::checkpoint_type ckpt;
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
  // std::vector<test_raft_type::peer_type> peers(cluster_size);
  // for(std::size_t i=0; i<cluster_size; ++i) {
  //   peers[i].peer_id = i;
  // }
  // test_raft_type::configuration_manager_type cm(0, peers);

  test_raft_type::configuration_manager_type cm(0);
  test_raft_type::configuration_manager_type::checkpoint_type ckpt;
  ckpt.index = 0;
  ckpt.description.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  cm.set_checkpoint(ckpt);
  BOOST_CHECK_EQUAL(0U, cm.configuration().configuration_id());
  BOOST_CHECK_EQUAL(5U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  std::size_t cluster_size = cm.configuration().num_known_peers();

  test_raft_type::communicator_type comm;
  test_raft_type::client_type c;
  test_raft_type::log_type l;
  test_raft_type::checkpoint_data_store_type store;
 
  test_raft_type s(comm, l, store, cm);
  BOOST_CHECK_EQUAL(0U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(1U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
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
    BOOST_CHECK_EQUAL(expected, boost::get<raft::native::request_vote>(comm.q.back()).recipient_id_);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::native::request_vote>(comm.q.back()).candidate_id_);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::native::request_vote>(comm.q.back()).term_number_);
    expected += 1;
    comm.q.pop_back();
  }

  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
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
    BOOST_CHECK_EQUAL(expected, boost::get<raft::native::request_vote>(comm.q.back()).recipient_id_);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::native::request_vote>(comm.q.back()).candidate_id_);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::native::request_vote>(comm.q.back()).term_number_);
    expected += 1;
    comm.q.pop_back();
  }

  // Provide one vote
  raft::native::vote_response vote_response_msg;
  vote_response_msg.peer_id = 1;
  vote_response_msg.term_number = 2;
  vote_response_msg.request_term_number = 2;
  vote_response_msg.granted = true;
  s.on_vote_response(vote_response_msg);
  // BOOST_CHECK(true == s.get_peer_from_id(1).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Any vote from prior term should be ignored
  vote_response_msg.peer_id = 2;
  vote_response_msg.term_number = 1;
  vote_response_msg.request_term_number = 1;
  vote_response_msg.granted = true;
  s.on_vote_response(vote_response_msg);
  // BOOST_CHECK(true == s.get_peer_from_id(1).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Valid vote! Become leader
  vote_response_msg.peer_id = 2;
  vote_response_msg.term_number = 2;
  vote_response_msg.request_term_number = 2;
  vote_response_msg.granted = true;
  s.on_vote_response(vote_response_msg);
  // BOOST_CHECK(true == s.get_peer_from_id(1).vote_);
  // BOOST_CHECK(true == s.get_peer_from_id(2).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::NOOP, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].type);
    raft::native::append_response resp;
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
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  
  // Wait a bit and should get a round of heartbeat messages
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    // TODO: What about the next 3 values ????
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    expected += 1;
    comm.q.pop_back();
  }

  // Old append_entry should elicit a response with updated term
  auto ae_msg = new test_raft_type::messages_type::append_entry_type();
  ae_msg->recipient_id = 0;
  ae_msg->term_number = 1;
  ae_msg->leader_id = 0;
  ae_msg->previous_log_index = ae_msg->previous_log_term = ae_msg->leader_commit_index = 0;
  ae_msg->entry.resize(0);
  s.on_append_entry(ae_msg, [ae_msg]() { delete ae_msg; });
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(3U, comm.q.back().which());
    BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
    comm.q.pop_back();
  }

  // Fire off a client_request
  raft::native::client_request cli_req;
  cli_req.command = "1";
  s.on_client_request(c, cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("1", boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
    raft::native::append_response resp;
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
  cli_req.command = "2";
  s.on_client_request(c, cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  cli_req.command = "3";
  s.on_client_request(c,cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log has sync'd to disk so we only need 2
  // other members of the cluster to ack
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  s.on_log_sync(4);
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("2", boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
    BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[1].type);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[1].term);
    BOOST_CHECK_EQUAL(0, ::strcmp("3", boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[1].data.c_str()));
    raft::native::append_response resp;
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
      c.responses.pop_back();
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
  cli_req.command = "4";
  s.on_client_request(c, cli_req);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  s.on_timer();
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(expected <= 2 ? 4U : 2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(4U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    if (expected <= 2) {
      BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
      BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].type);
      BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].term);
      BOOST_CHECK_EQUAL(0, ::strcmp("4", boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
    } else {
      BOOST_CHECK_EQUAL(3U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
      for(std::size_t i=0; i<=2; ++i) {
	BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[i].type);
	BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[i].term);
	BOOST_CHECK_EQUAL(0, ::strcmp((boost::format("%1%") % (i+2)).str().c_str(),
				      boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[i].data.c_str()));
      }
    }
    raft::native::append_response resp;
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
  test_raft_type::communicator_type comm;
  test_raft_type::client_type c;
  test_raft_type::log_type l;
  test_raft_type::checkpoint_data_store_type store;

  l.append(test_raft_type::get_bootstrap_log_entry(2, "192.168.1.1"));
  uint64_t term = l.last_entry_term();
  BOOST_CHECK_EQUAL(0U, term);
  l.update_header(term, test_raft_type::INVALID_PEER_ID);
 
  test_raft_type::configuration_manager_type cm(2);
  test_raft_type s(comm, l, store, cm);
  BOOST_CHECK_EQUAL(term, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s.get_state());
  BOOST_CHECK(cm.has_configuration_at(0));
  BOOST_CHECK_EQUAL(1U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  BOOST_CHECK_EQUAL(2U, cm.configuration().self().peer_id);
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Run timer then we should become leader of a single node cluster
  std::this_thread::sleep_for (std::chrono::milliseconds(500));
  s.on_timer();
  BOOST_CHECK_EQUAL(term+1U, s.current_term());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK(s.log_header_sync_required());
  s.on_log_header_sync();
  BOOST_CHECK(!s.log_header_sync_required());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
}

class RaftTestFixtureBase
{
public:
  std::size_t cluster_size;
  test_raft_type::communicator_type comm;
  test_raft_type::client_type c;
  test_raft_type::log_type l;
  test_raft_type::checkpoint_data_store_type store;
  std::shared_ptr<test_raft_type::configuration_manager_type> cm;
  std::shared_ptr<test_raft_type> s;
  test_raft_type::simple_configuration_description_type five_servers;
  test_raft_type::simple_configuration_description_type six_servers;

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
  test_raft_type::messages_type::set_configuration_request_type req;
  req.old_id = 0;
  req.new_configuration = six_servers;
  s->on_set_configuration(c, req);
  BOOST_CHECK_EQUAL(req.new_configuration.servers.size(), num_known_peers());
  
  // Run timer then we should get append_entries for the newly added server
  s->on_timer();
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(term, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(leader_id, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(commit_index, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(commit_index, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    raft::native::append_response resp;
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
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(0U, comm.q.back().which());
    comm.q.pop_back();
  }
  raft::native::vote_response vote_response_msg;
  for(uint64_t p=1; p!=num_known_peers(); ++p) {
    vote_response_msg.peer_id = p;
    vote_response_msg.term_number = term;
    vote_response_msg.request_term_number = term;
    vote_response_msg.granted = true;
    s->on_vote_response(vote_response_msg);
  }
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s->get_state());
  BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
  for(uint64_t p=1; p!=num_known_peers(); ++p) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::NOOP, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].type);
    if (respond_to_noop) {
      raft::native::append_response resp;
      resp.recipient_id = p;
      resp.term_number = term;
      resp.request_term_number = term;
      resp.begin_index = 0;
      resp.last_index = boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index+1;
      resp.success = true;
      s->on_append_response(resp);
    }
    comm.q.pop_back();
  }
}

void RaftTestFixtureBase::make_follower_with_checkpoint(uint64_t term, uint64_t log_entry)
{
  {
    test_raft_type::messages_type::append_checkpoint_chunk_type msg;
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
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(0U, comm.q.size());

  s->on_checkpoint_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(term, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(term, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
  comm.q.pop_back();
}

void RaftTestFixtureBase::become_follower_with_vote_request(uint64_t term)
{
  raft::native::request_vote msg;
  msg.set_recipient_id(0);
  msg.set_term_number(term);
  msg.set_candidate_id(1);
  msg.set_last_log_index(0);
  msg.set_last_log_term(0);
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(term, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(term, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
  // Don't worry about vote; it will be no unless the server's log was empty
  //BOOST_CHECK(boost::get<raft::native::vote_response>(comm.q.back()).granted);
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
  raft::native::client_request cli_req;

  cli_req.command = cmd;
  s->on_client_request(c, cli_req);
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  s->on_timer();
  BOOST_CHECK_EQUAL(term, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s->get_state());
  BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
  std::size_t expected = 1;
  std::size_t num_responses = 0;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(term, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(client_index, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    // Can't really check this in general
    // BOOST_CHECK_EQUAL(client_index > 0 ? term : 0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(client_index, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].type);
    BOOST_CHECK_EQUAL(term, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].term);
    BOOST_CHECK_EQUAL(0, ::strcmp(cmd, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[0].data.c_str()));
      raft::native::append_response resp;
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
      c.responses.pop_back();
    }
    expected += 1;
    comm.q.pop_back();
  }
}

class RaftTestFixture : public RaftTestFixtureBase
{
public:
  RaftTestFixture();
  ~RaftTestFixture()
  {
  }
};

RaftTestFixture::RaftTestFixture()
{
  cluster_size = 5;
  five_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  six_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"},  {5, "192.168.1.6"}};
  // std::vector<test_raft_type::peer_type> peers(cluster_size);
  // for(std::size_t i=0; i<cluster_size; ++i) {
  //   peers[i].peer_id = i;
  // }
  // cm.reset(new test_raft_type::configuration_manager_type(0, peers));
  cm.reset(new test_raft_type::configuration_manager_type(0));
  // std::vector<test_raft_type::log_entry_type> entries;
  // entries.push_back(test_raft_type::log_entry_type());
  // entries.back().type = test_raft_type::log_entry_type::CONFIGURATION;
  // entries.back().term = 0;
  // entries.back().configuration.from = five_servers;
  // l.append(entries.begin(), entries.end());
  // l.update_header(entries.back().term, test_raft_type::INVALID_PEER_ID);
  test_raft_type::configuration_manager_type::checkpoint_type ckpt;
  ckpt.index = 0;
  ckpt.description.from = five_servers;
  BOOST_CHECK(ckpt.is_valid());
  cm->set_checkpoint(ckpt);
  BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
  BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
  BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
  BOOST_CHECK(cm->configuration().includes_self());
  s.reset(new test_raft_type(comm, l, store, *cm.get()));
  // BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
  // BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
  // BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
  // BOOST_CHECK(cm->configuration().includes_self());
  BOOST_CHECK_EQUAL(0U, s->current_term());
  BOOST_CHECK_EQUAL(0U, s->commit_index());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // BOOST_CHECK_EQUAL(0U, l.start_index());
  // BOOST_CHECK_EQUAL(1U, l.last_index());
}

BOOST_FIXTURE_TEST_CASE(AppendEntriesLogSync, RaftTestFixture)
{
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 1;
    msg->leader_id = 1;
    msg->previous_log_index = 0;
    msg->previous_log_term = 0;
    msg->leader_commit_index = 0;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 1;
    msg->entry.back().data = "1";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_sync(1);
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Pretend a leader from expired term sends a message, this should respond with current term
  {
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 0;
    msg->leader_id = 2;
    msg->previous_log_index = 2;
    msg->previous_log_term = 1;
    msg->leader_commit_index = 1;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 0;
    msg->entry.back().data = "0";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(!boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Supposing the leader has committed lets go to another message
  // which creates a gap.  This should be rejected by the peer.
  {
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 1;
    msg->leader_id = 1;
    msg->previous_log_index = 2;
    msg->previous_log_term = 1;
    msg->leader_commit_index = 1;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 1;
    msg->entry.back().data = "3";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(!boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Send three messages with the first one a duplicate
  {
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 1;
    msg->leader_id = 1;
    msg->previous_log_index = 0;
    msg->previous_log_term = 0;
    msg->leader_commit_index = 0;
    for(std::size_t i=1; i<=3; ++i) {
      msg->entry.push_back(test_raft_type::log_entry_type());
      msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
      msg->entry.back().term = 1;
      msg->entry.back().data = (boost::format("%1%") % i).str();
    }
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_sync(3);
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Let's go to another message
  // which uses a newer term.  The idea in this example is that entries previously
  // sent to the peer didn't get committed but a new leader got elected and DID commit
  // at those indexes and is trying to append from them on the new term.  We must reject
  // so that the new leader backs up to find where its log agrees with that of the peer.
  {
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 3;
    msg->leader_id = 2;
    msg->previous_log_index = 3;
    msg->previous_log_term = 3;
    msg->leader_commit_index = 3;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 3;
    msg->entry.back().data = "4";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(!boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();

  // Let's suppose that only log entry at index 1 on term 1 got committed.  We should be able to
  // overwrite log entries starting at that point.
  {
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 3;
    msg->leader_id = 2;
    msg->previous_log_index = 1;
    msg->previous_log_term = 1;
    msg->leader_commit_index = 3;
    for (std::size_t i=2; i<=4; ++i) {
      msg->entry.push_back(test_raft_type::log_entry_type());
      msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
      msg->entry.back().term = 3;
      msg->entry.back().data = (boost::format("%1%a") % i).str();
    }
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_sync(4);
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(3U, comm.q.back().which());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(4U, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();
}

#include <iostream>
BOOST_FIXTURE_TEST_CASE(AppendEntriesNegativeResponse, RaftTestFixture)
{
  // Make me leader
  make_leader(1);
  // Client request to trigger append entries
  raft::native::client_request cli_req;
  cli_req.command = "1";
  s->on_client_request(c, cli_req);

  // On first attempt have clients respond negatively.  On second have them succeed
  for(std::size_t attempt=0; attempt<=1; ++attempt) {
    std::cout << "AppendEntriesNegativeResponse attempt " << attempt << std::endl;
    // Wait so the server will try to send log records.
    s->on_timer();
    BOOST_CHECK_EQUAL(1U, s->current_term());
    BOOST_CHECK_EQUAL(test_raft_type::LEADER, s->get_state());
    BOOST_CHECK_EQUAL(num_known_peers()-1, comm.q.size());
    uint64_t expected = 1;
    while(comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(2U, comm.q.back().which());
      BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
      BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
      BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
      BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
      BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
      BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
      BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 2U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
      BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[attempt].type);
      BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[attempt].term);
      BOOST_CHECK_EQUAL(0, ::strcmp("1", boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry[attempt].data.c_str()));
      raft::native::append_response resp;
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
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 1;
    msg->leader_id = 1;
    msg->previous_log_index = 0;
    msg->previous_log_term = 0;
    msg->leader_commit_index = 0;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 1;
    msg->entry.back().data = "1";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 2;
    msg->leader_id = 2;
    msg->previous_log_index = 0;
    msg->previous_log_term = 0;
    msg->leader_commit_index = 0;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 2;
    msg->entry.back().data = "2";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    test_raft_type::messages_type::append_checkpoint_chunk_type msg;
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
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // Wait enough time for a election timeout.  Current logic says that we won't start an election
  // if the log header sync is outstanding
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  s->on_timer();
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // This one doesn't require a new term so it gets queued awaiting the log header sync
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 1;
    msg->leader_id = 1;
    msg->previous_log_index = 1;
    msg->previous_log_term = 1;
    msg->leader_commit_index = 0;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 2;
    msg->entry.back().data = "2";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());

  // TODO: Validate that the 2 append entries have beeen processed and are
  // awaiting log sync.
  
  s->on_log_sync(2);
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(2U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(BasicOnVoteRequestTest, RaftTestFixture)
{
  // FOLLOWER -> FOLLOWER
  raft::native::request_vote msg;
  msg.set_recipient_id(0);
  msg.set_term_number(1);
  msg.set_candidate_id(1);
  msg.set_last_log_index(0);
  msg.set_last_log_term(0);
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::native::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(OnVoteRequestSlowHeaderSyncTest, RaftTestFixture)
{
  // FOLLOWER -> FOLLOWER
  raft::native::request_vote msg;
  msg.set_recipient_id(0);
  msg.set_term_number(1);
  msg.set_candidate_id(1);
  msg.set_last_log_index(0);
  msg.set_last_log_term(0);
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // We are still waiting for header to sync to disk so will ignore subsequent request
  msg.set_recipient_id(0);
  msg.set_term_number(2);
  msg.set_candidate_id(2);
  msg.set_last_log_index(0);
  msg.set_last_log_term(0);
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::native::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  // Send again now that we are sync'd
  s->on_request_vote(msg);
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(2U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(2U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::native::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  // Send a couple of entries
  for(std::size_t i=1; i<=3; ++i) {
    {
      auto msg = new test_raft_type::messages_type::append_entry_type();
      msg->recipient_id = 0;
      msg->term_number = 2;
      msg->leader_id = 1;
      msg->previous_log_index = i-1;
      msg->previous_log_term = i==1 ? 0 : 2;
      msg->leader_commit_index = i-1;
      msg->entry.push_back(test_raft_type::log_entry_type());
      msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
      msg->entry.back().term = 2;
      msg->entry.back().data = (boost::format("%1%") % i).str();
      s->on_append_entry(msg, [msg]() { delete msg; });
    }
    s->on_log_sync(i);
    BOOST_CHECK_EQUAL(2U, s->current_term());
    BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
    BOOST_CHECK_EQUAL(1U, comm.q.size());
    BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(2U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
    BOOST_CHECK_EQUAL(i-1, boost::get<raft::native::append_response>(comm.q.back()).begin_index);
    BOOST_CHECK_EQUAL(i, boost::get<raft::native::append_response>(comm.q.back()).last_index);
    BOOST_CHECK(boost::get<raft::native::append_response>(comm.q.back()).success);
    comm.q.pop_back();
  }

  // Now initiate a leadership change but without up to date log; this should advance term but
  // we should not get the vote.
  msg.set_recipient_id(0);
  msg.set_term_number(3);
  msg.set_candidate_id(1);
  msg.set_last_log_index(0);
  msg.set_last_log_term(0);
  s->on_request_vote(msg);
  // Updated current_term requires header sync
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(!boost::get<raft::native::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  // Now initiate a leadership change but with up to date log we'll get the vote
  msg.set_recipient_id(0);
  msg.set_term_number(3);
  msg.set_candidate_id(1);
  msg.set_last_log_index(3);
  msg.set_last_log_term(2);
  s->on_request_vote(msg);
  // Updated voted_for (but not update current_term) requires header sync
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Let's suppose that the leader here got a quorum from other peers
  // it could start appending entries.  We have the correct term so we should
  // queue these up and continue to wait for the existing header sync to complete.
  // TODO: We could also get an append_entry for the current term without ever seeing
  // a request_vote from the leader.
  {
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 3;
    msg->leader_id = 1;
    msg->previous_log_index = 3;
    msg->previous_log_term = 2;
    msg->leader_commit_index = 4;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 3;
    msg->entry.back().data = "4";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(boost::get<raft::native::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();

  s->on_log_sync(4);
  BOOST_CHECK_EQUAL(3U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(3U, boost::get<raft::native::append_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(3, boost::get<raft::native::append_response>(comm.q.back()).begin_index);
  BOOST_CHECK_EQUAL(4, boost::get<raft::native::append_response>(comm.q.back()).last_index);
  BOOST_CHECK(boost::get<raft::native::append_response>(comm.q.back()).success);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(AppendCheckpointChunk, RaftTestFixture)
{
  {
    test_raft_type::messages_type::append_checkpoint_chunk_type msg;
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
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(0U, comm.q.size());

  s->on_checkpoint_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
  comm.q.pop_back();
}

BOOST_FIXTURE_TEST_CASE(AppendCheckpointChunkSlowHeaderSync, RaftTestFixture)
{
  {
    test_raft_type::messages_type::append_checkpoint_chunk_type msg;
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
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    auto msg = new test_raft_type::messages_type::append_entry_type();
    msg->recipient_id = 0;
    msg->term_number = 2;
    msg->leader_id = 2;
    msg->previous_log_index = 0;
    msg->previous_log_term = 0;
    msg->leader_commit_index = 0;
    msg->entry.push_back(test_raft_type::log_entry_type());
    msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
    msg->entry.back().term = 2;
    msg->entry.back().data = "2";
    s->on_append_entry(msg, [msg]() { delete msg; });
  }
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  {
    // Since a log header sync is outstanding we will ignore a new term
    test_raft_type::messages_type::append_checkpoint_chunk_type msg;
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
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  {
    // This one doesn't require a new term so it gets queued awaiting the log header sync
    test_raft_type::messages_type::append_checkpoint_chunk_type msg;
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
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
  comm.q.pop_back();

  s->on_checkpoint_sync();
  BOOST_CHECK_EQUAL(1U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).request_term_number);
  BOOST_CHECK_EQUAL(2U, boost::get<raft::native::append_checkpoint_chunk_response>(comm.q.back()).bytes_stored);
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
  BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  raft::native::append_checkpoint_chunk_response resp;
  // Ack with bytes_stored=2 twice to validate the checkpoint protocol will resend data if requested
  for(int i=0; i<2; ++i) {
    resp.recipient_id = 1;
    resp.term_number = 1U;
    resp.request_term_number = 1U;
    resp.bytes_stored = 2U;
    s->on_append_checkpoint_chunk_response(resp);  
    BOOST_REQUIRE_EQUAL(1U, comm.q.size());
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
    BOOST_CHECK_EQUAL(4U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
    comm.q.pop_back();
  }

  resp.recipient_id = 1;
  resp.term_number = 1U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 4U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_REQUIRE_EQUAL(1U, comm.q.size());
  BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(4U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(5U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
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
  BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).recipient_id);
  BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).leader_id);
  BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_begin);
  BOOST_CHECK_EQUAL(2U, boost::get<test_raft_type::messages_type::append_checkpoint_chunk_type>(comm.q.back()).checkpoint_end);
  comm.q.pop_back();

  raft::native::append_checkpoint_chunk_response resp;
  resp.recipient_id = 1;
  resp.term_number = 2U;
  resp.request_term_number = 1U;
  resp.bytes_stored = 2U;
  s->on_append_checkpoint_chunk_response(resp);  
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(2U, s->current_term());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
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
  BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::COMMAND, l.entry(l.last_index()-1).type);
  // TODO: Should I really have to do this on_timer call to trigger the transitional entry????
  s->on_timer();
  auto & le(l.entry(l.last_index()-1));
  BOOST_CHECK(cm->configuration().is_transitional());
  BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::CONFIGURATION, le.type);
  BOOST_CHECK_EQUAL(5U, le.configuration.from.servers.size());
  BOOST_CHECK_EQUAL(6U, le.configuration.to.servers.size());

  uint64_t expected=1;
  BOOST_CHECK_EQUAL(num_known_peers() - 1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(commit_index+2, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    BOOST_CHECK_EQUAL(3U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    raft::native::append_response resp;
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
      auto & le(l.entry(l.last_index()-1));
      BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::CONFIGURATION, le.type);
      BOOST_CHECK_EQUAL(5U, le.configuration.from.servers.size());
      BOOST_CHECK_EQUAL(6U, le.configuration.to.servers.size());
    } else {
      BOOST_CHECK_EQUAL(commit_index + 3U, s->commit_index());
      // Should get a new stable config entry in the log
      BOOST_CHECK(cm->configuration().is_stable());
      BOOST_CHECK_EQUAL(6U, num_known_peers());
      auto & le(l.entry(l.last_index()-1));
      BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::CONFIGURATION, le.type);
      BOOST_CHECK_EQUAL(6U, le.configuration.from.servers.size());
      BOOST_CHECK_EQUAL(0U, le.configuration.to.servers.size());
    }
    if(expected != 4) {
      BOOST_CHECK_EQUAL(0U, c.configuration_responses.size());
    } else {
      BOOST_REQUIRE(0U < c.configuration_responses.size());
      BOOST_CHECK_EQUAL(1U, c.configuration_responses.size());
      BOOST_CHECK_EQUAL(raft::native::SUCCESS, c.configuration_responses.front().result);
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
  auto msg = new test_raft_type::messages_type::append_entry_type();
  msg->recipient_id = 0;
  msg->term_number = term+1;
  msg->leader_id = 1;
  msg->previous_log_index = log_index+2;
  msg->previous_log_term = term;
  msg->leader_commit_index = s->commit_index();
  msg->entry.push_back(test_raft_type::log_entry_type());
  msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
  msg->entry.back().term = term+1;
  msg->entry.back().data = "1";
  s->on_append_entry(msg, [msg]() { delete msg; });
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_REQUIRE(0U < c.configuration_responses.size());
  BOOST_CHECK_EQUAL(raft::native::FAIL, c.configuration_responses.front().result);
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
  auto msg = new test_raft_type::messages_type::append_entry_type();
  msg->recipient_id = 0;
  msg->term_number = term+1;
  msg->leader_id = 1;
  msg->previous_log_index = log_index+3;
  msg->previous_log_term = term;
  msg->leader_commit_index = commit_index+3;
  msg->entry.push_back(test_raft_type::log_entry_type());
  msg->entry.back().type = test_raft_type::log_entry_type::COMMAND;
  msg->entry.back().term = term+1;
  msg->entry.back().data = "1";
  s->on_append_entry(msg, [msg]() { delete msg; });
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_REQUIRE(0U < c.configuration_responses.size());
  BOOST_CHECK_EQUAL(raft::native::SUCCESS, c.configuration_responses.front().result);
}

BOOST_FIXTURE_TEST_CASE(CandidateVoteRequestAtSameTerm, RaftTestFixture)
{
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  s->on_timer();
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s->get_state());
  BOOST_CHECK(s->log_header_sync_required());
  BOOST_CHECK_EQUAL(1U, s->current_term());

  // Vote request from old term gets immediate negative response
  {
    raft::native::request_vote msg;
    msg.set_recipient_id(0);
    msg.set_term_number(0);
    msg.set_candidate_id(1);
    msg.set_last_log_index(0);
    msg.set_last_log_term(0);
    s->on_request_vote(msg);
    BOOST_CHECK_EQUAL(1U, comm.q.size());
    BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
    BOOST_CHECK(!boost::get<raft::native::vote_response>(comm.q.back()).granted);
    comm.q.pop_back();
  }

  // Now another server independently gets to term 1 and asks for a vote
  // we'll get response after a header sync but it won't be granted.  This server
  // will also send out vote requests to all other peers upon the header sync.
  {
    raft::native::request_vote msg;
    msg.set_recipient_id(0);
    msg.set_term_number(1);
    msg.set_candidate_id(1);
    msg.set_last_log_index(0);
    msg.set_last_log_term(0);
    s->on_request_vote(msg);
    BOOST_CHECK_EQUAL(0U, comm.q.size());
  }

  // We use a brittle implementation detail here; vote response generated before vote requests.
  // Don't be surprised if this breaks some day.
  s->on_log_header_sync();
  BOOST_CHECK(!s->log_header_sync_required());
  BOOST_CHECK_EQUAL(num_known_peers(), comm.q.size());
  BOOST_CHECK_EQUAL(0U, boost::get<raft::native::vote_response>(comm.q.back()).peer_id);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::vote_response>(comm.q.back()).term_number);
  BOOST_CHECK_EQUAL(1U, boost::get<raft::native::vote_response>(comm.q.back()).request_term_number);
  BOOST_CHECK(!boost::get<raft::native::vote_response>(comm.q.back()).granted);
  comm.q.pop_back();
  uint32_t expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(0U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<raft::native::request_vote>(comm.q.back()).recipient_id_);
    BOOST_CHECK_EQUAL(0U, boost::get<raft::native::request_vote>(comm.q.back()).candidate_id_);
    BOOST_CHECK_EQUAL(1U, boost::get<raft::native::request_vote>(comm.q.back()).term_number_);
    expected += 1;
    comm.q.pop_back();
  }
}

class RaftConfigurationTestFixture : public RaftTestFixtureBase
{
public:
  RaftConfigurationTestFixture();
  ~RaftConfigurationTestFixture()
  {
  }
};

RaftConfigurationTestFixture::RaftConfigurationTestFixture()
{
  cluster_size = 5;
  five_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  six_servers.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"},  {5, "192.168.1.6"}};
  cm.reset(new test_raft_type::configuration_manager_type(0));
  auto entry = new test_raft_type::log_entry_type();
  entry->type = test_raft_type::log_entry_type::CONFIGURATION;
  entry->term = 0;
  entry->configuration.from = five_servers;
  l.append(std::pair<const test_raft_type::log_entry_type *, std::function<void()>>(entry, [entry]() { delete entry; }));
  l.update_header(0, test_raft_type::INVALID_PEER_ID);
  s.reset(new test_raft_type(comm, l, store, *cm.get()));
  BOOST_CHECK_EQUAL(0U, cm->configuration().configuration_id());
  BOOST_CHECK_EQUAL(0U, cm->configuration().my_cluster_id());
  BOOST_CHECK_EQUAL(5U, cm->configuration().num_known_peers());
  BOOST_CHECK(cm->configuration().includes_self());
  BOOST_CHECK_EQUAL(0U, s->current_term());
  BOOST_CHECK_EQUAL(0U, s->commit_index());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s->get_state());
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
  auto msg = new test_raft_type::messages_type::append_entry_type();
  msg->recipient_id = 0;
  msg->term_number = term;
  msg->leader_id = 1;
  msg->previous_log_index = log_index;
  msg->previous_log_term = term;
  msg->leader_commit_index = commit_index;
  msg->entry.push_back(test_raft_type::log_entry_type());
  msg->entry.back().type = test_raft_type::log_entry_type::CONFIGURATION;
  msg->entry.back().term = term;
  msg->entry.back().configuration.from = five_servers;
  msg->entry.back().configuration.to = six_servers;
  s->on_append_entry(msg, [msg]() { delete msg; });
  BOOST_CHECK(cm->configuration().is_transitional());
  auto & le(l.entry(l.last_index()-1));
  BOOST_CHECK_EQUAL(5U, le.configuration.from.servers.size());
  BOOST_CHECK_EQUAL(6U, le.configuration.to.servers.size());

  make_leader(term+1, false);

  // Now leader and have the config entry so should try to replicate it but a new leader
  // is optimistic and assumes that all peers have its log entries.  It will append a NOOP
  // and replicate that.
  s->on_timer();
  uint64_t expected=1;
  BOOST_CHECK_EQUAL(num_known_peers() - 1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(2U, comm.q.back().which());
    BOOST_CHECK_EQUAL(expected, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).recipient_id);
    BOOST_CHECK_EQUAL(term+1, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).term_number);
    BOOST_CHECK_EQUAL(0U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_id);
    BOOST_CHECK_EQUAL(commit_index, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).leader_commit_index);
    // Assumes peer also has the transitional config log entry
    BOOST_CHECK_EQUAL(log_index+1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_index);
    BOOST_CHECK_EQUAL(term, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).previous_log_term);
    BOOST_CHECK_EQUAL(1U, boost::get<test_raft_type::messages_type::append_entry_type>(comm.q.back()).entry.size());
    raft::native::append_response resp;
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
      auto & le(l.entry(l.last_index()-1));
      BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::NOOP, le.type);
    } else {
      // The log should be fully committed up through the NOOP entry  regardless of where
      // commit_index started
      BOOST_CHECK_EQUAL(log_index+2, s->commit_index());
      // Should get a new stable config entry in the log
      BOOST_CHECK(cm->configuration().is_stable());
      BOOST_CHECK_EQUAL(6U, num_known_peers());
      auto & le(l.entry(l.last_index()-1));
      BOOST_CHECK_EQUAL(test_raft_type::log_entry_type::CONFIGURATION, le.type);
      BOOST_CHECK_EQUAL(6U, le.configuration.from.servers.size());
      BOOST_CHECK_EQUAL(0U, le.configuration.to.servers.size());
    }
    // Did not initiate the config change so should not send a response
    BOOST_CHECK_EQUAL(0U, c.configuration_responses.size());
    expected += 1;
  }  
}
// TODO: FOLLOWER gets a transitional config which doesn't get committed and is overwritten; make sure no client callback is attempted.
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


BOOST_AUTO_TEST_CASE(SliceTotalSize)
{
  std::string a("This is a test buffer");
  std::string b("This is also a test buffer");
  std::vector<raft::slice> v;
  v.push_back(raft::slice::create(a));
  v.push_back(raft::slice::create(b));
  BOOST_CHECK_EQUAL(a.size() + b.size(), raft::slice::total_size(v));

  std::array<raft::slice, 2> arr = { raft::slice::create(a), raft::slice::create(b) };
  BOOST_CHECK_EQUAL(a.size() + b.size(), raft::slice::total_size(arr));
}

BOOST_AUTO_TEST_CASE(SliceSequenceShare)
{
  std::string a("This is a test buffer");
  std::string b("This is also a test buffer");
  std::string c("This is also also a test buffer");
  std::vector<raft::slice> v;
  v.push_back(raft::slice::create(a));
  v.push_back(raft::slice::create(b));
  v.push_back(raft::slice::create(c));

  BOOST_CHECK_EQUAL(a.size(), raft::slice::buffer_size(v[0]));
  BOOST_CHECK_EQUAL(b.size(), raft::slice::buffer_size(v[1]));
  BOOST_CHECK_EQUAL(c.size(), raft::slice::buffer_size(v[2]));

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, 3, 6, std::back_inserter(w));
    BOOST_CHECK_EQUAL(1U, w.size());
    BOOST_CHECK_EQUAL(6U, raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&a[3], raft::slice::buffer_cast<const char *>(w[0]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, a.size() + 3, 6, std::back_inserter(w));
    BOOST_CHECK_EQUAL(1U, w.size());
    BOOST_CHECK_EQUAL(6U, raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&b[3], raft::slice::buffer_cast<const char *>(w[0]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, a.size() + b.size() + 3, 6, std::back_inserter(w));
    BOOST_CHECK_EQUAL(1U, w.size());
    BOOST_CHECK_EQUAL(6U, raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&c[3], raft::slice::buffer_cast<const char *>(w[0]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, 3, 6+a.size(), std::back_inserter(w));
    BOOST_CHECK_EQUAL(2U, w.size());
    BOOST_CHECK_EQUAL(a.size() - 3, raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&a[3], raft::slice::buffer_cast<const char *>(w[0]));
    BOOST_CHECK_EQUAL(9U, raft::slice::buffer_size(w[1]));
    BOOST_CHECK_EQUAL(&b[0], raft::slice::buffer_cast<const char *>(w[1]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, 3+a.size(), 6+b.size(), std::back_inserter(w));
    BOOST_CHECK_EQUAL(2U, w.size());
    BOOST_CHECK_EQUAL(b.size() - 3, raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&b[3], raft::slice::buffer_cast<const char *>(w[0]));
    BOOST_CHECK_EQUAL(9U, raft::slice::buffer_size(w[1]));
    BOOST_CHECK_EQUAL(&c[0], raft::slice::buffer_cast<const char *>(w[1]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, 3, 6+a.size()+b.size(), std::back_inserter(w));
    BOOST_CHECK_EQUAL(3U, w.size());
    BOOST_CHECK_EQUAL(a.size() - 3, raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&a[3], raft::slice::buffer_cast<const char *>(w[0]));
    BOOST_CHECK_EQUAL(b.size(), raft::slice::buffer_size(w[1]));
    BOOST_CHECK_EQUAL(&b[0], raft::slice::buffer_cast<const char *>(w[1]));
    BOOST_CHECK_EQUAL(9U, raft::slice::buffer_size(w[2]));
    BOOST_CHECK_EQUAL(&c[0], raft::slice::buffer_cast<const char *>(w[2]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, 0, a.size(), std::back_inserter(w));
    BOOST_CHECK_EQUAL(1U, w.size());
    BOOST_CHECK_EQUAL(a.size(), raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&a[0], raft::slice::buffer_cast<const char *>(w[0]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, a.size(), b.size(), std::back_inserter(w));
    BOOST_CHECK_EQUAL(1U, w.size());
    BOOST_CHECK_EQUAL(b.size(), raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&b[0], raft::slice::buffer_cast<const char *>(w[0]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, a.size()+b.size(), c.size(), std::back_inserter(w));
    BOOST_CHECK_EQUAL(1U, w.size());
    BOOST_CHECK_EQUAL(c.size(), raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&c[0], raft::slice::buffer_cast<const char *>(w[0]));
  }

  {
    std::vector<raft::slice> w;
    raft::slice::share(v, 0, a.size()+b.size(), std::back_inserter(w));
    BOOST_CHECK_EQUAL(2U, w.size());
    BOOST_CHECK_EQUAL(a.size(), raft::slice::buffer_size(w[0]));
    BOOST_CHECK_EQUAL(&a[0], raft::slice::buffer_cast<const char *>(w[0]));
    BOOST_CHECK_EQUAL(b.size(), raft::slice::buffer_size(w[1]));
    BOOST_CHECK_EQUAL(&b[0], raft::slice::buffer_cast<const char *>(w[1]));
  }
}

