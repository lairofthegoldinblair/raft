#define BOOST_TEST_MODULE RaftTests
#include <array>
#include <chrono>
#include <iostream>
#include <thread>

#include "protocol.hh"

#include "native/messages.hh"
#include "native/serialization.hh"
#include "flatbuffers/raft_flatbuffer_messages.hh"
#include "flatbuffers/serialization.hh"
#include "util/builder_communicator.hh"

#include "test_utilities.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/mpl/list.hpp>

#include <deque>
#include "boost/variant.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(PeerSchedulerTest)
{
  raft::peer_scheduler scheduler;
  auto now = std::chrono::steady_clock::now();
  scheduler.init(now);
  // First multiplicative increase
  BOOST_CHECK(scheduler.can_send(now, 1));
  for(uint32_t j=1; j<=128; j*=2) {
    for(uint32_t i=0; i<=j; ++i) {
      BOOST_CHECK(!scheduler.can_send(now+std::chrono::milliseconds(i), 1));
    }
    BOOST_CHECK(scheduler.can_send(now+std::chrono::milliseconds(j+1), 1));
    // Timer resets
    now = now+std::chrono::milliseconds(j+1);
  }
  // A new max index will be sent using the same delay
  BOOST_CHECK(scheduler.can_send(now, 2));
  BOOST_CHECK(!scheduler.can_send(now+std::chrono::milliseconds(256), 2));
  BOOST_CHECK(scheduler.can_send(now+std::chrono::milliseconds(257), 2));
  now = now+std::chrono::milliseconds(257);
  // Send ack for previous index, no change in delay
  scheduler.ack(now, 1);
  BOOST_CHECK(!scheduler.can_send(now+std::chrono::milliseconds(512), 2));
  BOOST_CHECK(scheduler.can_send(now+std::chrono::milliseconds(513), 2));
  now = now+std::chrono::milliseconds(513);
  // Send an ack and we should get additive decrease of delay by 10 milliseconds
  scheduler.ack(now, 2);
  BOOST_CHECK(scheduler.can_send(now, 3));
  BOOST_CHECK(!scheduler.can_send(now+std::chrono::milliseconds(1014), 3));
  BOOST_CHECK(scheduler.can_send(now+std::chrono::milliseconds(1015), 3));
  now = now+std::chrono::milliseconds(1015);
  // Additive decrease of delay should stop at 1 milli
  for(uint64_t idx=4; idx<250; ++idx) {
    BOOST_CHECK(scheduler.can_send(now, idx));
    scheduler.ack(now, idx);
  }
  BOOST_CHECK(scheduler.can_send(now, 250));
  BOOST_CHECK(!scheduler.can_send(now+std::chrono::milliseconds(1), 250));
  BOOST_CHECK(scheduler.can_send(now+std::chrono::milliseconds(2), 250));  
}

// Tests of native types and traits
BOOST_AUTO_TEST_CASE(testSimpleConfigurationTraits)
{
  typedef raft::native::server_description_traits sdt;
  typedef raft::native::simple_configuration_description_traits scdt;
  raft::native::simple_configuration_description desc;
  BOOST_CHECK_EQUAL(0U, scdt::size(&desc));
  desc.servers.push_back({ 3, "127.0.0.1" });
  BOOST_CHECK_EQUAL(1U, scdt::size(&desc));
  BOOST_CHECK_EQUAL(3U, sdt::id(&scdt::get(&desc, 0)));
}

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
    
  void on_append_entry_response(std::chrono::time_point<std::chrono::steady_clock> clock_now,
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
  void send(endpoint ep, const std::string& address, _T && msg)
  {
    q.push_front(std::move(msg));
  }
  
  void vote_request(endpoint ep, const std::string & address,
		    uint64_t request_id,
		    uint64_t recipient_id,
		    uint64_t term_number,
		    uint64_t candidate_id,
		    uint64_t log_index_end,
		    uint64_t last_log_term)
  {
    typename _Messages::vote_request_type msg;
    msg.request_id=request_id;
    msg.recipient_id=recipient_id;
    msg.term_number=term_number;
    msg.candidate_id=candidate_id;
    msg.log_index_end=log_index_end;
    msg.last_log_term=last_log_term;
    send(ep, address, std::move(msg));	
  }

  template<typename EntryProvider>
  void append_entry_request(endpoint ep, const std::string& address,
		    uint64_t request_id,
		    uint64_t recipient_id,
		    uint64_t term_number,
		    uint64_t leader_id,
		    uint64_t log_index_begin,
		    uint64_t previous_log_term,
		    uint64_t leader_commit_index_end,
		    uint64_t num_entries,
		    EntryProvider entries)
  {
    typename _Messages::append_entry_request_type msg;
    msg.request_id=request_id;
    msg.set_recipient_id(recipient_id);
    msg.set_term_number(term_number);
    msg.set_leader_id(leader_id);
    msg.set_log_index_begin(log_index_begin);
    msg.set_previous_log_term(previous_log_term);
    msg.set_leader_commit_index_end(leader_commit_index_end);
    for(uint64_t i=0; i<num_entries; ++i) {
      msg.add_entry(entries(i));
    }
    q.push_front(std::move(msg));
  }
	
  void append_entry_response(endpoint ep, const std::string& address,
			     uint64_t recipient_id,
			     uint64_t term_number,
			     uint64_t request_term_number,
                             uint64_t request_id,
			     uint64_t index_begin,
			     uint64_t index_end,
			     bool success)
  {
    typename _Messages::append_entry_response_type msg;
    msg.recipient_id = recipient_id;
    msg.term_number = term_number;
    msg.request_term_number = request_term_number;
    msg.request_id = request_id;
    msg.index_begin = index_begin;
    msg.index_end = index_end;
    msg.success = success;
    q.push_front(std::move(msg));
  }

  void vote_response(endpoint ep, const std::string& address,
		     uint64_t peer_id,
		     uint64_t term_number,
		     uint64_t request_term_number,
                     uint64_t request_id,
		     bool granted)
  {
    typename _Messages::vote_response_type msg;
    msg.peer_id = peer_id;
    msg.term_number = term_number;
    msg.request_term_number = request_term_number;
    msg.request_id = request_id;
    msg.granted = granted;
    q.push_front(std::move(msg));
  }

  void append_checkpoint_chunk_request(endpoint ep, const std::string& address,
                                       uint64_t request_id,
                                       uint64_t recipient_id,
                                       uint64_t term_number,
                                       uint64_t leader_id,
                                       const raft::native::checkpoint_header & last_checkpoint_header,
                                       uint64_t checkpoint_begin,
                                       uint64_t checkpoint_end,
                                       bool checkpoint_done,
                                       raft::slice data)
  {
    typename _Messages::append_checkpoint_chunk_request_type msg;
    msg.request_id=request_id;
    msg.recipient_id=recipient_id;
    msg.term_number=term_number;
    msg.leader_id=leader_id;
    msg.last_checkpoint_header=last_checkpoint_header;
    msg.checkpoint_begin=checkpoint_begin;
    msg.checkpoint_end=checkpoint_end;
    msg.checkpoint_done=checkpoint_done;
    msg.data.assign(raft::slice::buffer_cast<const uint8_t *>(data),
		    raft::slice::buffer_cast<const uint8_t *>(data) + raft::slice::buffer_size(data));
    q.push_front(std::move(msg));
  }		       
  
  void append_checkpoint_chunk_response(endpoint ep, const std::string& address,
					uint64_t recipient_id,
					uint64_t term_number,
					uint64_t request_term_number,
                                        uint64_t request_id,
					uint64_t bytes_stored)
  {
    typename _Messages::append_checkpoint_chunk_response_type msg;    
    msg.recipient_id = recipient_id;
    msg.term_number = term_number;
    msg.request_term_number = request_term_number;
    msg.request_id = request_id;
    msg.bytes_stored = bytes_stored;
    q.push_front(std::move(msg));
  }

  typedef boost::variant<typename _Messages::vote_request_type, typename _Messages::vote_response_type,
			 typename _Messages::append_entry_request_type, typename _Messages::append_entry_response_type,
			 typename _Messages::append_checkpoint_chunk_request_type, typename _Messages::append_checkpoint_chunk_response_type> any_msg_type;
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

typedef raft::protocol<communicator_metafunction, raft::test::in_memory_checkpoint_metafunction, raft::native::messages> test_raft_type;

struct init_logging
{
  init_logging() {
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::trace);
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
  raft::native::server_description::address_type address;
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

  raft::native::configuration_description desc;
  desc.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}};
  raft::configuration_algorithm<test_peer, raft::native::messages> config(0);
  BOOST_CHECK_EQUAL(0U, config.num_known_peers());
  BOOST_CHECK_EQUAL(0U, config.my_cluster_id());
  BOOST_CHECK(!config.includes_self());
  config.set_configuration(1, desc, std::chrono::steady_clock::now());
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
  raft::configuration_manager<test_peer_metafunction, raft::native::messages> mgr(0);
  for(uint64_t i=0; i<20; ++i) {
    BOOST_CHECK(!mgr.has_configuration_at(i));
  }
  raft::native::messages::log_entry_type desc;
  desc.type = raft::native::messages::log_entry_type::CONFIGURATION;
  desc.term = 0;
  desc.configuration.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}};
  mgr.add_logged_description(5, desc, std::chrono::steady_clock::now());
  BOOST_CHECK_EQUAL(5U, mgr.configuration().configuration_id());
  BOOST_CHECK_EQUAL(3U, mgr.configuration().num_known_peers());
  for(uint64_t i=0; i<5; ++i) {
    BOOST_CHECK(!mgr.has_configuration_at(i));
  }
  for(uint64_t i=5; i<20; ++i) {
    BOOST_CHECK(mgr.has_configuration_at(i));
  }
  raft::native::messages::log_entry_type desc2;
  desc2.type = raft::native::messages::log_entry_type::CONFIGURATION;
  desc2.term = 0;
  desc2.configuration.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"}};
  mgr.add_logged_description(10, desc2, std::chrono::steady_clock::now());
  BOOST_CHECK_EQUAL(10U, mgr.configuration().configuration_id());
  BOOST_CHECK_EQUAL(4U, mgr.configuration().num_known_peers());
  for(uint64_t i=0; i<5; ++i) {
    BOOST_CHECK(!mgr.has_configuration_at(i));
  }
  for(uint64_t i=5; i<10; ++i) {
    BOOST_CHECK(mgr.has_configuration_at(i));
    BOOST_CHECK_EQUAL(5U, mgr.get_configuration_index_at(i));
  }
  for(uint64_t i=10; i<20; ++i) {
    BOOST_CHECK(mgr.has_configuration_at(i));
    BOOST_CHECK_EQUAL(10U, mgr.get_configuration_index_at(i));
  }
}

BOOST_AUTO_TEST_CASE(ConfigurationManagerSetCheckpointTests)
{
  raft::configuration_manager<test_peer_metafunction, raft::native::messages> cm(0);
  raft::configuration_manager<test_peer_metafunction, raft::native::messages>::checkpoint_type ckpt;
  ckpt.configuration.index = ckpt.log_entry_index_end =  0;
  ckpt.configuration.description.from.servers = {{0, "192.168.1.1"}, {1, "192.168.1.2"}, {2, "192.168.1.3"}, {3, "192.168.1.4"},  {4, "192.168.1.5"}};
  cm.set_checkpoint(ckpt, std::chrono::steady_clock::now());
  BOOST_CHECK_EQUAL(0U, cm.configuration().configuration_id());
  BOOST_CHECK_EQUAL(0U, cm.configuration().my_cluster_id());
  BOOST_CHECK_EQUAL(5U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  BOOST_CHECK(cm.has_configuration_at(0));
}

static uint64_t get_cluster_time(std::chrono::time_point<std::chrono::steady_clock> base,
                                 std::chrono::time_point<std::chrono::steady_clock> now)
{
  return std::chrono::nanoseconds(now - base).count();
}

// Test types corresponding to native and flatbuffers
class native_test_type
{
public:
  typedef raft::native::messages messages_type;
  typedef raft::native::builders builders_type;
  typedef raft::native::serialization serialization_type;
};

// Helper for comparing results
using raft::test::string_slice_compare;

class flatbuffers_test_type
{
public:
  typedef raft::fbs::messages messages_type;
  typedef raft::fbs::builders builders_type;
  typedef raft::fbs::serialization serialization_type;
};

typedef boost::mpl::list<native_test_type, flatbuffers_test_type> test_types;

BOOST_AUTO_TEST_CASE_TEMPLATE(BasicTemplatedStateMachineTests, _TestType, test_types)
{
  typedef typename _TestType::messages_type::vote_request_traits_type::arg_type vote_request_arg_type;
  typedef typename _TestType::messages_type::vote_request_traits_type vote_request_traits;
  typedef typename _TestType::builders_type::vote_request_builder_type vote_request_builder;
  typedef typename _TestType::messages_type::vote_response_traits_type vote_response_traits;
  typedef typename _TestType::builders_type::vote_response_builder_type vote_response_builder;
  typedef typename _TestType::messages_type::append_entry_request_traits_type append_entry_request_traits;
  typedef typename _TestType::messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
  typedef typename _TestType::builders_type::append_entry_request_builder_type append_entry_request_builder;
  typedef typename _TestType::messages_type::append_entry_response_traits_type append_entry_response_traits;
  typedef typename _TestType::messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
  typedef typename _TestType::builders_type::append_entry_response_builder_type append_entry_response_builder;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
  typedef typename _TestType::messages_type::log_entry_traits_type log_entry_traits;
  typedef raft::protocol<raft::test::generic_communicator_metafunction, raft::test::in_memory_checkpoint_metafunction, typename _TestType::messages_type> raft_type;
  typedef raft::test::client<typename _TestType::messages_type> client_type;

  auto time_base = std::chrono::steady_clock::now();
  // Track the last update to s.cluster_time()
  auto cluster_now = time_base;
  // Tracks our synthetic wall clock
  auto now = time_base;

  typename raft_type::configuration_manager_type cm(0);
  // Builder interface only supports creating a checkpoint header in the context of an append_checkpoint_chunk message
  append_checkpoint_chunk_request_builder accb;
  {
    auto chb = accb.last_checkpoint_header();
    {
      auto cdb = chb.index(0).log_entry_index_end(0).last_log_entry_term(0).last_log_entry_cluster_time(0).configuration();
      {
	auto fsb = cdb.from();
	fsb.server().id(0).address("192.168.1.1");
	fsb.server().id(1).address("192.168.1.2");
	fsb.server().id(2).address("192.168.1.3");
	fsb.server().id(3).address("192.168.1.4");
	fsb.server().id(4).address("192.168.1.5");
      }
      {
	auto fsb = cdb.to();
      }
    }
  }
  auto acc_msg = accb.finish();
  cm.set_checkpoint(append_checkpoint_chunk_request_traits::last_checkpoint_header(acc_msg), time_base);
  BOOST_CHECK_EQUAL(0U, cm.configuration().configuration_id());
  BOOST_CHECK_EQUAL(5U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  std::size_t cluster_size = cm.configuration().num_known_peers();

  typename raft_type::communicator_type comm;
  client_type c;  
  typename raft_type::log_type l;
  typename raft_type::checkpoint_data_store_type store;

  raft_type s(comm, l, store, cm, now);
  bool restored=false;
  s.start(now, [&restored](std::chrono::time_point<std::chrono::steady_clock> clock_now){ BOOST_TEST(!restored); restored = true; });
  BOOST_CHECK(restored);
  BOOST_CHECK_EQUAL(0U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(raft_type::FOLLOWER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  now += std::chrono::milliseconds(500);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(1U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK(s.log_header_sync_required());
  s.on_log_header_sync(now);
  BOOST_CHECK(!s.log_header_sync_required());
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(1).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(cluster_size - 1, comm.q.size());
  std::size_t expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(expected, vote_request_traits::recipient_id(boost::get<vote_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, vote_request_traits::candidate_id(boost::get<vote_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(boost::get<vote_request_arg_type>(comm.q.back())));
    expected += 1;
    comm.q.pop_back();
  }

  now += std::chrono::milliseconds(500);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK(s.log_header_sync_required());
  s.on_log_header_sync(now);
  BOOST_CHECK(!s.log_header_sync_required());
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(1).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(cluster_size - 1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(expected, vote_request_traits::recipient_id(boost::get<vote_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, vote_request_traits::candidate_id(boost::get<vote_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, vote_request_traits::term_number(boost::get<vote_request_arg_type>(comm.q.back())));
    expected += 1;
    comm.q.pop_back();
  }

  // Provide one vote
  auto vote_response_msg = vote_response_builder().peer_id(1).term_number(2).request_term_number(2).granted(true).finish();
  s.on_vote_response(std::move(vote_response_msg), now);
  // BOOST_CHECK(true == s.get_peer_from_id(1).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Any vote from prior term should be ignored
  vote_response_msg = vote_response_builder().peer_id(2).term_number(1).request_term_number(1).granted(true).finish();
  s.on_vote_response(std::move(vote_response_msg), now);
  // BOOST_CHECK(true == s.get_peer_from_id(1).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(2).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Valid vote! Become leader
  vote_response_msg = vote_response_builder().peer_id(2).term_number(2).request_term_number(2).granted(true).finish();
  s.on_vote_response(std::move(vote_response_msg), now);
  // We will write log entry when we becom leader
  cluster_now = now;
  // BOOST_CHECK(true == s.get_peer_from_id(1).vote_);
  // BOOST_CHECK(true == s.get_peer_from_id(2).vote_);
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(3).vote_));
  BOOST_CHECK(boost::logic::indeterminate(s.get_peer_from_id(4).vote_));
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_REQUIRE_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK(log_entry_traits::is_noop(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
    auto resp = append_entry_response_builder().recipient_id(expected).term_number(2).request_term_number(2).index_begin(0).index_end(1).success(true).finish();
    s.on_append_entry_response(std::move(resp), now);
    if (expected<3) {
      BOOST_CHECK_EQUAL(0U, s.commit_index());
    } else {
      BOOST_CHECK_EQUAL(1U, s.commit_index());
    }
    expected += 1;
    comm.q.pop_back();
  }

  // Nothing should change here
  now += std::chrono::milliseconds(1);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  
  // Wait a bit and should get a round of heartbeat messages
  now += std::chrono::milliseconds(500);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    // TODO: What about the next 3 values ????
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
    expected += 1;
    comm.q.pop_back();
  }

  // Old append_entry should elicit a response with updated term
  now += std::chrono::milliseconds(500);
  auto ae_msg = append_entry_request_builder().recipient_id(0).term_number(1).leader_id(0).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).finish();
  s.on_append_entry_request(std::move(ae_msg), now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(1U, comm.q.size());
  while(comm.q.size() > 0) {
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(comm.q.back())));
    comm.q.pop_back();
  }
  // Fire off a client_request, will write log entry and update cluster_time epoch
  now += std::chrono::milliseconds(500);
  cluster_now = now;
  std::string command_str("1");
  s.on_command(std::make_pair(raft::slice::create(command_str), raft::util::call_on_delete()), now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  now += std::chrono::milliseconds(500);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    now += std::chrono::milliseconds(100);
    const auto & tmp(boost::get<append_entry_request_arg_type>(comm.q.back()));
    BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
    BOOST_CHECK_EQUAL(2U, log_entry_traits::term(&append_entry_request_traits::get_entry(tmp, 0)));
    BOOST_CHECK_EQUAL(0, string_slice_compare("1", log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0))));
    auto resp = append_entry_response_builder().recipient_id(expected).term_number(2).request_term_number(2).index_begin(1).index_end(2).success(true).finish();
    s.on_append_entry_response(std::move(resp), now);
    BOOST_CHECK_EQUAL(0U, c.responses.size());
    if (expected<3) {
      BOOST_CHECK_EQUAL(1U, s.commit_index());
    } else {
      BOOST_CHECK_EQUAL(2U, s.commit_index());
    }
    expected += 1;
    comm.q.pop_back();
  }
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());

  // Fire off two client requests
  now += std::chrono::milliseconds(100);
  cluster_now = now;
  command_str.assign("2");
  s.on_command(std::make_pair(raft::slice::create(command_str), raft::util::call_on_delete()), now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  now += std::chrono::milliseconds(100);
  cluster_now = now;
  command_str.assign("3");
  s.on_command(std::make_pair(raft::slice::create(command_str), raft::util::call_on_delete()), now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log has sync'd to disk so we only need 2
  // other members of the cluster to ack
  now += std::chrono::milliseconds(100);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  now += std::chrono::milliseconds(100);
  s.on_log_sync(4, now);
  expected = 1;
  while(comm.q.size() > 0) {
    now += std::chrono::milliseconds(100);
    BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
    BOOST_CHECK_EQUAL(2U, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
    BOOST_CHECK_EQUAL(0, string_slice_compare("2", log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0))));
    BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 1)));
    BOOST_CHECK_EQUAL(2U, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 1)));
    BOOST_CHECK_EQUAL(0, string_slice_compare("3", log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 1))));
    auto resp = append_entry_response_builder().recipient_id(expected).term_number(2).request_term_number(2).index_begin(2).index_end(4).success(true).finish();
    if (expected<=2) {
      s.on_append_entry_response(std::move(resp), now);
    }
    BOOST_CHECK_EQUAL(0U, c.responses.size());
    if (expected<2) {
      BOOST_CHECK_EQUAL(2U, s.commit_index());
    } else {
      BOOST_CHECK_EQUAL(4U, s.commit_index());
    }
    expected += 1;
    comm.q.pop_back();
  }
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());

  // We skipped append_entry_response from peers 3,4 in the last go round so they need resending of entries
  now += std::chrono::milliseconds(100);
  cluster_now = now;
  command_str.assign("4");
  s.on_command(std::make_pair(raft::slice::create(command_str), raft::util::call_on_delete()), now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(0U, comm.q.size());
  // This will send append_entries request to everyone in the cluster
  // When a majority of responses have come back we are committed.  Here log doesn't sync to disk.
  now += std::chrono::milliseconds(100);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(2U, s.current_term());
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, cluster_now), s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  BOOST_CHECK_EQUAL(cluster_size-1, comm.q.size());
  expected = 1;
  while(comm.q.size() > 0) {
    now += std::chrono::milliseconds(100);
    BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(expected <= 2 ? 4U : 2U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(comm.q.back())));
    BOOST_CHECK_EQUAL(4U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(comm.q.back())));
    if (expected <= 2) {
      BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
      BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
      BOOST_CHECK_EQUAL(2U, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0)));
      BOOST_CHECK_EQUAL(0, string_slice_compare("4", log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), 0))));
    } else {
      BOOST_CHECK_EQUAL(3U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(comm.q.back())));
      for(std::size_t i=0; i<=2; ++i) {
	BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), i)));
	BOOST_CHECK_EQUAL(2U, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), i)));
	BOOST_CHECK_EQUAL(0, string_slice_compare((boost::format("%1%") % (i+2)).str().c_str(), log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(comm.q.back()), i))));
      }
    }
    auto resp = append_entry_response_builder().recipient_id(expected).term_number(2).request_term_number(2).index_begin(expected <= 2 ? 4 : 2).index_end(5).success(true).finish();
    s.on_append_entry_response(std::move(resp), now);
    BOOST_CHECK_EQUAL(0U, c.responses.size());
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
  test_raft_type::log_type l;
  test_raft_type::checkpoint_data_store_type store;

  l.append(test_raft_type::get_bootstrap_log_entry(2, "192.168.1.1"));
  uint64_t term = l.last_entry_term();
  BOOST_CHECK_EQUAL(0U, term);
  auto cluster_time = l.last_entry_cluster_time();
  BOOST_CHECK_EQUAL(0U, cluster_time);
  l.update_header(term, test_raft_type::INVALID_PEER_ID());
 
  auto time_base = std::chrono::steady_clock::now();
  // Tracks our synthetic wall clock
  auto now = time_base;

  test_raft_type::configuration_manager_type cm(2);
  test_raft_type s(comm, l, store, cm, now);
  bool restored = false;
  s.start(now, [&restored](std::chrono::time_point<std::chrono::steady_clock> clock_now){ BOOST_TEST(!restored); restored = true; });
  BOOST_CHECK(restored);
  BOOST_CHECK_EQUAL(term, s.current_term());
  BOOST_CHECK_EQUAL(cluster_time, s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::FOLLOWER, s.get_state());
  BOOST_CHECK(cm.has_configuration_at(0));
  BOOST_CHECK_EQUAL(1U, cm.configuration().num_known_peers());
  BOOST_CHECK(cm.configuration().includes_self());
  BOOST_CHECK_EQUAL(2U, cm.configuration().self().peer_id);
  BOOST_CHECK_EQUAL(0U, comm.q.size());

  // Run timer then we should become leader of a single node cluster
  now += std::chrono::milliseconds(500);
  s.on_timer(now);
  BOOST_CHECK_EQUAL(term+1U, s.current_term());
  // Cluster time not updated yet
  BOOST_CHECK_EQUAL(cluster_time, s.cluster_time());
  BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, s.get_state());
  BOOST_CHECK(s.log_header_sync_required());
  now += std::chrono::milliseconds(500);
  s.on_log_header_sync(now);
  BOOST_CHECK(!s.log_header_sync_required());
  BOOST_CHECK_EQUAL(test_raft_type::LEADER, s.get_state());
  // Cluster time updates now that we are leader with NOOP log entry using timestamp
  // of the log header sync.
  BOOST_CHECK_EQUAL(get_cluster_time(time_base, now), s.cluster_time());
}

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

template<typename _TestType>
class RaftTestBase : public raft::test::RaftTestFixtureBase<_TestType>
{
public:
  typedef typename _TestType::messages_type messages_type;
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
  typedef typename _TestType::messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_arg_type;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type append_checkpoint_chunk_response_traits;
  typedef typename _TestType::messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;
  typedef typename _TestType::builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
  typedef typename _TestType::messages_type::checkpoint_header_traits_type checkpoint_header_traits;
  typedef typename _TestType::messages_type::log_entry_traits_type log_entry_traits;
  typedef typename _TestType::messages_type::server_description_traits_type server_description_traits;
  typedef typename _TestType::messages_type::simple_configuration_description_traits_type simple_configuration_description_traits;
  typedef typename _TestType::messages_type::configuration_description_traits_type configuration_description_traits;
  typedef typename _TestType::messages_type::set_configuration_request_traits_type set_configuration_request_traits;
  typedef typename _TestType::builders_type::set_configuration_request_builder_type set_configuration_request_builder;
  typedef typename _TestType::builders_type::log_entry_builder_type log_entry_builder;
  typedef raft::protocol<raft::test::generic_communicator_metafunction, raft::test::in_memory_checkpoint_metafunction, messages_type> raft_type;
  typedef typename raft_type::checkpoint_block_type checkpoint_block_type;

  RaftTestBase(raft::test::TestFixtureInitialization init = raft::test::TestFixtureInitialization::CHECKPOINT)
    :
    raft::test::RaftTestFixtureBase<_TestType>(init)
  {
  }
  ~RaftTestBase() {}

  void check_heartbeat(uint64_t recipient_id, const typename raft_type::communicator_type::any_msg_type & resp)
  {
    BOOST_CHECK_EQUAL(recipient_id, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(resp)));
    BOOST_CHECK_EQUAL(this->protocol->current_term(), append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(resp)));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(resp)));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(resp)));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(resp)));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(resp)));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(resp)));
  }
  void check_heartbeat(uint64_t recipient_id)
  {
    check_heartbeat(recipient_id, this->comm.q.back());
  }

  void AppendEntriesLogSync()
  {
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Term not valid when index=0 (empty log)
      auto le = log_entry_builder().term(1).cluster_time(7823).data("1").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(1).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync();
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());

    this->protocol->on_log_sync(1);
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Pretend a leader from expired term sends a message, this should respond with current term
    {
      auto le = log_entry_builder().term(0).cluster_time(23432343).data("0").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(0).leader_id(2).log_index_begin(2).previous_log_term(1).leader_commit_index_end(1).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(!append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Supposing the leader has committed lets go to another message
    // which creates a gap.  This should be rejected by the peer who
    // will tell us where we should start sending from.
    {
      auto le = log_entry_builder().term(1).cluster_time(8234544).data("3").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(1).leader_id(1).log_index_begin(2).previous_log_term(1).leader_commit_index_end(1).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(!append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Send three messages with the first one a duplicate.   So the peer told us to start
    // sending from log index 1 but we're actually going all the way back to 0.  The peer
    // should detect the duplicate at 0 and only append [1,3).
    {
      // Term not valid when index=0 (empty log)
      append_entry_request_builder bld;
      bld.recipient_id(0).term_number(1).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0);
      for(std::size_t i=1; i<=3; ++i) {
	bld.entry(log_entry_builder().term(1).cluster_time(7823U + i).data((boost::format("%1%") % i).str().c_str()).finish());
      }
      auto msg = bld.finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7826U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_sync(3);
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7826U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Let's go to another message
    // which uses a newer term.  The idea in this example is that entries previously
    // sent to the peer didn't get committed but a new leader got elected and DID commit
    // at those indexes and is trying to append from them on the new term.  We must reject
    // so that the new leader backs up to find where its log agrees with that of the peer.
    {
      auto le = log_entry_builder().term(3).cluster_time(8000).data("4").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(3).leader_id(2).log_index_begin(3).previous_log_term(3).leader_commit_index_end(3).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync();
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7826U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(!append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Let's suppose that only log entry at index 1 on term 1 got committed.  We should be able to
    // overwrite log entries starting at that point.
    {
      append_entry_request_builder bld;
      bld.recipient_id(0).term_number(3).leader_id(2).log_index_begin(1).previous_log_term(1).leader_commit_index_end(3);
      for (std::size_t i=2; i<=4; ++i) {
	bld.entry(log_entry_builder().term(3).cluster_time(8000U+i).data((boost::format("%1%a") % i).str().c_str()).finish());
      }
      auto msg = bld.finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(8004U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_sync(4);
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(8004U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(4U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }
  void AppendEntriesNegativeResponse()
  {
    // Make me leader
    this->make_leader(1);
    // Client request to trigger append entries
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    std::string command_str("1");
    this->now = this->now + std::chrono::milliseconds(1);
    this->protocol->on_command(std::make_pair(raft::slice::create(command_str), raft::util::call_on_delete()), this->now);
    BOOST_TEST(this->initial_cluster_time + 1000000 == this->protocol->cluster_time());
    this->initial_cluster_time = this->protocol->cluster_time();
    
    // On first attempt have clients respond negatively.  On second have them succeed
    for(std::size_t attempt=0; attempt<=1; ++attempt) {
      std::cout << "AppendEntriesNegativeResponse attempt " << attempt << std::endl;
      // Wait so the server will try to send log records.
      this->protocol->on_timer(this->now);
      BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
      BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::LEADER, this->protocol->get_state());
      if (1 == attempt) {
        BOOST_CHECK_EQUAL(0, this->comm.q.size());
        this->protocol->on_timer(this->now + std::chrono::milliseconds(2));
        BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
        BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
        BOOST_CHECK_EQUAL(raft_type::LEADER, this->protocol->get_state());
      }
      BOOST_CHECK_EQUAL(this->num_known_peers()-1, this->comm.q.size());
      uint64_t expected = 1;
      while(this->comm.q.size() > 0) {
	BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
	BOOST_CHECK_EQUAL(1U, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
	BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
	BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 0U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
	BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 0U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
	BOOST_CHECK_EQUAL(1U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
	BOOST_CHECK_EQUAL(attempt == 0 ? 1U : 2U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
	BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(this->comm.q.back()), attempt)));
	BOOST_CHECK_EQUAL(1U, log_entry_traits::term(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(this->comm.q.back()), attempt)));
	BOOST_CHECK_EQUAL(0, string_slice_compare("1", log_entry_traits::data(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(this->comm.q.back()), attempt))));
	auto resp = append_entry_response_builder().recipient_id(expected).term_number(1).request_term_number(1).index_begin(attempt == 0 ? 1 : 0).index_end(attempt == 0 ? 1 : 2).success(attempt == 0 ? false : true).finish();
	this->protocol->on_append_entry_response(std::move(resp), this->now);
        if (attempt==0 || expected<3) {
          BOOST_CHECK_EQUAL(1U, this->protocol->commit_index());
        } else {
          BOOST_CHECK_EQUAL(2U, this->protocol->commit_index());
        }
	expected += 1;
	this->comm.q.pop_back();
      }
    }
  }
  void AppendEntriesSlowHeaderSync()
  {
    {
      auto le = log_entry_builder().term(1).cluster_time(1000).data("1").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(1).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      auto le = log_entry_builder().term(2).cluster_time(2000).data("2").finish();
      // Since a log header sync is outstanding we will ignore a new term
      auto msg = append_entry_request_builder().recipient_id(0).term_number(2).leader_id(2).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Since a log header sync is outstanding we will ignore a new term
      uint8_t data=0;
      append_checkpoint_chunk_request_builder bld;
      bld.recipient_id(0).term_number(2).leader_id(2).checkpoint_begin(0).checkpoint_end(1).checkpoint_done(false).data(raft::slice(&data, 1));
      {
	auto chb = bld.last_checkpoint_header();
	{
	  auto cdb = chb.index(0).log_entry_index_end(0).last_log_entry_term(2).last_log_entry_cluster_time(0).configuration();
	  {
	    auto fsb = cdb.from();
	  }
	  {
	    auto fsb = cdb.to();
	  }
	}
      }
      auto msg = bld.finish();
      this->protocol->on_append_checkpoint_chunk_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    // Wait enough time for a election timeout.  Current logic says that we won't start an election
    // if the log header sync is outstanding
    this->now += std::chrono::milliseconds(500);
    this->protocol->on_timer(this->now);
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // This one doesn't require a new term so it gets queued awaiting the log header sync
      auto le = log_entry_builder().term(2).cluster_time(3000).data("2").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(1).leader_id(1).log_index_begin(1).previous_log_term(1).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());

    // TODO: Validate that the 2 append entries have beeen processed and are
    // awaiting log sync.
  
    this->protocol->on_log_sync(2, this->now);
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(3000U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(2U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }

  void BasicOnVoteRequestTest()
  {
    // FOLLOWER -> FOLLOWER
    auto msg = vote_request_builder().recipient_id(0).term_number(1).candidate_id(1).log_index_end(0).last_log_term(0).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(1U, this->log_header_write_.current_term_);
    BOOST_CHECK_EQUAL(1U, this->log_header_write_.voted_for_);
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(vote_response_traits::granted(boost::get<vote_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }

  void OnVoteRequestSlowHeaderSyncTest()
  {
    // FOLLOWER -> FOLLOWER
    auto msg = vote_request_builder().recipient_id(0).term_number(1).candidate_id(1).log_index_end(0).last_log_term(0).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(1U, this->log_header_write_.current_term_);
    BOOST_CHECK_EQUAL(1U, this->log_header_write_.voted_for_);
    this->log_header_write_.reset();
    
    // We are still waiting for header to sync to disk so will ignore subsequent request.
    msg = vote_request_builder().recipient_id(0).term_number(2).candidate_id(2).log_index_end(0).last_log_term(0).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK(this->log_header_write_.empty());

    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(vote_response_traits::granted(boost::get<vote_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK(this->log_header_write_.empty());

    // Send again now that we are sync'd
    msg = vote_request_builder().recipient_id(0).term_number(2).candidate_id(2).log_index_end(0).last_log_term(0).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(2U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(2U, this->log_header_write_.current_term_);
    BOOST_CHECK_EQUAL(2U, this->log_header_write_.voted_for_);
    this->log_header_write_.reset();

    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(2U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(vote_response_traits::granted(boost::get<vote_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK(this->log_header_write_.empty());

    // Send a couple of entries
    for(std::size_t i=1; i<=3; ++i) {
      {
	auto le = log_entry_builder().term(2).cluster_time(1000U + i).data((boost::format("%1%") % i).str().c_str()).finish();
	auto msg = append_entry_request_builder().recipient_id(0).term_number(2).leader_id(1).log_index_begin(i-1).previous_log_term(i==1 ? 0 : 2).leader_commit_index_end(i-1).entry(le).finish();
	this->protocol->on_append_entry_request(std::move(msg), this->now);
      }
      this->protocol->on_log_sync(i, this->now);
      BOOST_CHECK_EQUAL(2U, this->protocol->current_term());
      BOOST_CHECK_EQUAL(1000U+i, this->protocol->cluster_time());
      BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
      BOOST_CHECK_EQUAL(1U, this->comm.q.size());
      BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(2U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(2U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(i-1, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(i, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      this->comm.q.pop_back();
    }

    // Now initiate a leadership change but without up to date log; this should advance term but
    // we should not get the vote.
    msg = vote_request_builder().recipient_id(0).term_number(3).candidate_id(1).log_index_end(0).last_log_term(0).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    // Updated current_term requires header sync
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(1003U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(3U, this->log_header_write_.current_term_);
    BOOST_CHECK_EQUAL(std::numeric_limits<uint64_t>::max(), this->log_header_write_.voted_for_);
    this->log_header_write_.reset();
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(1003U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(!vote_response_traits::granted(boost::get<vote_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Now initiate a leadership change but with up to date log we'll get the vote.
    msg = vote_request_builder().recipient_id(0).term_number(3).candidate_id(1).log_index_end(3).last_log_term(2).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    // Updated voted_for (but not update current_term) requires header sync
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(1003U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(3U, this->log_header_write_.current_term_);
    BOOST_CHECK_EQUAL(1U, this->log_header_write_.voted_for_);
    this->log_header_write_.reset();

    // Let's suppose that the leader here got a quorum from other peers
    // it could start appending entries.  We have the correct term so we should
    // queue these up and continue to wait for the existing header sync to complete.
    // TODO: We could also get an append_entry for the current term without ever seeing
    // a vote_request from the leader.
    {
      auto le = log_entry_builder().term(3).cluster_time(2000).data("4").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(3).leader_id(1).log_index_begin(3).previous_log_term(2).leader_commit_index_end(4).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(1003U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK(this->log_header_write_.empty());
  
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(2000U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(vote_response_traits::granted(boost::get<vote_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK(this->log_header_write_.empty());

    this->protocol->on_log_sync(4, this->now);
    BOOST_CHECK_EQUAL(3U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(2000U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(4, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK(this->log_header_write_.empty());
  }

  void AppendEntriesTruncateUncommitted()
  {
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    // Append 3 committed entries at each of term 1 and 2, 3 uncommitted entries at term 3
    uint64_t idx=0;
    uint64_t commit_idx=0;
    for(uint64_t term=1; term<4; ++term) {
      for(std::size_t i=0; i<3; ++i, ++idx) {
        // This term sequence goes 0,1,1,1,2,2,2,3,3,3,...
        auto current_term = (idx+2)/3;
        // auto current_term = this->protocol->current_term();
        if (term < 3) {
          ++commit_idx;
        }
        {
          // Term not valid when index=0 (empty log)
          auto le = log_entry_builder().term(term).cluster_time(7823).data("1").finish();
          auto msg = append_entry_request_builder().recipient_id(0).term_number(term).leader_id(1).log_index_begin(idx).previous_log_term(current_term).leader_commit_index_end(commit_idx).entry(le).finish();
          this->protocol->on_append_entry_request(std::move(msg), this->now);
        }
        if (term != current_term) {
          BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
          BOOST_CHECK(this->protocol->log_header_sync_required());
          this->protocol->on_log_header_sync(this->now);
        }
        BOOST_CHECK(!this->protocol->log_header_sync_required());
        BOOST_CHECK_EQUAL(term, this->protocol->current_term());
        BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
        BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
        BOOST_CHECK_EQUAL(0U, this->comm.q.size());
        this->initial_cluster_time = 7823U;

        this->protocol->on_log_sync(idx+1, this->now);
        BOOST_CHECK_EQUAL(term, this->protocol->current_term());
        BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
        BOOST_CHECK_EQUAL(commit_idx, this->protocol->commit_index());
        BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
        BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
        BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(term, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(term, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(idx, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
        BOOST_CHECK_EQUAL(idx+1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
        BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
        this->comm.q.pop_back();
      }
    }
    // New leader sends the existing 3 committed entries at each of term 1 and 2 plus 3 new committed entries at term 4 that
    // truncating of log suffix from [6,9) prior to replacement
    idx=0;
    append_entry_request_builder bld;
    bld.recipient_id(0).term_number(10).leader_id(2).log_index_begin(0).previous_log_term(0).leader_commit_index_end(9U);
    std::array<uint64_t,3> terms = {1,2,4};
    std::array<uint64_t,9> previous_log_term = { 0,1,1,1,2,2,2,4,4 };
    for(auto term : terms) {
      for(std::size_t i=0; i<3; ++i, ++idx) {
        bld.entry(log_entry_builder().term(term).cluster_time(7823).data("1").finish());
      }
    }
    this->protocol->on_append_entry_request(bld.finish(), this->now);
    uint64_t term=10;
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync(this->now);
    this->protocol->on_log_sync(9U, this->now);
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(9U, this->protocol->commit_index());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(6U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(9U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }
  
  void AppendEntriesLogSyncEmptyLog()
  {
    // Send append entries to a peer that is initialized empty.   This will only work if there is
    // a configuration sent.
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Send append entries to a peer that is initialized empty but skip the configuration.   This can't generate a
      // response since without a config the peer doesn't know the leader's address.   It shouldn't crash the peer though.
      append_entry_request_builder bld;
      bld.recipient_id(0).term_number(1).leader_id(1).log_index_begin(1).previous_log_term(1).leader_commit_index_end(2);
      auto le = log_entry_builder().term(1).cluster_time(7823).data("1").finish();
      auto msg = bld.entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      append_entry_request_builder bld;
      bld.recipient_id(0).term_number(1).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(2);
      log_entry_builder leb;
      {
        auto cb = leb.term(1).cluster_time(7823).configuration();
        this->add_five_servers(cb.from());
        cb.to();
      }
      bld.entry(leb.finish());
      auto le = log_entry_builder().term(1).cluster_time(7823).data("1").finish();
      auto msg = bld.entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg));
    }
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync();
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());

    this->protocol->on_log_sync(2);
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(7823U, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }
  
  // Test the transition from follower to candiate to follower
  void FollowerToCandidateToFollower()
  {
    uint64_t term = 1;
    uint64_t cluster_time = 7823;
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Term not valid when index=0 (empty log)
      auto le = log_entry_builder().term(term).cluster_time(cluster_time).data("1").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(term).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());

    this->protocol->on_log_sync(1, this->now);
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Run timer then we should become CANDIDATE
    this->now += std::chrono::milliseconds(500);
    this->protocol->on_timer(this->now);
    BOOST_CHECK_EQUAL(term+1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, this->protocol->get_state());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->now += std::chrono::milliseconds(500);
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(test_raft_type::CANDIDATE, this->protocol->get_state());
    
    BOOST_CHECK_EQUAL(4U, this->comm.q.size());
    uint32_t expected = 1;
    while(this->comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(expected, vote_request_traits::recipient_id(boost::get<vote_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, vote_request_traits::candidate_id(boost::get<vote_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term + 1U, vote_request_traits::term_number(boost::get<vote_request_arg_type>(this->comm.q.back())));
      expected += 1;
      this->comm.q.pop_back();
    }

    // Now transition back to FOLLOWER with append entry and a different leader
    term += 1;
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Term not valid when index=0 (empty log)
      auto le = log_entry_builder().term(term).cluster_time(cluster_time).data("1").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(term).leader_id(2).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    // No header sync necessary because term didn't change
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_sync(1, this->now);
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }
  
  // Test the transition from follower to follower
  void FollowerToFollower()
  {
    uint64_t term = 1;
    uint64_t cluster_time = 7823;
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Term not valid when index=0 (empty log)
      auto le = log_entry_builder().term(term).cluster_time(cluster_time).data("1").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(term).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());

    this->protocol->on_log_sync(1, this->now);
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // Now append entry with a different leader and term
    term += 1;
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Term not valid when index=0 (empty log)
      auto le = log_entry_builder().term(term).cluster_time(cluster_time).data("1").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(term).leader_id(2).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_sync(1, this->now);
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }
  
  void AppendCheckpointChunk(bool async)
  {
    this->store.asynchronous(async);
    {
      uint8_t data=0;
      append_checkpoint_chunk_request_builder bld;
      bld.recipient_id(0).term_number(1).leader_id(1).checkpoint_begin(0).checkpoint_end(1).checkpoint_done(true).data(raft::slice(&data, 1));
      {
	auto chb = bld.last_checkpoint_header();
	{
	  auto cdb = chb.index(0).log_entry_index_end(2).last_log_entry_term(1).last_log_entry_cluster_time(0).configuration();
	  {
            this->add_five_servers(cdb.from());
	  }
	  {
	    auto fsb = cdb.to();
	  }
	}
      }
      auto msg = bld.finish();
      this->protocol->on_append_checkpoint_chunk_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());

    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    if (this->store.asynchronous()) {
      BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      // This is to write the one byte checkpoint chunk which is final
      // so must be synced before responding
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      this->store.completion_queue().pop_front();
      // The sync request is queued up
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      this->store.completion_queue().pop_front();
      // Now loading the one byte checkpoint in one read
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
      BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
    } else {
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    }

    BOOST_TEST(1 == this->checkpoint_load_state.size());
    BOOST_TEST_REQUIRE(0 < this->checkpoint_load_state.size());
    BOOST_TEST(0U == this->checkpoint_load_state[0]);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::recipient_id(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::request_term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::bytes_stored(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    BOOST_CHECK(this->l.empty());
    BOOST_CHECK_EQUAL(2U, this->l.index_begin());
    BOOST_CHECK_EQUAL(2U, this->l.index_end());

    // Make sure we can append the next entry
    {
      auto le = log_entry_builder().term(1U).cluster_time(this->initial_cluster_time).data("4").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(1).leader_id(1).log_index_begin(2).previous_log_term(1).leader_commit_index_end(2).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_sync(3, this->now);
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(3, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }

  void AppendCheckpointChunkSlowHeaderSync(bool async)
  {
    this->store.asynchronous(async);
    {
      uint8_t data=0;
      append_checkpoint_chunk_request_builder bld;
      bld.recipient_id(0).term_number(1).leader_id(1).checkpoint_begin(0).checkpoint_end(1).checkpoint_done(false).data(raft::slice(&data, 1));
      {
	auto chb = bld.last_checkpoint_header();
	{
	  auto cdb = chb.index(0).log_entry_index_end(2).last_log_entry_term(1).last_log_entry_cluster_time(0).configuration();
	  {
            this->add_five_servers(cdb.from());
	  }
	  {
	    auto fsb = cdb.to();
	  }
	}
      }
      auto msg = bld.finish();
      this->protocol->on_append_checkpoint_chunk_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Since a log header sync is outstanding we will ignore a new term
      auto le = log_entry_builder().term(2).cluster_time(2000).data("2").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(2).leader_id(2).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    {
      // Since a log header sync is outstanding we will ignore a new term
      uint8_t data=1;
      append_checkpoint_chunk_request_builder bld;
      bld.recipient_id(0).term_number(2).leader_id(2).checkpoint_begin(0).checkpoint_end(1).checkpoint_done(false).data(raft::slice(&data, 1));
      {
	auto chb = bld.last_checkpoint_header();
	{
	  auto cdb = chb.index(0).log_entry_index_end(10).last_log_entry_term(1).last_log_entry_cluster_time(0).configuration();
	  {
	    auto fsb = cdb.from();
	  }
	  {
	    auto fsb = cdb.to();
	  }
	}
      }
      auto msg = bld.finish();
      this->protocol->on_append_checkpoint_chunk_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());

    {
      // This one doesn't require a new term so it gets queued awaiting the log header sync
      // however, we want to validate that a new (double) sync request isn't made
      BOOST_CHECK(!this->log_header_write_.empty());
      this->log_header_write_.reset();
      BOOST_CHECK(this->log_header_write_.empty());
      uint8_t data=2;
      append_checkpoint_chunk_request_builder bld;
      bld.recipient_id(0).term_number(1).leader_id(1).checkpoint_begin(1).checkpoint_end(2).checkpoint_done(true).data(raft::slice(&data, 1));
      {
	auto chb = bld.last_checkpoint_header();
	{
	  auto cdb = chb.index(0).log_entry_index_end(2).last_log_entry_term(1).last_log_entry_cluster_time(0).configuration();
	  {
	    auto fsb = cdb.from();
	  }
	  {
	    auto fsb = cdb.to();
	  }
	}
      }
      auto msg = bld.finish();
      this->protocol->on_append_checkpoint_chunk_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->log_header_write_.empty());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_TEST(0 == this->checkpoint_load_state.size());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_TEST(0 == this->checkpoint_load_state.size());    
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    if (this->store.asynchronous()) {
      BOOST_TEST(0 == this->checkpoint_load_state.size());    
      BOOST_CHECK_EQUAL(0U, this->comm.q.size());
      // Writing of checkpoint chunk #1 (one byte), this will
      // generate a response since it is not final
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
      // Writing of checkpoint chunk #2 (one byte), this will
      // not generate a response until synced
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
      // Will be waiting on checkpoint file to be synced now
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
    } else {
      // If synchronous then the both chunks will have been written, synced
      // and committed (including loading the chckpoint).
      BOOST_TEST(2 == this->checkpoint_load_state.size());    
      BOOST_CHECK_EQUAL(2U, this->comm.q.size());
    }
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::recipient_id(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::request_term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::bytes_stored(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    if (this->store.asynchronous()) {
      // One operation to sync then loading checkpoint of size 2 is done in 1 async read
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      this->store.completion_queue().pop_front();
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      this->store.completion_queue().pop_front();
      BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
    }
    BOOST_TEST(2 == this->checkpoint_load_state.size());
    BOOST_TEST_REQUIRE(0 < this->checkpoint_load_state.size());
    BOOST_TEST(0U == this->checkpoint_load_state[0]);
    BOOST_TEST(2U == this->checkpoint_load_state[1]);
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::recipient_id(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_response_traits::request_term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_response_traits::bytes_stored(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();

    // TODO: What happens if we get an append entry for the new term while still waiting for the log header sync?  No reason it
    // couldn't be queued right?
  }

  void AppendCheckpointChunkTermAdvanceWaitingForSync()
  {
    // Verify assumed initial state
    BOOST_CHECK(this->l.empty());
    BOOST_CHECK_EQUAL(0U, this->l.index_begin());
    BOOST_CHECK_EQUAL(0U, this->l.index_end());
    this->store.asynchronous(true);
    uint64_t term = 1;
    {
      uint8_t data=0;
      append_checkpoint_chunk_request_builder bld;
      bld.recipient_id(0).term_number(term).leader_id(1).checkpoint_begin(0).checkpoint_end(1).checkpoint_done(true).data(raft::slice(&data, 1));
      {
	auto chb = bld.last_checkpoint_header();
	{
	  auto cdb = chb.index(0).log_entry_index_end(2).last_log_entry_term(term).last_log_entry_cluster_time(0).configuration();
	  {
            this->add_five_servers(cdb.from());
	  }
	  {
	    auto fsb = cdb.to();
	  }
	}
      }
      auto msg = bld.finish();
      this->protocol->on_append_checkpoint_chunk_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());

    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
    // Write the checkpoint chunk
    BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
    this->store.completion_queue().front()();
    this->store.completion_queue().pop_front();
    // It's final so we need to sync
    BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());

    // Bump the term with a vote request
    term += 1;
    auto expected_vote = term < this->protocol->current_term() || !this->protocol->candidate_log_more_complete(0, 0) ?
      raft_type::INVALID_PEER_ID() : 1U;
    auto msg = vote_request_builder().recipient_id(0).term_number(term).candidate_id(1).log_index_end(0).last_log_term(0).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(term, this->log_header_write_.current_term_);
    BOOST_CHECK_EQUAL(expected_vote, this->log_header_write_.voted_for_);

    // Checkpoint should be abandoned.
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::recipient_id(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_checkpoint_chunk_response_traits::term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term-1, append_checkpoint_chunk_response_traits::request_term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::bytes_stored(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());

    // Finish up with the vote response
    this->log_header_write_.reset();
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    //BOOST_CHECK(vote_response_traits::granted(boost::get<vote_response_arg_type>(comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK(this->log_header_write_.empty());
    
    BOOST_TEST(0 == this->checkpoint_load_state.size());
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());

    BOOST_CHECK(this->l.empty());
    BOOST_CHECK_EQUAL(0U, this->l.index_begin());
    BOOST_CHECK_EQUAL(0U, this->l.index_end());

    // Make sure we can append an next entry at the new term
    {
      auto le = log_entry_builder().term(term).cluster_time(this->initial_cluster_time).data("4").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(term).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_sync(1, this->now);
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }

  void AppendCheckpointChunkTermAdvanceWaitingForWrite()
  {
    // Verify assumed initial state
    BOOST_CHECK(this->l.empty());
    BOOST_CHECK_EQUAL(0U, this->l.index_begin());
    BOOST_CHECK_EQUAL(0U, this->l.index_end());
    this->store.asynchronous(true);
    uint64_t term = 1;
    {
      uint8_t data=0;
      append_checkpoint_chunk_request_builder bld;
      bld.recipient_id(0).term_number(term).leader_id(1).checkpoint_begin(0).checkpoint_end(1).checkpoint_done(true).data(raft::slice(&data, 1));
      {
	auto chb = bld.last_checkpoint_header();
	{
	  auto cdb = chb.index(0).log_entry_index_end(2).last_log_entry_term(term).last_log_entry_cluster_time(0).configuration();
	  {
            this->add_five_servers(cdb.from());
	  }
	  {
	    auto fsb = cdb.to();
	  }
	}
      }
      auto msg = bld.finish();
      this->protocol->on_append_checkpoint_chunk_request(std::move(msg), this->now);
    }
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());

    // Log header sync initiates write of checkpoint chunk
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
    BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());

    // While the write is pending bump the term with a vote request
    term += 1;
    auto expected_vote = term < this->protocol->current_term() || !this->protocol->candidate_log_more_complete(0, 0) ?
      raft_type::INVALID_PEER_ID() : 1U;
    auto msg = vote_request_builder().recipient_id(0).term_number(term).candidate_id(1).log_index_end(0).last_log_term(0).finish();
    this->protocol->on_vote_request(std::move(msg), this->now);
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(term, this->log_header_write_.current_term_);
    BOOST_CHECK_EQUAL(expected_vote, this->log_header_write_.voted_for_);

    // Checkpoint abandoned but the request isn't sent at this point (perhaps we should?)
    BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());

    // Finish up with the vote response
    this->log_header_write_.reset();
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    //BOOST_CHECK(vote_response_traits::granted(boost::get<vote_response_arg_type>(comm.q.back())));
    this->comm.q.pop_back();
    BOOST_CHECK(this->log_header_write_.empty());
    
    // Complete the checkpoint write
    this->store.completion_queue().front()();
    this->store.completion_queue().pop_front();
    BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
    // Now the checkpoint response shows up
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::recipient_id(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_checkpoint_chunk_response_traits::term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term-1, append_checkpoint_chunk_response_traits::request_term_number(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_response_traits::bytes_stored(boost::get<append_checkpoint_chunk_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();


    BOOST_CHECK(this->l.empty());
    BOOST_CHECK_EQUAL(0U, this->l.index_begin());
    BOOST_CHECK_EQUAL(0U, this->l.index_end());

    // Make sure we can append an next entry at the new term
    {
      auto le = log_entry_builder().term(term).cluster_time(this->initial_cluster_time).data("4").finish();
      auto msg = append_entry_request_builder().recipient_id(0).term_number(term).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(le).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
    }
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_sync(1, this->now);
    BOOST_CHECK_EQUAL(term, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0, append_entry_response_traits::index_begin(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1, append_entry_response_traits::index_end(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }

  void ClientCheckpointTest(bool async)
  {
    this->store.asynchronous(async);
    uint64_t term = 1;
    this->make_leader(term);

    const char * cmd = "1";
    uint64_t client_index=this->l.index_end();
    this->send_client_request_and_commit(term, cmd, client_index++);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
  
    auto ckpt = this->protocol->begin_checkpoint(client_index);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(client_index, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(this->initial_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(client_index, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());
    BOOST_REQUIRE(nullptr != this->protocol->last_checkpoint());
    std::size_t offset=0;
    checkpoint_block_type block;
    BOOST_CHECK(block.is_null());
    while(!this->protocol->last_checkpoint()->is_final(block)) {
      bool block_read = false;
      this->protocol->last_checkpoint()->next_block(this->now, std::move(block), [&block, &block_read](std::chrono::time_point<std::chrono::steady_clock> clock_now,
                                                                                                       checkpoint_block_type && blk) {
                                                      block_read = true;
                                                      block = std::move(blk);
                                                    });
      if (this->store.asynchronous()) {
        BOOST_CHECK(!block_read);
        BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
        this->store.completion_queue().front()();
        BOOST_CHECK(block_read);
        this->store.completion_queue().pop_front();
      } else {
        BOOST_TEST(block_read);
      }    
      BOOST_CHECK(!block.is_null());
      if (!this->protocol->last_checkpoint()->is_final(block)) {
        BOOST_TEST(offset + block.size() < 5U);
        BOOST_CHECK_EQUAL(this->protocol->last_checkpoint()->block_size(), block.size());
        BOOST_CHECK_EQUAL(0, ::memcmp(block.data(), &data[offset], block.size()));
        offset += block.size();
      } else {
        BOOST_TEST(offset + block.size() == 5U);
        BOOST_TEST(this->protocol->last_checkpoint()->block_size() >= block.size());
        BOOST_CHECK_EQUAL(0, ::memcmp(block.data(), &data[offset], block.size()));
      }
    }
    bool block_read = false;
    this->protocol->last_checkpoint()->next_block(this->now, std::move(block), [&block, &block_read](std::chrono::time_point<std::chrono::steady_clock> clock_now,
                                                                                                     checkpoint_block_type && blk) {
                                                    block_read = true;
                                                    block = std::move(blk);
                                                  });
    // Call next_block on a final block always returns synchronously
    BOOST_CHECK(block.is_null());

    BOOST_TEST(0U == this->checkpoint_load_state.size());
    bool loaded = false;
    this->protocol->load_checkpoint(this->now, [&loaded](std::chrono::time_point<std::chrono::steady_clock> clock_now){ BOOST_TEST(!loaded); loaded = true; });
    if (this->store.asynchronous()) {
      // Takes 3 async reads to load 5 bytes with block size of 2
      BOOST_CHECK(!loaded);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(!loaded);
      this->store.completion_queue().pop_front();
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(!loaded);
      this->store.completion_queue().pop_front();
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(loaded);
      this->store.completion_queue().pop_front();
      BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    } else {
      BOOST_TEST(loaded);
    }    
    BOOST_TEST_REQUIRE(5U == this->checkpoint_load_state.size());
    BOOST_TEST(0 == ::memcmp(&this->checkpoint_load_state[0], &data[0], 5));

    // Lastly the configuration should be pointing to the checkpoint header now
    // and the log entry containing the configuration should have been truncated.
    BOOST_CHECK_EQUAL(0U, simple_configuration_description_traits::size(&configuration_description_traits::to(this->cm->configuration().description())));
    BOOST_REQUIRE_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(this->cm->configuration().description())));
    for(std::size_t i=0; i<5; ++i) {
      BOOST_CHECK_EQUAL(i, server_description_traits::id(&simple_configuration_description_traits::get(&configuration_description_traits::from(this->cm->configuration().description()), i)));
      BOOST_CHECK_EQUAL(0, server_description_traits::address(&simple_configuration_description_traits::get(&configuration_description_traits::from(this->cm->configuration().description()), i)).compare((boost::format("192.168.1.%1%") % (i+1)).str()));
    }
  }

  void ClientPartialCheckpointTest(bool async)
  {
    this->store.asynchronous(async);
    uint64_t term = 1;
    this->make_leader(term);

    uint64_t client_index=this->l.index_end();
    this->send_client_request_and_commit(term, "1", client_index++);  
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    uint64_t expected_cluster_time = this->initial_cluster_time;
    this->send_client_request_and_commit(term, "2", client_index++);  
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());

    auto ckpt = this->protocol->begin_checkpoint(2U);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(2U, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(expected_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());

    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(2U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(expected_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());
  }

  void ClientCheckpointOldTermTest(bool async)
  {
    this->store.asynchronous(async);
    uint64_t term = 1;
    this->make_leader(term);

    uint64_t client_index=this->l.index_end();
    this->send_client_request_and_commit(term, "1", client_index++);  
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    uint64_t expected_cluster_time = this->initial_cluster_time;
    // Advance term; we should still be able to checkpoint at the old term
    term = 2;
    this->become_follower_with_vote_request(term);
    term = 3;
    this->make_leader(term);
    client_index=this->l.index_end();
    this->send_client_request_and_commit(term, "2", client_index++);  
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
  
    auto ckpt = this->protocol->begin_checkpoint(2U);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(2U, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(expected_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());

    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(2U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(expected_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());
  }

  void ClientCheckpointNegativeTest()
  {
    uint64_t term = 1;
    this->make_leader(term);

    uint64_t client_index=this->l.index_end();
    this->send_client_request_and_commit(term, "1", client_index++);  
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    auto ckpt = this->protocol->begin_checkpoint(this->l.index_end()+1);
    BOOST_CHECK(nullptr == ckpt.get());
  }

  // Test that append_entries will send a checkpoint that needs log entries the leader has discarded
  // post checkpoint
  void AppendEntriesCheckpoint(bool async)
  {
    this->store.asynchronous(async);
    uint64_t term = 1;
    this->make_leader(term);

    const char * cmd = "1";
    uint64_t client_index=this->l.index_end();
    // Send success response from all peers except 1.  This will commit entry
    // so that it can be checkpointed.
    boost::dynamic_bitset<> responses;
    responses.resize(this->num_known_peers(), true);
    responses.flip(1);
    this->send_client_request(term, cmd, client_index++, responses);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
  
    BOOST_CHECK_EQUAL(2U, this->protocol->applied_index());
    uint64_t expected_cluster_time = this->initial_cluster_time;
    auto ckpt = this->protocol->begin_checkpoint(2U);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(2U, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(expected_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(2U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(expected_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());

    // Fire timer.  Peer 1 still doesn't have first log entry but since that entry is
    // discarded, a checkpoint will need to be sent to 1.
    this->protocol->on_timer(this->now);
    if (this->store.asynchronous()) {
      // Must read the checkpoint from the store in order to send it
      BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
    } else {
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    }
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
    {
      const auto & cfg(checkpoint_header_traits::configuration(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
      BOOST_CHECK_EQUAL(0U, simple_configuration_description_traits::size(&configuration_description_traits::to(&cfg)));
      BOOST_REQUIRE_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(&cfg)));
      for(std::size_t i=0; i<5; ++i) {
	BOOST_CHECK_EQUAL(i, server_description_traits::id(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)));
	BOOST_CHECK_EQUAL(0, server_description_traits::address(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)).compare((boost::format("192.168.1.%1%") % (i+1)).str()));
      }
    }
    
    this->comm.q.pop_back();

    // Ack with bytes_stored=2 twice to validate the checkpoint protocol will resend data if requested
    for(int i=0; i<2; ++i) {
      auto resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(1U).request_term_number(1U).bytes_stored(2U).finish();
      this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);
      if (this->store.asynchronous()) {
        BOOST_CHECK_EQUAL(0U, this->comm.q.size());
        BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
        this->store.completion_queue().front()();
        BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
        this->store.completion_queue().pop_front();
        BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
        BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      } else {
        BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      }
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(4U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
      this->comm.q.pop_back();
    }

    auto resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(1U).request_term_number(1U).bytes_stored(4U).finish();
    this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);  
    if (this->store.asynchronous()) {
      BOOST_CHECK_EQUAL(0U, this->comm.q.size());
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
      BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    } else {
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    }
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(4U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(5U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
    this->comm.q.pop_back();

    resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(1U).request_term_number(1U).bytes_stored(5U).finish();
    this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);  
    BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());

    // Now test that we can append to all peers
    this->send_client_request_and_commit(term, cmd, client_index++);
  }

  void AppendEntriesCheckpointAllInOneChunk(bool async)
  {
    this->store.asynchronous(async);
    // Set so that the entire checkpoint fits in one chunk
    this->store.block_size(1024);
    uint64_t term = 1;
    this->make_leader(term);

    const char * cmd = "1";
    uint64_t client_index=this->l.index_end();
    // Send success response from all peers except 1.  This will commit entry
    // so that it can be checkpointed.
    boost::dynamic_bitset<> responses;
    responses.resize(this->num_known_peers(), true);
    responses.flip(1);
    this->send_client_request(term, cmd, client_index++, responses);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
  
    BOOST_CHECK_EQUAL(2U, this->protocol->applied_index());
    uint64_t expected_cluster_time = this->initial_cluster_time;
    auto ckpt = this->protocol->begin_checkpoint(2U);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(2U, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(expected_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(2U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(expected_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());

    // Fire timer.  Peer 1 still doesn't have first log entry but since that entry is
    // discarded, a checkpoint will need to be sent to 1.
    this->protocol->on_timer(this->now);
    if (this->store.asynchronous()) {
      // Must read the checkpoint from the store in order to send it
      BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
    } else {
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    }
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(5U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
    {
      const auto & cfg(checkpoint_header_traits::configuration(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
      BOOST_CHECK_EQUAL(0U, simple_configuration_description_traits::size(&configuration_description_traits::to(&cfg)));
      BOOST_REQUIRE_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(&cfg)));
      for(std::size_t i=0; i<5; ++i) {
	BOOST_CHECK_EQUAL(i, server_description_traits::id(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)));
	BOOST_CHECK_EQUAL(0, server_description_traits::address(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)).compare((boost::format("192.168.1.%1%") % (i+1)).str()));
      }
    }
    
    this->comm.q.pop_back();

    auto resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(1U).request_term_number(1U).bytes_stored(5U).finish();
    this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);  
    BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());

    // Now test that we can append to all peers
    this->send_client_request_and_commit(term, cmd, client_index++);
  }

  // Test that a checkpoint transfer is properly cancelled by a term update
  void AppendEntriesCheckpointAbandon(bool async)
  {
    this->store.asynchronous(async);
    uint64_t term = 1;
    this->make_leader(term);
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());

    const char * cmd = "1";
    uint64_t expected_cluster_time = this->initial_cluster_time;
    uint64_t client_index=this->l.index_end();
    // Send success response from all peers except 1.  This will commit entry
    // so that it can be checkpointed.
    boost::dynamic_bitset<> responses;
    responses.resize(this->num_known_peers(), true);
    responses.flip(1);
    this->send_client_request(term, cmd, client_index++, responses);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
  
    auto ckpt = this->protocol->begin_checkpoint(1U);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(expected_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(expected_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());

    // Fire timer.  Peer 1 still doesn't have first log entry but since that entry is
    // discarded, a checkpoint will need to be sent to 1.
    this->protocol->on_timer(this->now);
    if (this->store.asynchronous()) {
      // Must read the checkpoint from the store in order to send it
      BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
    } else {
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    }
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
    this->comm.q.pop_back();

    auto resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(2U).request_term_number(1U).bytes_stored(2U).finish();
    this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);  
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(2U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->protocol->on_log_header_sync(this->now);

    // Send unexpected response; this should be happily ignored
    resp = append_checkpoint_chunk_response_builder().recipient_id(2).term_number(2U).request_term_number(1U).bytes_stored(2U).finish();
    this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);  
  }

  // Test that we can continue to send log entries to other peers while
  // sending a checkpoint to one
  void AppendEntriesCheckpointAppendWhileSendingCheckpoint(bool async)
  {
    this->store.asynchronous(async);
    uint64_t term = 1;
    this->make_leader(term);

    const char * cmd = "1";
    uint64_t expected_cluster_time = this->initial_cluster_time;
    uint64_t client_index=this->l.index_end();
    // Send success response from all peers except 1.  This will commit entry
    // so that it can be checkpointed.
    boost::dynamic_bitset<> responses;
    responses.resize(this->num_known_peers(), true);
    responses.flip(1);
    this->send_client_request(term, cmd, client_index++, responses);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    BOOST_CHECK_EQUAL(client_index, this->l.index_end());
  
    auto ckpt = this->protocol->begin_checkpoint(1U);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(expected_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    BOOST_CHECK_EQUAL(client_index, this->l.index_end());
    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(expected_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());
    BOOST_CHECK_EQUAL(client_index, this->l.index_end());
    BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());

    // Fire timer.  Peer 1 still doesn't have first log entry but since that entry is
    // discarded, a checkpoint will need to be sent to 1.
    this->protocol->on_timer(this->now);
    if (this->store.asynchronous()) {
      // Must read the checkpoint from the store in order to send it
      BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      this->store.completion_queue().pop_front();
    } else {
      BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    }
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
    {
      const auto & cfg(checkpoint_header_traits::configuration(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
      BOOST_CHECK_EQUAL(0U, simple_configuration_description_traits::size(&configuration_description_traits::to(&cfg)));
      BOOST_REQUIRE_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(&cfg)));
      for(std::size_t i=0; i<5; ++i) {
	BOOST_CHECK_EQUAL(i, server_description_traits::id(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)));
	BOOST_CHECK_EQUAL(0, server_description_traits::address(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)).compare((boost::format("192.168.1.%1%") % (i+1)).str()));
      }
    }
    
    this->comm.q.pop_back();

    // Check that we can send client request to other peers.
    this->send_client_request(term, cmd, client_index++, responses, responses);    
  }
  
  // Test that append_entries will send a checkpoint that needs log entries the leader has discarded
  // post checkpoint
  void AppendEntriesCheckpointResend(bool async)
  {
    this->store.asynchronous(async);
    uint64_t term = 1;
    this->make_leader(term);

    const char * cmd = "1";
    uint64_t expected_cluster_time = this->initial_cluster_time;
    uint64_t client_index=this->l.index_end();
    // Send success response from all peers except 1.  This will commit entry
    // so that it can be checkpointed.
    boost::dynamic_bitset<> responses;
    responses.resize(this->num_known_peers(), true);
    responses.flip(1);
    this->send_client_request(term, cmd, client_index++, responses);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
  
    auto ckpt = this->protocol->begin_checkpoint(1U);
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(0U, this->protocol->last_checkpoint_cluster_time());
    BOOST_REQUIRE(nullptr != ckpt.get());
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::log_entry_index_end(&ckpt->header()));
    BOOST_CHECK_EQUAL(1U, checkpoint_header_traits::last_log_entry_term(&ckpt->header()));
    BOOST_CHECK_EQUAL(expected_cluster_time, checkpoint_header_traits::last_log_entry_cluster_time(&ckpt->header()));
    BOOST_CHECK(nullptr == this->protocol->last_checkpoint().get());
    uint8_t data [] = { 0U, 1U, 2U, 3U, 4U };
    bool wrote=false;
    BOOST_CHECK_EQUAL(0U, this->store.completion_queue().size());
    ckpt->write(this->now, &data[0], 5U, [&wrote](std::chrono::time_point<std::chrono::steady_clock> clock_now) { wrote = true; });
    if (this->store.asynchronous()) {
      BOOST_CHECK(!wrote);
      BOOST_CHECK_EQUAL(1U, this->store.completion_queue().size());
      this->store.completion_queue().front()();
      BOOST_CHECK(wrote);
      this->store.completion_queue().pop_front();
    } else {
      BOOST_CHECK(wrote);
    }
    this->complete_checkpoint(ckpt);
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_index_end());
    BOOST_CHECK_EQUAL(1U, this->protocol->last_checkpoint_term());
    BOOST_CHECK_EQUAL(expected_cluster_time, this->protocol->last_checkpoint_cluster_time());
    BOOST_CHECK(ckpt == this->protocol->last_checkpoint());

    // Fire timer.  Peer 1 still doesn't have first log entry but since that entry is
    // discarded, a checkpoint will need to be sent to 1.   Don't respond to the first
    // message and wait.   Eventually it will be sent again.
    // Note that we'll get heartbeats for the other peers as well
    for(std::size_t j=0; j<3; ++j) {
      this->protocol->on_timer(this->now);
      if (j == 0 || j == 2) {
        if (this->store.asynchronous()) {
          // Must read the checkpoint from the store in order to send it
          BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
          this->store.completion_queue().front()();
          BOOST_REQUIRE(1U <= this->comm.q.size());
          this->store.completion_queue().pop_front();
          BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
        }
      }
      BOOST_REQUIRE(1U <= this->comm.q.size());
      if (j == 0 || j == 2) {
        // With asynchronous reads, the checkpoint request doesn't go out until after the read from the store
        // completes (hence not until after any heartbeats).   That means it's at the front of the queue.
        // In the synchronous read case the checkpoint request goes out before the heartbeats so is at the back (see below for other
        // consequences of that behavior).
        auto & msg(this->store.asynchronous() ? this->comm.q.front() : this->comm.q.back());
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK(!append_checkpoint_chunk_request_traits::checkpoint_done(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(msg))));
        {
          const auto & cfg(checkpoint_header_traits::configuration(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(msg))));
          BOOST_CHECK_EQUAL(0U, simple_configuration_description_traits::size(&configuration_description_traits::to(&cfg)));
          BOOST_REQUIRE_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(&cfg)));
          for(std::size_t i=0; i<5; ++i) {
            BOOST_CHECK_EQUAL(i, server_description_traits::id(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)));
            BOOST_CHECK_EQUAL(0, server_description_traits::address(&simple_configuration_description_traits::get(&configuration_description_traits::from(&cfg), i)).compare((boost::format("192.168.1.%1%") % (i+1)).str()));
          }
        }
        if (this->store.asynchronous()) {
          this->comm.q.pop_front();
          if (j==2) {
            // This is ridiculously subtle.   When reads are async, the append_checkpoint_chunk_request to peer 1
            // won't yet be sent in on_timer, so a heartbeat is also sent to peer 1.   In the synchrnous read case
            // on_timer knows that the append checkpoint has gone out so the heartbeat doesn't.
            check_heartbeat(1);
            this->comm.q.pop_back();
          }
        } else {
          this->comm.q.pop_back();
        }
      } else {
        check_heartbeat(1);
        this->comm.q.pop_back();
      }
      if (j == 0) {
        BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      } else {
        BOOST_REQUIRE_EQUAL(this->num_known_peers()-2, this->comm.q.size());
        for(uint64_t k=2; k<this->num_known_peers(); ++k) {
          check_heartbeat(k);
          this->comm.q.pop_back();
        }
      }
      this->now += std::chrono::milliseconds(600);
    }

    // Ack with bytes_stored=2 twice to validate the checkpoint protocol will resend data if requested
    for(int i=0; i<2; ++i) {
      auto resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(1U).request_term_number(1U).bytes_stored(2U).finish();
      this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);
      if (this->store.asynchronous()) {
        BOOST_CHECK_EQUAL(0U, this->comm.q.size());
        BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
        this->store.completion_queue().front()();
        BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
        this->store.completion_queue().pop_front();
        BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
        BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      } else {
        BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
      }
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(2U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(4U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK(!append_checkpoint_chunk_request_traits::checkpoint_done(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(this->comm.q.back()))));
      this->comm.q.pop_back();
    }

    auto resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(1U).request_term_number(1U).bytes_stored(4U).finish();
    this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);
    // Check resending of the final block as well.
    for(std::size_t j=0; j<3; ++j) {
      if (j>0) {
        this->protocol->on_timer(this->now);
      }
      if (j == 0 || j == 2) {
        if (this->store.asynchronous()) {
          // Must read the checkpoint from the store in order to send it
          BOOST_REQUIRE_EQUAL(1U, this->store.completion_queue().size());
          this->store.completion_queue().front()();
          BOOST_REQUIRE(1U <= this->comm.q.size());
          this->store.completion_queue().pop_front();
          BOOST_REQUIRE_EQUAL(0U, this->store.completion_queue().size());
        }
      }
      BOOST_REQUIRE(1 <= this->comm.q.size());
      if (j == 0 || j == 2) {
        auto & msg(this->store.asynchronous() ? this->comm.q.front() : this->comm.q.back());
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::recipient_id(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::term_number(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(0U, append_checkpoint_chunk_request_traits::leader_id(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(4U, append_checkpoint_chunk_request_traits::checkpoint_begin(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(5U, append_checkpoint_chunk_request_traits::checkpoint_end(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK(append_checkpoint_chunk_request_traits::checkpoint_done(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::checkpoint_index_end(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(1U, append_checkpoint_chunk_request_traits::last_checkpoint_term(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(expected_cluster_time, append_checkpoint_chunk_request_traits::last_checkpoint_cluster_time(boost::get<append_checkpoint_chunk_arg_type>(msg)));
        BOOST_CHECK_EQUAL(0U, checkpoint_header_traits::index(&append_checkpoint_chunk_request_traits::last_checkpoint_header(boost::get<append_checkpoint_chunk_arg_type>(msg))));
        if (this->store.asynchronous()) {
          this->comm.q.pop_front();
          if (j==2) {
            // This is ridiculously subtle.   When reads are async, the append_checkpoint_chunk_request to peer 1
            // won't yet be sent in on_timer, so a heartbeat is also sent to peer 1.   In the synchrnous read case
            // on_timer knows that the append checkpoint has gone out so the heartbeat doesn't.
            check_heartbeat(1);
            this->comm.q.pop_back();
          }
        } else {
          this->comm.q.pop_back();
        }
      } else {
        check_heartbeat(1);
        this->comm.q.pop_back();
      }
      if (j == 0) {
        BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());
      } else {
        BOOST_REQUIRE_EQUAL(this->num_known_peers()-2, this->comm.q.size());
        for(uint64_t k=2; k<this->num_known_peers(); ++k) {
          check_heartbeat(k);
          this->comm.q.pop_back();
        }
      }
      this->now += std::chrono::milliseconds(600);
    }

    resp = append_checkpoint_chunk_response_builder().recipient_id(1).term_number(1U).request_term_number(1U).bytes_stored(5U).finish();
    this->protocol->on_append_checkpoint_chunk_response(std::move(resp), this->now);  
    BOOST_REQUIRE_EQUAL(0U, this->comm.q.size());

    // We only checkpointed the first log entry, so peer 1 is still missing log entry 2.
    // Check that we get an append entry for it!   Get rid of the last increment to timestamp to avoid getting
    // heartbeats.
    this->now -= std::chrono::milliseconds(600);
    this->protocol->on_timer(this->now);
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(1, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(term, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
    BOOST_CHECK(log_entry_traits::is_command(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(this->comm.q.back()), 0U)));
    this->comm.q.pop_back();
  }

  void JointConsensusAddServer()
  {
    uint64_t term=1;
    this->make_leader(term);
    uint64_t request_id = 0;
    uint64_t commit_index = this->protocol->commit_index();
    uint64_t client_index=this->l.index_end();
    this->send_client_request_and_commit(term, "1", client_index++);  

    BOOST_CHECK_EQUAL(5U, this->num_known_peers());

    // Make sure configuration is five servers with (initial) id 0
    this->check_configuration_servers(term, request_id++, 0, 5U);

    // Stage a sixth server
    this->stage_new_server(term, commit_index+1);
    
    // This should catch up newly added server.  Based on this we should
    // move to transitional
    BOOST_CHECK(!this->cm->configuration().staging_servers_caught_up());
    this->send_client_request_and_commit(term, "2", client_index++);
    BOOST_CHECK(this->cm->configuration().staging_servers_caught_up());
    BOOST_CHECK(log_entry_traits::is_command(&this->l.entry(this->l.index_end()-1)));
    // TODO: Should I really have to do this on_timer call to trigger the transitional entry????
    this->protocol->on_timer(this->now);
    auto & le(this->l.entry(this->l.index_end()-1));
    BOOST_CHECK(this->cm->configuration().is_transitional());
    BOOST_CHECK(log_entry_traits::is_configuration(&le));
    BOOST_CHECK_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(&log_entry_traits::configuration(&le))));
    BOOST_CHECK_EQUAL(6U, simple_configuration_description_traits::size(&configuration_description_traits::to(&log_entry_traits::configuration(&le))));

    uint64_t expected=1;
    BOOST_CHECK_EQUAL(this->num_known_peers() - 1U, this->comm.q.size());
    while(this->comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(commit_index+2, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(3U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      auto resp = append_entry_response_builder().recipient_id(expected).term_number(1).request_term_number(1).index_begin(3).index_end(4).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      this->comm.q.pop_back();

      // We need quorum from both the old set of 5 servers and the new set of 6
      if(expected < 4) {
	BOOST_CHECK_EQUAL(commit_index + 2U, this->protocol->commit_index());
	// Should have the transitional entry in the log.
	BOOST_CHECK(this->cm->configuration().is_transitional());
	BOOST_CHECK_EQUAL(6U, this->num_known_peers());
	auto & le(this->l.entry(this->l.index_end()-1));
	BOOST_CHECK(log_entry_traits::is_configuration(&le));
	BOOST_CHECK_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(&log_entry_traits::configuration(&le))));
	BOOST_CHECK_EQUAL(6U, simple_configuration_description_traits::size(&configuration_description_traits::to(&log_entry_traits::configuration(&le))));
      } else {
	BOOST_CHECK_EQUAL(commit_index + 3U, this->protocol->commit_index());
	// Should get a new stable config entry in the log
	BOOST_CHECK(this->cm->configuration().is_stable());
	BOOST_CHECK_EQUAL(6U, this->num_known_peers());
	auto & le(this->l.entry(this->l.index_end()-1));
	BOOST_CHECK(log_entry_traits::is_configuration(&le));
	BOOST_CHECK_EQUAL(6U, simple_configuration_description_traits::size(&configuration_description_traits::from(&log_entry_traits::configuration(&le))));
	BOOST_CHECK_EQUAL(0U, simple_configuration_description_traits::size(&configuration_description_traits::to(&log_entry_traits::configuration(&le))));
      }
      if(expected != 4) {
	BOOST_CHECK_EQUAL(0U, this->c.configuration_responses.size());
      } else {
	BOOST_REQUIRE(0U < this->c.configuration_responses.size());
	BOOST_CHECK_EQUAL(1U, this->c.configuration_responses.size());
	BOOST_CHECK_EQUAL(raft::native::SUCCESS, this->c.configuration_responses.front().result);
	BOOST_CHECK_EQUAL(0U, this->c.configuration_responses.front().bad_servers.servers.size());
	this->c.configuration_responses.pop_back();
      }
      expected += 1;
    }
    // Stable configuration is NOT committed at this point so we get a retry
    BOOST_CHECK_EQUAL(commit_index + 3U, this->protocol->commit_index());
    this->check_configuration_retry(term, request_id++);
    BOOST_CHECK_EQUAL(commit_index + 3U, this->protocol->commit_index());

    // Hit timer so that leader sends stable configuration to peers
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    this->now += std::chrono::milliseconds(600);
    this->protocol->on_timer(this->now);
    expected=1;
    BOOST_CHECK_EQUAL(this->num_known_peers() - 1U, this->comm.q.size());
    while(this->comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(commit_index+3U, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(commit_index+3U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_REQUIRE_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK(log_entry_traits::is_configuration(&append_entry_request_traits::get_entry(boost::get<append_entry_request_arg_type>(this->comm.q.back()), 0)));
      auto resp = append_entry_response_builder().recipient_id(expected).term_number(term).request_term_number(term).index_begin(commit_index+3U).index_end(commit_index+4U).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      this->comm.q.pop_back();
      expected += 1;
    }    
    BOOST_CHECK_EQUAL(commit_index + 4U, this->protocol->commit_index());
    // Make sure configuration is six servers
    this->check_configuration_servers(term, request_id++, commit_index+3U, 6U);
  }

  void JointConsensusAddServerLostLeadershipFailure()
  {
    uint64_t term=1;
    this->make_leader(term);
    uint64_t commit_index=this->protocol->commit_index();
    uint64_t log_index=this->l.index_end();
    this->send_client_request_and_commit(term, "1", log_index);  

    BOOST_CHECK_EQUAL(5U, this->num_known_peers());
    this->stage_new_server(term, commit_index+1U);

    // This should catch up newly added server.  Based on this we should
    // move to transitional
    this->send_client_request_and_commit(term, "2", log_index+1);
    BOOST_CHECK(this->cm->configuration().staging_servers_caught_up());
    // TODO: Should I really have to do this on_timer call to trigger the transitional entry????
    this->protocol->on_timer(this->now);
    BOOST_CHECK(this->cm->configuration().is_transitional());
    BOOST_CHECK_EQUAL(0U, this->c.configuration_responses.size());

    // Now lose leadership by sending a log entry that conflicts with the transitional
    // configuration.  This triggers a failure in the configuration change and rollback
    // to prior configuration
    auto le = log_entry_builder().term(term+1).data("1").finish();
    auto msg = append_entry_request_builder().recipient_id(0).term_number(term+1).leader_id(1).log_index_begin(log_index+2).previous_log_term(term).leader_commit_index_end(this->protocol->commit_index()).entry(le).finish();
    this->protocol->on_append_entry_request(std::move(msg), this->now);
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_REQUIRE(0U < this->c.configuration_responses.size());
    BOOST_CHECK_EQUAL(raft::native::FAIL, this->c.configuration_responses.front().result);
    BOOST_CHECK(this->cm->configuration().is_stable());
    BOOST_CHECK_EQUAL(5U, this->num_known_peers());
  }

  void JointConsensusAddServerLostLeadershipSuccess()
  {
    uint64_t term=1;
    this->make_leader(term);
    uint64_t commit_index=this->protocol->commit_index();
    uint64_t log_index=this->l.index_end();
    this->send_client_request_and_commit(term, "1", log_index);  

    BOOST_CHECK_EQUAL(5U, this->num_known_peers());
    this->stage_new_server(term, commit_index+1U);

    // This should catch up newly added server.  Based on this we should
    // move to transitional
    this->send_client_request_and_commit(term, "2", log_index+1);
    BOOST_CHECK(this->cm->configuration().staging_servers_caught_up());
    // TODO: Should I really have to do this on_timer call to trigger the transitional entry????
    this->protocol->on_timer(this->now);
    BOOST_CHECK(this->cm->configuration().is_transitional());
    BOOST_CHECK_EQUAL(0U, this->c.configuration_responses.size());

    // Now lose leadership by sending a log entry that is consistent with the transitional
    // configuration and in fact tells the former leader the transitional config is committed.
    // This triggers a successful completion of the configuration change.  Since I am not the leader
    // I don't log the new stable config (the new leader does that).
    auto le = log_entry_builder().term(term+1).data("1").finish();
    auto msg = append_entry_request_builder().recipient_id(0).term_number(term+1).leader_id(1).log_index_begin(log_index+3).previous_log_term(term).leader_commit_index_end(commit_index+3).entry(le).finish();
    this->protocol->on_append_entry_request(std::move(msg), this->now);
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_REQUIRE(0U < this->c.configuration_responses.size());
    BOOST_CHECK_EQUAL(raft::native::SUCCESS, this->c.configuration_responses.front().result);
  }

  void CandidateVoteRequestAtSameTerm()
  {
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    this->now += std::chrono::milliseconds(500);
    this->protocol->on_timer(this->now);
    BOOST_CHECK_EQUAL(raft_type::CANDIDATE, this->protocol->get_state());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());

    // Vote request from old term gets immediate negative response
    {
      auto msg = vote_request_builder().recipient_id(0).term_number(0).candidate_id(1).log_index_end(0).last_log_term(0).finish();
      this->protocol->on_vote_request(std::move(msg), this->now);
      BOOST_CHECK_EQUAL(1U, this->comm.q.size());
      BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK(!vote_response_traits::granted(boost::get<vote_response_arg_type>(this->comm.q.back())));
      this->comm.q.pop_back();
    }

    // Now another server independently gets to term 1 and asks for a vote
    // we'll get response after a header sync but it won't be granted.  This server
    // will also send out vote requests to all other peers upon the header sync.
    {
      auto msg = vote_request_builder().recipient_id(0).term_number(1).candidate_id(1).log_index_end(0).last_log_term(0).finish();
      this->protocol->on_vote_request(std::move(msg), this->now);
      BOOST_CHECK_EQUAL(0U, this->comm.q.size());
    }

    // We use a brittle implementation detail here; vote response generated before vote requests.
    // Don't be surprised if this breaks some day.
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(this->num_known_peers(), this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, vote_response_traits::peer_id(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, vote_response_traits::term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, vote_response_traits::request_term_number(boost::get<vote_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(!vote_response_traits::granted(boost::get<vote_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
    uint32_t expected = 1;
    while(this->comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(expected, vote_request_traits::recipient_id(boost::get<vote_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, vote_request_traits::candidate_id(boost::get<vote_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(boost::get<vote_request_arg_type>(this->comm.q.back())));
      expected += 1;
      this->comm.q.pop_back();
    }
  }

  void CandidateAppendEntriesAtSameTerm()
  {
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    this->now += std::chrono::milliseconds(500);
    this->protocol->on_timer(this->now);
    BOOST_CHECK_EQUAL(raft_type::CANDIDATE, this->protocol->get_state());
    BOOST_CHECK(this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());

    // Append entry from old term gets immediate negative response
    {
      auto msg = append_entry_request_builder().recipient_id(0).request_id(2).term_number(0).leader_id(1).log_index_begin(1).previous_log_term(0).leader_commit_index_end(0).entry(log_entry_traits::create_noop(0, this->initial_cluster_time)).finish();
      this->protocol->on_append_entry_request(std::move(msg), this->now);
      BOOST_CHECK_EQUAL(raft_type::CANDIDATE, this->protocol->get_state());
      BOOST_CHECK(this->protocol->log_header_sync_required());
      BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(2U, append_entry_response_traits::request_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      BOOST_CHECK(!append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
      this->comm.q.pop_back();
    }

    // Header sync will send vote requests
    this->protocol->on_log_header_sync(this->now);
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(4U, this->comm.q.size());
    uint32_t expected = 1;
    while(this->comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(expected, vote_request_traits::recipient_id(boost::get<vote_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, vote_request_traits::candidate_id(boost::get<vote_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, vote_request_traits::term_number(boost::get<vote_request_arg_type>(this->comm.q.back())));
      expected += 1;
      this->comm.q.pop_back();
    }

    // Now another server independently gets leadership at term 1 and sends append entry.
    // We become follower without log header sync.
    auto msg = append_entry_request_builder().recipient_id(0).request_id(2).term_number(1).leader_id(1).log_index_begin(0).previous_log_term(0).leader_commit_index_end(0).entry(log_entry_traits::create_noop(0, this->initial_cluster_time)).finish();
    this->protocol->on_append_entry_request(std::move(msg), this->now);
    BOOST_CHECK_EQUAL(raft_type::FOLLOWER, this->protocol->get_state());
    BOOST_CHECK_EQUAL(1U, this->protocol->current_term());
    BOOST_CHECK_EQUAL(this->initial_cluster_time, this->protocol->cluster_time());
    BOOST_CHECK(!this->protocol->log_header_sync_required());
    BOOST_CHECK_EQUAL(0U, this->comm.q.size());

    // Once log is sync'd we'll send the response.
    this->protocol->on_log_sync(1, this->now);
    BOOST_REQUIRE_EQUAL(1U, this->comm.q.size());
    BOOST_CHECK_EQUAL(0U, append_entry_response_traits::recipient_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(1U, append_entry_response_traits::request_term_number(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK_EQUAL(2U, append_entry_response_traits::request_id(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    BOOST_CHECK(append_entry_response_traits::success(boost::get<append_entry_response_arg_type>(this->comm.q.back())));
    this->comm.q.pop_back();
  }

  void JointConsensusAddServerNewLeaderFinishesCommit()
  {
    // As FOLLOWER, get a transitional config from the leader
    BOOST_CHECK(this->cm->configuration().is_stable());
    uint64_t term=0;
    uint64_t log_index=this->l.index_end();
    uint64_t commit_index=this->protocol->commit_index();
    append_entry_request_builder aeb;
    aeb.recipient_id(0).term_number(term).leader_id(1).log_index_begin(log_index).previous_log_term(term).leader_commit_index_end(commit_index);
    {
      log_entry_builder leb;
      {	
	auto cb = leb.term(term).configuration();
	this->add_five_servers(cb.from());
	this->add_six_servers(cb.to());
      }
      aeb.entry(leb.finish());
    }
    auto msg = aeb.finish();
    this->protocol->on_append_entry_request(std::move(msg), this->now);
    BOOST_CHECK(this->cm->configuration().is_transitional());
    auto & le(this->l.entry(this->l.index_end()-1));
    BOOST_CHECK(log_entry_traits::is_configuration(&le));
    BOOST_CHECK_EQUAL(5U, simple_configuration_description_traits::size(&configuration_description_traits::from(&log_entry_traits::configuration(&le))));
    BOOST_CHECK_EQUAL(6U, simple_configuration_description_traits::size(&configuration_description_traits::to(&log_entry_traits::configuration(&le))));

    this->make_leader(term+1, false);

    // Now leader and have the config entry so should try to replicate it but a new leader
    // is optimistic and assumes that all peers have its log entries.  It will append a NOOP
    // and replicate that.   We need the retransmit timer to go off so that we can see the entries
    this->protocol->on_timer(this->now);
    BOOST_CHECK_EQUAL(0, this->comm.q.size());
    this->now += std::chrono::milliseconds(1);
    this->protocol->on_timer(this->now);
    BOOST_CHECK_EQUAL(0, this->comm.q.size());
    this->now += std::chrono::milliseconds(1);
    this->protocol->on_timer(this->now);
    uint64_t expected=1;
    BOOST_CHECK_EQUAL(this->num_known_peers() - 1U, this->comm.q.size());
    while(this->comm.q.size() > 0) {
      BOOST_CHECK_EQUAL(expected, append_entry_request_traits::recipient_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term+1, append_entry_request_traits::term_number(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(0U, append_entry_request_traits::leader_id(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(commit_index, append_entry_request_traits::leader_commit_index_end(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      // Assumes peer also has the transitional config log entry
      BOOST_CHECK_EQUAL(log_index+1U, append_entry_request_traits::log_index_begin(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(term, append_entry_request_traits::previous_log_term(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      BOOST_CHECK_EQUAL(1U, append_entry_request_traits::num_entries(boost::get<append_entry_request_arg_type>(this->comm.q.back())));
      auto resp = append_entry_response_builder().recipient_id(expected).term_number(term+1).request_term_number(term+1).index_begin(log_index+1).index_end(log_index+2).success(true).finish();
      this->protocol->on_append_entry_response(std::move(resp), this->now);
      this->comm.q.pop_back();

      // We need quorum from both the old set of 5 servers and the new set of 6.
      // When we do commit, we'll commit the transitional entry and the NOOP
      if(expected < 4) {
	BOOST_CHECK_EQUAL(commit_index, this->protocol->commit_index());
	// Should have the transitional entry in the log.
	BOOST_CHECK(this->cm->configuration().is_transitional());
	BOOST_CHECK_EQUAL(6U, this->num_known_peers());
	auto & le(this->l.entry(this->l.index_end()-1));
	BOOST_CHECK(log_entry_traits::is_noop(&le));
      } else {
	// The log should be fully committed up through the NOOP entry  regardless of where
	// commit_index started
	BOOST_CHECK_EQUAL(log_index+2, this->protocol->commit_index());
	// Should get a new stable config entry in the log
	BOOST_CHECK(this->cm->configuration().is_stable());
	BOOST_CHECK_EQUAL(6U, this->num_known_peers());
	auto & le(this->l.entry(this->l.index_end()-1));
	BOOST_CHECK(log_entry_traits::is_configuration(&le));
	BOOST_CHECK_EQUAL(6U, simple_configuration_description_traits::size(&configuration_description_traits::from(&log_entry_traits::configuration(&le))));
	BOOST_CHECK_EQUAL(0U, simple_configuration_description_traits::size(&configuration_description_traits::to(&log_entry_traits::configuration(&le))));
      }
      // Did not initiate the config change so should not send a response
      BOOST_CHECK_EQUAL(0U, this->c.configuration_responses.size());
      expected += 1;
    }  
  }
};

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedBasicOnVoteRequestTest, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.BasicOnVoteRequestTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedOnVoteRequestSlowHeaderSyncTest, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.OnVoteRequestSlowHeaderSyncTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesLogSync, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesLogSync();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesLogSyncEmptyLog, _TestType, test_types)
{
  RaftTestBase<_TestType> t(raft::test::TestFixtureInitialization::EMPTY);
  t.AppendEntriesLogSyncEmptyLog();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesNegativeResponse, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesNegativeResponse();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesSlowHeaderSync, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesSlowHeaderSync();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesTruncateUncommitted, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesTruncateUncommitted();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedFollowerToCandidateToFollower, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.FollowerToCandidateToFollower();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedFollowerToFollower, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.FollowerToFollower();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunk, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendCheckpointChunk(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunkAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendCheckpointChunk(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunkEmptyLog, _TestType, test_types)
{
  RaftTestBase<_TestType> t(raft::test::TestFixtureInitialization::EMPTY);
  t.AppendCheckpointChunk(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunkSlowHeaderSync, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendCheckpointChunkSlowHeaderSync(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunkSlowHeaderSyncAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendCheckpointChunkSlowHeaderSync(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunkSlowHeaderSyncEmptyLog, _TestType, test_types)
{
  RaftTestBase<_TestType> t(raft::test::TestFixtureInitialization::EMPTY);
  t.AppendCheckpointChunkSlowHeaderSync(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunkTermAdvanceWaitingForSync, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendCheckpointChunkTermAdvanceWaitingForSync();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendCheckpointChunkTermAdvanceWaitingForWrite, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendCheckpointChunkTermAdvanceWaitingForWrite();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedClientCheckpointTest, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.ClientCheckpointTest(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedClientCheckpointTestAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.ClientCheckpointTest(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedClientCheckpointLogInitializationTest, _TestType, test_types)
{
  RaftTestBase<_TestType> t(raft::test::TestFixtureInitialization::LOG);
  t.ClientCheckpointTest(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedClientCheckpointLogInitializationTestAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t(raft::test::TestFixtureInitialization::LOG);
  t.ClientCheckpointTest(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedPartialClientCheckpointTest, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.ClientPartialCheckpointTest(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedPartialClientCheckpointTestAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.ClientPartialCheckpointTest(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedClientCheckpointOldTermTest, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.ClientCheckpointOldTermTest(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedClientCheckpointOldTermTestAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.ClientCheckpointOldTermTest(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedClientCheckpointNegativeTest, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.ClientCheckpointNegativeTest();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpoint, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpoint(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpoint(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointAllInOneChunk, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointAllInOneChunk(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointAllInOneChunkAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointAllInOneChunk(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointAbandon, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointAbandon(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointAbandonAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointAbandon(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointAppendWhileSendingCheckpoint, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointAppendWhileSendingCheckpoint(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointAppendWhileSendingCheckpointAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointAppendWhileSendingCheckpoint(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointResend, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointResend(false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedAppendEntriesCheckpointResendAsynchronous, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.AppendEntriesCheckpointResend(true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedJointConsensusAddServer, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.JointConsensusAddServer();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedJointConsensusAddServerLostLeadershipFailure, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.JointConsensusAddServerLostLeadershipFailure();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedJointConsensusAddServerLostLeadershipSuccesss, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.JointConsensusAddServerLostLeadershipSuccess();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedCandidateVoteRequestAtSameTerm, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.CandidateVoteRequestAtSameTerm();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedCandidateAppendEntriesAtSameTerm, _TestType, test_types)
{
  RaftTestBase<_TestType> t;
  t.CandidateAppendEntriesAtSameTerm();
}

BOOST_AUTO_TEST_CASE_TEMPLATE(TemplatedJointConsensusAddServerNewLeaderFinishesCommit, _TestType, test_types)
{
  RaftTestBase<_TestType> t(raft::test::TestFixtureInitialization::LOG);
  t.JointConsensusAddServerNewLeaderFinishesCommit();
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
// TODO: Restore checkpoint to a FOLLOWER whose state machine is behind with respect to the checkpoint.
// TODO: Restore a checkpoint to a state machine while the state machine is in the middle of an async log application.
// TODO: Initiate a checkpoint of a state machine that hasn't applied all of the log entries in the checkpoint.

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

