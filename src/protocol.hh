#ifndef __RAFT_PROTOCOL_HH__
#define __RAFT_PROTOCOL_HH__

#include <functional>
#include <random>
#include <tuple>

#include <boost/lexical_cast.hpp>
#include <boost/log/trivial.hpp>

#include "configuration.hh"
#include "checkpoint.hh"
#include "log.hh"
#include "peer.hh"
#include "slice.hh"

// General TODO:
// log_entry_type::entry should probably be async since we shouldn't assume the entire log can fit into memory.

namespace raft {

  /**
   * cluster_clock - A consistent cluster-wide clock
   *
   * The cluster clock is used as part of the implementation of linearizable semantics.   Part of that implementation
   * requires that we be able to timeout and garbage collect dead clients.   It is crucial that all protocol instances
   * agree on when a client is timed out, otherwise state will not be consistent.   
   *
   * The intuition behind the cluster_clock is that it represents the number of nanoseconds that there has been a leader
   * in the cluster.   The cluster_clock is advanced in the leader and is shared among followers in log_entries.   
   */
  class cluster_clock
  {
  private:
    // This is the cluster time at the current time point
    uint64_t cluster_time_;
    // This is the local time at the current time point
    std::chrono::steady_clock::time_point local_time_;
  public:

    cluster_clock(std::chrono::steady_clock::time_point now)
      :
      cluster_time_(0),
      local_time_(now)
    {
    }
    
    // Set the cluster clock to this cluster_time
    void set(uint64_t cluster_time,
             std::chrono::steady_clock::time_point local_now)
    {
      cluster_time_ = cluster_time;
      local_time_ = local_now;
    }

    // Used for testing purposes not for the algorithm
    uint64_t cluster_time() const
    {
      return cluster_time_;
    }

    // Advance the cluster_clock and return the current cluster_time
    uint64_t advance(std::chrono::steady_clock::time_point local_now)
    {
      uint64_t elapsed = std::chrono::nanoseconds(local_now - local_time_).count();
      cluster_time_ += elapsed;
      local_time_ = local_now;
      return cluster_time_;
    }

    // Return the current cluster_time but do not update it
    uint64_t now(std::chrono::steady_clock::time_point local_now) const
    {
      uint64_t elapsed = std::chrono::nanoseconds(local_now - local_time_).count();
      return cluster_time_ + elapsed;
    }
  };

  class append_entry_continuation
  {
  public:
    // Leader that sent the append entry request we are responding to.
    uint64_t leader_id;
    // The log index we need flushed in order to respond request
    uint64_t begin_index;
    uint64_t end_index;
    // The term that the client request was part of
    uint64_t term;
    // The request_id we are responding to
    uint64_t request_id;
  };

  class append_checkpoint_chunk_response_continuation
  {
  public:
    uint64_t leader_id;
    uint64_t term_number;
    uint64_t request_term_number;
    uint64_t request_id;
    uint64_t bytes_stored;
  };

  class vote_response_continuation
  {
  public:
    uint64_t candidate_id;
    uint64_t term_number;
    uint64_t request_term_number;
    uint64_t request_id;
    bool granted;
  };

  // Per-server state related to checkpoints
  // This class is a CRTP to add checkpointability to an underlying Raft protocol
  //
  // The _Protocol class is assumed to have the following public methods:
  //
  // uint64_t my_cluster_id();
  // uint64_t current_term();
  // checkpoint_header_type create_checkpoint_header(uint64_t last_index_in_checkpoint);
  // bool check_log_has_checkpoint(checkpoint_data_ptr);
  // void set_checkpoint(checkpoint_data_ptr);
  // void set_checkpoint_header(checkpoint_header_type &);
  // void send_append_checkpoint_chunk_response(append_checkpoint_chunk_response_continuation);
  template <typename _Messages, typename _Protocol>
  class server_checkpoint
  {
  public:
    typedef _Protocol protocol_type;
    typedef _Messages messages_type;
    typedef typename messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits_type;
    typedef typename messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_request_arg_type;
    typedef checkpoint_data_store<messages_type> checkpoint_data_store_type;
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef typename checkpoint_data_store_type::header_type checkpoint_header_type;
    typedef typename checkpoint_data_store_type::header_traits_type checkpoint_header_traits_type;
    
  private:
    protocol_type * protocol()
    {
      return static_cast<protocol_type *>(this);
    }
    const protocol_type * protocol() const
    {
      return static_cast<const protocol_type *>(this);
    }
    void sync(const append_checkpoint_chunk_response_continuation & resp) {
      checkpoint_chunk_response_sync_continuations_.emplace_back(resp);
    }
    void update_last_checkpoint(const checkpoint_header_type & header)
    {
      last_checkpoint_index_ = checkpoint_header_traits_type::last_log_entry_index(&header);
      last_checkpoint_term_ = checkpoint_header_traits_type::last_log_entry_term(&header);
      last_checkpoint_cluster_time_ = checkpoint_header_traits_type::last_log_entry_cluster_time(&header);
    }

    // One after last log entry checkpointed.
    uint64_t last_checkpoint_index_;
    // Term of last checkpoint
    uint64_t last_checkpoint_term_;
    // Cluster time of last checkpoint
    uint64_t last_checkpoint_cluster_time_;
    // continuations depending on checkpoint sync events
    std::vector<append_checkpoint_chunk_response_continuation > checkpoint_chunk_response_sync_continuations_;
    // Object to manage receiving a new checkpoint from a leader and writing it to a reliable
    // location (the data_store)
    std::shared_ptr<in_progress_checkpoint<checkpoint_data_store_type> > current_checkpoint_;
    // Checkpoints live here
    checkpoint_data_store_type & store_;
    // Interface to state machine for restoring checkpoint
    std::function<void(checkpoint_block, bool)> state_machine_;
  
    server_checkpoint(checkpoint_data_store_type & store)
      :
      last_checkpoint_index_(0),
      last_checkpoint_term_(0),
      last_checkpoint_cluster_time_(0),
      store_(store),
      state_machine_([](checkpoint_block, bool) { })
    {
    }
    friend _Protocol;

  protected:
    // Append Checkpoint Chunk processing
    std::pair<uint64_t, bool> write_checkpoint_chunk(append_checkpoint_chunk_request_arg_type && req)
    {
      typedef append_checkpoint_chunk_request_traits_type acc;

      // This is a bit unpleasant.   If this req initiates a checkpoint then
      // we have to transfer ownership of it to the in_progress_checkpoint via
      // an opaque deleter.   Just in case we have to unpack all of the internal
      // details here for processing.
      auto req_checkpoint_done = acc::checkpoint_done(req);
      auto req_last_checkpoint_index = acc::last_checkpoint_index(req);
      auto req_leader_id = acc::leader_id(req);
      auto req_request_id = acc::request_id(req);
      auto req_data = acc::data(req);
      auto req_checkpoint_begin = acc::checkpoint_begin(req);
      auto req_checkpoint_end = acc::checkpoint_end(req);
      
      if (!current_checkpoint_) {
	const auto & header(acc::last_checkpoint_header(req));
	current_checkpoint_.reset(new in_progress_checkpoint<checkpoint_data_store_type>(store_, &header, [r = std::move(req)](){}));
      }

      // N.B. req might be invalid at this point

      if (req_checkpoint_begin != current_checkpoint_->end()) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << protocol()->my_cluster_id() << ") at term " << protocol()->current_term() <<
	  " received checkpoint chunk at " << req_checkpoint_begin <<
	  " expecting at offset " << current_checkpoint_->end() << ".  Ignoring append_checkpoint_chunk_request message.";
	return { current_checkpoint_->end(), false };
      }

      current_checkpoint_->write(std::move(req_data));
      BOOST_LOG_TRIVIAL(info) << "Server(" << protocol()->my_cluster_id() << ") at term " << protocol()->current_term() <<
        " received checkpoint chunk from leader_id=" << req_leader_id << " request_id=" << req_request_id <<
        " containing byte_range=[" << req_checkpoint_begin << "," << req_checkpoint_end << ")"
        " for checkpoint at index " << req_last_checkpoint_index;

      if (req_checkpoint_done) {
	if (req_last_checkpoint_index < last_checkpoint_index()) {
	  BOOST_LOG_TRIVIAL(warning) << "Server(" << protocol()->my_cluster_id() << ") at term " << protocol()->current_term() <<
	    " received completed checkpoint at index " << req_last_checkpoint_index <<
	    " but already have a checkpoint at " << last_checkpoint_index() << ".  Ignoring entire out of date checkpoint.";
	  abandon_checkpoint();
	} else {
          // Don't respond until the checkpoint is sync'd. Make a note to
          // do so.
	  append_checkpoint_chunk_response_continuation resp;
	  resp.leader_id = req_leader_id;
	  resp.term_number = resp.request_term_number = protocol()->current_term();
          resp.request_id = req_request_id;
	  resp.bytes_stored = current_checkpoint_->end();
	  sync(resp);
          BOOST_LOG_TRIVIAL(info) << "Server(" << protocol()->my_cluster_id() << ") at term " << protocol()->current_term() <<
            " received final checkpoint chunk, waiting for checkpoint sync.";
        }
      }
      // If checkpoint not done then we can respond immediately.
      return { current_checkpoint_->end(), !req_checkpoint_done };
    }
    
  public:

    // Attach the state machine
    template<typename _Callback>
    void set_state_machine_for_checkpoint(_Callback && cb)
    {
      state_machine_ = std::move(cb);
    }
    
    // One past the last log index of the last completed checkpoint
    uint64_t last_checkpoint_index() const {
      return last_checkpoint_index_;
    }
    
    // The term of the last log entry of the last completed checkpoint
    uint64_t last_checkpoint_term() const {
      return last_checkpoint_term_;
    }
    
    // The cluster time of the last log entry of the last completed checkpoint
    uint64_t last_checkpoint_cluster_time() const {
      return last_checkpoint_cluster_time_;
    }

    bool checkpoint_sync_required() const {
      return !checkpoint_chunk_response_sync_continuations_.empty();
    }

    void abandon_checkpoint() {
      if (!!current_checkpoint_) {
	store_.discard(current_checkpoint_->file_);
	current_checkpoint_.reset();
      }
    }

    // State of the last completed checkpoint
    checkpoint_data_ptr last_checkpoint() {
      return store_.last_checkpoint();
    }

    checkpoint_data_ptr begin_checkpoint(uint64_t last_index_in_checkpoint) const
    {
      auto header = protocol()->create_checkpoint_header(last_index_in_checkpoint);
      if (header.first != nullptr) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << protocol()->my_cluster_id() << ") at term " << protocol()->current_term() <<
	  " initiated checkpoint at index " << last_index_in_checkpoint << " with checkpoint header" <<
          " last_index=" <<checkpoint_header_traits_type::last_log_entry_index(header.first) <<
          " last_term=" <<  checkpoint_header_traits_type::last_log_entry_term(header.first) <<
          " last_cluster_time=" << checkpoint_header_traits_type::last_log_entry_cluster_time(header.first);
        return store_.create(std::move(header));
      } else {
        return checkpoint_data_ptr();
      }
    }

    bool complete_checkpoint(checkpoint_data_ptr ckpt, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      auto last_index_in_checkpoint = checkpoint_header_traits_type::last_log_entry_index(&ckpt->header());

    
      if (last_index_in_checkpoint <= last_checkpoint_index()) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << protocol()->my_cluster_id() << ") at term " << protocol()->current_term() <<
	  " got request to complete checkpoint at index " << last_index_in_checkpoint << " but already has checkpoint " <<
	  " at index " << last_checkpoint_index();
	return false;
      }

      if (!protocol()->check_log_has_checkpoint(ckpt)) {
        return false;
      }

      update_last_checkpoint(ckpt->header());

      BOOST_ASSERT(last_checkpoint_index() == checkpoint_header_traits_type::last_log_entry_index(&ckpt->header()));
      BOOST_ASSERT(last_checkpoint_term() == checkpoint_header_traits_type::last_log_entry_term(&ckpt->header()));
      BOOST_ASSERT(last_checkpoint_cluster_time() == checkpoint_header_traits_type::last_log_entry_cluster_time(&ckpt->header()));

      protocol()->set_checkpoint(ckpt, clock_now);

      store_.commit(ckpt);
    
      return true;
    }
    
    void load_checkpoint(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      checkpoint_data_ptr ckpt = last_checkpoint();
      if (!ckpt) {
	// TODO: Should be a WARNING or ERROR
	return;
      }
      const checkpoint_header_type & header(ckpt->header());

      if (checkpoint_header_traits_type::last_log_entry_index(&header) < last_checkpoint_index()) {
        // We've already loaded a more recent checkpoint than this one.   Note that on_checkpoint_sync
        // commits to the store (hence changes the return value of last_checkpoint()) without
        // calling update_last_checkpoint (which sets the value of last_checkpoint_index()).   So,
        // In that scenario we can get a more recent checkpoint in the store than the values in last_checkpoint_*().
        // TODO: It seems that if this is true we have blown away our most recent checkpoint and that would
        // be very bad!   In LogCabin, the corresponding condition is a PANIC.
        throw std::runtime_error("checkpoint_header_traits_type::last_log_entry_index(&header) < last_checkpoint_index()");
      }

      // Update server_checkpoint
      update_last_checkpoint(header);

      BOOST_ASSERT(last_checkpoint_index() == checkpoint_header_traits_type::last_log_entry_index(&header));
      BOOST_ASSERT(last_checkpoint_term() == checkpoint_header_traits_type::last_log_entry_term(&header));
      BOOST_ASSERT(last_checkpoint_cluster_time() == checkpoint_header_traits_type::last_log_entry_cluster_time(&header));

      protocol()->set_checkpoint_header(header, clock_now);

      // TODO: Make the state machine restore async
      checkpoint_block block;
      do {
        state_machine_(block, false);
        block = last_checkpoint()->next_block(block);
      } while(!last_checkpoint()->is_final(block));
      state_machine_(block, true);
    }

    // Update of checkpoint file is synced to disk
    void on_checkpoint_sync()
    {
      on_checkpoint_sync(std::chrono::steady_clock::now());
    }
    
    void on_checkpoint_sync(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      BOOST_ASSERT(current_checkpoint_);
      BOOST_ASSERT(1U == checkpoint_chunk_response_sync_continuations_.size());
      if (protocol()->current_term() == checkpoint_chunk_response_sync_continuations_[0].term_number) {
	// What happens if the current term has changed while we were waiting for sync?  Right now I am suppressing the
	// message but perhaps better to let the old leader know things have moved on.
	// TODO: Interface to load checkpoint state now that it is complete
	// TODO: Do we want an async interface for loading checkpoint state into memory?
	store_.commit(current_checkpoint_->file_);
        protocol()->send_append_checkpoint_chunk_response(checkpoint_chunk_response_sync_continuations_[0]);
        this->load_checkpoint(clock_now);
      }
      current_checkpoint_.reset();
      checkpoint_chunk_response_sync_continuations_.resize(0);
    }
  };

  // A protocol instance encapsulates what a participant in the Raft consensus protocol knows about itself
  template<typename _Communicator, typename _Client, typename _Messages>
  class protocol : public server_checkpoint<_Messages, protocol<_Communicator, _Client, _Messages>>
  {
  public:
    // Communicator types
    typedef _Messages messages_type;
    typedef typename _Communicator::template apply<messages_type>::type communicator_type;

    // Client types
    typedef typename _Client::template apply<messages_type>::type client_type;

    // Checkpoint types
    typedef checkpoint_data_store<messages_type> checkpoint_data_store_type;
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef typename checkpoint_data_store_type::header_type checkpoint_header_type;
    typedef typename checkpoint_data_store_type::header_traits_type checkpoint_header_traits_type;
    typedef peer_checkpoint<checkpoint_data_store_type> peer_checkpoint_type;

    // A peer with this checkpoint data store and whatever the configuration manager brings to the table
    struct peer_metafunction
    {
      template <typename configuration_change_type>
      struct apply
      {
	typedef peer<checkpoint_data_store_type, configuration_change_type> type;
      };
    };

    // Log types
    typedef typename messages_type::log_entry_type log_entry_type;
    typedef typename messages_type::log_entry_traits_type log_entry_traits_type;
    typedef typename messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
    typedef in_memory_log<log_entry_type, log_entry_traits_type> log_type;
    typedef typename log_type::index_type log_index_type;

    // Message argument types and traits for looking at them.    We don't have concrete/value types
    // so we can not call c'tors/d'tors etc.   That is quite intentional.
    typedef typename messages_type::client_result_type client_result_type;
    typedef typename messages_type::vote_request_traits_type vote_request_traits_type;
    typedef typename messages_type::vote_request_traits_type::arg_type vote_request_arg_type;
    typedef typename messages_type::vote_response_traits_type vote_response_traits_type;
    typedef typename messages_type::vote_response_traits_type::arg_type vote_response_arg_type;
    typedef typename messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits_type;
    typedef typename messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_request_arg_type;
    typedef typename messages_type::append_checkpoint_chunk_response_traits_type append_checkpoint_chunk_response_traits_type;
    typedef typename messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;
    typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits_type;
    typedef typename messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
    typedef typename messages_type::append_entry_response_traits_type append_entry_response_traits_type;
    typedef typename messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
    typedef typename messages_type::set_configuration_request_traits_type set_configuration_request_traits_type;
    typedef typename messages_type::set_configuration_request_traits_type::arg_type set_configuration_request_arg_type;

    // Configuration types
    typedef configuration_manager<peer_metafunction, messages_type> configuration_manager_type;
    typedef typename configuration_manager_type::configuration_type configuration_type;

    // The complete peer type which includes info about both per-peer checkpoint and configuration state.
    typedef typename configuration_manager_type::peer_type peer_type;

    enum state { LEADER, FOLLOWER, CANDIDATE };

    static const std::size_t INVALID_PEER_ID = std::numeric_limits<std::size_t>::max();
    static std::size_t INVALID_PEER_ID_FUN()
    {
      return std::numeric_limits<std::size_t>::max();
    }

  private:

    communicator_type & comm_;

    // TODO: Probably much better to use a weak ptr here.
    client_type * config_change_client_;

    // This is the main state as per the Raft description but given our full blown state
    // machine approach there are some other subordinate states to consider mostly related to
    // asynchronous interactions with the log and client.
    state state_;

    // To prevent multiple votes per-term and invalid log truncations (TODO: describe these) we must
    // persist any changes of current_term_ or voted_for_ before proceeding with the protocol.
    // This flag indicates that a change has been made to either of the above two members
    bool log_header_sync_required_;
    
    // Algorithm config parameters
    std::chrono::milliseconds election_timeout_max_;
    std::chrono::milliseconds election_timeout_min_;

    // Leader id if I know it (learned from append_entry_request and append_checkpoint_chunk_request messages)
    std::size_t leader_id_;

    // My current term number
    uint64_t current_term_;

    // the peer that got my vote (could be myself)
    peer_type * voted_for_;

    // We can learn of the commit from a
    // checkpoint or from a majority of peers acknowledging a log entry.
    // last_committed_index_ represents the last point in the log that we KNOW
    // is replicated and therefore safe to apply to a state machine.  It may
    // be an underestimate of last successfully replicated log entry but that fact
    // will later be learned (e.g. when a leader tries to append again and ???).
    // N.B. This points one past the last committed entry of the log (in particular
    // if last_committed_index_==0 then there is nothing committed in the log).
    uint64_t last_committed_index_;

    // One past the last log entry sync'd to disk.  A FOLLOWER needs to sync
    // before telling the LEADER that a log entry is accepted.  A leader lazily waits
    // for a log entry to sync and uses the sync state to determine whether it accepts a
    // log entry.  When a majority accept the entry, it is committed.
    // N.B.  This points one past the last synced entry in the log.
    uint64_t last_synced_index_;

    // One past the last log index successfully applied to the state machine.
    uint64_t last_applied_index_;

    // This is the state machine to which committed log entries are applied
    std::function<void(log_entry_const_arg_type, uint64_t, std::size_t)> state_machine_;

    // TODO: Fold this into the state machine
    std::function<void(state, uint64_t)> state_change_listener_;

    // The consistent cluster wide clock
    cluster_clock cluster_clock_;

    // If LEADER, this is the request id to use for next vote_request, append_entry_request or append_checkpoint_chunk_request.
    // It is not necessary to update this every time we send such a message, but only when we really
    // want to know if the message has been ack'd by peers.
    // For example, to do linearizable read only query, we need to know that we are the "current" leader
    // so we send out heartbeats with an incremented request id and wait to get a quorum of responses
    // with request id at least as large.
    uint64_t request_id_;
    // If LEADER this is the index of the NOOP entry we make at the beginning of our term.   We cannot service
    // read-only queries until this is committed.
    uint64_t leader_read_only_commit_fence_;

    // Common log state
    log_type & log_;

    // State related to configuration management
    configuration_manager_type & configuration_;
    
    const configuration_type & configuration() const
    {
      return configuration_.configuration();
    }

    configuration_type & configuration()
    {
      return configuration_.configuration();
    }

    peer_type & self() {
      return configuration().self();
    }

    peer_type & peer_from_id(uint64_t peer_id) {
      return configuration().peer_from_id(peer_id);
    }

    std::size_t num_known_peers() const
    {
      return configuration().num_known_peers();
    }

    bool has_quorum() const {
      return configuration().has_quorum();
    }

    // continuations depending on log sync events
    std::multimap<uint64_t, append_entry_continuation> append_entry_continuations_;
    // continuations depending on log header sync events
    std::vector<append_entry_request_arg_type> append_entry_header_sync_continuations_;
    std::vector<append_checkpoint_chunk_request_arg_type> append_checkpoint_chunk_request_header_sync_continuations_;
    std::vector<vote_response_continuation> vote_response_header_sync_continuations_;
    std::map<uint64_t, std::function<void(client_result_type)>> request_id_quorum_continuations_;
    std::map<uint64_t, std::vector<std::function<void(client_result_type)>>> state_machine_applied_continuations_;
    // This flag is essentially another log header sync continuation that indicates vote requests 
    // should be sent out after log header sync (which is the case when we transition to CANDIDATE
    // but not when we transition to FOLLOWER).
    bool send_vote_requests_;


    bool no_log_header_sync_continuations_exist() const {
      return !send_vote_requests_ && 0 == vote_response_header_sync_continuations_.size() &&
	0 == append_entry_header_sync_continuations_.size();
    }

    uint64_t commit_index_term() const {
      // Either:
      // 1) commit is in current log
      // 2) log is empty
      // 3) commit is the last entry checkpointed
      BOOST_ASSERT(last_committed_index_ > log_.start_index() || 0 == last_committed_index_ || last_committed_index_ == this->last_checkpoint_index());
      return last_committed_index_ > this->last_checkpoint_index() ?  log_.term(last_committed_index_ - 1) :
        (last_committed_index_ > 0 ? this->last_checkpoint_term() : 0);
    }
    
    uint64_t last_log_entry_term() const {
      // Something subtle with snapshots/checkpoints occurs here.  
      // After a checkpoint we may have no log entries so the term has to be recorded at the time
      // of the checkpoint.
      return log_.empty() ? this->last_checkpoint_term() : log_.last_entry_term();
    }
    uint64_t last_log_entry_cluster_time() const {
      // Something subtle with snapshots/checkpoints occurs here.  
      // After a checkpoint we may have no log entries so the cluster time has to be recorded at the time
      // of the checkpoint.
      return log_.empty() ? this->last_checkpoint_cluster_time() : log_.last_entry_cluster_time();
    }
    // Inserting this public/private pair to avoid colliding some some subsequent refactoring
  public:
    uint64_t last_log_entry_index() const {
      return log_.last_index();
    }
  private:
    uint64_t log_start_index() const {
      return log_.start_index();
    }

    // Used by FOLLOWER and CANDIDATE to decide when to initiate a new election.
    std::chrono::time_point<std::chrono::steady_clock> election_timeout_;

    std::chrono::time_point<std::chrono::steady_clock> new_election_timeout(std::chrono::time_point<std::chrono::steady_clock> clock_now) const
    {
      std::default_random_engine generator;
      std::uniform_int_distribution<std::chrono::milliseconds::rep> dist(election_timeout_min_.count(), 
									 election_timeout_max_.count());
      return clock_now + std::chrono::milliseconds(dist(generator));
    }

    std::chrono::time_point<std::chrono::steady_clock> new_heartbeat_timeout(std::chrono::time_point<std::chrono::steady_clock> clock_now) const
    {
      return clock_now + std::chrono::milliseconds(election_timeout_min_.count()/2);
    }

    static const char * state_string(state s)
    {
      switch(s) {
      case LEADER:
	return "LEADER";
      case CANDIDATE:
	return "CANDIDATE";
      case FOLLOWER:
	return "FOLLOWER";
      default:
	return "INVALID";
      }
    }

    void send_vote_requests()
    {
      BOOST_ASSERT(!log_header_sync_required_);
      for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
	auto i = peer_it->peer_id;
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " requesting vote from peer " << i;
	peer_type & p(*peer_it);
	p.vote_ = boost::logic::indeterminate;
	if (i != my_cluster_id()) {
	  comm_.vote_request(p.peer_id, p.address,
			     request_id_,
                             i,
			     current_term_,
			     my_cluster_id(),
			     last_log_entry_index(),
			     last_log_entry_term());
	}
      }
    }
    
    void send_append_entries(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      // TODO: Implement some pacing.  We want to give peers a chance to process entries before deciding that
      // they never will and resending (e.g. temporary network glitch).  Right now this just blasts away at them depending
      // on the timer period.
      for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
	peer_type & p(*peer_it);
	auto i =  p.peer_id;
	if (i != my_cluster_id() && last_log_entry_index() > p.match_index_) {
	  // This is next log entry the peer needs
	  uint64_t previous_log_index = p.next_index_;
	  if (p.next_index_ < log_start_index()) {
	    // We have checkpointed a portion of the log the peer requires
	    // we must send a checkpoint instead and then we can apply log entries.
            BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ 
                                    << " peer " << i << " requires log entries starting at index " << p.next_index_
                                    << " but log starts at " << log_start_index() << ".  Sending checkpoint.";
	    send_checkpoint_chunk(clock_now, i);
	    continue;
	  }

	  // Last time we sent to the peer it was at this term
	  uint64_t previous_log_term = 0;
	  if (previous_log_index > log_start_index()) {
	    // We've still got the entry so grab the actual term
	    previous_log_term = log_.term(previous_log_index-1);
	  } else if (previous_log_index == 0) {
	    // First log entry.  No previous log term.
	    previous_log_term = 0;
	  } else if (previous_log_index == this->last_checkpoint_index()) {
	    // Boundary case: Last log entry sent was the last entry in the last checkpoint
	    // so we know what term that was
	    previous_log_term = this->last_checkpoint_term();
	  } else {
	    // How do we get here??? 
	    // This is what we can conclude:
	    // 1) p.next_index_ = log_start_index() (checked via 2 if statements above)
	    // 2) log_start_index() <= this->last_checkpoint_index() (invariant; can't throw away log that isn't checkpointed)
	    // 3) p.next_index_ != this->last_checkpoint_term()
	    // This implies p.next_index_ < this->last_checkpoint_index().
	    // So the following can get us here:  checkpoint  [0,p.next_index_) then truncate the
	    // log prefix to make log_start_index()=p.next_index_.  Without updating p.next_index_, take another checkpoint strictly past p.next_index_
	    // and DON'T truncate the prefix (e.g. keep it around precisely to avoid having to send checkpoints :-)
	    // In any case, sending checkpoint seems to be the right response as we can't determine the previously sent
	    // term since the second checkpoint could be at a different term than the one that truncated up to p.next_index_.
	    send_checkpoint_chunk(clock_now, i);
	    continue;
	  }

          if (p.scheduler_.can_send(clock_now, last_log_entry_index())) {
            BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
              " sending append_entry_request to peer " << i << "; request_id=" << request_id_ << " last_log_entry_index()=" << last_log_entry_index() <<
              " peer.match_index=" << p.match_index_ << "; peer.next_index_=" << p.next_index_;

            // Are p.next_index_ and previous_log_index equal at this point?
            // TODO: The answer appears to be yes and it is just confusing to be using both of them
            // at this point.  Pick one or the other.
            //
            BOOST_ASSERT(p.next_index_ == previous_log_index);

            // // TODO: For efficiency avoid sending actual data in messages unless p.is_next_index_reliable_ == true
            uint64_t log_entries_sent = (uint64_t) (last_log_entry_index() - p.next_index_);
            comm_.append_entry_request(p.peer_id, p.address, request_id_, i, current_term_, my_cluster_id(), previous_log_index, previous_log_term,
                               std::min(last_committed_index_, previous_log_index+log_entries_sent),
                               log_entries_sent,
                               [this, &p](uint64_t i) -> const log_entry_type & {
                                 return this->log_.entry(p.next_index_ + i);
                               });
            p.requires_heartbeat_ = new_heartbeat_timeout(clock_now);
          } else {
            BOOST_LOG_TRIVIAL(trace) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
              " can't send append_entry_request to peer " << i << " for range [" << p.next_index_ << ","
                                     << last_log_entry_index() << ") for " << std::chrono::duration_cast<std::chrono::milliseconds>(p.scheduler_.retransmit_timeout()-clock_now).count()
                                     << " milliseconds";
          }
	}  else if (i != my_cluster_id()) {
          BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_
                                   << " peer " << i << " is up to date with log at " << p.match_index_;
        }
      }
    }

    void send_heartbeats(bool force, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
	peer_type & p(*peer_it);
	if (p.peer_id != my_cluster_id() && (force || p.requires_heartbeat_ <= clock_now)) {
	  BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " sending empty append_entry_request to peer " << p.peer_id << " as heartbeat with request id " << request_id_;
	  comm_.append_entry_request(p.peer_id, p.address, request_id_, p.peer_id, current_term_, my_cluster_id(), 0, 0,
			     0,
			     0,
			     [this, &p](uint64_t i) {
			       // Should not be called
			       return this->log_.entry(0);
			     });
	  p.requires_heartbeat_ = new_heartbeat_timeout(clock_now);
	}
      }
    }

    void send_checkpoint_chunk(std::chrono::time_point<std::chrono::steady_clock> clock_now, uint64_t peer_id)
    {
      peer_type & p(peer_from_id(peer_id));

      // Internal error if we call this method without a checkpoint to send
      BOOST_ASSERT(this->last_checkpoint());

      // TODO: Should we arrange for abstractions that permit use of sendfile for checkpoint data?  Manual chunking
      // involves extra work and shouldn't really be necessary (though the manual chunking does send heartbeats).
      if (!p.checkpoint_) {
	const auto & header(configuration_.get_checkpoint());
	BOOST_ASSERT(this->last_checkpoint_index() == checkpoint_header_traits_type::last_log_entry_index(&header));
	BOOST_ASSERT(this->last_checkpoint_term() == checkpoint_header_traits_type::last_log_entry_term(&header));
	BOOST_ASSERT(this->last_checkpoint_cluster_time() == checkpoint_header_traits_type::last_log_entry_cluster_time(&header));
	p.checkpoint_.reset(new peer_checkpoint_type(header, this->last_checkpoint()));
      }

      // Are we waiting for a response to a previous chunk?  If so and sent within the last second then
      // keep waiting for a response.
      if (p.checkpoint_->awaiting_ack_ && p.checkpoint_->last_block_sent_time_ + std::chrono::seconds(1) > clock_now) {
	return;
      }

      // Did we already finish the checkpoint? 
      if (p.checkpoint_->data_->is_final(p.checkpoint_->last_block_sent_) &&
          (!p.checkpoint_->awaiting_ack_ || p.checkpoint_->last_block_sent_time_ + std::chrono::seconds(1) > clock_now)) {
	return;
      }

      // Move to next chunk and send
      p.checkpoint_->last_block_sent_ = p.checkpoint_->data_->block_at_offset(p.checkpoint_->checkpoint_next_byte_);

      comm_.append_checkpoint_chunk_request(peer_id, p.address,
				    request_id_,
				    peer_id,
				    current_term_,
				    my_cluster_id(),
				    p.checkpoint_->checkpoint_last_header_,
				    p.checkpoint_->data_->block_begin(p.checkpoint_->last_block_sent_),
				    p.checkpoint_->data_->block_end(p.checkpoint_->last_block_sent_),
				    p.checkpoint_->data_->is_final(p.checkpoint_->last_block_sent_),
				    slice(p.checkpoint_->last_block_sent_.block_data_,
					  p.checkpoint_->last_block_sent_.block_length_));
      p.checkpoint_->awaiting_ack_ = true;
      p.checkpoint_->last_block_sent_time_ = clock_now;

      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" sending append_checkpoint_chunk_request to peer " << peer_id << "; log_start_index()=" << log_start_index() <<
	" last_log_entry_index()=" << last_log_entry_index() <<
	" peer.match_index=" << p.match_index_ << " peer.next_index_=" << p.next_index_ <<
	" checkpoint.last_log_entry_index=" << checkpoint_header_traits_type::last_log_entry_index(&p.checkpoint_->checkpoint_last_header_) <<
	" checkpoint.last_log_entry_term=" << checkpoint_header_traits_type::last_log_entry_term(&p.checkpoint_->checkpoint_last_header_) <<
	" checkpoint.last_log_entry_cluster_time=" << checkpoint_header_traits_type::last_log_entry_cluster_time(&p.checkpoint_->checkpoint_last_header_) <<
	" bytes_sent=" << (p.checkpoint_->data_->block_end(p.checkpoint_->last_block_sent_) -
			   p.checkpoint_->data_->block_begin(p.checkpoint_->last_block_sent_)) <<
        " byte_range=[" << p.checkpoint_->data_->block_begin(p.checkpoint_->last_block_sent_) << "," <<
        p.checkpoint_->data_->block_end(p.checkpoint_->last_block_sent_) << ")" <<
        " checkpoint_done=" << (p.checkpoint_->data_->is_final(p.checkpoint_->last_block_sent_) ? "true" : "false");

      p.requires_heartbeat_ = new_heartbeat_timeout(clock_now);
    }

    // Based on log sync and/or append_entry_response try to advance the commit point.
    void try_to_commit(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      if (state_ != LEADER) {
	return;
      }

      // Committed index is relative to a configuration...
      uint64_t committed = configuration().get_committed(last_synced_index_);
      if (last_committed_index_ >= committed) {
	// Nothing new to commit
	// TODO: Can committed go backwards (e.g. on a configuration change)?  
	return;
      }

      // If we checkpointed uncommitted log that is VERY BAD
      BOOST_ASSERT(committed > log_start_index());

      if (log_.term(committed-1) != current_term_) {
	// See section 3.6.2 of Ongaro's thesis for why we do not directly commit entries at previous terms;
        // they will be committed indirectly when the commits subsequent entries at its current term.
	BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " has majority vote on last log entry at index " << (committed-1) <<
	  " and term " << log_.term(committed-1) << " but cannot directly commit log entry from previous term";
	return;
      }

      // This is the first commit of our term, which unblocks processing read only queries
      bool is_first_commit_of_term = last_committed_index_ <= leader_read_only_commit_fence_ && committed != last_committed_index_;
      
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" committed log entries [" << last_committed_index_ <<
	"," << committed << ")";
      update_last_committed_index(committed);

      // Now try to apply any newly committed log entries
      apply_log_entries();

      if (is_first_commit_of_term) {
        update_last_request_id_quorum();
      }
      
      // TODO: Check use of >= vs. >
      if (state_ == LEADER && last_committed_index_ > configuration().configuration_id()) {
	if (!configuration().includes_self()) {
	  // We've committed a new configuration that doesn't include us anymore so give up
	  // leadership
	  if (can_become_follower_at_term(current_term_+1)) {
	    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " is LEADER and committed log entry with configuration that does not include me.  Becoming FOLLOWER";
	    become_follower(current_term_+1, clock_now);
	    BOOST_ASSERT(log_header_sync_required_);
	    if (log_header_sync_required_) {
	      log_.sync_header();
	    }
	    // TODO: Anything else to do here?  Do we need a continuation for the log header sync?
	    return;
	  } else {
	    // TODO:  What do I do here???  This should never happen because I was leader.
	    BOOST_ASSERT(false);
	  }
	}

	if (configuration().is_transitional()) {
	  // Log the new stable configuration and get rid of old servers
	  auto indices = log_.append(log_entry_traits_type::create_configuration(current_term_, cluster_clock_.advance(clock_now), configuration().get_stable_configuration()));
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " committed transitional configuration at index " << configuration().configuration_id() << 
	    ".  Logging new stable configuration at index " << indices.first;
	  // Should will move from TRANSITIONAL to STABLE
	  configuration_.add_logged_description(indices.first, log_.entry(indices.first), clock_now);
	  BOOST_ASSERT(configuration().is_stable());
	}
      }
    }

    // Actually update the commit point and execute any necessary actions
    void update_last_committed_index(uint64_t committed)
    {
      last_committed_index_ = committed;

      if (configuration().is_transitional_initiator() && last_committed_index_ > configuration().configuration_id()) {
	// Respond to the client that we've succeeded
	BOOST_ASSERT(config_change_client_ != nullptr);
	config_change_client_->on_configuration_response(messages_type::client_result_success());
	config_change_client_ = nullptr;
      }
    }

    const char * log_entry_type_string(log_entry_const_arg_type e)
    {
      if (log_entry_traits_type::is_command(e)) {
        return "COMMAND";
      } else if (log_entry_traits_type::is_configuration(e)) {
        return "CONFIGURATION";
      } else if (log_entry_traits_type::is_noop(e)) {
        return "NOOP";
      } else {
        return "UNKNOWN";
      }
    }

    void apply_log_entries()
    {
      if (last_applied_index_ < last_committed_index_) {
        BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
          " initiating application of log entry " << last_applied_index_ << " of type " <<
          log_entry_type_string(&log_.entry(last_applied_index_)) << " to state machine";
        state_machine_(&log_.entry(last_applied_index_), last_applied_index_, leader_id_);
      }
    }

    // Append Entry processing
    void internal_append_entry_request(append_entry_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      typedef append_entry_request_traits_type ae;
      auto req_leader_id = ae::leader_id(req);
      auto req_term_number = ae::term_number(req);
      auto req_id = ae::request_id(req);

      BOOST_LOG_TRIVIAL(trace) << "[internal_append_entry_request] Server(" << my_cluster_id() << ") at term " << current_term_
			       << " processing append_entry_request from  Server(" << req_leader_id
			       << ") at term " << req_term_number
			       << " for log entries ["
			       << ae::previous_log_index(req) << "," << ae::previous_log_index(req)+ae::num_entries(req) << ")";
      
      // TODO: Is this really true?  For example, if a log header sync takes a really long time and I don't hear
      // from leader is it possible to time out and transition to CANDIDATE?  I don't think so because we currently
      // suppress transitions to CANDIDATE when a header sync is outstanding
      if (req_term_number != current_term_ && state_ != FOLLOWER) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " and state " << state_string(state_) << " processing append entry and expected to be at term " <<
	  req_term_number << " and state FOLLOWER";
	BOOST_ASSERT(req_term_number == current_term_ && state_ == FOLLOWER);
      }

      // May be the first append_entry_request for this term; that's when we learn
      // who the leader actually is.
      if (leader_id_ == INVALID_PEER_ID) {
	leader_id_ = req_leader_id;
      } else {
	BOOST_ASSERT(leader_id_ == req_leader_id);
      }

      // TODO: Finish

      // Do we have all log entries up to this one?
      if (ae::previous_log_index(req) > last_log_entry_index()) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " received append entry with gap.  req.previous_log_index=" << ae::previous_log_index(req) <<
	  " last_log_entry_index()=" << last_log_entry_index();
	comm_.append_entry_response(req_leader_id, get_peer_from_id(req_leader_id).address,
				    my_cluster_id(),
				    current_term_,
				    req_term_number,
                                    req_id,
				    last_log_entry_index(),
				    last_log_entry_index(),
				    false);
	BOOST_LOG_TRIVIAL(trace) << "[internal_append_entry_request] Exiting";
	return;
      }

      // Do the terms of the last log entry agree?  By the Log Matching Property this will guarantee
      // the logs of the leader that is appending will be identical at the point we begin appending.
      // See the extended Raft paper Figure 7 for examples of how this can come to pass.
      if (ae::previous_log_index(req) > log_start_index() &&
	  ae::previous_log_term(req) != log_.term(ae::previous_log_index(req)-1)) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " received append entry with mismatch term.  req.previous_log_index=" << ae::previous_log_index(req) <<
	  " req.previous_log_term=" << ae::previous_log_term(req) <<
	  " log_.entry(" << (ae::previous_log_index(req)-1) << ").term=" << log_.term(ae::previous_log_index(req)-1);
	comm_.append_entry_response(req_leader_id, get_peer_from_id(req_leader_id).address,
				    my_cluster_id(),
				    current_term_,
				    req_term_number,
                                    req_id,
				    ae::previous_log_index(req),
				    ae::previous_log_index(req),
				    false);
	BOOST_LOG_TRIVIAL(trace) << "[internal_append_entry_request] Exiting";
	return;
      }

      // Detect duplicate log entry
      // Here we guard against
      // 1) Getting a duplicate that causes index to go backwards (and would
      // cause truncation of already committed/accepted entries if not guarded).
      // 2) Getting an entry, successfully committing and then getting a duplicate
      // and getting a failure when trying to commit the duplicate (corrupting an already
      // committed entry).

      std::size_t it = 0;
      std::size_t entry_end = ae::num_entries(req);
      uint64_t entry_log_index = ae::previous_log_index(req);
      // Scan through entries in message to find where to start appending from
      for(; it!=entry_end; ++it, ++entry_log_index) {
	// TODO: log_start_index() isn't valid if log is empty, I think we need to check for that here
	if (entry_log_index < log_start_index()) {
	  // This might happen if a leader sends us an entry we've already checkpointed
	  continue;
	}
	if (last_log_entry_index() > entry_log_index) {
	  if (log_.term(entry_log_index) == log_entry_traits_type::term(&ae::get_entry(req, it))) {
	    // We've already got this log entry, keep looking for something new.
	    continue;
	  }
	  // We've got some uncommitted cruft in our log.  Truncate it and then start appending with
	  // this entry.
	  // At this point we know that the leader log differs from what we've got so get rid of things
	  // from here to the end
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " truncating uncommitted log entries [" << entry_log_index <<
	    "," << last_log_entry_index() << ") starting at term " << log_.term(entry_log_index) <<
	    ".  Committed portion of log is [0," << last_committed_index_ << ")";
	  BOOST_ASSERT(last_committed_index_ <= entry_log_index);
	  log_.truncate_suffix(entry_log_index);
          if (last_synced_index_ > entry_log_index) {
            last_synced_index_ = entry_log_index;
          }
	  // If we are truncating a transitional configuration then we have learned that an attempt to set
	  // configuration has failed.  We need to tell the client this and reset to the prior stable configuration.
	  // TODO: Is this necessarily only going to be true where the config change originated?  E.g. couldn't I be a
	  // FOLLOWER that go a transitional config that didn't get committed?
	  if(configuration().is_transitional() && configuration().configuration_id() >= entry_log_index) {
	    BOOST_ASSERT(config_change_client_ != nullptr);
	    config_change_client_->on_configuration_response(messages_type::client_result_fail());
	    config_change_client_ = nullptr;
	  }
	  configuration_.truncate_suffix(entry_log_index, clock_now);
	  // TODO: Something with configurations.  Anything other than truncate the state in the manager?
	  break;
	} else {
	  // A new entry; start appending from here.
	  break;
	}
      }

      // Update committed id
      // TODO: Do we need to wait for log flush to update this?  I don't see why since the leader has determined
      // this value (possibly without our vote).  On the other hand there might be a problem if we allow a state machine
      // to have entries applied without having them be persistent yet; I think this concern would be valid if the state machine was
      // persisting recoverable state outside of the Raft checkpoint.  I think a state machine persisted data outside of the
      // checkpoint then it would need database-like functionality to rollback such changes on startup when it realizes that
      // the committed part of its log is behind (meaning compensation info would have to be persisted outside the checkpoint as well).
      // In any case, Ongaro makes it clear that there is no need to sync this to disk here.
      // This is the first commit of our term, which unblocks processing read only queries
      bool is_first_commit_of_term = last_committed_index_ <= leader_read_only_commit_fence_ && ae::leader_commit_index(req) != last_committed_index_;
      
      if (last_committed_index_ <ae::leader_commit_index(req)) {
	update_last_committed_index(ae::leader_commit_index(req));
      }

      // Append to the log as needed
      uint64_t entry_end_index = entry_log_index;
      if (it != entry_end) {
	BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " appending log entries [" << entry_log_index <<
	  "," << (entry_log_index + (entry_end-it)) << ") starting at term " << log_entry_traits_type::term(&ae::get_entry(req, it));

	entry_end_index += (entry_end-it);
	// This is a wee bit ugly, append each log entry in turn, the first N-1 with a NOOP deleter and the last
	// that frees the append_entry_request message which holds all of the memory
	uint64_t idx = entry_log_index;
	for(std::size_t i=it; i<entry_end; ++i, ++idx) {
	  // Make sure any configuration entries added are reflected in the configuration manager.
	  if (log_entry_traits_type::is_configuration(&ae::get_entry(req, it))) {
	    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " received new configuration at index " << idx;
	    configuration_.add_logged_description(idx, ae::get_entry(req, it), clock_now);
	  }
	  // Append to the log
	  std::pair<log_index_type, log_index_type> range;
	  if (i+1 < entry_end) {
	    range = log_.append(std::make_pair(&ae::get_entry(req, i), raft::util::call_on_delete()));
	  } else {
	    // Transfer ownership of input message to the log
	    auto entry = &ae::get_entry(req, i);
	    raft::util::call_on_delete final_deleter([r = std::move(req)](){});
	    range = log_.append(std::make_pair(entry, std::move(final_deleter)));
	  }
	  if (i == it) {
	    BOOST_ASSERT(range.first == entry_log_index);
	  }
	}
        cluster_clock_.set(log_.last_entry_cluster_time(), clock_now);
      } else {
	// Nothing to append.  We're not returning here because we want to tell leader that we've got all
	// the entries but we may need to wait for a log sync before doing so (e.g. an overly aggressive leader
	// could try to send us entries before our disk sync has occurred).
	BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " received append_entry_request message from " << req_leader_id << " for range [" << ae::previous_log_index(req) <<
	  "," << (ae::previous_log_index(req) + ae::num_entries(req)) << ") but already had all log entries";
      }

      // Log entries in place, now try to apply the committed ones
      apply_log_entries();

      if (is_first_commit_of_term) {
        update_last_request_id_quorum();
      }
      
      // N.B. The argument req is no longer valid at this point. It has either been free or has ownership has been passed to the
      // log

      // Common case here is that we've got new entries that must be synced to disk before we respond to
      // the leader.  It is also possible that we already had everything the leader had and that it is already
      // on disk; if that is the case we can respond to the leader right now with success.
      if (entry_end_index > last_synced_index_) {
	BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " last_synced_index_=" << last_synced_index_ <<
          ", waiting for log sync before sending append_entry_request response:  request_term_number=" << req_term_number <<
	  " request_id=" << req_id <<
	  " begin_index=" << entry_log_index <<
	  " last_index=" << entry_end_index;
	// Create a continuation for when the log is flushed to disk
	// TODO: Better to use lambdas for the continuation?  That way we can include the code right here.
	// The thing is that I don't know how to get the type of the corresponding lambda to store it.  I should
	// take a gander at how ASIO handles a similar issue.
	append_entry_continuation cont;
	cont.leader_id = req_leader_id;
	cont.begin_index = entry_log_index;
	cont.end_index = entry_end_index;
	cont.term = req_term_number;
	cont.request_id = req_id;
	append_entry_continuations_.insert(std::make_pair(cont.end_index, cont));
      } else {
	// TODO: Can I respond to more than what was requested?  Possibly useful idea for pipelined append_entry_request
	// requests in which we can sync the log once for multiple append_entries.
	BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " sending append_entry response:  request_term_number=" << req_term_number <<
	  " request_id=" << req_id <<
	  " begin_index=" << entry_log_index <<
	  " last_index=" << last_synced_index_ <<
          " success=true";
	comm_.append_entry_response(req_leader_id, get_peer_from_id(req_leader_id).address,
				    my_cluster_id(),
				    current_term_,
				    req_term_number,
				    req_id,
				    entry_log_index,
				    last_synced_index_,
				    true);
      }

      // Question: When do we flush the log?  Presumably before we respond since the leader takes our
      // response as durable. Answer: Yes.
      // Question: When do we apply the entry to the state machine?  Do we need to wait until the leader informs us this
      // entry is committed?  Answer: Yes.  Oddly that could happen before our disk has synced (if our disk is running really slowly
      // compared to the rest of the cluster).
      BOOST_LOG_TRIVIAL(trace) << "[internal_append_entry_request] Exiting";
    }

    // Append Checkpoint Chunk processing
    void internal_append_checkpoint_chunk_request(append_checkpoint_chunk_request_arg_type && req)
    {
      typedef append_checkpoint_chunk_request_traits_type acc;
      BOOST_ASSERT(acc::term_number(req) == current_term_ && state_ == FOLLOWER);

      // May be the first message for this term; that's when we learn
      // who the leader actually is.
      if (leader_id_ == INVALID_PEER_ID) {
	leader_id_ = acc::leader_id(req);
      } else {
	BOOST_ASSERT(leader_id_ == acc::leader_id(req));
      }

      // This is a bit unpleasant.   If this req initiates a checkpoint then
      // we have to transfer ownership of it to the checkpointer via
      // an opaque deleter.   Just in case we have to unpack all of the required internal
      // details here for processing.
      auto req_checkpoint_done = acc::checkpoint_done(req);
      auto req_leader_id = acc::leader_id(req);
      auto req_request_id = acc::request_id(req);
      
      auto ret = this->write_checkpoint_chunk(std::move(req));

      // N.B. req might be invalid at this point

      // If checkpoint done then we should not be told to send a response.
      BOOST_ASSERT(!(req_checkpoint_done && ret.second));

      if (ret.second) {
        comm_.append_checkpoint_chunk_response(req_leader_id, get_peer_from_id(req_leader_id).address,
                                               my_cluster_id(),
                                               current_term_,
                                               current_term_,
                                               req_request_id,
                                               ret.first);
      }
    }

    // This temporary transition to from private to public is a temporary expedient that
    // intends to make diffs for a major refactorization easier to read.   There will be a follow on
    // commit which is purely for moving code around.
  public:
    void set_checkpoint_header(const checkpoint_header_type & header,
                               std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      auto last_index = checkpoint_header_traits_type::last_log_entry_index(&header);
      auto last_term = checkpoint_header_traits_type::last_log_entry_term(&header);
      auto last_cluster_time = checkpoint_header_traits_type::last_log_entry_cluster_time(&header);

      // How to handle the state machine?  We just learned of commits that are incorporated into the
      // checkpoint state, but we won't get the log entries corresponding to those commits, we can't call apply_log_entries() here.
      // TENTATIVE ANSWER: Checkpoint was loaded into the state machine so we only have to update last_applied_index_ to
      // the last log entry of the checkpoint.   Given that we do this before calling update_last_committed_index, we should be
      // able to refold apply_log_entries() into update_last_commit_index().
      last_applied_index_ = last_index;

      // Anything checkpointed must be committed so we learn of commits this way
      // TODO: What if we get asked to load a checkpoint while we still have
      // configuration().is_transitional_initiator() == true?  Should we wait until
      // after handling the log and configuration before updating the commit index?
      // I suppose it is possible that a configuration that this server initiated gets
      // committed as part of a checkpoint received after losing leadership?
      if(last_committed_index_ < last_index) {
	update_last_committed_index(last_index);
      }

      // TODO: Now get the log in order.  If my log is consistent with the checkpoint then I
      // may want to keep some of it around.  For example, if I become leader then I may need to
      // bring some peers up to date and it is likely cheaper to send them some log entries
      // rather than a full checkpoint.  If my log is shorter than the range covered by the checkpoint
      // then my log is useless (I can't bring anyone fully up to date and will have to resort to the
      // checkpoint).  If my log is at least as long the checkpoint
      // range but the term at the end of the checkpoint range doesn't match the checkpoint term
      // then I've got the wrong entries in the log and I should trash the entire log as well.
      if (last_log_entry_index() < last_index ||
	  (log_start_index() < last_index &&
	   log_.term(last_index-1) != last_term)) {
        // Now safe to truncate
	log_.truncate_prefix(last_index);
	log_.truncate_suffix(last_index);
	configuration_.truncate_prefix(last_index, clock_now);
	configuration_.truncate_suffix(last_index, clock_now);

        if (last_synced_index_ > last_index) {
          last_synced_index_ = last_index;
        }

        // Log is emptied so cluster time needs to be reset to the cluster time of the
        // checkpoint itself
        cluster_clock_.set(last_cluster_time, clock_now);
      
	// TODO: What's up with the log sync'ing logic here????
	// logcabin waits for the log to sync when FOLLOWER (but not when
	// LEADER).  logcabin assumes that all log writes are synchronous in a FOLLOWER
	// but I'm not sure I think that needs to be assumed (just that we sync the
	// log before responding to LEADER about append_entries).
      }

      configuration_.set_checkpoint(header, clock_now);
    }

  private:
    // Guards for state transitions.  Most of the cases in which I can't make a transition
    // it is because the transition requires a disk write of the header and such a write is already in
    // progress.  If we have a sufficiently slow disk we are essentially dead!
    bool can_become_follower_at_term(uint64_t term)
    {
      if (term > current_term_ && log_header_sync_required_) {
	// I need to advance term but I'm already trying to do that and have an outstanding
	// disk write.  Since I can't cancel the current write I am going to have to wait to
	// issue another.
	return false;
      } else {
	return true;
      }
    }

    ////////////////////////////
    ////////////////////////////
    // State Transitions
    ////////////////////////////
    ////////////////////////////
    void become_follower(uint64_t term, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      BOOST_ASSERT(term >= current_term_);
      if (term > current_term_) {
	BOOST_ASSERT(can_become_follower_at_term(term));
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " becoming FOLLOWER at term " << term;
	state_ = FOLLOWER;
	current_term_ = term;
	voted_for_ = nullptr;
	// We've updated current_term_ and voted_for_ ; must sync to disk before responding 
	// Don't call log_.sync_header() here.  We've set the flag and the caller needs to request the sync
	// before returning.
	log_header_sync_required_ = true;
	// ??? Do this now or after the sync completes ???
	election_timeout_ = new_election_timeout(clock_now);
	leader_id_ = INVALID_PEER_ID;
	log_.update_header(current_term_, INVALID_PEER_ID);

        // Notify listener of state change
        state_change_listener_(state_, current_term_);

        // We do NOT request the log sync here because if we became follower
        // due to vote request, the voted_for_ part of the header is potentially going
        // to be updated.  We want to wait for that information before the sync is requested.
        // That makes is really dangerous that some one forgets to issue the request.

	// Cancel any pending configuration change
	configuration().reset_staging_servers(clock_now);

	// Cancel any in-progress checkpoint
	this->abandon_checkpoint();

        // Clear the read only fence in case we were leader
        leader_read_only_commit_fence_ = std::numeric_limits<uint64_t>::max();

        // Cancel any read only queries
        for(auto & f : request_id_quorum_continuations_) {
          f.second(messages_type::client_result_not_leader());
        }
        request_id_quorum_continuations_.clear();
        for(auto & c : state_machine_applied_continuations_) {
          for(auto & f : c.second) {
            f(messages_type::client_result_not_leader());
          }
        }
        state_machine_applied_continuations_.clear();
      } else if (state_ != FOLLOWER) {
        // If we are LEADER then can't become FOLLOWER at same term, so must be CANDIDATE
        BOOST_ASSERT(state_ == CANDIDATE);
        BOOST_ASSERT(0 == request_id_quorum_continuations_.size());
        BOOST_ASSERT(0 == state_machine_applied_continuations_.size());
        BOOST_ASSERT(leader_read_only_commit_fence_ == std::numeric_limits<uint64_t>::max());
        // TODO: We cancel checkpoint when becoming CANDIDATE, is it worth adding a method to server_checkpoint
        // just to be able to assert that there isn't an in-progress checkpoint?
        BOOST_ASSERT(can_become_follower_at_term(term));
        BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
          " becoming FOLLOWER at term " << term;
        state_ = FOLLOWER;
      }

      // TODO: If transitioning from leader there may be a log sync to disk
      // that we need to issue?  LogCabin does this but I'm not sure that I see why that it is needed from the protocol perspective.
      // What is required is that the FOLLOWER sync before ack'ing any append entries.
      // Do that now after waiting for any in-progress flush to complete.

      // TODO: If we are transitioning from leader there may be some pending client requests.  What to
      // do about them???  Probably no harm is just letting them linger since the requests are committed.
    }

    void become_candidate(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      BOOST_ASSERT(state_ != LEADER);
      if (!configuration().is_valid()) {
	// No configuration yet, we just wait for one to arrive
	election_timeout_ = new_election_timeout(clock_now);
	return;
      }
    
      if (!log_header_sync_required_) {
	BOOST_ASSERT(no_log_header_sync_continuations_exist());
	BOOST_ASSERT(!send_vote_requests_);
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " becoming CANDIDATE and starting election at term " << (current_term_+1) <<
	  ".  Syncing log header.";

	// Cancel any in-progress checkpoint
	this->abandon_checkpoint();      

        // Clear the read only fence in case we were leader
        leader_read_only_commit_fence_ = std::numeric_limits<uint64_t>::max();
        
        // Cancel any read only queries
        for(auto & f : request_id_quorum_continuations_) {
          f.second(messages_type::client_result_not_leader());
        }
        request_id_quorum_continuations_.clear();
        for(auto & c : state_machine_applied_continuations_) {
          for(auto & f : c.second) {
            f(messages_type::client_result_not_leader());
          }
        }
        state_machine_applied_continuations_.clear();

	current_term_ += 1;
	state_ = CANDIDATE;
	election_timeout_ = new_election_timeout(clock_now);
	voted_for_ = &self();
        leader_id_ = INVALID_PEER_ID;
	// Log and flush
	log_header_sync_required_ = true;
	log_.update_header(current_term_, voted_for_->peer_id);
	log_.sync_header();
	// Set flag so that vote requests get sent once the log header sync completes (this
	// flag is basically a continuation).  TODO: Do we really need this flag or should this be
	// based on checking that state_ == CANDIDATE after the log header sync is done (in on_log_header_sync)?
	send_vote_requests_ = true;
        // Notify listener of state change
        state_change_listener_(state_, current_term_);
      } else {
	// TODO: Perhaps make a continuation here and make the candidate transition after the sync?
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " can't become CANDIDATE at term " << (current_term_+1) << " due to outstanding log header sync";
      }  
    }

    void become_candidate_on_log_header_sync(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      if (send_vote_requests_) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " is CANDIDATE and log header has synced.  Sending vote requests.";

	send_vote_requests_ = false;
	// If not a candidate then presumably shouldn't send out vote requests.
	// TODO: On the other hand, if I am a candidate then is it even possible that
	// send_vote_requests_ = false?  We know that setting state_=CANDIDATE implies
	// send_vote_requests_=true, so the question is whether after becoming a CANDIDATE
	// is it possible to trigger multiple log header syncs
	if (state_ == CANDIDATE) {
	  send_vote_requests();
	  // Special case: single server cluster become_leader without any votes received
	  if (has_quorum()) {
	    become_leader(clock_now);
	  }
	}
      }
    }

    void become_leader(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" becoming LEADER";
      BOOST_ASSERT(!log_header_sync_required_);
      state_ = LEADER;
      leader_id_ = my_cluster_id();
      // TODO: Should we set the election timeout to infinity?
      election_timeout_ = new_election_timeout(clock_now);

      // Notify listener of state change
      state_change_listener_(state_, current_term_);
      
      // TODO: Make this relative to the configuration.  I do think this is already correct; send out to all known servers.
      // Send out empty append entries to assert leadership
      for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
	peer_type & p(*peer_it);
	if (p.peer_id != my_cluster_id()) {
	  p.requires_heartbeat_ = clock_now;
	  // Guess that peer is up to date with our log; this may be optimistic.  We'll avoid chewing up bandwidth until
	  // we hear back from the peer about it's last log entry.
	  p.next_index_ = last_log_entry_index();
	  p.is_next_index_reliable_ = false;
	  p.match_index_ = 0;
          p.scheduler_.init(clock_now);
	  p.checkpoint_.reset();
	} else {
	  // TODO: Is this really guaranteed to be true (i.e. the last log entry is synced)?
	  // We do know that the log header has been synced, should that also imply a log sync?
	  // In LogCabin I believe this is always true because all log writes in a non-leader are synchronous.
	  // It is also true that in LogCabin the last_synced_index isn't updated when not leader so in fact it
	  // is necessary to update its value when one becomes leader because it must be assumed stale;
	  // maybe the sync isn't necessary just being able to know the correct value necessary (and  that
	  // happens to be the end of the log)?
	  // Perhaps we have to wait for the log to sync before we proceed?
	  // My current understanding is that there isn't any need for the leader log to be sync'd
	  // last_synced_index_ = last_log_entry_index();
	}
      }

      // Append a new log entry in order to get the committed index updated. 
      // The empty log entry will also trigger append_entry_request messages to the peers to let them know we have
      // become leader.  The latter could also be achieved with heartbeats, but would it get the commit index advanced (I think the answer is no
      // a heartbeat response will not wait for a log sync; there could be a log sync outstanding too using the synced index from heartbeat response
      // could result in an underestimate of the committed index if the log sync completes after the heartbeat response is sent)?
      // The main reason for the NOOP instead of heartbeat is that entries from a previous term are not committed directly by counting how many
      // servers have received them; they are only committed indirectly by committing entries from the current term on the LEADER
      // (see Section 3.6.2 from Ongaro's thesis).   That still begs the question of why we force the issue of committing
      // those old entries right now?
      // Knowing the last committed index in the cluster also allows us to guarantee linearizable semantics for
      // read only queries simply by verifying we are still leader via some heartbeats (see Section 6.4 of Ongago's thesis
      // and reference the method protocol::async_linearizable_read_only_query_fence below).
      // We note the index of this entry, because we won't process read-only queries until this entry is committed.
      auto indices = log_.append(log_entry_traits_type::create_noop(current_term_, cluster_clock_.advance(clock_now)));
      leader_read_only_commit_fence_ = indices.first;
      send_append_entries(clock_now);
    
      // TODO: As an optimization cancel any outstanding vote_request since they are not needed
      // we've already got quorum
    }

    void update_last_request_id_quorum()
    {
      if (request_id_quorum_continuations_.empty()) {
        return;
      }
      
      if (last_committed_index_ <= leader_read_only_commit_fence_) {
        BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_
                                 << " commit index at " << last_committed_index_
                                 << " has not yet committed read only query fence at index " << leader_read_only_commit_fence_
                                 << " cannot yet process linearizable read only queries.";

        return;
      }
      
      // Acknowledged request id  is relative to a configuration...
      uint64_t acknowledged_request_id = configuration().get_last_request_id(request_id_);
      
      BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_
                               << " updating last request id min quorum request_id_ " << request_id_
                               << " acknowledged_request_id " << acknowledged_request_id
                               << " commit_index_term " << commit_index_term();

        
      if (commit_index_term() == current_term_) {
        auto it = request_id_quorum_continuations_.begin();
        auto e = request_id_quorum_continuations_.upper_bound(acknowledged_request_id);
        for(; it != e; ++it) {
          // Validated that we are current leader and have up to date log
          // Now only have to wait for state machine to complete applying
          // all entries up to committed index (if not already true)
          if (last_applied_index_ == last_committed_index_) {
            BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_
                                     << " read only query at request id " << it->first
                                     << " confirmed LEADER and state machine is up to date at commit index "
                                     << last_committed_index_;
            it->second(messages_type::client_result_success());
          } else {
            BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_
                                     << " read only query at request id " << it->first
                                     << " confirmed LEADER and state machine not up to date commit index "
                                     << last_committed_index_ << " applied index " << last_applied_index_;
            state_machine_applied_continuations_[last_committed_index_].push_back(std::move(it->second));
          }
        }
        request_id_quorum_continuations_.erase(request_id_quorum_continuations_.begin(), e);
      }
    }

  public:
    protocol(communicator_type & comm, log_type & l,
	     checkpoint_data_store_type & store,
	     configuration_manager_type & config_manager,
             std::chrono::time_point<std::chrono::steady_clock> now = std::chrono::steady_clock::now())
      :
      server_checkpoint<_Messages, protocol<_Communicator, _Client, _Messages>>(store),
      comm_(comm),
      config_change_client_(nullptr),
      state_(FOLLOWER),
      election_timeout_max_(300),
      election_timeout_min_(150),
      leader_id_(INVALID_PEER_ID),
      current_term_(0),
      voted_for_(nullptr),
      log_(l),
      log_header_sync_required_(false),
      last_committed_index_(0),
      last_synced_index_(0),
      last_applied_index_(0),
      cluster_clock_(now),
      request_id_(0),
      leader_read_only_commit_fence_(std::numeric_limits<uint64_t>::max()),
      configuration_(config_manager),
      send_vote_requests_(false)
    {
      election_timeout_ = new_election_timeout(now);      

      // Default dummy state machine that synchronously completes every entry as noop
      state_machine_ = [this](log_entry_const_arg_type , uint64_t idx, size_t ) { this->on_command_applied(idx); };

      state_change_listener_ = [](state s, uint64_t current_term) { };

      // TODO: Read the log if non-empty and apply configuration entries as necessary
      // TODO: This should be async.
      for(auto idx = log_.start_index(); idx < log_.last_index(); ++idx) {
	const log_entry_type & e(log_.entry(idx));
	if (log_entry_traits_type::is_configuration(&e)) {
	  configuration_.add_logged_description(idx, e, now);
	}
      }

      if (!log_.empty()) {
        cluster_clock_.set(log_.last_entry_cluster_time(), now);
      }

      // NOTE: We set voted_for_ and current_term_ from the log header so we do not
      // sync!  This is the only place in which this should set them without a sync.
      if (INVALID_PEER_ID != log_.voted_for()) {
	voted_for_ = &configuration().peer_from_id(log_.voted_for());
      }
      current_term_ = log_.current_term();	

      // Read any checkpoint
      this->load_checkpoint(now);
    }

    ~protocol()
    {
    }

    std::size_t my_cluster_id() const
    {
      return configuration().my_cluster_id();
    }

    template<typename _Callback>
    void set_state_machine(_Callback && state_machine)
    {
      state_machine_ = std::move(state_machine);
    }

    template<typename _Callback>
    void set_state_change_listener(_Callback && listener)
    {
      state_change_listener_ = std::move(listener);
    }

    // Events
    void on_timer()
    {
      on_timer(std::chrono::steady_clock::now());
    }

    void on_timer(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      // TODO: Add checks for slow writes (log flushes and checkpoint writes).  At a miniumum we want to cry
      // for help in such situations.
      switch(state_) {
      case FOLLOWER:
	if (election_timeout_ < clock_now) {
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " FOLLOWER lost contact with leader.";
	  become_candidate(clock_now);
	}
	break;
      case CANDIDATE:
	if (election_timeout_ < clock_now) {
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " CANDIDATE timed out election.";
	  become_candidate(clock_now);
	}
	break;
      case LEADER:
	// TODO: Encapsulate this...
	if (configuration().is_staging()) {
	  if (!configuration().staging_servers_caught_up()) {
	    BOOST_ASSERT(config_change_client_ != nullptr);
	    auto bad_servers = configuration().staging_servers_making_progress();
	    if (0 < bad_servers.size()) {
	      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
		" some server in new configuration not making progress so rejecting configuration.";
	      configuration().reset_staging_servers(clock_now);
	      config_change_client_->on_configuration_response(messages_type::client_result_fail(), bad_servers);
	      config_change_client_ = nullptr;
	    } else {
	      BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
		" has servers in new configuration not yet caught up but all making progress.";
	    }
	  } else {
	    // Append a transitional configuration entry to the log
	    // and wait for it to commit before moving to stable.
	    auto indices = log_.append(log_entry_traits_type::create_configuration(current_term_, cluster_clock_.advance(clock_now), configuration().get_transitional_configuration()));
	    // This will update the value of configuration() with the transitional config we just logged
	    configuration_.add_logged_description(indices.first, log_.entry(indices.first), clock_now);
	    BOOST_ASSERT(configuration().is_transitional());
	    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " servers in new configuration caught up. Logging transitional entry at index " << indices.first;
	  }
	}
	// Send heartbeats and append_entries if needed
	send_append_entries(clock_now);
	send_heartbeats(false, clock_now);
	break;
      }
    }

    std::tuple<client_result_type, uint64_t, uint64_t> on_command(std::pair<raft::slice, raft::util::call_on_delete> && req)
    {
      return on_command(std::move(req), std::chrono::steady_clock::now());
    }
    
    std::tuple<client_result_type, uint64_t, uint64_t> on_command(std::pair<raft::slice, raft::util::call_on_delete> && req,
                                                                  std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      switch(state_) {
      case FOLLOWER:
      case CANDIDATE:
	{
          BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
            " received command while not leader";
	  // Not leader don't bother.
	  return { messages_type::client_result_not_leader(), leader_id_, current_term_ };
	}
      case LEADER:
	{
	  // Create a log entry and try to replicate it
	  // Append to the in memory log then we have to wait for
	  // the new log entry to be replicated to a majority (TODO: to disk?)/committed before
	  // returning to the client
	  // auto indices = log_.append_command(current_term_, req.get_command_data());
	  auto indices = log_.append(log_entry_traits_type::create_command(current_term_, cluster_clock_.advance(clock_now), std::move(req)));
	  // TODO: Do we want to let the log know that we'd like a flush?
	  // TODO: What triggers the sending of append_entries to peers?  Currently it is a timer
	  // but that limits the minimum latency of replication.  Is that a problem?  I suppose we
	  // could just call append_entries or on_timer here.
          BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
            " received command, replicating log entry at index " << (indices.second-1);
	  return { messages_type::client_result_success(), indices.second-1, current_term_ };
	}
      }
      return { messages_type::client_result_not_leader(), leader_id_, current_term_ };
    }

    void on_set_configuration(client_type & client, set_configuration_request_arg_type && req,
                              std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      typedef set_configuration_request_traits_type scr;
      if (LEADER != state_) {
	client.on_configuration_response(messages_type::client_result_not_leader());
	return;
      }

      if (configuration().configuration_id() != scr::old_id(req)) {
	client.on_configuration_response(messages_type::client_result_fail());
	return;
      }

      if (!configuration().is_stable()) {
	client.on_configuration_response(messages_type::client_result_fail());
	return;
      }

      BOOST_ASSERT(config_change_client_ == nullptr);
      config_change_client_ = &client;
      configuration().set_staging_configuration(scr::new_configuration(req), clock_now);
      BOOST_ASSERT(!configuration().is_stable());
    }

    void on_vote_request(vote_request_arg_type && req)
    {
      return on_vote_request(std::move(req), std::chrono::steady_clock::now());
    }
    
    void on_vote_request(vote_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      typedef vote_request_traits_type rv;
      if (rv::term_number(req) < current_term_) {
        BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
          " received vote request at lower term " << rv::term_number(req) <<
          " from candidate " << rv::candidate_id(req) << ", voting false.";
	// Peer is behind the times, let it know
	comm_.vote_response(rv::candidate_id(req), get_peer_from_id(rv::candidate_id(req)).address,
			    my_cluster_id(),
			    current_term_,
			    rv::term_number(req),
			    rv::request_id(req),
			    false);
	return;
      }

      // Remember whether a header sync is outstanding at this point.  Probably better to
      // make log_header_sync_required_ an enum rather than a bool.
      // I'm really not sure about this logic.
      // On the one hand, it's not clear that is makes sense to process a vote request at all if there is a
      // header sync outstanding (e.g. what happens if we have to update term and voted_for; they are already in flight).
      //
      // My best theory is that this logic has been hanging around since some time early in development when become_follower
      // issued a log header sync rather than just setting the flag.   The idea MAY have been that we wanted to avoid "double
      // syncing".   However, become_follower changed to not do a sync automatically because it doesn't have all of the info
      // that it needs regarding voted_for_.   Thus is now changes so simply set the flag and there is no need to suppress a sync
      // in this method (indeed it would be bad not to sync if the header is updated in this method).
      bool header_sync_already_outstanding = log_header_sync_required_;
      
      // QUESTION: Under what circumstances might we get req.term_number == current_term_ and nullptr == voted_for_?
      // ANSWER: I may have gotten a vote request from a candidate that bumped up my current_term_ but didn't have
      // all of my log entries hence didn't get my vote.
    
      // QUESTION: Is it possible for req.term_number > current_term_ but false==candidate_log_more_complete?
      // ANSWER: Yes.  Here is an example.  I am leader at current_term_, one of my followers gets disconnected 
      // and its log falls behind.  It starts a new election, connectivity returns and I get a vote_request on a new term.
    
      // Term has advanced so become a follower.  Note that we may not vote for this candidate even though
      // we are possibly giving up leadership.
      if (rv::term_number(req) > current_term_) {
	if (!can_become_follower_at_term(rv::term_number(req))) {
	  BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " cannot advance to term " << rv::term_number(req) <<
	    " due to outstanding log header sync.  Ignoring vote_request message from candidate " <<
	    rv::candidate_id(req) << ".";
	  return;
	}
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " becoming follower due to vote request from " << rv::candidate_id(req) << " at higher term " << rv::term_number(req);
	become_follower(rv::term_number(req), clock_now);
      }

      // TODO: Is it the case that every entry in a FOLLOWER log is committed?  No, since a FOLLOWER will receive log entries
      // from a LEADER that may not get quorum hence never get committed.  Therefore
      // the log completeness check is strictly stronger than "does the candidate have all my committed entries"?  How do we know that
      // there is a possible leader when this stronger criterion is enforced?  Specifically I am concerned that leadership
      // is flapping and FOLLOWER logs are getting entries in them that never get committed and this makes it hard for anyone to get votes.
      // Maybe a good way of thinking about why this isn't an issue is that we can lexicographically order peers by the pair (last_log_term, last_log_index)
      // which gives a total ordering.  During any election cycle peers maximal with respect to the total ordering are possible leaders.
      bool candidate_log_more_complete_than_mine = rv::last_log_term(req) > last_log_entry_term() ||
	(rv::last_log_term(req) == last_log_entry_term() && rv::last_log_index(req) >= last_log_entry_index());

      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_
                               << " with current vote = " << (voted_for_ != nullptr ? boost::lexical_cast<std::string>(voted_for_->peer_id) : "null")
                               << " candidate_log_more_complete_than_mine = rv::last_log_term(req) > last_log_entry_term() ||"
                               << " (rv::last_log_term(req) == last_log_entry_term() && rv::last_log_index(req) >= last_log_entry_index()) = "
                               << rv::last_log_term(req) << " > " << last_log_entry_term() << " || "
                               << "(" << rv::last_log_term(req) << " == " << last_log_entry_term() << " && " << rv::last_log_index(req)
                               << " >= " << last_log_entry_index() << ") = " << candidate_log_more_complete_than_mine;
      

      // Vote at most once in each term and only vote for a candidate that has all of my committed log entries.
      // The point here is that Raft has the property that a canidate cannot become leader unless it has all committed entries; this
      // means there is no need for a protocol to identify and ship around committed entries after the fact.
      // The fact that the check above implies that the candidate has all of my committed log entries is
      // proven in Ongaro's thesis.
      //
      // Note that in fact it is impossible for rv::term_number(req) > current_term_ at this point because
      // we have already updated current_term_ above in that case and become a FOLLOWER.  Logically this way of writing things
      // is a bit clearer to me.
      if (rv::term_number(req) >= current_term_ && candidate_log_more_complete_than_mine && nullptr == voted_for_) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " voting for " << rv::candidate_id(req) << " at to become LEADER at term " << rv::term_number(req) <<
          " as candidate has more complete log and no vote cast at this term." <<
          " header_sync_already_outstanding = " << header_sync_already_outstanding;
	become_follower(rv::term_number(req), clock_now);
	voted_for_ = &peer_from_id(rv::candidate_id(req));
	log_header_sync_required_ = true;
	election_timeout_ = new_election_timeout(clock_now);
	// Changed voted_for_ and possibly current_term_ ; propagate to log
	log_.update_header(current_term_, rv::candidate_id(req));
	// Must sync log header here because we can't have a crash make us vote twice; the
	// sync call is done below.
      }

      if (log_header_sync_required_) {
	if (!header_sync_already_outstanding) {
	  BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " syncing header due to vote request from " << rv::candidate_id(req) << " at term " << rv::term_number(req);
	  log_.sync_header();
	}
	vote_response_continuation msg;
	msg.candidate_id = rv::candidate_id(req);
	msg.term_number = current_term_;
	msg.request_term_number = rv::term_number(req);
	msg.request_id = rv::request_id(req);
	msg.granted = current_term_ == rv::term_number(req) && voted_for_ == &peer_from_id(rv::candidate_id(req));
	vote_response_header_sync_continuations_.push_back(msg);
      } else {    
	comm_.vote_response(rv::candidate_id(req), get_peer_from_id(rv::candidate_id(req)).address,
			    my_cluster_id(),
			    current_term_,
			    rv::term_number(req),
			    rv::request_id(req),
			    current_term_ == rv::term_number(req) && voted_for_ == &peer_from_id(rv::candidate_id(req)));
      }
    }

    void on_vote_response(vote_response_arg_type && resp)
    {
      on_vote_response(std::move(resp), std::chrono::steady_clock::now());
    }
    
    void on_vote_response(vote_response_arg_type && resp,
                          std::chrono::time_point<std::chrono::steady_clock> now)
    {
      typedef vote_response_traits_type vr;
      switch(state_) {
      case CANDIDATE:
	// Detect if this response if from a vote_request on a previous
	// term (e.g. we started an election, it timed out we started a new one and then we
	// got this response from the old one).  If so ignore it.
	if (current_term_ == vr::request_term_number(resp)) {
	  if (current_term_ < vr::term_number(resp)) {
	    if (can_become_follower_at_term(vr::term_number(resp))) {
	      become_follower(vr::term_number(resp), now);
	      BOOST_ASSERT(log_header_sync_required_);
	      if (log_header_sync_required_) {
		BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
		  " syncing header due to vote response from " << vr::peer_id(resp) << " at term " << vr::term_number(resp);
		log_.sync_header();
	      }
	    } else {
	      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
		" cannot advance to term " << vr::term_number(resp) <<
		" due to outstanding log header sync.  Ignoring vote_response message.";
	    }
	  } else {
	    // During voting we requested header sync when we updated the term and voted for ourselves.
	    // Vote requests didn't go out until header was synced.
	    BOOST_ASSERT(!log_header_sync_required_);
	    BOOST_ASSERT(current_term_ == vr::term_number(resp));
	    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " received vote " << vr::granted(resp) << " from peer " << vr::peer_id(resp) <<
              " in response to request_id " << vr::request_id(resp);
	    peer_type & p(peer_from_id(vr::peer_id(resp)));
            p.acknowledge_request_id(vr::request_id(resp));
	    // TODO: What happens if we get multiple responses from the same peer
	    // but with different votes?  Is that a protocol error????
	    p.vote_ = vr::granted(resp);
	    if (has_quorum()) {
	      become_leader(now);
	    }
            update_last_request_id_quorum();
	  }
	}
	break;
      case FOLLOWER:
      case LEADER:
	// Nothing to do.
	break;
      }
    }

    void on_append_entry_request(append_entry_request_arg_type && req)
    {
      on_append_entry_request(std::move(req), std::chrono::steady_clock::now());
    }
    
    void on_append_entry_request(append_entry_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      typedef append_entry_request_traits_type ae;
      if (ae::term_number(req) < current_term_) {
	// There is a leader out there that needs to know it's been left behind.
	comm_.append_entry_response(ae::leader_id(req), get_peer_from_id(ae::leader_id(req)).address, 
				    my_cluster_id(),
				    current_term_,
				    ae::term_number(req),
				    ae::request_id(req),
				    last_log_entry_index(),
				    last_log_entry_index(),
				    false);
	return;
      }

      // Should not get an append entry with current_term_ as LEADER
      // that would mean I am sending append_entry_request messages to myself
      // or multiple leaders on the same term.
      BOOST_ASSERT(ae::term_number(req) > current_term_ || state_ != LEADER);

      // I highly doubt a disk write can be reliably cancelled so we have to wait for
      // an outstanding one to complete.  Instead of queuing up just pretend the message is lost.
      // TODO: Would it be better to queue the message and wait for a log header sync?
      if (!can_become_follower_at_term(ae::term_number(req))) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " cannot advance to term " << ae::term_number(req) <<
	  " due to outstanding log header sync.  Ignoring append_entry_request message.";
	return;
      }

      // Just for checking that log header doesn't change while a header sync is outstanding.
      bool header_sync_already_outstanding = log_header_sync_required_;
      auto current_log_header_term = log_.current_term();
      auto current_log_header_voted_for = log_.voted_for();

      // At this point doesn't really matter what state I'm currently in
      // as I'm going to become a FOLLOWER.
      become_follower(ae::term_number(req), clock_now);

      // If we were FOLLOWER we didn't reset election timer in become_follower() so do
      // it here as well
      if (log_header_sync_required_) {
	// TODO: We need to put a limit on number of continuations we'll queue up
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " waiting for log header sync.  Queuing append_entry_request message from leader " <<
	  ae::leader_id(req) << " previous_log_term " << ae::previous_log_term(req) <<
	  " previous_log_index " << ae::previous_log_index(req);
	append_entry_header_sync_continuations_.push_back(std::move(req));
        if (!header_sync_already_outstanding) {
          log_.sync_header();
        } else {
          // become_follower didn't update election timeout
          election_timeout_ = new_election_timeout(clock_now);
          BOOST_ASSERT(current_log_header_term == log_.current_term() && current_log_header_voted_for == log_.voted_for());
        }
      } else {
	election_timeout_ = new_election_timeout(clock_now);
	internal_append_entry_request(std::move(req), clock_now);
      }
    }

    void on_append_entry_response(append_entry_response_arg_type && resp)
    {
      on_append_entry_response(std::move(resp), std::chrono::steady_clock::now());
    }
    
    void on_append_entry_response(append_entry_response_arg_type && resp, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      typedef append_entry_response_traits_type ae;
      if (ae::request_term_number(resp) != current_term_) {
	BOOST_ASSERT(ae::request_term_number(resp) < current_term_);
	// Discard
	return;
      }

      // I sent out an append_entries at current_term_, term hasn't changed
      // so I must be leader.
      BOOST_ASSERT(state_ == LEADER);

      if (ae::term_number(resp) > current_term_) {
        // I learning (perhaps from a heartbeat) that I am no longer leader.
	if (can_become_follower_at_term(ae::term_number(resp))) {
	  become_follower(ae::term_number(resp), clock_now);
	  // TODO: What do I do here????  Is all the processing below still relevant?
          // TODO: If so, would all of the processing below
          // have to wait until after the log header is synced?
          // Most of what is below can safely be skipped, but I do need to think
          // about what happens if a configuration change is in progress.
          BOOST_ASSERT(log_header_sync_required_);
	  if (log_header_sync_required_) {
	    log_.sync_header();
	  }
	} else {
	  // This may be impossible because if we are waiting for a log header flush
	  // for current_term_ then we should never have been leader at current_term_
	  // and thus won't be getting a response.
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " cannot become follower at term " << ae::term_number(resp) <<
	    ". Ignoring append_entry_response message";
	}
	return;
      }
      if (!configuration().is_valid_peer(ae::recipient_id(resp))) {
        BOOST_LOG_TRIVIAL(error) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
          " received append_entry_response from unknown peer " << ae::recipient_id(resp) <<
          ". Ignoring append_entry_response message";
        return;
      }
      peer_type & p(peer_from_id(ae::recipient_id(resp)));
      p.acknowledge_request_id(ae::request_id(resp));
      p.scheduler_.ack(clock_now, ae::last_index(resp));
      update_last_request_id_quorum();        
      if (ae::success(resp)) {
	if (p.match_index_ > ae::last_index(resp)) {
	  // TODO: This is unexpected if we don't pipeline append entries message,
	  // otherwise it is OK but we need more complex state around the management of
	  // what entries the peer has acknowledged (have we done enough here???)
	  BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " peer " << ae::recipient_id(resp) << " acknowledged index entries [" << ae::begin_index(resp) <<
	    "," << ae::last_index(resp) <<
	    ") but we already had acknowledgment up to index " << p.match_index_;
	} else {
          if (p.match_index_ != ae::last_index(resp)) {
            BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
              " peer " << ae::recipient_id(resp) << " acknowledged index entries [" << ae::begin_index(resp) <<
              "," << ae::last_index(resp) << ") at request id " << ae::request_id(resp);
            p.match_index_ = ae::last_index(resp);
          } else {
            BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
              " peer " << ae::recipient_id(resp) << " acknowledged index entries [" << ae::begin_index(resp) <<
              "," << ae::last_index(resp) << ") at request id " << ae::request_id(resp);
          }
	  // Now figure out whether we have a majority of peers ack'ing and can commit something
	  try_to_commit(clock_now);
	}
	// TODO: I suppose if we are pipelining append_entries to a peer then this could cause next_index_
	// to go backwards.  That wouldn't be a correctness issue merely inefficient because we'd send
	// already matched entries.  Nonetheless it is easy to prevent.
	if (p.next_index_ < p.match_index_ || !p.is_next_index_reliable_) {
	  p.next_index_ = p.match_index_;
	  p.is_next_index_reliable_ = true;
	}

	// A configuration change is underway, we are tracking how quickly the peers are accepting
	// log entries in order to decide whether we should allow the peers into a new config.
	if (p.configuration_change_) {
	  p.configuration_change_->on_append_entry_response(clock_now, p.match_index_, last_log_entry_index());
	}
      } else {
	// Peer failed processing the append_entries request (e.g. our append request would have caused
	// a gap in the peer's log or a term mismatch on the log).  Backup the next_index_ to send to this peer.  
	if (p.next_index_ > 0) {
	  p.next_index_ -= 1;
	}
	// Peer tells us about the end of its log.  In the case that we failing because of gaps this can
	// help us close the gap more quickly.  It doesn't necessarily help in case a term mismatch.
	// Things to consider:
	// How does peer.next_index_ get ahead of the peer's log???  It can happen when we become leader
	// when assume that the peer has all of our log when in fact the peer is out of date (in which case
	// is_next_index_reliable_ will be false).  TODO: Can it ever happen with is_next_index_reliable_ is true?
	// TODO: If we are pipelining append_entries then is it possible that we get an old response that causes next_index_ to
	// regress unnecessarily?  Is there anyway to detect and prevent that?
	if (p.next_index_ > ae::last_index(resp)) {
	  if (p.is_next_index_reliable_) {
	    BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " peer " << ae::recipient_id(resp) << " rejected append_entries with peer log at " << ae::last_index(resp) <<
	      " but leader had a reliable next index of  " << p.next_index_;
	  }
	  p.next_index_ = ae::last_index(resp);
	}
      }
    }

    void on_append_checkpoint_chunk_request(append_checkpoint_chunk_request_arg_type && req)
    {
      return on_append_checkpoint_chunk_request(std::move(req), std::chrono::steady_clock::now());
    }
    void on_append_checkpoint_chunk_request(append_checkpoint_chunk_request_arg_type && req, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      typedef append_checkpoint_chunk_request_traits_type acc;
      if (acc::term_number(req) < current_term_) {
	// There is a leader out there that needs to know it's been left behind.
	comm_.append_checkpoint_chunk_response(acc::leader_id(req), get_peer_from_id(acc::leader_id(req)).address,
					       my_cluster_id(),
					       current_term_,
					       acc::term_number(req),
                                               acc::request_id(req),
					       0);
	return;
      }

      // Should not get an append checkpoint with current_term_ as LEADER
      // that would mean I am sending append_checkpoint messages to myself
      // or multiple leaders on the same term.
      BOOST_ASSERT(acc::term_number(req) > current_term_ || state_ != LEADER);

      // I highly doubt a disk write can be reliably cancelled so we have to wait for
      // an outstanding one to complete.  Instead of queuing up just pretend the message is lost.
      if (!can_become_follower_at_term(acc::term_number(req))) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " cannot advance to term " << acc::term_number(req) <<
	  " due to outstanding log header sync.  Ignoring append_checkpoint_chunk_request message.";
	return;
      }

      // At this point doesn't really matter what state I'm currently in
      // as I'm going to become a FOLLOWER.
      become_follower(acc::term_number(req), clock_now);

      // If we were already FOLLOWER we didn't reset election timer in become_follower() so do
      // it here as well
      if (log_header_sync_required_) {
	// TODO: We need to put a limit on number of continuations we'll queue up
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " waiting for log header sync.  Queuing append_checkpoint_chunk_request message from recipient " <<
	  acc::recipient_id(req) << " checkpoint_begin " << acc::checkpoint_begin(req) <<
	  " checkpoint_end " << acc::checkpoint_end(req);
	log_.sync_header();
	append_checkpoint_chunk_request_header_sync_continuations_.push_back(std::move(req));
      } else {
	election_timeout_ = new_election_timeout(clock_now);
	internal_append_checkpoint_chunk_request(std::move(req));
      }
    }

    void on_append_checkpoint_chunk_response(append_checkpoint_chunk_response_arg_type && resp)
    {
      on_append_checkpoint_chunk_response(std::move(resp), std::chrono::steady_clock::now());
    }
    
    void on_append_checkpoint_chunk_response(append_checkpoint_chunk_response_arg_type && resp,
                                             std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      typedef append_checkpoint_chunk_response_traits_type acc;
      if (acc::request_term_number(resp) != current_term_) {
	BOOST_ASSERT(acc::request_term_number(resp) < current_term_);
	// Discard
	return;
      }

      // I know I sent append_checkpoint_chunk_request at current_term_, term hasn't changed
      // so I must be leader
      BOOST_ASSERT(state_ == LEADER);
      BOOST_ASSERT(!log_header_sync_required_);
    
      if (acc::term_number(resp) > current_term_) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " received on_append_checkpoint_chunk_response at term " << acc::term_number(resp) <<
	  ". Becoming FOLLOWER and abandoning the append_checkpoint process.";
	BOOST_ASSERT(can_become_follower_at_term(acc::term_number(resp)));
	if (can_become_follower_at_term(acc::term_number(resp))) {
	  become_follower(acc::term_number(resp), clock_now);
	  BOOST_ASSERT(log_header_sync_required_);
	  if (log_header_sync_required_) {
	    log_.sync_header();
	  }
	  // TODO: What to do here.  We should no longer bother sending the rest of the checkpoint, that much
	  // is clear.
	  peer_type & p(peer_from_id(acc::recipient_id(resp)));
	  p.checkpoint_.reset();
	} else {
	  // This may be impossible because if we are waiting for a log header flush
	  // for current_term_ then we should never have been leader at current_term_
	  // and thus won't be getting a response.
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " cannot become follower at term " << acc::term_number(resp) <<
	    ". Ignoring on_append_checkpoint_chunk_response message";
	}
	return;
      }    

      peer_type & p(peer_from_id(acc::recipient_id(resp)));
      p.acknowledge_request_id(acc::request_id(resp));
      update_last_request_id_quorum();        
      if(!p.checkpoint_ || !p.checkpoint_->awaiting_ack_) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " received unexpected append_checkpoint_chunk_message from peer " << acc::recipient_id(resp) <<
	  ". Ignoring message.";
	return;
      }

      // Peer tells us what is needed next
      p.checkpoint_->checkpoint_next_byte_ = acc::bytes_stored(resp);

      p.checkpoint_->awaiting_ack_ = false;
      if (!p.checkpoint_->data_->is_final(p.checkpoint_->last_block_sent_)) {
        // Send next chunk
        send_checkpoint_chunk(clock_now, acc::recipient_id(resp));
      } else {
        // Final chunk ack'd so we're done and can update our knowledge of the peer log and clear the peer checkpoint state
        p.next_index_ = checkpoint_header_traits_type::last_log_entry_index(&p.checkpoint_->checkpoint_last_header_);
        p.match_index_ = p.next_index_;
        p.checkpoint_.reset();
        BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
          " successfully transferred checkpoint to peer " << acc::recipient_id(resp) <<
          " setting p.next_index_=" << p.next_index_ <<
          " p.match_index_=" << p.match_index_;
      }
    }  


    // Called when part of log is synced to disk
    void on_log_sync(uint64_t index)
    {
      on_log_sync(index, std::chrono::steady_clock::now());
    }
    
    void on_log_sync(uint64_t index, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      last_synced_index_ = index;
      BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" log synced to " << last_synced_index_ <<
	" with end of log at " << last_log_entry_index();
      try_to_commit(clock_now);

      // This is mostly relevant if I am a FOLLOWER (because I can't respond to
      // append_entries until the log entry is sync'd).
      // TODO: What happens if I've changed terms at this point???  I think this
      // will send responses with the updated term and success = false.
      {
	auto it = append_entry_continuations_.begin();
	auto e = append_entry_continuations_.upper_bound(last_synced_index_);
	if (it != e) {
	  for(; it != e; ++it) {
	    BOOST_ASSERT(it->second.end_index <= last_synced_index_);
            BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
              " sending append_entry response:  request_term_number=" << it->second.term <<
              " begin_index=" << it->second.begin_index <<
              " last_index=" << it->second.end_index <<
              " success=" << (it->second.term == current_term_ ? "true" : "false");
	    // TODO: Can I respond to more than what was requested?  Possibly useful idea for pipelined append_entry_request
	    // requests in which we can sync the log once for multiple append_entries.
	    comm_.append_entry_response(it->second.leader_id, get_peer_from_id(it->second.leader_id).address,
					my_cluster_id(),
					current_term_,
					it->second.term,
                                        it->second.request_id,
					it->second.begin_index,
					it->second.end_index,
					it->second.term == current_term_);
	  }
	  append_entry_continuations_.erase(append_entry_continuations_.begin(), e);
	} else {
	  BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " log synced to " << last_synced_index_ <<
	    " but no append_entries successfully synced";
	}
      }
    }

    // Update of log header (current_term_, voted_for_) synced to disk
    void on_log_header_sync()
    {
      on_log_header_sync(std::chrono::steady_clock::now());
    }
    
    void on_log_header_sync(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      if (!log_header_sync_required_) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " received unexpected log header sync event.";
	BOOST_ASSERT(log_header_sync_required_);
      }
      // TODO: I think we can only be FOLLOWER or CANDIDATE.  A LEADER never
      // updates its log header (or maybe snapshots and config changes will make it do so).
      // No, the log header is just voted_for and current_term and neither of those can change
      // when we are LEADER.
      BOOST_ASSERT(state_ == FOLLOWER || state_ == CANDIDATE);
      log_header_sync_required_ = false;
      for(auto & vr : vote_response_header_sync_continuations_) {
	// TODO: I think the following is currently valid because I disallow advancing the term
	// when a header sync has been requested.
	// BOOST_ASSERT(current_term_ == vr.term_number);
	comm_.vote_response(vr.candidate_id, get_peer_from_id(vr.candidate_id).address,
			    my_cluster_id(),
			    vr.term_number,
			    vr.request_term_number,
                            vr.request_id,
			    vr.granted);
      }
      vote_response_header_sync_continuations_.clear();
      for(auto & acc : append_checkpoint_chunk_request_header_sync_continuations_) {
	internal_append_checkpoint_chunk_request(std::move(acc));
      }
      append_checkpoint_chunk_request_header_sync_continuations_.clear();
      for(auto & ae : append_entry_header_sync_continuations_) {
	internal_append_entry_request(std::move(ae), clock_now);
      }
      append_entry_header_sync_continuations_.clear();
    
      become_candidate_on_log_header_sync(clock_now);
    }

    // Checkpoint stuff
    // TODO: I don't really have a handle on how this should be structured.  Questions:
    // 1. Who initiates periodic checkpoints (doesn't seem like it should be the consensus box; should be client/state machine)
    // 2. How to support mixing of client checkpoint state and consensus checkpoint state (e.g.
    // one could have consensus enforce that client checkpoint state includes consensus state via
    // a subclass)
    // 3. Interface between consensus and existing checkpoint state (which must be read both to restore
    // from a checkpoint and to send checkpoint data from a leader to the peer).

    std::pair<typename checkpoint_header_traits_type::const_arg_type, raft::util::call_on_delete> create_checkpoint_header(uint64_t last_index_in_checkpoint) const
    {
      std::pair<typename checkpoint_header_traits_type::const_arg_type, raft::util::call_on_delete> error(nullptr, raft::util::call_on_delete());
      // TODO: What if log_header_sync_required_==true?

      if (last_index_in_checkpoint > last_committed_index_) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " got request to checkpoint the log range [0," << last_index_in_checkpoint << ") but current committed log is [0," <<
	  last_committed_index_ << ")";
	return error;
      }

      // Is it even acceptable to checkpoint before last applied?
      if (last_index_in_checkpoint > last_applied_index_) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " got request to checkpoint the log range [0," << last_index_in_checkpoint << ") but have only applied log in the range is [0," <<
	  last_committed_index_ << ")";
	return error;
      }

      uint64_t arg_last_log_entry_index = 0;
      uint64_t arg_last_log_entry_term = 0;
      uint64_t arg_last_log_entry_cluster_time = 0;
      arg_last_log_entry_index = last_index_in_checkpoint;
      if (last_index_in_checkpoint > log_start_index() &&
	  last_index_in_checkpoint <= last_log_entry_index()) {
	// checkpoint index is still in the log
	arg_last_log_entry_term = log_.term(last_index_in_checkpoint-1);
	arg_last_log_entry_cluster_time = log_.cluster_time(last_index_in_checkpoint-1);
      } else if (last_index_in_checkpoint == 0) {
	// Requesting an empty checkpoint, silly but OK
	arg_last_log_entry_term = 0;      
	arg_last_log_entry_cluster_time = 0;      
      } else if (last_index_in_checkpoint == this->last_checkpoint_index()) {
	// requesting the checkpoint we've most recently made so we've save the term
	// and don't need to look at log entries
	arg_last_log_entry_term = this->last_checkpoint_term();
	arg_last_log_entry_cluster_time = this->last_checkpoint_cluster_time();
      } else {
	// we can't perform the requested checkpoint.  for example it is possible that
	// we've gotten a checkpoint from a leader that is newer than anything the client
	// is aware of yet.
	return error;
      }

      // Configuration state as of the checkpoint goes in the header...
      return checkpoint_header_traits_type::build(arg_last_log_entry_index,
                                                  arg_last_log_entry_term,
                                                  arg_last_log_entry_cluster_time,
                                                  configuration_.get_configuration_index_at(last_index_in_checkpoint),
                                                  configuration_.get_configuration_description_at(last_index_in_checkpoint));
    }

    bool check_log_has_checkpoint(checkpoint_data_ptr ckpt)
    {
      auto last_index_in_checkpoint = checkpoint_header_traits_type::last_log_entry_index(&ckpt->header());

      // Only remaining case here is that the checkpoint index is in the log
      if (last_index_in_checkpoint <= log_start_index() ||
	  last_index_in_checkpoint > last_log_entry_index()) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " got request to complete checkpoint at index " << last_index_in_checkpoint << " but log interval is [" <<
	  log_start_index() << "," << last_log_entry_index() << ")";
	return false;
      }

      BOOST_ASSERT(log_.term(last_index_in_checkpoint-1) == checkpoint_header_traits_type::last_log_entry_term(&ckpt->header()));
      BOOST_ASSERT(log_.cluster_time(last_index_in_checkpoint-1) == checkpoint_header_traits_type::last_log_entry_cluster_time(&ckpt->header()));
      return true;
    }

    void set_checkpoint(checkpoint_data_ptr ckpt, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      auto last_index_in_checkpoint = checkpoint_header_traits_type::last_log_entry_index(&ckpt->header());
      auto last_term_in_checkpoint = checkpoint_header_traits_type::last_log_entry_term(&ckpt->header());
      auto last_cluster_time_in_checkpoint = checkpoint_header_traits_type::last_log_entry_cluster_time(&ckpt->header());
      configuration_.set_checkpoint(ckpt->header(), clock_now);
 
      // Discard log entries
      // TODO: Understand the log sync'ing implications here
      // TODO: Also make sure that all of these entries have completed being applied
      // to state machine.
      log_.truncate_prefix(last_index_in_checkpoint);
      configuration_.truncate_prefix(last_index_in_checkpoint, clock_now);

      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" completed checkpoint at index " << last_index_in_checkpoint << ", term " <<
	last_term_in_checkpoint << " and cluster time " << last_cluster_time_in_checkpoint <<
        " current log entries [" << log_start_index() << "," <<
	last_log_entry_index() << ")";

    }

    void send_append_checkpoint_chunk_response(const append_checkpoint_chunk_response_continuation & resp)
    {
      comm_.append_checkpoint_chunk_response(resp.leader_id, get_peer_from_id(resp.leader_id).address,
                                             my_cluster_id(),
                                             resp.term_number,
                                             resp.request_term_number,
                                             resp.request_id,
                                             resp.bytes_stored);
    }
  
    void on_command_applied(uint64_t log_index)
    {
      if (log_index >= last_applied_index_) {
        BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
          " finished applying log entry " << log_index << " to state machine";
        last_applied_index_ = log_index + 1;

        {
          auto it = state_machine_applied_continuations_.begin();
          auto e = state_machine_applied_continuations_.upper_bound(last_applied_index_);
          for(; it != e; ++it) {
            for(auto & f : it->second) {
              f(messages_type::client_result_success());
            }
          }
          state_machine_applied_continuations_.erase(state_machine_applied_continuations_.begin(), e);
        }
      }
      apply_log_entries();
    }

    template<typename _Callback>
    void async_linearizable_read_only_query_fence(_Callback && cb, std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
      // First get quorum at a new request id to make sure we are still the cluster leader, if not the leader, we might be missing
      // a command the client committed and our response would break linearizability (though it would still be
      // serializable).
      ++request_id_;
      request_id_quorum_continuations_[request_id_] = std::function<void(client_result_type)>(std::move(cb));
      BOOST_LOG_TRIVIAL(debug) << "Enqueuing linearizable read only query at request_id_ " << request_id_;
      send_heartbeats(true, clock_now);
    }

    // Create a seed configuration record with a single server configuration for a log.
    // This should be run exactly once on a single server in a cluster.
    static std::pair<const log_entry_type *, raft::util::call_on_delete> get_bootstrap_log_entry(uint64_t id, const char * address)
    {
      return log_entry_traits_type::create_bootstrap_log_entry(id, address);
    }

    // Observers for testing
    const peer_type & get_peer_from_id(uint64_t peer_id) const {
      return configuration().get_peer_from_id(peer_id);
    }
    uint64_t current_term() const {
      return current_term_;
    }
    // N.B. even on a LEADER this commit index may be an underestimate of the
    // commit index of the cluster (e.g. a leader may have become partitioned and
    // fallen behind but not yet discovered that it has lost leadership).
    // See Ongaro Thesis Section 6.4.
    uint64_t commit_index() const {
      return last_committed_index_;
    }
    uint64_t applied_index() const {
      return last_applied_index_;
    }
    state get_state() const {
      return state_;
    }
    bool log_header_sync_required() const {
      return log_header_sync_required_;
    }
    uint64_t cluster_time() const {
      return cluster_clock_.cluster_time();
    }
    uint64_t leader_id() const {
      return leader_id_;
    }
    uint64_t request_id() const {
      return request_id_;
    }
  };
}




  


#endif
