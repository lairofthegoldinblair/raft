#ifndef __RAFTSERVER_HH__
#define __RAFTSERVER_HH__

#include <chrono>
#include <map>
#include <vector>

#include "boost/logic/tribool.hpp"

// For test_communicator: I think I'll templatize and eventually move this code out of here
#include <deque>
#include "boost/variant.hpp"

#include "checkpoint.hh"
#include "configuration.hh"
#include "log.hh"

namespace raft {

  enum client_result { SUCCESS, FAIL, RETRY, NOT_LEADER };
  class client_request
  {
  public:
    uint64_t id;
    std::string command;
  };

  class client_response
  {
  public:
    uint64_t id;
    client_result result;
    uint64_t index;
    std::size_t leader_id;
  };

  class request_vote
  {
  public:
    uint64_t recipient_id;
    uint64_t term_number;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
  };

  class vote_response
  {
  public:
    uint64_t peer_id;
    uint64_t term_number;
    uint64_t request_term_number;
    bool granted;
  };

  template<typename _LogEntry>
  class append_entry
  {
  public:
    typedef _LogEntry log_entry_type;
    uint64_t recipient_id;
    uint64_t term_number;
    uint64_t leader_id;
    // Basic point of Raft is the Log Matching Property which comprises:
    // 1) Index and Term of a log entry uniquely define the content
    // 2) If two logs have entries at the same index with the same term then all preceeding entries
    // also agree
    //
    // Part 1) of the property arises by having a leader be the only one that proposes entries and guaranteeing
    // that a leader never modifies a log entry with a given (index,term) once created.  Part 2) is guaranteed
    // by ensuring that a peer never appends to its log from a leader unless the content of the last entry is correct; by
    // Part 1) the content of that last entry may be specified by sending the (index,term) from the leader on every append request.
    // One after the last log entry sent.  If no log entries sent yet then 0.
    uint64_t previous_log_index;
    // The last term sent (only valid if previous_log_index > 0).
    uint64_t previous_log_term;
    // Last log entry in message that is committed on leader
    uint64_t leader_commit_index;
    std::vector<log_entry_type> entry;
  };

  class append_response
  {
  public:
    uint64_t recipient_id;
    uint64_t term_number;
    uint64_t request_term_number;
    // Beginning of range of entries appended
    uint64_t begin_index;
    // One after the last log entry appended
    uint64_t last_index;
    bool success;
  };

  // TODO: Why do we assume that the checkpoint term is in the checkpoint_header in the
  // data and not in the chunk message?
  template<typename checkpoint_data_store_type>
  class append_checkpoint_chunk
  {
  public:
    uint64_t recipient_id;
    uint64_t term_number;
    uint64_t leader_id;
    // Only needed on a chunk if checkpoint_done==true; a client can know
    // whether the checkpoint is up to date without looking at the data itself
    // (which is is assumed to carry a checkpoint_header that also has the index).
    uint64_t last_checkpoint_index;
    // Ongaro's logcabin does not put the term in the message but assumes that it is
    // serialized as part of the data (the actual checkpoint file).
    // I'm not sure I like that model so I am putting it in the chunk message as well;
    // we'll see how that goes for me :-)  I am only look at this value in the first chunk
    // of a checkpoint.
    uint64_t last_checkpoint_term;
    // Ongaro's logcabin does not put the configuration in the message but assumes that it is
    // serialized as part of the data (the actual checkpoint file).
    // I'm not sure I like that model so I am putting it in the chunk message as well;
    // we'll see how that goes for me :-)  I am only look at this value in the first chunk
    // of a checkpoint.
    typename checkpoint_data_store_type::configuration_type last_checkpoint_configuration;
    uint64_t checkpoint_begin;
    uint64_t checkpoint_end;
    bool checkpoint_done;
    std::vector<uint8_t> data;
  };

  class append_checkpoint_chunk_response
  {
  public:
    uint64_t recipient_id;
    uint64_t term_number;
    uint64_t request_term_number;
    uint64_t bytes_stored;
  };

  template<typename append_checkpoint_chunk_type, typename append_entry_type>
  class test_communicator
  {
  public:
    typedef size_t endpoint;
    template<typename _T>
    void send(endpoint ep, const _T & msg)
    {
      q.push_front(msg);
    }

    typedef boost::variant<request_vote, vote_response, append_entry_type, append_response,
			   append_checkpoint_chunk_type, append_checkpoint_chunk_response> any_msg_type;
    std::deque<any_msg_type> q;
  };

  // Raft only really cares about the fact that a checkpoint maintains the index and term
  // at which the checkpoint is taken.  What client data and how that client data is communicated
  // to peers and disk is more or less irrelevant.  I want to structure things so that these latter
  // aspects are decoupled.
  template<typename checkpoint_data_store_type>
  class peer_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef typename checkpoint_data_store_type::block_type block_type;
    typedef typename checkpoint_data_store_type::configuration_type configuration_type;
    typedef append_checkpoint_chunk<checkpoint_data_store_type> append_checkpoint_chunk_type;
    
    // One past last byte to written in checkpoint file.
    uint64_t checkpoint_next_byte_;
    // The last log entry that the checkpoint covers.  Need to resume
    // sending log entries to peer from this point.
    uint64_t checkpoint_last_log_entry_index_;
    // The term of the last log entry that the checkpoint covers.  
    uint64_t checkpoint_last_log_entry_term_;
    // Configuration in effect at the time the checkpoint occured
    configuration_type checkpoint_last_configuration_;
    // Checkpoint data we are sending to this peer
    checkpoint_data_ptr data_;
    // Last block sent.  We do not assume that the checkpoint bytes are contiguous in memory
    // so cannot use checkpoint_next_byte_ to know where the next chunk is in data_.
    block_type last_block_sent_;
    // Has the last block been acked?  TODO: Generalize to a window/credit system?
    bool awaiting_ack_;

    peer_checkpoint(uint64_t checkpoint_last_log_entry_index, uint64_t checkpoint_last_log_entry_term,
		    const configuration_type & config, checkpoint_data_ptr data)
      :
      checkpoint_next_byte_(0),
      checkpoint_last_log_entry_index_(checkpoint_last_log_entry_index),
      checkpoint_last_log_entry_term_(checkpoint_last_log_entry_term),
      checkpoint_last_configuration_(config),
      data_(data),
      awaiting_ack_(false)
    {
    }

    bool prepare_checkpoint_chunk(append_checkpoint_chunk_type & msg)
    {
      if (awaiting_ack_) {
	return false;
      }

      if (data_->is_final(last_block_sent_)) {
	return false;
      }

      last_block_sent_ = data_->block_at_offset(checkpoint_next_byte_);

      msg.last_checkpoint_index = checkpoint_last_log_entry_index_;
      msg.last_checkpoint_term = checkpoint_last_log_entry_term_;
      msg.last_checkpoint_configuration = checkpoint_last_configuration_;
      msg.checkpoint_begin = data_->block_begin(last_block_sent_);
      msg.checkpoint_end = data_->block_end(last_block_sent_);
      msg.checkpoint_done = data_->is_final(last_block_sent_);
      msg.data.assign(last_block_sent_.block_data_,
		      last_block_sent_.block_data_+last_block_sent_.block_length_);
      awaiting_ack_ = true;
      return true;
    }
  };

  // A peer encapsulates what a server knows about other servers in the cluster
  template<typename checkpoint_data_store_type>
  class peer
  {
  public:
    typedef typename checkpoint_data_store_type::configuration_type::address_type address_type;

  public:
    // peer id = same as index in peer array
    uint64_t peer_id;
    // TODO: Templatize; do we actually want multiple addresses?  Does protocol really
    // need to know whether this is one or more addresses?
    address_type address;
    // Leader specific state about peers
    // Index of next log entry to send
    uint64_t next_index_;
    // One past the index of last log entry known to be replicated to peer
    uint64_t match_index_;
    // Vote we got from this peer.  boost::logic::indeterminate means we haven't heard back yet.
    boost::logic::tribool vote_;
    // Used only when LEADER; when does this peer need another heartbeat?
    std::chrono::time_point<std::chrono::steady_clock> requires_heartbeat_;
    // Is the value of next_index_ a guess or has it been confirmed by communication with the peer
    bool is_next_index_reliable_;
    // TODO: Implement the exit protocol
    bool exiting_;
    // Checkpoint state for sending a checkpoint from a server to this peer
    std::shared_ptr<peer_checkpoint<checkpoint_data_store_type> > checkpoint_;
    // State for tracking whether a peer that is newly added to a configuration tracking
    // log appends
    std::shared_ptr<peer_configuration_change> configuration_change_;

    void exit()
    {
      exiting_ = true;
    }
  };

  class client_response_continuation
  {
  public:
    uint64_t client_request_id;
    // The log index we need flushed
    uint64_t index;
    // The term that the client request was part of
    uint64_t term;
    // TODO: Do we want/need this?  Time when the client request should simply timeout.
  };

  class append_entry_continuation
  {
  public:
    // Leader that sent the append entry request we are responding to.
    uint64_t leader_id;
    // The log index we need flushed
    uint64_t begin_index;
    uint64_t end_index;
    // The term that the client request was part of
    uint64_t term;
    // Commit point associated with the append_entry request
    uint64_t commit_index;
  };

  // A test client
  class client
  {
  public:
    std::deque<client_response> responses;
    void on_client_response(const client_response & resp)
    {
      responses.push_front(resp);
    }
  };

  // Peer side of a checkpoint transfer
  template<typename checkpoint_data_store_type>
  class in_progress_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::header_type header_type;
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
  public:
    uint64_t end_;
    checkpoint_data_ptr file_;
    
    uint64_t end() const {
      return end_;
    }

    void write(const std::vector<uint8_t> & data)
    {
      // TODO: Support async here
      file_->write(&data[0], data.size());
      end_ += data.size();
    }

    in_progress_checkpoint(checkpoint_data_store_type & store,
			   const header_type & header)
      :
      end_(0),
      file_(store.create(header))
    {
    }
  };

  // Per-server state related to checkpoints
  template <typename checkpoint_data_store_type>
  class server_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef typename checkpoint_data_store_type::configuration_type configuration_type;
  public:
    // One after last log entry checkpointed.
    uint64_t last_checkpoint_index_;
    // Term of last checkpoint
    uint64_t last_checkpoint_term_;
    // Configuration description at last checkpoint
    configuration_type last_checkpoint_configuration_;
    // continuations depending on checkpoint sync events
    std::vector<std::pair<std::size_t, append_checkpoint_chunk_response> > checkpoint_chunk_response_sync_continuations_;
    // Object to manage receiving a new checkpoint from a leader and writing it to a reliable
    // location
    std::shared_ptr<in_progress_checkpoint<checkpoint_data_store_type> > current_checkpoint_;
    // Checkpoints live here
    checkpoint_data_store_type store_;

    uint64_t last_checkpoint_index() const {
      return last_checkpoint_index_;
    }
    
    uint64_t last_checkpoint_term() const {
      return last_checkpoint_term_;
    }
    
    void sync(std::size_t leader_id, const append_checkpoint_chunk_response & resp) {
      checkpoint_chunk_response_sync_continuations_.push_back(std::make_pair(leader_id,resp));
    }

    void abandon() {
      if (!!current_checkpoint_) {
	store_.discard(current_checkpoint_->file_);
	current_checkpoint_.reset();
      }
    }

    checkpoint_data_ptr last_checkpoint() {
      return store_.last_checkpoint();
    }

    server_checkpoint()
      :
      last_checkpoint_index_(0),
      last_checkpoint_term_(0)    
    {
    }
  };

  template<typename peer_type, typename configuration_description_type>
  class test_configuration
  {
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

  template<typename peer_type, typename configuration_description_type>
  class test_configuration_manager
  {
  public:
    typedef test_configuration<peer_type, configuration_description_type> configuration_type;
    typedef configuration_description description_type;
    typedef typename configuration_description_type::checkpoint_type checkpoint_type;
  private:
    configuration_type configuration_;
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

  // A server encapsulates what a participant in the consensus protocol knows about itself
  class server
  {
  public:
    // Checkpoint types
    typedef checkpoint_data_store<configuration_description::checkpoint_type> checkpoint_data_store_type;
    typedef checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef checkpoint_data_store_type::header_type checkpoint_header_type;
    typedef peer_checkpoint<checkpoint_data_store_type> peer_checkpoint_type;
    typedef server_checkpoint<checkpoint_data_store_type> server_checkpoint_type;

    typedef peer<checkpoint_data_store_type> peer_type;

    // Log types
    typedef in_memory_log<configuration_description> log_type;
    typedef log_type::entry_type log_entry_type;
    typedef log_type::index_type log_index_type;

    // Message types
    typedef append_checkpoint_chunk<checkpoint_data_store_type> append_checkpoint_chunk_type;
    typedef append_entry<log_entry_type> append_entry_type;

    typedef test_communicator<append_checkpoint_chunk_type, append_entry_type> communicator_type;

    // Configuration types
    typedef configuration_manager<peer_type, configuration_description> configuration_manager_type;
    // typedef test_configuration_manager<peer_type, configuration_description> configuration_manager_type;
    typedef configuration_manager_type::configuration_type configuration_type;
    typedef configuration_description configuration_description_type;
    
    enum state { LEADER, FOLLOWER, CANDIDATE };
  private:
    static const std::size_t INVALID_PEER_ID = std::numeric_limits<std::size_t>::max();
    
    communicator_type & comm_;

    client & client_;

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

    // Leader id if I know it (learned from append_entry and append_checkpoint_chunk messages)
    std::size_t leader_id_;

    // My current term number
    uint64_t current_term_;

    // the peer that got my vote (could be myself)
    peer_type * voted_for_;

    // Common log state
    log_type & log_;

    // We can learn of the commit from a
    // checkpoint or from a majority of peers acknowledging a log entry.
    // last_committed_index_ represents the last point in the log that we KNOW
    // is replicated and therefore safe to apply to a state machine.  It may
    // be an underestimate of last successfully replicated log entry but that fact
    // will later be learned (e.g. when a leader tries to append again and ???).
    // TODO: Does this point to the actual last index committed or one past????
    uint64_t last_committed_index_;
    // TODO: How to determine applied
    uint64_t last_applied_index_;
    uint64_t last_synced_index_;

    // State related to checkpoint processing
    server_checkpoint_type checkpoint_;

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

    std::size_t my_cluster_id() const
    {
      return configuration().my_cluster_id();
    }

    bool has_quorum() const {
      return configuration().has_quorum();
    }

    // continuations depending on log sync events
    std::multimap<uint64_t, client_response_continuation> client_response_continuations_;
    std::multimap<uint64_t, append_entry_continuation> append_entry_continuations_;
    // continuations depending on log header sync events
    std::vector<append_entry_type> append_entry_header_sync_continuations_;
    std::vector<append_checkpoint_chunk_type> append_checkpoint_chunk_header_sync_continuations_;
    std::vector<std::pair<uint64_t, vote_response> > vote_response_header_sync_continuations_;
    // This flag is essentially another log header sync continuation that indicates vote requests 
    // should be sent out after log header sync (which is the case when we transition to CANDIDATE
    // but not when we transition to FOLLOWER).
    bool send_vote_requests_;


    bool no_log_header_sync_continuations_exist() const {
      return !send_vote_requests_ && 0 == vote_response_header_sync_continuations_.size() &&
	0 == append_entry_header_sync_continuations_.size();
    }
    
    uint64_t last_log_entry_term() const {
      // Something subtle with snapshots/checkpoints occurs here.  
      // After a checkpoint we may have no log entries so the term has to be recorded at the time
      // of the checkpoint.
      return log_.empty() ? checkpoint_.last_checkpoint_term_ : log_.last_entry().term;
    }
    uint64_t last_log_entry_index() const {
      return log_.last_index();
    }        
    uint64_t log_start_index() const {
      return log_.start_index();
    }
    
    // Used by FOLLOWER and CANDIDATE to decide when to initiate a new election.
    std::chrono::time_point<std::chrono::steady_clock> election_timeout_;

    std::chrono::time_point<std::chrono::steady_clock> new_election_timeout() const;
    std::chrono::time_point<std::chrono::steady_clock> new_heartbeat_timeout(std::chrono::time_point<std::chrono::steady_clock> clock_now) const;

    void send_vote_requests();
    void send_vote_response(const vote_response & resp);
    void send_append_entries(std::chrono::time_point<std::chrono::steady_clock> clock_now);
    void send_heartbeats(std::chrono::time_point<std::chrono::steady_clock> clock_now);
    void send_checkpoint_chunk(std::chrono::time_point<std::chrono::steady_clock> clock_now, uint64_t peer_id);

    // Based on log sync and/or append_response try to advance the commit point.
    void try_to_commit();

    // Append Entry processing
    void internal_append_entry(const append_entry_type & req);

    // Append Checkpoint Chunk processing
    void internal_append_checkpoint_chunk(const append_checkpoint_chunk_type & req);
    void internal_append_checkpoint_chunk_sync(std::size_t leader_id, const append_checkpoint_chunk_response & resp);
    void load_checkpoint();

    // Guards for state transitions.  Most of the cases in which I can't make a transition
    // it is because the transition requires a disk write of the header and such a write is already in
    // progress.  If we have a sufficiently slow disk we are essentially dead!
    bool can_become_follower_at_term(uint64_t term);

    // State Transitions
    void become_follower(uint64_t term);
    void become_candidate();
    void become_leader();
  public:
    server(communicator_type & comm, client & c, log_type & l, configuration_manager_type & config_manager);
    ~server();

    // Events
    void on_timer();
    void on_client_request(const client_request & req);
    void on_request_vote(const request_vote & req);
    void on_vote_response(const vote_response & resp);
    void on_append_entry(const append_entry_type & req);
    void on_append_response(const append_response & req);
    void on_append_checkpoint_chunk(const append_checkpoint_chunk_type & req);
    void on_append_checkpoint_chunk_response(const append_checkpoint_chunk_response & resp);
    // Append to log flushed to disk
    void on_log_sync(uint64_t index);
    // Update of log header (current_term_, voted_for_) flushed to disk
    void on_log_header_sync();
    // Update of checkpoint file
    void on_checkpoint_sync();

    // Checkpoint stuff
    // TODO: I don't really have a handle on how this should be structured.  Questions:
    // 1. Who initiates periodic checkpoints (doesn't seem like it should be the consensus box; should be client/state machine)
    // 2. How to support mixing of client checkpoint state and consensus checkpoint state (e.g.
    // one could have consensus enforce that client checkpoint state includes consensus state via
    // a subclass)
    // 3. Interface between consensus and existing checkpoint state (which must be read both to restore
    // from a checkpoint and to send checkpoint data from a leader to the peer).

    // This avoids having consensus constrain how the client takes and writes a checkpoint, but eventually
    // have to let consensus know when a checkpoint is complete so that it can be sent to a peer (for example).
    checkpoint_data_ptr begin_checkpoint(uint64_t last_index_in_checkpoint) const;
    bool complete_checkpoint(uint64_t last_index_in_checkpoint, checkpoint_data_ptr i_dont_like_this);

    // Observers for testing
    const peer_type & get_peer_from_id(uint64_t peer_id) const {
      return configuration().get_peer_from_id(peer_id);
    }
    uint64_t current_term() const {
      return current_term_;
    }
    uint64_t commit_index() const {
      return last_committed_index_;
    }
    state get_state() const {
      return state_;
    }
    bool log_header_sync_required() const {
      return log_header_sync_required_;
    }
    server_checkpoint_type & checkpoint() {
      return checkpoint_;
    }
  };
}
#endif
