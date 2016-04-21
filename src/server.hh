#ifndef __RAFTSERVER_HH__
#define __RAFTSERVER_HH__

#include <chrono>
#include <map>
#include <vector>

#include "boost/logic/tribool.hpp"

// For test_communicator: I think I'll templatize and eventually move this code out of here
#include <deque>
#include "boost/variant.hpp"

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

  class append_entry
  {
  public:
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
    std::vector<log_entry> entry;
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

  class append_checkpoint_chunk
  {
  };

  class append_checkpoint_chunk_response
  {
  };

  

  class test_communicator
  {
  public:
    typedef size_t endpoint;
    template<typename _T>
    void send(endpoint ep, const _T & msg)
    {
      q.push_front(msg);
    }

    typedef boost::variant<request_vote, vote_response, append_entry, append_response> any_msg_type;
    std::deque<any_msg_type> q;
  };

  // A peer encapsulates what a server knows about other servers in the cluster
  class peer
  {
  public:
    // peer id = same as index in peer array
    uint64_t peer_id;
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

  // A server encapsulates what a server knows about itself
  class server
  {
  public:
    enum state { LEADER, FOLLOWER, CANDIDATE };
  private:
    static const std::size_t INVALID_PEER_ID = std::numeric_limits<std::size_t>::max();
    
    test_communicator & comm_;

    client & client_;
    
    state state_;

    // Algorithm config parameters
    std::chrono::milliseconds election_timeout_max_;
    std::chrono::milliseconds election_timeout_min_;

    // The cluster 
    std::vector<peer> cluster_;

    // My cluster id/index
    std::size_t cluster_idx_;

    // Leader id if I know it (learned from append_entry messages)
    std::size_t leader_id_;

    // My current term number
    uint64_t current_term_;

    // the peer that got my vote (could be myself)
    peer * voted_for_;

    // Common log state
    in_memory_log log_;

    // TODO: How to determine committed.  Right now this is only updated
    // in on_log_sync; is that right?  No, we can learn of the commit from a
    // checkpoint or from a majority of peers acknowledging a log entry.
    // last_committed_index_ represents the last point in the log that we KNOW
    // is replicated and therefore safe to apply to a state machine.  It may
    // be an underestimate of last successfully replicated log entry but that fact
    // will later be learned (e.g. when a leader tries to append again).
    uint64_t last_committed_index_;
    // TODO: How to determine applied
    uint64_t last_applied_index_;
    uint64_t last_synced_index_;
    // One after last log entry checkpointed.
    uint64_t last_checkpoint_index_;
    uint64_t last_checkpoint_term_;

    // continuations depending on log sync events
    std::multimap<uint64_t, client_response_continuation> client_response_continuations_;
    std::multimap<uint64_t, append_entry_continuation> append_entry_continuations_;

    uint64_t last_log_entry_term() const {
      // Something subtle with snapshots/checkpoints occurs here.  
      // Presumably after a checkpoint we have no log entries so the term has to be recorded at the time
      // of the checkpoint!
      return log_.empty() ? last_checkpoint_term_ : log_.last_entry().term;
    }
    uint64_t last_log_entry_index() const {
      return log_.last_index();
    }        
    uint64_t log_start_index() const {
      return log_.start_index();
    }
    
    // Used by FOLLOWER and CANDIDATE to decide when to initiate a new election.
    std::chrono::time_point<std::chrono::steady_clock> election_timeout_;

    peer & self() {
      return cluster_[cluster_idx_];
    }

    peer & peer_from_id(uint64_t peer_id) {
      return cluster_[peer_id];
    }

    std::chrono::time_point<std::chrono::steady_clock> new_election_timeout() const;
    std::chrono::time_point<std::chrono::steady_clock> new_heartbeat_timeout(std::chrono::time_point<std::chrono::steady_clock> clock_now) const;

    void send_vote_requests();
    void send_vote_response(const vote_response & resp);
    void send_append_entries(std::chrono::time_point<std::chrono::steady_clock> clock_now);
    void send_heartbeats(std::chrono::time_point<std::chrono::steady_clock> clock_now);

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

    // Based on log sync and/or append_response try to advance the commit point.
    void try_to_commit();

    // State Transitions
    void become_follower(uint64_t term);
    void become_candidate();
    void become_leader();
  public:
    server(test_communicator & comm, client & c, std::size_t cluster_idx, const std::vector<peer>& peers);
    ~server();

    // Events
    void on_timer();
    void on_client_request(const client_request & req);
    void on_request_vote(const request_vote & req);
    void on_vote_response(const vote_response & resp);
    void on_append_entry(const append_entry & req);
    void on_append_response(const append_response & req);
    void on_log_sync(uint64_t index);

    // Observers for testing
    uint64_t current_term() const {
      return current_term_;
    }
    state get_state() const {
      return state_;
    }
    const peer & get_peer_from_id(uint64_t peer_id) const {
      return cluster_.at(peer_id);
    }
  };
}
#endif
