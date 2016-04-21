#include <random>

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>

#include "server.hh"

namespace raft {
  server::server(test_communicator & comm, client & c, std::size_t cluster_idx, const std::vector<peer>& peers)
    :
    comm_(comm),
    client_(c),
    state_(FOLLOWER),
    election_timeout_max_(300),
    election_timeout_min_(150),
    cluster_(peers),
    cluster_idx_(cluster_idx),
    current_term_(0),
    voted_for_(nullptr),
    last_committed_index_(0),
    last_applied_index_(0),
    last_synced_index_(0),
    last_checkpoint_index_(0),
    last_checkpoint_term_(0)
  {
    election_timeout_ = new_election_timeout();
  }

  server::~server()
  {
  }

  std::chrono::time_point<std::chrono::steady_clock> server::new_election_timeout() const
  {
    std::default_random_engine generator;
    std::uniform_int_distribution<std::chrono::milliseconds::rep> dist(election_timeout_min_.count(), 
								       election_timeout_max_.count());
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(generator));
  }

  std::chrono::time_point<std::chrono::steady_clock>
  server::new_heartbeat_timeout(std::chrono::time_point<std::chrono::steady_clock> clock_now) const
  {
    std::default_random_engine generator;
    std::uniform_int_distribution<std::chrono::milliseconds::rep> dist(election_timeout_min_.count()/2, 
								       election_timeout_max_.count()/2);
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(generator));
  }

  void server::send_vote_requests()
  {
    // request_vote message to peers
    for(std::size_t i=0; i<cluster_.size(); ++i) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" requesting vote from peer " << i;
      cluster_[i].vote_ = boost::logic::indeterminate;
      if (i != cluster_idx_) {
	request_vote msg;
	msg.recipient_id = i;
	msg.term_number = current_term_;
	msg.candidate_id = cluster_idx_;
	msg.last_log_index = last_log_entry_index();
	msg.last_log_term = last_log_entry_term();
	// TODO: Templatize and abstract out the communicator address of the peer.
	comm_.send(i, msg);
      }
    }
  }

  void server::send_append_entries(std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    for(std::size_t i=0; i<cluster_.size(); ++i) {
      if (i != cluster_idx_ && last_log_entry_index() > cluster_[i].match_index_) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	  " sending append_entry to peer " << i << "; last_log_entry_index()=" << last_log_entry_index() <<
	  " peer.match_index=" << cluster_[i].match_index_;
	// This is next log entry the peer needs
	uint64_t previous_log_index = cluster_[i].next_index_;
	if (cluster_[i].next_index_ < log_start_index()) {
	  // TODO: We have checkpointed a portion of the log the peer requires
	  // we must send a checkpoint instead and then we can apply log entries.
	  return;
	}
	// Last time we sent to the peer it was at this term
	uint64_t previous_log_term = 0;
	if (previous_log_index > log_start_index()) {
	  // We've still got the entry so grab the actual term
	  previous_log_term = log_.entry(previous_log_index-1).term;
	} else if (previous_log_index == 0) {
	  // First log entry.  No previous log term.
	  previous_log_term = 0;
	} else if (previous_log_index == last_checkpoint_index_) {
	  // Boundary case: Last log entry sent was the last entry in the last checkpoint
	  // so we know what term that was
	  previous_log_term = last_checkpoint_term_;
	} else {
	  // TODO: How do we get here??? Send checkpoint
	}

	append_entry msg;
	msg.recipient_id = i;
	msg.term_number = current_term_;
	msg.leader_id = cluster_idx_;
	msg.previous_log_index = previous_log_index;
	msg.previous_log_term = previous_log_term;

	// TODO: For efficiency avoid sending actual data in messages unless p.is_next_index_reliable_ == true
	uint64_t log_entries_sent = 0;
	// Actually put the entries into the message.
	uint64_t idx_end = last_log_entry_index();
	for(uint64_t idx = cluster_[i].next_index_; idx != idx_end; ++idx) {
	  // TODO: Enforce message size limits
	  msg.entry.push_back(log_.entry(idx));
	  log_entries_sent += 1;
	}
	msg.leader_commit_index = std::min(last_committed_index_, previous_log_index+log_entries_sent-1);
	
	// TODO: Templatize and abstract out the communicator address of the peer.
	comm_.send(i, msg);
	cluster_[i].requires_heartbeat_ = new_heartbeat_timeout(clock_now);
      }
    }
  }

  void server::send_heartbeats(std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    for(std::size_t i=0; i<cluster_.size(); ++i) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" sending empty append_entry to peer " << i;
      if (i != cluster_idx_ && cluster_[i].requires_heartbeat_ <= clock_now) {
	append_entry msg;
	msg.recipient_id = i;
	msg.term_number = current_term_;
	msg.leader_id = cluster_idx_;
	// TODO: What to set in these 3???  Should this logic be the same
	// as send_append_entries???
	msg.previous_log_index = 0;
	msg.previous_log_term = 0;
	msg.leader_commit_index = 0;
	// No entries
	//msg.entry;
	// TODO: Templatize and abstract out the communicator address of the peer.
	comm_.send(i, msg);
	cluster_[i].requires_heartbeat_ = new_heartbeat_timeout(clock_now);
      }
    }
  }

  void server::become_follower(uint64_t term)
  {
    if (term > current_term_) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" becoming FOLLOWER at term " << term;
      state_ = FOLLOWER;
      current_term_ = term;
      voted_for_ = nullptr;
      election_timeout_ = new_election_timeout();
      leader_id_ = INVALID_PEER_ID;
      log_.append(current_term_, INVALID_PEER_ID);
    }

    // TODO: If transitioning from leader there may be a flush to disk
    // that we need to issue?  Do that now after waiting for any in-progress flush to complete.

    // TODO: If we are transitioning from leader there may be some pending client requests.  What to
    // do about them???  This is intimately related to the flushing question above.
  }

  void server::become_candidate()
  {
    // TODO: In case of dynamic configuration I may not be a member of the
    // cluster...

    BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
      " becoming CANDIDATE and starting election at term " << (current_term_+1);
    current_term_ += 1;
    state_ = CANDIDATE;
    election_timeout_ = new_election_timeout();
    voted_for_ = &self();
    send_vote_requests();

    // TODO: Cancel any snapshot/checkpoint that is in progress?

    // TODO: Log (and flush?)
    log_.append(current_term_, voted_for_->peer_id);

    // TODO: Special case single server cluster become_leader
  
  }

  void server::become_leader()
  {
    BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
      " becoming LEADER";
    state_ = LEADER;
    leader_id_ = cluster_idx_;
    election_timeout_ = new_election_timeout();
    // Send out empty append entries to assert leadership
    std::chrono::time_point<std::chrono::steady_clock> clock_now = std::chrono::steady_clock::now();
    for(std::size_t i=0; i<cluster_.size(); ++i) {
      if (i != cluster_idx_) {
	cluster_[i].requires_heartbeat_ = clock_now;
	// Guess that peer is up to date with our log.  We'll avoid chewing up bandwidth until
	// we hear back from the peer about it's last log entry.
	cluster_[i].next_index_ = last_log_entry_index();
	cluster_[i].is_next_index_reliable_ = false;
      }
    }
    send_heartbeats(clock_now);

    // Append a new log entry.  TODO: Do we need to flush the log????

    // TODO: As an optimization cancel any outstanding request_vote since they are not needed
    // we've already got quorum
  }

  void server::on_timer()
  {
    std::chrono::time_point<std::chrono::steady_clock> clock_now = std::chrono::steady_clock::now();
    switch(state_) {
    case FOLLOWER:
      if (election_timeout_ < clock_now) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	  " FOLLOWER lost contact with leader.";
	become_candidate();
      }
      break;
    case CANDIDATE:
      if (election_timeout_ < clock_now) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	  " CANDIATE timed out election.";
	become_candidate();
      }
      break;
    case LEADER:
      // Send heartbeats and append_entries if needed
      send_append_entries(clock_now);
      send_heartbeats(clock_now);
      break;
    }
  }

  // Called when the log has been synced to disk
  void server::on_log_sync(uint64_t index)
  {
    last_synced_index_ = index;
    BOOST_LOG_TRIVIAL(debug) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
      " log synced to " << last_synced_index_ <<
      " with end of log at " << last_log_entry_index();
    try_to_commit();

    // This is mostly relevant if I am a FOLLOWER
    // TODO: What happens if I've changed terms at this point???  I think this
    // will send responses with the updated term and success = false.
    {
      auto it = append_entry_continuations_.begin();
      auto e = append_entry_continuations_.upper_bound(last_synced_index_);
      if (it != e) {
	for(; it != e; ++it) {
	  BOOST_ASSERT(it->second.end_index <= last_synced_index_);
	  append_response resp;
	  resp.recipient_id = cluster_idx_;
	  resp.term_number = current_term_;
	  resp.request_term_number = it->second.term;
	  resp.begin_index = it->second.begin_index;
	  // TODO: Can I respond to more that what was requested?  Possibly useful idea for pipelined append_entry
	  // requests in which we can sync the log once for multiple append_entries.
	  resp.last_index = it->second.end_index;
	  // Leaders won't look at this if it's false
	  resp.success = it->second.term == current_term_;
	  comm_.send(it->second.leader_id, resp);
	}
	append_entry_continuations_.erase(append_entry_continuations_.begin(), e);
      } else {
	BOOST_LOG_TRIVIAL(debug) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	  " log synced to " << last_synced_index_ <<
	  " but no append_entries successfully synced";
      }
    }
  }

  void server::on_client_request(const client_request & req)
  {
    switch(state_) {
    case FOLLOWER:
    case CANDIDATE:
      // Not leader don't bother.
      client_response resp;
      resp.id = req.id;
      resp.result = NOT_LEADER;
      resp.index = 0;
      resp.leader_id = leader_id_;
      client_.on_client_response(resp);
      return;
    case LEADER:
      {
	// Create a log entry and try to replicate it
	std::vector<log_entry> v(1);
	v.back().type = log_entry::COMMAND;
	v.back().data = req.command;
	v.back().term = current_term_;
	// Append to the in memory log then we have to wait for
	// the new log entry to be replicated to a majority (TODO: to disk?)/committed before
	// returning to the client
	auto indices = log_.append(v);
	client_response_continuation cont;
	cont.client_request_id = req.id;
	cont.index = indices.second-1;
	cont.term = current_term_;
	client_response_continuations_.insert(std::make_pair(cont.index, cont));
	// TODO: Do we want to let the log know that we'd like a flush?
	// TODO: What triggers the sending of append_entries to peers?  Currently it is a timer
	// but that limits the minimum latency of replication.  Is that a problem?  I suppose we
	// could just call append_entries or on_timer here.
	return;
      }
    }
  }

  void server::on_request_vote(const request_vote & req)
  {
    // QUESTION: Under what circumstances might we get req.term_number == current_term_ and nullptr == voted_for_?
    // ANSWER: I may have gotten a vote request from a candidate that bumped up my current_term_ but didn't have
    // all of my log entries hence didn't get my vote.
    
    // TODO: Is it possible for req.term_number > current_term_ but false==candidate_log_more_complete?
    // I suppose it might be if candidate was at current_term_ and then lost some heartbeats and started a new election
    // without having all of my log entries
    
    // Term has advanced so become a follower.  Note that we may not vote for this candidate even though
    // we are possibly giving up leadership.
    if (req.term_number > current_term_) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" becoming follower due to vote request from " << req.candidate_id << " at higher term " << req.term_number;
      become_follower(req.term_number);
    }
    
    bool candidate_log_more_complete = req.last_log_term > last_log_entry_term() ||
      (req.last_log_term == last_log_entry_term() && req.last_log_index >= last_log_entry_index());

    // Vote at most once in each term and only vote for a candidate that has all of my log entries
    // Note that in fact it is impossible for req.term_number > current_term_ at this point because
    // we have already update current_term_ above in that case.  Logically this way of writing things
    // is a bit clearer to me.
    if (req.term_number >= current_term_ && candidate_log_more_complete && nullptr == voted_for_) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" voting for " << req.candidate_id << " at term " << req.term_number;
      become_follower(req.term_number);
      voted_for_ = &peer_from_id(req.candidate_id);
      election_timeout_ = new_election_timeout();
      // Changed voted_for_ and possibly current_term_ ; propagate to log
      log_.append(current_term_, req.candidate_id);
      // TODO: Must sync log here.
    }

    vote_response msg;
    msg.term_number = current_term_;
    msg.request_term_number = req.term_number;
    msg.granted = current_term_ == req.term_number && voted_for_ == &peer_from_id(req.candidate_id);
    
    // TODO: Templatize and abstract out the communicator address of the peer.
    comm_.send(req.candidate_id, msg);

    // It doesn't appear that much here is state dependent...
    // switch(state_) {
    // case FOLLOWER:
    //   break;
    // case CANDIDATE:
    //   break;
    // case LEADER:
    //   break;
    // }
  }

  void server::on_vote_response(const vote_response & resp)
  {
    switch(state_) {
    case CANDIDATE:
      // Detect if this response if from a vote_request on a previous
      // term (e.g. we started an election, it timed out we started a new one and then we
      // got this response from the old one).  If so ignore it.
      if (current_term_ == resp.request_term_number) {
	if (current_term_ < resp.term_number) {
	  become_follower(resp.term_number);
	} else {
	  BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	    " received vote " << resp.granted << " from peer " << resp.peer_id;
	  peer & p(peer_from_id(resp.peer_id));
	  // TODO: What happens if we get multiple responses from the same peer
	  // but with different votes?  Is that a protocol error????
	  p.vote_ = resp.granted;
	  if (has_quorum()) {
	    become_leader();
	  }
	}
      }
      break;
    case FOLLOWER:
    case LEADER:
      // Nothing to do.
      break;
    }
  }

  void server::on_append_entry(const append_entry & req)
  {
    if (req.term_number < current_term_) {
      // There is a leader out there that needs to know it's been left behind.
      append_response msg;
      msg.recipient_id = cluster_idx_;
      msg.term_number = current_term_;
      msg.request_term_number = req.term_number;
      msg.last_index = last_log_entry_index();
      msg.success = false;
      comm_.send(req.leader_id, msg);
      return;
    }

    // Should not get an append entry with current_term_ as LEADER
    // that would mean I am sending append_entry messages to myself
    // or multiple leaders on the same term.
    BOOST_ASSERT(req.term_number > current_term_ || state_ != LEADER);

    // At this point doesn't really matter what state I'm currently in
    // as I'm going to become a FOLLOWER.
    become_follower(req.term_number);

    // If we were FOLLOWER we didn't reset election timer in become_follower() so do
    // it here as well
    election_timeout_ = new_election_timeout();

    // May be the first append_entry for this term; that's when we learn
    // who the leader actually is.
    if (leader_id_ == INVALID_PEER_ID) {
      leader_id_ = req.leader_id;
    } else {
      BOOST_ASSERT(leader_id_ == req.leader_id);
    }

    // TODO: Finish

    // Do we have all log entries up to this one?
    if (req.previous_log_index > last_log_entry_index()) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" received append entry with gap.  req.previous_log_index=" << req.previous_log_index <<
	" last_log_entry_index()=" << last_log_entry_index();
      append_response msg;
      msg.recipient_id = cluster_idx_;
      msg.term_number = current_term_;
      msg.request_term_number = req.term_number;
      msg.last_index = last_log_entry_index();
      msg.success = false;
      comm_.send(req.leader_id, msg);
      return;
    }

    // Do the terms of the last log entry agree?  By the Log Matching Property this will guarantee
    // the logs of the leader that is appending will be identical at the point we begin appending.
    // See the extended Raft paper Figure 7 for examples of how this can come to pass.
    if (req.previous_log_index > log_start_index() &&
	req.previous_log_term != log_.entry(req.previous_log_index-1).term) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" received append entry with mismatch term.  req.previous_log_index=" << req.previous_log_index <<
	" req.previous_log_term=" << req.previous_log_term <<
	" log_.entry(" << (req.previous_log_index-1) << ").term=" << log_.entry(req.previous_log_index-1).term;
      append_response msg;
      msg.recipient_id = cluster_idx_;
      msg.term_number = current_term_;
      msg.request_term_number = req.term_number;
      msg.last_index = req.previous_log_index;
      msg.success = false;
      comm_.send(req.leader_id, msg);
      return;
    }

    // Detect duplicate log entry
    // Here we guard against
    // 1) Getting a duplicate that causes index to go backwards (and would
    // cause truncation of already committed/accepted entries if not guarded).
    // 2) Getting an entry, successfully committing and then getting a duplicate
    // and getting a failure when trying to commit the duplicate (corrupting an already
    // committed entry).

    auto it = req.entry.begin();
    auto entry_end = req.entry.end();
    uint64_t entry_index = req.previous_log_index;
    // Scan through entries in message to find where to start appending from
    for(; it!=entry_end; ++it, ++entry_index) {
      // TODO: should this be entry_index <= log_start_index()?
      if (entry_index < log_start_index()) {
	// This might happen if a leader sends us an entry we've already checkpointed
	continue;
      }
      if (last_log_entry_index() > entry_index) {
	if (log_.entry(entry_index).term == current_term_) {
	  // We've already got this message, keep looking for something new.
	  continue;
	}
	// We've got some uncommitted cruft in our log.  Truncate it and then start appending with
	// this entry.
	BOOST_ASSERT(last_committed_index_ < entry_index);
	// At this point we know that the leader log differs from what we've got so get rid of things
	// from here to the end
	BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	  " truncating uncommitted log entries [" << entry_index <<
	  "," << last_log_entry_index() << ") starting at term " << log_.entry(entry_index).term;
	log_.truncate_suffix(entry_index);
	// TODO: Something with configurations
	break;
      } else {
	// A new entry; start appending from here.
	break;
      }
    }

    // TODO: If nothing remains to append we should just return here.  Should update the committed id even if no new log entries

    if (it != entry_end) {
      BOOST_LOG_TRIVIAL(debug) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	" appending log entries [" << entry_index <<
	"," << (entry_index + std::distance(it,entry_end)) << ") starting at term " << it->term;
    }

    std::vector<log_entry> to_append;
    uint64_t entry_end_index = entry_index;
    for(; it != entry_end; ++it, ++entry_end_index) {
      to_append.push_back(*it);
    }
    log_.append(to_append);

    // Update committed id
    // TODO: Do we need to wait for log flush to update this?  I don't see why since the leader has determined
    // this (possibly without our vote).  On the other hand there might be a problem if we allow a state machine
    // to have entries applied without having them be persistent yet.
    if (last_committed_index_ < req.leader_commit_index) {
      last_committed_index_ = req.leader_commit_index;
    }

    // Create a continuation for when the log is flushed to disk
    append_entry_continuation cont;
    cont.leader_id = req.leader_id;
    cont.begin_index = entry_index;
    cont.end_index = entry_end_index;
    cont.term= req.term_number;
    // TODO: Not currently using this since I advance last_committed_index_ above.  Figure out if that
    // is the correct thing to do.
    cont.commit_index = req.leader_commit_index;
    append_entry_continuations_.insert(std::make_pair(cont.end_index, cont));
        
    // TODO: When do we flush the log?  Presumably before we respond since the leader takes our
    // response as durable. Yes.
    // TODO: When do we apply the entry to the state machine?  Do we need to wait until the leader informs us this
    // entry is committed?  Yes.
  }

  void server::try_to_commit()
  {
    if (state_ != LEADER) {
      return;
    }
    // TODO: make this relative to a configuration...
    
    // Figure out the minimum over a quorum.
    std::vector<uint64_t> acked;
    // For a leader, syncing to a log is "acking"
    acked.push_back(last_synced_index_);
    // For peers we need an append response to get an ack
    for(auto & c : cluster_) {
      if (c.peer_id != cluster_idx_) {
	acked.push_back(c.match_index_);
      }
    }
    std::sort(acked.begin(), acked.end());
    uint64_t committed = acked[(acked.size()-1)/2];
    if (last_committed_index_ >= committed) {
      // Nothing new to commit
      // TODO: Can committed go backwards?
      return;
    }

    // If we checkpointed uncommitted log that is VERY BAD
    BOOST_ASSERT(committed > log_start_index());

    if (log_.entry(committed-1).term != current_term_) {
      // TODO: What is going on here?
      return;
    }

    BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
      " committed log entries [" << last_committed_index_ <<
      "," << committed << ")";
    last_committed_index_ = committed;

    // TODO: If we have committed a new configuration and we are leader then
    // we need to become_follower().

    // Based on the new commit point we may be able to complete some client requests/configuration change
    // if we are leader or we may be able to respond to the leader for some
    // append entries requests if we are follower
    auto it = client_response_continuations_.begin();
    auto e = client_response_continuations_.upper_bound(last_committed_index_);
    for(; it != e; ++it) {
      BOOST_ASSERT(it->second.index <= last_committed_index_);
      client_response resp;
      resp.id = it->second.client_request_id;
      resp.leader_id = leader_id_;
      resp.index = it->second.index;
      // If I lost leadership I can't guarantee that I committed.
      // TODO: Come up with some interesting test case here (e.g. we
      // get a quorum to ack the entry but I've lost leadership by then;
      // is it really not successful???).  Actually the answer is that the
      // commit of a log entry occurs when a majority of peers have written
      // the log entry to disk.  
      resp.result = it->second.term == current_term_ ? SUCCESS : NOT_LEADER;
      client_.on_client_response(resp);
    }
    client_response_continuations_.erase(client_response_continuations_.begin(), e);
  }

  void server::on_append_response(const append_response & req)
  {
    if (req.request_term_number != current_term_) {
      BOOST_ASSERT(req.request_term_number < current_term_);
      // Discard
      return;
    }
    if (req.term_number > current_term_) {
      become_follower(req.term_number);
      return;
    }
    BOOST_ASSERT(state_ == LEADER);
    peer & p(cluster_[req.recipient_id]);
    if (req.success) {
      if (p.match_index_ > req.last_index) {
	// TODO: This is unexpected if we don't pipeline append entries message,
	// otherwise it is OK but we need more complex state around the management of
	// what entries the peer has acknowledged (have we done enough here???)
	BOOST_LOG_TRIVIAL(warning) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	  " peer " << req.recipient_id << " acknowledged index entries [" << req.begin_index <<
	  "," << req.last_index <<
	  ") but we already had acknowledgment up to index " << p.match_index_;
      } else {
	BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	  " peer " << req.recipient_id << " acknowledged index entries [" << req.begin_index <<
	  "," << req.last_index << ")";
	p.match_index_ = req.last_index;
	// Now figure out whether we have a majority of peers ack'ing and can commit something
	try_to_commit();
      }
      // TODO: I suppose if we are pipelining append_entries to a peer then this could cause next_index_
      // to go backwards.  That wouldn't be a correctness issue merely inefficient because we'd send
      // already matched entries.  Nonetheless it is easy to prevent.
      if (p.next_index_ < p.match_index_ || !p.is_next_index_reliable_) {
	p.next_index_ = p.match_index_;
	p.is_next_index_reliable_ = true;
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
      if (p.next_index_ > req.last_index) {
	if (p.is_next_index_reliable_) {
	  BOOST_LOG_TRIVIAL(warning) << "Server(" << cluster_idx_ << ") at term " << current_term_ <<
	    " peer " << req.recipient_id << " rejected append_entries with peer log at " << req.last_index <<
	    " but leader had a reliable next index of  " << p.next_index_;
	}
	p.next_index_ = req.last_index;
      }
    }
  }
}
