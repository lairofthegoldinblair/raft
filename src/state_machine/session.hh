#ifndef __RAFT_STATE_MACHINE_SESSION_HH__
#define __RAFT_STATE_MACHINE_SESSION_HH__

#include <limits>
#include <map>
#include <memory>
#include <tuple>

#include <boost/log/trivial.hpp>

namespace raft {
  namespace state_machine {
    template<typename _Messages>
    class client_session_state
    {
    public:
      typedef _Messages messages_type;
      typedef typename messages_type::client_response_traits_type::arg_type client_response_arg_type;
      typedef typename messages_type::linearizable_command_traits_type linearizable_command_traits_type;
      typedef typename messages_type::linearizable_command_traits_type::const_view_type linearizable_command_const_view_type;
    private:

      // For timeout of session, the last cluster time of a command we've received
      uint64_t last_command_received_cluster_time_;

      // For expiring memoized responses.   We can remove any client responses with
      // sequence number less than this value.
      uint64_t first_unacknowledged_sequence_number_;

      // Memoized responses to client requests.  sequence_number => client response.  Used to avoid at most once semantics.
      std::map<uint64_t, std::pair<raft::slice, raft::util::call_on_delete>> memo_;

      // Set first_unacknowledged_sequence_number and clean up unneeded responses.
      void first_unacknowledged_sequence_number (uint64_t val)
      {
        if (val <= first_unacknowledged_sequence_number_) {
          return;
        }
        first_unacknowledged_sequence_number_ = val;
        for(auto it = memo_.begin(); it != memo_.end(); ) {
          if (it->first < first_unacknowledged_sequence_number_) {
            it = memo_.erase(it);
          } else {
            ++it;
          }
        }
      }

    public:
      client_session_state(uint64_t cluster_time)
        :
        last_command_received_cluster_time_(cluster_time),
        first_unacknowledged_sequence_number_(0)
      {
      }
      uint64_t last_command_received_cluster_time() const
      {
        return last_command_received_cluster_time_;
      }
      bool apply(linearizable_command_const_view_type cmd, uint64_t cluster_time)
      {
        first_unacknowledged_sequence_number(linearizable_command_traits_type::first_unacknowledged_sequence_number(cmd));
        auto sequence_number = linearizable_command_traits_type::sequence_number(cmd);
        if (sequence_number < first_unacknowledged_sequence_number_) {
          // This command has been acknowledged a fortiori has already been applied
          return false;
        }
        auto ret = memo_.insert(std::make_pair(sequence_number, std::make_pair(raft::slice(), raft::util::call_on_delete())));
        if (ret.second) {
          // Actually apply to the state machine.   Must do this asynchronously.
          last_command_received_cluster_time_ = cluster_time;
          return true;
        } else {
          // We've already kicked off the request
          return false;
        }
      }
      void update_response(uint64_t sequence_number, std::pair<raft::slice, raft::util::call_on_delete> && response)
      {
        memo_[sequence_number] = std::move(response);
      }
      raft::slice response(uint64_t sequence_number) const
      {
        auto it = memo_.find(sequence_number);
        return it != memo_.end() ? raft::slice(reinterpret_cast<const uint8_t *>(it->second.first.data()), it->second.first.size()) : raft::slice();
      }
    };

    template<typename _Messages, typename _Builders, typename _Serialization, typename _Protocol, typename _Communicator, typename _StateMachine>
    class client_session_manager
    {
    public:
      typedef _Messages messages_type;
      typedef _Builders builders_type;
      typedef _Serialization serialization_type;
      typedef _Protocol protocol_type;
      typedef _Communicator communicator_type;
      typedef _StateMachine state_machine_type;
      typedef typename communicator_type::endpoint_type endpoint_type;
      typedef typename messages_type::client_request_traits_type client_request_traits_type;
      typedef typename messages_type::client_result_type client_result_type;
      typedef typename messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
      typedef typename messages_type::log_entry_traits_type log_entry_traits_type;
      typedef typename messages_type::log_entry_command_traits_type log_entry_command_traits_type;
      typedef typename messages_type::linearizable_command_traits_type linearizable_command_traits_type;
      typedef typename messages_type::linearizable_command_traits_type::arg_type linearizable_command_arg_type;
      typedef typename messages_type::client_response_traits_type client_response_traits_type;
      typedef typename builders_type::client_response_builder_type client_response_builder;
      typedef typename messages_type::open_session_request_traits_type open_session_request_traits_type;
      typedef typename messages_type::open_session_request_traits_type::arg_type open_session_request_arg_type;
      typedef typename builders_type::open_session_response_builder_type open_session_response_builder;
      typedef typename messages_type::close_session_request_traits_type close_session_request_traits_type;
      typedef typename messages_type::close_session_request_traits_type::arg_type close_session_request_arg_type;
      typedef typename builders_type::close_session_response_builder_type close_session_response_builder;
      typedef client_session_state<messages_type> client_session_type;
    private:
      protocol_type & protocol_;
      communicator_type & comm_;
      state_machine_type & state_machine_;
      // log index/session_id => (endpoint, term)
      std::map<uint64_t, std::pair<endpoint_type, uint64_t>> open_session_responses_;
      // log index => (endpoint, term)
      std::map<uint64_t, std::pair<endpoint_type, uint64_t>> close_session_responses_;
      // log index => (endpoint, term)
      std::map<uint64_t, std::pair<endpoint_type, uint64_t>> client_responses_;
      // session_id => session
      std::map<uint64_t, std::unique_ptr<client_session_type>> sessions_;      
      uint64_t session_timeout_nanos_ = 500000000000UL;
      // completion state of async state machine command application
      class command_application_state
      {
      private:
        uint64_t log_index_=std::numeric_limits<uint64_t>::max();
        uint64_t log_term_=std::numeric_limits<uint64_t>::max();
        uint64_t session_id_=std::numeric_limits<uint64_t>::max();
        uint64_t sequence_number_=std::numeric_limits<uint64_t>::max();
        size_t leader_id_=std::numeric_limits<size_t>::max();
      public:
        bool is_outstanding() const
        {
          return session_id_ < std::numeric_limits<uint64_t>::max();
        }
        uint64_t log_index() const
        {
          return log_index_;
        }
        uint64_t session_id() const
        {
          return session_id_;
        }
        uint64_t sequence_number() const
        {
          return sequence_number_;
        }
        client_response_builder & response(client_response_builder & bld, uint64_t expected_term) const
        {
          return bld.result(log_term_ == expected_term ? messages_type::client_result_success() : messages_type::client_result_not_leader()).index(log_index_).leader_id(leader_id_);
        }
        void begin(uint64_t _log_index, uint64_t _log_term, uint64_t _session_id, size_t _leader_id, uint64_t _sequence_number)
        {
          BOOST_ASSERT(_session_id < std::numeric_limits<uint64_t>::max());
          BOOST_ASSERT(!is_outstanding());
          log_index_ = _log_index;
          log_term_ = _log_term;
          session_id_ = _session_id;
          leader_id_ = _leader_id;
          sequence_number_ = _sequence_number;
        }
        void finish()
        {
          log_index_ = std::numeric_limits<uint64_t>::max();
          log_term_ = std::numeric_limits<uint64_t>::max();
          session_id_ = std::numeric_limits<uint64_t>::max();
          leader_id_ = std::numeric_limits<size_t>::max();
          sequence_number_ = std::numeric_limits<size_t>::max();
        }
      };
      command_application_state command_application_;

      void expire_sessions(uint64_t cluster_time)
      {
        for(auto it = sessions_.begin(); it != sessions_.end(); ) {
          uint64_t expired = session_timeout_nanos_ < cluster_time ? cluster_time - session_timeout_nanos_ : 0U;
          if (it->second->last_command_received_cluster_time() < expired &&
              (!command_application_.is_outstanding() || command_application_.session_id() != it->first)) {
            it = sessions_.erase(it);
          } else {
            ++it;
          }
        }
      }

      // TODO: Send response
      void complete_command(bool ret, std::pair<raft::slice, raft::util::call_on_delete> && response)
      {
        auto session_id = command_application_.session_id();
        auto sess_it = sessions_.find(session_id);
        auto log_index = command_application_.log_index();
        if (sessions_.end() != sess_it) {
          sess_it->second->update_response(command_application_.sequence_number(), std::move(response));
          auto it = client_responses_.find(log_index);
          if (client_responses_.end() != it) {
            client_response_builder bld;
            auto resp = command_application_.response(bld, it->second.second).response(sess_it->second->response(command_application_.sequence_number())).finish();
            BOOST_LOG_TRIVIAL(info) << "[session_manager::complete_command] Session(" << session_id << ")"
                                    << " sending client response log entry " << log_index << " of type COMMAND due to completed state machine application: { "
                                    << "\"client_result\" : \"" << client_result_string(client_response_traits_type::result(resp)) << "\""
                                    << ", \"log_index\" : " << client_response_traits_type::index(resp)
                                    << ", \"leader_id\" : " << client_response_traits_type::leader_id(resp)
                                    << " }";
            comm_.send_client_response(std::move(resp), it->second.first);
            client_responses_.erase(it);
          }
        }
        command_application_.finish();
        // It's important to wait until after the command application is finished before
        // responding to the protocol, as the protocol may call back into the session with the
        // next log entry to be applied to the state machine.   If the command application isn't
        // finished, that log entry will bounce off of us and have to be retried.
        BOOST_LOG_TRIVIAL(info) << "[session_manager::complete_command] Completing application of log entry " << log_index << " of type COMMAND due to completed state machine application";
        protocol_.on_command_applied(log_index);
        // TODO: Think about retry logic.  Deterministic failures should be OK and will result in
        // the same state everywhere but non-deterministic failures have to be retried until success or
        // deterministic failure.
        BOOST_ASSERT(ret);
      }

      const char * client_result_string(client_result_type cr)
      {
        if (messages_type::client_result_success() == cr) {
          return "SUCCESS";
        } else if (messages_type::client_result_fail() == cr) {
          return "FAIL";
        } else if (messages_type::client_result_retry() == cr) {
          return "RETRY";
        } else if (messages_type::client_result_not_leader() == cr) {
          return "NOT_LEADER";
        } else if (messages_type::client_result_session_expired() == cr) {
          return "SESSION_EXPIRED";
        } else {
          return "UNKNOWN";
        }
      }
    public:
      void apply(log_entry_const_arg_type le, uint64_t idx, size_t leader_id)
      {
        auto cluster_time = log_entry_traits_type::cluster_time(le);
        // Expire sessions as needed
        expire_sessions(cluster_time);
        // Can't have multiple commands being applied concurrently, use the log as a buffer.
        // Technically we could probably allow sessions to be opened or closed while a state machine command
        // but I don't think it is worth the risk.
        if (command_application_.is_outstanding()) {
          BOOST_LOG_TRIVIAL(debug) << "[session_manager::apply] Log entry at " << idx << " cannot be applied because command is oustanding.";
          return;
        }
        auto log_term = log_entry_traits_type::term(le);
        if (log_entry_traits_type::is_command(le)) {
          typename serialization_type::log_entry_command_view_deserialization_type cmd_view(le);
          if (log_entry_command_traits_type::is_linearizable_command(cmd_view.view())) {
              auto lcmd = log_entry_command_traits_type::linearizable_command(cmd_view.view());
              auto session_id = linearizable_command_traits_type::session_id(lcmd);
              auto sequence_number = linearizable_command_traits_type::sequence_number(lcmd);
              auto sess_it = sessions_.find(session_id);
              if (sessions_.end() != sess_it) {
                // Check if already applied?
                bool should_apply = sess_it->second->apply(lcmd, cluster_time);
                if (should_apply) {
                  BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") initiating state machine application of log entry " << idx << " of type COMMAND";
                  command_application_.begin(idx, log_term, session_id, leader_id, sequence_number);
                  state_machine_.on_command(std::move(cmd_view), [this] (bool ret, std::pair<raft::slice, raft::util::call_on_delete> && response) { this->complete_command(ret, std::move(response)); });
                  return;
                } else {
                  auto it = client_responses_.find(idx);
                  BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") completing application of log entry " << idx << " of type COMMAND due to memoized response";;
                  protocol_.on_command_applied(idx);
                  if (client_responses_.end() != it) {
                    auto to_return = it->second.second == log_term ? messages_type::client_result_success() : messages_type::client_result_not_leader();
                    BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") sending client response log entry " << idx << " of type COMMAND due to memoized response: { "
                                            << "\"client_result\" : \"" << client_result_string(to_return) << "\""
                                            << ", \"log_index\" : " << idx
                                            << ", \"leader_id\" : " << leader_id
                                            << " }";
                    auto resp = client_response_builder().result(to_return).index(idx).leader_id(leader_id).response(sess_it->second->response(sequence_number)).finish();
                    comm_.send_client_response(std::move(resp), it->second.first);
                    client_responses_.erase(it);
                  }
                }
              } else {
                auto it = client_responses_.find(idx);
                BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") completing application of log entry " << idx << " of type COMMAND due to expired session";
                protocol_.on_command_applied(idx);
                if (client_responses_.end() != it) {
                  BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") sending client response log entry " << idx << " of type COMMAND due to expired session: { "
                                          << "\"client_result\" : \"" << client_result_string(messages_type::client_result_session_expired()) << "\""
                                          << ", \"log_index\" : " << idx
                                          << ", \"leader_id\" : " << leader_id
                                          << " }";
                  auto resp = client_response_builder().result(messages_type::client_result_session_expired()).index(idx).leader_id(leader_id).finish();
                  comm_.send_client_response(std::move(resp), it->second.first);
                  client_responses_.erase(it);
                }
              }

          } else if (log_entry_command_traits_type::is_open_session(cmd_view.view())) {
            // Use the log entry index as the session id and use that to determine where to send
            // the response if we need to.
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Opening session with id " << idx;
            sessions_[idx] = std::make_unique<client_session_type>(cluster_time);
            auto it = open_session_responses_.find(idx);
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type COMMAND opening session with id " << idx;
            protocol_.on_command_applied(idx);
            // TODO: Send not_leader if commit entry at index has different term
            if (open_session_responses_.end() != it) {
              if (it->second.second == log_term) {
                BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Sending open session response for session with id " << idx << " to endpoint " << it->second.first;
                auto resp = open_session_response_builder().session_id(idx).finish();
                comm_.send_open_session_response(std::move(resp), it->second.first);
              }
              open_session_responses_.erase(it);
            }
          } else if (log_entry_command_traits_type::is_close_session(cmd_view.view())) {
            auto close = log_entry_command_traits_type::close_session(cmd_view.view());
            auto session_id = close_session_request_traits_type::session_id(close);
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Closing session with id " << session_id;
            sessions_.erase(session_id);
            auto it = close_session_responses_.find(idx);
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type COMMAND closing session " << session_id;
            protocol_.on_command_applied(idx);
            // TODO: Send not_leader if commit entry at index has different term
            if (close_session_responses_.end() != it) {
              if (it->second.second == log_term) {                
                BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Sending close session response for session with id " << session_id << " to endpoint " << it->second.first;
                auto resp = close_session_response_builder().finish();
                comm_.send_close_session_response(std::move(resp), it->second.first);
              }
              close_session_responses_.erase(it);
            }
          }
        } else if (log_entry_traits_type::is_configuration(le)) {
          BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type CONFIGURATION";
          protocol_.on_command_applied(idx);
        } else if (log_entry_traits_type::is_noop(le)) {
          BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type NOOP";
          protocol_.on_command_applied(idx);
        } else {
          BOOST_ASSERT(false);
        }
      }

      void on_open_session(const endpoint_type & ep,
                           open_session_request_arg_type && req,
                           std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        auto buf = serialization_type::serialize_log_entry_command(std::move(req));
        auto ret = protocol_.on_command(std::move(buf), clock_now);
        if (messages_type::client_result_success() == std::get<0>(ret)) {
          open_session_responses_[std::get<1>(ret)] = std::make_pair(ep, std::get<2>(ret));
        } else {
          auto resp = open_session_response_builder().session_id(0).finish();
          comm_.send_open_session_response(std::move(resp), ep);
        }
      }

      void on_close_session(const endpoint_type & ep,
                            close_session_request_arg_type && req,
                            std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        auto buf = serialization_type::serialize_log_entry_command(std::move(req));
        auto ret = protocol_.on_command(std::move(buf), clock_now);
        if (messages_type::client_result_success() == std::get<0>(ret)) {
          close_session_responses_[std::get<1>(ret)] = std::make_pair(ep, std::get<2>(ret));
        } else {
          auto resp = close_session_response_builder().finish();
          comm_.send_close_session_response(std::move(resp), ep);
        }
      }

      void on_linearizable_command(const endpoint_type & ep,
                                   linearizable_command_arg_type && req,
                                   std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        auto buf = serialization_type::serialize_log_entry_command(std::move(req));
        auto ret = protocol_.on_command(std::move(buf), clock_now);
        if (messages_type::client_result_success() == std::get<0>(ret)) {
          client_responses_[std::get<1>(ret)] = std::make_pair(ep, std::get<2>(ret));
        } else {
          auto resp = client_response_builder().result(std::get<0>(ret)).index(0).leader_id(protocol_.leader_id()).finish();
          comm_.send_client_response(std::move(resp), ep);
        }
      }

      client_session_manager(protocol_type & protocol, communicator_type & comm, state_machine_type & state_machine)
        :
        protocol_(protocol),
        comm_(comm),
        state_machine_(state_machine)
      {
      }

      uint64_t session_timeout_nanos() const
      {
        return session_timeout_nanos_;
      }

      std::size_t size() const
      {
        return sessions_.size();
      }
    };
  }
}
  
#endif
