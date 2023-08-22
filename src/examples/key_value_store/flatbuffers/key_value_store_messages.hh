#ifndef __RAFT_KEY_VALUE_STORE_FLATBUFFER_MESSAGES_HH__
#define __RAFT_KEY_VALUE_STORE_FLATBUFFER_MESSAGES_HH__

#include <memory>
#include <string_view>

#include "key_value_store_generated.h"
#include "slice.hh"
#include "../../../util/call_on_delete.hh"

namespace raft {
  namespace examples {
    namespace key_value_store {
      struct put_request_traits
      {
        typedef std::pair<const message *, raft::util::call_on_delete> arg_type;
        typedef const arg_type & const_arg_type;
        static const put_request *  get_put_request(const_arg_type ae)
        {
          return  ae.first->inner_as_put_request();
        }      
        static slice key(const_arg_type cr)
        {
          return slice(reinterpret_cast<const uint8_t *>(get_put_request(cr)->key()->c_str()),
                       get_put_request(cr)->key()->size());
        }
        static slice value(const_arg_type cr)
        {
          return slice(reinterpret_cast<const uint8_t *>(get_put_request(cr)->value()->c_str()),
                       get_put_request(cr)->value()->size());
        }
      };

      struct put_response_traits
      {
        typedef std::pair<const message *, raft::util::call_on_delete> arg_type;
        typedef const arg_type & const_arg_type;
        static const put_response *  get_put_response(const_arg_type ae)
        {
          return  ae.first->inner_as_put_response();
        }      
      };

      struct get_request_traits
      {
        typedef std::pair<const message *, raft::util::call_on_delete> arg_type;
        typedef const arg_type & const_arg_type;
        static const get_request *  get_get_request(const_arg_type ae)
        {
          return  ae.first->inner_as_get_request();
        }      
        static slice key(const_arg_type cr)
        {
          return slice(reinterpret_cast<const uint8_t *>(get_get_request(cr)->key()->c_str()),
                       get_get_request(cr)->key()->size());
        }
      };

      struct get_response_traits
      {
        typedef std::pair<const message *, raft::util::call_on_delete> arg_type;
        typedef const arg_type & const_arg_type;
        static const get_response *  get_get_response(const_arg_type ae)
        {
          return  ae.first->inner_as_get_response();
        }      
        static slice value(const_arg_type cr)
        {
          return slice(reinterpret_cast<const uint8_t *>(get_get_response(cr)->value()->c_str()),
                       get_get_response(cr)->value()->size());
        }
      };

      class messages
      {
      public:
        typedef put_request put_request_type;
        typedef put_request_traits put_request_traits_type;
        typedef put_response put_response_type;
        typedef put_response_traits put_response_traits_type;
        typedef get_request get_request_type;
        typedef get_request_traits get_request_traits_type;
        typedef get_response get_response_type;
        typedef get_response_traits get_response_traits_type;
      };      

      template<typename _Derived, typename _FlatType>
      class message_builder_base
      {
      public:
        typedef _FlatType fbs_type;
        typedef typename _FlatType::Builder fbs_builder_type;
      private:
        std::unique_ptr<flatbuffers::FlatBufferBuilder> fbb_;
      public:
        flatbuffers::FlatBufferBuilder & fbb()
        {
          if (!fbb_) {
            fbb_ = std::make_unique<flatbuffers::FlatBufferBuilder>();
          }
          return *fbb_;
        }
        std::pair<const message *, raft::util::call_on_delete> finish()
        {
          static_cast<_Derived *>(this)->preinitialize();
          fbs_builder_type bld(fbb());
          static_cast<_Derived *>(this)->initialize(&bld);
          auto rv = bld.Finish();
          auto m = Createmessage(fbb(), raft::fbs::any_messageTraits<fbs_type>::enum_value, rv.Union());
          fbb().FinishSizePrefixed(m);
          auto obj = GetSizePrefixedmessage(fbb().GetBufferPointer());
          BOOST_ASSERT(fbb().GetBufferPointer()+sizeof(::flatbuffers::uoffset_t) == ::flatbuffers::GetBufferStartFromRootPointer(obj));
          auto ret = std::pair<const message *, raft::util::call_on_delete>(obj, [fbb = fbb_.release()]() { delete fbb; });
          return ret;
        }
      };

      class put_request_builder : public message_builder_base<put_request_builder, put_request>
      {
      private:
        ::flatbuffers::Offset<::flatbuffers::String> key_;
        ::flatbuffers::Offset<::flatbuffers::String> value_;
      public:
        void preinitialize()
        {
        }
      
        void initialize(fbs_builder_type * bld)
        {
          bld->add_key(key_);
          bld->add_value(value_);
        }
        put_request_builder & key(raft::slice && val)
        {
          key_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
                                    raft::slice::buffer_size(val));
          return *this;
        }
        put_request_builder & value(raft::slice && val)
        {
          value_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
                                    raft::slice::buffer_size(val));
          return *this;
        }
      };
      class put_response_builder : public message_builder_base<put_response_builder, put_response>
      {
      public:
        void preinitialize()
        {
        }
      
        void initialize(fbs_builder_type * bld)
        {
        }
      };
      class get_request_builder : public message_builder_base<get_request_builder, get_request>
      {
      private:
        ::flatbuffers::Offset<::flatbuffers::String> key_;
      public:
        void preinitialize()
        {
        }
      
        void initialize(fbs_builder_type * bld)
        {
          bld->add_key(key_);
        }
        get_request_builder & key(raft::slice && val)
        {
          key_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
                                    raft::slice::buffer_size(val));
          return *this;
        }
      };
      class get_response_builder : public message_builder_base<get_response_builder, get_response>
      {
      private:
        ::flatbuffers::Offset<::flatbuffers::String> value_;
      public:
        void preinitialize()
        {
        }
      
        void initialize(fbs_builder_type * bld)
        {
          bld->add_value(value_);
        }
        get_response_builder & value(raft::slice && val)
        {
          value_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
                                    raft::slice::buffer_size(val));
          return *this;
        }
      };

      class builders
      {
      public:
        typedef put_request_builder put_request_builder_type;
        typedef put_response_builder put_response_builder_type;
        typedef get_request_builder get_request_builder_type;
        typedef get_response_builder get_response_builder_type;
      };
    }
  }
}

#endif
