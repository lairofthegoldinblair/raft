#ifndef __BASIC_FILE_OBJECT_H__
#define __BASIC_FILE_OBJECT_H__

#include <boost/asio/basic_io_object.hpp>
#include "asio/file_base.hh"

template<typename FileService>
class basic_file_object : public boost::asio::basic_io_object<FileService>, file_base
{
public:
  typedef typename FileService::native_handle_type native_handle_type;
  typedef basic_file_object<FileService> lowest_layer_type;
private:
  struct initiate_async_read
  {
    template <typename ReadHandler, typename MutableBufferSequence>
    void operator()(BOOST_ASIO_MOVE_ARG(ReadHandler) handler,
        basic_file_object* self, const MutableBufferSequence& buffers) const
    {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a ReadHandler.
      BOOST_ASIO_READ_HANDLER_CHECK(ReadHandler, handler) type_check;

      boost::asio::detail::non_const_lvalue<ReadHandler> handler2(handler);
      self->get_service().impl().async_read(
          self->get_implementation(), buffers, handler2.value);
    }
  };

  struct initiate_async_read_at
  {
    template <typename ReadHandler, typename MutableBufferSequence>
    void operator()(BOOST_ASIO_MOVE_ARG(ReadHandler) handler,
                    basic_file_object* self, const MutableBufferSequence& buffers, uint64_t offset) const
    {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a ReadHandler.
      BOOST_ASIO_READ_HANDLER_CHECK(ReadHandler, handler) type_check;

      boost::asio::detail::non_const_lvalue<ReadHandler> handler2(handler);
      self->get_service().impl().async_read_at(self->get_implementation(), buffers, offset, handler2.value);
    }
  };

  struct initiate_async_write
  {
    template <typename WriteHandler, typename ConstBufferSequence>
    void operator()(BOOST_ASIO_MOVE_ARG(WriteHandler) handler,
        basic_file_object* self, const ConstBufferSequence& buffers) const
    {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a WriteHandler.
      BOOST_ASIO_WRITE_HANDLER_CHECK(WriteHandler, handler) type_check;

      boost::asio::detail::non_const_lvalue<WriteHandler> handler2(handler);
      self->get_service().impl().async_write(
          self->get_implementation(), buffers, handler2.value);
    }
  };

  struct initiate_async_sync
  {
    template <typename FileOpHandler>
    void operator()(BOOST_ASIO_MOVE_ARG(FileOpHandler) handler,
        basic_file_object* self) const
    {
      boost::asio::detail::non_const_lvalue<FileOpHandler> handler2(handler);
      self->get_service().impl().async_sync(
          self->get_implementation(), handler2.value);
    }
  };

public:

  explicit basic_file_object(boost::asio::io_service & ios)
    :
    boost::asio::basic_io_object<FileService>(ios)
  {
  }

  explicit basic_file_object(boost::asio::io_service & ios,
			     native_handle_type & handle)
    :
    boost::asio::basic_io_object<FileService>(ios)
  {
    boost::system::error_code ec;
    this->get_service().assign(this->get_implementation(), handle, ec);
    boost::asio::detail::throw_error(ec, "error");
  }

  bool is_open()
  {
    return this->get_service().is_open(this->get_implementation());
  }

  uint64_t seek(uint64_t offset, seek_type ty)
  {
    return this->get_service().seek(this->get_implementation(), offset, ty);
  }

  template<typename MutableBufferSequence, typename ReadHandler>
  BOOST_ASIO_INITFN_RESULT_TYPE(ReadHandler, void (boost::system::error_code, std::size_t))
    async_read(const MutableBufferSequence & buffers,
	       BOOST_ASIO_MOVE_ARG(ReadHandler) handler)
  {
    return boost::asio::async_initiate<ReadHandler,
                                       void (boost::system::error_code, std::size_t)>(initiate_async_read(), handler, this, buffers);
  }  

  template<typename MutableBufferSequence, typename ReadHandler>
  BOOST_ASIO_INITFN_RESULT_TYPE(ReadHandler, void (boost::system::error_code, std::size_t))
    async_read_at(const MutableBufferSequence & buffers,
                  uint64_t offset,
                  BOOST_ASIO_MOVE_ARG(ReadHandler) handler)
  {
    return boost::asio::async_initiate<ReadHandler,
                                       void (boost::system::error_code, std::size_t)>(initiate_async_read_at(), handler, this, buffers, offset);
  }  

  template<typename ConstBufferSequence, typename WriteHandler>
  BOOST_ASIO_INITFN_RESULT_TYPE(WriteHandler, void (boost::system::error_code, std::size_t))
    async_write(const ConstBufferSequence & buffers,
		BOOST_ASIO_MOVE_ARG(WriteHandler) handler)
  {
    return boost::asio::async_initiate<WriteHandler,
                                       void (boost::system::error_code, std::size_t)>(initiate_async_write(), handler, this, buffers);
    
  }  

  template<typename FileOpHandler>
  BOOST_ASIO_INITFN_RESULT_TYPE(FileOpHandler, void (boost::system::error_code))
    async_sync(BOOST_ASIO_MOVE_ARG(FileOpHandler) handler)
  {
    return boost::asio::async_initiate<FileOpHandler,
                                       void (boost::system::error_code)>(initiate_async_sync(), handler, this);
  }  
};

#endif
