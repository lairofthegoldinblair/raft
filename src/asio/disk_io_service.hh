#ifndef __DISK_IO_SERVICE_HH__
#define __DISK_IO_SERVICE_HH__

#include <unistd.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/detail/buffer_sequence_adapter.hpp>
#include <boost/asio/detail/fenced_block.hpp>
#include <boost/asio/detail/handler_alloc_helpers.hpp>
#include <boost/asio/detail/handler_invoke_helpers.hpp>
#include <boost/asio/detail/operation.hpp>

#include "asio/file_base.hh"

// This is an implementation of a thread (pool) based async disk service.   It is modelled on the
// thread based resolver service in ASIO.
// TODO: Should we add a strand here to prevent out of order IO ops on a file?
// If we have multiple threads in the thread pool we'd need that to be true I think (i.e.
// it would be invalid for multiple threads to be working concurrently on IOs to a single file.
struct file_ops
{
  typedef int file_type;
  static const int invalid_file = -1;

  static void close(file_type f)
  {
    ::close(f);
  }
  
  static uint64_t seek(file_type f, uint64_t offset, file_base::seek_type whence)
  {
    return ::lseek(f, offset, whence);
  }
  
  static std::size_t read(file_type f, iovec * buf, std::size_t count,
			  boost::system::error_code & ec)
  {
    if (f == invalid_file) {
      ec = boost::asio::error::bad_descriptor;
      return 0;
    }

    ssize_t bytes = ::readv(f, buf, count);
    if (bytes > 0) {
      ec = boost::system::error_code();
      return bytes;
    }

    if (0 == bytes) {
      ec = boost::asio::error::eof;
      return 0;
    }

    ec = boost::system::error_code(errno, boost::system::generic_category());
    return 0;
  }

  static std::size_t read_at(file_type f, iovec * buf, std::size_t count, uint64_t offset,
                             boost::system::error_code & ec)
  {
    if (f == invalid_file) {
      ec = boost::asio::error::bad_descriptor;
      return 0;
    }

    ssize_t bytes = ::preadv(f, buf, count, offset);
    if (bytes > 0) {
      ec = boost::system::error_code();
      return bytes;
    }

    if (0 == bytes) {
      ec = boost::asio::error::eof;
      return 0;
    }

    ec = boost::system::error_code(errno, boost::system::generic_category());
    return 0;
  }

  static std::size_t write(file_type f, iovec * buf, std::size_t count,
			   boost::system::error_code & ec)
  {
    if (f == invalid_file) {
      ec = boost::asio::error::bad_descriptor;
      return 0;
    }

    ssize_t bytes = ::writev(f, buf, count);
    if (bytes >= 0) {
      ec = boost::system::error_code();
      return bytes;
    }

    ec = boost::system::error_code(errno, boost::system::generic_category());
    return 0;
  }

  static void sync(file_type f, boost::system::error_code & ec)
  {
    if (f == invalid_file) {
      ec = boost::asio::error::bad_descriptor;
      return;
    }

    int ret = ::fsync(f);
    
    if (ret == 0) {
      ec = boost::system::error_code();
    } else {      
      ec = boost::system::error_code(errno, boost::system::generic_category());
    }
  }
};

template<typename MutableBufferSequence, typename Handler>
class disk_read_operation : public boost::asio::detail::operation
{
private:
  file_ops::file_type file_;
  boost::asio::detail::io_context_impl & io_context_impl_;
  MutableBufferSequence buffers_;
  Handler handler_;
  boost::system::error_code ec_;
  std::size_t bytes_transferred_;
public:
  disk_read_operation(file_ops::file_type f,
                         const MutableBufferSequence & buffers,
                         boost::asio::detail::io_context_impl & io_context_impl,
                         Handler & handler)
    :
    boost::asio::detail::operation(&disk_read_operation::do_complete),
    file_(f),
    buffers_(buffers),
    io_context_impl_(io_context_impl),
    handler_(BOOST_ASIO_MOVE_CAST(Handler)(handler))
  {
  }

  static void do_complete(void * owner,
  			  boost::asio::detail::operation * base,
  			  const boost::system::error_code& /*ec*/,
  			  std::size_t /*bytes_transferred*/)
  {
    disk_read_operation * op (static_cast<disk_read_operation *>(base));
    if (owner && owner != &op->io_context_impl_) {
      // In the worker io_context, so do the work and then signal main io_context
      // that we are done
      boost::asio::detail::buffer_sequence_adapter<boost::asio::mutable_buffer,
						   MutableBufferSequence> bufs(op->buffers_);
      op->bytes_transferred_ = file_ops::read(op->file_, bufs.buffers(), bufs.count(), op->ec_);
      // The point of post_deferred_completion instead of post_immediate_completion
      // is that this operation already is accounted for as a work item on the main service
      // via an direct call to work_started() in start_operation so we don't want to double count it.
      op->io_context_impl_.post_deferred_completion(op);
    } else {
      // Make a copy of the handler so that the memory can be deallocated before                                                                                        
      // the upcall is made. Even if we're not about to make an upcall, a                                                                                               
      // sub-object of the handler may be the true owner of the memory associated                                                                                       
      // with the handler. Consequently, a local copy of the handler is required                                                                                        
      // to ensure that any owning sub-object remains valid until after we have                                                                                         
      // deallocated the memory here.
      boost::asio::detail::binder2<Handler, boost::system::error_code, std::size_t>
	handler(op->handler_, op->ec_, op->bytes_transferred_);
      ptr p = { boost::asio::detail::addressof(handler.handler_), op, op };
      p.reset();
      
      // Make the upcall if required.                                                                                                                                   
      if (owner) {
	boost::asio::detail::fenced_block b(boost::asio::detail::fenced_block::half);
	// TODO: Shoud this be boost_asio_handler_invoke_helpers::invoke(handler, handler);
	boost_asio_handler_invoke_helpers::invoke(handler, handler.handler_);
      }
    }
  }

  BOOST_ASIO_DEFINE_HANDLER_PTR(disk_read_operation);
};

template<typename MutableBufferSequence, typename Handler>
class disk_read_at_operation : public boost::asio::detail::operation
{
private:
  file_ops::file_type file_;
  boost::asio::detail::io_context_impl & io_context_impl_;
  MutableBufferSequence buffers_;
  uint64_t offset_;
  Handler handler_;
  boost::system::error_code ec_;
  std::size_t bytes_transferred_;
public:
  disk_read_at_operation(file_ops::file_type f,
                         const MutableBufferSequence & buffers,
                         uint64_t offset,
                         boost::asio::detail::io_context_impl & io_context_impl,
                         Handler & handler)
    :
    boost::asio::detail::operation(&disk_read_at_operation::do_complete),
    file_(f),
    buffers_(buffers),
    offset_(offset),
    io_context_impl_(io_context_impl),
    handler_(BOOST_ASIO_MOVE_CAST(Handler)(handler))
  {
  }

  static void do_complete(void * owner,
  			  boost::asio::detail::operation * base,
  			  const boost::system::error_code& /*ec*/,
  			  std::size_t /*bytes_transferred*/)
  {
    disk_read_at_operation * op (static_cast<disk_read_at_operation *>(base));
    if (owner && owner != &op->io_context_impl_) {
      // In the worker io_context, so do the work and then signal main io_context
      // that we are done
      boost::asio::detail::buffer_sequence_adapter<boost::asio::mutable_buffer,
						   MutableBufferSequence> bufs(op->buffers_);
      op->bytes_transferred_ = file_ops::read_at(op->file_, bufs.buffers(), bufs.count(), op->offset_, op->ec_);
      // The point of post_deferred_completion instead of post_immediate_completion
      // is that this operation already is accounted for as a work item on the main service
      // via an direct call to work_started() in start_operation so we don't want to double count it.
      op->io_context_impl_.post_deferred_completion(op);
    } else {
      // Make a copy of the handler so that the memory can be deallocated before                                                                                        
      // the upcall is made. Even if we're not about to make an upcall, a                                                                                               
      // sub-object of the handler may be the true owner of the memory associated                                                                                       
      // with the handler. Consequently, a local copy of the handler is required                                                                                        
      // to ensure that any owning sub-object remains valid until after we have                                                                                         
      // deallocated the memory here.
      boost::asio::detail::binder2<Handler, boost::system::error_code, std::size_t>
	handler(op->handler_, op->ec_, op->bytes_transferred_);
      ptr p = { boost::asio::detail::addressof(handler.handler_), op, op };
      p.reset();
      
      // Make the upcall if required.                                                                                                                                   
      if (owner) {
	boost::asio::detail::fenced_block b(boost::asio::detail::fenced_block::half);
	// TODO: Shoud this be boost_asio_handler_invoke_helpers::invoke(handler, handler);
	boost_asio_handler_invoke_helpers::invoke(handler, handler.handler_);
      }
    }
  }

  BOOST_ASIO_DEFINE_HANDLER_PTR(disk_read_at_operation);
};

template<typename ConstBufferSequence, typename Handler>
class disk_write_operation : public boost::asio::detail::operation
{
private:
  file_ops::file_type file_;
  boost::asio::detail::io_context_impl & io_context_impl_;
  ConstBufferSequence buffers_;
  Handler handler_;
  boost::system::error_code ec_;
  std::size_t bytes_transferred_;
public:
  disk_write_operation(file_ops::file_type f,
		      const ConstBufferSequence & buffers,
		      boost::asio::detail::io_context_impl & io_context_impl,
		      Handler & handler)
    :
    boost::asio::detail::operation(&disk_write_operation::do_complete),
    file_(f),
    buffers_(buffers),
    io_context_impl_(io_context_impl),
    handler_(BOOST_ASIO_MOVE_CAST(Handler)(handler))
  {
  }

  static void do_complete(void * owner,
			  boost::asio::detail::operation * base,
			  const boost::system::error_code& /*ec*/,
			  std::size_t /*bytes_transferred*/)
  {
    disk_write_operation * op (static_cast<disk_write_operation *>(base));
    if (owner && owner != &op->io_context_impl_) {
      // In the worker io_context, so do the work and then signal main io_context
      // that we are done
      boost::asio::detail::buffer_sequence_adapter<boost::asio::const_buffer,
						   ConstBufferSequence> bufs(op->buffers_);
      op->bytes_transferred_ = file_ops::write(op->file_, bufs.buffers(), bufs.count(), op->ec_);
      op->io_context_impl_.post_deferred_completion(op);
    } else {
      // Make a copy of the handler so that the memory can be deallocated before                                                                                        
      // the upcall is made. Even if we're not about to make an upcall, a                                                                                               
      // sub-object of the handler may be the true owner of the memory associated                                                                                       
      // with the handler. Consequently, a local copy of the handler is required                                                                                        
      // to ensure that any owning sub-object remains valid until after we have                                                                                         
      // deallocated the memory here.
      boost::asio::detail::binder2<Handler, boost::system::error_code, std::size_t>
	handler(op->handler_, op->ec_, op->bytes_transferred_);
      ptr p = { boost::asio::detail::addressof(handler.handler_), op, op };
      p.reset();
      
      // Make the upcall if required.                                                                                                                                   
      if (owner) {
	boost::asio::detail::fenced_block b(boost::asio::detail::fenced_block::half);
	// TODO: Shoud this be boost_asio_handler_invoke_helpers::invoke(handler, handler);
	boost_asio_handler_invoke_helpers::invoke(handler, handler.handler_);
      }
    }
  }

  BOOST_ASIO_DEFINE_HANDLER_PTR(disk_write_operation);
};

template<typename Handler>
class disk_sync_operation : public boost::asio::detail::operation
{
private:
  file_ops::file_type file_;
  boost::asio::detail::io_context_impl & io_context_impl_;
  Handler handler_;
  boost::system::error_code ec_;
public:
  disk_sync_operation(file_ops::file_type f,
		      boost::asio::detail::io_context_impl & io_context_impl,
		      Handler & handler)
    :
    boost::asio::detail::operation(&disk_sync_operation::do_complete),
    file_(f),
    io_context_impl_(io_context_impl),
    handler_(BOOST_ASIO_MOVE_CAST(Handler)(handler))
  {
  }

  static void do_complete(void * owner,
			  boost::asio::detail::operation * base,
			  const boost::system::error_code& /*ec*/,
			  std::size_t /*bytes_transferred*/)
  {
    disk_sync_operation * op (static_cast<disk_sync_operation *>(base));
    if (owner && owner != &op->io_context_impl_) {
      // In the worker io_context, so do the work and then signal main io_context
      // that we are done
      file_ops::sync(op->file_, op->ec_);
      op->io_context_impl_.post_deferred_completion(op);
    } else {
      // Make a copy of the handler so that the memory can be deallocated before                                                                                        
      // the upcall is made. Even if we're not about to make an upcall, a                                                                                               
      // sub-object of the handler may be the true owner of the memory associated                                                                                       
      // with the handler. Consequently, a local copy of the handler is required                                                                                        
      // to ensure that any owning sub-object remains valid until after we have                                                                                         
      // deallocated the memory here.
      boost::asio::detail::binder1<Handler, boost::system::error_code>
	handler(op->handler_, op->ec_);
      ptr p = { boost::asio::detail::addressof(handler.handler_), op, op };
      p.reset();
      
      // Make the upcall if required.                                                                                                                                   
      if (owner) {
	boost::asio::detail::fenced_block b(boost::asio::detail::fenced_block::half);
	// TODO: Shoud this be boost_asio_handler_invoke_helpers::invoke(handler, handler);
	boost_asio_handler_invoke_helpers::invoke(handler, handler.handler_);
      }
    }
  }

  BOOST_ASIO_DEFINE_HANDLER_PTR(disk_sync_operation);
};

namespace detail {
  class disk_io_service
  {
  public:
    typedef file_ops::file_type native_handle_type;

    // TODO: May eventually want some base class e.g. to support Win32 FileIO
    struct implementation_type
    {
      file_ops::file_type file_;
    };

  private:
    boost::asio::detail::io_context_impl & main_service_impl_;

    // Below is the private io_context infrastructure used to manage the
    // disk worker thread pool.
    std::mutex mutex_;
    std::unique_ptr<boost::asio::io_context> worker_io_context_;
    boost::asio::detail::io_context_impl & worker_context_impl_;
    std::unique_ptr<boost::asio::io_context::work> worker_io_context_work_;
    // TODO: Make this a thread pool
    std::unique_ptr<std::thread> worker_thread_;

    void start_worker_threads()
    {
      std::unique_lock<std::mutex> lk(mutex_);
      if (!worker_thread_) {
	worker_thread_.reset(new std::thread([this]() { this->worker_io_context_->run(); }));
      }
    }

    void start_operation(boost::asio::detail::operation * op)
    {
      start_worker_threads();
      // ????  What is this about ????
      // I think this is a bit of a hack to let the main service know that there is outstanding work without
      // actually enqueuing anything (work_started() is usually called as part of post_immediate_completion()).
      // Later on when we are ready to have the main service call the handler we will use post_deferred_completion()
      // which assumes that work_started() has been called.  Without work_started() having been called, any run call
      // on the main service will simply return immediately.
      main_service_impl_.work_started();
      // I think point here is that the operation doesn't need to wait for any condition
      // to be processed, just get it assigned to a thread
      worker_context_impl_.post_immediate_completion(op, false);
    }
    
  public:

    disk_io_service(boost::asio::io_context & ios)
      :
      main_service_impl_(boost::asio::use_service<boost::asio::detail::io_context_impl>(ios)),
      worker_io_context_(new boost::asio::io_context()),
      worker_context_impl_(boost::asio::use_service<boost::asio::detail::io_context_impl>(*worker_io_context_)),
      worker_io_context_work_(new boost::asio::io_context::work(*worker_io_context_))
    {
    }

    ~disk_io_service()
    {
      shutdown_service();
    }
    
    void shutdown_service()
    {
      worker_io_context_work_.reset();
      if(!!worker_io_context_) {
	worker_io_context_->stop();
	if(!!worker_thread_) {
	  worker_thread_->join();
	  worker_thread_.reset();
	}
	worker_io_context_.reset();
      }
    }

    void fork_service(boost::asio::io_context::fork_event fe)
    {
      // TODO:
    }

    void construct(implementation_type & impl)
    {
      impl.file_ = file_ops::invalid_file;
    }

    void destroy(implementation_type & impl)
    {
      if (impl.file_ != file_ops::invalid_file) {
	file_ops::close(impl.file_);
      }
    }

    void cancel(implementation_type & impl)
    {
      // TODO:
    }

    bool is_open(implementation_type & impl) const
    {
      return impl.file_ != file_ops::invalid_file;
    }

    uint64_t seek(implementation_type & impl, uint64_t offset, file_base::seek_type whence) const
    {
      return file_ops::seek(impl.file_, offset, whence);
    }

    boost::system::error_code assign(implementation_type & impl, native_handle_type fd,
				     boost::system::error_code & ec) const
    {
      if (is_open(impl)) {
	ec = boost::asio::error::already_open;
	return ec;
      }
      impl.file_ = fd;

      ec = boost::system::error_code();
      return ec;      
    }
    
    template<typename MutableBufferSequence, typename ReadHandler>
    void async_read(implementation_type & impl,
		    const MutableBufferSequence & buffers,
		    ReadHandler & handler)
    {
      // Allocate an operation and start it.
      typedef disk_read_operation<MutableBufferSequence, ReadHandler> op_type;
      typename op_type::ptr p = { boost::asio::detail::addressof(handler),
				  op_type::ptr::allocate(handler),
				  nullptr };

      p.p = new (p.v) op_type(impl.file_, buffers, main_service_impl_, handler);
      
      start_operation(p.p);

      // Clear pointers so that d'tor doesn't free them
      p.v = p.p = nullptr;
    }
  
    template<typename MutableBufferSequence, typename ReadHandler>
    void async_read_at(implementation_type & impl,
                       const MutableBufferSequence & buffers,
                       uint64_t offset, 
                       ReadHandler & handler)
    {
      // Allocate an operation and start it.
      typedef disk_read_at_operation<MutableBufferSequence, ReadHandler> op_type;
      typename op_type::ptr p = { boost::asio::detail::addressof(handler),
				  op_type::ptr::allocate(handler),
				  nullptr };

      p.p = new (p.v) op_type(impl.file_, buffers, offset, main_service_impl_, handler);
      
      start_operation(p.p);

      // Clear pointers so that d'tor doesn't free them
      p.v = p.p = nullptr;
    }
  
    template<typename ConstBufferSequence, typename WriteHandler>
    void async_write(implementation_type & impl,
		     const ConstBufferSequence & buffers,
		     WriteHandler & handler)
    {
      // Allocate an operation and start it.
      typedef disk_write_operation<ConstBufferSequence, WriteHandler> op_type;
      typename op_type::ptr p = { boost::asio::detail::addressof(handler),
				  op_type::ptr::allocate(handler),
				  nullptr };

      p.p = new (p.v) op_type(impl.file_, buffers, main_service_impl_, handler);
      
      start_operation(p.p);

      // Clear pointers so that d'tor doesn't free them
      p.v = p.p = nullptr;
    }

    template<typename FileOpHandler>
    void async_sync(implementation_type & impl,
		    FileOpHandler & handler)
    {
      // Allocate an operation and start it.
      typedef disk_sync_operation<FileOpHandler> op_type;
      typename op_type::ptr p = { boost::asio::detail::addressof(handler),
				  op_type::ptr::allocate(handler),
				  nullptr };

      p.p = new (p.v) op_type(impl.file_, main_service_impl_, handler);
      
      start_operation(p.p);

      // Clear pointers so that d'tor doesn't free them
      p.v = p.p = nullptr;
    }
  };
}

class disk_io_service : public boost::asio::detail::service_base<disk_io_service>
{
private:
  // Prepare for platform specific impl type
  typedef detail::disk_io_service service_impl_type;
  
  service_impl_type service_impl_;
public:
  typedef typename service_impl_type::implementation_type implementation_type;
  typedef typename service_impl_type::native_handle_type native_handle_type;

  service_impl_type & impl()
  {
    return service_impl_;
  }
  
  explicit disk_io_service(boost::asio::io_context & ios)
    :
    boost::asio::detail::service_base<disk_io_service>(ios),
    service_impl_(ios)
  {
  }

  void shutdown_service()
  {
    service_impl_.shutdown_service();
  }

  void fork_service(boost::asio::io_context::fork_event fe)
  {
    service_impl_.fork_service(fe);
  }

  void construct(implementation_type & impl)
  {
    service_impl_.construct(impl);
  }

  void destroy(implementation_type & impl)
  {
    service_impl_.destroy(impl);
  }

  void cancel(implementation_type & impl)
  {
    service_impl_.cancel(impl);
  }

  bool is_open(implementation_type & impl) const
  {
    return service_impl_.is_open(impl);
  }

  uint64_t seek(implementation_type & impl, uint64_t offset, file_base::seek_type whence) const
  {
    return service_impl_.seek(impl, offset, whence);
  }

  boost::system::error_code assign(implementation_type & impl, native_handle_type fd, boost::system::error_code & ec) const
  {
    return service_impl_.assign(impl, fd, ec);
  }
};

#endif
