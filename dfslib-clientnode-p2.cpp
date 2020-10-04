#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;


using dfs_service::DFSService;
using dfs_service::Fname;
using dfs_service::Fstat;
using dfs_service::ListInfo;
using dfs_service::param;
using dfs_service::FileBytes;
using dfs_service::StoreBytes;
using dfs_service::Response;

extern dfs_log_level_e DFS_LOG_LEVEL;


using FileRequestType = dfs_service::FileRequest;
using FileListResponseType = dfs_service::FileList;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

    // This request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

	Fname request;
	std::string cd = ClientId();
        request.set_filename(filename);
        request.set_cid(cd);
        param reply;

        ClientContext context;
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
        context.set_deadline(deadline);

        Status status = service_stub->RequestWriteAccess(&context,request,&reply);

        if (status.ok())
        {
                return StatusCode::OK;

        }
        else
        {
                return status.error_code();

        }

}
    //
    // Send request to store a file here.
    //
    // Perform check to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // Request a write lock before attempting to store.
    //
    // If the write lock request fails, it returns a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

        std::string path = WrapPath(filename);
        std::uint32_t client_crc = dfs_file_checksum(path, &this->crc_table);
        StoreBytes request;
        param val,reply;
	Fname req;
        request.set_file(filename);
        request.set_checksum(client_crc);
	req.set_filename(filename);
	req.set_cid(ClientId());
        ClientContext context, context_acc;
        size_t count;
        size_t buffer_size = 10*1024;
        char * buffer = new char [buffer_size];


        std::ifstream inFile;
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
        context.set_deadline(deadline);
        context_acc.set_deadline(deadline);
        Status fstatus = service_stub->RequestWriteAccess(&context_acc,req,&reply);

        if (fstatus.ok())
        {
		std::cout<<"LOCK GIVEN "<<std::endl;
        }
        else
        {
        
		delete [] buffer;
                return fstatus.error_code();

        }

        std::unique_ptr<ClientWriter<StoreBytes> > writer(service_stub->Store(&context, &val));
		inFile.open(path.c_str());

        
		if(inFile.fail())
        
		{
                
			delete [] buffer;
                
			return StatusCode::NOT_FOUND;
        
		}

        while(!inFile.eof())
        {
                inFile.read(buffer, buffer_size);
                count = inFile.gcount();

                std::string sent(reinterpret_cast< char const *> (buffer), count);
                request.set_sb(sent);
                request.set_flen(count);

                if(!writer->Write(request)){
                        break;
                }

        }
        writer->WritesDone();
        Status status = writer->Finish();
        inFile.close();
        delete [] buffer;

        if (status.ok())
        {

                return StatusCode::OK;
        }
        else
	{

                return status.error_code();

        }

}

    //
    // Request to fetch a file here. 
    //
    // Perform check to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Compare mtime on local files to the server's mtime
    //
grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

	Fname request;
	Fstat reply;
	Status fstatus;
        request.set_filename(filename);
        const std::string path = WrapPath(request.filename());
	std::uint32_t client_crc = dfs_file_checksum(path, &this->crc_table);
	FileBytes info;
	long modtime = 0;
        ClientContext context_stat, context_fetch;
        std::ofstream outfile;
        std::string buffer;
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
	context_stat.set_deadline(deadline);
        context_fetch.set_deadline(deadline);
        fstatus = service_stub->GetStat(&context_stat,request,&reply);

        if (fstatus.ok())
        {
		if(reply.cksum()==client_crc)
		{
			return StatusCode::ALREADY_EXISTS;

		}

        }
        else
        {

                return fstatus.error_code();

        }

        std::unique_ptr<ClientReader<FileBytes> > reader(service_stub->Fetch(&context_fetch, request));
        outfile.open(path.c_str());
        while (reader->Read(&info))
        {

		
                outfile.write(info.fb().c_str(), info.len());

        }

        Status status = reader->Finish();
        outfile.close();

        if (status.ok())
        {

                return StatusCode::OK;
        }
        else
        {
                return status.error_code();

        }

}
    //
    // Request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // Also add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, it returns a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

        Fname request;
        request.set_filename(filename);

        param reply;

        ClientContext context;
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
        context.set_deadline(deadline);

        Status status = service_stub->Delete(&context,request,&reply);

        if (status.ok())
        {
                return StatusCode::OK;

        }
        else
        {
                return status.error_code();

        }

}

    //
    // Request to get list files here.
    //
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

        param request;
        ListInfo info;

        ClientContext context;

        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
        context.set_deadline(deadline);

        std::unique_ptr<ClientReader<ListInfo> > reader(service_stub->GetList(&context, request));
        while (reader->Read(&info))
        {


                file_map->insert({info.name(),info.modtime()});
        }

        Status status = reader->Finish();

        if (status.ok())
        {

                return StatusCode::OK;
        }
        else
        {

                return status.error_code();

        }

}
    //
    // Request to get the status of a file here.
    //
    // The StatusCode response:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

        Fname request;
        request.set_filename(filename);

        Fstat reply;

        ClientContext context;
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
        context.set_deadline(deadline);

        Status status = service_stub->GetStat(&context,request,&reply);

        if (status.ok())
        {
                return StatusCode::OK;
        }
        else
        {
                return status.error_code();

        }

}

    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {



    callback();

}

    //
    // This method handles the gRPC asynchronous callbacks from the server.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called.
    //
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    std::string path;
    std::uint32_t client_crc, cksum;
    long modt;
    struct stat stat_buf;
    StatusCode st;


    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {
                // based on the file listing returned from the server,
                // the client compares the mod time of its file with the
	        // same file in the server. If the client file is out of date
                // it fetches the updated file from server or it stores it's 
		// version if client file is most updated one
                //

                dfs_log(LL_DEBUG3) << "Handling async callback ";
		FileListResponseType rep = call_data->reply;
		for(int i=0; i<rep.list().size(); i++)
		{
			const Response &reply = rep.list(i);
			path = WrapPath(reply.name());
			cksum = reply.checksum();
			modt = reply.modtime();
			client_crc=dfs_file_checksum(path, &this->crc_table);

			if(cksum != client_crc)
			{
				int rc = stat(path.c_str(), &stat_buf);
				if(stat_buf.st_mtime < modt)
				{
					st = Fetch(reply.name());

				}
				else if(stat_buf.st_mtime > modt)
				{
					st = Store(reply.name());
				}
			}

		}

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}



