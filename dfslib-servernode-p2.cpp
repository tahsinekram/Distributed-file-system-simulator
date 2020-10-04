#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;
using dfs_service::Fname;
using dfs_service::Fstat;
using dfs_service::ListInfo;
using dfs_service::param;
using dfs_service::FileBytes;
using dfs_service::StoreBytes;
using dfs_service::Response;


// "using" aliases to the specific
// message types in the `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = dfs_service::FileRequest;
using FileListResponseType = dfs_service::FileList;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:

    //hash table to store file name and value to indicate if it is locked or not by a client
    std::map<std::string,std::string> write_lock_map;
    std::mutex map_lock;

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    Status RequestWriteAccess(ServerContext *context, const Fname* request,
                   param* reply) override {

            std::string curr_id;

            if (context->IsCancelled()) {
                    return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
            }

	    std::cout<<"LOCKING MAP MUTEX "<<request->cid()<<std::endl;
	    //obtain a mutex lock before accessing the hashtable with file lock information since it is a shared
	    //resource among multiple clients
	    this->map_lock.lock();
	    std::cout<<"THE COUNT IS "<<this->write_lock_map.count(request->filename())<<std::endl;
	    //if the associated file name has a count greater than zero, it means a client has it locked
	    //if locked, check if the client requesting the lock already has the lock. if true return status ok
	    //if false, another client is currently editing the file so the requesting client is denied lock
                    if((this->write_lock_map.count(request->filename()))>0)
                    {
		    
                            curr_id = this->write_lock_map.operator[](request->filename());
		
			    std::cout<<"CLIENT HAS LOCK "<<this->write_lock_map.operator[](request->filename())<<std::endl;
				    
			    std::cout<<"REQUESTING CLIENT "<<request->cid()<<std::endl;
                            if(strcmp(curr_id.c_str(), request->cid().c_str())==0)
                            {

			    
				    std::cout<<"CLIENT HAS LOCK "<<this->write_lock_map.operator[](request->filename())<<std::endl;
			    
				    std::cout<<"REQUESTING CLIENT "<<request->cid()<<std::endl;
          
				    this->map_lock.unlock();
				    std::cout<<"UNLOCKING MAP MUTEX "<<request->cid()<<std::endl;
                                    return Status::OK;
                            }
                            else
                            {
		    
				    std::cout<<"REJECTED LOCK "<<std::endl;
				    this->map_lock.unlock();
				    std::cout<<"UNLOCKING MAP MUTEX "<<request->cid()<<std::endl;
                                    return Status(StatusCode::RESOURCE_EXHAUSTED, "Resource exhausted");
                            }
                    }
		    //if the file doesn't exist in the hash table then the file doesn't have any client locking it and using it
		    //thus the requesting client is given the lock by adding the requested file to hash table
                    else
                    {
			    std::cout<<"ADDING "<<request->filename()<<" FOR "<<request->cid()<<std::endl;
                            this->write_lock_map.insert({request->filename(),request->cid()});
			    this->map_lock.unlock();
			    std::cout<<"UNLOCKING MAP MUTEX "<<request->cid()<<std::endl;
                            return Status::OK;
                    }

    }


    Status Store(ServerContext *context, ServerReader<StoreBytes>* reader,
                 param* code) override {

            if (context->IsCancelled()) {
                    return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            std::string path;
            std::ofstream outfile;
	    //grpc struct to hold the file contents and meta-data sent by the client to the server during store
	    //operation
            StoreBytes sinfo;
	    std::uint32_t server_crc;
            int flag = 1;
            while(reader->Read(&sinfo))
            {
		    //check if the file being stored is already updated and there in the server. If it already
		    //exists in the server return. Or else open the file once for writing
                    if (flag==1)
                    {
			    path = WrapPath(sinfo.file());
                            server_crc = dfs_file_checksum(path, &this->crc_table);
                            if(server_crc == sinfo.checksum())
                            {

				    this->map_lock.lock();
                                    this->write_lock_map.erase(sinfo.file());
                                    this->map_lock.unlock();
                                    return Status(StatusCode::ALREADY_EXISTS, "Already exists");
                            }
                            outfile.open(path.c_str());

                            flag = 0;
                    }

                    outfile.write(sinfo.sb().c_str(), sinfo.flen());
            }
            outfile.close();
	    //once store operation is done update the hash table that keep tracks of which files are locked to remove the filename
	    //since the client that is storing the file is done using it.
	    this->map_lock.lock();
	    this->write_lock_map.erase(sinfo.file());
	    this->map_lock.unlock();

            return Status::OK;
    }

    Status Fetch(ServerContext *context, const Fname* request,
                 ServerWriter<FileBytes>* writer) override {

            if (context->IsCancelled()) {
                    return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            const std::string path = WrapPath(request->filename());
	    //grpc struct to hold the file contents and meta-data sent to the client by the server
            FileBytes finfo;
            size_t count;
            size_t buffer_size = 10*1024;
            char * buffer = new char [buffer_size];

            std::ifstream inFile;
            inFile.open(path.c_str());
	    //open file and transfer its contents until end of file is reached
            if(inFile.fail())
            {
                    std::string msg("File not found");
                    return Status(StatusCode::NOT_FOUND, msg);
            }

            while(!inFile.eof())
            {
                    inFile.read(buffer, buffer_size);
                    count = inFile.gcount();
                    std::string sent(reinterpret_cast< char const *> (buffer), count);
                    finfo.set_fb(sent);
                    finfo.set_len(count);

                    writer->Write(finfo);


            }
            inFile.close();
            delete [] buffer;
            return Status::OK;
    }
    //Function to return a list of all the files in the server
    Status GetList(ServerContext *context, const param* request,
                   ServerWriter<ListInfo>* writer) override {

            DIR *pdir = NULL;
            struct dirent *pent = NULL;
            struct stat stat_buf;
            if (context->IsCancelled()) {
                    return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            std::string path = mount_path;
            ListInfo info;
            pdir = opendir(mount_path.c_str());
            if (pdir == NULL)
            {
                    std::string msg("Directory not found");
                    return Status(StatusCode::NOT_FOUND, msg);
            }
            while ((pent = readdir(pdir))!=NULL)
            {

                    std::string wp = WrapPath(pent->d_name);
                    int rc = stat(wp.c_str(), &stat_buf);
                    info.set_name(pent->d_name);
                    info.set_modtime(stat_buf.st_mtime);
                    writer->Write(info);
            }
            closedir(pdir);
            return Status::OK;
    }
    //obtain the stats of a particular file within the server
    Status GetStat(ServerContext *context, const Fname* request,
                   Fstat* reply) override {

            if (context->IsCancelled()) {
                    return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            const std::string path = WrapPath(request->filename());
	    std::uint32_t server_crc = dfs_file_checksum(path, &this->crc_table);
            struct stat stat_buf;
            int rc = stat(path.c_str(), &stat_buf);
            if (rc == -1)
            {
                    std::string msg("File not found");
                    return Status(StatusCode::NOT_FOUND, msg);
            }

            reply->set_fsize(stat_buf.st_size);
            reply->set_mdtime(stat_buf.st_mtime);
            reply->set_crtime(stat_buf.st_ctime);
	    reply->set_cksum(server_crc);
            return Status::OK;
    }

    Status Delete(ServerContext *context, const Fname* request,
                   param* reply) override {

            if (context->IsCancelled()) {
                    return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            const std::string path = WrapPath(request->filename());

            if((std::remove(path.c_str()))!=0)
            {
                    std::string msg("File not found");
                    return Status(StatusCode::NOT_FOUND, msg);
            }
            return Status::OK;
    }


    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // Code to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //

	    DIR *pdir = NULL;
            struct dirent *pent = NULL;
	    std::uint32_t server_crc;
            struct stat stat_buf;
            pdir = opendir(mount_path.c_str());
            while ((pent = readdir(pdir))!=NULL)
            {
                    std::string wp = WrapPath(pent->d_name);
                    server_crc=dfs_file_checksum(wp, &this->crc_table); 
		    int rc = stat(wp.c_str(), &stat_buf);
		    Response* resp = response->add_list();
		    resp->set_modtime(stat_buf.st_mtime);
		    resp->set_checksum(server_crc);
		    resp->set_name(pent->d_name);

            }
            closedir(pdir);
	
	

    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

};

/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

