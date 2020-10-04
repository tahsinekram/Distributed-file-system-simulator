# Distributed-file-system-simulator

This is a distirbuted file system implemented with a weakly consistent cache strategy and based on the Andrew File system. A weak consistency model consist of read and write operations on an open file are directed only to the locally cached copy. When a modified file is closed, the changed portions are copied back to the file server. Cache consistency is maintained by callback mechanism. When a file is cached, the server makes a note of this and promises to inform the client if the file is updated by someone else. Callbacks are discarded and must be re-established after any client, server, or network failure, including a timeout. Re-establishing a callback involves a status check and does not require re-reading the file itself. The following breaks down the flow of the client, server and asynchronous operations.

The client stub functions have the following flow:
1. Initialize request and reply messages with appropriate parameters
2. Set the connectin deadline timeout.
3. Create a stub variable linking to the appropriate service call
4. Begin the call based on what kind of function the client serves which could be reading streams of bytes or sending request all at once.
5. For reading streams the client uses a while loop to read until as long as the service is sending back the data if not it discontinues the while loop and obtains the status
6. For writing streams the client also uses a while loop and verifies inside if the write has been correctly done and if not it breaks and obtains the status
7. The client returns the status code ok if the status has the ok variable as true or else it returns the corresponding error code.

The server services have the folowing flow:
1. Parses information nested in the request message in to local variables for later use
2. Before it proceeds with further processing request and sending information back to client it makes a quick check for to see if the client context is cancelled due to any abrupt connection issues on the client side
3. next it performs the corresponding actions based on the request and fills in the parameters embedded in the reply messages.
4. Next it returns the corresponding status embedded with a message for debugging purposes

Async Call flow
1. Server dequeues a request and starts populating a response type of repeated message in protobuf. The repeated message acts as a dynamically sized array or like a map where it adds the filename, checksum and modtimes that is to be sent to the client.
2. Upon receiving the request client parses the repeated message and fields to compare modtimes in the even the file checksums dont match.
3. It fetches if motime of client file is less that server file and store if vice versa

The key design aspect was to control the access to the file map using mutexes. As it is the sole informatin keeping of what client has what file name serializing acces to it using a global mutext allowed proper synchronization.

The store operation by the client has the following flow:
1. Upon store request the client makes a call to the RequestAcessWrite stub service
2. Inside the request write lock it obtains a mutex lock on a shared map which consists of filename as key and client id as value.
3. Check to see if the requested filename already exists in the map. If so the check the associated client. Return ok if the client id is same the requested client id if not return RESOURCE_EXHAUSTED. Unlock the shared mutex
4. Inside the while loop responsible for sending file byes from the client send file checksum to the service and the service compares them and responses with already exists or not.
5. Complete the store request.

Server design considerations:
The fetch and store services had the challenge of hadnling the type that protobuf for c++ uses to interpret bytes. Since protobuf uses strings to handle bytes both fetch and store used reinterpret cast to cast the contents of file inside a char buffer and produce a null terminated string based on the length provided.

The testing for each service was done by using seperate folders as mounts for both client and server and performing all the operations. After each operations the files md5 checksum was compared to verify complete transfer.
