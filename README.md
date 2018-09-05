# Distributed-file-system
A could-based distributed-file-system

# Overview
SurfStore is a networked file storage application that supports four basic commands:

• Create a file

• Read the contents of a file

• Change the contents of a file

• Delete a file

Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. Clients accessing SurfStore “see” a consistent set of updates to files, but SurfStore does not offer any guarantees about operations across files, meaning that it does not support multi-file transactions (such as atomic move).

The SurfStore service is composed of the following two sub-services:

• BlockStore: The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. The BlockStore service stores these blocks, and when given an identifier, retrieves and returns the appropriate block.

• MetadataStore: The MetadataStore service holds the mapping of filenames/paths to blocks.

# Details
Used a replicated log plus 2-phase commit protocol ensuring that the service can survive, and continue operating, even if one of its processes fails,  the back-up servers are still able to rejoin the distributed systems and get up-to-date.

Supported concurrently connection to the SurfStore service for clients to access a common, shared set of files.

# Implementation details
configCentralized.txt

    M: 1
    L: 1
    metadata1: <port>
    block: <port>
    
configDistributed.txt

    M: 3
    L: 2
    metadata1: <port>
    metadata2: <port>
    metadata3: <port>
    block: <port>
The initial line M defines the number of Metadata servers.

The second line L denotes which of the Metadata servers is the leader. In the centralized example, metadata1 is the leader. In the distributed example, metadata2 is the leader.

The ‘metadata1’ line specifies the port number of your metadata server. Note the ‘1’, ‘2’, etc after the word metadata to indicate the ports for the different instances of the service.

‘block’ denotes the port number of your BlockStore.

This config file will be available to the client and servers when they are started. This configuration file helps the server or client know the cluster information and also how many metadata servers are present in the service. Note that because you’re going to run the client, the BlockStore, and the Metadata server all on the same machine, you will need to use unique ports. The configuration file we provide will always be valid and will not contain any errors or problems.

## Client

The client will perform one or more operations before exiting.

Note that before downloading any blocks from the BlockStore, the client should check whether any of those blocks exist locally first. It only needs to check whether that block already is present in the destination directory. For example, if the client is downloading a file to the /home/aturing/downloads directory, then the block protocol only needs to look for local blocks in /home/aturing/downloads, not everywhere else (and not in any subdirectories either, just that one directory).

## Client output

You can print out whatever messages you want to stderr. To stdout, please either print:

OK: if the operation succeeded

Not Found: if the client tried to download a file that was not found on the server, or tried to upload a file which does not exist on the local machine. Also return this if GetVersion is called on a file which does not exist.
