package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;

import java.util.*;
import surfstore.SurfStoreBasic.*;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.lang.Thread;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
    // connect with blockStore
    private ManagedChannel blockChannel;
    private BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    // connect with following metadataStore
    private ManagedChannel metadataChannel;
    private MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
        // this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
        //     .usePlaintext(true).build();
        // this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
	}

	private void start(int port, int numThreads) throws IOException {
        // check if this port is leader
        boolean isLeader = (config.getLeaderPort() == port);

        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(isLeader, config))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }


    // class Worker implements Runnable {

    // }
    //             public class Worker extends Thread {
    //                 MetadataStoreImpl meta = new MetadataStoreImpl(true, config);

    //                 @Override
    //                 public void run() {
    //                     try {
    //                         while (true) {
    //                             meta.sendHeartbeat();

    //                             Thread.sleep(500);
    //                         }

    //                     } catch(InterruptedException v) {
    //                         System.out.println(v);
    //                     }
    //                 }
    //             }
        

    /*static*/ class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        
        // use two maps to store name-version pair and name-hashlist pair
        protected Map<String, Integer> metaVersion;
        protected Map<String, List<String>> metaHash;

        boolean isLeader;
        boolean isCrashed;
        List<LogEntry> logs;
        int local_commit = 0;

        List<MetadataStoreGrpc.MetadataStoreBlockingStub> followers = new ArrayList<>();

        public MetadataStoreImpl(boolean isLeader, ConfigReader config) {
            super();
            this.isLeader = isLeader;
            metaVersion = new HashMap<>();
            metaHash = new HashMap<>();
            isCrashed = false;
            logs = new ArrayList<>();

            if (isLeader) {
                // leader should connect to block store
                blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
                blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

                // leader should connect to follower metadata
                List<Integer> followersId = config.getFollowerId();
                

                for (Integer id : followersId) {
                    metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(id))
                        .usePlaintext(true).build();
                    metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
                    followers.add(metadataStub);
                }

                Thread t1 = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            sendHeartbeat();
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException v) {
                                System.err.println(v);
                            }
                        }
                    }  
                });

                t1.start();

                // int follower1 = followers.get(0);
                // int follower2 = followers.get(1);

                // this.metadataChannel_1 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(follower1))
                // .usePlaintext(true).build();
                // this.metadataStub_1 = MetadataStoreGrpc.newBlockingStub(metadataChannel_1);

                // this.metadataChannel_2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(follower2))
                // .usePlaintext(true).build();
                // this.metadataStub_2 = MetadataStoreGrpc.newBlockingStub(metadataChannel_2);
            }
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void readFile(FileInfo req, final StreamObserver<FileInfo> responseObserver) {
            // logger.info("Client request read file " + req.getFilename());
            
            // if (isCrashed) {
            //     // only set filename and version
            //     FileInfo.Builder responseBuilder = FileInfo.newBuilder();
            //     String filename = req.getFilename();
            //     int version = metaVersion.get(filename);
            //     responseBuilder.setFilename(filename).setVersion(version);
            //     FileInfo response = responseBuilder.build();
            //     responseObserver.onNext(response);
            //     responseObserver.onCompleted();
            // }

            String filename = req.getFilename();
            int version = 0;

            // get version
            if (metaVersion.containsKey(filename)) {
                version = metaVersion.get(filename);
                if (version < 0) version = 0;
            }

            // generate response
            FileInfo.Builder responseBuilder = FileInfo.newBuilder();

            // if crashed, only set filename and version and return
            // if version is 0, then the server does not have such files
            if (!isCrashed && version != 0) {
                List<String> hashList = new ArrayList<>();
                // get hashlist
                if (metaHash.containsKey(filename)) {
                    hashList = metaHash.get(filename);
                }

                // change hashlist to array
                String[] values = new String[hashList.size()];
                values = hashList.toArray(values);
                
                // set blocklist
                for (String val : values) {
                    responseBuilder.addBlocklist(val);
                }
            }
            
            // set filename and version
            responseBuilder.setFilename(filename).setVersion(version);
            // ready to build response
            FileInfo response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void modifyFile(FileInfo req, final StreamObserver<WriteResult> responseObserver) {
            // new_version = cur_version + 1
            // get input info
            
            String filename = req.getFilename();
            int client_version = req.getVersion();
            List<String> client_blocklist = req.getBlocklistList();

            int server_version = metaVersion.containsKey(filename) ? metaVersion.get(filename) : 0;

            if (client_version < 0) client_version = 0;
            if (server_version < 0) server_version = 0;

            // store missing blocks
            List<String> missingBlocks = new ArrayList<>();

            // check missing blocks
            for (String blockHash : client_blocklist) {
                Block requestBlock = Block.newBuilder().setHash(blockHash).build();
                SimpleAnswer answer = blockStub.hasBlock(requestBlock);
                if (answer.getAnswer() == false) {
                    missingBlocks.add(blockHash);
                }
            }

            // build WriteResult
            WriteResult.Builder resultBuilder = WriteResult.newBuilder();
            
            // if not leader
            if (!isLeader) {
                resultBuilder.setResult(WriteResult.Result.NOT_LEADER);
            }

            // if version is old
            else if (server_version != client_version - 1) {
                resultBuilder.setCurrentVersion(server_version);
                resultBuilder.setResult(WriteResult.Result.OLD_VERSION);
            }

            // if has any missing block
            else if (missingBlocks.size() != 0) {
                // set missing blocks
                String[] values = new String[missingBlocks.size()];
                values = missingBlocks.toArray(values);
                for (String val : values) {
                    resultBuilder.addMissingBlocks(val);
                }
                // set version
                resultBuilder.setCurrentVersion(server_version);
                // set result
                resultBuilder.setResult(WriteResult.Result.MISSING_BLOCKS);


            } 
            
            // OK
            else {
                LogEntry log = LogEntry.newBuilder().setIndex(local_commit + 1).setFile(req).setCommand("modify").build();
                if (twoPhase(log)) {
                    resultBuilder.setCurrentVersion(client_version);
                    resultBuilder.setResult(WriteResult.Result.OK);
                    // update
                    metaVersion.put(filename, client_version);
                    metaHash.put(filename, client_blocklist);
                } else {
                    // not enough votes
                    System.err.println("Not enough votes.");
                }
                
            }

            WriteResult result = resultBuilder.build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();

        }

        @Override
        public void deleteFile(FileInfo req, final StreamObserver<WriteResult> responseObserver) {
            String filename = req.getFilename();
            int client_version = req.getVersion();
            int server_version = metaVersion.containsKey(filename) ? metaVersion.get(filename) : 0;

            if (client_version < 0) client_version = 0;
            if (server_version < 0) server_version = 0;

            WriteResult.Builder resultBuilder = WriteResult.newBuilder();

            // if not leader
            if (!isLeader) {
                resultBuilder.setResult(WriteResult.Result.NOT_LEADER);
            } 

            // // if file not found in server
            // else if (server_version == 0) {
            //     System.out.println("Not Found");
            // }

            // if version is old
            else if (server_version != client_version - 1) {
                resultBuilder.setCurrentVersion(server_version);
                resultBuilder.setResult(WriteResult.Result.OLD_VERSION);
            } 

            // OK
            else {
                LogEntry log = LogEntry.newBuilder().setIndex(local_commit + 1).setFile(req).setCommand("delete").build();
                
                if (twoPhase(log)) {
                    resultBuilder.setCurrentVersion(client_version);
                    resultBuilder.setResult(WriteResult.Result.OK);
                    // update
                    metaVersion.put(filename, client_version);
                    List<String> temp = new ArrayList<>();
                    temp.add("0");
                    metaHash.put(filename, temp);
                } 

                else {
                    System.err.println("Not enough votes.");
                }
            }

            WriteResult result = resultBuilder.build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }


        @Override
        public void isLeader(Empty req, final StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isLeader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }



        public boolean twoPhase(LogEntry log) {
            // first append to its own log
            logs.add(log);
            // vote
            int count = 0;
            for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                SimpleAnswer voteResult = follow.vote(log);
                if (voteResult.getAnswer()) {
                    count++;
                }
            }

            if (count >= followers.size() / 2) {
                // commit
                local_commit = log.getIndex(); // record index in leader

                for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                    follow.commit(log); // follower commit
                }
                return true;
            } else {
                // did not get enough votes
                logs.remove(logs.size() - 1);
                for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                    follow.abort(log);
                }
                return false;
            }
        }

        @Override
        public void vote(LogEntry log, final StreamObserver<SimpleAnswer> responseObserver) {

            SimpleAnswer.Builder answerBuilder = SimpleAnswer.newBuilder();

            // if crashed
            if (isCrashed) {
                answerBuilder.setAnswer(false);
            }
            else if (local_commit + 1 == log.getIndex()) {
                answerBuilder.setAnswer(true);
                logs.add(log);
            } else {
                // gap between commit index
                answerBuilder.setAnswer(false);
            }
            SimpleAnswer answer = answerBuilder.build();
            responseObserver.onNext(answer);
            responseObserver.onCompleted();
        }

        @Override
        public void commit(LogEntry log, final StreamObserver<Empty> responseObserver) {
            
            if (!isCrashed && local_commit + 1 == log.getIndex()) {
                // update local commit index
                local_commit += 1;
                // update local fileinfo
                FileInfo f = log.getFile();
                metaVersion.put(f.getFilename(), f.getVersion());
                metaHash.put(f.getFilename(), f.getBlocklistList());
            }
            
            // response to leader
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void abort(LogEntry log, final StreamObserver<Empty> responseObserver) {

            if (logs.get(logs.size() - 1).getIndex() == log.getIndex() && !isCrashed) {
                logs.remove(logs.size() - 1);
            }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void crash(Empty empty, final StreamObserver<Empty> responseObserver) {
            isCrashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }        

        @Override
        public void restore(Empty empty, final StreamObserver<Empty> responseObserver) {
            isCrashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } 

        @Override
        public void isCrashed(Empty empty, final StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isCrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } 

        @Override
        public void getVersion(FileInfo req, final StreamObserver<FileInfo> responseObserver) {
            
            String filename = req.getFilename();

            FileInfo.Builder fileBuilder = FileInfo.newBuilder();

            // if (isLeader) {
            //     int leader_version = metaVersion.containsKey(filename) ? metaVersion.get(filename) : 0;
            //     List<Integer> versions = new ArrayList<>();
            //     versions.add(leader_version);
            //     // get follower versions
            //     if (followers.size() > 0) {
            //         for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
            //             FileInfo f = follow.getVersion(req);
            //             versions.add(f.getVersion());
            //         }
            //     }
            //     // set allversions
            //     Integer[] values = new Integer[versions.size()];
            //     values = versions.toArray(values);
            //     for (Integer val : values) {
            //         fileBuilder.addAllVersions(val);
            //     }

            //     fileBuilder.setFilename(filename).setVersion(leader_version);
            // } 

            // follower
      //      else {
                int version = metaVersion.containsKey(filename) ? metaVersion.get(filename) : 0;
                fileBuilder.setFilename(filename).setVersion(version);
      //      }

            //response
            FileInfo response = fileBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } 

        @Override
        public void heartbeat(HeartbeatRequest req, final StreamObserver<HeartbeatResponse> responseObserver) {
            HeartbeatResponse.Builder responseBuilder = HeartbeatResponse.newBuilder();

            int leader_commit = req.getCommitIndex();

            // Leave me alone. I am crashed.
            if (isCrashed) {
                responseBuilder.setNeedUpdate(false);
            }

            else if (local_commit < leader_commit) {
                // need update
                responseBuilder.setCurIndex(local_commit);
                responseBuilder.setNeedUpdate(true);

            } 

            else {
                // already up-to-date
                responseBuilder.setNeedUpdate(false);
            }     

            HeartbeatResponse response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();     
        }   


        @Override
        public void bringBack(NeedLogs req, final StreamObserver<Empty> responseObserver) {
            List<LogEntry> missingLogs = req.getMissingLogsList();
            for (LogEntry log : missingLogs) {
                FileInfo f = log.getFile();
                metaVersion.put(f.getFilename(), f.getVersion());
                metaHash.put(f.getFilename(), f.getBlocklistList());
                logs.add(log);
                local_commit = logs.size();
            }
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        public void sendHeartbeat() {
            HeartbeatRequest request = HeartbeatRequest.newBuilder().setCommitIndex(local_commit).build();
            // send current commit index to all followers
            for (MetadataStoreGrpc.MetadataStoreBlockingStub follow : followers) {
                HeartbeatResponse response = follow.heartbeat(request);

                if (response.getNeedUpdate()) {
                    int cur_index = response.getCurIndex();

                    // prepare missing logs
                    List<LogEntry> missingLogs = new ArrayList<>();
                    for (int i = cur_index; i < logs.size(); i++) {
                        LogEntry temp = logs.get(i);
                        missingLogs.add(temp);
                    }

                    // bring back
                    NeedLogs.Builder neededLogsBuilder = NeedLogs.newBuilder();
                    // set all needed logs
                    LogEntry[] values = new LogEntry[missingLogs.size()];
                    values = missingLogs.toArray(values);
                    for (LogEntry val : values) {
                        neededLogsBuilder.addMissingLogs(val);
                    }
                    NeedLogs neededLogs = neededLogsBuilder.build();
                    // update
                    follow.bringBack(neededLogs);
                }
            }
        }      
    }
}