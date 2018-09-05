package surfstore;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;

import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import surfstore.SurfStoreBasic.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.file.FileSystems;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    // block size: 4k
    private static final int BLOCK_SIZE = 4096;

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;


    // // connect to follower
    // private final ManagedChannel metadataFollower;
    // private final MetadataStoreGrpc.MetadataStoreBlockingStub followerStub;


    // clients hold a local version map and a local blocklist map
    //protected HashMap<String, Integer> local_version;
    protected HashMap<String, byte[]> local_block;
    protected Set<String> allFileName;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;

        // // connect to follower
        // this.metadataFollower = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
        //         .usePlaintext(true).build();
        // this.followerStub = MetadataStoreGrpc.newBlockingStub(metadataFollower);


        //local_version = new HashMap<>();
        local_block = new HashMap<>();
        allFileName = new HashSet<>();

        addLocal(Paths.get(".").toAbsolutePath().normalize().toString());
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }


    private void ensure(boolean b) {
        if (b == false) {
            throw new RuntimeException("Assertion failed!");
        } else if (b == true) {
            System.out.println("Success!");
        }
    }

    
	private void go(String command, String filepath) {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");
        
        // TODO: Implement your client here

        String absolutePath = FileSystems.getDefault().getPath(filepath).normalize().toAbsolutePath().toString();

        if (command.equals("upload")) {
            upload(absolutePath);
        }

        else if (command.equals("download")) {
            download(absolutePath);
        }

        else if (command.equals("delete")) {
            delete(absolutePath);
        }

        else if (command.equals("getversion")) {
            getVersion(absolutePath);
        }

        
        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // delete("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // download("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // upload("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // download("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // upload("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // delete("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // download("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // followerStub.crash(Empty.newBuilder().build());
        // System.out.println("Crashed follower 3");

        // upload("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // download("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // followerStub.restore(Empty.newBuilder().build());
        // System.out.println("Restored follower 3");

        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");
        
        // try        
        //     {
        //         TimeUnit.SECONDS.sleep(3);
        //     } 
        //     catch(InterruptedException ex) 
        //     {
        //         System.out.println("ex");
        //     }


        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

       
        // upload("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        // getVersion("/Users/qliu/Downloads/291/p2-qliu67/java/test1.txt");

        
	}

	/*
	 * TODO: Add command line handling here
	 */



    public void upload(String filename) {
        if (!allFileName.contains(filename)) {
            System.out.println("Not Found");
            return;
        }

        FileInfo.Builder requestBuilder = FileInfo.newBuilder();
        // set filename
        requestBuilder.setFilename(filename);
        // set version
        int version = 0;
        FileInfo f = metadataStub.readFile(FileInfo.newBuilder().setFilename(filename).build());

        version = f.getVersion();

        if (version < 0) version = 0;

        requestBuilder.setVersion(version + 1);
        
        // set blocklist
        List<String> blocklist = new ArrayList<>();
        List<Block> localBlocks= nameToBlock(filename);

        // add to blocklist and store blocks to local map
        for (Block b : localBlocks) {
            blocklist.add(b.getHash());
            // local_block.put(b.getHash(), b.getData().toByteArray());
        }
        // set repeated string: blocklist
        String[] values = new String[blocklist.size()];
        values = blocklist.toArray(values);
        for (String val : values) {
            requestBuilder.addBlocklist(val);
        }
        //build
        FileInfo request = requestBuilder.build();

        //System.out.println(request.getVersion());

        // upload to metadata

        for (int i = 0; i < 2; i++) {
            WriteResult response = metadataStub.modifyFile(request);

            if (response.getResult() == WriteResult.Result.MISSING_BLOCKS) {
                List<String> missedBlocks = response.getMissingBlocksList();
                for (String s : missedBlocks) {

                    // System.out.println(s);
                    // System.out.println(local_block.containsKey(s));

                    Block blk = Block.newBuilder().setHash(s).setData(ByteString.copyFrom(local_block.get(s))).build();
                    blockStub.storeBlock(blk);
                }
            } else if (response.getResult() == WriteResult.Result.OK) {
                System.out.println("OK");
                return;
            } else if (response.getResult() == WriteResult.Result.OLD_VERSION) {
                logger.info("You need to download the latest version.");
                return;
            } else if (response.getResult() == WriteResult.Result.NOT_LEADER) {
                logger.info("This metadata server is not a leader.");
                return;
            }
        }
    }


    public void download(String filename) {
        
        // readfile from metadata server
        FileInfo requestToMeta = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo metaResponse = metadataStub.readFile(requestToMeta);
        
        // set version in local client
        int version = metaResponse.getVersion();

        if (version <= 0) {
            System.out.println("Not Found");
            return;
        }

        //local_version.put(filename, version);

        // findout missing blocks in the client
        List<String> blocklist = metaResponse.getBlocklistList();

        // System.out.println(blocklist.size());
        // System.out.println(blocklist.get(0));

        // if have been deleted
        if (blocklist.size() == 1 && blocklist.get(0).equals("0")) {
            System.out.println("Not Found");
            return;
        }

        List<String> needBlock = new ArrayList<>();
        for (String s : blocklist) {
            if (!local_block.containsKey(s)) {
                needBlock.add(s);
            }
        }

        // download needed blocks from blockstore
        for (String s : needBlock) {
            Block blk = Block.newBuilder().setHash(s).build();
            Block b = blockStub.getBlock(blk);
            local_block.put(b.getHash(), b.getData().toByteArray());
        }

        System.out.println("OK");

        // merge blocks

    }


    public void delete(String filename) {
        FileInfo.Builder requestBuilder = FileInfo.newBuilder();
        // set filename
        requestBuilder.setFilename(filename);
        // set version
        int version = 0;
        FileInfo f = metadataStub.readFile(FileInfo.newBuilder().setFilename(filename).build());
        version = f.getVersion();

        // System.out.println(version);

        if (version <= 0) {
            System.out.println("Not Found");
            return;
        }

        requestBuilder.setVersion(version + 1);
        
        //build
        FileInfo request = requestBuilder.build();

        WriteResult response = metadataStub.deleteFile(request);

        if (response.getResult() == WriteResult.Result.OK) {
            System.out.println("OK");
            return;
        } 

        else if (response.getResult() == WriteResult.Result.OLD_VERSION) {
            System.out.println("Not Found");
            return;
        }

        else if (response.getResult() == WriteResult.Result.NOT_LEADER) {
            System.out.println("Not Found");
            return;
        }

    }


    public void getVersion(String filename) {
        FileInfo request = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo response = metadataStub.getVersion(request);
        // if (response.getAllVersionsList().size() > 1) {
        //     // distributed metadata
        //     List<Integer> versions = response.getAllVersionsList();
        //     StringBuilder sb = new StringBuilder();
        //     String space = "";
        //     for (Integer i : versions) {
        //         sb.append(space).append(i);
        //         space = " ";
        //     }
        //     String res = sb.toString();
        //     // System.out.println(res);
        //     System.out.println(response.getVersion());
        //     return;
        // }

       //  else {
            // centralized metadata
            int res = response.getVersion();
            System.out.println(res);
            return;
     //   }
    }


        // given a file name, output blocks
    public List<Block> nameToBlock(String s) {

        Path path = Paths.get(s);
        byte[] data = null;

        try {
            data = Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println("Can not read files");
            System.exit(1);
        }

        int len = data.length;
        List<Block> res = new ArrayList<>();

        for (int i = 0; i < len; i += BLOCK_SIZE) {
            if (i + BLOCK_SIZE < len) {
                res.add(createBlock(Arrays.copyOfRange(data, i, i + BLOCK_SIZE)));
            } else {
                res.add(createBlock(Arrays.copyOfRange(data, i, len)));
            }
        }

        return res;
    }


    // calculate hash and store it into block
    public Block createBlock(byte[] data) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            System.err.println("Can not read files");
            System.exit(2);
        }
        byte[] hash = digest.digest(data);
        String encoded = Base64.getEncoder().encodeToString(hash);

        return Block.newBuilder().setHash(encoded).setData(ByteString.copyFrom(data)).build();
    }


    // get all files in the same direction
    public void getAllFiles(String dir) {
        File folder = new File(dir);
        for (File f : folder.listFiles()) {
            // if (f.isDirectory()) {
            //     getAllFiles(dir + "/" + f.getName());
            // } else if (f.isFile()) {
            //     allFileName.add(dir + "/" + f.getName());
            // }
            if (f.isFile()) {
                allFileName.add(dir + "/" + f.getName());
            }
        }
    }


    // add all local blocks and version
    public void addLocal(String dir) {
        getAllFiles(dir);
        for (String s : allFileName) {
            //local_version.put(s, 0);
            List<Block> blocks = nameToBlock(s);
            for (Block b : blocks) {
                local_block.put(b.getHash(), b.getData().toByteArray());
            }
        }
    }





    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("command").type(String.class)
                .help("The operations for clients");
        parser.addArgument("filepath").type(String.class)
                .help("Path to file");
        // parser.addArgument("destination").type(String.class).setDefault(".")
        //         .help("Path to store the download file");
        
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

        Client client = new Client(config);

        String command = c_args.getString("command");
        String filepath = c_args.getString("filepath");
        // String destination = c_args.getString("destination");

        
        try {
        	client.go(command, filepath);
        } finally {
            client.shutdown();
        }
    }

}
