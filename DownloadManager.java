import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This class operates the download manager. It creates the relevant threads
 * (a few RangeGetters and one Write thread) in order to download the file.
 */
public class DownloadManager {
    private final static double CHUNK_SIZE  = 4096.0; // Size of chunk to download
    private final static Path currentRelativePath = Paths.get("");
    private final static int TIME_TO_WAIT = 2000; // Time to wait while opening connections
    private final static int MINIMAL_FILESIZE = 256 * (int) CHUNK_SIZE; // The minimal file size is 1MB
                                                                        // less then that downloads with one connection
    public static void main(String[] args) {
        int numOfConnections = 1;

        BlockingDeque<Chunk> queue = new LinkedBlockingDeque<Chunk>();

        // Checks number of arguments
        if(args.length < 1 || args.length > 2){
            System.err.println("usage:\n\tjava DownloadManager URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]");
            System.exit(1);
        }

        ArrayList<URL> URLs = new ArrayList<URL>();
        Path mirrorsListPath = null;
        File mirrorsListFile = null;

        // Checks if args[0] is a URL or file
		// If true, then there exists a file containing a list of servers
        if (!args[0].contains("http")){
            try {
				mirrorsListPath = currentRelativePath.resolve(args[0].trim());
				mirrorsListFile = new File(mirrorsListPath.toString());

                BufferedReader bufferedReader = new BufferedReader(new FileReader(mirrorsListFile));
                String line;

                while ((line = bufferedReader.readLine()) != null){
                    URLs.add(new URL(line));
                }

            } catch (IOException e){
                System.err.println("Failed reading given server list file");
                System.err.println("Download failed");
                System.exit(1);
            }
        } else {
            // If reached here, the given argument isn't a file
            try {
                URLs.add(new URL(args[0]));
            } catch (MalformedURLException e) {
                // If this exception occurs - the argument is not a valid URL
                System.err.println("The given argument is not a valid URL");
                System.err.println("Download failed");
                System.exit(1);
            }
        }

        // If the user asks for a maximal connection number
        if (args.length == 2){
            int requestedConnectionsNum = Integer.parseInt(args[1]);
            numOfConnections = requestedConnectionsNum;
        }

        String fileNameToDownload = URLs.get(0).toString().substring(URLs.get(0).toString().lastIndexOf('/') + 1);
        run(URLs, fileNameToDownload, numOfConnections, queue);
    }


    private static void run(ArrayList<URL> URLs, String fileNameToDownload, int numOfConnections, BlockingDeque<Chunk> queue) {
        int fileSize = 0;
        long numOfChunksInRange;
        URL urlToDownload;

        Path metadataPath = currentRelativePath.resolve(fileNameToDownload + ".tmp");
        File metadataFile = new File(metadataPath.toString());

        boolean isOnResume = metadataFile.exists();

        try {
            urlToDownload = URLs.get(0);
            HttpURLConnection connection = (HttpURLConnection) urlToDownload.openConnection();

            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(TIME_TO_WAIT); // Sets a 2 sec timeout when the connection is established

            fileSize = connection.getContentLength();
            if (fileSize < 0) {
                System.err.println("There was an error while sending a HTTP HEAD request to the server");
                System.err.println("Download failed");
                System.exit(1);
            }
            if (fileSize < MINIMAL_FILESIZE){
                numOfConnections = 1;
            }

            connection.disconnect();

        } catch (IOException e) {
            System.err.println("There was an error while sending a HTTP HEAD request to the server");
            System.err.println("Download failed");
            System.exit(1);
        }

        // Metadata size is the number of chunks we need for the file
        int metaDataSize = (int) Math.ceil(fileSize / CHUNK_SIZE);
        Metadata metadata = new Metadata(metaDataSize);

        if(isOnResume && metadataFile.length() > 0){
            metadata = getMetadataObject(fileNameToDownload + ".tmp");
        } else if (isOnResume && (metadataFile.length() == 0)){
            // If the program stops before writing to disk but metadata file was created
            if(!metadataFile.delete()){
                System.err.println("Unable to establish HTTP HEAD request connection");
                System.err.println("Download failed");
                System.exit(1);
            }
        }

        numOfChunksInRange = metaDataSize / numOfConnections;
        
        // Initialize writer thread
        Thread writer = new Thread(new Writer(queue, fileNameToDownload, fileSize, metaDataSize));

        // Initialize rangeGetter threads
        Thread[] threadsPool = new Thread[numOfConnections];
        Random r = new Random();

        for (int i = 0; i < numOfConnections; i++){
            long start = i * numOfChunksInRange * (long)CHUNK_SIZE;
            long end = (i + 1) * numOfChunksInRange * (long) CHUNK_SIZE - 1;

            // If fileSize/connectionsNum  is not an integer, one thread should read more bytes
            if (i == numOfConnections - 1) {
                end = fileSize - 1;
            }

            int index = r.nextInt(URLs.size());
            urlToDownload = URLs.get(index);
            Thread getter = new Thread(new RangeGetter(start, end, queue, urlToDownload, metadata));
            threadsPool[i] = getter;
            threadsPool[i].start();
        }

        writer.start();

        try {
            writer.join();
            for (Thread thread : threadsPool){
                thread.join();
            }
        } catch (InterruptedException e){
            System.err.println("Failed to join threads");
            System.err.println("Download failed");
            System.exit(1);
        }
    }

    /**
     * This method opens the metadata file and gets the object in the file
     * using deserialization.
     * @return the object in the file
     */
    private static Metadata getMetadataObject(String metadataFilename){
        Metadata metadataObject = null;
        try {
            FileInputStream fileIn = new FileInputStream(metadataFilename);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            metadataObject = (Metadata) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException | ClassNotFoundException e){
            System.err.println("Failed while reading the object from the metadata file");
            System.err.println("Download failed");
            System.exit(1);
        }
        return metadataObject;
    }
}
