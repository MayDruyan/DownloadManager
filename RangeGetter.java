import java.io.*;
import java.net.*;
import javax.net.ssl.SSLException;
import java.util.concurrent.BlockingDeque;

/**
 * This class represents a RangeGetter worker. Each opens a connection with the
 * url and gets the required range we define ahead. It reads the range,
 * divide it to chunks and pushes those chunks to the queue.
 */
public class RangeGetter implements Runnable {

    private static final int CHUNK_SIZE  = 4096; // Size of chunk to download
    private final static int TIME_TO_WAIT = 10000; // Time to wait while opening connections
    private long startByte;
    private long endByte;
    private long mDownloaded = 0;
    private long bytesToDownload;
    private URL mURL;
    private Metadata metaData;
    private BlockingDeque<Chunk> queue;


    public RangeGetter(long startByte, long endByte, BlockingDeque<Chunk> queue,
                       URL url, Metadata metaData){
        this.startByte = startByte;
        this.endByte = endByte;
        this.queue = queue;
        this.mURL = url;
        this.metaData = metaData;
    }

    /**
     * This method goes over the chunks of this RangeGetter in the metadata
     * array and returns the index of the first chunk that hasn't been
     * downloaded yet.
     * @param metadata the metadata object of this download
     * @return the index of the first chunk in the metadata of this range
     * that has not been downloaded yet
     */
    private int startIndexOnResume(Metadata metadata){
        for (int i = (int)this.startByte; i < this.endByte + 1; i += CHUNK_SIZE){
            int index = i/CHUNK_SIZE;
            if (!metadata.get(index)) return index;
        }
        // If we reached here, then all of this range has been downloaded,
        // therefore return -1 to indicate this
        return -1;
    }

    private Chunk readChunkAndPutInQueue(byte[] chunkData, int bytesToRead,
                                     int offset, InputStream inputStream,
                                        BlockingDeque<Chunk> queue) throws IOException, InterruptedException {
        chunkData = new byte[bytesToRead];
        inputStream.read(chunkData, 0, bytesToRead);
        Chunk chunk = new Chunk(chunkData, offset);
        queue.put(chunk);
        return chunk;

    }

    /**
     * RangeGetter thread - first checks if we are on resume or if this is a
     * regular download. Operates the download method according to the mode
     */
    @Override
    public void run() {
        Thread thread = Thread.currentThread();

        // Checks if the program is on resume mode, or if this is a new download
        if (this.metaData == null){
            System.out.println("[" + thread.getId() + "] Start downloading range (" +
                    this.startByte + " - " + this.endByte + ") from: " + this.mURL.toString());
            download();
        } else {
            int startIndexOnResume = startIndexOnResume(this.metaData);
            if (startIndexOnResume != -1) {
                this.startByte = startIndexOnResume(this.metaData) * CHUNK_SIZE;
                System.out.println("[" + thread.getId() + "] Start downloading range " +
                        "(" + this.startByte + " - " + this.endByte + ") from: " + this.mURL.toString());
                download();
            }
        }
        System.out.println("[" + thread.getId() + "] Finished downloading");
    }

    /**
     * This method implements the downloading methodology.
     */
    public void download(){
        this.bytesToDownload = this.endByte - this.startByte + 1;
        HttpURLConnection httpUrlConnection;
        InputStream inputStream;
        try {
            try {
                httpUrlConnection = (HttpURLConnection) this.mURL.openConnection();
                // Sets a timeout to a disconnection for 10 seconds
                httpUrlConnection.setConnectTimeout(TIME_TO_WAIT);
                httpUrlConnection.setReadTimeout(TIME_TO_WAIT);
                // Request the needed range
                httpUrlConnection.setRequestProperty("Range", "bytes=" + this.startByte + "-" + this.endByte);
            } catch (IOException e){
                System.err.println("Failed while opening a range connection with: " + this.mURL);
                System.err.println("Download failed");
                return;
            }

            inputStream = httpUrlConnection.getInputStream();

            byte[] chunkData = new byte[CHUNK_SIZE];
            int offset = (int) this.startByte;

            // If the range we got is less than CHUNK_SIZE, then we should
            // download a chunk that is less than CHUNK_SIZE
            if (this.bytesToDownload < CHUNK_SIZE){
                int index = offset/CHUNK_SIZE;

                // If we are in resume mode and we have'nt downloaded this
                // chunk yet, or if we are not in resume mode, then we need to
                // download one chunk
                if (this.metaData == null || !this.metaData.get(index)){
                    Chunk chunk = this.readChunkAndPutInQueue(chunkData, (int) bytesToDownload, offset, inputStream, this.queue);
                    offset += chunk.getSize();
                    this.mDownloaded += chunk.getSize();
                }

            // If we have more bytes to download than CHUNK_SIZE
            } else {
                while ((inputStream.read(chunkData, 0, CHUNK_SIZE)) > 0) {
                    int index = offset/CHUNK_SIZE;
                    // If we are on resume, and we already downloaded this
                    // chunk, we can skip this chunk
                    if (this.metaData != null && this.metaData.get(index)){
                        offset += CHUNK_SIZE;
                        this.mDownloaded += CHUNK_SIZE;
                        chunkData = new byte[CHUNK_SIZE];
                        continue;
                    }
                    // If we haven't downloaded this chunk so far
                    Chunk chunk = new Chunk(chunkData, offset);
                    this.queue.put(chunk);
                    offset += chunk.getSize();
                    this.mDownloaded += chunk.getSize();
                    chunkData = new byte[CHUNK_SIZE];

                    if (this.bytesToDownload - this.mDownloaded < CHUNK_SIZE){
                        break;
                    }
                }

                // If we reached here, so there is a possibility that we
                // finished downloading our range, or that we have some bytes
                // left to downloaded, but they are less than CHUNK_SIZE
                if (this.bytesToDownload - this.mDownloaded > 0) {
                    int index = offset/CHUNK_SIZE;
                    if (this.metaData == null || !metaData.get(index)) {
                        int leftToRead = (int)this.bytesToDownload - (int) this.mDownloaded;
                        this.readChunkAndPutInQueue(chunkData, leftToRead, offset, inputStream, this.queue);
                    }
                }
            }

            inputStream.close();
            httpUrlConnection.disconnect();

        } catch (InterruptedException e) {
            System.err.println("Failed while putting a chunk inputStream the queue");
            System.err.println("Download failed");
            System.exit(1);
        } catch (SSLException | SocketTimeoutException | ConnectException e){
            System.err.println("Internet connection lost");
            System.err.println("Download failed");
            System.exit(1);
        } catch (IOException e){
            System.err.println("A trouble occurred while trying to read range: " + this.startByte + "-" + this.endByte + " from: " + this.mURL);
            System.err.println("Download failed");
            System.exit(1);
        }
    }
}
