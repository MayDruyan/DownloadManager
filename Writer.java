import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingDeque;

/**
 * This class is responsible for writing the downloaded file. Meaning, it
 * takes chunks out of the queue are writes them down to the disk in the
 * correct order, such that when the writer finishes it's work, the file is
 * downloaded.
 */
public class Writer implements Runnable {

    private BlockingDeque<Chunk> queue;
    private final static double CHUNK_SIZE = 4096.0; // Size of chunk to download
    private int mFileSize;
    private int mDownloaded = 0;
    private int numOfChunks;

    private Path currentRelativePath;
    private Path temporaryPath;
    private String pathToDownloadTo;
    private Path metaDataPath;
    private String temporaryPathString;
    private File metaData;
    private File fileToDownload;

    private File tmpMetaDataFile;
    Path tempPath;

    public Writer(BlockingDeque<Chunk> queue, String fileName, int fileSize, int numOfChunks){
        this.queue = queue;
        this.fileToDownload = new File(fileName);
        this.mFileSize = fileSize;
        this.numOfChunks = numOfChunks;

        // Configure all the relevant paths for the metadata and the tmp
        // metadata
        currentRelativePath = Paths.get("");
        pathToDownloadTo = currentRelativePath.toAbsolutePath().toString();
        fileToDownload = new File(pathToDownloadTo + '/' + fileName);
        temporaryPath = currentRelativePath.resolve(fileName + ".1.tmp");
        metaDataPath = currentRelativePath.resolve(fileName + ".tmp");
        temporaryPathString = temporaryPath.toString();

        metaData = new File(metaDataPath.toString());
        tmpMetaDataFile = new File(temporaryPathString);
        tempPath = Paths.get(tmpMetaDataFile.getAbsolutePath());

    }

    /**
     * This method is intended to serialize the Metadata object. Meaning, it
     * receives a metadata object and saves it to a file in the disk
     * @param metadataObject the object we wish to serialize
     */
    private void serialize(Metadata metadataObject){
        try {
            // Write to a tmp file, so if the program stops in the middle of
            // the process and the tmp will be corrupted, then the metadata
            // will be safe
            FileOutputStream tmpMetaData = new FileOutputStream(temporaryPathString);
            ObjectOutputStream out = new ObjectOutputStream(tmpMetaData);
            out.writeObject(metadataObject);
            out.close();
            tmpMetaData.close();
            // Rename the tmp file to the metadata file
            Files.move(tempPath, metaDataPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        } catch (IOException e){
            System.err.println("Unable to write to metadata file");
            System.err.println("Download failed");
            System.exit(1);
        }
    }

     /**
      * This method is intended to deserialize the Metadata object from the
      * file. Meaning, it gets a file from the disk and translate it to
      * Metadata object.
      * @return metadataObject that is in the file
      */
    private Metadata deserialize(){
        Metadata metadataObject = null;
        try {
            FileInputStream fileIn = new FileInputStream(metaDataPath.toString());
            ObjectInputStream in = new ObjectInputStream(fileIn);
            metadataObject = (Metadata) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException | ClassNotFoundException e){
            System.err.println("Unable to get object from metadata file");
            System.err.println("Download failed");
            System.exit(1);
        }
        return metadataObject;
    }

    /**
     * The methods gets a chunk from the queue and writes it to the
     * downloaded file. It also updates the metadata that this chunk was
     * downloaded.
     * @param file the file we wish to download to
     * @param metaData the metadata we update
     */
    private void readChunk(RandomAccessFile file, Metadata metaData){
        try {
            Chunk chunk = queue.take();
            int index = (int)(chunk.getOffset()/CHUNK_SIZE);

            file.seek(chunk.getOffset());
            file.write(chunk.getData());

            metaData.setIndexToTrue(index);
            if (this.mDownloaded == 0){
                System.out.println("Downloaded 0%");
            }
            this.mDownloaded += chunk.getSize();

        } catch (InterruptedException | IOException e){
            System.err.println("Unable to write data to downloaded file");
            System.err.println("Download failed");
            System.exit(1);
        }
        // Once the metadata is updated, we want to save it to the disk so if
        // the program is exited or stopped, we can resume to the download
        serialize(metaData);
    }


    /**
     * Writer thread - as long there are chunks in the queue, reads them
     * and writes in file.
     * If no chunks in queue, waits until a chunk is added
     */
    @Override
    public void run() {
        Metadata metaDataObject;
        boolean metaDataExists = metaData.exists();

        try {
            // Checks if the metadata file exists. If not, we are on a
            // regular download
            if (!metaDataExists){
                fileToDownload.createNewFile();
                metaData.createNewFile();
                metaDataObject = new Metadata(numOfChunks);
            } else {
                // If the metadata file exists, we are on resume mode and
                // should use the metadata
                metaDataObject = deserialize();
                updateBytesDownloaded(metaDataObject);
            }

            RandomAccessFile raf = new RandomAccessFile(fileToDownload, "rw");
            System.out.println("Downloading...");

            int previousProgress = 0;
            while (this.mDownloaded < this.mFileSize) {
                double progress = getProgress();
                int intProgress = (int)progress;

                if(intProgress != previousProgress){
                    System.out.println("Downloaded " + intProgress + "%");
                    previousProgress = intProgress;
                }
                readChunk(raf, metaDataObject);
            }
            System.out.println("Download succeeded");

        } catch (IOException ex){
            System.err.println("Unable to create file");
            System.err.println("Download failed");
            System.exit(1);
        }

        if (!metaData.delete()){
            System.err.println("Unable to delete the metadata file");
            System.err.println("Download failed");
            System.exit(1);
        }
    }

    /**
     * This method updates the number of bytes that has been downloaded.
     * It goes over the metadata array and checks which elements are true -
     * downloaded, and sums them up.
     * @param metadata the metadata object we read from
     */
    private void updateBytesDownloaded(Metadata metadata){
        int downloadedBytes = 0;
        for (int i = 0; i < metadata.getMetadataSize(); i++){
            if (metadata.get(i)){
                if (i == (metadata.getMetadataSize() - 1)){
                    int lastChunkSize =  this.mFileSize % (int) CHUNK_SIZE;
                    downloadedBytes += lastChunkSize;
                } else {
                    downloadedBytes += CHUNK_SIZE;
                }
            }
        }
        this.mDownloaded = downloadedBytes;
    }

    /**
     * This method computes the percentage of the bytes downloaded out of the
     * whole file
     * @return the percentage
     */
    public double getProgress(){
        return ((double) this.mDownloaded / this.mFileSize) * 100;
    }
}
