import java.io.Serializable;

/**
 * This class represents the metadata about the file that is being downloaded.
 * It contains all kinds of methods that operate on the metadata array
 */
public class Metadata implements Serializable {

    private boolean[] metaDataArray;

    public Metadata(int bitMapSize) {
        this.metaDataArray = new boolean[bitMapSize];
    }

    /**
     * A getter method. This method returns an element from the array,
     * indicated by index
     * @param index the element we want to return from this index
     * @return the element from the array
     */
    public boolean get(int index){
        return this.metaDataArray[index];
    }

    /**
     * A setter method. This method changes the value of an element in the array
     * indicated by index
     * @param index the index of the element we wish to change.
     */
    public void setIndexToTrue(int index) {
        this.metaDataArray[index] = true;
    }

    /**
     * A getter method.
     * @return the size of the array
     */
    public int getMetadataSize() {
        return this.metaDataArray.length;
    }
}