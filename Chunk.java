/**
 * This class represents a Chunk object. Each chunk represent part of the
 * downloaded file. It holds a byte array of data and the offest of the chunk
 * in the file.
 */
public class Chunk {

    private byte[] data;
    private int offset;

    public Chunk(byte[] data, int offset) {
        this.data = data;
        this.offset = offset;
    }

    public int getSize() {
        return this.data.length;
    }
    public int getOffset() {
        return offset;
    }
    public byte[] getData() {
        return data;
    }
}
