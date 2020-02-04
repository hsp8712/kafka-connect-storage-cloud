package io.confluent.connect.s3.storage;

import java.io.ByteArrayInputStream;
import java.nio.*;
import java.util.LinkedList;
import java.util.List;

public class MultipartByteBuffer  {

    public static final int MIN_PART_SIZE = 128 * 1024; // 128 KB
    public static final int INCREMENT_FACT = 4;
    private final int capacity;

    private int position;
    private byte[] currentPart;
    private int currentPartPos;

    private int currentTotalAllocSize;

    private List<byte[]> multipart = new LinkedList<>();

    private int nextPartSize() {
        int preNextPartSize = this.currentPart == null ?
                MIN_PART_SIZE : this.currentPart.length * INCREMENT_FACT;

        if (this.capacity >= (currentTotalAllocSize + preNextPartSize)) {
            return preNextPartSize;
        } else {
            return this.capacity - this.currentTotalAllocSize;
        }
    }

    private void addNewPart() {
        int nextPartSize = nextPartSize();
        if (nextPartSize > 0) {
            byte[] newPart = new byte[nextPartSize];
            multipart.add(newPart);
            currentPart = newPart;
            currentPartPos = 0;
            currentTotalAllocSize += nextPartSize;
        }
    }

    public MultipartByteBuffer(int capacity) {
        this.capacity = capacity;
        this.currentTotalAllocSize = 0;
        addNewPart();
        position = 0;
    }

    public void put(byte b) {
        if (position >= capacity) {
            throw new BufferOverflowException();
        }

        if (currentPart.length <= currentPartPos) {
            // current part full
            addNewPart();
        }

        currentPart[currentPartPos] = b;
        currentPartPos++;
        position++;
    }

    public void put(byte[] src, int offset, int length) {

        checkBounds(offset, length, src.length);

        if (length > remaining()) {
            throw new BufferOverflowException();
        }

        int remainingOffset = offset;
        int remainingLen = length;
        while (remainingLen > 0) {
            if (currentPart.length <= currentPartPos) {
                // current part full
                addNewPart();
            }
            int currentPartRemaining = currentPart.length - currentPartPos;
            if (currentPartRemaining >= remainingLen) {
                System.arraycopy(src, remainingOffset, currentPart, currentPartPos, remainingLen);
                currentPartPos += remainingLen;
                remainingOffset += remainingLen;
                remainingLen = 0;
            } else {
                System.arraycopy(src, remainingOffset, currentPart, currentPartPos, currentPartRemaining);
                currentPartPos += currentPartRemaining;
                remainingOffset += currentPartRemaining;
                remainingLen -= currentPartRemaining;
            }
        }

    }

    static void checkBounds(int off, int len, int size) { // package-private
        if ((off | len | (off + len) | (size - (off + len))) < 0)
            throw new IndexOutOfBoundsException();
    }

    public boolean hasRemaining() {
        return capacity > position;
    }

    public int remaining() {
        return capacity - position;
    }

    public int position() {
        return this.position;
    }

    public void clear() {
        int size = multipart.size();
        for (int i = 1; i < size; i++) {
            multipart.remove(1);
        }
    }

    public ByteArrayInputStream getArrayStream() {
        if (multipart.size() == 1) {
            return new ByteArrayInputStream(multipart.get(0), 0, this.position);
        } else {
            byte[] finalBytes = new byte[this.position];
            int offset = 0;
            for (int i = 0; i < (multipart.size() - 1); i++) {
                byte[] part = multipart.get(i);
                System.arraycopy(part, 0, finalBytes, offset, part.length);
                offset += part.length;
            }

            System.arraycopy(currentPart, 0, finalBytes, offset, currentPartPos);
            return new ByteArrayInputStream(finalBytes);
        }
    }

}
