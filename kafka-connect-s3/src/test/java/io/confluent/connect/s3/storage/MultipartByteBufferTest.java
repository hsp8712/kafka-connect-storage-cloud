package io.confluent.connect.s3.storage;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.BufferOverflowException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MultipartByteBufferTest {

    @Test
    public void testOnePartCase() {
        MultipartByteBuffer buffer = new MultipartByteBuffer(10);
        assertEquals(1, buffer.partNum());
        assertEquals(10, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(0, buffer.position());

        assertEquals(0, buffer.getArrayStream().available());


        buffer.put((byte) 0x11);
        assertEquals(1, buffer.partNum());
        assertEquals(9, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(1, buffer.position());

        assertEquals(1, buffer.getArrayStream().available());
        assertEquals(0x11, buffer.getArrayStream().read());

        buffer.put(new byte[] {
                0x22, 0x22, 0x22, 0x22
        }, 0, 4);
        assertEquals(1, buffer.partNum());
        assertEquals(5, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(5, buffer.position());

        assertEquals(5, buffer.getArrayStream().available());

        buffer.put(new byte[] {
                0x33, 0x33, 0x33, 0x33, 0x33
        }, 0, 5);
        assertEquals(1, buffer.partNum());
        assertEquals(0, buffer.remaining());
        assertEquals(false, buffer.hasRemaining());
        assertEquals(10, buffer.position());

        assertEquals(10, buffer.getArrayStream().available());

        try {
            buffer.put((byte) 0x44);
            fail("Expect BufferOverflowException!");
        } catch (Exception e) {
            if (!(e instanceof BufferOverflowException)) {
                fail("Expect BufferOverflowException!");
            }
        }

        try {
            buffer.put(new byte[] {
                    0x55, 0x55
            }, 0, 2);
            fail("Expect BufferOverflowException!");
        } catch (Exception e) {
            if (!(e instanceof BufferOverflowException)) {
                fail("Expect BufferOverflowException!");
            }
        }

        buffer.clear();
        assertEquals(1, buffer.partNum());
        assertEquals(10, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.getArrayStream().available());
    }

    @Test
    public void testMultipartCase() {
        int capacity = 10 + (128 * 1024) + (4 * 128 * 1024);
        System.out.println("capacity: " + capacity);
        MultipartByteBuffer buffer = new MultipartByteBuffer(capacity);
        assertEquals(1, buffer.partNum());
        assertEquals(capacity, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.getArrayStream().available());

        buffer.put((byte) 0x11);
        assertEquals(1, buffer.partNum());
        assertEquals(capacity - 1, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(1, buffer.position());
        assertEquals(1, buffer.getArrayStream().available());
        assertEquals(0x11, buffer.getArrayStream().read());

        byte[] _128kb = new byte[128 * 1024];
        Arrays.fill(_128kb, (byte) 0x22);

        buffer.put(_128kb, 0, _128kb.length);
        assertEquals(2, buffer.partNum());
        assertEquals((capacity - (128 * 1024) - 1), buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals((128 * 1024) + 1, buffer.position());
        assertEquals((128 * 1024) + 1, buffer.getArrayStream().available());

        buffer.clear();

        // test again after clear

        assertEquals(1, buffer.partNum());
        assertEquals(capacity, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.getArrayStream().available());

        assertEquals(1, buffer.partNum());
        assertEquals(capacity, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.getArrayStream().available());

        buffer.put((byte) 0x11);
        assertEquals(1, buffer.partNum());
        assertEquals(capacity - 1, buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals(1, buffer.position());
        assertEquals(1, buffer.getArrayStream().available());
        assertEquals(0x11, buffer.getArrayStream().read());

        byte[] _128kb_2 = new byte[128 * 1024];
        Arrays.fill(_128kb_2, (byte) 0x22);

        buffer.put(_128kb_2, 0, _128kb_2.length);
        assertEquals(2, buffer.partNum());
        assertEquals((capacity - (128 * 1024) - 1), buffer.remaining());
        assertEquals(true, buffer.hasRemaining());
        assertEquals((128 * 1024) + 1, buffer.position());
        assertEquals((128 * 1024) + 1, buffer.getArrayStream().available());
    }

    public void execStringTest(MultipartByteBuffer buffer, int strLen) {
        System.out.println("execStringTest-------------------------");
        System.out.println("strLen: " + strLen);
        String str = RandomStringUtils.randomAlphanumeric(strLen);
        byte[] bytes = str.getBytes();
        assertEquals(strLen, bytes.length);

        buffer.put(bytes, 0, bytes.length);

        System.out.println("partNum: " + buffer.partNum());
        System.out.println("position: " + buffer.position());

        ByteArrayInputStream stream = buffer.getArrayStream();
        assertEquals(strLen, stream.available());

        byte[] decodeBytes = new byte[strLen];
        stream.read(decodeBytes, 0, strLen);
        String decodeStr = new String(decodeBytes);

        assertEquals(str, decodeStr);
    }

    @Test
    public void testOnePartForRandomData() {
        MultipartByteBuffer buffer = new MultipartByteBuffer(1000);
        execStringTest(buffer, 900);
        buffer.clear();
        execStringTest(buffer, 900);
        buffer.clear();
        execStringTest(buffer, 1000);
    }

    @Test
    public void testMultipartForRandomData() {
        MultipartByteBuffer buffer = new MultipartByteBuffer(25 * 1024 * 1024);
        execStringTest(buffer, 900);
        buffer.clear();
        execStringTest(buffer, 10 * 1024 * 1024);
        buffer.clear();
        execStringTest(buffer, 25 * 1024 * 1024);
    }

/*    public static int cal(int parts) {
        int init = 128 * 1024;
        int sum = 0;
        for (int i = 0; i < parts; i++) {
            sum += Math.pow(4, i) * init;
        }
        return sum;
    }

    public static void main(String[] args) {
        System.out.println("1 - " + cal(1));
        System.out.println("2 - " + cal(2));
        System.out.println("3 - " + cal(3));
        System.out.println("4 - " + cal(4));
        System.out.println("5 - " + cal(5));
    }*/
}