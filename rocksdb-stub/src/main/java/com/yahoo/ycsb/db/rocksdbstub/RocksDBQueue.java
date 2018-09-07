package com.yahoo.ycsb.db.rocksdbstub;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RocksDBQueue {
  public enum Type {
    REQUESTS,
    RESPONSE;

    public String path(long pid) {
      final String prefix = this == REQUESTS ? "rocksdb-request-queue." : "rocksdb-response-queue.";
      final Path path = Paths.get("/","dev", "shm", prefix + Long.toString(pid, 10));
      return path.toString();
    }
  }

  private final int INT_SIZE = Integer.SIZE / 8;
  private final int HEAD_INDEX = 0;
  private final int TAIL_INDEX = INT_SIZE;
  private final int HEADER_SIZE = INT_SIZE * 2;
  private final MappedByteBuffer buffer;

  public RocksDBQueue(long pid, Type type) throws IOException {
    final File file = new File(type.path(pid));
    final FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size());
    buffer.order(ByteOrder.nativeOrder());

    assert INT_SIZE == 4;
  }

  private int getHead() {
    return buffer.getInt(HEAD_INDEX);
  }

  private void setHead(int value) {
    buffer.putInt(HEAD_INDEX, value);
  }

  private int getTail() {
    return buffer.getInt(TAIL_INDEX);
  }

  private void setTail(int value) {
    buffer.putInt(TAIL_INDEX, value);
  }


  private int size() {
    final int head = getHead();
    final int tail = getTail();
    if (head >= tail) {
      return head - tail;
    } else {
      return (limit() - tail) + head;
    }
  }

  private int limit() {
    return buffer.limit();
  }

  private void put(int index, byte b) {
    buffer.put(HEADER_SIZE + index, b);
  }

  private byte get(int index) {
    return buffer.get(HEADER_SIZE + index);
  }

  private int getInt(int index) {
    return buffer.getInt(HEADER_SIZE + index);
  }

  private void putInt(int index, int value) {
    buffer.putInt(HEADER_SIZE + index, value);
  }

  public void push(byte[] bytes) {
    final int msgLen = bytes.length + INT_SIZE;
    while (size() + msgLen > limit()) {}

    int head = getHead();
    putInt(head, bytes.length);
    head += INT_SIZE;

    for (int i = 0; i < bytes.length; i++) {
      put(head, bytes[i]);
      head++;
      head %= limit();
    }

    setHead(head);
  }

  public byte[] pop() {
    while (size() == 0) {}
    int tail = getTail();
    final int elemLen = getInt(tail);
    tail += INT_SIZE;

    final byte[] bytes = new byte[elemLen];

    for (int i = 0; i < elemLen; i++) {
      bytes[i] = get(tail % limit());
      tail++;
      tail %= limit();
    }

    setTail(tail);

    return bytes;
  }
}
