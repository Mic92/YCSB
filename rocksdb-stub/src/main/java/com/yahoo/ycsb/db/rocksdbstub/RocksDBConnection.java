package com.yahoo.ycsb.db.rocksdbstub;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.*;

public class RocksDBConnection {
  final RocksDBQueue requests, responses;
  int requestNumber = 0;
  private final int INT_SIZE = Integer.SIZE / 8;

  public static synchronized long getPidOfProcess(Process p) throws IOException {
    try {
      Field f = p.getClass().getDeclaredField("pid");
      f.setAccessible(true);
      long pid = f.getLong(p);
      f.setAccessible(false);
      return pid;
    } catch (NoSuchFieldException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  public RocksDBConnection(Process process) throws IOException {
    this(getPidOfProcess(process));
  }

  public RocksDBConnection(long pid) throws IOException {
    requests = new RocksDBQueue(pid, RocksDBQueue.Type.REQUESTS);
    responses = new RocksDBQueue(pid, RocksDBQueue.Type.RESPONSE);
  }

  public byte[] request(byte[] data) {
    requestNumber++;
    ByteBuffer buf = ByteBuffer.allocate(data.length + INT_SIZE);
    buf.putInt(requestNumber);
    buf.put(data);

    byte[] reqBuf = buf.array();
    requests.push(reqBuf);
    return responses.pop();
  }
}
