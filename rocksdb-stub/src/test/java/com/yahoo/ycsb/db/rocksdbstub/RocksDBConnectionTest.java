package com.yahoo.ycsb.db.rocksdbstub;


import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class RocksDBConnectionTest {
  final int testPid = 10;
  File requestsFile = null, responseFile = null;

  private File createQueue(String path, int size) throws IOException {
    File file = new File(path);
    file.createNewFile();
    file.deleteOnExit();
    RandomAccessFile f = new RandomAccessFile(file, "rw");
    f.setLength(4096);
    f.close();
    return file;
  }

  @Before
  public void setup() throws Exception {
    requestsFile = createQueue(RocksDBQueue.Type.REQUESTS.path(testPid), 4096);
    responseFile = createQueue(RocksDBQueue.Type.RESPONSE.path(testPid), 4096);
  }

  @Test
  public void getAndPut() throws Exception {
    RocksDBConnection conn = new RocksDBConnection(testPid);
    final int[] requestNumber = new int[1];

    Thread server = new Thread(() -> {
      RocksDBQueue requests = null;
      RocksDBQueue responses = null;
      try {
        requests = new RocksDBQueue(testPid,  RocksDBQueue.Type.REQUESTS);
        responses = new RocksDBQueue(testPid, RocksDBQueue.Type.RESPONSE);
      } catch (IOException e) {
        e.printStackTrace();
      }
      byte[] bytes = requests.pop();
      assertNotEquals(null, bytes);
      ByteBuffer buf = ByteBuffer.wrap(bytes);

      requestNumber[0] = buf.getInt();
      ByteBuffer resp = ByteBuffer.allocate(Integer.SIZE / 8);
      responses.push(resp.array());
    });

    Thread client = new Thread(() -> {
      conn.request(new byte[0]);
    });

    // fill the queue
    client.start();
    server.start();

    server.join();
    client.join();

    assertEquals(1, requestNumber[0]);

  }
}
