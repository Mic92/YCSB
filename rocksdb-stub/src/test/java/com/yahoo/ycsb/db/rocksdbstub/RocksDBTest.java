package com.yahoo.ycsb.db.rocksdbstub;

import com.yahoo.ycsb.DBException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;

public class RocksDBTest {
  public static void deleteDirectory(String directoryFilePath) throws IOException {
    Path directory = Paths.get(directoryFilePath);

    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
          Files.delete(path);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path directory, IOException ioException) throws IOException {
          Files.delete(directory);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }

  private Path dbDir = null;

  @Before
  public void setup() throws IOException {
    dbDir = Files.createTempDirectory("myFile");
  }

  @After
  public void tearDown() throws Exception {
    if (dbDir == null) {
      deleteDirectory(dbDir.toString());
    }
  }

  @Test
  public void testServer() throws DBException {
    RocksDBClientStub instance = new RocksDBClientStub();

    final Properties properties = new Properties();
    properties.setProperty(RocksDBClientStub.PROPERTY_ROCKSDB_DIR, dbDir.toString());
    properties.setProperty(RocksDBClientStub.PROPERTY_ROCKSDB_EXECUTABLE, "./ycsb-rocksdb-server");
    instance.setProperties(properties);

    instance.init();
  }
}
