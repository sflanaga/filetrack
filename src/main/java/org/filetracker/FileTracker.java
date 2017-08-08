package org.filetracker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.filetracker.FileTracker.TrackInfo;

public class FileTracker {

    public final static Path END = Paths.get("");

    public final static AtomicLong bytesRead = new AtomicLong();
    public final static AtomicLong filesRead = new AtomicLong();
    public final static AtomicReference<String> lastchangemsg = new AtomicReference<String>("");
    
    public static class FileReader extends Thread {
        private final LinkedBlockingQueue<Path> jobQueue;
        private final LinkedBlockingQueue<TrackInfo> sqlQueue;

        public FileReader(LinkedBlockingQueue<Path> jobQueue, LinkedBlockingQueue<TrackInfo> sqlQueue) {
            this.jobQueue = jobQueue;
            this.sqlQueue = sqlQueue;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Path p = jobQueue.take();
                    if (p == END)
                        break;

                    try {
                        process(p);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } 
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void process(Path path) throws Exception {
            String sha1 = createSha1(path);
            long lastMod = Files.getLastModifiedTime(path).toMillis();
            long size = Files.size(path);
            TrackInfo info = new TrackInfo(path, sha1, lastMod, size);
            sqlQueue.put(info);
            //System.out.println(info);
        }

        public String createSha1(Path path) throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            InputStream fis = Files.newInputStream(path);
            int n = 0;
            byte[] buffer = new byte[1024 * 64];
            while (n != -1) {
                n = fis.read(buffer);
                if (n > 0) {
                    digest.update(buffer, 0, n);
                }
            }
            return new HexBinaryAdapter().marshal(digest.digest());
            // return digest.digest();
        }
    }

    private static TrackInfo ENDTRACK = new TrackInfo(null, null, 0, 0);

    public static class Ticker extends Thread {
        
        @Override
        public void run() {
            long lastsize = 0L;
            while(true) {
                System.out.printf("size: %s  files read: %d\n%s\n", Util.greekSize(bytesRead.get()-lastsize), filesRead.get(),lastchangemsg.get());
                
                lastsize = bytesRead.get();
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
    
    public static class SqlThread extends Thread {
        final LinkedBlockingQueue<TrackInfo> sqlQueue;
        long processstart = System.currentTimeMillis();
        public SqlThread(LinkedBlockingQueue<TrackInfo> sqlQueue) {
            this.sqlQueue = sqlQueue;
        }

        @Override
        public void run() {
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }

            try (Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db")) {
                connection.setAutoCommit(false);
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(30); // set timeout to 30 sec.
                statement.executeUpdate("pragma cache_size=-8000");
                statement.executeUpdate("create table IF NOT EXISTS file ( path text primary key, lastupdate int, sha1_key text, size int, count int) ");
                connection.commit();

                long commit_interval = 10;
                long changecount = 0;
                while (true) {
                    TrackInfo info = sqlQueue.take();
                    if (info == ENDTRACK)
                        break;

                    bytesRead.addAndGet(info.size);
                    filesRead.incrementAndGet();
                    
                    String selectSql = String.format("select path, lastupdate, sha1_key, size, count from file where path = \"%s\"", info.path.toAbsolutePath().toString());
                    lastchangemsg.set(selectSql);
                    //System.out.println(selectSql);
                    ResultSet rs = statement.executeQuery(selectSql);
                    if (rs.next()) {
                        String currPath = rs.getString(1);
                        long lastupdate = rs.getLong(2);
                        String currSha1 = rs.getString(3);
                        long size = rs.getLong(4);
                        long count = rs.getLong(5);
                        if (info.lastMod != lastupdate || !currSha1.equals(info.sha1) || size != info.size) {
                            String updateSql = "update file set lastupdate=" + info.lastMod + ", sha1_key = \"" + info.sha1 + "\", size=" + info.size + ", count=" + count + 1
                                    + " where path = \"" + currPath + "\"";
                          //   System.out.println(updateSql);
                            lastchangemsg.set(updateSql);
                            changecount++;
                        }
                        if (rs.next())
                            System.err.println("duplicate path: " + info.path.toAbsolutePath().toString());
                    } else {
                        String insertSql = String.format("insert into file(path, lastupdate, sha1_key, size, count) values (\"%s\",%d, \"%s\", %d, %d) ",
                                info.path.toAbsolutePath().toString(), info.lastMod, info.sha1, info.size, 0);
                        lastchangemsg.set(insertSql);

                        //System.out.println(insertSql);
                        statement.executeUpdate(insertSql);
                        changecount++;
                    }
                    if (changecount % commit_interval == 0 ) {
                        connection.commit();
                        //System.out.println("commit at " + changecount);
                    }
                    
                    //if ( bytecount % (1024*1024) == 0 ) 
         //               System.out.println("byte count: " + bytecount );

                } // while forever

                connection.commit();

            } catch (SQLException | InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } finally {
                long processend = System.currentTimeMillis();
                System.out.printf("runtime: %s", Util.longSpanToStringShort((processend-processstart), 2));
            }
        }
    }

    public static class TrackInfo {
        public final Path path;
        public final String sha1;
        public final long lastMod;
        public final long size;

        public TrackInfo(Path path, String sha1, long lastMod, long size) {
            super();
            this.path = path;
            this.sha1 = sha1;
            this.lastMod = lastMod;
            this.size = size;
        }

        @Override
        public String toString() {
            return String.format("TrackInfo [path=%s, sha1=%s, lastMod=%s, size=%s]", path, sha1, lastMod, size);
        }
    }

    public static void main(String[] args) {
        int no = 4;

        final Ticker ticker = new Ticker();
        ticker.setDaemon(true);
        ticker.start();
        
        final LinkedBlockingQueue<Path> jobQueue = new LinkedBlockingQueue<Path>(100);
        final LinkedBlockingQueue<TrackInfo> sqlQueue = new LinkedBlockingQueue<TrackInfo>(100);

        FileReader[] readers = new FileReader[no];
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new FileReader(jobQueue, sqlQueue);
            readers[i].start();
        }
        SqlThread sqlthread = new SqlThread(sqlQueue);
        sqlthread.start();

        try {
           Files.walkFileTree(Paths.get(args[0]), new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                 if(!attrs.isDirectory()){
                      try {
                        jobQueue.put(file);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                 }
                 return FileVisitResult.CONTINUE;
             }
            });
        } catch (IOException e) {
             e.printStackTrace();
        }
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(args[0]))) {
            for (Path entry : stream) {
                if (Files.isReadable(entry) && Files.isRegularFile(entry))
                    jobQueue.put(entry);
            }
            for (int i = 0; i < readers.length; i++) {
                jobQueue.put(END);
            }
            for (int i = 0; i < readers.length; i++) {
                readers[i].join();
            }
            sqlQueue.put(ENDTRACK);
            sqlthread.join();

        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
