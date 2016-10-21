package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * TODO:
 *
 * 1. Corruption detection, incl CRC checks
 * 2. Version header
 * 3. Batching
 * 4. Configurable fsync
 *
 *
 *
 * Created by tim on 07/10/16.
 */
public class FileLog implements Log {

    private final static Logger log = LoggerFactory.getLogger(FileLog.class);

    private static final int MAX_CREATE_BUFF_SIZE = 10 * 1024 * 1024;
    private static final String LOG_INFO_FILE_TAIL = "-log-info.dat";

    private final Vertx vertx;
    private final FileAccessManager faf;
    private final String channel;
    private final FileLogManagerOptions options;

    private BasicFile currWriteFile;
    private BasicFile nextWriteFile;
    private int fileNumber; // Number of log file containing current head
    private int filePos;    // Position of head in head file
    private long headPos;   // Overall position of head in log
    private CompletableFuture<Void> nextFileCF;

    public FileLog(Vertx vertx, FileAccessManager faf, FileLogManagerOptions options, String channel) {
        this.vertx = vertx;
        this.channel = channel;
        this.options = options;
        this.faf = faf;
        if (options.getMaxLogChunkSize() < 1) {
            throw new IllegalArgumentException("maxLogChunkSize must be > 1");
        }
        if (options.getPreallocateSize() < 0) {
            throw new IllegalArgumentException("maxLogChunkSize must be >= 0");
        }
        if (options.getMaxRecordSize() < 1) {
            throw new IllegalArgumentException("maxRecordSize must be > 1");
        }
        if (options.getMaxRecordSize() > options.getMaxLogChunkSize()) {
            throw new IllegalArgumentException("maxRecordSize must be <= maxLogChunkSize");
        }
        if (options.getPreallocateSize() > options.getMaxLogChunkSize()) {
            throw new IllegalArgumentException("preallocateSize must be <= maxLogChunkSize");
        }
    }

    public synchronized CompletableFuture<Void> start() {
        log.trace("Starting file log for channel {}", channel);

        loadInfo();
        checkAndLoadFiles();
        File currFile = getFile(fileNumber);
        CompletableFuture<Void> cfCreate = null;
        if (!currFile.exists()) {
            if (fileNumber == 0 && filePos == 0) {
                // This is OK, new log
                log.trace("Creating new log info file for channel {}", channel);
                saveInfo(true);
                // Create a new first file
                cfCreate = createAndFillFile(getFileName(0));
            } else {
                throw new MewException("Info file for channel {} doesn't match data file(s)");
            }
        }
        final File cFile = currFile;
        // Now open the BasicFile
        CompletableFuture<BasicFile> cf;
        if (cfCreate != null) {
            cf = cfCreate.thenCompose(v -> faf.openBasicFile(cFile));
        } else {
            cf = faf.openBasicFile(currFile);
        }
        return cf.thenApply(bf -> {
            currWriteFile = bf;
            return (Void)null;
        });
    }

    @Override
    public CompletableFuture<ReadStream> openReadStream(long pos) {
        // TODO limit number of open streams to avoid exhausting file handles
        // TODO pos
        File file = getFile(0);
        return faf.openBasicFile(file).thenApply(bf -> new LogReadStream((int)pos, bf));
    }

    @Override
    public synchronized CompletableFuture<Long> append(BsonObject obj) {

        Buffer record = obj.encode();
        int len  = record.length();
        if (record.length() > options.getMaxRecordSize()) {
            throw new MewException("Record too long " +len + " max " + options.getMaxRecordSize());
        }

        CompletableFuture<Long> cf;

        log.trace("in append filePos is {} file size is {}", filePos, options.getMaxLogChunkSize());

        int remainingSpace = options.getMaxLogChunkSize() - filePos;
        log.trace("Remaining space is {}", remainingSpace);
        if (record.length() > remainingSpace) {
            if (remainingSpace > 0) {
                // Write into the remaining space so all log chunk files are same size
                Buffer buffer = Buffer.buffer(new byte[remainingSpace]);
                append0(buffer.length(), buffer);
            }
            // Move to next file
            if (nextWriteFile != null) {
                log.trace("Moving to next log file");
                currWriteFile = nextWriteFile;
                filePos = 0;
                fileNumber++;
                nextWriteFile = null;
            } else {
                log.warn("Eager create of next file too slow, nextFileCF {}", nextFileCF);
                checkCreateNextFile();
                // Next file creation is in progress, just wait for it
                cf = new CompletableFuture<>();
                nextFileCF.thenAccept(v -> {
                    log.trace("Next file cf completed");
                    // When complete just call append again
                    CompletableFuture<Long> again = append(obj);
                    again.handle((pos, t) -> {
                        if (t != null) {
                            cf.completeExceptionally(t);
                        } else {
                            cf.complete(pos);
                        }
                        return null;
                    });
                });
                return cf;
            }
        }

        cf = append0(len, record);
        checkCreateNextFile();
        return cf;
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        CompletableFuture<Void> ret;
        if (currWriteFile != null) {
            ret = currWriteFile.close();
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        saveInfo(true);
        currWriteFile = null;
        nextWriteFile = null;
        nextFileCF = null;
        fileNumber = 0;
        filePos = 0;
        headPos = 0;
        return ret;
    }

    @Override
    public synchronized int getFileNumber() {
        return fileNumber;
    }

    @Override
    public synchronized long getHeadPos() {
        return headPos;
    }

    @Override
    public synchronized int getFilePos() {
        return filePos;
    }

    private CompletableFuture<Long> append0(int len, Buffer record) {
        int writePos = filePos;
        long overallWritePos = headPos;
        filePos += len;
        headPos += len;
        return currWriteFile.append(record, writePos).thenApply(v -> overallWritePos);
    }

    private void saveInfo(boolean shutdown) {
        BsonObject info = new BsonObject();
        info.put("fileNumber", fileNumber);
        info.put("headPos", headPos);
        info.put("fileHeadPos", filePos);
        info.put("shutdown", shutdown);
        saveFileInfo(info);
    }

    private void loadInfo() {
        BsonObject info = loadFileInfo();
        if (info != null) {
            try {
                Integer fNumber = info.getInteger("fileNumber");
                if (fNumber == null) {
                    throw new MewException("Invalid log info file, no fileNumber");
                }
                if (fNumber < 0){
                    throw new MewException("Invalid log info file, negative fileNumber");
                }
                this.fileNumber = fNumber;
                Integer hPos = info.getInteger("headPos");
                if (hPos == null) {
                    throw new MewException("Invalid log info file, no headPos");
                }
                if (hPos < 0){
                    throw new MewException("Invalid log info file, negative headPos");
                }
                this.headPos = hPos;
                Integer fhPos = info.getInteger("fileHeadPos");
                if (fhPos == null) {
                    throw new MewException("Invalid log info file, no fileHeadPos");
                }
                if (fhPos < 0){
                    throw new MewException("Invalid log info file, negative fileHeadPos");
                }
                this.filePos = fhPos;
                Boolean shutdown = info.getBoolean("shutdown");
                if (shutdown == null) {
                    throw new MewException("Invalid log info file, no shutdown");
                }
            } catch (ClassCastException e) {
                throw new MewException("Invalid info file for channel " + channel, e);
            }
        }
    }

    private BsonObject loadFileInfo() {
        File f = new File(options.getLogDir(), getLogInfoFileName());
        if (!f.exists()) {
            return null;
        } else {
            try {
                byte[] bytes = Files.readAllBytes(f.toPath());
                Buffer buff = Buffer.buffer(bytes);
                return new BsonObject(buff);
            } catch (IOException e) {
                throw new MewException(e);
            }
        }
    }

    private void saveFileInfo(BsonObject info) {
        Buffer buff = info.encode();
        File f = new File(options.getLogDir(), getLogInfoFileName());
        try {
            if (!f.exists()) {
                if (!f.createNewFile()) {
                    throw new MewException("Failed to create file " + f);
                }
            }
            Files.write(f.toPath(), buff.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new MewException(e);
        }
    }

    private File getFile(int fileNumber) {
        return new File(options.getLogDir(), getFileName(fileNumber));
    }

    private void checkCreateNextFile() {
        // We create a next file when the current file is half written
        if (nextFileCF == null && nextWriteFile == null && filePos > options.getMaxLogChunkSize() / 2) {
            nextFileCF = new CompletableFuture<>();
            CompletableFuture<Void> cfNext = createAndFillFile(getFileName(fileNumber + 1));
            CompletableFuture<BasicFile> cfBf = cfNext.thenCompose(v -> {
                File next = getFile(fileNumber + 1);
                return faf.openBasicFile(next);
            });
            cfBf.handle((bf, t) -> {
               if (t == null) {
                   nextWriteFile = bf;
                   nextFileCF.complete(null);
                   nextFileCF = null;
               } else {
                   log.error("Failed to create next file", t);
               }
               return null;
            });
        }
    }

    private CompletableFuture<Void> createAndFillFile(String fileName) {
        AsyncResCF<Void> cf = new AsyncResCF<>();
        File next =  new File(options.getLogDir(), fileName);
        vertx.executeBlocking(fut -> {
            createAndFillFileBlocking(next, options.getPreallocateSize());
            fut.complete(null);
        }, false, cf);
        return cf;
    }

    private void createAndFillFileBlocking(File file, int size) {
        log.trace("Creating log file {} with size {}", file, size);
        ByteBuffer buff = ByteBuffer.allocate(MAX_CREATE_BUFF_SIZE);
        try (RandomAccessFile rf = new RandomAccessFile(file, "rw")) {
            FileChannel ch = rf.getChannel();
            int pos = 0;
            // We fill the file in chunks in case it is v. big - we don't want to allocate a huge byte buffer
            while (pos < size) {
                int writeSize = Math.min(MAX_CREATE_BUFF_SIZE, size - pos);
                buff.limit(writeSize);
                buff.position(0);
                ch.position(pos);
                ch.write(buff);
                pos += writeSize;
            }
            ch.force(true);
            ch.position(0);
            ch.close();
        } catch (Exception e) {
            throw new MewException("Failed to create log file", e);
        }
        log.trace("Created log file {}", file);
    }

    /*
    List and check all the files in the log dir for the channel
     */
    private void checkAndLoadFiles() {
        Map<Integer, File> fileMap = new HashMap<>();
        File logDir = new File(options.getLogDir());
        File[] files = logDir.listFiles(file -> {
            String name = file.getName();
            int lpos = name.lastIndexOf("-");
            if (name.endsWith(LOG_INFO_FILE_TAIL)) {
                return false;
            }
            if (lpos == -1) {
                log.warn("Unexpected file in log dir: " + file);
                return false;
            } else {
                String chName = name.substring(0, lpos);
                int num = Integer.valueOf(name.substring(lpos + 1, name.length() - 4));
                boolean matches = chName.equals(channel);
                if (matches) {
                    fileMap.put(num, file);
                }
                return matches;
            }
        });
        if (files == null) {
            throw new MewException("Failed to list files in dir {}", logDir.toString());
        }

        Arrays.sort(files, (f1, f2) -> f1.compareTo(f2));
        // All files before the head file must be right size
        // TODO test this
        for (int i = 0; i < files.length; i++) {
            if (i < fileNumber && options.getMaxLogChunkSize() != files[i].length()) {
                throw new MewException("File unexpected size: " + files[i]);
            }
        }

        log.trace("There are {} files in {} for channel {}", files.length, logDir, channel);

        for (int i = 0; i < fileMap.size(); i++) {
            // Check file names are contiguous
            String fname = getFileName(i);
            if (!fileMap.containsKey(i)) {
                throw new MewException("Log files not in expected sequence, can't find " + fname);
            }
        }
    }


    private String getFileName(int i) {
        return channel + "-" + i + ".log";
    }

    private String getLogInfoFileName() {
        return channel + LOG_INFO_FILE_TAIL;
    }

    private static final class QueueEntry {
        final int receivedPos;
        final BsonObject bson;

        public QueueEntry(int receivedPos, BsonObject bson) {
            this.receivedPos = receivedPos;
            this.bson = bson;
        }
    }

    private final class LogReadStream implements ReadStream {

        private static final int READ_BUFFER_SIZE = 4 * 1024;

        private BasicFile fa;
        private int streamFileNumber;
        private int readPos; // TODO should be long?
        private int receivedPos; // TODO should be long?
        private int size = -1;
        private RecordParser parser;
        private BiConsumer<Long, BsonObject> handler;
        private Consumer<Throwable> exceptionHandler;
        private Queue<QueueEntry> queue = new LinkedList<>();
        private boolean paused;

        public LogReadStream(int readPos, BasicFile fa) {
            this.readPos = readPos;
            this.fa = fa;
            parser = RecordParser.newFixed(4, this::handleRec);
        }

        private void handleRec(Buffer buff) {
            if (size == -1) {
                size = buff.getIntLE(0) - 4;
                log.trace("Size is {}", size);
                parser.fixedSizeMode(size);
            } else {
                //log.trace("Got frame size {}", size);
                if (size != 0) {
                    log.trace("Got frame size {}", size);
                    handleFrame(receivedPos, size, buff);
                    receivedPos += buff.length() + 4;
                    parser.fixedSizeMode(4);
                    size = -1;
                }
            }
        }

        private void handleFrame(int receivedPos, int size, Buffer buffer) {
            // TODO bit clunky - need to add size back in so it can be decoded, improve this!
            Buffer buff2 = Buffer.buffer(buffer.length() + 4);
            buff2.appendIntLE(size + 4).appendBuffer(buffer);
            BsonObject bson = new BsonObject(buff2);
            if (handler != null) {
                if (paused) {
                    queue.add(new QueueEntry(receivedPos, bson));
                } else {
                    handler.accept((long)receivedPos, bson);
                }
            } else {
                log.trace("No handler");
            }
        }

        @Override
        public void exceptionHandler(Consumer<Throwable> handler) {
            this.exceptionHandler = handler;
        }

        @Override
        public void handler(BiConsumer<Long, BsonObject> handler) {
            this.handler = handler;
        }

        @Override
        public synchronized void pause() {
            paused = true;
        }

        @Override
        public synchronized void resume() {
            paused = false;
            while (!paused) {
                QueueEntry entry = queue.poll();
                if (entry == null) {
                    break;
                }
                handler.accept((long)entry.receivedPos, entry.bson);
            }
            if (!paused) {
                doRead();
            }
        }

        @Override
        public void close() {
            paused = true;
            fa.close();
        }

        private void doRead() {
            Buffer readBuff = Buffer.buffer(READ_BUFFER_SIZE);
            CompletableFuture<Void> cf = fa.read(readBuff, READ_BUFFER_SIZE, readPos);
            cf.handle((v, t) -> {
                if (t == null) {
                    log.trace("Passing buffer length {} to parser", readBuff);
                    if (readBuff.length() > 0) {
                        parser.handle(readBuff);
                    }
                    if (readBuff.length() < READ_BUFFER_SIZE) {
                        // Move to next file

                        // TODO thread safety
                        if (streamFileNumber < fileNumber) {
                            streamFileNumber++;
                            if (streamFileNumber == fileNumber) {
                                // TODO careful about last record
                            } else {
                                fa.close();
                                CompletableFuture<BasicFile> cfOpen =
                                        faf.openBasicFile(new File(options.getLogDir(), getFileName(streamFileNumber)));
                                cfOpen.handle((bf, t2) -> {
                                   if (t2 != null) {
                                       log.error("Failed to open file", t2);
                                   } else {
                                       fa = bf;
                                   }
                                   return null;
                                });
                            }
                        }
                    } else {
                        readPos += readBuff.length();
                        if (!paused) {
                            faf.scheduleOp(this::doRead);
                        }
                    }
                } else {
                    handleException(t);
                }
                return null;
            });
        }

        private void handleException(Throwable t) {
            if (exceptionHandler != null) {
                exceptionHandler.accept(t);
            } else {
                log.error("Failed to read", t);
            }
        }
    }

}
