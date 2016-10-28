package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.common.SubDescriptor;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 *
 * Public methods always accessed from same event loop
 *
 * Package protected methods accessed from event loop of emitter
 *
 * Created by tim on 22/10/16.
 */
public class FileLogStream implements ReadStream {

    private final static Logger log = LoggerFactory.getLogger(FileLogStream.class);

    private final FileLog fileLog;
    private final SubDescriptor subDescriptor;
    private final Context context;
    private final int readBufferSize;
    private final Queue<BufferedRecord> buffered = new LinkedList<>();
    private BiConsumer<Long, BsonObject> handler;
    private Consumer<Throwable> exceptionHandler;

    private boolean paused;
    private long deliveredPos = -1;
    private boolean retro;
    private long fileStreamPos;
    private boolean ignoreFirst;
    private int fileNumber;
    private int fileReadPos;
    private BasicFile streamFile;
    private int fileSize;
    private RecordParser parser;
    private int recordSize = -1;

    public FileLogStream(FileLog fileLog, SubDescriptor subDescriptor, int readBufferSize,
                         int fileSize) {
        this.fileLog = fileLog;
        this.subDescriptor = subDescriptor;
        this.context = Vertx.currentContext();
        this.readBufferSize = readBufferSize;
        this.fileSize = fileSize;
        resetParser();
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
    public synchronized void start() {
        if (subDescriptor.getStartPos() != -1) {
            goRetro(false, subDescriptor.getStartPos());
        } else {
            fileLog.readdSubHolder(this);
        }
    }

    @Override
    public synchronized void pause() {
        paused = true;
    }

    @Override
    public synchronized void resume() {
        if (!paused) {
            return;
        }
        paused = false;
        if (!buffered.isEmpty()) {
            while (true) {
                BufferedRecord br = buffered.poll();
                if (br == null) {
                    break;
                }
                handle0(br.pos, br.bson);
                if (paused) {
                    return;
                }
            }
        }
        if (!retro && fileLog.getLastWrittenPos() > deliveredPos) {
            // Missed message(s)
            goRetro(true, deliveredPos);
        }
    }

    private void goRetro(boolean ignoreFirst, long pos) {
        fileLog.removeSubHolder(this);
        retro = true;
        openFileStream(pos, ignoreFirst);
    }

    @Override
    public synchronized void close() {
        paused = true;
        fileLog.removeSubHolder(this);
        if (streamFile != null) {
            streamFile.close();
        }
    }

    synchronized boolean isRetro() {
        return retro;
    }

    boolean matches(BsonObject bsonObject) {
        return true;
    }

    synchronized void handle(long pos, BsonObject bsonObject) {
        if (paused) {
            return;
        }
        if (pos <= deliveredPos) {
            // This can happen if the stream is retro and a message is persisted, delivered from file, then
            // the stream readded then the message delivered live, so we can just ignore it
            return;
        }
        handle0(pos, bsonObject);
    }

    private void resetParser() {
        parser = RecordParser.newFixed(4, this::handleRec);
    }

    private void handle0(long pos, BsonObject bsonObject) {
        if (handler != null) {
            handler.accept(pos, bsonObject);
            deliveredPos = pos;

        } else {
            throw new IllegalStateException("No handler");
        }
    }

    private void openFileStream(long pos, boolean ignoreFirst) {
        this.ignoreFirst = ignoreFirst;
        this.fileStreamPos = pos;
        // Open a file
        FileLog.FileCoord coord = fileLog.getCoord(pos);
        fileLog.openFile(coord.fileNumber).handle((bf, t) -> {
            if (t == null) {
                streamFile = bf;
                fileNumber = coord.fileNumber;
                fileReadPos = coord.filePos;
                scheduleRead();
            } else {
                handleException(t);
            }
            return null;
        });
    }

    private void handleRec(Buffer buff) {
        if (recordSize == -1) {
            int intle = buff.getIntLE(0);
            if (intle == 0) {
                // Padding at end of file
            } else {
                recordSize = intle - 4;
                parser.fixedSizeMode(recordSize);
            }
        } else {
            if (recordSize != 0) {
                handleFrame(buff);
                parser.fixedSizeMode(4);
                recordSize = -1;
            }
        }
    }

    private synchronized void handleFrame(Buffer buffer) {
        // TODO bit clunky - need to add size back in so it can be decoded, improve this!
        int bl = buffer.length() + 4;
        Buffer buff2 = Buffer.buffer(bl);
        buff2.appendIntLE(bl).appendBuffer(buffer);
        BsonObject bson = new BsonObject(buff2);
        if (ignoreFirst) {
            ignoreFirst = false;
        } else {
            long lwep = fileLog.getLastWrittenEndPos();
            if (fileStreamPos <= lwep) {
                if (paused) {
                    buffered.add(new BufferedRecord(fileStreamPos, bson));
                } else {
                    handle0(fileStreamPos, bson);
                }
            }
            if (fileStreamPos == lwep) {
                // Need to lock to prevent messages sneaking in before we readd the stream
                synchronized (fileLog) {
                    lwep = fileLog.getLastWrittenEndPos();
                    if (fileStreamPos == lwep) {
                        // We've got to the head
                        retro = false;
                        streamFile.close();
                        streamFile = null;
                        resetParser();
                        fileLog.readdSubHolder(this);
                    }
                }
            }
        }
        fileStreamPos += bl;
    }

    private void handleException(Throwable t) {
        if (exceptionHandler != null) {
            exceptionHandler.accept(t);
        } else {
            log.error("Failed to read", t);
        }
    }

    private void doRead() {
        try {
            if (streamFile != null) {
                // Could have been set to null if previous read gets the head
                Buffer readBuff = Buffer.buffer(readBufferSize);
                CompletableFuture<Void> cf = streamFile.read(readBuff, readBufferSize, fileReadPos);
                cf.handle((v, t) -> {
                    if (t == null) {
                        boolean endOfFile = readBuff.length() < readBufferSize;
                        if (readBuff.length() > 0) {
                            parser.handle(readBuff);
                        }
                        fileReadPos += readBuff.length();
                        if (streamFile != null && endOfFile && fileReadPos == fileSize) {
                            // We read a whole file
                            moveToNextFile();
                        } else if (!paused) {
                            scheduleRead();
                        }
                    } else {
                        handleException(t);
                    }
                    return null;
                });
            }
        } catch (RejectedExecutionException e) {
            // Can happen if pool is being shutdown
            log.warn("Read rejected as pool being shutdown", e);
        }
    }

    private void moveToNextFile() {
        streamFile.close();
        streamFile = null;
        resetParser();
        int headFileNumber = fileLog.getFileNumber();
        if (headFileNumber < fileNumber) {
            log.warn("Invalid file number {} head {}", fileNumber, headFileNumber);
            return;
        }
        openFileStream((fileNumber + 1) * fileSize, false);
    }

    private void scheduleRead() {
        fileLog.scheduleOp(this::doRead);
    }

    private static final class BufferedRecord {
        final long pos;
        final BsonObject bson;

        BufferedRecord(long pos, BsonObject bson) {
            this.pos = pos;
            this.bson = bson;
        }
    }

}

