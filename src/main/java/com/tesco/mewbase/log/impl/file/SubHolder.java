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

import java.util.concurrent.CompletableFuture;
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
public class SubHolder implements ReadStream {

    private final static Logger log = LoggerFactory.getLogger(SubHolder.class);

    private final FileLog fileLog;
    private final SubDescriptor subDescriptor;
    private final Context context;
    private final int readBufferSize;

    private BiConsumer<Long, BsonObject> handler;
    private Consumer<Throwable> exceptionHandler;

    private boolean paused;
    private long channelPos;
    private long deliveredPos;
    private boolean retro;

    private long fileStreamPos;
    private boolean ignoreFirst;
    private int fileNumber;
    private int fileReadPos;
    private BasicFile streamFile;
    private int fileSize;

    private RecordParser parser;
    private int recordSize = -1;


    // TODO buffer of messages

    public SubHolder(FileLog fileLog, SubDescriptor subDescriptor, long channelPos, long startPos, int readBufferSize,
                     int fileSize) {
        this.fileLog = fileLog;
        this.subDescriptor = subDescriptor;
        this.context = Vertx.currentContext();
        this.readBufferSize = readBufferSize;
        this.fileSize = fileSize;
        this.channelPos = channelPos;
        resetParser();
        if (startPos != -1) {
            retro = true;
            openFileStream(startPos, false);
        }
    }

    private void resetParser() {
        parser = RecordParser.newFixed(4, this::handleRec);
    }

    boolean matches(BsonObject bsonObject) {
        return true;
    }

    synchronized boolean handle(long pos, BsonObject bsonObject) {
        this.channelPos = pos; // TODO Maybe get rid of this if we ask the filelog
        if (retro | paused) {
            return false;
        }
        handle0(pos, bsonObject);
        return true;
    }

    private void handle0(long pos, BsonObject bsonObject) {
        if (handler != null) {
            // TODO maybe buffer
            deliveredPos = pos;
            handler.accept(pos, bsonObject);
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

    /*
    Next: dry run through logic with single file

    start streaming from non zero position,
    all the while messages are arriving at head.
    before get to head, pause then resume.
    when gets to head verify file closes and switches to live
    now pause again
    and go into slow
    catch up again
    go fast again

    consider edge cases where:

    open basic file right at end
    empty space at end of file

    */

    @Override
    public synchronized void resume() {
        if (!paused) {
            return;
        }
        paused = false;
        if (!retro && channelPos > deliveredPos) {
            // Missed message(s)
            openFileStream(deliveredPos, true);
        }
    }

    @Override
    public void close() {
        fileLog.removeSubHolder(this);
    }

    private void openFileStream(long pos, boolean ignoreFirst) {
        this.ignoreFirst = ignoreFirst;
        this.fileStreamPos = pos;
        log.trace("Opening file for pos {}", pos);
        // Open a file
        FileLog.FileCoord coord = fileLog.getCoord(pos);
        fileLog.openFile(coord.fileNumber).handle((bf, t) -> {
            if (t == null) {
                streamFile = bf;
                fileNumber = coord.fileNumber;
                fileReadPos = coord.filePos;
                log.trace("Opened file, file number is {} read pos is {}", fileNumber, fileReadPos);
                scheduleRead();
            } else {
                handleException(t);
            }
            return null;
        });
    }

//    private void openFileStream(long pos, boolean ignoreFirst) {
//        log.trace("Opening file for pos {}", pos);
//        // Open a file
//        FileLog.FileCoord coord = fileLog.getCoord(pos);
//        openFileStream(coord.fileNumber, coord.filePos, ignoreFirst);
//    }
//
//    private void openFileStream(int fileNumber, int filePos, boolean ignoreFirst) {
//        this.ignoreFirst = ignoreFirst;
//        this.fileStreamPos = fileSize * fileNumber + filePos;
//        log.trace("Opening file for pos {}", fileStreamPos);
//        // Open a file
//        fileLog.openFile(fileNumber).handle((bf, t) -> {
//            if (t == null) {
//                streamFile = bf;
//                this.fileNumber = fileNumber;
//                this.fileReadPos = filePos;
//                log.trace("Opened file, file number is {} read pos is {}", fileNumber, fileReadPos);
//                scheduleRead();
//            } else {
//                handleException(t);
//            }
//            return null;
//        });
//    }

    //private boolean readPadding;

    private void handleRec(Buffer buff) {
        if (recordSize == -1) {
            int intle = buff.getIntLE(0);
            if (intle == 0) {
                // Padding at end of file
                //readPadding = true;
            } else {
                recordSize = intle - 4;
                log.trace("Size is {}", recordSize);
                parser.fixedSizeMode(recordSize);
            }
        } else {
            if (recordSize != 0) {
                //log.trace("Got frame size {}", recordSize);
                handleFrame(buff);
                parser.fixedSizeMode(4);
                recordSize = -1;
            }
        }
    }

    private void handleFrame(Buffer buffer) {
        log.trace("Handling frame");
        // TODO bit clunky - need to add size back in so it can be decoded, improve this!
        // TODO what about empty space at end of file?
        int bl = buffer.length() + 4;
        Buffer buff2 = Buffer.buffer(bl);
        buff2.appendIntLE(bl).appendBuffer(buffer);
        BsonObject bson = new BsonObject(buff2);
        if (ignoreFirst) {
            ignoreFirst = false;
        } else {
            // TODO - what if it's paused? Need to buffer
            long lwp = fileLog.getLastWrittenPos();
            if (fileStreamPos <= lwp) {
                handle0(fileStreamPos, bson);
            }
            if (fileStreamPos == lwp) {
                // We've got to the head
                retro = false;
                streamFile.close();
                streamFile = null;
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
        Buffer readBuff = Buffer.buffer(readBufferSize);
        CompletableFuture<Void> cf = streamFile.read(readBuff, readBufferSize, fileReadPos);
        cf.handle((v, t) -> {
            if (t == null) {
                log.trace("Passing buffer length {} to parser", readBuff.length());
                boolean endOfFile = readBuff.length() < readBufferSize;
                if (readBuff.length() > 0) {
                    parser.handle(readBuff);
                }
                fileReadPos += readBuff.length();

                log.trace("File read pos is {}", fileReadPos);

                if (streamFile != null) {
                    // Might be null if reached head
                    if (endOfFile) {
                        moveToNextFile();
                    } else if (!paused) {
                        scheduleRead();
                    }
                }
            } else {
                handleException(t);
            }
            return null;
        });
    }

    private void moveToNextFile() {
        log.trace("Got to end of file");
        resetParser();
        int headFileNumber = fileLog.getFileNumber();
        if (headFileNumber <= fileNumber) {
            throw new IllegalStateException("Invalid file number");
        }
        openFileStream((fileNumber + 1) * fileSize, false);
    }

    private void scheduleRead() {
        fileLog.scheduleOp(this::doRead);
    }
}

