package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable{

    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());

    public boolean keepRunning = true;
    private int updateInterval;
    private File file;
    private long filePointer= 0; // 파일 현재 위치
    private EventHandler eventHandler;

    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {

        try {
            while (this.keepRunning) {
                // 시작하자 마자 잠시 중지
                Thread.sleep(this.updateInterval);
                // file 크기 계산
                long len = this.file.length();

                if (len < this.filePointer) {
                    logger.info("file was reset");
                    filePointer = len;
                } else if (len > this.filePointer) {
                    readAppendAndSend();    // ✅ 메서드 호출
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r");
        raf.seek(this.filePointer);
        String line = null;
        while ( (line = raf.readLine()) != null) {
            sendMessage(line);
        }
        // file이 변경되었으므로 file의 filePointer를 현재 file의 마지막으로 재설정 해야함.
        this.filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String[] tokens = line.split(","); // ","을 기준으로 split
        String key = tokens[0]; // 키 추출 (P001)
        StringBuffer value = new StringBuffer();

        for (int i=1; i<tokens.length; i++) {
            if (i != (tokens.length - 1)) { // 마지막에는 ","을 안붙이기 위한 if문 처리
                value.append(tokens[i] + ",");
            } else {
                value.append(tokens[i]);
            }
        }
        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        this.eventHandler.onMessage(messageEvent);
    }

}
