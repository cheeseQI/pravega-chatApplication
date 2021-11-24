package com.dellemc.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

/**
 * This implements a chat application based on Pravega Streams.
 * One stream are used to record chatting text, and the other stream is used to do file transmission.
 * Thus there are two writers, two readers, two readGroups for this class.
 * The conversation channel can be built between double or multiple number of people.
 * check comment in Client.java to see how to run the chat application.
 */
public class ChatApplication {
    private URI uri;
    private String stream;
    private String selfName, channelName, fileChannelGroup;
    private boolean endChat;
    private boolean fileMode;
    private EventStreamWriter<byte[]> writer, fileWriter;
    private EventStreamReader<byte[]> reader, fileReader;
    private ReaderGroupManager readerGroupManager, fileReaderGroupManager;

    public ChatApplication(String selfNameIn, String channelNameIn) throws Exception {
        String url = "tcp://127.0.0.1:9090";
        uri = new URI(url);
        String scope = "dell";
        selfName = selfNameIn;
        channelName = selfNameIn + "-" + channelNameIn;
        String fileStream = channelNameIn + "-" + "fileStream";
        fileChannelGroup = channelName + "-" + "fileGroup";
        endChat = false;
        fileMode = false;
        stream = channelNameIn;
        createStream(scope, stream);
        createStream(scope, fileStream);
        writer = getWriter(scope, stream);
        fileWriter = getWriter(scope, fileStream);
        readerGroupManager = ReaderGroupManager.withScope(scope, uri);
        fileReaderGroupManager = ReaderGroupManager.withScope(scope, uri);
        createReaderGroup(scope, stream, channelName, readerGroupManager);
        createReaderGroup(scope, fileStream, fileChannelGroup, fileReaderGroupManager);
        reader = createReader(scope, stream, channelName);
        fileReader = createReader(scope, fileStream, fileChannelGroup);
    }

    //create stream
    private void createStream(String scope, String stream) {
        StreamManager streamManager = StreamManager.create(uri);
        streamManager.createScope(scope);
        StreamConfiguration build = StreamConfiguration.builder().build();
        streamManager.createStream(scope, stream, build);
        streamManager.close();
    }

    //create writer
    private EventStreamWriter<byte[]> getWriter(String scope, String stream) {
        ClientConfig build = ClientConfig.builder().controllerURI(uri).build();
        EventStreamClientFactory streamClientFactory = EventStreamClientFactory.withScope(scope, build);
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        return streamClientFactory.createEventWriter(stream, new JavaSerializer<>(), writerConfig);
    }

    //write data into stream
    private void writeData(EventStreamWriter<byte[]> writer, String message) throws Exception {
        CompletableFuture<Void> future = writer.writeEvent(message.getBytes()); //string to bytes
        future.get();
    }

    //create readerGroup
    private void createReaderGroup(String scope, String stream, String groupName, ReaderGroupManager readerGroupManager) {
        ReaderGroupConfig build = ReaderGroupConfig.builder().stream(scope + "/" + stream).build();
        readerGroupManager.createReaderGroup(groupName, build);
    }

    //create reader
    private EventStreamReader<byte[]> createReader(String scope, String readerId, String groupName) {
        ClientConfig build = ClientConfig.builder().controllerURI(uri).build();
        EventStreamClientFactory streamClientFactory = EventStreamClientFactory.withScope(scope, build);
        ReaderConfig build1 = ReaderConfig.builder().build();
        return streamClientFactory.createReader(readerId, groupName, new JavaSerializer<>(), build1);
    }

    /**
     * Upload the file according to given path into the stream
     * Translate all file content into byte code and write into the stream
     *
     * @param pathName is the fileName with its path
     */
    private void uploadFile(String pathName) throws Exception {
        File file = new File(pathName);
        byte[] byteData;
        byteData = Files.readAllBytes(file.toPath());
        fileWriter.writeEvent(byteData);
        System.out.println("upload success!");
    }

    /**
     * Download the file by getting byte code and writing them into file located at E:\ans.txt
     *
     * @param fileEvent is the event of the stream containing file's byte code
     */
    private void downloadFile(EventRead<byte[]> fileEvent) throws Exception {
        File file = new File("E:\\ans.txt");
        if (!file.exists()) file.createNewFile();
        OutputStream outData = new FileOutputStream(file);
        outData.write(fileEvent.getEvent());
        outData.close();
    }

    /**
     * Send the text/file message to the stream with the help of writer.
     * implement runnable to achieve multiple threads, make sure it run until chat is end.
     * if the client write normal text message, it will be written into the chatting stream.
     * if the client write upload@ message, it will call the upload function and write file into file stream
     * and set fileMode as true, for the reader to read file stream instead of chatting stream.
     * "bye" is used as stop signal, to indicate that this chat should be end
     */
    class MessageSender implements Runnable {
        @Override
        public void run() {
            try {
                Scanner scanner = new Scanner(System.in);
                String msg;
                System.out.println("Welcome to " + stream + "!");
                System.out.println("User " + selfName + " is registered!");
                while (!endChat) {
                    msg = selfName + ": " + scanner.nextLine();
                    writeData(writer, msg);
                    if (msg.equals(selfName + ": bye")) {
                        System.out.println("User " + selfName + " is logged out!");
                        endChat = true;
                        writer.close();                                     //close writers
                        fileWriter.close();
                    } else if (msg.startsWith(selfName + ": upload@")) {
                        fileMode = true;
                        uploadFile(msg.substring(msg.indexOf('@') + 1));    //find pathName
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Receive the text/file message from the stream with the help of reader.
     * if fileMode is true read fileStream and call downloadFile to write byte into local directory.
     * else read the chatting stream and print it.
     */
    class MessageReceiver implements Runnable {
        @Override
        public void run() {
            try {
                while (!endChat) {
                    EventRead<byte[]> event = reader.readNextEvent(100);
                    EventRead<byte[]> fileEvent = fileReader.readNextEvent(100);
                    if (fileMode) {
                        downloadFile(fileEvent);
                        fileMode = false;
                    }
                    if (event.getEvent() != null) {
                        System.out.println();
                        System.out.println(byteToString(event.getEvent()));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            closeReader();
        }
    }

    /**
     * start the two thread of ChatApplication
     * one is used to send message, the other one is used to receive message
     */
    public void startChat() {
        MessageSender clientSender = new MessageSender();
        Thread clientSenderThread = new Thread(clientSender);
        clientSenderThread.start();
        MessageReceiver clientReceiver = new MessageReceiver();
        Thread clientReceiverThread = new Thread(clientReceiver);
        clientReceiverThread.start();
    }

    //convert byte to String
    private String byteToString(byte[] bytes) {
        return new String(bytes);
    }

    //close readers and readGroups
    private void closeReader() {
        reader.close();
        fileReader.close();
        readerGroupManager.deleteReaderGroup(channelName);
        readerGroupManager.deleteReaderGroup(fileChannelGroup);
        readerGroupManager.close();
        fileReaderGroupManager.close();
    }
}

