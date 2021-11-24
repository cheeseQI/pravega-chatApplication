package com.dellemc.pravega;

/**
 * This is the Client program used to read user input from command line.
 * It read two arguments : self name of the user, channel name of the chat room.
 * Example : Siqi chatRoom1
 * Multi-Client: Siqi chatRoom1; Xu chatRoom1; Li chatRoom1; Zhang chatRoom2; Zhao chatRoom2...
 * Upload file format:upload@D:\myTest\test.txt
 * <p>
 * Base function:
 * one to one chat, group chat, history record, file transmission.
 * <p>
 * Additional function:
 * different chat rooms channels are separated, chatRoom1, chatRoom2....
 * they can record their own chat history, running correctly at the same time.
 * if you want to chat with a person/ group of people. please enter same channelName!
 */

public class Client {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            System.out.println("Argument Usage: self name and peer name");
            System.exit(1);
        }
        String selfName = args[0].trim();
        String channelName = args[1].trim();
        System.out.println("Program start!");
        ChatApplication chat = new ChatApplication(selfName, channelName);
        chat.startChat();
    }
}
