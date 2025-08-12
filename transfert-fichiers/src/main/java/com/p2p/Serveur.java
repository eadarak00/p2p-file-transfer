package com.p2p;

import java.io.*;
import java.net.*;

public class Serveur {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(5000);
        System.out.println("Serveur démarré sur le port 5000...");
        Socket socket = serverSocket.accept();
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        System.out.println("Message reçu : " + in.readLine());
        socket.close();
        serverSocket.close();
    }
}
