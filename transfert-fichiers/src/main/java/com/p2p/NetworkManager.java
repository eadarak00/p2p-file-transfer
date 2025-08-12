package com.p2p;

import java.io.*;
import java.net.*;

public class NetworkManager {

    private int port;

    public NetworkManager(int port) {
        this.port = port;
    }

    // Serveur TCP simple
    public void demarrerServeur() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("Serveur TCP démarré sur le port " + port);
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connecté : " + clientSocket.getInetAddress());

                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String message = in.readLine();
                System.out.println("Message reçu du client : " + message);

                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    // Client TCP simple
    public void envoyerMessage(String adresseServeur, String message) {
        try (Socket socket = new Socket(adresseServeur, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println(message);
            System.out.println("Message envoyé au serveur : " + message);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
