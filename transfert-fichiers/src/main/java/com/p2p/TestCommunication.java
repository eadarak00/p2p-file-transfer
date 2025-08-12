package com.p2p;

public class TestCommunication {

    public static void main(String[] args) throws InterruptedException {
        int port = 5000;
        NetworkManager networkManager = new NetworkManager(port);

        // Démarrer le serveur TCP
        networkManager.demarrerServeur();

        // Pause pour que le serveur soit prêt
        Thread.sleep(1000);

        // Envoyer un message client vers serveur
        networkManager.envoyerMessage("localhost", "Hello Peer");
    }
}
