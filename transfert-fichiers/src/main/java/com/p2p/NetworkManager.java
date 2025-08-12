package com.p2p;

import java.io.*;
import java.net.*;
import java.util.List;
import com.google.gson.Gson;

public class NetworkManager {
    private int port;
    private Gson gson = new Gson();
    private FileManager fileManager;

    public NetworkManager(int port, String dossierPartage) {
        this.port = port;
        if (dossierPartage != null) {
            this.fileManager = new FileManager(dossierPartage);
        }
    }

    // Récupère la liste des métadonnées via FileManager
    private List<Metadata> getListeMetadonnees() throws Exception {
        List<File> fichiers = fileManager.listerFichiers();
        // Calculer les Metadata à partir des fichiers
        return fichiers.stream().filter(File::isFile).map(f -> {
            try {
                String checksum = fileManager.calculerChecksum(f);
                return new Metadata(f.getName(), f.length(), checksum);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }).filter(m -> m != null).toList();
    }

    // Serveur TCP qui répond à la commande LIST
    public void demarrerServeur() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("Serveur démarré sur le port " + port);
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connecté : " + clientSocket.getInetAddress());

                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                String commande = in.readLine();
                System.out.println("Commande reçue : " + commande);

                if ("LIST".equalsIgnoreCase(commande)) {
                    List<Metadata> metadonnees = getListeMetadonnees();
                    String json = gson.toJson(metadonnees);
                    out.println(json);
                    System.out.println("Liste envoyée au client.");
                } else {
                    out.println("Commande inconnue");
                }

                clientSocket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    // Client TCP : envoie une commande et récupère la réponse
    public String envoyerCommande(String adresseServeur, String commande) {
        try (Socket socket = new Socket(adresseServeur, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println(commande);
            String reponse = in.readLine();
            System.out.println("Réponse du serveur : " + reponse);
            return reponse;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
