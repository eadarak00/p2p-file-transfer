package com.p2p;

import java.io.*;
import java.net.*;
import java.util.*;
import com.google.gson.Gson;

public class NetworkManager {
    private int port;
    private String dossierPartage;
    private Gson gson = new Gson();

    public NetworkManager(int port, String dossierPartage) {
        this.port = port;
        this.dossierPartage = dossierPartage;
    }

    // Liste les métadonnées des fichiers dans dossierPartage
    private List<Metadata> getListeMetadonnees() throws Exception {
        File dossier = new File(dossierPartage);
        File[] fichiers = dossier.listFiles();
        List<Metadata> liste = new ArrayList<>();

        if (fichiers != null) {
            for (File f : fichiers) {
                if (f.isFile()) {
                    String checksum = calculerChecksum(f);
                    liste.add(new Metadata(f.getName(), f.length(), checksum));
                }
            }
        }
        return liste;
    }

    // Calcul SHA-256
    private String calculerChecksum(File fichier) throws Exception {
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        byte[] bytes = java.nio.file.Files.readAllBytes(fichier.toPath());
        byte[] hash = digest.digest(bytes);
        StringBuilder hex = new StringBuilder();
        for (byte b : hash) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
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
