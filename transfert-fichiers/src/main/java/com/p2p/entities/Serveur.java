package com.p2p.entities;


import java.io.*;
import java.net.*;
import java.util.List;

import com.p2p.FileManager;
import com.p2p.Metadata;

public class Serveur {
    private int port;
    private FileManager fileManager;

    public Serveur(int port, String dossierPartage) {
        this.port = port;
        this.fileManager = new FileManager(dossierPartage);
    }

    public void demarrer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("Serveur démarré sur le port " + port);

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client connecté : " + clientSocket.getInetAddress());

                    new Thread(() -> gererClient(clientSocket)).start();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void gererClient(Socket clientSocket) {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            OutputStream socketOut = clientSocket.getOutputStream();
        ) {
            String commande = in.readLine();
            System.out.println("Commande reçue : " + commande);

            if ("LIST".equalsIgnoreCase(commande)) {
                List<File> fichiers = fileManager.listerFichiers();
                List<Metadata> metadonnees = fichiers.stream()
                        .filter(File::isFile)
                        .map(f -> {
                            try {
                                String checksum = fileManager.calculerChecksum(f);
                                return new Metadata(f.getName(), f.length(), checksum);
                            } catch (Exception e) {
                                e.printStackTrace();
                                return null;
                            }
                        })
                        .filter(m -> m != null)
                        .toList();

                String json = new com.google.gson.Gson().toJson(metadonnees);
                out.println(json);
                System.out.println("Liste envoyée au client.");

            } else if (commande != null && commande.startsWith("GET ")) {
                String nomFichier = commande.substring(4).trim();
                File fichier = new File(fileManager.getDossierPartage(), nomFichier);

                if (fichier.exists() && fichier.isFile()) {
                    String checksum = fileManager.calculerChecksum(fichier);
                    out.println(checksum);  // envoyer checksum en texte
                    out.flush();

                    try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fichier))) {
                        byte[] buffer = new byte[4096];
                        int read;
                        while ((read = bis.read(buffer)) != -1) {
                            socketOut.write(buffer, 0, read);
                        }
                        socketOut.flush();
                    }
                    System.out.println("Fichier '" + nomFichier + "' envoyé au client.");
                } else {
                    out.println("ERREUR: fichier introuvable");
                    System.out.println("Fichier '" + nomFichier + "' introuvable.");
                }

            } else {
                out.println("Commande inconnue");
                System.out.println("Commande inconnue reçue : " + commande);
            }

            clientSocket.close();
            System.out.println("Connexion client fermée.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
