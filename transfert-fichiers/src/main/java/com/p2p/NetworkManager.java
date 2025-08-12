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
        return fichiers.stream()
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
    }

    // Serveur TCP qui répond à plusieurs commandes (LIST, GET <nom_fichier>)
    public void demarrerServeur() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("Serveur démarré sur le port " + port);

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client connecté : " + clientSocket.getInetAddress());

                    // On gère chaque client dans un thread séparé pour pouvoir traiter plusieurs clients
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
            InputStream socketIn = clientSocket.getInputStream();
            OutputStream socketOut = clientSocket.getOutputStream();
        ) {
            String commande = in.readLine();
            System.out.println("Commande reçue : " + commande);

            if ("LIST".equalsIgnoreCase(commande)) {
                List<Metadata> metadonnees = getListeMetadonnees();
                String json = gson.toJson(metadonnees);
                out.println(json);
                System.out.println("Liste envoyée au client.");

            } else if (commande != null && commande.startsWith("GET ")) {
                String nomFichier = commande.substring(4).trim();
                File fichier = new File(fileManager.getDossierPartage(), nomFichier);

                if (fichier.exists() && fichier.isFile()) {
                    // Calcul du checksum
                    String checksum = fileManager.calculerChecksum(fichier);
                    // On envoie d'abord le checksum (ligne texte)
                    out.println(checksum);
                    out.flush();

                    // Puis on envoie le fichier en binaire
                    try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fichier));
                         BufferedOutputStream bos = new BufferedOutputStream(socketOut)) {

                        byte[] buffer = new byte[4096];
                        int read;
                        while ((read = bis.read(buffer)) != -1) {
                            bos.write(buffer, 0, read);
                        }
                        bos.flush();
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

    // Client TCP : envoie une commande et récupère la réponse (texte)
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

    // Client TCP : télécharge un fichier complet depuis le serveur via GET <nom_fichier>
    public boolean telechargerFichier(String adresseServeur, String nomFichier, String cheminDestination) {
        try (Socket socket = new Socket(adresseServeur, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             InputStream socketIn = socket.getInputStream()) {

            // Envoyer la commande GET
            out.println("GET " + nomFichier);

            // Lire le checksum envoyé par le serveur
            String checksumServeur = in.readLine();
            if (checksumServeur == null || checksumServeur.startsWith("ERREUR")) {
                System.err.println("Erreur serveur : " + checksumServeur);
                return false;
            }

            // Recevoir le fichier en binaire et l'écrire sur disque
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(cheminDestination))) {
                byte[] buffer = new byte[4096];
                int read;
                // Attention, lire directement depuis socketIn (flux brut)
                while ((read = socketIn.read(buffer)) != -1) {
                    bos.write(buffer, 0, read);
                }
                bos.flush();
            }

            // Calculer checksum local et comparer
            String checksumLocal = fileManager.calculerChecksum(new File(cheminDestination));
            if (checksumLocal.equalsIgnoreCase(checksumServeur)) {
                System.out.println("Transfert OK : checksum validé.");
                return true;
            } else {
                System.err.println("Erreur transfert : checksum différent !");
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
