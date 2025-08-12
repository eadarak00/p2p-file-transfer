package com.p2p.entities;

import java.io.*;
import java.net.Socket;

import com.p2p.FileManager;

public class Client {
    private String adresseServeur;
    private int port;
    private FileManager fileManager;
    private String dossierUpload;

    public Client(String adresseServeur, int port, String dossierLocal) {
        this.adresseServeur = adresseServeur;
        this.port = port;
        this.fileManager = new FileManager(dossierLocal);

        // Créer le dossier upload dans dossierLocal si il n'existe pas
        this.dossierUpload = dossierLocal + File.separator + "upload";
        File uploadDir = new File(dossierUpload);
        if (!uploadDir.exists()) {
            if (uploadDir.mkdirs()) {
                System.out.println("Dossier upload créé : " + dossierUpload);
            } else {
                System.err.println("Erreur lors de la création du dossier upload : " + dossierUpload);
            }
        }
    }

    public String envoyerCommande(String commande) {
        try (Socket socket = new Socket(adresseServeur, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println(commande);
            return in.readLine();

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Télécharge un fichier depuis le serveur et l’enregistre dans le dossier upload local.
     * @param nomFichier nom du fichier à télécharger
     * @return true si le transfert et la validation du checksum réussissent, false sinon
     */
    public boolean telechargerFichier(String nomFichier) {
        String cheminDestination = dossierUpload + File.separator + nomFichier;

        try (Socket socket = new Socket(adresseServeur, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             InputStream socketIn = socket.getInputStream()) {

            out.println("GET " + nomFichier);

            String checksumServeur = in.readLine();
            if (checksumServeur == null || checksumServeur.startsWith("ERREUR")) {
                System.err.println("Erreur serveur : " + checksumServeur);
                return false;
            }

            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(cheminDestination))) {
                byte[] buffer = new byte[4096];
                int read;
                while ((read = socketIn.read(buffer)) != -1) {
                    bos.write(buffer, 0, read);
                }
                bos.flush();
            }

            String checksumLocal = fileManager.calculerChecksum(new File(cheminDestination));
            if (checksumLocal.equalsIgnoreCase(checksumServeur)) {
                System.out.println("Transfert réussi, checksum validé. Fichier stocké dans : " + cheminDestination);
                return true;
            } else {
                System.err.println("Erreur transfert, checksum différent !");
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
