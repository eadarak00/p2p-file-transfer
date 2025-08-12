package com.p2p.entities;

import com.p2p.FileManager;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Client {
    private static final Map<String, Client> instances = new ConcurrentHashMap<>();

    private final String pseudo;
    private final String adresse;
    private final int port;
    private final File dossierPerso;
    private final FileManager fileManager;

    private Client(String pseudo, String adresse, int port, String dossierBase) {
        this.pseudo = pseudo;
        this.adresse = adresse;
        this.port = port;
        // dossierBase = "./uploads/clients"
        this.dossierPerso = new File(dossierBase, pseudo);
        if (!dossierPerso.exists()) {
            dossierPerso.mkdirs();
        }
        this.fileManager = new FileManager(dossierPerso.getPath());
    }

    public static Client getInstance(String pseudo, String adresse, int port, String dossierBase) {
        return instances.computeIfAbsent(pseudo, p -> new Client(p, adresse, port, dossierBase));
    }

    public String envoyerCommande(String commande) {
        try (Socket socket = new Socket(adresse, port);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(commande);
            return in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // public boolean telechargerFichier(String nomFichier) {
    // try (Socket socket = new Socket(adresse, port);
    // BufferedReader in = new BufferedReader(new
    // InputStreamReader(socket.getInputStream()));
    // PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    // InputStream socketIn = socket.getInputStream()) {

    // // Envoi la commande GET
    // out.println("GET " + nomFichier);

    // String checksumServeur = in.readLine();
    // if (checksumServeur == null || checksumServeur.startsWith("ERREUR"))
    // return false;

    // String tailleStr = in.readLine();
    // if (tailleStr == null)
    // return false;

    // long tailleFichier = Long.parseLong(tailleStr);

    // File fichierLocal = new File(dossierPerso, nomFichier);
    // try (BufferedOutputStream bos = new BufferedOutputStream(new
    // FileOutputStream(fichierLocal))) {
    // byte[] buffer = new byte[4096];
    // long reste = tailleFichier;
    // while (reste > 0) {
    // int toRead = (int) Math.min(buffer.length, reste);
    // int lu = socketIn.read(buffer, 0, toRead);
    // if (lu == -1)
    // break;
    // bos.write(buffer, 0, lu);
    // reste -= lu;
    // }
    // }

    // // Vérification checksum
    // try {
    // String checksumLocal = fileManager.calculerChecksum(fichierLocal);
    // return checksumLocal.equals(checksumServeur);
    // } catch (Exception ex) {
    // ex.printStackTrace();
    // return false;
    // }

    // } catch (IOException e) {
    // e.printStackTrace();
    // return false;
    // }
    // }

    public boolean telechargerFichier(String nomFichier) {
        File fichierLocal = new File(dossierPerso, nomFichier);
        long tailleLocale = fichierLocal.exists() ? fichierLocal.length() : 0;

        try (Socket socket = new Socket(adresse, port);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("GET " + nomFichier + " " + tailleLocale);

            // Lecture checksum et taille avec BufferedReader
            String checksumServeur = in.readLine();
            if (checksumServeur == null || checksumServeur.startsWith("ERREUR"))
                return false;

            String tailleStr = in.readLine();
            if (tailleStr == null)
                return false;

            long tailleFichierServeur = Long.parseLong(tailleStr);

            if (tailleLocale > tailleFichierServeur) {
                tailleLocale = 0;
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fichierLocal))) {
                    // Efface fichier
                }
            }

            // IMPORTANT: passer directement au flux binaire de la socket (pas via
            // BufferedReader)
            InputStream socketIn = socket.getInputStream();

            // Déplacer le pointeur du fichier selon tailleLocale (mode append)
            try (RandomAccessFile raf = new RandomAccessFile(fichierLocal, "rw")) {
                raf.seek(tailleLocale);
                byte[] buffer = new byte[4096];
                long reste = tailleFichierServeur - tailleLocale;
                while (reste > 0) {
                    int toRead = (int) Math.min(buffer.length, reste);
                    int lu = socketIn.read(buffer, 0, toRead);
                    if (lu == -1)
                        break;
                    raf.write(buffer, 0, lu);
                    reste -= lu;
                }
            }
            // Vérification checksum
            try {
                String checksumLocal = fileManager.calculerChecksum(fichierLocal);
                return checksumLocal.equals(checksumServeur);
            } catch (Exception ex) {
                ex.printStackTrace();
                return false;
            }

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public String getPseudo() {
        return pseudo;
    }
}
