package com.p2p.entities;

import java.io.*;
import java.net.Socket;

import com.p2p.manager.FileManager;

public class Client2 {
    private static Client2 instance = null;

    private final String pseudo;
    private final String adresse;
    private final int port;
    private final File dossierPerso;
    private final FileManager fileManager;

    private Client2(String pseudo, String adresse, int port, String dossierBase) {
        this.pseudo = pseudo;
        this.adresse = adresse;
        this.port = port;
        this.dossierPerso = new File(dossierBase, pseudo);
        if (!dossierPerso.exists()) {
            dossierPerso.mkdirs();
        }
        this.fileManager = new FileManager(dossierPerso.getPath());
    }

    public static synchronized Client2 getInstance(String pseudo, String adresse, int port, String dossierBase) {
        if (instance == null) {
            instance = new Client2(pseudo, adresse, port, dossierBase);
        }
        return instance;
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

    public boolean telechargerFichier(String nomFichier) {
        try (Socket socket = new Socket(adresse, port);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                InputStream socketIn = socket.getInputStream()) {

            out.println("GET " + pseudo + " " + nomFichier);

            String checksumServeur = in.readLine();
            if (checksumServeur == null || checksumServeur.startsWith("ERREUR"))
                return false;

            File fichierLocal = new File(dossierPerso, nomFichier);
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fichierLocal))) {
                byte[] buffer = new byte[4096];
                int read;
                while ((read = socketIn.read(buffer)) != -1) {
                    bos.write(buffer, 0, read);
                }
            }

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
