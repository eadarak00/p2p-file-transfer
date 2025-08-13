package com.p2p.entities;

import com.p2p.manager.FileManager;
import com.p2p.manager.Metadata;
import com.google.gson.Gson;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Serveur {

    private final int port;
    private final File dossierPartage; // ./uploads/public
    private final File dossierClients; // ./uploads/clients
    private final Object fileLock = new Object();

    public Serveur(int port, String cheminDossierBase) {
        this.port = port;

        // dossierPartage = ./uploads/public
        this.dossierPartage = new File(cheminDossierBase, "public");
        if (!dossierPartage.exists()) {
            dossierPartage.mkdirs();
        }

        // dossierClients = ./uploads/clients
        this.dossierClients = new File(cheminDossierBase, "clients");
        if (!dossierClients.exists()) {
            dossierClients.mkdirs();
        }
    }

    public void demarrer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("Serveur démarré sur le port " + port);

                while (true) {
                    Socket socket = serverSocket.accept();
                    System.out.println("Connexion client acceptée : " + socket.getInetAddress());
                    new Thread(() -> gererClient(socket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void gererClient(Socket socket) {
        try (
                InputStream socketIn = socket.getInputStream();
                OutputStream socketOut = socket.getOutputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(socketIn));
                PrintWriter out = new PrintWriter(socketOut, true)) {
            String commande = in.readLine();
            if (commande == null)
                return;

            System.out.println("Commande reçue : " + commande);
            String[] parts = commande.split(" ", 3);

            switch (parts[0].toUpperCase()) {
                case "LIST":
                    if (parts.length == 2 && !parts[1].isEmpty()) {
                        File dossierClient = new File(dossierClients, sanitize(parts[1]));
                        if (!dossierClient.exists())
                            dossierClient.mkdirs();
                        envoyerListeFichiers(dossierClient, out);
                    } else {
                        envoyerListeFichiers(dossierPartage, out);
                    }
                    break;

                // case "GET":
                //     if (parts.length == 2) {
                //         String nomFichier = parts[1];
                //         envoyerFichier(dossierPartage, nomFichier, socketOut, out);
                //     } else {
                //         out.println("ERREUR: commande GET invalide");
                //     }
                //     break;
                case "GET":
                    if (parts.length >= 2) {
                        String nomFichier = parts[1];
                        long offset = 0;
                        if (parts.length == 3) {
                            try {
                                offset = Long.parseLong(parts[2]);
                            } catch (NumberFormatException e) {
                                offset = 0;
                            }
                        }
                        envoyerFichier(dossierPartage, nomFichier, socketOut, out, offset);
                    } else {
                        out.println("ERREUR: commande GET invalide");
                    }
                    break;


                case "UPLOAD":
                    if (parts.length == 3) {
                        String pseudo = sanitize(parts[1]);
                        String nomFichier = parts[2];
                        recevoirFichier(pseudo, nomFichier, socketIn, out);
                    } else {
                        out.println("ERREUR: commande UPLOAD invalide");
                    }
                    break;

                default:
                    out.println("ERREUR: commande inconnue");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void envoyerListeFichiers(File dossier, PrintWriter out) {
        synchronized (fileLock) {
            FileManager fm = new FileManager(dossier.getPath());
            List<File> fichiers = fm.listerFichiers();
            List<Metadata> meta = fichiers.stream()
                    .filter(File::isFile)
                    .map(f -> {
                        try {
                            return new Metadata(f.getName(), f.length(), fm.calculerChecksum(f));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    })
                    .filter(m -> m != null)
                    .toList();

            String json = new Gson().toJson(meta);
            out.println(json);
            System.out.println("Liste envoyée (" + meta.size() + " fichiers).");
        }
    }

    private void envoyerFichier(File dossierSource, String nomFichier, OutputStream socketOut, PrintWriter out, long offset) {
    synchronized (fileLock) {
        try {
            File fichier = new File(dossierSource, nomFichier);
            if (!fichier.exists() || !fichier.isFile()) {
                out.println("ERREUR: fichier introuvable");
                System.out.println("Fichier introuvable : " + fichier.getAbsolutePath());
                return;
            }

            FileManager fm = new FileManager(dossierSource.getPath());
            String checksum = fm.calculerChecksum(fichier);
            long taille = fichier.length();

            out.println(checksum);
            out.println(taille);
            out.flush();

            try (RandomAccessFile raf = new RandomAccessFile(fichier, "r")) {
                if (offset > 0) {
                    raf.seek(offset);
                }
                byte[] buffer = new byte[4096];
                long reste = taille - offset;
                int read;
                while (reste > 0 && (read = raf.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
                    socketOut.write(buffer, 0, read);
                    reste -= read;
                }
            }
            socketOut.flush();
            System.out.println("Fichier envoyé : " + fichier.getAbsolutePath() + " depuis offset " + offset);

        } catch (Exception e) {
            e.printStackTrace();
            out.println("ERREUR lors de l'envoi du fichier");
            out.flush();
        }
    }
}


    private void recevoirFichier(String pseudo, String nomFichier, InputStream socketIn, PrintWriter out) {
        synchronized (fileLock) {
            try {
                File dossierClient = new File(dossierClients, pseudo);
                if (!dossierClient.exists())
                    dossierClient.mkdirs();

                File fichier = new File(dossierClient, nomFichier);
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fichier))) {
                    byte[] buffer = new byte[4096];
                    int read;
                    while ((read = socketIn.read(buffer)) != -1) {
                        bos.write(buffer, 0, read);
                    }
                }
                out.println("UPLOAD OK");
                System.out.println("Fichier reçu et stocké : " + fichier.getAbsolutePath());

            } catch (IOException e) {
                e.printStackTrace();
                out.println("ERREUR upload");
            }
        }
    }

    private String sanitize(String input) {
        return input.replaceAll("[^a-zA-Z0-9_-]", "_");
    }
}
