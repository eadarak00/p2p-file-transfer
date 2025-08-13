package com.p2p.entities;

import com.p2p.manager.FileManager;
import com.p2p.manager.Metadata;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Classe Peer améliorée avec synchronisation automatique et logs nettoyés
 */
public class Peer {
    private final String pseudo;
    private final int portEcoute;
    private final File dossierPartage;
    private final FileManager fileManager;
    private final Object fileLock = new Object();

    // Liste des autres peers connus
    private final List<PeerInfo> peersConnus = new CopyOnWriteArrayList<>();

    // Serveur socket pour écouter les connexions entrantes
    private ServerSocket serverSocket;
    private volatile boolean actif = false;

    // Cache des fichiers disponibles chez chaque peer
    private final Map<String, List<Metadata>> cacheFichiersPeers = new ConcurrentHashMap<>();

    // Scheduler pour les tâches périodiques
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    // Timeout pour considérer qu'un peer est inactif (30 secondes)
    private static final long PEER_TIMEOUT_MS = 3000;

    // Mode debug - mettre à false pour une interface propre
    private static final boolean DEBUG_MODE = false;

    public Peer(String pseudo, int portEcoute, String dossierPartage) {
        this.pseudo = pseudo;
        this.portEcoute = portEcoute;
        this.dossierPartage = new File(dossierPartage);

        if (!this.dossierPartage.exists()) {
            this.dossierPartage.mkdirs();
        }

        this.fileManager = new FileManager(this.dossierPartage.getPath());
    }

    /**
     * Démarre le peer avec synchronisation automatique
     */
    public void demarrer() {
        try {
            serverSocket = new ServerSocket(portEcoute);
            actif = true;

            // Thread pour écouter les connexions entrantes
            new Thread(this::ecouterConnexions).start();

            System.out.println("Peer '" + pseudo + "' démarré sur le port " + portEcoute);

            // Découverte initiale des peers (silencieuse)
            scheduler.schedule(this::decouvriPeersSilencieux, 1, TimeUnit.SECONDS);

            // Synchronisation périodique des peers (toutes les 10 secondes) - silencieuse
            scheduler.scheduleAtFixedRate(this::synchroniserPeersSilencieux, 10, 10, TimeUnit.SECONDS);

            // Nettoyage des peers inactifs (toutes les 30 secondes) - silencieux
            scheduler.scheduleAtFixedRate(this::nettoyerPeersInactifsSilencieux, 30, 30, TimeUnit.SECONDS);

            // Mise à jour du cache des fichiers (toutes les 20 secondes) - silencieuse
            scheduler.scheduleAtFixedRate(this::mettreAJourTousLesCacheSilencieux, 20, 20, TimeUnit.SECONDS);

        } catch (IOException e) {
            System.err.println("Erreur lors du démarrage du peer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Arrête le peer et le scheduler
     */
    public void arreter() {
        actif = false;
        scheduler.shutdown();
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            System.out.println("Peer '" + pseudo + "' arrêté");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Traite une requête d'un autre peer (version silencieuse)
     */
    private void traiterRequetePeer(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                InputStream socketIn = clientSocket.getInputStream();
                OutputStream socketOut = clientSocket.getOutputStream()) {

            String commande = in.readLine();
            if (commande == null)
                return;

            // Seulement afficher en mode debug
            if (DEBUG_MODE) {
                System.out.println("Requête reçue de " + clientSocket.getInetAddress() + ": " + commande);
            }

            String[] parts = commande.split(" ", 4);

            switch (parts[0].toUpperCase()) {
                case "PING":
                    // Répondre au ping avec nos infos
                    out.println("PONG " + pseudo + " " + portEcoute);
                    break;

                case "LIST":
                    envoyerListeFichiersSilencieux(out);
                    break;

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
                        envoyerFichierSilencieux(nomFichier, socketOut, out, offset);
                    } else {
                        out.println("ERREUR: commande GET invalide");
                    }
                    break;

                case "PEERS":
                    envoyerListePeersSilencieux(out);
                    break;

                case "ANNOUNCE":
                    if (parts.length >= 3) {
                        String pseudoAnnonce = parts[1];
                        int portAnnonce = Integer.parseInt(parts[2]);
                        String adresseAnnonce = clientSocket.getInetAddress().getHostAddress();

                        PeerInfo nouveauPeer = new PeerInfo(adresseAnnonce, portAnnonce, pseudoAnnonce);
                        ajouterPeerSilencieux(nouveauPeer);
                        out.println("OK PEER_ADDED");
                    } else {
                        out.println("ERREUR: commande ANNOUNCE invalide");
                    }
                    break;

                default:
                    out.println("ERREUR: commande inconnue");
            }

        } catch (IOException e) {
            if (DEBUG_MODE) {
                System.err.println("Erreur lors du traitement d'une requête: " + e.getMessage());
            }
        } finally {
            try {
                clientSocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Version silencieuse de la synchronisation des peers
     */
    private void synchroniserPeersSilencieux() {
        if (!actif)
            return;

        Set<PeerInfo> nouveauxPeers = new HashSet<>();

        // Annoncer notre présence à tous les peers connus (silencieusement)
        for (PeerInfo peer : new ArrayList<>(peersConnus)) {
            try {
                annoncerAuPeerSilencieux(peer);
                Set<PeerInfo> peersDuPeer = recupererPeersDuPeerSilencieux(peer);
                nouveauxPeers.addAll(peersDuPeer);
            } catch (Exception e) {
                // Erreurs silencieuses
            }
        }

        // Ajouter les nouveaux peers découverts (silencieusement)
        for (PeerInfo nouveauPeer : nouveauxPeers) {
            if (!nouveauPeer.getAdresse().equals("localhost") ||
                    nouveauPeer.getPort() != portEcoute) {
                ajouterPeerSilencieux(nouveauPeer);
            }
        }
    }

    /**
     * S'annonce auprès d'un peer spécifique (version silencieuse)
     */
    private void annoncerAuPeerSilencieux(PeerInfo peer) {
        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            socket.setSoTimeout(2000);
            out.println("ANNOUNCE " + pseudo + " " + portEcoute);
            String reponse = in.readLine();

            if (reponse != null && reponse.startsWith("OK")) {
                peer.updatePing();
            }

        } catch (IOException e) {
            // Échec silencieux
        }
    }

    /**
     * Récupère la liste des peers connus par un peer spécifique (version
     * silencieuse)
     */
    private Set<PeerInfo> recupererPeersDuPeerSilencieux(PeerInfo peer) {
        Set<PeerInfo> peersDistants = new HashSet<>();

        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            socket.setSoTimeout(2000);
            out.println("PEERS");
            String json = in.readLine();

            if (json != null && !json.startsWith("ERREUR")) {
                Gson gson = new Gson();
                List<PeerInfo> peers = gson.fromJson(json, new TypeToken<List<PeerInfo>>() {
                }.getType());
                if (peers != null) {
                    peersDistants.addAll(peers);
                }
            }

        } catch (IOException e) {
            // Échec silencieux
        }

        return peersDistants;
    }

    /**
     * Version silencieuse du nettoyage des peers inactifs
     */
    private void nettoyerPeersInactifsSilencieux() {
        if (!actif)
            return;

        List<PeerInfo> peersASupprimer = new ArrayList<>();

        for (PeerInfo peer : peersConnus) {
            if (!peer.estActif(PEER_TIMEOUT_MS)) {
                if (!testerConnexionPeerSilencieux(peer.getAdresse(), peer.getPort())) {
                    peersASupprimer.add(peer);
                } else {
                    peer.updatePing();
                }
            }
        }

        if (!peersASupprimer.isEmpty()) {
            peersConnus.removeAll(peersASupprimer);

            // Nettoyer le cache
            for (PeerInfo peer : peersASupprimer) {
                String cle = peer.getAdresse() + ":" + peer.getPort();
                cacheFichiersPeers.remove(cle);
            }
        }
    }

    /**
     * Version silencieuse de la mise à jour de tous les caches
     */
    private void mettreAJourTousLesCacheSilencieux() {
        if (!actif)
            return;

        for (PeerInfo peer : new ArrayList<>(peersConnus)) {
            mettreAJourCachePeerSilencieux(peer);
        }
    }

    /**
     * Version silencieuse de la mise à jour du cache d'un peer
     */
    private void mettreAJourCachePeerSilencieux(PeerInfo peer) {
        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            socket.setSoTimeout(3000);
            out.println("LIST");
            String json = in.readLine();

            if (json != null && !json.startsWith("ERREUR")) {
                Gson gson = new Gson();
                List<Metadata> fichiers = gson.fromJson(json, new TypeToken<List<Metadata>>() {
                }.getType());
                cacheFichiersPeers.put(peer.getAdresse() + ":" + peer.getPort(), fichiers);
                peer.updatePing();
            }

        } catch (IOException e) {
            // Échec silencieux
        }
    }

    /**
     * Teste si un peer est actif (version silencieuse)
     */
    private boolean testerConnexionPeerSilencieux(String adresse, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(adresse, port), 1000);

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            socket.setSoTimeout(1000);
            out.println("PING");
            String reponse = in.readLine();

            return reponse != null && reponse.startsWith("PONG");

        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Commande manuelle pour forcer la synchronisation (avec messages)
     */
    public void synchroniserMaintenant() {
        System.out.println("Synchronisation manuelle déclenchée...");

        Set<PeerInfo> nouveauxPeers = new HashSet<>();

        for (PeerInfo peer : new ArrayList<>(peersConnus)) {
            try {
                annoncerAuPeerSilencieux(peer);
                Set<PeerInfo> peersDuPeer = recupererPeersDuPeerSilencieux(peer);
                nouveauxPeers.addAll(peersDuPeer);
            } catch (Exception e) {
                if (DEBUG_MODE) {
                    System.err.println("Erreur lors de la sync avec " + peer + ": " + e.getMessage());
                }
            }
        }

        int nouveauxAjoutes = 0;
        for (PeerInfo nouveauPeer : nouveauxPeers) {
            if (!nouveauPeer.getAdresse().equals("localhost") ||
                    nouveauPeer.getPort() != portEcoute) {
                if (!peersConnus.contains(nouveauPeer)) {
                    ajouterPeerSilencieux(nouveauPeer);
                    nouveauxAjoutes++;
                }
            }
        }

        System.out.println(
                "Synchronisation terminée. " + nouveauxAjoutes + " nouveaux peers. Total: " + peersConnus.size());
    }

    // Méthodes pour l'interface utilisateur (avec messages appropriés)
    public List<PeerInfo> rechercherFichier(String nomFichier) {
        List<PeerInfo> peersAvecFichier = new ArrayList<>();

        for (PeerInfo peer : peersConnus) {
            String clePeer = peer.getAdresse() + ":" + peer.getPort();
            List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);

            if (fichiers != null) {
                for (Metadata meta : fichiers) {
                    if (meta.getNom().equals(nomFichier)) {
                        peersAvecFichier.add(peer);
                        break;
                    }
                }
            }
        }

        return peersAvecFichier;
    }

    public boolean telechargerFichier(String nomFichier) {
        List<PeerInfo> peersAvecFichier = rechercherFichier(nomFichier);

        if (peersAvecFichier.isEmpty()) {
            System.out.println("Fichier '" + nomFichier + "' introuvable sur le réseau");
            return false;
        }

        System.out.println("Fichier trouvé chez " + peersAvecFichier.size() + " peer(s)");

        for (PeerInfo peer : peersAvecFichier) {
            System.out.println("Tentative de téléchargement depuis " + peer);
            if (telechargerDepuisPeer(peer, nomFichier)) {
                return true;
            }
        }

        System.err.println("Échec du téléchargement depuis tous les peers");
        return false;
    }

    public boolean telechargerDepuisPeer(PeerInfo peer, String nomFichier) {
        File fichierLocal = new File(dossierPartage, nomFichier);
        if (fichierLocal.exists()) {
            String nomLocal = trouverNomCopie(fichierLocal);
            System.out.println("Fichier existant, sauvegarde sous: " + nomLocal);
            fichierLocal = new File(dossierPartage, nomLocal);
        }

        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
                InputStream socketIn = socket.getInputStream();
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("GET " + nomFichier + " 0");

            String checksumServeur = lireLigne(socketIn);
            if (checksumServeur == null || checksumServeur.startsWith("ERREUR")) {
                System.err.println("Erreur lors de la demande du fichier: " + checksumServeur);
                return false;
            }

            String tailleStr = lireLigne(socketIn);
            if (tailleStr == null)
                return false;

            long tailleFichier = Long.parseLong(tailleStr);

            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fichierLocal))) {
                byte[] buffer = new byte[8192];
                long reste = tailleFichier;
                int lu;

                while (reste > 0 && (lu = socketIn.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
                    bos.write(buffer, 0, lu);
                    reste -= lu;
                }
            }

            try {
                String checksumLocal = fileManager.calculerChecksum(fichierLocal);
                if (checksumLocal.equals(checksumServeur)) {
                    System.out.println("Fichier téléchargé avec succès: " + fichierLocal.getName());
                    return true;
                } else {
                    System.err.println("Erreur checksum pour " + nomFichier);
                    fichierLocal.delete();
                    return false;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                return false;
            }

        } catch (IOException e) {
            System.err.println("Erreur lors du téléchargement depuis " + peer + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Version silencieuse de l'envoi de la liste des fichiers
     */
    private void envoyerListeFichiersSilencieux(PrintWriter out) {
        synchronized (fileLock) {
            List<File> fichiers = fileManager.listerFichiers();
            List<Metadata> meta = fichiers.stream()
                    .filter(File::isFile)
                    .map(f -> {
                        try {
                            return new Metadata(f.getName(), f.length(), fileManager.calculerChecksum(f));
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .toList();

            String json = new Gson().toJson(meta);
            out.println(json);
            // Pas de message de log
        }
    }

    /**
     * Version silencieuse de l'envoi de fichier
     */
    private void envoyerFichierSilencieux(String nomFichier, OutputStream socketOut, PrintWriter out, long offset) {
        synchronized (fileLock) {
            try {
                File fichier = new File(dossierPartage, nomFichier);
                if (!fichier.exists() || !fichier.isFile()) {
                    out.println("ERREUR: fichier introuvable");
                    return;
                }

                String checksum = fileManager.calculerChecksum(fichier);
                long taille = fichier.length();

                out.println(checksum);
                out.println(taille);
                out.flush();

                try (RandomAccessFile raf = new RandomAccessFile(fichier, "r")) {
                    if (offset > 0) {
                        raf.seek(offset);
                    }

                    byte[] buffer = new byte[8192];
                    long reste = taille - offset;
                    int read;

                    while (reste > 0 && (read = raf.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
                        socketOut.write(buffer, 0, read);
                        reste -= read;
                    }
                }

                socketOut.flush();
                // Seulement afficher en mode debug
                if (DEBUG_MODE) {
                    System.out.println("Fichier envoyé: " + nomFichier + " (offset: " + offset + ")");
                }

            } catch (Exception e) {
                if (DEBUG_MODE) {
                    e.printStackTrace();
                }
                out.println("ERREUR lors de l'envoi du fichier");
            }
        }
    }

    /**
     * Écoute les connexions entrantes (mode serveur)
     */
    private void ecouterConnexions() {
        while (actif && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> traiterRequetePeer(clientSocket)).start();
            } catch (IOException e) {
                if (actif && DEBUG_MODE) {
                    System.err.println("Erreur lors de l'acceptation d'une connexion: " + e.getMessage());
                }
            }
        }
    }

    // Méthodes utilitaires inchangées
    private String lireLigne(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n')
                break;
            buffer.write(b);
        }
        return buffer.toString("UTF-8").trim();
    }

    private String trouverNomCopie(File fichier) {
        String nom = fichier.getName();
        int pointIndex = nom.lastIndexOf('.');
        String base = (pointIndex == -1) ? nom : nom.substring(0, pointIndex);
        String extension = (pointIndex == -1) ? "" : nom.substring(pointIndex);
        int compteur = 1;

        String nouveauNom;
        do {
            nouveauNom = base + "(" + compteur + ")" + extension;
            compteur++;
        } while (new File(dossierPartage, nouveauNom).exists());

        return nouveauNom;
    }

    private String getAdresseReseauLocal() {
        try {
            return InetAddress.getLocalHost().getHostAddress().substring(0,
                    InetAddress.getLocalHost().getHostAddress().lastIndexOf('.'));
        } catch (UnknownHostException e) {
            return "192.168.1";
        }
    }

    // Getters
    public String getPseudo() {
        return pseudo;
    }

    public int getPort() {
        return portEcoute;
    }

    public List<PeerInfo> getPeersConnus() {
        return new ArrayList<>(peersConnus);
    }

    public File getDossierPartage() {
        return dossierPartage;
    }

    public void afficherEtatReseau() {
        System.out.println("\n=== État du réseau P2P ===");
        System.out.println("Peer: " + pseudo + " (port " + portEcoute + ")");
        System.out.println("Peers connus: " + peersConnus.size());

        for (PeerInfo peer : peersConnus) {
            String clePeer = peer.getAdresse() + ":" + peer.getPort();
            List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
            int nbFichiers = (fichiers != null) ? fichiers.size() : 0;
            long inactiviteMs = System.currentTimeMillis() - peer.getDernierePing();
            String statut = peer.estActif(PEER_TIMEOUT_MS) ? "ACTIF" : "INACTIF";
            System.out.println(
                    "  - " + peer + " (" + nbFichiers + " fichiers) [" + statut + " - " + (inactiviteMs / 1000) + "s]");
        }

        List<File> mesFichiers = fileManager.listerFichiers();
        System.out.println("Mes fichiers partagés: " + mesFichiers.size());
        for (File f : mesFichiers) {
            if (f.isFile()) {
                System.out.println("  - " + f.getName() + " (" + f.length() + " octets)");
            }
        }
        System.out.println("========================\n");
    }

    /**
 * Version silencieuse de l'envoi de la liste des peers
 * Filtre le peer courant et les peers sans pseudo
 */
private void envoyerListePeersSilencieux(PrintWriter out) {
    List<PeerInfo> peersActifs = peersConnus.stream()
            .filter(p -> p.estActif(PEER_TIMEOUT_MS))
            // Exclure le peer courant - vérifier à la fois localhost et 127.0.0.1
            .filter(p -> !((p.getAdresse().equals("localhost") || p.getAdresse().equals("127.0.0.1")) 
                          && p.getPort() == this.portEcoute))
            // Exclure les peers sans pseudo (pseudo vide ou null)
            .filter(p -> p.getPseudo() != null && !p.getPseudo().trim().isEmpty())
            .toList();

    String json = new Gson().toJson(peersActifs);
    out.println(json);
    // Pas de message de log
}

/**
 * Version améliorée pour ajouter un peer (avec vérification pour éviter de s'ajouter soi-même)
 */
private boolean ajouterPeerSilencieux(PeerInfo peerInfo) {
    // Ne pas s'ajouter soi-même
    if ((peerInfo.getAdresse().equals("localhost") || peerInfo.getAdresse().equals("127.0.0.1")) 
        && peerInfo.getPort() == this.portEcoute) {
        return false;
    }
    
    if (!peersConnus.contains(peerInfo)) {
        peersConnus.add(peerInfo);

        // Récupérer la liste de ses fichiers (silencieusement)
        scheduler.execute(() -> mettreAJourCachePeerSilencieux(peerInfo));
        return true;
    } else {
        // Mettre à jour le ping du peer existant
        for (PeerInfo p : peersConnus) {
            if (p.equals(peerInfo)) {
                p.updatePing();
                if (peerInfo.getPseudo() != null && !peerInfo.getPseudo().isEmpty()) {
                    p.setPseudo(peerInfo.getPseudo());
                }
                break;
            }
        }
        return false;
    }
}

/**
 * Version publique pour ajouter un peer manuellement (avec message et vérification)
 */
public void ajouterPeer(PeerInfo peerInfo) {
    // Ne pas s'ajouter soi-même
    if ((peerInfo.getAdresse().equals("localhost") || peerInfo.getAdresse().equals("127.0.0.1")) 
        && peerInfo.getPort() == this.portEcoute) {
        System.out.println("Impossible de s'ajouter soi-même comme peer");
        return;
    }
    
    if (!peersConnus.contains(peerInfo)) {
        peersConnus.add(peerInfo);
        System.out.println("Nouveau peer ajouté: " + peerInfo);

        // Récupérer immédiatement la liste de ses fichiers
        scheduler.execute(() -> mettreAJourCachePeerSilencieux(peerInfo));
    } else {
        // Mettre à jour le ping du peer existant
        for (PeerInfo p : peersConnus) {
            if (p.equals(peerInfo)) {
                p.updatePing();
                if (peerInfo.getPseudo() != null && !peerInfo.getPseudo().isEmpty()) {
                    p.setPseudo(peerInfo.getPseudo());
                }
                break;
            }
        }
        System.out.println("Peer mis à jour: " + peerInfo);
    }
}

/**
 * Découvre automatiquement les peers sur le réseau local (version silencieuse améliorée)
 */
private void decouvriPeersSilencieux() {
    try {
        int peersInitiaux = peersConnus.size();

        for (int port = 8000; port <= 8100; port++) {
            if (port == portEcoute) continue; // Ne pas se tester soi-même

            try {
                String adresse = "localhost";
                if (testerConnexionPeerSilencieux(adresse, port)) {
                    // Ne pas ajouter si c'est nous-même (double vérification)
                    if (port != this.portEcoute) {
                        ajouterPeerSilencieux(new PeerInfo(adresse, port, ""));
                    }
                }
            } catch (Exception e) {
                // Pas de peer sur ce port
            }

            Thread.sleep(10);
        }

        int nouveauxPeers = peersConnus.size() - peersInitiaux;
        if (nouveauxPeers > 0) {
            System.out.println("Réseau P2P: " + nouveauxPeers + " peer(s) découvert(s)");
        }

    } catch (Exception e) {
        if (DEBUG_MODE) {
            System.err.println("Erreur lors de la découverte: " + e.getMessage());
        }
    }
}
}