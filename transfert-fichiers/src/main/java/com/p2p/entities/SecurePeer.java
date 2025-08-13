package com.p2p.entities;

import com.p2p.manager.FileManager;
import com.p2p.manager.Metadata;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;

/**
 * Classe Peer sécurisée avec protection contre les attaques et gestion avancée
 */
public class SecurePeer {
    private final String pseudo;
    private final int portEcoute;
    private final File dossierPartage;
    private final FileManager fileManager;
    private final Object fileLock = new Object();
    private final SecureRandom secureRandom = new SecureRandom();

    // Configuration sécurisée
    private static final int MAX_CONNECTIONS = 50;
    private static final int MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB
    private static final int MAX_PEERS = 1000;
    private static final int CONNECTION_TIMEOUT = 10000; // 10s
    private static final int SOCKET_TIMEOUT = 30000; // 30s
    private static final long PEER_TIMEOUT_MS = 30000; // 30s
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int RATE_LIMIT_REQUESTS_PER_MINUTE = 60;

    // Gestion des connexions et limitation de débit
    private final Map<String, AtomicLong> rateLimitMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> connectionCount = new ConcurrentHashMap<>();
    private final Set<String> blacklistedIPs = ConcurrentHashMap.newKeySet();
    
    // Liste des peers avec protection thread-safe
    private final List<PeerInfo> peersConnus = new CopyOnWriteArrayList<>();
    private final Map<String, List<Metadata>> cacheFichiersPeers = new ConcurrentHashMap<>();
    
    // Serveur et threads
    private ServerSocket serverSocket;
    private volatile boolean actif = false;
    private final ExecutorService connectionPool = Executors.newFixedThreadPool(MAX_CONNECTIONS);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
    
    // Métriques et monitoring
    private final AtomicLong totalConnections = new AtomicLong(0);
    private final AtomicLong totalBytesTransferred = new AtomicLong(0);
    private final AtomicLong successfulDownloads = new AtomicLong(0);
    private final AtomicLong failedDownloads = new AtomicLong(0);

    public SecurePeer(String pseudo, int portEcoute, String dossierPartage) {
        this.pseudo = sanitizeInput(pseudo, 50);
        this.portEcoute = validatePort(portEcoute);
        this.dossierPartage = new File(dossierPartage);

        // Validation et création sécurisée du dossier
        if (!createSecureDirectory(this.dossierPartage)) {
            throw new SecurityException("Impossible de créer le dossier de partage sécurisé");
        }

        this.fileManager = new FileManager(this.dossierPartage.getPath());
    }

    /**
     * Démarrage sécurisé du peer
     */
    public void demarrer() {
        try {
            serverSocket = new ServerSocket(portEcoute);
            serverSocket.setSoTimeout(1000); // Timeout pour permettre l'arrêt propre
            actif = true;

            System.out.println("Peer sécurisé '" + pseudo + "' démarré sur le port " + portEcoute);

            // Thread principal d'écoute
            new Thread(this::ecouterConnexionsSecurisees, "PeerListener-" + portEcoute).start();

            // Tâches périodiques sécurisées
            scheduler.scheduleAtFixedRate(this::maintenanceSecurite, 30, 30, TimeUnit.SECONDS);
            scheduler.scheduleAtFixedRate(this::synchroniserPeersSecurise, 15, 15, TimeUnit.SECONDS);
            scheduler.scheduleAtFixedRate(this::nettoyerRessources, 60, 60, TimeUnit.SECONDS);
            scheduler.scheduleAtFixedRate(this::mettreAJourCacheSecurise, 20, 20, TimeUnit.SECONDS);
            
            // Découverte initiale différée
            scheduler.schedule(this::decouvriPeersSecurise, 2, TimeUnit.SECONDS);

        } catch (IOException e) {
            System.err.println("Erreur critique lors du démarrage: " + e.getMessage());
            throw new RuntimeException("Impossible de démarrer le peer", e);
        }
    }

    /**
     * Arrêt sécurisé avec nettoyage complet
     */
    public void arreter() {
        System.out.println("Arrêt du peer '" + pseudo + "' en cours...");
        actif = false;

        // Arrêt des schedulers
        scheduler.shutdown();
        connectionPool.shutdown();

        try {
            // Attendre la fin des tâches en cours
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!connectionPool.awaitTermination(10, TimeUnit.SECONDS)) {
                connectionPool.shutdownNow();
            }

            // Fermeture du socket serveur
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }

            // Nettoyage des caches
            cacheFichiersPeers.clear();
            rateLimitMap.clear();
            connectionCount.clear();

            System.out.println("Peer '" + pseudo + "' arrêté proprement");
            afficherStatistiquesFin();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interruption lors de l'arrêt");
        } catch (IOException e) {
            System.err.println("Erreur lors de la fermeture: " + e.getMessage());
        }
    }

    /**
     * Écoute sécurisée des connexions avec limitation
     */
    private void ecouterConnexionsSecurisees() {
        while (actif) {
            try {
                Socket clientSocket = serverSocket.accept();
                totalConnections.incrementAndGet();

                // Vérifications de sécurité
                String clientIP = clientSocket.getInetAddress().getHostAddress();
                
                if (!validerConnexionEntrante(clientIP)) {
                    clientSocket.close();
                    continue;
                }

                // Traitement asynchrone de la connexion
                connectionPool.submit(() -> traiterConnexionSecurisee(clientSocket, clientIP));

            } catch (SocketTimeoutException e) {
                // Timeout normal pour vérifier actif
                continue;
            } catch (IOException e) {
                if (actif) {
                    System.err.println("Erreur lors de l'acceptation d'une connexion: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Validation des connexions entrantes
     */
    private boolean validerConnexionEntrante(String clientIP) {
        // Vérifier la blacklist
        if (blacklistedIPs.contains(clientIP)) {
            System.out.println("Connexion refusée: IP blacklistée " + clientIP);
            return false;
        }

        // Limitation du nombre de connexions par IP
        Integer currentConnections = connectionCount.getOrDefault(clientIP, 0);
        if (currentConnections >= 5) {
            System.out.println("Connexion refusée: trop de connexions depuis " + clientIP);
            return false;
        }

        // Rate limiting
        if (!checkRateLimit(clientIP)) {
            System.out.println("Connexion refusée: rate limit dépassé pour " + clientIP);
            return false;
        }

        return true;
    }

    /**
     * Vérification du rate limiting
     */
    private boolean checkRateLimit(String clientIP) {
        long currentTime = System.currentTimeMillis();
        long windowStart = currentTime - 60000; // Fenêtre d'1 minute

        AtomicLong requestCount = rateLimitMap.computeIfAbsent(clientIP, k -> new AtomicLong(0));
        
        // Simple rate limiting (à améliorer avec une implémentation sliding window)
        if (requestCount.get() > RATE_LIMIT_REQUESTS_PER_MINUTE) {
            return false;
        }
        
        requestCount.incrementAndGet();
        return true;
    }

    /**
     * Traitement sécurisé des connexions
     */
    private void traiterConnexionSecurisee(Socket clientSocket, String clientIP) {
        connectionCount.merge(clientIP, 1, Integer::sum);
        
        try {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT);
            
            try (BufferedReader in = new BufferedReader(new InputStreamReader(
                    clientSocket.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(
                    clientSocket.getOutputStream(), StandardCharsets.UTF_8), true);
                 InputStream socketIn = clientSocket.getInputStream();
                 OutputStream socketOut = clientSocket.getOutputStream()) {

                String commande = in.readLine();
                if (commande == null || commande.trim().isEmpty()) {
                    return;
                }

                // Validation de la commande
                if (!validerCommande(commande)) {
                    out.println("ERREUR: Commande invalide");
                    return;
                }

                traiterCommandeSecurisee(commande.trim(), out, socketIn, socketOut, clientIP);

            }
        } catch (IOException e) {
            System.err.println("Erreur lors du traitement de " + clientIP + ": " + e.getMessage());
        } finally {
            connectionCount.merge(clientIP, -1, Integer::sum);
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException ignored) {}
        }
    }

    /**
     * Validation des commandes reçues
     */
    private boolean validerCommande(String commande) {
        if (commande == null || commande.length() > 1000) {
            return false;
        }

        String[] parts = commande.split("\\s+");
        if (parts.length == 0) {
            return false;
        }

        String cmd = parts[0].toUpperCase();
        return Arrays.asList("PING", "LIST", "GET", "PEERS", "ANNOUNCE").contains(cmd);
    }

    /**
     * Traitement sécurisé des commandes
     */
    private void traiterCommandeSecurisee(String commande, PrintWriter out, 
            InputStream socketIn, OutputStream socketOut, String clientIP) {
        
        String[] parts = commande.split("\\s+", 4);
        String cmd = parts[0].toUpperCase();

        try {
            switch (cmd) {
                case "PING":
                    traiterPingSecurise(out);
                    break;

                case "LIST":
                    traiterListSecurise(socketOut);
                    break;

                case "GET":
                    if (parts.length >= 2) {
                        String nomFichier = sanitizeFileName(parts[1]);
                        long offset = 0;
                        if (parts.length >= 3) {
                            try {
                                offset = Math.max(0, Long.parseLong(parts[2]));
                            } catch (NumberFormatException e) {
                                offset = 0;
                            }
                        }
                        traiterGetSecurise(nomFichier, socketOut, out, offset, clientIP);
                    } else {
                        out.println("ERREUR: Paramètres GET invalides");
                    }
                    break;

                case "PEERS":
                    traiterPeersSecurise(socketOut);
                    break;

                case "ANNOUNCE":
                    if (parts.length >= 3) {
                        String pseudoAnnonce = sanitizeInput(parts[1], 50);
                        try {
                            int portAnnonce = validatePort(Integer.parseInt(parts[2]));
                            traiterAnnounceSecurise(pseudoAnnonce, portAnnonce, clientIP, out);
                        } catch (NumberFormatException e) {
                            out.println("ERREUR: Port invalide");
                        }
                    } else {
                        out.println("ERREUR: Paramètres ANNOUNCE invalides");
                    }
                    break;

                default:
                    out.println("ERREUR: Commande non supportée");
            }
        } catch (Exception e) {
            System.err.println("Erreur lors du traitement de '" + cmd + "': " + e.getMessage());
            out.println("ERREUR: Erreur interne du serveur");
        }
    }

    /**
     * Traitement sécurisé du PING
     */
    private void traiterPingSecurise(PrintWriter out) {
        out.println("PONG " + pseudo + " " + portEcoute);
    }

    /**
     * Traitement sécurisé du LIST
     */
    private void traiterListSecurise(OutputStream socketOut) {
        synchronized (fileLock) {
            try {
                List<File> fichiers = fileManager.listerFichiers();
                List<Metadata> metadata = new ArrayList<>();
                
                for (File f : fichiers) {
                    if (f.isFile() && f.length() <= MAX_FILE_SIZE) {
                        try {
                            String checksum = fileManager.calculerChecksum(f);
                            Metadata meta = new Metadata(f.getName(), f.length(), checksum);
                            metadata.add(meta);
                        } catch (Exception e) {
                            // Ignorer les fichiers corrompus
                            System.err.println("Erreur checksum pour " + f.getName() + ": " + e.getMessage());
                        }
                    }
                }

                byte[] data = serialiserListeMetadataSecurise(metadata);
                envoyerDonneesSecurisees(socketOut, data);

            } catch (Exception e) {
                System.err.println("Erreur lors de LIST: " + e.getMessage());
                try {
                    envoyerDonneesSecurisees(socketOut, new byte[0]);
                } catch (IOException ignored) {}
            }
        }
    }

    /**
     * Traitement sécurisé du GET
     */
    private void traiterGetSecurise(String nomFichier, OutputStream socketOut, 
            PrintWriter out, long offset, String clientIP) {
        
        synchronized (fileLock) {
            try {
                File fichier = new File(dossierPartage, nomFichier);
                
                // Vérifications de sécurité
                if (!validerAccesFichier(fichier)) {
                    out.println("ERREUR: Accès au fichier refusé");
                    return;
                }

                if (!fichier.exists() || !fichier.isFile()) {
                    out.println("ERREUR: Fichier introuvable");
                    return;
                }

                if (fichier.length() > MAX_FILE_SIZE) {
                    out.println("ERREUR: Fichier trop volumineux");
                    return;
                }

                // Calcul du checksum sécurisé
                String checksum = fileManager.calculerChecksum(fichier);
                long taille = fichier.length();

                out.println(checksum);
                out.println(taille);
                out.flush();

                // Envoi sécurisé du fichier
                envoyerFichierSecurise(fichier, socketOut, offset);
                totalBytesTransferred.addAndGet(taille - offset);

                System.out.println("Fichier envoyé à " + clientIP + ": " + nomFichier + 
                                 " (offset: " + offset + ", taille: " + (taille - offset) + ")");

            } catch (Exception e) {
                System.err.println("Erreur lors de GET " + nomFichier + ": " + e.getMessage());
                out.println("ERREUR: Impossible d'envoyer le fichier");
            }
        }
    }

    /**
     * Validation de l'accès aux fichiers (protection path traversal)
     */
    private boolean validerAccesFichier(File fichier) {
        try {
            String cheminCanonique = fichier.getCanonicalPath();
            String dossierCanonique = dossierPartage.getCanonicalPath();
            return cheminCanonique.startsWith(dossierCanonique);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Envoi sécurisé de fichier avec vérification d'intégrité
     */
    private void envoyerFichierSecurise(File fichier, OutputStream socketOut, long offset) 
            throws IOException {
        
        try (RandomAccessFile raf = new RandomAccessFile(fichier, "r");
             BufferedOutputStream bos = new BufferedOutputStream(socketOut)) {
            
            if (offset > 0) {
                raf.seek(offset);
            }

            byte[] buffer = new byte[8192];
            long reste = fichier.length() - offset;
            int lu;

            while (reste > 0 && (lu = raf.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
                bos.write(buffer, 0, lu);
                reste -= lu;
            }
            
            bos.flush();
        }
    }

    /**
     * Téléchargement sécurisé avec retry et vérification
     */
    public boolean telechargerFichierSecurise(String nomFichier) {
        List<PeerInfo> peersAvecFichier = rechercherFichier(nomFichier);

        if (peersAvecFichier.isEmpty()) {
            System.out.println("Fichier '" + nomFichier + "' introuvable sur le réseau");
            return false;
        }

        // Mélanger la liste pour équilibrer la charge
        Collections.shuffle(peersAvecFichier, secureRandom);

        System.out.println("Fichier trouvé chez " + peersAvecFichier.size() + " peer(s)");

        for (PeerInfo peer : peersAvecFichier) {
            System.out.println("Tentative de téléchargement depuis " + peer);
            
            for (int tentative = 1; tentative <= MAX_RETRY_ATTEMPTS; tentative++) {
                if (telechargerDepuisPeerSecurise(peer, nomFichier, tentative)) {
                    successfulDownloads.incrementAndGet();
                    return true;
                }
                
                if (tentative < MAX_RETRY_ATTEMPTS) {
                    System.out.println("Tentative " + tentative + " échouée, nouvelle tentative...");
                    try {
                        Thread.sleep(1000 * tentative); // Backoff exponentiel
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        failedDownloads.incrementAndGet();
        System.err.println("Échec du téléchargement depuis tous les peers après " + MAX_RETRY_ATTEMPTS + " tentatives");
        return false;
    }

    /**
     * Téléchargement depuis un peer avec validation renforcée
     */
    private boolean telechargerDepuisPeerSecurise(PeerInfo peer, String nomFichier, int tentative) {
        File fichierLocal = new File(dossierPartage, nomFichier);
        
        // Gestion des fichiers existants
        if (fichierLocal.exists()) {
            String nomSauvegarde = genererNomSauvegarde(nomFichier);
            System.out.println("Fichier existant, sauvegarde sous: " + nomSauvegarde);
            fichierLocal = new File(dossierPartage, nomSauvegarde);
        }

        File fichierTemp = new File(dossierPartage, nomFichier + ".tmp" + System.currentTimeMillis());

        try (Socket socket = new Socket()) {
            // Connexion avec timeout
            socket.connect(new InetSocketAddress(peer.getAdresse(), peer.getPort()), CONNECTION_TIMEOUT);
            socket.setSoTimeout(SOCKET_TIMEOUT);

            try (InputStream socketIn = socket.getInputStream();
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(
                    socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

                out.println("GET " + nomFichier + " 0");

                String checksumServeur = lireLigneSecurisee(socketIn);
                if (checksumServeur == null || checksumServeur.startsWith("ERREUR")) {
                    System.err.println("Erreur serveur: " + checksumServeur);
                    return false;
                }

                String tailleStr = lireLigneSecurisee(socketIn);
                if (tailleStr == null) return false;

                long tailleFichier;
                try {
                    tailleFichier = Long.parseLong(tailleStr);
                    if (tailleFichier <= 0 || tailleFichier > MAX_FILE_SIZE) {
                        System.err.println("Taille de fichier invalide: " + tailleFichier);
                        return false;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Taille de fichier invalide: " + tailleStr);
                    return false;
                }

                // Téléchargement avec vérification en temps réel
                if (!telechargerAvecVerification(socketIn, fichierTemp, tailleFichier, checksumServeur)) {
                    return false;
                }

                // Renommage atomique
                if (!fichierTemp.renameTo(fichierLocal)) {
                    System.err.println("Impossible de finaliser le téléchargement");
                    fichierTemp.delete();
                    return false;
                }

                System.out.println("✓ Téléchargement réussi (tentative " + tentative + "): " + fichierLocal.getName());
                totalBytesTransferred.addAndGet(tailleFichier);
                return true;

            }
        } catch (IOException e) {
            System.err.println("Erreur réseau (tentative " + tentative + "): " + e.getMessage());
            if (fichierTemp.exists()) {
                fichierTemp.delete();
            }
            return false;
        }
    }

    /**
     * Téléchargement avec vérification d'intégrité en temps réel
     */
    private boolean telechargerAvecVerification(InputStream socketIn, File fichierTemp, 
            long tailleFichier, String checksumAttendu) throws IOException {
        
        try (FileOutputStream fos = new FileOutputStream(fichierTemp);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            long reste = tailleFichier;
            long totalLu = 0;
            int lu;
            long derniereProgression = 0;

            while (reste > 0 && (lu = socketIn.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
                bos.write(buffer, 0, lu);
                digest.update(buffer, 0, lu);
                reste -= lu;
                totalLu += lu;
                
                // Affichage de progression pour gros fichiers
                if (tailleFichier > 1024 * 1024) {
                    long progression = (totalLu * 100) / tailleFichier;
                    if (progression != derniereProgression && progression % 10 == 0) {
                        System.out.print("\rTéléchargement: " + progression + "%");
                        derniereProgression = progression;
                    }
                }
            }
            
            if (tailleFichier > 1024 * 1024) {
                System.out.println(); // Nouvelle ligne
            }

            // Vérification finale du checksum
            byte[] hash = digest.digest();
            StringBuilder checksumCalcule = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) checksumCalcule.append('0');
                checksumCalcule.append(hex);
            }

            if (!checksumCalcule.toString().equals(checksumAttendu)) {
                System.err.println("Erreur checksum - fichier corrompu");
                return false;
            }

            return totalLu == tailleFichier;

        } catch (Exception e) {
            System.err.println("Erreur lors de la vérification: " + e.getMessage());
            return false;
        }
    }

    // Méthodes utilitaires de sécurité
    private String sanitizeInput(String input, int maxLength) {
        if (input == null) return "";
        String sanitized = input.replaceAll("[^a-zA-Z0-9._-]", "");
        return sanitized.length() > maxLength ? sanitized.substring(0, maxLength) : sanitized;
    }

    private String sanitizeFileName(String fileName) {
        if (fileName == null) return "";
        // Empêcher path traversal
        String sanitized = fileName.replaceAll("[^a-zA-Z0-9._-]", "");
        return sanitized.length() > 255 ? sanitized.substring(0, 255) : sanitized;
    }

    private int validatePort(int port) {
        if (port < 1024 || port > 65535) {
            throw new IllegalArgumentException("Port invalide: " + port);
        }
        return port;
    }

    private boolean createSecureDirectory(File directory) {
        if (directory.exists()) {
            return directory.isDirectory() && directory.canRead() && directory.canWrite();
        }
        return directory.mkdirs();
    }

    private String lireLigneSecurisee(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        int compteur = 0;
        while ((b = in.read()) != -1 && compteur < 1000) { // Limite de taille
            if (b == '\n') break;
            if (b != '\r') { // Ignorer \r
                buffer.write(b);
                compteur++;
            }
        }
        return buffer.toString(StandardCharsets.UTF_8).trim();
    }

    private String genererNomSauvegarde(String nomOriginal) {
        String baseName = nomOriginal;
        String extension = "";
        
        int dotIndex = nomOriginal.lastIndexOf('.');
        if (dotIndex != -1) {
            baseName = nomOriginal.substring(0, dotIndex);
            extension = nomOriginal.substring(dotIndex);
        }
        
        int compteur = 1;
        String nouveauNom;
        do {
            nouveauNom = baseName + "_" + compteur + extension;
            compteur++;
        } while (new File(dossierPartage, nouveauNom).exists());
        
        return nouveauNom;
    }

    // Maintenance et nettoyage
    private void maintenanceSecurite() {
        // Nettoyage des rate limits
        long cutoff = System.currentTimeMillis() - 60000;
        rateLimitMap.entrySet().removeIf(entry -> {
            // Simple cleanup - dans une vraie implémentation, utiliser une structure plus sophistiquée
            return entry.getValue().get() == 0;
        });

        // Nettoyage des connexions inactives
        connectionCount.entrySet().removeIf(entry -> entry.getValue() <= 0);

        // Vérification des peers suspects (optionnel)
        verifierPeersSuspects();
    }

    private void verifierPeersSuspects() {
        // Logique pour détecter et blacklister les peers suspects
        // (par exemple, ceux qui envoient trop de requêtes invalides)
    }

    private void nettoyerRessources() {
        // Nettoyage du cache des fichiers
        long maintenant = System.currentTimeMillis();
        fileManager.nettoyerCache();
        
        // Nettoyage des peers inactifs
        peersConnus.removeIf(peer -> !peer.estActif(PEER_TIMEOUT_MS * 2));
        
        System.gc(); // Suggestion de garbage collection
    }

    // Sérialisation sécurisée
    private byte[] serialiserListeMetadataSecurise(List<Metadata> metadatas) throws IOException {
        if (metadatas == null || metadatas.size() > 10000) { // Limite de sécurité
            return new byte[0];
        }
        
        // Utiliser la sérialisation existante mais avec validation
        return serialiserListeMetadata(metadatas);
    }

    private void envoyerDonneesSecurisees(OutputStream out, byte[] data) throws IOException {
        if (data.length > 10 * 1024 * 1024) { // Max 10MB
            throw new IOException("Données trop volumineuses");
        }
        
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.putInt(data.length);
        out.write(lengthBuffer.array());
        out.write(data);
        out.flush();
    }

    // Méthodes publiques de l'interface
    public List<PeerInfo> rechercherFichier(String nomFichier) {
        List<PeerInfo> peersAvecFichier = new ArrayList<>();
        String nomSecurise = sanitizeFileName(nomFichier);

        for (PeerInfo peer : peersConnus) {
            if (!peer.estActif(PEER_TIMEOUT_MS)) continue;
            
            String clePeer = peer.getAdresse() + ":" + peer.getPort();
            List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);

            if (fichiers != null) {
                for (Metadata meta : fichiers) {
                    if (meta.getNom().equals(nomSecurise)) {
                        peersAvecFichier.add(peer);
                        break;
                    }
                }
            }
        }

        return peersAvecFichier;
    }

    public void ajouterPeer(PeerInfo peerInfo) {
        if (peerInfo == null || !peerInfo.estValide()) {
            System.out.println("Peer invalide, ignoré");
            return;
        }

        // Ne pas s'ajouter soi-même
        if (estMoiMeme(peerInfo)) {
            System.out.println("Impossible de s'ajouter soi-même comme peer");
            return;
        }

        // Vérifier la limite de peers
        if (peersConnus.size() >= MAX_PEERS) {
            System.out.println("Limite de peers atteinte (" + MAX_PEERS + ")");
            return;
        }

        if (!peersConnus.contains(peerInfo)) {
            // Tester la connectivité avant d'ajouter
            if (testerConnexionPeerSecurise(peerInfo.getAdresse(), peerInfo.getPort())) {
                peersConnus.add(peerInfo);
                System.out.println("Nouveau peer ajouté: " + peerInfo);
                
                // Mise à jour asynchrone du cache
                scheduler.execute(() -> mettreAJourCachePeerSecurise(peerInfo));
            } else {
                System.out.println("Peer non accessible: " + peerInfo);
            }
        } else {
            // Mettre à jour le peer existant
            mettreAJourPeerExistant(peerInfo);
        }
    }

    private boolean estMoiMeme(PeerInfo peer) {
        return (peer.getAdresse().equals("localhost") || 
                peer.getAdresse().equals("127.0.0.1") ||
                peer.getAdresse().equals(getLocalAddress())) && 
               peer.getPort() == portEcoute;
    }

    private String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    private void mettreAJourPeerExistant(PeerInfo nouveauPeer) {
        for (PeerInfo p : peersConnus) {
            if (p.equals(nouveauPeer)) {
                p.updatePing();
                if (nouveauPeer.getPseudo() != null && !nouveauPeer.getPseudo().isEmpty()) {
                    p.setPseudo(nouveauPeer.getPseudo());
                }
                System.out.println("Peer mis à jour: " + p);
                break;
            }
        }
    }

    // Synchronisation sécurisée des peers
    private void synchroniserPeersSecurise() {
        if (!actif || peersConnus.isEmpty()) return;

        List<PeerInfo> peersActifs = new ArrayList<>();
        Set<PeerInfo> nouveauxPeers = new HashSet<>();

        // Vérifier et synchroniser avec les peers existants
        for (PeerInfo peer : new ArrayList<>(peersConnus)) {
            try {
                if (testerConnexionPeerSecurise(peer.getAdresse(), peer.getPort())) {
                    peersActifs.add(peer);
                    peer.updatePing();
                    
                    // Récupérer les peers connus par ce peer
                    Set<PeerInfo> peersDuPeer = recupererPeersDuPeerSecurise(peer);
                    nouveauxPeers.addAll(peersDuPeer);
                }
            } catch (Exception e) {
                System.err.println("Erreur lors de la sync avec " + peer + ": " + e.getMessage());
            }
        }

        // Ajouter les nouveaux peers découverts
        for (PeerInfo nouveauPeer : nouveauxPeers) {
            if (peersConnus.size() < MAX_PEERS && 
                !estMoiMeme(nouveauPeer) && 
                !peersConnus.contains(nouveauPeer)) {
                ajouterPeerSilencieux(nouveauPeer);
            }
        }

        // Supprimer les peers inactifs
        peersConnus.removeIf(peer -> !peer.estActif(PEER_TIMEOUT_MS));
    }

    private void ajouterPeerSilencieux(PeerInfo peerInfo) {
        if (peerInfo.estValide() && 
            testerConnexionPeerSecurise(peerInfo.getAdresse(), peerInfo.getPort())) {
            peersConnus.add(peerInfo);
            scheduler.execute(() -> mettreAJourCachePeerSecurise(peerInfo));
        }
    }

    private boolean testerConnexionPeerSecurise(String adresse, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(adresse, port), 2000);
            socket.setSoTimeout(2000);

            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(
                    socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(
                    socket.getInputStream(), StandardCharsets.UTF_8))) {

                out.println("PING");
                String reponse = in.readLine();
                return reponse != null && reponse.startsWith("PONG");
            }
        } catch (IOException e) {
            return false;
        }
    }

    private Set<PeerInfo> recupererPeersDuPeerSecurise(PeerInfo peer) {
        Set<PeerInfo> peersDistants = new HashSet<>();

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.getAdresse(), peer.getPort()), CONNECTION_TIMEOUT);
            socket.setSoTimeout(SOCKET_TIMEOUT);
            
            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                // Envoyer la commande PEERS
                String commande = "PEERS\n";
                out.write(commande.getBytes(StandardCharsets.UTF_8));
                out.flush();

                // Lire la réponse binaire
                byte[] data = lireDonneesSecurisees(in);
                if (data.length > 0) {
                    List<PeerInfo> peers = PeerInfo.deserialiserListe(data);
                    peersDistants.addAll(peers);
                }
            }
        } catch (IOException e) {
            // Échec silencieux
        }

        return peersDistants;
    }

    private byte[] lireDonneesSecurisees(InputStream in) throws IOException {
        byte[] lengthBytes = new byte[4];
        if (in.read(lengthBytes) != 4) {
            throw new IOException("Impossible de lire la longueur");
        }
        
        int length = ByteBuffer.wrap(lengthBytes).getInt();
        if (length < 0 || length > 10 * 1024 * 1024) { // Max 10MB
            throw new IOException("Longueur de données invalide: " + length);
        }
        
        if (length == 0) {
            return new byte[0];
        }
        
        byte[] data = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(data, totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Connexion fermée prématurément");
            }
            totalRead += read;
        }
        
        return data;
    }

    // Mise à jour sécurisée du cache
    private void mettreAJourCacheSecurise() {
        if (!actif) return;

        List<PeerInfo> peersAMettreAJour = new ArrayList<>(peersConnus);
        Collections.shuffle(peersAMettreAJour, secureRandom);

        // Limiter le nombre de mises à jour simultanées
        int maxUpdates = Math.min(10, peersAMettreAJour.size());
        for (int i = 0; i < maxUpdates; i++) {
            PeerInfo peer = peersAMettreAJour.get(i);
            scheduler.execute(() -> mettreAJourCachePeerSecurise(peer));
        }
    }

    private void mettreAJourCachePeerSecurise(PeerInfo peer) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.getAdresse(), peer.getPort()), CONNECTION_TIMEOUT);
            socket.setSoTimeout(SOCKET_TIMEOUT);
            
            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                // Envoyer la commande LIST
                String commande = "LIST\n";
                out.write(commande.getBytes(StandardCharsets.UTF_8));
                out.flush();

                // Lire la réponse
                byte[] data = lireDonneesSecurisees(in);
                if (data.length > 0) {
                    List<Metadata> fichiers = deserialiserListeMetadataSecurise(data);
                    cacheFichiersPeers.put(peer.getAdresse() + ":" + peer.getPort(), fichiers);
                    peer.updatePing();
                }
            }
        } catch (IOException e) {
            // Échec silencieux - peer probablement hors ligne
            cacheFichiersPeers.remove(peer.getAdresse() + ":" + peer.getPort());
        }
    }

    // Découverte sécurisée des peers
    private void decouvriPeersSecurise() {
        if (!actif) return;

        int peersInitiaux = peersConnus.size();
        ExecutorService discoveryPool = Executors.newFixedThreadPool(20);

        // Scan des ports communs sur localhost
        for (int port = 8000; port <= 8100; port++) {
            if (port == portEcoute) continue;
            
            final int finalPort = port;
            discoveryPool.submit(() -> {
                try {
                    if (testerConnexionPeerSecurise("localhost", finalPort)) {
                        PeerInfo nouveauPeer = new PeerInfo("localhost", finalPort, "");
                        if (!estMoiMeme(nouveauPeer) && !peersConnus.contains(nouveauPeer)) {
                            ajouterPeerSilencieux(nouveauPeer);
                        }
                    }
                } catch (Exception e) {
                    // Ignore
                }
            });
            
            try {
                Thread.sleep(5); // Petite pause pour éviter la surcharge
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        discoveryPool.shutdown();
        try {
            discoveryPool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int nouveauxPeers = peersConnus.size() - peersInitiaux;
        if (nouveauxPeers > 0) {
            System.out.println("Réseau P2P: " + nouveauxPeers + " peer(s) découvert(s)");
        }
    }

    // Traitement des autres commandes sécurisées
    private void traiterPeersSecurise(OutputStream socketOut) {
        try {
            List<PeerInfo> peersActifs = new ArrayList<>();
            
            for (PeerInfo p : peersConnus) {
                if (p.estActif(PEER_TIMEOUT_MS) && p.estValide() && !estMoiMeme(p)) {
                    peersActifs.add(p);
                }
            }

            // Limiter le nombre de peers retournés
            if (peersActifs.size() > 100) {
                Collections.shuffle(peersActifs, secureRandom);
                peersActifs = peersActifs.subList(0, 100);
            }

            byte[] data = PeerInfo.serialiserListe(peersActifs);
            envoyerDonneesSecurisees(socketOut, data);
            
        } catch (Exception e) {
            System.err.println("Erreur lors de PEERS: " + e.getMessage());
            try {
                envoyerDonneesSecurisees(socketOut, new byte[0]);
            } catch (IOException ignored) {}
        }
    }

    private void traiterAnnounceSecurise(String pseudo, int port, String adresseIP, PrintWriter out) {
        try {
            PeerInfo nouveauPeer = new PeerInfo(adresseIP, port, pseudo);
            
            if (nouveauPeer.estValide() && !estMoiMeme(nouveauPeer)) {
                if (peersConnus.size() < MAX_PEERS) {
                    if (!peersConnus.contains(nouveauPeer)) {
                        peersConnus.add(nouveauPeer);
                        scheduler.execute(() -> mettreAJourCachePeerSecurise(nouveauPeer));
                        out.println("OK PEER_ADDED");
                        System.out.println("Peer annoncé ajouté: " + nouveauPeer);
                    } else {
                        mettreAJourPeerExistant(nouveauPeer);
                        out.println("OK PEER_UPDATED");
                    }
                } else {
                    out.println("ERREUR: Limite de peers atteinte");
                }
            } else {
                out.println("ERREUR: Peer invalide");
            }
        } catch (Exception e) {
            System.err.println("Erreur lors d'ANNOUNCE: " + e.getMessage());
            out.println("ERREUR: Erreur interne");
        }
    }

    // Sérialisation sécurisée - réutilisation des méthodes existantes avec validation
    private byte[] serialiserListeMetadata(List<Metadata> metadatas) throws IOException {
        if (metadatas == null) metadatas = new ArrayList<>();
        
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            writeInt(bos, metadatas.size());
            
            for (Metadata meta : metadatas) {
                if (meta != null) {
                    byte[] metaData = serialiserMetadata(meta);
                    writeInt(bos, metaData.length);
                    bos.write(metaData);
                } else {
                    writeInt(bos, 0);
                }
            }
            
            return bos.toByteArray();
        }
    }

    private List<Metadata> deserialiserListeMetadataSecurise(byte[] data) throws IOException {
        if (data == null || data.length < 4) {
            return new ArrayList<>();
        }

        List<Metadata> metadatas = new ArrayList<>();
        
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            int count = readInt(bis);
            if (count < 0 || count > 1000) { // Limite sécurisée
                throw new IOException("Nombre de metadata invalide: " + count);
            }
            
            for (int i = 0; i < count; i++) {
                int metaDataLen = readInt(bis);
                
                if (metaDataLen == 0) continue;
                
                if (metaDataLen < 0 || metaDataLen > 10000) {
                    throw new IOException("Longueur de données metadata invalide: " + metaDataLen);
                }
                
                byte[] metaData = new byte[metaDataLen];
                if (bis.read(metaData) != metaDataLen) {
                    throw new IOException("Impossible de lire les données du metadata " + i);
                }
                
                try {
                    Metadata meta = deserialiserMetadata(metaData);
                    if (meta.getNom().length() <= 255 && meta.getTaille() >= 0) {
                        metadatas.add(meta);
                    }
                } catch (IOException e) {
                    // Ignorer ce metadata corrompu
                }
            }
        }
        
        return metadatas;
    }

    private byte[] serialiserMetadata(Metadata meta) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] nomBytes = meta.getNom().getBytes(StandardCharsets.UTF_8);
            writeInt(bos, nomBytes.length);
            bos.write(nomBytes);
            
            writeLong(bos, meta.getTaille());
            
            String checksum = meta.getChecksum() != null ? meta.getChecksum() : "";
            byte[] checksumBytes = checksum.getBytes(StandardCharsets.UTF_8);
            writeInt(bos, checksumBytes.length);
            bos.write(checksumBytes);
            
            return bos.toByteArray();
        }
    }

    private Metadata deserialiserMetadata(byte[] data) throws IOException {
        if (data == null || data.length < 16) {
            throw new IOException("Données de sérialisation metadata invalides");
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            int nomLen = readInt(bis);
            if (nomLen < 0 || nomLen > 255) {
                throw new IOException("Longueur de nom invalide: " + nomLen);
            }
            
            byte[] nomBytes = new byte[nomLen];
            if (bis.read(nomBytes) != nomLen) {
                throw new IOException("Impossible de lire le nom complet");
            }
            String nom = new String(nomBytes, StandardCharsets.UTF_8);
            
            long taille = readLong(bis);
            if (taille < 0) {
                throw new IOException("Taille invalide: " + taille);
            }
            
            int checksumLen = readInt(bis);
            if (checksumLen < 0 || checksumLen > 64) { // SHA-256 = 64 char hex
                throw new IOException("Longueur de checksum invalide: " + checksumLen);
            }
            
            byte[] checksumBytes = new byte[checksumLen];
            if (bis.read(checksumBytes) != checksumLen) {
                throw new IOException("Impossible de lire le checksum complet");
            }
            String checksum = new String(checksumBytes, StandardCharsets.UTF_8);
            
            return new Metadata(nom, taille, checksum);
        }
    }

    // Méthodes utilitaires pour la sérialisation
    private static void writeInt(OutputStream os, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        os.write(buffer.array());
    }
    
    private static int readInt(InputStream is) throws IOException {
        byte[] buffer = new byte[4];
        if (is.read(buffer) != 4) {
            throw new IOException("Impossible de lire un entier (4 bytes)");
        }
        return ByteBuffer.wrap(buffer).getInt();
    }
    
    private static void writeLong(OutputStream os, long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        os.write(buffer.array());
    }
    
    private static long readLong(InputStream is) throws IOException {
        byte[] buffer = new byte[8];
        if (is.read(buffer) != 8) {
            throw new IOException("Impossible de lire un long (8 bytes)");
        }
        return ByteBuffer.wrap(buffer).getLong();
    }

    // Interface publique - Getters et méthodes d'information
    public String getPseudo() { return pseudo; }
    public int getPort() { return portEcoute; }
    public List<PeerInfo> getPeersConnus() { return new ArrayList<>(peersConnus); }
    public File getDossierPartage() { return dossierPartage; }
    public boolean estActif() { return actif; }

    /**
     * Affichage sécurisé de l'état du réseau
     */
    public void afficherEtatReseau() {
        System.out.println("\n=== État du réseau P2P sécurisé ===");
        System.out.println("Peer: " + pseudo + " (port " + portEcoute + ")");
        System.out.println("Statut: " + (actif ? "ACTIF" : "INACTIF"));
        System.out.println("Peers connus: " + peersConnus.size() + "/" + MAX_PEERS);

        int peersActifs = 0;
        for (PeerInfo peer : peersConnus) {
            if (peer.estActif(PEER_TIMEOUT_MS)) {
                peersActifs++;
                String clePeer = peer.getAdresse() + ":" + peer.getPort();
                List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
                int nbFichiers = (fichiers != null) ? fichiers.size() : 0;
                long inactiviteMs = System.currentTimeMillis() - peer.getDernierePing();
                
                System.out.println("  ✓ " + peer + " (" + nbFichiers + " fichiers) [" + 
                                 (inactiviteMs / 1000) + "s]");
            }
        }

        if (peersConnus.size() > peersActifs) {
            System.out.println("  (" + (peersConnus.size() - peersActifs) + " peer(s) inactif(s))");
        }

        List<File> mesFichiers = fileManager.listerFichiers();
        int fichiersValides = 0;
        long tailleTotal = 0;
        
        for (File f : mesFichiers) {
            if (f.isFile() && f.length() <= MAX_FILE_SIZE) {
                fichiersValides++;
                tailleTotal += f.length();
            }
        }

        System.out.println("Mes fichiers: " + fichiersValides + " (" + formatTaille(tailleTotal) + ")");
        System.out.println("Blacklist: " + blacklistedIPs.size() + " IP(s)");
        System.out.println("=====================================\n");
    }

    /**
     * Affichage des statistiques détaillées
     */
    public void afficherStatistiquesSecurite() {
        System.out.println("\n=== Statistiques de sécurité ===");
        System.out.println("Connexions totales: " + totalConnections.get());
        System.out.println("Bytes transférés: " + formatTaille(totalBytesTransferred.get()));
        System.out.println("Téléchargements réussis: " + successfulDownloads.get());
        System.out.println("Téléchargements échoués: " + failedDownloads.get());
        
        double tauxReussite = 0;
        long total = successfulDownloads.get() + failedDownloads.get();
        if (total > 0) {
            tauxReussite = (successfulDownloads.get() * 100.0) / total;
        }
        System.out.println("Taux de réussite: " + String.format("%.1f%%", tauxReussite));
        
        System.out.println("IPs avec rate limit actif: " + rateLimitMap.size());
        System.out.println("IPs blacklistées: " + blacklistedIPs.size());
        
        Map<String, Object> cacheStats = fileManager.getStatistiquesCache();
        System.out.println("Cache checksums: " + cacheStats.get("taille_cache_checksums"));
        System.out.println("==============================\n");
    }

    private void afficherStatistiquesFin() {
        System.out.println("\n=== Statistiques finales ===");
        afficherStatistiquesSecurite();
    }

    private String formatTaille(long octets) {
        if (octets < 1024) return octets + " B";
        if (octets < 1024 * 1024) return String.format("%.1f KB", octets / 1024.0);
        if (octets < 1024 * 1024 * 1024) return String.format("%.1f MB", octets / (1024.0 * 1024));
        return String.format("%.1f GB", octets / (1024.0 * 1024 * 1024));
    }

    /**
     * Blackliste une IP (protection contre abus)
     */
    public void blacklisterIP(String ip) {
        if (ip != null && !ip.isEmpty()) {
            blacklistedIPs.add(ip);
            System.out.println("IP blacklistée: " + ip);
        }
    }

    /**
     * Retire une IP de la blacklist
     */
    public void retirerBlacklist(String ip) {
        if (blacklistedIPs.remove(ip)) {
            System.out.println("IP retirée de la blacklist: " + ip);
        }
    }

    /**
     * Méthode publique pour le téléchargement (interface simplifiée)
     */
    public boolean telechargerFichier(String nomFichier) {
        return telechargerFichierSecurise(nomFichier);
    }

    /**
     * Test de connectivité sécurisé
     */
    public void testerConnectivite() {
        System.out.println("\n=== Test de connectivité sécurisé ===");
        
        if (peersConnus.isEmpty()) {
            System.out.println("Aucun peer connu pour tester la connectivité");
            return;
        }
        
        int peersActifs = 0;
        int peersTotal = peersConnus.size();
        
        for (PeerInfo peer : new ArrayList<>(peersConnus)) {
            System.out.print("Test de " + peer + "... ");
            if (testerConnexionPeerSecurise(peer.getAdresse(), peer.getPort())) {
                System.out.println("✓ OK");
                peersActifs++;
                peer.updatePing();
            } else {
                System.out.println("✗ ECHEC");
            }
        }
        
        System.out.println("Résultat: " + peersActifs + "/" + peersTotal + " peers actifs");
        System.out.println("=====================================\n");
    }

    /**
     * Synchronisation manuelle sécurisée
     */
    public void synchroniserMaintenant() {
        System.out.println("Synchronisation sécurisée déclenchée...");
        synchroniserPeersSecurise();
        System.out.println("Synchronisation terminée. Peers actifs: " + 
                         peersConnus.stream().mapToInt(p -> p.estActif(PEER_TIMEOUT_MS) ? 1 : 0).sum() + 
                         "/" + peersConnus.size());
    }

    /**
     * Recherche de fichiers par terme
     */
    public List<String> rechercherFichiersParNom(String terme) {
        Set<String> fichiersCorrespondants = new HashSet<>();
        String termeSecurise = sanitizeInput(terme.toLowerCase(), 100);
        
        if (termeSecurise.isEmpty()) return new ArrayList<>();
        
        for (PeerInfo peer : peersConnus) {
            if (!peer.estActif(PEER_TIMEOUT_MS)) continue;
            
            String clePeer = peer.getAdresse() + ":" + peer.getPort();
            List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
            
            if (fichiers != null) {
                for (Metadata meta : fichiers) {
                    if (meta.getNom().toLowerCase().contains(termeSecurise)) {
                        fichiersCorrespondants.add(meta.getNom());
                    }
                }
            }
        }
        
        return new ArrayList<>(fichiersCorrespondants);
    }

    public FileManager getFileManager(){
        return this.fileManager;
    }
}