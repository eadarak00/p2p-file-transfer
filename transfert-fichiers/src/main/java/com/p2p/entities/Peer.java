// package com.p2p.entities;

// import com.p2p.manager.FileManager;
// import com.p2p.manager.Metadata;

// import java.io.*;
// import java.net.*;
// import java.nio.ByteBuffer;
// import java.nio.charset.StandardCharsets;
// import java.security.MessageDigest;
// import java.util.*;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.CopyOnWriteArrayList;
// import java.util.concurrent.Executors;
// import java.util.concurrent.ScheduledExecutorService;
// import java.util.concurrent.TimeUnit;

// /**
//  * Classe Peer améliorée avec sérialisation custom (sans dépendances externes)
//  */
// public class Peer {
//     private final String pseudo;
//     private final int portEcoute;
//     private final File dossierPartage;
//     private final FileManager fileManager;
//     private final Object fileLock = new Object();

//     // Liste des autres peers connus
//     private final List<PeerInfo> peersConnus = new CopyOnWriteArrayList<>();

//     // Serveur socket pour écouter les connexions entrantes
//     private ServerSocket serverSocket;
//     private volatile boolean actif = false;

//     // Cache des fichiers disponibles chez chaque peer
//     private final Map<String, List<Metadata>> cacheFichiersPeers = new ConcurrentHashMap<>();

//     // Scheduler pour les tâches périodiques
//     private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

//     // Timeout pour considérer qu'un peer est inactif (30 secondes)
//     private static final long PEER_TIMEOUT_MS = 30000;

//     // Mode debug - mettre à false pour une interface propre
//     private static final boolean DEBUG_MODE = false;

//     public Peer(String pseudo, int portEcoute, String dossierPartage) {
//         this.pseudo = pseudo;
//         this.portEcoute = portEcoute;
//         this.dossierPartage = new File(dossierPartage);

//         if (!this.dossierPartage.exists()) {
//             this.dossierPartage.mkdirs();
//         }

//         this.fileManager = new FileManager(this.dossierPartage.getPath());
//     }

//     /**
//      * Démarre le peer avec synchronisation automatique
//      */
//     public void demarrer() {
//         try {
//             serverSocket = new ServerSocket(portEcoute);
//             actif = true;

//             // Thread pour écouter les connexions entrantes
//             new Thread(this::ecouterConnexions).start();

//             System.out.println("Peer '" + pseudo + "' démarré sur le port " + portEcoute);

//             // Découverte initiale des peers (silencieuse)
//             scheduler.schedule(this::decouvriPeersSilencieux, 1, TimeUnit.SECONDS);

//             // Synchronisation périodique des peers (toutes les 10 secondes) - silencieuse
//             scheduler.scheduleAtFixedRate(this::synchroniserPeersSilencieux, 10, 10, TimeUnit.SECONDS);

//             // Nettoyage des peers inactifs (toutes les 30 secondes) - silencieux
//             scheduler.scheduleAtFixedRate(this::nettoyerPeersInactifsSilencieux, 30, 30, TimeUnit.SECONDS);

//             // Mise à jour du cache des fichiers (toutes les 20 secondes) - silencieuse
//             scheduler.scheduleAtFixedRate(this::mettreAJourTousLesCacheSilencieux, 20, 20, TimeUnit.SECONDS);

//         } catch (IOException e) {
//             System.err.println("Erreur lors du démarrage du peer: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     /**
//      * Arrête le peer et le scheduler
//      */
//     public void arreter() {
//         actif = false;
//         scheduler.shutdown();
//         try {
//             if (serverSocket != null && !serverSocket.isClosed()) {
//                 serverSocket.close();
//             }
//             System.out.println("Peer '" + pseudo + "' arrêté");
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//     }

//     /**
//      * Traite une requête d'un autre peer (version silencieuse)
//      */
//     private void traiterRequetePeer(Socket clientSocket) {
//         try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//                 InputStream socketIn = clientSocket.getInputStream();
//                 OutputStream socketOut = clientSocket.getOutputStream()) {

//             String commande = in.readLine();
//             if (commande == null)
//                 return;

//             // Seulement afficher en mode debug
//             if (DEBUG_MODE) {
//                 System.out.println("Requête reçue de " + clientSocket.getInetAddress() + ": " + commande);
//             }

//             String[] parts = commande.split(" ", 4);

//             switch (parts[0].toUpperCase()) {
//                 case "PING":
//                     // Répondre au ping avec nos infos
//                     out.println("PONG " + pseudo + " " + portEcoute);
//                     break;

//                 case "LIST":
//                     envoyerListeFichiersSilencieux(socketOut);
//                     break;

//                 case "GET":
//                     if (parts.length >= 2) {
//                         String nomFichier = parts[1];
//                         long offset = 0;
//                         if (parts.length == 3) {
//                             try {
//                                 offset = Long.parseLong(parts[2]);
//                             } catch (NumberFormatException e) {
//                                 offset = 0;
//                             }
//                         }
//                         envoyerFichierSilencieux(nomFichier, socketOut, out, offset);
//                     } else {
//                         out.println("ERREUR: commande GET invalide");
//                     }
//                     break;

//                 case "PEERS":
//                     envoyerListePeersSilencieux(socketOut);
//                     break;

//                 case "ANNOUNCE":
//                     if (parts.length >= 3) {
//                         String pseudoAnnonce = parts[1];
//                         int portAnnonce = Integer.parseInt(parts[2]);
//                         String adresseAnnonce = clientSocket.getInetAddress().getHostAddress();

//                         PeerInfo nouveauPeer = new PeerInfo(adresseAnnonce, portAnnonce, pseudoAnnonce);
//                         ajouterPeerSilencieux(nouveauPeer);
//                         out.println("OK PEER_ADDED");
//                     } else {
//                         out.println("ERREUR: commande ANNOUNCE invalide");
//                     }
//                     break;

//                 default:
//                     out.println("ERREUR: commande inconnue");
//             }

//         } catch (IOException e) {
//             if (DEBUG_MODE) {
//                 System.err.println("Erreur lors du traitement d'une requête: " + e.getMessage());
//             }
//         } finally {
//             try {
//                 clientSocket.close();
//             } catch (IOException ignored) {
//             }
//         }
//     }

//     /**
//      * Version silencieuse de la synchronisation des peers
//      */
//     private void synchroniserPeersSilencieux() {
//         if (!actif)
//             return;

//         Set<PeerInfo> nouveauxPeers = new HashSet<>();

//         // Annoncer notre présence à tous les peers connus (silencieusement)
//         for (PeerInfo peer : new ArrayList<>(peersConnus)) {
//             try {
//                 annoncerAuPeerSilencieux(peer);
//                 Set<PeerInfo> peersDuPeer = recupererPeersDuPeerSilencieux(peer);
//                 nouveauxPeers.addAll(peersDuPeer);
//             } catch (Exception e) {
//                 // Erreurs silencieuses
//             }
//         }

//         // Ajouter les nouveaux peers découverts (silencieusement)
//         for (PeerInfo nouveauPeer : nouveauxPeers) {
//             if (!nouveauPeer.getAdresse().equals("localhost") ||
//                     nouveauPeer.getPort() != portEcoute) {
//                 ajouterPeerSilencieux(nouveauPeer);
//             }
//         }
//     }

//     /**
//      * S'annonce auprès d'un peer spécifique (version silencieuse)
//      */
//     private void annoncerAuPeerSilencieux(PeerInfo peer) {
//         try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
//                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

//             socket.setSoTimeout(2000);
//             out.println("ANNOUNCE " + pseudo + " " + portEcoute);
//             String reponse = in.readLine();

//             if (reponse != null && reponse.startsWith("OK")) {
//                 peer.updatePing();
//             }

//         } catch (IOException e) {
//             // Échec silencieux
//         }
//     }

//     /**
//      * Récupère la liste des peers connus par un peer spécifique (version silencieuse)
//      */
//     private Set<PeerInfo> recupererPeersDuPeerSilencieux(PeerInfo peer) {
//         Set<PeerInfo> peersDistants = new HashSet<>();

//         try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
//                 OutputStream out = socket.getOutputStream()) {

//             socket.setSoTimeout(2000);
            
//             // Envoyer la commande PEERS
//             String commande = "PEERS\n";
//             out.write(commande.getBytes(StandardCharsets.UTF_8));
//             out.flush();

//             // Lire la réponse binaire
//             InputStream in = socket.getInputStream();
//             byte[] lengthBytes = new byte[4];
//             if (in.read(lengthBytes) == 4) {
//                 int length = ByteBuffer.wrap(lengthBytes).getInt();
//                 if (length > 0 && length < 100000) { // Limite de sécurité
//                     byte[] data = new byte[length];
//                     int totalRead = 0;
//                     while (totalRead < length) {
//                         int read = in.read(data, totalRead, length - totalRead);
//                         if (read == -1) break;
//                         totalRead += read;
//                     }
                    
//                     if (totalRead == length) {
//                         List<PeerInfo> peers = PeerInfo.deserialiserListe(data);
//                         peersDistants.addAll(peers);
//                     }
//                 }
//             }

//         } catch (IOException e) {
//             // Échec silencieux
//         }

//         return peersDistants;
//     }

//     /**
//      * Version silencieuse du nettoyage des peers inactifs
//      */
//     private void nettoyerPeersInactifsSilencieux() {
//         if (!actif)
//             return;

//         List<PeerInfo> peersASupprimer = new ArrayList<>();

//         for (PeerInfo peer : peersConnus) {
//             if (!peer.estActif(PEER_TIMEOUT_MS)) {
//                 if (!testerConnexionPeerSilencieux(peer.getAdresse(), peer.getPort())) {
//                     peersASupprimer.add(peer);
//                 } else {
//                     peer.updatePing();
//                 }
//             }
//         }

//         if (!peersASupprimer.isEmpty()) {
//             peersConnus.removeAll(peersASupprimer);

//             // Nettoyer le cache
//             for (PeerInfo peer : peersASupprimer) {
//                 String cle = peer.getAdresse() + ":" + peer.getPort();
//                 cacheFichiersPeers.remove(cle);
//             }
//         }
//     }

//     /**
//      * Version silencieuse de la mise à jour de tous les caches
//      */
//     private void mettreAJourTousLesCacheSilencieux() {
//         if (!actif)
//             return;

//         for (PeerInfo peer : new ArrayList<>(peersConnus)) {
//             mettreAJourCachePeerSilencieux(peer);
//         }
//     }

//     /**
//      * Version silencieuse de la mise à jour du cache d'un peer
//      */
//     private void mettreAJourCachePeerSilencieux(PeerInfo peer) {
//         try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
//                 OutputStream out = socket.getOutputStream();
//                 InputStream in = socket.getInputStream()) {

//             socket.setSoTimeout(3000);
            
//             // Envoyer la commande LIST
//             String commande = "LIST\n";
//             out.write(commande.getBytes(StandardCharsets.UTF_8));
//             out.flush();

//             // Lire la réponse binaire
//             byte[] lengthBytes = new byte[4];
//             if (in.read(lengthBytes) == 4) {
//                 int length = ByteBuffer.wrap(lengthBytes).getInt();
//                 if (length > 0 && length < 1000000) { // Limite de sécurité
//                     byte[] data = new byte[length];
//                     int totalRead = 0;
//                     while (totalRead < length) {
//                         int read = in.read(data, totalRead, length - totalRead);
//                         if (read == -1) break;
//                         totalRead += read;
//                     }
                    
//                     if (totalRead == length) {
//                         List<Metadata> fichiers = deserialiserListeMetadata(data);
//                         cacheFichiersPeers.put(peer.getAdresse() + ":" + peer.getPort(), fichiers);
//                         peer.updatePing();
//                     }
//                 }
//             }

//         } catch (IOException e) {
//             // Échec silencieux
//         }
//     }

//     /**
//      * Teste si un peer est actif (version silencieuse)
//      */
//     private boolean testerConnexionPeerSilencieux(String adresse, int port) {
//         try (Socket socket = new Socket()) {
//             socket.connect(new InetSocketAddress(adresse, port), 1000);

//             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

//             socket.setSoTimeout(1000);
//             out.println("PING");
//             String reponse = in.readLine();

//             return reponse != null && reponse.startsWith("PONG");

//         } catch (IOException e) {
//             return false;
//         }
//     }

//     /**
//      * Commande manuelle pour forcer la synchronisation (avec messages)
//      */
//     public void synchroniserMaintenant() {
//         System.out.println("Synchronisation manuelle déclenchée...");

//         Set<PeerInfo> nouveauxPeers = new HashSet<>();

//         for (PeerInfo peer : new ArrayList<>(peersConnus)) {
//             try {
//                 annoncerAuPeerSilencieux(peer);
//                 Set<PeerInfo> peersDuPeer = recupererPeersDuPeerSilencieux(peer);
//                 nouveauxPeers.addAll(peersDuPeer);
//             } catch (Exception e) {
//                 if (DEBUG_MODE) {
//                     System.err.println("Erreur lors de la sync avec " + peer + ": " + e.getMessage());
//                 }
//             }
//         }

//         int nouveauxAjoutes = 0;
//         for (PeerInfo nouveauPeer : nouveauxPeers) {
//             if (!nouveauPeer.getAdresse().equals("localhost") ||
//                     nouveauPeer.getPort() != portEcoute) {
//                 if (!peersConnus.contains(nouveauPeer)) {
//                     ajouterPeerSilencieux(nouveauPeer);
//                     nouveauxAjoutes++;
//                 }
//             }
//         }

//         System.out.println(
//                 "Synchronisation terminée. " + nouveauxAjoutes + " nouveaux peers. Total: " + peersConnus.size());
//     }

//     // Méthodes pour l'interface utilisateur (avec messages appropriés)
//     public List<PeerInfo> rechercherFichier(String nomFichier) {
//         List<PeerInfo> peersAvecFichier = new ArrayList<>();

//         for (PeerInfo peer : peersConnus) {
//             String clePeer = peer.getAdresse() + ":" + peer.getPort();
//             List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);

//             if (fichiers != null) {
//                 for (Metadata meta : fichiers) {
//                     if (meta.getNom().equals(nomFichier)) {
//                         peersAvecFichier.add(peer);
//                         break;
//                     }
//                 }
//             }
//         }

//         return peersAvecFichier;
//     }

//     public boolean telechargerFichier(String nomFichier) {
//         List<PeerInfo> peersAvecFichier = rechercherFichier(nomFichier);

//         if (peersAvecFichier.isEmpty()) {
//             System.out.println("Fichier '" + nomFichier + "' introuvable sur le réseau");
//             return false;
//         }

//         System.out.println("Fichier trouvé chez " + peersAvecFichier.size() + " peer(s)");

//         for (PeerInfo peer : peersAvecFichier) {
//             System.out.println("Tentative de téléchargement depuis " + peer);
//             if (telechargerDepuisPeer(peer, nomFichier)) {
//                 return true;
//             }
//         }

//         System.err.println("Échec du téléchargement depuis tous les peers");
//         return false;
//     }

//     public boolean telechargerDepuisPeer(PeerInfo peer, String nomFichier) {
//         File fichierLocal = new File(dossierPartage, nomFichier);
//         if (fichierLocal.exists()) {
//             String nomLocal = trouverNomCopie(fichierLocal);
//             System.out.println("Fichier existant, sauvegarde sous: " + nomLocal);
//             fichierLocal = new File(dossierPartage, nomLocal);
//         }

//         try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
//                 InputStream socketIn = socket.getInputStream();
//                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

//             out.println("GET " + nomFichier + " 0");

//             String checksumServeur = lireLigne(socketIn);
//             if (checksumServeur == null || checksumServeur.startsWith("ERREUR")) {
//                 System.err.println("Erreur lors de la demande du fichier: " + checksumServeur);
//                 return false;
//             }

//             String tailleStr = lireLigne(socketIn);
//             if (tailleStr == null)
//                 return false;

//             long tailleFichier = Long.parseLong(tailleStr);

//             try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fichierLocal))) {
//                 byte[] buffer = new byte[8192];
//                 long reste = tailleFichier;
//                 int lu;

//                 while (reste > 0 && (lu = socketIn.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
//                     bos.write(buffer, 0, lu);
//                     reste -= lu;
//                 }
//             }

//             try {
//                 String checksumLocal = fileManager.calculerChecksum(fichierLocal);
//                 if (checksumLocal.equals(checksumServeur)) {
//                     System.out.println("Fichier téléchargé avec succès: " + fichierLocal.getName());
//                     return true;
//                 } else {
//                     System.err.println("Erreur checksum pour " + nomFichier);
//                     fichierLocal.delete();
//                     return false;
//                 }
//             } catch (Exception ex) {
//                 ex.printStackTrace();
//                 return false;
//             }

//         } catch (IOException e) {
//             System.err.println("Erreur lors du téléchargement depuis " + peer + ": " + e.getMessage());
//             return false;
//         }
//     }

//     /**
//      * Version silencieuse de l'envoi de la liste des fichiers (binaire)
//      */
//     private void envoyerListeFichiersSilencieux(OutputStream socketOut) {
//         synchronized (fileLock) {
//             try {
//                 List<File> fichiers = fileManager.listerFichiers();
//                 List<Metadata> meta = new ArrayList<>();
                
//                 for (File f : fichiers) {
//                     if (f.isFile()) {
//                         try {
//                             Metadata m = new Metadata(f.getName(), f.length(), fileManager.calculerChecksum(f));
//                             meta.add(m);
//                         } catch (Exception e) {
//                             // Ignorer ce fichier
//                         }
//                     }
//                 }

//                 // Sérialiser en binaire
//                 byte[] data = serialiserListeMetadata(meta);
                
//                 // Envoyer la longueur puis les données
//                 ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
//                 lengthBuffer.putInt(data.length);
//                 socketOut.write(lengthBuffer.array());
//                 socketOut.write(data);
//                 socketOut.flush();
                
//             } catch (Exception e) {
//                 if (DEBUG_MODE) {
//                     e.printStackTrace();
//                 }
//                 try {
//                     // Envoyer longueur 0 en cas d'erreur
//                     ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
//                     lengthBuffer.putInt(0);
//                     socketOut.write(lengthBuffer.array());
//                     socketOut.flush();
//                 } catch (IOException ignored) {
//                 }
//             }
//         }
//     }

//     /**
//      * Version silencieuse de l'envoi de fichier
//      */
//     private void envoyerFichierSilencieux(String nomFichier, OutputStream socketOut, PrintWriter out, long offset) {
//         synchronized (fileLock) {
//             try {
//                 File fichier = new File(dossierPartage, nomFichier);
//                 if (!fichier.exists() || !fichier.isFile()) {
//                     out.println("ERREUR: fichier introuvable");
//                     return;
//                 }

//                 String checksum = fileManager.calculerChecksum(fichier);
//                 long taille = fichier.length();

//                 out.println(checksum);
//                 out.println(taille);
//                 out.flush();

//                 try (RandomAccessFile raf = new RandomAccessFile(fichier, "r")) {
//                     if (offset > 0) {
//                         raf.seek(offset);
//                     }

//                     byte[] buffer = new byte[8192];
//                     long reste = taille - offset;
//                     int read;

//                     while (reste > 0 && (read = raf.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
//                         socketOut.write(buffer, 0, read);
//                         reste -= read;
//                     }
//                 }

//                 socketOut.flush();
//                 // Seulement afficher en mode debug
//                 if (DEBUG_MODE) {
//                     System.out.println("Fichier envoyé: " + nomFichier + " (offset: " + offset + ")");
//                 }

//             } catch (Exception e) {
//                 if (DEBUG_MODE) {
//                     e.printStackTrace();
//                 }
//                 out.println("ERREUR lors de l'envoi du fichier");
//             }
//         }
//     }

//     /**
//      * Écoute les connexions entrantes (mode serveur)
//      */
//     private void ecouterConnexions() {
//         while (actif && !serverSocket.isClosed()) {
//             try {
//                 Socket clientSocket = serverSocket.accept();
//                 new Thread(() -> traiterRequetePeer(clientSocket)).start();
//             } catch (IOException e) {
//                 if (actif && DEBUG_MODE) {
//                     System.err.println("Erreur lors de l'acceptation d'une connexion: " + e.getMessage());
//                 }
//             }
//         }
//     }

//     /**
//      * Version silencieuse de l'envoi de la liste des peers (binaire)
//      */
//     private void envoyerListePeersSilencieux(OutputStream socketOut) {
//         try {
//             List<PeerInfo> peersActifs = new ArrayList<>();
            
//             for (PeerInfo p : peersConnus) {
//                 if (p.estActif(PEER_TIMEOUT_MS)) {
//                     // Exclure le peer courant
//                     if (!((p.getAdresse().equals("localhost") || p.getAdresse().equals("127.0.0.1")) 
//                           && p.getPort() == this.portEcoute)) {
//                         // Exclure les peers sans pseudo
//                         if (p.getPseudo() != null && !p.getPseudo().trim().isEmpty()) {
//                             peersActifs.add(p);
//                         }
//                     }
//                 }
//             }

//             // Sérialiser la liste
//             byte[] data = PeerInfo.serialiserListe(peersActifs);
            
//             // Envoyer la longueur puis les données
//             ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
//             lengthBuffer.putInt(data.length);
//             socketOut.write(lengthBuffer.array());
//             socketOut.write(data);
//             socketOut.flush();
            
//         } catch (Exception e) {
//             if (DEBUG_MODE) {
//                 e.printStackTrace();
//             }
//             try {
//                 // Envoyer longueur 0 en cas d'erreur
//                 ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
//                 lengthBuffer.putInt(0);
//                 socketOut.write(lengthBuffer.array());
//                 socketOut.flush();
//             } catch (IOException ignored) {
//             }
//         }
//     }

//     /**
//      * Version améliorée pour ajouter un peer
//      */
//     private boolean ajouterPeerSilencieux(PeerInfo peerInfo) {
//         // Ne pas s'ajouter soi-même
//         if ((peerInfo.getAdresse().equals("localhost") || peerInfo.getAdresse().equals("127.0.0.1")) 
//             && peerInfo.getPort() == this.portEcoute) {
//             return false;
//         }
        
//         if (!peersConnus.contains(peerInfo)) {
//             peersConnus.add(peerInfo);

//             // Récupérer la liste de ses fichiers (silencieusement)
//             scheduler.execute(() -> mettreAJourCachePeerSilencieux(peerInfo));
//             return true;
//         } else {
//             // Mettre à jour le ping du peer existant
//             for (PeerInfo p : peersConnus) {
//                 if (p.equals(peerInfo)) {
//                     p.updatePing();
//                     if (peerInfo.getPseudo() != null && !peerInfo.getPseudo().isEmpty()) {
//                         p.setPseudo(peerInfo.getPseudo());
//                     }
//                     break;
//                 }
//             }
//             return false;
//         }
//     }

//     /**
//      * Version publique pour ajouter un peer manuellement
//      */
//     public void ajouterPeer(PeerInfo peerInfo) {
//         // Ne pas s'ajouter soi-même
//         if ((peerInfo.getAdresse().equals("localhost") || peerInfo.getAdresse().equals("127.0.0.1")) 
//             && peerInfo.getPort() == this.portEcoute) {
//             System.out.println("Impossible de s'ajouter soi-même comme peer");
//             return;
//         }
        
//         if (!peersConnus.contains(peerInfo)) {
//             peersConnus.add(peerInfo);
//             System.out.println("Nouveau peer ajouté: " + peerInfo);

//             // Récupérer immédiatement la liste de ses fichiers
//             scheduler.execute(() -> mettreAJourCachePeerSilencieux(peerInfo));
//         } else {
//             // Mettre à jour le ping du peer existant
//             for (PeerInfo p : peersConnus) {
//                 if (p.equals(peerInfo)) {
//                     p.updatePing();
//                     if (peerInfo.getPseudo() != null && !peerInfo.getPseudo().isEmpty()) {
//                         p.setPseudo(peerInfo.getPseudo());
//                     }
//                     break;
//                 }
//             }
//             System.out.println("Peer mis à jour: " + peerInfo);
//         }
//     }

//     /**
//      * Découvre automatiquement les peers sur le réseau local
//      */
//     private void decouvriPeersSilencieux() {
//         try {
//             int peersInitiaux = peersConnus.size();

//             for (int port = 8000; port <= 8100; port++) {
//                 if (port == portEcoute) continue; // Ne pas se tester soi-même

//                 try {
//                     String adresse = "localhost";
//                     if (testerConnexionPeerSilencieux(adresse, port)) {
//                         // Ne pas ajouter si c'est nous-même (double vérification)
//                         if (port != this.portEcoute) {
//                             ajouterPeerSilencieux(new PeerInfo(adresse, port, ""));
//                         }
//                     }
//                 } catch (Exception e) {
//                     // Pas de peer sur ce port
//                 }

//                 Thread.sleep(10);
//             }

//             int nouveauxPeers = peersConnus.size() - peersInitiaux;
//             if (nouveauxPeers > 0) {
//                 System.out.println("Réseau P2P: " + nouveauxPeers + " peer(s) découvert(s)");
//             }

//         } catch (Exception e) {
//             if (DEBUG_MODE) {
//                 System.err.println("Erreur lors de la découverte: " + e.getMessage());
//             }
//         }
//     }

//     // Méthodes utilitaires
//     private String lireLigne(InputStream in) throws IOException {
//         ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//         int b;
//         while ((b = in.read()) != -1) {
//             if (b == '\n')
//                 break;
//             buffer.write(b);
//         }
//         return buffer.toString("UTF-8").trim();
//     }

//     private String trouverNomCopie(File fichier) {
//         String nom = fichier.getName();
//         int pointIndex = nom.lastIndexOf('.');
//         String base = (pointIndex == -1) ? nom : nom.substring(0, pointIndex);
//         String extension = (pointIndex == -1) ? "" : nom.substring(pointIndex);
//         int compteur = 1;

//         String nouveauNom;
//         do {
//             nouveauNom = base + "(" + compteur + ")" + extension;
//             compteur++;
//         } while (new File(dossierPartage, nouveauNom).exists());

//         return nouveauNom;
//     }

//     /**
//      * Sérialise une liste de Metadata en tableau de bytes
//      * Format: [COUNT(4)][META1_LEN(4)][META1_DATA][META2_LEN(4)][META2_DATA]...
//      */
//     private byte[] serialiserListeMetadata(List<Metadata> metadatas) throws IOException {
//         if (metadatas == null) {
//             metadatas = new ArrayList<>();
//         }

//         try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
//             // Nombre de metadata
//             writeInt(bos, metadatas.size());
            
//             // Chaque metadata
//             for (Metadata meta : metadatas) {
//                 if (meta != null) {
//                     byte[] metaData = serialiserMetadata(meta);
//                     writeInt(bos, metaData.length);
//                     bos.write(metaData);
//                 } else {
//                     // Metadata null - écrire longueur 0
//                     writeInt(bos, 0);
//                 }
//             }
            
//             return bos.toByteArray();
//         }
//     }

//     /**
//      * Désérialise une liste de Metadata depuis un tableau de bytes
//      */
//     private List<Metadata> deserialiserListeMetadata(byte[] data) throws IOException {
//         if (data == null || data.length < 4) {
//             return new ArrayList<>();
//         }

//         List<Metadata> metadatas = new ArrayList<>();
        
//         try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
//             // Nombre de metadata
//             int count = readInt(bis);
//             if (count < 0 || count > 10000) { // Limite raisonnable
//                 throw new IOException("Nombre de metadata invalide: " + count);
//             }
            
//             // Lire chaque metadata
//             for (int i = 0; i < count; i++) {
//                 int metaDataLen = readInt(bis);
                
//                 if (metaDataLen == 0) {
//                     // Metadata null - ignorer
//                     continue;
//                 }
                
//                 if (metaDataLen < 0 || metaDataLen > 10000) { // Limite raisonnable
//                     throw new IOException("Longueur de données metadata invalide: " + metaDataLen);
//                 }
                
//                 byte[] metaData = new byte[metaDataLen];
//                 if (bis.read(metaData) != metaDataLen) {
//                     throw new IOException("Impossible de lire les données du metadata " + i);
//                 }
                
//                 try {
//                     Metadata meta = deserialiserMetadata(metaData);
//                     metadatas.add(meta);
//                 } catch (IOException e) {
//                     // Ignorer ce metadata corrompu et continuer
//                     if (DEBUG_MODE) {
//                         System.err.println("Metadata " + i + " corrompu, ignoré: " + e.getMessage());
//                     }
//                 }
//             }
//         }
        
//         return metadatas;
//     }

//     /**
//      * Sérialise un Metadata en tableau de bytes
//      * Format: [NOM_LEN(4)][NOM][TAILLE(8)][CHECKSUM_LEN(4)][CHECKSUM]
//      */
//     private byte[] serialiserMetadata(Metadata meta) throws IOException {
//         try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
//             // Nom
//             byte[] nomBytes = meta.getNom().getBytes(StandardCharsets.UTF_8);
//             writeInt(bos, nomBytes.length);
//             bos.write(nomBytes);
            
//             // Taille
//             writeLong(bos, meta.getTaille());
            
//             // Checksum
//             String checksum = meta.getChecksum() != null ? meta.getChecksum() : "";
//             byte[] checksumBytes = checksum.getBytes(StandardCharsets.UTF_8);
//             writeInt(bos, checksumBytes.length);
//             bos.write(checksumBytes);
            
//             return bos.toByteArray();
//         }
//     }

//     /**
//      * Désérialise un Metadata depuis un tableau de bytes
//      */
//     private Metadata deserialiserMetadata(byte[] data) throws IOException {
//         if (data == null || data.length < 16) { // Minimum: 4+0+8+4+0
//             throw new IOException("Données de sérialisation metadata invalides");
//         }

//         try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
//             // Nom
//             int nomLen = readInt(bis);
//             if (nomLen < 0 || nomLen > 1000) { // Limite raisonnable
//                 throw new IOException("Longueur de nom invalide: " + nomLen);
//             }
//             byte[] nomBytes = new byte[nomLen];
//             if (bis.read(nomBytes) != nomLen) {
//                 throw new IOException("Impossible de lire le nom complet");
//             }
//             String nom = new String(nomBytes, StandardCharsets.UTF_8);
            
//             // Taille
//             long taille = readLong(bis);
//             if (taille < 0) {
//                 throw new IOException("Taille invalide: " + taille);
//             }
            
//             // Checksum
//             int checksumLen = readInt(bis);
//             if (checksumLen < 0 || checksumLen > 1000) { // Limite raisonnable
//                 throw new IOException("Longueur de checksum invalide: " + checksumLen);
//             }
//             byte[] checksumBytes = new byte[checksumLen];
//             if (bis.read(checksumBytes) != checksumLen) {
//                 throw new IOException("Impossible de lire le checksum complet");
//             }
//             String checksum = new String(checksumBytes, StandardCharsets.UTF_8);
            
//             return new Metadata(nom, taille, checksum);
//         }
//     }

//     /**
//      * Méthodes utilitaires pour la sérialisation binaire
//      */
//     private static void writeInt(OutputStream os, int value) throws IOException {
//         ByteBuffer buffer = ByteBuffer.allocate(4);
//         buffer.putInt(value);
//         os.write(buffer.array());
//     }
    
//     private static int readInt(InputStream is) throws IOException {
//         byte[] buffer = new byte[4];
//         if (is.read(buffer) != 4) {
//             throw new IOException("Impossible de lire un entier (4 bytes)");
//         }
//         return ByteBuffer.wrap(buffer).getInt();
//     }
    
//     private static void writeLong(OutputStream os, long value) throws IOException {
//         ByteBuffer buffer = ByteBuffer.allocate(8);
//         buffer.putLong(value);
//         os.write(buffer.array());
//     }
    
//     private static long readLong(InputStream is) throws IOException {
//         byte[] buffer = new byte[8];
//         if (is.read(buffer) != 8) {
//             throw new IOException("Impossible de lire un long (8 bytes)");
//         }
//         return ByteBuffer.wrap(buffer).getLong();
//     }

//     // Getters
//     public String getPseudo() {
//         return pseudo;
//     }

//     public int getPort() {
//         return portEcoute;
//     }

//     public List<PeerInfo> getPeersConnus() {
//         return new ArrayList<>(peersConnus);
//     }

//     public File getDossierPartage() {
//         return dossierPartage;
//     }

//     /**
//      * Affiche l'état actuel du réseau P2P
//      */
//     public void afficherEtatReseau() {
//         System.out.println("\n=== État du réseau P2P ===");
//         System.out.println("Peer: " + pseudo + " (port " + portEcoute + ")");
//         System.out.println("Peers connus: " + peersConnus.size());

//         for (PeerInfo peer : peersConnus) {
//             String clePeer = peer.getAdresse() + ":" + peer.getPort();
//             List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
//             int nbFichiers = (fichiers != null) ? fichiers.size() : 0;
//             long inactiviteMs = System.currentTimeMillis() - peer.getDernierePing();
//             String statut = peer.estActif(PEER_TIMEOUT_MS) ? "ACTIF" : "INACTIF";
//             System.out.println(
//                     "  - " + peer + " (" + nbFichiers + " fichiers) [" + statut + " - " + (inactiviteMs / 1000) + "s]");
//         }

//         List<File> mesFichiers = fileManager.listerFichiers();
//         System.out.println("Mes fichiers partagés: " + mesFichiers.size());
//         for (File f : mesFichiers) {
//             if (f.isFile()) {
//                 System.out.println("  - " + f.getName() + " (" + f.length() + " octets)");
//             }
//         }
//         System.out.println("========================\n");
//     }

//     /**
//      * Affiche tous les fichiers disponibles sur le réseau
//      */
//     public void afficherFichiersDisponibles() {
//         System.out.println("\n=== Fichiers disponibles sur le réseau ===");
        
//         Map<String, List<PeerInfo>> fichiersParNom = new HashMap<>();
        
//         // Collecter tous les fichiers de tous les peers
//         for (PeerInfo peer : peersConnus) {
//             if (!peer.estActif(PEER_TIMEOUT_MS)) continue;
            
//             String clePeer = peer.getAdresse() + ":" + peer.getPort();
//             List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
            
//             if (fichiers != null) {
//                 for (Metadata meta : fichiers) {
//                     fichiersParNom.computeIfAbsent(meta.getNom(), k -> new ArrayList<>()).add(peer);
//                 }
//             }
//         }
        
//         if (fichiersParNom.isEmpty()) {
//             System.out.println("Aucun fichier disponible sur le réseau");
//         } else {
//             for (Map.Entry<String, List<PeerInfo>> entry : fichiersParNom.entrySet()) {
//                 String nomFichier = entry.getKey();
//                 List<PeerInfo> peers = entry.getValue();
//                 System.out.print("  - " + nomFichier + " (disponible chez " + peers.size() + " peer(s): ");
//                 for (int i = 0; i < peers.size(); i++) {
//                     if (i > 0) System.out.print(", ");
//                     System.out.print(peers.get(i).getPseudo());
//                 }
//                 System.out.println(")");
//             }
//         }
//         System.out.println("==========================================\n");
//     }

//     /**
//      * Recherche tous les fichiers contenant un terme dans leur nom
//      */
//     public List<String> rechercherFichiersParNom(String terme) {
//         Set<String> fichiersCorrespondants = new HashSet<>();
//         String termeMinuscule = terme.toLowerCase();
        
//         for (PeerInfo peer : peersConnus) {
//             if (!peer.estActif(PEER_TIMEOUT_MS)) continue;
            
//             String clePeer = peer.getAdresse() + ":" + peer.getPort();
//             List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
            
//             if (fichiers != null) {
//                 for (Metadata meta : fichiers) {
//                     if (meta.getNom().toLowerCase().contains(termeMinuscule)) {
//                         fichiersCorrespondants.add(meta.getNom());
//                     }
//                 }
//             }
//         }
        
//         return new ArrayList<>(fichiersCorrespondants);
//     }

//     /**
//      * Obtient des statistiques sur le réseau
//      */
//     public void afficherStatistiques() {
//         System.out.println("\n=== Statistiques du réseau P2P ===");
        
//         int peersActifs = 0;
//         int totalFichiers = 0;
//         long totalTaille = 0;
        
//         for (PeerInfo peer : peersConnus) {
//             if (peer.estActif(PEER_TIMEOUT_MS)) {
//                 peersActifs++;
//                 String clePeer = peer.getAdresse() + ":" + peer.getPort();
//                 List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
                
//                 if (fichiers != null) {
//                     totalFichiers += fichiers.size();
//                     for (Metadata meta : fichiers) {
//                         totalTaille += meta.getTaille();
//                     }
//                 }
//             }
//         }
        
//         // Mes propres fichiers
//         List<File> mesFichiers = fileManager.listerFichiers();
//         int mesFichiersCount = 0;
//         long mesTailleTotal = 0;
//         for (File f : mesFichiers) {
//             if (f.isFile()) {
//                 mesFichiersCount++;
//                 mesTailleTotal += f.length();
//             }
//         }
        
//         System.out.println("Peers actifs: " + peersActifs + "/" + peersConnus.size());
//         System.out.println("Mes fichiers: " + mesFichiersCount + " (" + formatTaille(mesTailleTotal) + ")");
//         System.out.println("Fichiers réseau: " + totalFichiers + " (" + formatTaille(totalTaille) + ")");
//         System.out.println("===================================\n");
//     }
    
//     /**
//      * Formate une taille en octets de manière lisible
//      */
//     private String formatTaille(long octets) {
//         if (octets < 1024) return octets + " B";
//         if (octets < 1024 * 1024) return String.format("%.1f KB", octets / 1024.0);
//         if (octets < 1024 * 1024 * 1024) return String.format("%.1f MB", octets / (1024.0 * 1024));
//         return String.format("%.1f GB", octets / (1024.0 * 1024 * 1024));
//     }

//     /**
//      * Teste la connectivité avec tous les peers connus
//      */
//     public void testerConnectivite() {
//         System.out.println("\n=== Test de connectivité ===");
        
//         if (peersConnus.isEmpty()) {
//             System.out.println("Aucun peer connu pour tester la connectivité");
//             return;
//         }
        
//         int peersActifs = 0;
//         for (PeerInfo peer : peersConnus) {
//             System.out.print("Test de " + peer + "... ");
//             if (testerConnexionPeerSilencieux(peer.getAdresse(), peer.getPort())) {
//                 System.out.println("OK");
//                 peersActifs++;
//                 peer.updatePing();
//             } else {
//                 System.out.println("ECHEC");
//             }
//         }
        
//         System.out.println("Résultat: " + peersActifs + "/" + peersConnus.size() + " peers actifs");
//         System.out.println("============================\n");
//     }

//     /**
//      * Version améliorée du téléchargement avec vérification d'intégrité renforcée
//      */
//     public boolean telechargerDepuisPeerSecurise(PeerInfo peer, String nomFichier) {
//         File fichierLocal = new File(dossierPartage, nomFichier);
//         if (fichierLocal.exists()) {
//             String nomLocal = trouverNomCopie(fichierLocal);
//             System.out.println("Fichier existant, sauvegarde sous: " + nomLocal);
//             fichierLocal = new File(dossierPartage, nomLocal);
//         }

//         try (Socket socket = new Socket(peer.getAdresse(), peer.getPort());
//              InputStream socketIn = socket.getInputStream();
//              PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

//             socket.setSoTimeout(30000); // Timeout plus long pour gros fichiers
//             out.println("GET " + nomFichier + " 0");

//             String checksumServeur = lireLigne(socketIn);
//             if (checksumServeur == null || checksumServeur.startsWith("ERREUR")) {
//                 System.err.println("Erreur lors de la demande du fichier: " + checksumServeur);
//                 return false;
//             }

//             String tailleStr = lireLigne(socketIn);
//             if (tailleStr == null) return false;

//             long tailleFichier = Long.parseLong(tailleStr);
//             if (tailleFichier <= 0 || tailleFichier > 10L * 1024 * 1024 * 1024) { // Max 10GB
//                 System.err.println("Taille de fichier invalide: " + tailleFichier);
//                 return false;
//             }

//             // Téléchargement avec vérification progressive
//             MessageDigest digest = MessageDigest.getInstance("SHA-256");
            
//             try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fichierLocal))) {
//                 byte[] buffer = new byte[8192];
//                 long reste = tailleFichier;
//                 long totalLu = 0;
//                 int lu;

//                 while (reste > 0 && (lu = socketIn.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
//                     bos.write(buffer, 0, lu);
//                     digest.update(buffer, 0, lu);
//                     reste -= lu;
//                     totalLu += lu;
                    
//                     // Afficher progression pour gros fichiers
//                     if (tailleFichier > 1024 * 1024 && totalLu % (1024 * 1024) == 0) {
//                         int progression = (int) ((totalLu * 100) / tailleFichier);
//                         System.out.print("\rTéléchargement: " + progression + "%");
//                     }
//                 }
                
//                 if (tailleFichier > 1024 * 1024) {
//                     System.out.println(); // Nouvelle ligne après progression
//                 }
//             }

//             // Vérification finale du checksum
//             byte[] hash = digest.digest();
//             StringBuilder checksumCalcule = new StringBuilder();
//             for (byte b : hash) {
//                 String hex = Integer.toHexString(0xff & b);
//                 if (hex.length() == 1) checksumCalcule.append('0');
//                 checksumCalcule.append(hex);
//             }

//             if (checksumCalcule.toString().equals(checksumServeur)) {
//                 System.out.println("Fichier téléchargé avec succès: " + fichierLocal.getName());
//                 return true;
//             } else {
//                 System.err.println("Erreur checksum pour " + nomFichier);
//                 System.err.println("Attendu: " + checksumServeur);
//                 System.err.println("Calculé: " + checksumCalcule.toString());
//                 fichierLocal.delete();
//                 return false;
//             }

//         } catch (Exception e) {
//             System.err.println("Erreur lors du téléchargement depuis " + peer + ": " + e.getMessage());
//             if (fichierLocal.exists()) {
//                 fichierLocal.delete();
//             }
//             return false;
//         }
//     }

//     /**
//      * Validation des données reçues avant traitement
//      */
//     private boolean validerDonneesRecues(byte[] donnees, int longueurAttendue) {
//         if (donnees == null) return false;
//         if (donnees.length != longueurAttendue) return false;
        
//         // Vérifications basiques de sanité
//         if (longueurAttendue > 100 * 1024 * 1024) { // 100MB max pour éviter les attaques mémoire
//             return false;
//         }
        
//         return true;
//     }

// }

package com.p2p.entities;

import com.p2p.manager.FileManager;
import com.p2p.manager.Metadata;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Classe Peer refactorisée avec architecture moderne et gestion d'erreurs robuste
 */
public class Peer {
    private final String pseudo;
    private final int portEcoute;
    private final File dossierPartage;
    private final FileManager fileManager;
    private final Object fileLock = new Object();

    // Gestion des connexions réseau
    private final List<PeerInfo> peersConnus = new CopyOnWriteArrayList<>();
    private final Map<String, List<Metadata>> cacheFichiersPeers = new ConcurrentHashMap<>();
    private ServerSocket serverSocket;
    private volatile boolean actif = false;

    // Pool de threads pour la gestion des tâches
    private final ExecutorService executorPrincipal = Executors.newCachedThreadPool();
    private final ScheduledExecutorService schedulerMaintenance = Executors.newScheduledThreadPool(3);

    // Configuration
    private static final long PEER_TIMEOUT_MS = 30000;
    private static final int SOCKET_TIMEOUT_MS = 5000;
    private static final int BUFFER_SIZE = 8192;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final boolean DEBUG_MODE = false;

    public Peer(String pseudo, int portEcoute, String dossierPartage) {
        this.pseudo = validateString(pseudo, "Pseudo");
        this.portEcoute = validatePort(portEcoute);
        this.dossierPartage = new File(dossierPartage);
        
        if (!this.dossierPartage.exists()) {
            this.dossierPartage.mkdirs();
        }
        
        this.fileManager = new FileManager(this.dossierPartage.getPath());
    }

    /**
     * Démarre le peer avec tous ses services
     */
    public CompletableFuture<Void> demarrer() {
        return CompletableFuture.runAsync(() -> {
            try {
                serverSocket = new ServerSocket(portEcoute);
                actif = true;

                // Démarrer le serveur d'écoute
                executorPrincipal.submit(this::ecouterConnexions);

                // Programmer les tâches de maintenance
                programmerTachesMaintenance();

                logInfo("Peer '" + pseudo + "' démarré sur le port " + portEcoute);

                // Découverte initiale différée
                schedulerMaintenance.schedule(this::decouvriePeers, 2, TimeUnit.SECONDS);

            } catch (IOException e) {
                logError("Erreur lors du démarrage du peer", e);
                throw new RuntimeException(e);
            }
        }, executorPrincipal);
    }

    /**
     * Arrête proprement le peer et tous ses services
     */
    public void arreter() {
        logInfo("Arrêt du peer '" + pseudo + "'...");
        
        actif = false;
        
        // Arrêter les services dans l'ordre
        shutdownExecutor(schedulerMaintenance, "Scheduler de maintenance", 2);
        shutdownExecutor(executorPrincipal, "Executor principal", 5);
        
        // Fermer le socket serveur
        closeResource(serverSocket, "Socket serveur");
        
        logInfo("Peer '" + pseudo + "' arrêté");
    }

    /**
     * Programme les tâches de maintenance périodiques
     */
    private void programmerTachesMaintenance() {
        // Synchronisation des peers (toutes les 15 secondes)
        schedulerMaintenance.scheduleAtFixedRate(
            this::synchroniserPeersSilencieux, 15, 15, TimeUnit.SECONDS);
        
        // Nettoyage des peers inactifs (toutes les 45 secondes)
        schedulerMaintenance.scheduleAtFixedRate(
            this::nettoyerPeersInactifs, 45, 45, TimeUnit.SECONDS);
        
        // Mise à jour du cache des fichiers (toutes les 30 secondes)
        schedulerMaintenance.scheduleAtFixedRate(
            this::mettreAJourCacheComplet, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Écoute les connexions entrantes
     */
    private void ecouterConnexions() {
        logInfo("Serveur d'écoute démarré");
        
        while (actif && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                executorPrincipal.submit(() -> traiterRequetePeer(clientSocket));
            } catch (IOException e) {
                if (actif) {
                    logError("Erreur lors de l'acceptation d'une connexion", e);
                }
            }
        }
    }

    /**
     * Traite une requête d'un peer distant
     */

    private void traiterRequetePeer(Socket clientSocket) {
    try {
        clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);

        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
             InputStream socketIn = clientSocket.getInputStream();
             OutputStream socketOut = clientSocket.getOutputStream()) {

            String commande = in.readLine();
            if (commande == null) return;

            logDebug("Requête reçue: " + commande);

            String[] parts = commande.split(" ", 4);
            String cmd = parts[0].toUpperCase();

            switch (cmd) {
                case "PING": handlePing(out); break;
                case "LIST": handleListFiles(socketOut); break;
                case "GET": handleGetFile(parts, socketOut, out); break;
                case "PEERS": handleGetPeers(socketOut); break;
                case "ANNOUNCE": handleAnnounce(parts, clientSocket, out); break;
                default: out.println("ERREUR: commande inconnue");
            }
        }

    } catch (Exception e) {
        logError("Erreur lors du traitement d'une requête", e);
    }
}

    // private void traiterRequetePeer(Socket clientSocket) {
    //     try (clientSocket;
    //          BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    //          PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
    //          InputStream socketIn = clientSocket.getInputStream();
    //          OutputStream socketOut = clientSocket.getOutputStream()) {

    //         clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
    //         String commande = in.readLine();
            
    //         if (commande == null) return;

    //         logDebug("Requête reçue: " + commande);
            
    //         String[] parts = commande.split(" ", 4);
    //         String cmd = parts[0].toUpperCase();

    //         switch (cmd) {
    //             case "PING" -> handlePing(out);
    //             case "LIST" -> handleListFiles(socketOut);
    //             case "GET" -> handleGetFile(parts, socketOut, out);
    //             case "PEERS" -> handleGetPeers(socketOut);
    //             case "ANNOUNCE" -> handleAnnounce(parts, clientSocket, out);
    //             default -> out.println("ERREUR: commande inconnue");
    //         }

    //     } catch (Exception e) {
    //         logError("Erreur lors du traitement d'une requête", e);
    //     }
    // }

    // Handlers pour les différentes commandes
    private void handlePing(PrintWriter out) {
        out.println("PONG " + pseudo + " " + portEcoute);
    }

    private void handleListFiles(OutputStream socketOut) {
        synchronized (fileLock) {
            try {
                List<Metadata> metadatas = collecterMetadatasFichiers();
                byte[] data = serialiserListeMetadata(metadatas);
                envoyerDonneesBinaires(socketOut, data);
            } catch (Exception e) {
                logError("Erreur lors de l'envoi de la liste des fichiers", e);
                envoyerDonneesBinaires(socketOut, new byte[0]);
            }
        }
    }

    private void handleGetFile(String[] parts, OutputStream socketOut, PrintWriter out) {
        if (parts.length < 2) {
            out.println("ERREUR: commande GET invalide");
            return;
        }

        String nomFichier = parts[1];
        long offset = parts.length >= 3 ? parseOffset(parts[2]) : 0;
        
        envoyerFichier(nomFichier, socketOut, out, offset);
    }

    private void handleGetPeers(OutputStream socketOut) {
        try {
            List<PeerInfo> peersActifs = filtrerPeersActifs();
            byte[] data = PeerInfo.serialiserListe(peersActifs);
            envoyerDonneesBinaires(socketOut, data);
        } catch (Exception e) {
            logError("Erreur lors de l'envoi de la liste des peers", e);
            envoyerDonneesBinaires(socketOut, new byte[0]);
        }
    }

    private void handleAnnounce(String[] parts, Socket clientSocket, PrintWriter out) {
        if (parts.length < 3) {
            out.println("ERREUR: commande ANNOUNCE invalide");
            return;
        }

        try {
            String pseudoAnnonce = parts[1];
            int portAnnonce = Integer.parseInt(parts[2]);
            String adresseAnnonce = clientSocket.getInetAddress().getHostAddress();

            PeerInfo nouveauPeer = new PeerInfo(adresseAnnonce, portAnnonce, pseudoAnnonce);
            if (ajouterPeerSilencieux(nouveauPeer)) {
                out.println("OK PEER_ADDED");
            } else {
                out.println("OK PEER_UPDATED");
            }
        } catch (Exception e) {
            logError("Erreur lors de l'annonce", e);
            out.println("ERREUR: données invalides");
        }
    }

    /**
     * Découverte automatique des peers sur le réseau local
     */
    private void decouvriePeers() {
        logInfo("Démarrage de la découverte de peers...");
        
        CompletableFuture.runAsync(() -> {
            int peersInitiaux = peersConnus.size();
            List<CompletableFuture<Void>> taches = new ArrayList<>();

            // Scanner les ports de manière asynchrone
            for (int port = 8000; port <= 8100; port++) {
                if (port == portEcoute) continue;
                
                final int finalPort = port;
                CompletableFuture<Void> tache = CompletableFuture.runAsync(() -> {
                    if (testerConnexionPeer("localhost", finalPort)) {
                        ajouterPeerSilencieux(new PeerInfo("localhost", finalPort, ""));
                    }
                }, executorPrincipal);
                
                taches.add(tache);
            }

            // Attendre toutes les tâches avec timeout
            CompletableFuture<Void> toutesLesTaches = CompletableFuture.allOf(
                taches.toArray(new CompletableFuture[0]));
            
            try {
                toutesLesTaches.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                logError("Timeout lors de la découverte", e);
            }

            int nouveauxPeers = peersConnus.size() - peersInitiaux;
            if (nouveauxPeers > 0) {
                logInfo("Découverte terminée: " + nouveauxPeers + " nouveau(x) peer(s)");
            }
        }, executorPrincipal);
    }

    /**
     * Synchronisation silencieuse avec les peers connus
     */
    private void synchroniserPeersSilencieux() {
        if (!actif) return;

        Set<PeerInfo> nouveauxPeers = Collections.synchronizedSet(new HashSet<>());
        List<CompletableFuture<Void>> tachesSynchronisation = new ArrayList<>();

        for (PeerInfo peer : new ArrayList<>(peersConnus)) {
            CompletableFuture<Void> tache = CompletableFuture.runAsync(() -> {
                try {
                    annoncerAuPeer(peer);
                    Set<PeerInfo> peersDuPeer = recupererPeersDuPeer(peer);
                    nouveauxPeers.addAll(peersDuPeer);
                } catch (Exception e) {
                    logDebug("Erreur de sync avec " + peer + ": " + e.getMessage());
                }
            }, executorPrincipal);
            
            tachesSynchronisation.add(tache);
        }

        // Attendre toutes les synchronisations avec timeout
        CompletableFuture.allOf(tachesSynchronisation.toArray(new CompletableFuture[0]))
            .orTimeout(15, TimeUnit.SECONDS)
            .whenComplete((result, throwable) -> {
                // Ajouter les nouveaux peers découverts
                for (PeerInfo nouveauPeer : nouveauxPeers) {
                    if (!estPeerLocal(nouveauPeer)) {
                        ajouterPeerSilencieux(nouveauPeer);
                    }
                }
            });
    }

    /**
     * Nettoyage des peers inactifs
     */
    private void nettoyerPeersInactifs() {
        if (!actif) return;

        List<PeerInfo> peersASupprimer = peersConnus.stream()
            .filter(peer -> !peer.estActif(PEER_TIMEOUT_MS))
            .filter(peer -> !testerConnexionPeer(peer.getAdresse(), peer.getPort()))
            .collect(Collectors.toList());

        if (!peersASupprimer.isEmpty()) {
            peersConnus.removeAll(peersASupprimer);
            
            // Nettoyer le cache
            peersASupprimer.forEach(peer -> {
                String cle = peer.getAdresse() + ":" + peer.getPort();
                cacheFichiersPeers.remove(cle);
            });
            
            logDebug("Nettoyé " + peersASupprimer.size() + " peer(s) inactif(s)");
        }
    }

    /**
     * Mise à jour complète du cache des fichiers
     */
    private void mettreAJourCacheComplet() {
        if (!actif) return;

        List<CompletableFuture<Void>> tachesMiseAJour = peersConnus.stream()
            .filter(peer -> peer.estActif(PEER_TIMEOUT_MS))
            .map(peer -> CompletableFuture.runAsync(() -> 
                mettreAJourCachePeer(peer), executorPrincipal))
            .collect(Collectors.toList());

        // Exécuter toutes les mises à jour en parallèle avec timeout
        CompletableFuture.allOf(tachesMiseAJour.toArray(new CompletableFuture[0]))
            .orTimeout(20, TimeUnit.SECONDS)
            .whenComplete((result, throwable) -> {
                if (throwable != null) {
                    logDebug("Timeout lors de la mise à jour du cache");
                }
            });
    }

    /**
     * Test de connexion à un peer
     */
    private boolean testerConnexionPeer(String adresse, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(adresse, port), 2000);
            socket.setSoTimeout(2000);
            
            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                
                out.println("PING");
                String reponse = in.readLine();
                return reponse != null && reponse.startsWith("PONG");
            }
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Annonce ce peer à un peer distant
     */
    private void annoncerAuPeer(PeerInfo peer) throws IOException {
        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort())) {
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            
            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                
                out.println("ANNOUNCE " + pseudo + " " + portEcoute);
                String reponse = in.readLine();
                
                if (reponse != null && reponse.startsWith("OK")) {
                    peer.updatePing();
                }
            }
        }
    }

    /**
     * Récupère la liste des peers connus par un peer distant
     */
    private Set<PeerInfo> recupererPeersDuPeer(PeerInfo peer) {
        Set<PeerInfo> peersDistants = new HashSet<>();
        
        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort())) {
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            
            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {
                
                // Envoyer commande PEERS
                out.write("PEERS\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                
                // Lire réponse binaire
                byte[] data = lireDonneesBinaires(in);
                if (data.length > 0) {
                    List<PeerInfo> peers = PeerInfo.deserialiserListe(data);
                    peersDistants.addAll(peers);
                }
            }
        } catch (Exception e) {
            logDebug("Erreur lors de la récupération des peers de " + peer + ": " + e.getMessage());
        }
        
        return peersDistants;
    }

    /**
     * Met à jour le cache des fichiers d'un peer
     */
    private void mettreAJourCachePeer(PeerInfo peer) {
        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort())) {
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            
            try (OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {
                
                // Envoyer commande LIST
                out.write("LIST\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                
                // Lire réponse binaire
                byte[] data = lireDonneesBinaires(in);
                if (data.length > 0) {
                    List<Metadata> fichiers = deserialiserListeMetadata(data);
                    cacheFichiersPeers.put(peer.getAdresse() + ":" + peer.getPort(), fichiers);
                    peer.updatePing();
                }
            }
        } catch (Exception e) {
            logDebug("Erreur lors de la mise à jour du cache pour " + peer + ": " + e.getMessage());
        }
    }

    // ==================== MÉTHODES PUBLIQUES ====================

    /**
     * Recherche un fichier sur le réseau
     */
    public List<PeerInfo> rechercherFichier(String nomFichier) {
        return peersConnus.stream()
            .filter(peer -> peer.estActif(PEER_TIMEOUT_MS))
            .filter(peer -> {
                String clePeer = peer.getAdresse() + ":" + peer.getPort();
                List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
                return fichiers != null && fichiers.stream()
                    .anyMatch(meta -> meta.getNom().equals(nomFichier));
            })
            .collect(Collectors.toList());
    }

    /**
     * Télécharge un fichier depuis le réseau
     */
    public boolean telechargerFichier(String nomFichier) {
        List<PeerInfo> peersAvecFichier = rechercherFichier(nomFichier);
        
        if (peersAvecFichier.isEmpty()) {
            logInfo("Fichier '" + nomFichier + "' introuvable sur le réseau");
            return false;
        }
        
        logInfo("Fichier trouvé chez " + peersAvecFichier.size() + " peer(s)");
        
        for (PeerInfo peer : peersAvecFichier) {
            logInfo("Tentative de téléchargement depuis " + peer);
            if (telechargerDepuisPeer(peer, nomFichier)) {
                return true;
            }
        }
        
        logInfo("Échec du téléchargement depuis tous les peers");
        return false;
    }

    /**
     * Télécharge un fichier depuis un peer spécifique
     */
    public boolean telechargerDepuisPeer(PeerInfo peer, String nomFichier) {
        File fichierLocal = new File(dossierPartage, nomFichier);
        if (fichierLocal.exists()) {
            String nomLocal = genererNomUnique(fichierLocal);
            logInfo("Fichier existant, sauvegarde sous: " + nomLocal);
            fichierLocal = new File(dossierPartage, nomLocal);
        }

        try (Socket socket = new Socket(peer.getAdresse(), peer.getPort())) {
            socket.setSoTimeout(30000);
            
            try (InputStream socketIn = socket.getInputStream();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                
                out.println("GET " + nomFichier + " 0");
                
                String checksumServeur = lireLigne(socketIn);
                if (checksumServeur == null || checksumServeur.startsWith("ERREUR")) {
                    logError("Erreur lors de la demande du fichier: " + checksumServeur);
                    return false;
                }
                
                String tailleStr = lireLigne(socketIn);
                if (tailleStr == null) return false;
                
                long tailleFichier = Long.parseLong(tailleStr);
                
                // Télécharger le fichier
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fichierLocal))) {
                    copierAvecProgression(socketIn, bos, tailleFichier, nomFichier);
                }
                
                // Vérifier l'intégrité
                if (verifierIntegriteFichier(fichierLocal, checksumServeur)) {
                    logInfo("Fichier téléchargé avec succès: " + fichierLocal.getName());
                    return true;
                } else {
                    logError("Erreur checksum pour " + nomFichier);
                    fichierLocal.delete();
                    return false;
                }
            }
        } catch (Exception e) {
            logError("Erreur lors du téléchargement depuis " + peer, e);
            if (fichierLocal.exists()) {
                fichierLocal.delete();
            }
            return false;
        }
    }

    /**
     * Ajoute manuellement un peer
     */
    public void ajouterPeer(PeerInfo peerInfo) {
        if (estPeerLocal(peerInfo)) {
            logInfo("Impossible de s'ajouter soi-même comme peer");
            return;
        }
        
        if (ajouterPeerSilencieux(peerInfo)) {
            logInfo("Nouveau peer ajouté: " + peerInfo);
        } else {
            logInfo("Peer mis à jour: " + peerInfo);
        }
    }

    /**
     * Synchronisation manuelle
     */
    public void synchroniserMaintenant() {
        logInfo("Synchronisation manuelle déclenchée...");
        
        CompletableFuture.runAsync(() -> {
            synchroniserPeersSilencieux();
            logInfo("Synchronisation terminée. Total: " + peersConnus.size() + " peers");
        }, executorPrincipal);
    }

    // ==================== MÉTHODES D'AFFICHAGE ====================

    public void afficherEtatReseau() {
        System.out.println("\n=== État du réseau P2P ===");
        System.out.println("Peer: " + pseudo + " (port " + portEcoute + ")");
        System.out.println("Peers connus: " + peersConnus.size());

        for (PeerInfo peer : peersConnus) {
            String clePeer = peer.getAdresse() + ":" + peer.getPort();
            List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
            int nbFichiers = fichiers != null ? fichiers.size() : 0;
            long inactiviteMs = System.currentTimeMillis() - peer.getDernierePing();
            String statut = peer.estActif(PEER_TIMEOUT_MS) ? "ACTIF" : "INACTIF";
            System.out.println("  - " + peer + " (" + nbFichiers + " fichiers) [" 
                + statut + " - " + (inactiviteMs / 1000) + "s]");
        }

        List<File> mesFichiers = fileManager.listerFichiers();
        System.out.println("Mes fichiers partagés: " + mesFichiers.size());
        for (File f : mesFichiers) {
            if (f.isFile()) {
                System.out.println("  - " + f.getName() + " (" + formatTaille(f.length()) + ")");
            }
        }
        System.out.println("========================\n");
    }

    public void afficherFichiersDisponibles() {
        System.out.println("\n=== Fichiers disponibles sur le réseau ===");
        
        Map<String, List<PeerInfo>> fichiersParNom = new HashMap<>();
        
        for (PeerInfo peer : peersConnus) {
            if (!peer.estActif(PEER_TIMEOUT_MS)) continue;
            
            String clePeer = peer.getAdresse() + ":" + peer.getPort();
            List<Metadata> fichiers = cacheFichiersPeers.get(clePeer);
            
            if (fichiers != null) {
                for (Metadata meta : fichiers) {
                    fichiersParNom.computeIfAbsent(meta.getNom(), k -> new ArrayList<>()).add(peer);
                }
            }
        }
        
        if (fichiersParNom.isEmpty()) {
            System.out.println("Aucun fichier disponible sur le réseau");
        } else {
            fichiersParNom.forEach((nomFichier, peers) -> {
                System.out.print("  - " + nomFichier + " (disponible chez " + peers.size() + " peer(s): ");
                String pseudos = peers.stream()
                    .map(PeerInfo::getPseudo)
                    .collect(Collectors.joining(", "));
                System.out.println(pseudos + ")");
            });
        }
        System.out.println("==========================================\n");
    }

    // ==================== MÉTHODES UTILITAIRES ====================

    private boolean ajouterPeerSilencieux(PeerInfo peerInfo) {
        if (estPeerLocal(peerInfo)) return false;
        
        Optional<PeerInfo> existant = peersConnus.stream()
            .filter(p -> p.equals(peerInfo))
            .findFirst();
            
        if (existant.isPresent()) {
            existant.get().updatePing();
            if (peerInfo.getPseudo() != null && !peerInfo.getPseudo().isEmpty()) {
                existant.get().setPseudo(peerInfo.getPseudo());
            }
            return false;
        } else {
            peersConnus.add(peerInfo);
            schedulerMaintenance.execute(() -> mettreAJourCachePeer(peerInfo));
            return true;
        }
    }

    private boolean estPeerLocal(PeerInfo peer) {
        return (peer.getAdresse().equals("localhost") || peer.getAdresse().equals("127.0.0.1")) 
            && peer.getPort() == this.portEcoute;
    }

    private List<PeerInfo> filtrerPeersActifs() {
        return peersConnus.stream()
            .filter(peer -> peer.estActif(PEER_TIMEOUT_MS))
            .filter(peer -> !estPeerLocal(peer))
            .filter(peer -> peer.getPseudo() != null && !peer.getPseudo().trim().isEmpty())
            .collect(Collectors.toList());
    }

    private List<Metadata> collecterMetadatasFichiers() {
        return fileManager.listerFichiers().stream()
            .filter(File::isFile)
            .map(this::creerMetadata)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    private Metadata creerMetadata(File fichier) {
        try {
            return new Metadata(fichier.getName(), fichier.length(), 
                fileManager.calculerChecksum(fichier));
        } catch (Exception e) {
            logError("Erreur lors de la création des métadonnées pour " + fichier.getName(), e);
            return null;
        }
    }

    private void envoyerFichier(String nomFichier, OutputStream socketOut, PrintWriter out, long offset) {
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

                    copierFichier(raf, socketOut, taille - offset);
                }

                logDebug("Fichier envoyé: " + nomFichier);

            } catch (Exception e) {
                logError("Erreur lors de l'envoi du fichier " + nomFichier, e);
                out.println("ERREUR lors de l'envoi du fichier");
            }
        }
    }

    private void copierFichier(RandomAccessFile source, OutputStream destination, long taille) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long reste = taille;
        int lu;

        while (reste > 0 && (lu = source.read(buffer, 0, (int) Math.min(buffer.length, reste))) != -1) {
            destination.write(buffer, 0, lu);
            reste -= lu;
        }
        destination.flush();
    }

    private void copierAvecProgression(InputStream source, OutputStream destination, 
                                     long taille, String nomFichier) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long totalLu = 0;
        int lu;

        while (totalLu < taille && (lu = source.read(buffer, 0, 
               (int) Math.min(buffer.length, taille - totalLu))) != -1) {
            destination.write(buffer, 0, lu);
            totalLu += lu;
            
            // Afficher progression pour gros fichiers
            if (taille > 1024 * 1024 && totalLu % (1024 * 1024) == 0) {
                int progression = (int) ((totalLu * 100) / taille);
                System.out.print("\rTéléchargement " + nomFichier + ": " + progression + "%");
            }
        }
        
        if (taille > 1024 * 1024) {
            System.out.println(); // Nouvelle ligne après progression
        }
    }

    private boolean verifierIntegriteFichier(File fichier, String checksumAttendu) {
        try {
            String checksumCalcule = fileManager.calculerChecksum(fichier);
            return checksumCalcule.equals(checksumAttendu);
        } catch (Exception e) {
            logError("Erreur lors de la vérification du checksum", e);
            return false;
        }
    }

    private String genererNomUnique(File fichier) {
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

    // ==================== SÉRIALISATION ====================

    private byte[] serialiserListeMetadata(List<Metadata> metadatas) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            writeInt(bos, metadatas.size());
            
            for (Metadata meta : metadatas) {
                if (meta != null) {
                    byte[] metaData = meta.serialiser();
                    writeInt(bos, metaData.length);
                    bos.write(metaData);
                } else {
                    writeInt(bos, 0);
                }
            }
            
            return bos.toByteArray();
        } catch (IOException e) {
            logError("Erreur lors de la sérialisation des métadonnées", e);
            return new byte[0];
        }
    }

    private List<Metadata> deserialiserListeMetadata(byte[] data) {
        if (data == null || data.length < 4) {
            return new ArrayList<>();
        }

        List<Metadata> metadatas = new ArrayList<>();
        
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            int count = readInt(bis);
            if (count < 0 || count > 10000) {
                throw new IOException("Nombre de métadonnées invalide: " + count);
            }
            
            for (int i = 0; i < count; i++) {
                int metaDataLen = readInt(bis);
                
                if (metaDataLen == 0) continue;
                
                if (metaDataLen < 0 || metaDataLen > 100000) {
                    throw new IOException("Longueur de métadonnées invalide: " + metaDataLen);
                }
                
                byte[] metaData = new byte[metaDataLen];
                if (bis.read(metaData) != metaDataLen) {
                    throw new IOException("Impossible de lire les métadonnées " + i);
                }
                
                try {
                    Metadata meta = Metadata.deserialiser(metaData);
                    metadatas.add(meta);
                } catch (IOException e) {
                    logDebug("Métadonnées " + i + " corrompues, ignorées: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            logError("Erreur lors de la désérialisation des métadonnées", e);
        }
        
        return metadatas;
    }

    private void envoyerDonneesBinaires(OutputStream out, byte[] data) {
        try {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            lengthBuffer.putInt(data.length);
            out.write(lengthBuffer.array());
            out.write(data);
            out.flush();
        } catch (IOException e) {
            logError("Erreur lors de l'envoi de données binaires", e);
        }
    }

    private byte[] lireDonneesBinaires(InputStream in) throws IOException {
        byte[] lengthBytes = new byte[4];
        if (in.read(lengthBytes) != 4) {
            throw new IOException("Impossible de lire la longueur");
        }
        
        int length = ByteBuffer.wrap(lengthBytes).getInt();
        if (length < 0 || length > 10_000_000) { // 10MB max
            throw new IOException("Longueur de données invalide: " + length);
        }
        
        if (length == 0) return new byte[0];
        
        byte[] data = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(data, totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Fin de flux inattendue");
            }
            totalRead += read;
        }
        
        return data;
    }

    private String lireLigne(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            buffer.write(b);
        }
        return buffer.toString("UTF-8").trim();
    }

    // ==================== MÉTHODES UTILITAIRES DIVERSES ====================

    private static void writeInt(OutputStream os, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        os.write(buffer.array());
    }
    
    private static int readInt(InputStream is) throws IOException {
        byte[] buffer = new byte[4];
        if (is.read(buffer) != 4) {
            throw new IOException("Impossible de lire un entier");
        }
        return ByteBuffer.wrap(buffer).getInt();
    }

    private long parseOffset(String offsetStr) {
        try {
            return Long.parseLong(offsetStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String formatTaille(long octets) {
        if (octets < 1024) return octets + " B";
        if (octets < 1024 * 1024) return String.format("%.1f KB", octets / 1024.0);
        if (octets < 1024 * 1024 * 1024) return String.format("%.1f MB", octets / (1024.0 * 1024));
        return String.format("%.1f GB", octets / (1024.0 * 1024 * 1024));
    }

    // ==================== VALIDATION ====================

    private String validateString(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " ne peut pas être vide");
        }
        return value.trim();
    }

    private int validatePort(int port) {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port invalide: " + port);
        }
        return port;
    }

    // ==================== LOGGING ====================

    private void logInfo(String message) {
        System.out.println("[INFO] " + message);
    }

    private void logError(String message, Exception e) {
        System.err.println("[ERROR] " + message + ": " + e.getMessage());
        if (DEBUG_MODE) {
            e.printStackTrace();
        }
    }

    private void logError(String message) {
        System.err.println("[ERROR] " + message);
    }

    private void logDebug(String message) {
        if (DEBUG_MODE) {
            System.out.println("[DEBUG] " + message);
        }
    }

    // ==================== GESTION DES RESSOURCES ====================

    private void shutdownExecutor(ExecutorService executor, String name, int timeoutSeconds) {
        if (executor != null && !executor.isShutdown()) {
            logDebug("Arrêt de " + name + "...");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                    logDebug("Arrêt forcé de " + name);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void closeResource(AutoCloseable resource, String name) {
        if (resource != null) {
            try {
                resource.close();
                logDebug(name + " fermé");
            } catch (Exception e) {
                logError("Erreur lors de la fermeture de " + name, e);
            }
        }
    }

    // ==================== GETTERS ====================

    public String getPseudo() { return pseudo; }
    public int getPort() { return portEcoute; }
    public List<PeerInfo> getPeersConnus() { return new ArrayList<>(peersConnus); }
    public File getDossierPartage() { return dossierPartage; }
    public boolean estActif() { return actif; }

    /**
     * Obtient des statistiques sur le peer
     */
    public Map<String, Object> getStatistiques() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("pseudo", pseudo);
        stats.put("port", portEcoute);
        stats.put("actif", actif);
        stats.put("peers_connus", peersConnus.size());
        stats.put("peers_actifs", peersConnus.stream()
            .mapToInt(p -> p.estActif(PEER_TIMEOUT_MS) ? 1 : 0).sum());
        stats.put("fichiers_partages", fileManager.listerFichiers().size());
        stats.put("cache_fichiers_peers", cacheFichiersPeers.size());
        return stats;
    }

    /**
     * Test de connectivité avec tous les peers
     */
    public void testerConnectivite() {
        System.out.println("\n=== Test de connectivité ===");
        
        if (peersConnus.isEmpty()) {
            System.out.println("Aucun peer connu pour tester la connectivité");
            return;
        }
        
        List<CompletableFuture<Boolean>> tests = peersConnus.stream()
            .map(peer -> CompletableFuture.supplyAsync(() -> {
                System.out.print("Test de " + peer + "... ");
                boolean connecte = testerConnexionPeer(peer.getAdresse(), peer.getPort());
                System.out.println(connecte ? "OK" : "ECHEC");
                if (connecte) peer.updatePing();
                return connecte;
            }, executorPrincipal))
            .collect(Collectors.toList());

        // Attendre tous les tests
        CompletableFuture.allOf(tests.toArray(new CompletableFuture[0]))
            .orTimeout(10, TimeUnit.SECONDS)
            .thenRun(() -> {
                long peersActifs = tests.stream()
                    .mapToLong(future -> {
                        try {
                            return future.get() ? 1 : 0;
                        } catch (Exception e) {
                            return 0;
                        }
                    }).sum();
                System.out.println("Résultat: " + peersActifs + "/" + peersConnus.size() + " peers actifs");
                System.out.println("============================\n");
            })
            .exceptionally(throwable -> {
                System.out.println("Timeout lors des tests de connectivité");
                System.out.println("============================\n");
                return null;
            });
    }
}