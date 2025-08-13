package com.p2p;


import com.p2p.entities.SecurePeer;
import com.p2p.entities.PeerInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Scanner;

/**
 * Application de démonstration du système P2P sécurisé
 */
public class SecureP2PDemo {
    
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("=== Démonstration Système P2P Sécurisé ===");
        System.out.print("Entrez votre pseudo: ");
        String pseudo = scanner.nextLine();
        
        System.out.print("Entrez le port d'écoute (ex: 8001): ");
        int port = scanner.nextInt();
        scanner.nextLine(); // Consommer la ligne
        
        String dossierPartage = "./secure_uploads/" + pseudo;
        
        // Créer le peer sécurisé
        SecurePeer peer = new SecurePeer(pseudo, port, dossierPartage);
        
        // Créer quelques fichiers de test si le dossier est vide
        creerFichiersTest(new File(dossierPartage));
        
        // Démarrer le peer sécurisé
        peer.demarrer();
        
        // Ajouter un hook pour arrêter proprement
        Runtime.getRuntime().addShutdownHook(new Thread(peer::arreter));
        
        // Interface utilisateur
        afficherMenu();
        
        String commande;
        while (!(commande = scanner.nextLine()).equalsIgnoreCase("quit")) {
            traiterCommande(peer, commande, scanner);
        }
        
        peer.arreter();
        System.out.println("Au revoir!");
    }
    
    private static void afficherMenu() {
        System.out.println("\n=== MENU P2P SÉCURISÉ ===");
        System.out.println("status     - Afficher l'état du réseau sécurisé");
        System.out.println("search <fichier> - Rechercher un fichier (sécurisé)");
        System.out.println("download <fichier> - Télécharger un fichier (vérifié)");
        System.out.println("connect <ip> <port> - Se connecter à un peer (authentifié)");
        System.out.println("files      - Lister mes fichiers (protégés)");
        System.out.println("peers      - Lister les peers connectés (vérifiés)");
        System.out.println("security   - Afficher les statistiques de sécurité");
        System.out.println("test       - Tester la connectivité sécurisée");
        System.out.println("sync       - Synchronisation manuelle sécurisée");
        System.out.println("help       - Afficher ce menu");
        System.out.println("quit       - Quitter");
        System.out.print("\nCommande: ");
    }
    
    private static void traiterCommande(SecurePeer peer, String commande, Scanner scanner) {
        String[] parts = commande.trim().split("\\s+");
        if (parts.length == 0 || parts[0].isEmpty()) {
            System.out.print("\nCommande: ");
            return;
        }
        String cmd = parts[0].toLowerCase();
        
        switch (cmd) {
            case "status":
                peer.afficherEtatReseau();
                break;
                
            case "search":
                if (parts.length < 2) {
                    System.out.println("Usage: search <nom_fichier>");
                } else {
                    String nomFichier = parts[1];
                    List<PeerInfo> peers = peer.rechercherFichier(nomFichier);
                    if (peers.isEmpty()) {
                        System.out.println("Fichier '" + nomFichier + "' introuvable");
                    } else {
                        System.out.println("Fichier trouvé chez " + peers.size() + " peer(s) vérifié(s):");
                        for (PeerInfo p : peers) {
                            System.out.println("  - " + p);
                        }
                    }
                }
                break;
                
            case "download":
                if (parts.length < 2) {
                    System.out.println("Usage: download <nom_fichier>");
                } else {
                    String nomFichier = parts[1];
                    System.out.println("Téléchargement sécurisé de '" + nomFichier + "'...");
                    boolean succes = peer.telechargerFichierSecurise(nomFichier);
                    if (succes) {
                        System.out.println("✓ Téléchargement vérifié et réussi!");
                    } else {
                        System.out.println("✗ Échec du téléchargement (fichier corrompu ou non vérifié)");
                    }
                }
                break;
                
            case "connect":
                if (parts.length < 3) {
                    System.out.println("Usage: connect <adresse> <port>");
                } else {
                    String adresse = parts[1];
                    int port = Integer.parseInt(parts[2]);
                    PeerInfo nouveauPeer = new PeerInfo(adresse, port, "");
                    peer.ajouterPeer(nouveauPeer);
                    System.out.println("Tentative de connexion sécurisée à " + adresse + ":" + port);
                }
                break;
                
            case "files":
                System.out.println("Mes fichiers partagés (protégés):");
                File[] fichiers = peer.getDossierPartage().listFiles();
                if (fichiers != null) {
                    for (File f : fichiers) {
                        if (f.isFile()) {
                            try {
                                String checksum = peer.getFileManager().calculerChecksum(f);
                                System.out.println("  - " + f.getName() + " (" + f.length() + 
                                                 " octets) [checksum: " + checksum + "]");
                            } catch (Exception e) {
                                System.out.println("  - " + f.getName() + " (ERREUR: fichier corrompu)");
                            }
                        }
                    }
                } else {
                    System.out.println("  Aucun fichier");
                }
                break;
                
            case "peers":
                List<PeerInfo> peersConnus = peer.getPeersConnus();
                if (peersConnus.isEmpty()) {
                    System.out.println("Aucun peer connecté");
                } else {
                    System.out.println("Peers connectés et vérifiés (" + peersConnus.size() + "):");
                    for (PeerInfo p : peersConnus) {
                        String statut = p.estActif(30000) ? "✓" : "✗";
                        System.out.println("  " + statut + " " + p);
                    }
                }
                break;
                
            case "security":
                peer.afficherStatistiquesSecurite();
                break;
                
            case "test":
                peer.testerConnectivite();
                break;
                
            case "sync":
                peer.synchroniserMaintenant();
                break;
                
            case "help":
                afficherMenu();
                return; // Ne pas redemander la commande
                
            case "":
                break; // Ignore les lignes vides
                
            default:
                System.out.println("Commande inconnue: " + cmd);
                System.out.println("Tapez 'help' pour voir les commandes disponibles");
        }
        
        System.out.print("\nCommande: ");
    }
    
    /**
     * Crée quelques fichiers de test pour la démonstration
     */
    private static void creerFichiersTest(File dossier) {
        dossier.mkdirs();
        
        try {
            // Créer quelques fichiers de test s'ils n'existent pas
            File fichier1 = new File(dossier, "test1.txt");
            File fichier2 = new File(dossier, "document.txt");
            File fichier3 = new File(dossier, "readme.md");
            
            if (!fichier1.exists()) {
                Files.write(fichier1.toPath(), 
                    ("Fichier de test 1 sécurisé\nCréé par " + dossier.getName() + 
                     "\nContenu pour démonstration P2P sécurisé\n" +
                     "Timestamp: " + System.currentTimeMillis()).getBytes());
            }
            
            if (!fichier2.exists()) {
                Files.write(fichier2.toPath(), 
                    ("Document exemple sécurisé\n" +
                     "Ce fichier est partagé de manière sécurisée entre peers\n" +
                     "Ligne 3\nLigne 4\nLigne 5\n").getBytes());
            }
            
            if (!fichier3.exists()) {
                Files.write(fichier3.toPath(), 
                    ("# README Sécurisé\n\n" +
                     "Ce dossier contient les fichiers partagés de " + dossier.getName() + "\n\n" +
                     "## Instructions Sécurisées\n\n" +
                     "1. Démarrer le peer sécurisé\n" +
                     "2. Se connecter à d'autres peers vérifiés\n" +
                     "3. Partager et télécharger des fichiers avec intégrité vérifiée\n").getBytes());
            }
            
        } catch (IOException e) {
            System.err.println("Erreur lors de la création des fichiers de test: " + e.getMessage());
        }
    }
    
    /**
     * Démo rapide avec plusieurs peers automatiques sécurisés
     */
    public static void demoAutomatiqueSecurisee() {
        System.out.println("=== DÉMONSTRATION AUTOMATIQUE P2P SÉCURISÉE ===");
        
        // Créer 3 peers sécurisés
        SecurePeer peer1 = new SecurePeer("Alice", 8001, "./secure_uploads/alice");
        SecurePeer peer2 = new SecurePeer("Bob", 8002, "./secure_uploads/bob");
        SecurePeer peer3 = new SecurePeer("Charlie", 8003, "./secure_uploads/charlie");
        
        // Créer fichiers de test pour chaque peer
        creerFichiersTest(new File("./secure_uploads/alice"));
        creerFichiersTest(new File("./secure_uploads/bob"));
        creerFichiersTest(new File("./secure_uploads/charlie"));
        
        // Démarrer tous les peers
        peer1.demarrer();
        peer2.demarrer();
        peer3.demarrer();
        
        try {
            Thread.sleep(2000); // Attendre que tous démarrent
            
            // Connecter les peers entre eux avec validation
            peer1.ajouterPeer(new PeerInfo("localhost", 8002, "Bob"));
            peer1.ajouterPeer(new PeerInfo("localhost", 8003, "Charlie"));
            
            peer2.ajouterPeer(new PeerInfo("localhost", 8001, "Alice"));
            peer2.ajouterPeer(new PeerInfo("localhost", 8003, "Charlie"));
            
            peer3.ajouterPeer(new PeerInfo("localhost", 8001, "Alice"));
            peer3.ajouterPeer(new PeerInfo("localhost", 8002, "Bob"));
            
            Thread.sleep(3000); // Laisser le temps aux découvertes
            
            // Afficher l'état des réseaux
            peer1.afficherEtatReseau();
            peer2.afficherEtatReseau();
            peer3.afficherEtatReseau();
            
            // Test de téléchargement sécurisé
            System.out.println("Test: Alice télécharge un fichier de Bob avec vérification d'intégrité...");
            boolean succes = peer1.telechargerFichierSecurise("test1.txt");
            System.out.println("Résultat: " + (succes ? "Succès (fichier vérifié)" : "Échec (fichier corrompu ou non vérifié)"));
            
            // Afficher les statistiques de sécurité
            System.out.println("\nStatistiques de sécurité:");
            peer1.afficherStatistiquesSecurite();
            peer2.afficherStatistiquesSecurite();
            peer3.afficherStatistiquesSecurite();
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // Arrêter tous les peers
            peer1.arreter();
            peer2.arreter();
            peer3.arreter();
        }
    }
}