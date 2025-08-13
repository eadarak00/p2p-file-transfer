package com.p2p;

import com.p2p.entities.Peer;
import com.p2p.entities.PeerInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Scanner;

/**
 * Application de démonstration du système P2P
 * Adaptée au Peer refactorisé (démarrage asynchrone)
 */
public class P2PDemo {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("=== Démonstration Système P2P ===");
        System.out.print("Entrez votre pseudo: ");
        String pseudo = scanner.nextLine();

        System.out.print("Entrez le port d'écoute (ex: 8001): ");
        int port = scanner.nextInt();
        scanner.nextLine(); // Consommer la ligne

        String dossierPartage = "./uploads/" + pseudo;

        // Créer le peer
        Peer peer = new Peer(pseudo, port, dossierPartage);

        // Créer quelques fichiers de test si le dossier est vide
        creerFichiersTest(new File(dossierPartage));

        // Démarrer le peer et attendre qu'il soit prêt
        peer.demarrer().join();

        // Ajouter un hook pour arrêter proprement à la fermeture du programme
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
        System.out.println("\n=== MENU P2P ===");
        System.out.println("status     - Afficher l'état du réseau");
        System.out.println("search <fichier> - Rechercher un fichier");
        System.out.println("download <fichier> - Télécharger un fichier");
        System.out.println("connect <ip> <port> - Se connecter à un peer");
        System.out.println("files      - Lister mes fichiers");
        System.out.println("peers      - Lister les peers connectés");
        System.out.println("help       - Afficher ce menu");
        System.out.println("quit       - Quitter");
        System.out.print("\nCommande: ");
    }

    private static void traiterCommande(Peer peer, String commande, Scanner scanner) {
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
                        System.out.println("Fichier trouvé chez " + peers.size() + " peer(s):");
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
                    System.out.println("Téléchargement de '" + nomFichier + "'...");
                    boolean succes = peer.telechargerFichier(nomFichier);
                    if (succes) {
                        System.out.println("✓ Téléchargement réussi!");
                    } else {
                        System.out.println("✗ Échec du téléchargement");
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
                    System.out.println("Tentative de connexion à " + adresse + ":" + port);
                }
                break;

            case "files":
                System.out.println("Mes fichiers partagés:");
                File[] fichiers = peer.getDossierPartage().listFiles();
                if (fichiers != null) {
                    for (File f : fichiers) {
                        if (f.isFile()) {
                            System.out.println("  - " + f.getName() + " (" + f.length() + " octets)");
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
                    System.out.println("Peers connectés (" + peersConnus.size() + "):");
                    for (PeerInfo p : peersConnus) {
                        System.out.println("  - " + p);
                    }
                }
                break;

            case "help":
                afficherMenu();
                return;

            case "":
                break;

            default:
                System.out.println("Commande inconnue: " + cmd);
                System.out.println("Tapez 'help' pour voir les commandes disponibles");
        }

        System.out.print("\nCommande: ");
    }

    private static void creerFichiersTest(File dossier) {
        dossier.mkdirs();

        try {
            File fichier1 = new File(dossier, "test1.txt");
            File fichier2 = new File(dossier, "document.txt");
            File fichier3 = new File(dossier, "readme.md");

            if (!fichier1.exists()) {
                Files.write(fichier1.toPath(),
                        ("Fichier de test 1\nCréé par " + dossier.getName() +
                                "\nContenu pour démonstration P2P\n" +
                                "Timestamp: " + System.currentTimeMillis()).getBytes());
            }

            if (!fichier2.exists()) {
                Files.write(fichier2.toPath(),
                        ("Document exemple\n" +
                                "Ce fichier peut être partagé entre peers\n" +
                                "Ligne 3\nLigne 4\nLigne 5\n").getBytes());
            }

            if (!fichier3.exists()) {
                Files.write(fichier3.toPath(),
                        ("# README\n\n" +
                                "Ce dossier contient les fichiers partagés de " + dossier.getName() + "\n\n" +
                                "## Instructions\n\n" +
                                "1. Démarrer le peer\n" +
                                "2. Se connecter à d'autres peers\n" +
                                "3. Partager et télécharger des fichiers\n").getBytes());
            }

        } catch (IOException e) {
            System.err.println("Erreur lors de la création des fichiers de test: " + e.getMessage());
        }
    }
}
