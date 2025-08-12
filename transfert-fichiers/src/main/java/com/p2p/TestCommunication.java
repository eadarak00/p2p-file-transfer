package com.p2p;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.p2p.entities.Client;
import com.p2p.entities.Serveur;
import com.p2p.manager.Metadata;
import com.p2p.utils.RessourceUtils;

import java.lang.reflect.Type;
import java.util.List;

public class TestCommunication {

    public static void main(String[] args) throws Exception {
        int port = 5000;

        // On passe ici la racine des dossiers ("./uploads") et non le dossier public
        String dossierBase = RessourceUtils.getCheminBase();
        String dossierBaseClients = "./uploads/clients";

        // Lancer le serveur
        Serveur serveur = new Serveur(port, dossierBase);
        serveur.demarrer();
        System.out.println("Serveur démarré sur le port " + port);

        // Attendre un peu que le serveur soit bien prêt
        Thread.sleep(1500);

        // Créer 3 clients avec pseudo et dossiers locaux différents
        Client client1 = Client.getInstance("client1", "localhost", port, dossierBaseClients);
        Client client2 = Client.getInstance("client2", "localhost", port, dossierBaseClients);
        Client client3 = Client.getInstance("client3", "localhost", port, dossierBaseClients);

        // Lancer chaque client dans son propre thread
        Thread t1 = new Thread(() -> scenarioClient(client1));
        Thread t2 = new Thread(() -> scenarioClient(client2));
        Thread t3 = new Thread(() -> scenarioClient(client3));

        t1.start();
        t2.start();
        t3.start();

        // Attendre que tous les clients aient fini leur scénario
        t1.join();
        t2.join();
        t3.join();

        System.out.println("Test terminé.");
    }

    /**
     * Scénario d'un client : liste fichiers publics, télécharge un fichier, liste son dossier privé.
     */
    private static void scenarioClient(Client client) {
        try {
            // 1. Liste des fichiers dans le dossier public (dossier de partage)
            String jsonListePublique = client.envoyerCommande("LIST");

            if (jsonListePublique == null) {
                System.err.println("[" + client.getPseudo() + "] Pas de réponse du serveur.");
                return;
            }

            // Désérialisation de la liste JSON reçue en liste Metadata
            Gson gson = new Gson();
            Type listeType = new TypeToken<List<Metadata>>() {}.getType();
            List<Metadata> listePublique = gson.fromJson(jsonListePublique, listeType);

            // Affichage synchronisé dans la console pour éviter le mélange
            synchronized (System.out) {
                System.out.println("\n[" + client.getPseudo() + "] Fichiers disponibles dans le dossier public:");
                for (Metadata meta : listePublique) {
                    System.out.println("- " + meta.getNom() + " | taille: " + meta.getTaille() + " | checksum: " + meta.getChecksum());
                }
            }

            // 2. Téléchargement du premier fichier si la liste n'est pas vide
            if (!listePublique.isEmpty()) {
                Metadata fichierARecup = listePublique.get(0);
                synchronized (client) {
                    boolean telechargementOk = client.telechargerFichier(fichierARecup.getNom());
                    if (telechargementOk) {
                        System.out.println("[" + client.getPseudo() + "] Téléchargement réussi : " + fichierARecup.getNom());
                    } else {
                        System.err.println("[" + client.getPseudo() + "] Échec du téléchargement : " + fichierARecup.getNom());
                    }
                }
            } else {
                System.out.println("[" + client.getPseudo() + "] Aucun fichier disponible à télécharger.");
            }

            // 3. Liste des fichiers dans le dossier privé du client
            String jsonListePrivee = client.envoyerCommande("LIST " + client.getPseudo());

            if (jsonListePrivee != null) {
                List<Metadata> listePrivee = gson.fromJson(jsonListePrivee, listeType);
                synchronized (System.out) {
                    System.out.println("\n[" + client.getPseudo() + "] Fichiers dans son dossier privé :");
                    for (Metadata meta : listePrivee) {
                        System.out.println("- " + meta.getNom() + " | taille: " + meta.getTaille() + " | checksum: " + meta.getChecksum());
                    }
                }
            } else {
                System.err.println("[" + client.getPseudo() + "] Pas de réponse pour LIST privé.");
            }

        } catch (Exception e) {
            System.err.println("[" + client.getPseudo() + "] Exception dans scenarioClient :");
            e.printStackTrace();
        }
    }
}
