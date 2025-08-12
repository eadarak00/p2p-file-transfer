package com.p2p;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.p2p.entities.Client;
import com.p2p.entities.Serveur;
import com.p2p.utils.RessourceUtils;

import java.lang.reflect.Type;
import java.util.List;

public class TestCommunication {
    public static void main(String[] args) throws Exception {
        int port = 5000;
        String dossierServeur = RessourceUtils.getCheminPartage();
        String dossierClient = ".";  // dossier local pour stockage fichiers

        // Démarrage du serveur
        Serveur serveur = new Serveur(port, dossierServeur);
        serveur.demarrer();

        // Attendre que le serveur soit prêt
        Thread.sleep(1500);

        // Création du client avec dossier local (où dossier upload sera créé automatiquement)
        Client client = new Client("localhost", port, dossierClient);

        // Demander la liste des fichiers
        String jsonListe = client.envoyerCommande("LIST");

        if (jsonListe == null) {
            System.err.println("Erreur : aucune réponse du serveur.");
            return;
        }

        // Désérialiser la liste des métadonnées reçue
        Gson gson = new Gson();
        Type listeType = new TypeToken<List<Metadata>>(){}.getType();
        List<Metadata> liste = gson.fromJson(jsonListe, listeType);

        System.out.println("Fichiers disponibles :");
        for (Metadata m : liste) {
            System.out.println(m.getNom() + " | taille: " + m.getTaille() + " | checksum: " + m.getChecksum());
        }

        // Si des fichiers disponibles, télécharger le premier dans dossierClient/upload
        if (!liste.isEmpty()) {
            Metadata fichierATelecharger = liste.get(0);
            String nomFichier = fichierATelecharger.getNom();

            System.out.println("\nTéléchargement du fichier : " + nomFichier);
            boolean succes = client.telechargerFichier(nomFichier);

            if (succes) {
                System.out.println("Téléchargement terminé avec succès.");
            } else {
                System.err.println("Erreur lors du téléchargement.");
            }
        } else {
            System.out.println("Aucun fichier disponible pour téléchargement.");
        }
    }
}
