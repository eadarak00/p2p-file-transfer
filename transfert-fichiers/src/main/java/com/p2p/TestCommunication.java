package com.p2p;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.p2p.utils.RessourceUtils;

import java.lang.reflect.Type;
import java.util.List;

public class TestCommunication {
    public static void main(String[] args) throws Exception {
        int port = 5000;
        String dossier = RessourceUtils.getCheminPartage();

        NetworkManager serverManager = new NetworkManager(port, dossier);
        serverManager.demarrerServeur();

        // Attendre un peu plus longtemps pour être sûr que le serveur est prêt
        Thread.sleep(1500);

        NetworkManager clientManager = new NetworkManager(port, null);
        String jsonListe = clientManager.envoyerCommande("localhost", "LIST");

        if (jsonListe == null) {
            System.err.println("Erreur : aucune réponse du serveur.");
            return;
        }

        Gson gson = new Gson();
        Type listeType = new TypeToken<List<Metadata>>(){}.getType();
        List<Metadata> liste = gson.fromJson(jsonListe, listeType);

        System.out.println("Fichiers disponibles :");
        for (Metadata m : liste) {
            System.out.println(m.getNom() + " | taille: " + m.getTaille() + " | checksum: " + m.getChecksum());
        }
    }
}
