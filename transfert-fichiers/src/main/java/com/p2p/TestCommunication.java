package com.p2p;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;

public class TestCommunication {
    public static void main(String[] args) throws InterruptedException {
        int port = 5000;
        String dossier = "/chemin/vers/dossierPartage";  // adapte ici

        NetworkManager serverManager = new NetworkManager(port, dossier);
        serverManager.demarrerServeur();

        Thread.sleep(1000); // attendre que le serveur soit prêt

        NetworkManager clientManager = new NetworkManager(port, null);
        String jsonListe = clientManager.envoyerCommande("localhost", "LIST");

        Gson gson = new Gson();
        Type listeType = new TypeToken<List<Metadata>>(){}.getType();
        List<Metadata> liste = gson.fromJson(jsonListe, listeType);

        System.out.println("Fichiers disponibles :");
        for (Metadata m : liste) {
            System.out.println(m.getNom() + " | taille: " + m.getTaille() + " | checksum: " + m.getChecksum());
        }
    }
}
