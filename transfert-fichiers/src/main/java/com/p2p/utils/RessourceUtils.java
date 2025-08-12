package com.p2p.utils;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class RessourceUtils {

    /**
     * Renvoie le chemin absolu vers le dossier 'partage' dans src/main/resources.
     * Utilisé côté serveur pour le dossier public.
     */
    public static String getCheminPartage() throws Exception {
        ClassLoader classLoader = RessourceUtils.class.getClassLoader();
        URL resource = classLoader.getResource("partage");

        if (resource == null) {
            throw new Exception("Dossier 'partage' introuvable dans resources !");
        }
        try {
            return new File(resource.toURI()).getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Erreur lors de la récupération du chemin du dossier 'partage'", e);
        }
    }

    /**
     * Renvoie le chemin racine d'uploads (par exemple ./uploads).
     * Peut être utile pour passer au serveur.
     * Ici, on considère qu'il y a un dossier uploads dans le projet.
     */
    public static String getCheminBase() {
        File uploads = new File("./uploads");
        if (!uploads.exists()) {
            uploads.mkdirs();
        }
        return uploads.getAbsolutePath();
    }
}
