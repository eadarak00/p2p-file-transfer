package com.p2p.utils;

import java.io.File;
import java.net.URL;

public class RessourceUtils {

    // Renvoie le chemin absolu vers src/main/resources/partage à l'exécution
    public static String getCheminPartage() throws Exception {
        ClassLoader classLoader = RessourceUtils.class.getClassLoader();
        URL resource = classLoader.getResource("partage");

        if (resource == null) {
            throw new Exception("Dossier 'partage' introuvable dans resources !");
        }
        return new File(resource.toURI()).getAbsolutePath();
    }
}