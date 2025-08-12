package com.p2p;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

public class FileManager {
    private final String dossierPartage;

    public FileManager(String dossierPartage) {
        this.dossierPartage = dossierPartage;
    }

    public List<File> listerFichiers() {
        File dossier = new File(dossierPartage);
        File[] fichiers = dossier.listFiles();
        return fichiers != null ? Arrays.asList(fichiers) : new ArrayList<>();
    }

    public String calculerChecksum(File fichier) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] bytes = Files.readAllBytes(fichier.toPath());
        byte[] hash = digest.digest(bytes);

        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public byte[] lireFichier(File fichier) throws IOException {
        return Files.readAllBytes(fichier.toPath());
    }

    public void ecrireFichier(String chemin, byte[] donnees) throws IOException {
        Files.write(Paths.get(chemin), donnees);
    }
}
