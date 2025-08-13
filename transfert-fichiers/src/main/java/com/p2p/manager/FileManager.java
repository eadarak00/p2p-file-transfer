package com.p2p.manager;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Version améliorée du FileManager avec meilleure gestion des checksums
 */
public class FileManager {
    private final String dossierPartage;
    private final Map<String, String> cacheChecksums = new ConcurrentHashMap<>();
    private final Map<String, Long> cacheTimestamps = new ConcurrentHashMap<>();
    
    public FileManager(String dossierPartage) {
        this.dossierPartage = dossierPartage;
    }

    public List<File> listerFichiers() {
        File dossier = new File(dossierPartage);
        File[] fichiers = dossier.listFiles();
        return fichiers != null ? Arrays.asList(fichiers) : new ArrayList<>();
    }

    /**
     * Calcul de checksum avec cache intelligent
     */
    public String calculerChecksum(File fichier) throws Exception {
        String cheminAbsolu = fichier.getAbsolutePath();
        long derniereModif = fichier.lastModified();
        
        // Vérifier le cache
        Long timestampCache = cacheTimestamps.get(cheminAbsolu);
        if (timestampCache != null && timestampCache == derniereModif) {
            String checksumCache = cacheChecksums.get(cheminAbsolu);
            if (checksumCache != null) {
                return checksumCache;
            }
        }
        
        // Calculer le checksum
        String checksum = calculerChecksumDirect(fichier);
        
        // Mettre à jour le cache
        cacheChecksums.put(cheminAbsolu, checksum);
        cacheTimestamps.put(cheminAbsolu, derniereModif);
        
        return checksum;
    }

    /**
     * Calcul direct du checksum sans cache
     */
    private String calculerChecksumDirect(File fichier) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        
        // Lecture par chunks pour économiser la mémoire
        try (FileInputStream fis = new FileInputStream(fichier);
             BufferedInputStream bis = new BufferedInputStream(fis)) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = bis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        
        byte[] hash = digest.digest();
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    /**
     * Vérification d'intégrité d'un fichier avec son checksum attendu
     */
    public boolean verifierIntegrite(File fichier, String checksumAttendu) {
        try {
            String checksumActuel = calculerChecksum(fichier);
            return checksumActuel.equals(checksumAttendu);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Lecture sécurisée d'un fichier avec vérification
     */
    public byte[] lireFichierSecurise(File fichier, String checksumAttendu) throws IOException {
        if (!fichier.exists() || !fichier.isFile()) {
            throw new IOException("Fichier inexistant ou invalide: " + fichier.getPath());
        }
        
        byte[] donnees = Files.readAllBytes(fichier.toPath());
        
        // Vérifier l'intégrité si checksum fourni
        if (checksumAttendu != null && !checksumAttendu.isEmpty()) {
            try {
                String checksumActuel = calculerChecksumPourBytes(donnees);
                if (!checksumActuel.equals(checksumAttendu)) {
                    throw new IOException("Checksum invalide - fichier corrompu");
                }
            } catch (Exception e) {
                throw new IOException("Erreur lors de la vérification du checksum: " + e.getMessage());
            }
        }
        
        return donnees;
    }

    /**
     * Écriture sécurisée avec vérification
     */
    public void ecrireFichierSecurise(String chemin, byte[] donnees, String checksumAttendu) throws IOException {
        // Vérifier le checksum des données avant écriture
        if (checksumAttendu != null && !checksumAttendu.isEmpty()) {
            try {
                String checksumActuel = calculerChecksumPourBytes(donnees);
                if (!checksumActuel.equals(checksumAttendu)) {
                    throw new IOException("Les données ne correspondent pas au checksum attendu");
                }
            } catch (Exception e) {
                throw new IOException("Erreur lors de la vérification du checksum: " + e.getMessage());
            }
        }
        
        // Écriture atomique (temporaire puis renommage)
        Path cheminFinal = Paths.get(chemin);
        Path cheminTemp = Paths.get(chemin + ".tmp");
        
        try {
            Files.write(cheminTemp, donnees, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            Files.move(cheminTemp, cheminFinal, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // Nettoyer le fichier temporaire en cas d'erreur
            try {
                Files.deleteIfExists(cheminTemp);
            } catch (IOException ignored) {}
            throw e;
        }
        
        // Invalider le cache pour ce fichier
        String cheminAbsolu = cheminFinal.toAbsolutePath().toString();
        cacheChecksums.remove(cheminAbsolu);
        cacheTimestamps.remove(cheminAbsolu);
    }

    /**
     * Calcul de checksum pour un tableau de bytes
     */
    private String calculerChecksumPourBytes(byte[] donnees) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(donnees);
        
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    /**
     * Nettoyage du cache des checksums
     */
    public void nettoyerCache() {
        cacheChecksums.clear();
        cacheTimestamps.clear();
    }

    /**
     * Obtenir statistiques du cache
     */
    public Map<String, Object> getStatistiquesCache() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("taille_cache_checksums", cacheChecksums.size());
        stats.put("taille_cache_timestamps", cacheTimestamps.size());
        return stats;
    }

    public String getDossierPartage() {
        return dossierPartage;
    }
}
