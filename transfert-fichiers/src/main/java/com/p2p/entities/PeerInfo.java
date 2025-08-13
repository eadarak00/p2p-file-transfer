package com.p2p.entities;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Classe représentant les informations d'un peer distant
 * Avec sérialisation binaire custom solide
 */
public class PeerInfo {
    // Version de sérialisation pour compatibilité future
    private static final int SERIALIZATION_VERSION = 1;
    
    private final String adresse;
    private final int port;
    private String pseudo;
    private long dernierePing;

    public PeerInfo(String adresse, int port, String pseudo) {
        this.adresse = adresse != null ? adresse : "";
        this.port = port;
        this.pseudo = pseudo != null ? pseudo : "";
        this.dernierePing = System.currentTimeMillis();
    }

    // Getters
    public String getAdresse() {
        return adresse;
    }

    public int getPort() {
        return port;
    }

    public String getPseudo() {
        return pseudo;
    }

    public void setPseudo(String pseudo) {
        this.pseudo = pseudo != null ? pseudo : "";
    }

    public long getDernierePing() {
        return dernierePing;
    }

    public void updatePing() {
        this.dernierePing = System.currentTimeMillis();
    }

    // Vérifier si le peer est encore actif (ping récent)
    public boolean estActif(long timeoutMs) {
        return (System.currentTimeMillis() - dernierePing) < timeoutMs;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PeerInfo peerInfo = (PeerInfo) obj;
        return port == peerInfo.port && Objects.equals(adresse, peerInfo.adresse);
    }

    @Override
    public int hashCode() {
        return Objects.hash(adresse, port);
    }

    @Override
    public String toString() {
        return pseudo + "@" + adresse + ":" + port;
    }

    /**
     * Sérialise ce PeerInfo en tableau de bytes
     * Format: [VERSION(4)][ADRESSE_LEN(4)][ADRESSE][PORT(4)][PSEUDO_LEN(4)][PSEUDO][PING(8)]
     */
    public byte[] serialiser() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // Version de sérialisation
            writeInt(bos, SERIALIZATION_VERSION);
            
            // Adresse
            byte[] adresseBytes = adresse.getBytes(StandardCharsets.UTF_8);
            writeInt(bos, adresseBytes.length);
            bos.write(adresseBytes);
            
            // Port
            writeInt(bos, port);
            
            // Pseudo
            byte[] pseudoBytes = pseudo.getBytes(StandardCharsets.UTF_8);
            writeInt(bos, pseudoBytes.length);
            bos.write(pseudoBytes);
            
            // Dernière ping
            writeLong(bos, dernierePing);
            
            return bos.toByteArray();
        }
    }

    /**
     * Désérialise un PeerInfo depuis un tableau de bytes
     */
    public static PeerInfo deserialiser(byte[] data) throws IOException {
        if (data == null || data.length < 20) { // Minimum: 4+4+0+4+4+0+8
            throw new IOException("Données de sérialisation invalides");
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            // Version
            int version = readInt(bis);
            if (version != SERIALIZATION_VERSION) {
                throw new IOException("Version de sérialisation non supportée: " + version);
            }
            
            // Adresse
            int adresseLen = readInt(bis);
            if (adresseLen < 0 || adresseLen > 1000) { // Limite raisonnable
                throw new IOException("Longueur d'adresse invalide: " + adresseLen);
            }
            byte[] adresseBytes = new byte[adresseLen];
            if (bis.read(adresseBytes) != adresseLen) {
                throw new IOException("Impossible de lire l'adresse complète");
            }
            String adresse = new String(adresseBytes, StandardCharsets.UTF_8);
            
            // Port
            int port = readInt(bis);
            if (port < 0 || port > 65535) {
                throw new IOException("Port invalide: " + port);
            }
            
            // Pseudo
            int pseudoLen = readInt(bis);
            if (pseudoLen < 0 || pseudoLen > 1000) { // Limite raisonnable
                throw new IOException("Longueur de pseudo invalide: " + pseudoLen);
            }
            byte[] pseudoBytes = new byte[pseudoLen];
            if (bis.read(pseudoBytes) != pseudoLen) {
                throw new IOException("Impossible de lire le pseudo complet");
            }
            String pseudo = new String(pseudoBytes, StandardCharsets.UTF_8);
            
            // Dernière ping
            long dernierePing = readLong(bis);
            
            PeerInfo peer = new PeerInfo(adresse, port, pseudo);
            peer.dernierePing = dernierePing;
            return peer;
        }
    }

    /**
     * Sérialise une liste de peers en tableau de bytes
     * Format: [COUNT(4)][PEER1_LEN(4)][PEER1_DATA][PEER2_LEN(4)][PEER2_DATA]...
     */
    public static byte[] serialiserListe(List<PeerInfo> peers) throws IOException {
        if (peers == null) {
            peers = new ArrayList<>();
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // Nombre de peers
            writeInt(bos, peers.size());
            
            // Chaque peer
            for (PeerInfo peer : peers) {
                if (peer != null) {
                    byte[] peerData = peer.serialiser();
                    writeInt(bos, peerData.length);
                    bos.write(peerData);
                } else {
                    // Peer null - écrire longueur 0
                    writeInt(bos, 0);
                }
            }
            
            return bos.toByteArray();
        }
    }

    /**
     * Désérialise une liste de peers depuis un tableau de bytes
     */
    public static List<PeerInfo> deserialiserListe(byte[] data) throws IOException {
        if (data == null || data.length < 4) {
            return new ArrayList<>();
        }

        List<PeerInfo> peers = new ArrayList<>();
        
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            // Nombre de peers
            int count = readInt(bis);
            if (count < 0 || count > 10000) { // Limite raisonnable
                throw new IOException("Nombre de peers invalide: " + count);
            }
            
            // Lire chaque peer
            for (int i = 0; i < count; i++) {
                int peerDataLen = readInt(bis);
                
                if (peerDataLen == 0) {
                    // Peer null - ignorer
                    continue;
                }
                
                if (peerDataLen < 0 || peerDataLen > 10000) { // Limite raisonnable
                    throw new IOException("Longueur de données peer invalide: " + peerDataLen);
                }
                
                byte[] peerData = new byte[peerDataLen];
                if (bis.read(peerData) != peerDataLen) {
                    throw new IOException("Impossible de lire les données du peer " + i);
                }
                
                try {
                    PeerInfo peer = deserialiser(peerData);
                    peers.add(peer);
                } catch (IOException e) {
                    // Ignorer ce peer corrompu et continuer
                    System.err.println("Peer " + i + " corrompu, ignoré: " + e.getMessage());
                }
            }
        }
        
        return peers;
    }

    /**
     * Méthodes utilitaires pour la sérialisation binaire
     */
    
    private static void writeInt(OutputStream os, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        os.write(buffer.array());
    }
    
    private static int readInt(InputStream is) throws IOException {
        byte[] buffer = new byte[4];
        if (is.read(buffer) != 4) {
            throw new IOException("Impossible de lire un entier (4 bytes)");
        }
        return ByteBuffer.wrap(buffer).getInt();
    }
    
    private static void writeLong(OutputStream os, long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        os.write(buffer.array());
    }
    
    private static long readLong(InputStream is) throws IOException {
        byte[] buffer = new byte[8];
        if (is.read(buffer) != 8) {
            throw new IOException("Impossible de lire un long (8 bytes)");
        }
        return ByteBuffer.wrap(buffer).getLong();
    }

    /**
     * Sérialise au format JSON simple (alternative lisible)
     */
    public String serialiserJSON() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"adresse\":\"").append(escapeJson(adresse)).append("\",");
        sb.append("\"port\":").append(port).append(",");
        sb.append("\"pseudo\":\"").append(escapeJson(pseudo)).append("\",");
        sb.append("\"dernierePing\":").append(dernierePing);
        sb.append("}");
        return sb.toString();
    }

    /**
     * Désérialise depuis JSON simple
     */
    public static PeerInfo deserialiserJSON(String json) throws IOException {
        if (json == null || json.trim().isEmpty()) {
            throw new IOException("JSON vide");
        }

        try {
            json = json.trim();
            if (!json.startsWith("{") || !json.endsWith("}")) {
                throw new IOException("Format JSON invalide");
            }

            json = json.substring(1, json.length() - 1); // Enlever { }
            
            String adresse = "";
            int port = 0;
            String pseudo = "";
            long dernierePing = System.currentTimeMillis();

            String[] parts = json.split(",");
            for (String part : parts) {
                part = part.trim();
                if (part.startsWith("\"adresse\":")) {
                    adresse = extractJsonString(part, "adresse");
                } else if (part.startsWith("\"port\":")) {
                    port = Integer.parseInt(part.split(":")[1].trim());
                } else if (part.startsWith("\"pseudo\":")) {
                    pseudo = extractJsonString(part, "pseudo");
                } else if (part.startsWith("\"dernierePing\":")) {
                    dernierePing = Long.parseLong(part.split(":")[1].trim());
                }
            }

            PeerInfo peer = new PeerInfo(adresse, port, pseudo);
            peer.dernierePing = dernierePing;
            return peer;

        } catch (Exception e) {
            throw new IOException("Erreur lors de la désérialisation JSON: " + e.getMessage());
        }
    }

    /**
     * Sérialise une liste au format JSON
     */
    public static String serialiserListeJSON(List<PeerInfo> peers) {
        if (peers == null || peers.isEmpty()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < peers.size(); i++) {
            if (i > 0) sb.append(",");
            if (peers.get(i) != null) {
                sb.append(peers.get(i).serialiserJSON());
            } else {
                sb.append("null");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Désérialise une liste depuis JSON
     */
    public static List<PeerInfo> deserialiserListeJSON(String json) throws IOException {
        if (json == null || json.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<PeerInfo> peers = new ArrayList<>();
        json = json.trim();
        
        if (!json.startsWith("[") || !json.endsWith("]")) {
            throw new IOException("Format JSON array invalide");
        }

        if (json.equals("[]")) {
            return peers;
        }

        json = json.substring(1, json.length() - 1); // Enlever [ ]
        
        // Parser simple pour objets JSON
        int braceCount = 0;
        int start = 0;
        
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '{') {
                braceCount++;
            } else if (c == '}') {
                braceCount--;
                if (braceCount == 0) {
                    // Fin d'un objet
                    String objJson = json.substring(start, i + 1).trim();
                    if (!objJson.equals("null")) {
                        try {
                            peers.add(deserialiserJSON(objJson));
                        } catch (IOException e) {
                            System.err.println("Objet JSON ignoré: " + e.getMessage());
                        }
                    }
                    start = i + 2; // Passer la virgule
                }
            }
        }

        return peers;
    }

    // Méthodes utilitaires pour JSON
    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\"", "\\\"").replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static String extractJsonString(String part, String key) {
        String[] keyValue = part.split(":", 2);
        if (keyValue.length != 2) return "";
        String value = keyValue[1].trim();
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    /**
     * Méthode de validation pour vérifier l'intégrité
     */
    public boolean estValide() {
        return adresse != null && !adresse.trim().isEmpty() && 
               port > 0 && port <= 65535 && 
               pseudo != null && 
               dernierePing > 0;
    }

    /**
     * Crée une copie de ce PeerInfo
     */
    public PeerInfo copier() {
        PeerInfo copie = new PeerInfo(adresse, port, pseudo);
        copie.dernierePing = this.dernierePing;
        return copie;
    }
}