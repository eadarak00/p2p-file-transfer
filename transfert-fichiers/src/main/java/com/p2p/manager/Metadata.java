package com.p2p.manager;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Version améliorée de la classe Metadata avec vérification d'intégrité
 */
public class Metadata {
    private final String nom;
    private final long taille;
    private final String checksum;
    private final long timestamp; // Ajout pour détecter les modifications
    
    // Magic bytes pour vérifier l'intégrité de la sérialisation
    private static final int MAGIC_BYTES = 0x4D455441; // "META" en hex
    private static final int VERSION = 1;

    public Metadata(String nom, long taille, String checksum) {
        this.nom = nom != null ? nom : "";
        this.taille = taille;
        this.checksum = checksum != null ? checksum : "";
        this.timestamp = System.currentTimeMillis();
    }

    // Version avec timestamp custom
    public Metadata(String nom, long taille, String checksum, long timestamp) {
        this.nom = nom != null ? nom : "";
        this.taille = taille;
        this.checksum = checksum != null ? checksum : "";
        this.timestamp = timestamp;
    }

    // Getters existants + nouveau
    public String getNom() { return nom; }
    public long getTaille() { return taille; }
    public String getChecksum() { return checksum; }
    public long getTimestamp() { return timestamp; }

    /**
     * Sérialisation améliorée avec vérification d'intégrité
     * Format: [MAGIC(4)][VERSION(4)][NOM_LEN(4)][NOM][TAILLE(8)][CHECKSUM_LEN(4)][CHECKSUM][TIMESTAMP(8)][CRC32(4)]
     */
    public byte[] serialiser() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // Magic bytes et version
            writeInt(bos, MAGIC_BYTES);
            writeInt(bos, VERSION);
            
            // Données
            byte[] nomBytes = nom.getBytes(StandardCharsets.UTF_8);
            writeInt(bos, nomBytes.length);
            bos.write(nomBytes);
            
            writeLong(bos, taille);
            
            byte[] checksumBytes = checksum.getBytes(StandardCharsets.UTF_8);
            writeInt(bos, checksumBytes.length);
            bos.write(checksumBytes);
            
            writeLong(bos, timestamp);
            
            // Calculer CRC32 des données
            byte[] dataBytes = bos.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(dataBytes);
            writeInt(bos, (int) crc.getValue());
            
            return bos.toByteArray();
        }
    }

    /**
     * Désérialisation avec vérification d'intégrité
     */
    public static Metadata deserialiser(byte[] data) throws IOException {
        if (data == null || data.length < 28) { // Minimum requis
            throw new IOException("Données invalides - taille insuffisante");
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
            // Vérifier magic bytes
            int magic = readInt(bis);
            if (magic != MAGIC_BYTES) {
                throw new IOException("Magic bytes invalides: 0x" + Integer.toHexString(magic));
            }
            
            // Vérifier version
            int version = readInt(bis);
            if (version != VERSION) {
                throw new IOException("Version non supportée: " + version);
            }
            
            // Extraire CRC32 à la fin
            byte[] dataForCrc = Arrays.copyOfRange(data, 0, data.length - 4);
            int expectedCrc = ByteBuffer.wrap(data, data.length - 4, 4).getInt();
            
            // Vérifier CRC32
            CRC32 crc = new CRC32();
            crc.update(dataForCrc);
            if ((int) crc.getValue() != expectedCrc) {
                throw new IOException("Checksum CRC32 invalide - données corrompues");
            }
            
            // Lire les données
            int nomLen = readInt(bis);
            if (nomLen < 0 || nomLen > 1000) {
                throw new IOException("Longueur de nom invalide: " + nomLen);
            }
            byte[] nomBytes = new byte[nomLen];
            if (bis.read(nomBytes) != nomLen) {
                throw new IOException("Impossible de lire le nom complet");
            }
            String nom = new String(nomBytes, StandardCharsets.UTF_8);
            
            long taille = readLong(bis);
            if (taille < 0) {
                throw new IOException("Taille invalide: " + taille);
            }
            
            int checksumLen = readInt(bis);
            if (checksumLen < 0 || checksumLen > 1000) {
                throw new IOException("Longueur de checksum invalide: " + checksumLen);
            }
            byte[] checksumBytes = new byte[checksumLen];
            if (bis.read(checksumBytes) != checksumLen) {
                throw new IOException("Impossible de lire le checksum complet");
            }
            String checksum = new String(checksumBytes, StandardCharsets.UTF_8);
            
            long timestamp = readLong(bis);
            
            return new Metadata(nom, taille, checksum, timestamp);
        }
    }

    // Méthodes utilitaires...
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
}
