package com.p2p;


public class Metadata {
    private String nom;
    private long taille;
    private String checksum;

    public Metadata(String nom, long taille, String checksum) {
        this.nom = nom;
        this.taille = taille;
        this.checksum = checksum;
    }

    public String getNom() { return nom; }
    public long getTaille() { return taille; }
    public String getChecksum() { return checksum; }
}
