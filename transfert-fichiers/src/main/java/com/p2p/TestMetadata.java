package com.p2p;

import com.p2p.utils.JsonUtils;

public class TestMetadata {
    public static void main(String[] args) throws Exception {
        Metadata meta = new Metadata("fichier.txt", 12345, "abc123");
        JsonUtils.ecrireMetadata(meta, "metadata.json");

        Metadata lu = JsonUtils.lireMetadata("metadata.json");
        System.out.println("Nom: " + lu.getNom());
        System.out.println("Taille: " + lu.getTaille());
        System.out.println("Checksum: " + lu.getChecksum());
    }
}
