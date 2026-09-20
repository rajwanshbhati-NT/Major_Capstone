package com.caseStudy.beam.tools;

import com.caseStudy.beam.security.EncryptionService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Scanner;

public final class DecryptTool {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String key = System.getenv("INGESTION_AES_KEY");
        if (key == null || key.isBlank()) {
            System.err.println("ERROR: Set INGESTION_AES_KEY env var first.");
            System.err.println("  PowerShell: $env:INGESTION_AES_KEY=\"XixbmEP85D9z75Nij8zwnuoTVYAPQNwWVi5Cw7rtxdQ=\"");
            System.exit(1);
        }

        EncryptionService svc = new EncryptionService(key);

        if (args.length > 0) {
            decryptAndPrint(svc, args[0]);
        } else {
            Scanner sc = new Scanner(System.in);
            System.out.println("=== Decryption Tool ===");
            System.out.println("Paste an encrypted Base64 value and press Enter (Ctrl+C to exit):");
            while (sc.hasNextLine()) {
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;
                decryptAndPrint(svc, line);
                System.out.println("--- Next value: ---");
            }
        }
    }

    private static void decryptAndPrint(EncryptionService svc, String input) {
        try {
            if (input.startsWith("{")) {
                JsonNode node = MAPPER.readTree(input);
                if (node.has("phone_number_encrypted")) {
                    String phone = svc.decrypt(node.path("phone_number_encrypted").asText());
                    String salary = svc.decrypt(node.path("salary_encrypted").asText());
                    System.out.println("phone_number: " + phone);
                    System.out.println("salary:       " + salary);
                    return;
                }
            }
            String decrypted = svc.decrypt(input);
            System.out.println("decrypted: " + decrypted);
        } catch (Exception e) {
            System.err.println("Decrypt failed: " + e.getMessage());
        }
    }
}