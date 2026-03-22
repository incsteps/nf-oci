package incsteps.plugin.oci.nio
import java.security.*;

class PrivKeyUtil {
    static String generatePrivateKeyPem() throws Exception {

        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();

        PrivateKey privateKey = keyPair.getPrivate();
        byte[] privateKeyBytes = privateKey.getEncoded();
        String base64PrivateKey = Base64.getEncoder().encodeToString(privateKeyBytes);

        StringBuilder pem = new StringBuilder();
        pem.append("-----BEGIN PRIVATE KEY-----\n");

        for (int i = 0; i < base64PrivateKey.length(); i += 64) {
            int end = Math.min(i + 64, base64PrivateKey.length());
            pem.append(base64PrivateKey.substring(i, end)).append("\n");
        }

        pem.append("-----END PRIVATE KEY-----\n");

        return pem.toString();
    }
}
