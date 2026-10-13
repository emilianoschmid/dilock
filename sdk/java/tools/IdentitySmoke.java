import org.dilock.redis.Identity;

/** Dependency-free check for the identity source on toolchains without Maven. */
public final class IdentitySmoke {
    public static void main(String[] args) {
        if (args.length == 3 && "--vector".equals(args[0])) {
            String resource = new String(java.util.Base64.getDecoder().decode(args[2]), java.nio.charset.StandardCharsets.UTF_8);
            System.out.print(Identity.resourceId(args[1], resource));
            return;
        }
        if (args.length == 2 && "--codes".equals(args[0])) {
            String resource = new String(java.util.Base64.getDecoder().decode(args[1]), java.nio.charset.StandardCharsets.UTF_8);
            resource.codePoints().mapToObj(Integer::toHexString).forEach(code -> System.out.print(code + ","));
            return;
        }
        rejected("", "r");
        rejected("Prod", "r");
        rejected("a/b", "r");
        rejected("default", "");
        rejected("default", "a\0b");
        rejected("default", "\ud800");
        rejected("default", "x".repeat(1025));
        System.out.println("PASS Java identity invalid boundaries");
    }
    private static void rejected(String namespace, String resource) {
        try { Identity.resourceId(namespace, resource); }
        catch (IllegalArgumentException expected) { return; }
        throw new AssertionError("invalid identity was accepted");
    }
}
