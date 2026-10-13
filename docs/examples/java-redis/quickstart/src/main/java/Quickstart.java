import org.dilock.redis.AcquireOptions;
import org.dilock.redis.RedisProvider;
import org.dilock.redis.RedisProviderOptions;

public final class Quickstart {
    public static void main(String[] args) throws Exception {
        String endpoint = env("DILOCK_REDIS_ENDPOINT", "redis://127.0.0.1:6379");
        String epoch = env("DILOCK_EPOCH", "dev-1");
        if (args.length == 0) {
            try (var provider = RedisProvider.connect(endpoint, epoch)) {
                var daily = provider.lock("reports/daily");
                try (var lease = daily.acquire()) {
                    lease.assertValid();
                    System.out.println("Ran a short Redis protected operation.");
                }
            }
        } else if ("--configured".equals(args[0])) {
            var policy = new AcquireOptions(10_000, 60_000);
            var settings = new RedisProviderOptions(endpoint, env("DILOCK_NAMESPACE", "reports"), epoch, 2_000, 2_000, policy,
                    observation -> System.err.println("cleanup " + observation.outcome() + " " + observation.reason()));
            try (var provider = RedisProvider.connect(settings)) {
                var daily = provider.lock("daily-configured");
                try (var lease = daily.acquire(policy)) {
                    lease.assertValid();
                    System.out.println("Ran one configured Redis protected operation.");
                }
            }
        } else if ("--shared-provider".equals(args[0])) {
            try (var provider = RedisProvider.connect(endpoint, epoch)) {
                var daily = provider.lock("reports/daily");
                var weekly = provider.lock("reports/weekly");
                try (var dailyLease = daily.acquire()) {
                    try (var weeklyLease = weekly.acquire()) {
                        dailyLease.assertValid();
                        weeklyLease.assertValid();
                        System.out.println("Two distinct Redis resources are held through one provider.");
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Usage: Quickstart [--configured|--shared-provider]");
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
