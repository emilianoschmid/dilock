import org.dilock.redis.AcquireOptions;
import org.dilock.etcd.EtcdProvider;
import org.dilock.etcd.EtcdProviderOptions;

public final class Quickstart {
    public static void main(String[] args) throws Exception {
        String endpoint = env("DILOCK_ETCD_ENDPOINT", "http://127.0.0.1:2379");
        String epoch = env("DILOCK_EPOCH", "dev-1");
        if (args.length == 0) {
            try (var provider = EtcdProvider.connect(endpoint, epoch)) {
                var daily = provider.lock("reports/daily");
                try (var lease = daily.acquire()) {
                    lease.assertValid();
                    System.out.println("Ran a short etcd protected operation at revision " + lease.createRevision() + ".");
                }
            }
        } else if ("--configured".equals(args[0])) {
            var policy = new AcquireOptions(10_000, 60_000);
            var settings = new EtcdProviderOptions(endpoint, env("DILOCK_NAMESPACE", "reports"), epoch, 2_000, 2_000, policy,
                    observation -> System.err.println("cleanup " + observation.outcome() + " " + observation.reason()));
            try (var provider = EtcdProvider.connect(settings)) {
                var daily = provider.lock("daily-configured");
                try (var lease = daily.acquire(policy)) {
                    lease.assertValid();
                    System.out.println("Ran one configured etcd operation at revision " + lease.createRevision() + ".");
                }
            }
        } else if ("--shared-provider".equals(args[0])) {
            try (var provider = EtcdProvider.connect(endpoint, epoch)) {
                var daily = provider.lock("reports/daily");
                var weekly = provider.lock("reports/weekly");
                try (var dailyLease = daily.acquire()) {
                    try (var weeklyLease = weekly.acquire()) {
                        dailyLease.assertValid();
                        weeklyLease.assertValid();
                        System.out.println("Two distinct etcd resources share one provider; revisions=" + dailyLease.createRevision() + "," + weeklyLease.createRevision() + ".");
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
