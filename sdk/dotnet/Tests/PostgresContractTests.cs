using Dilock.Postgres;
using Xunit;

namespace Dilock.Tests;

public sealed class PostgresContractTests
{
    [Fact]
    public void AdvisoryKeyUsesSignedBigEndianIdentityPrefix()
    {
        Assert.Equal(1L, PostgresProvider.AdvisoryKey("0000000000000001" + new string('0', 48)));
        Assert.Equal(-1L, PostgresProvider.AdvisoryKey("ffffffffffffffff" + new string('0', 48)));
    }

    [Fact]
    public void MetadataProfileAndResourceIdentityAreSeparate()
    {
        var id = Dilock.ResourceIdentity.Hash("test", "invoice/1");
        Assert.Equal(64, id.Length);
        Assert.Equal(PostgresProvider.AdvisoryKey(id), PostgresProvider.AdvisoryKey(id));
    }
}
