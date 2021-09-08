using Buildersoft.Messaging.Utility;

namespace Buildersoft.Messaging.Configuration.Parameters
{
    public enum MessageProtocol
    {
        [StringContent("persistent")]
        Persistent,
        [StringContent("non-persistent")]
        NonPersistent
    }
}
