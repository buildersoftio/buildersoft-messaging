using Newtonsoft.Json;
using System;

namespace Buildersoft.Messaging.Utility.Extensions
{
    public static class JsonSerialization
    {
        public static string ToJson(this object obj)
        {
            return JsonConvert.SerializeObject(obj, new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
        }

        public static string ToJson<T>(this T tTopic)
        {
            return JsonConvert.SerializeObject(tTopic, new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
        }

        public static T ToTopicObject<T>(this string jsonRaw)
        {
            return JsonConvert.DeserializeObject<T>(jsonRaw, new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
        }

        public static T ToObject<T>(this string jsonRaw)
        {
            return JsonConvert.DeserializeObject<T>(jsonRaw, new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
        }

        public static dynamic JsonToDynamic(this string jsonRaw, Type type)
        {
            return JsonConvert.DeserializeObject<dynamic>(jsonRaw, new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
        }
    }

}
