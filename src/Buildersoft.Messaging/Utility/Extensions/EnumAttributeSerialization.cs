using System;
using System.Reflection;

namespace Buildersoft.Messaging.Utility.Extensions
{
    public static class EnumAttributeSerialization
    {
        public static string GetStringContent(this Enum value)
        {
            string output = null;
            Type type = value.GetType();

            FieldInfo fi = type.GetField(value.ToString());
            StringContent[] attrs =
               fi.GetCustomAttributes(typeof(StringContent),
                                       false) as StringContent[];
            if (attrs.Length > 0)
            {
                output = attrs[0].Content;
            }

            return output;
        }
    }
}
