using System;

namespace Buildersoft.Messaging.Utility
{
    public class StringContent : Attribute
    {
        private readonly string _content;

        public StringContent(string content)
        {
            _content = content;
        }

        public string Content
        {
            get { return _content; }
        }
    }
}
